package workpool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// Handler is the interface of working routine handler
//
// Do function is the main worker routine. It is the real routine called from Sync/Async.
// The interface{} represents the input params from caller and the bool flag means the call
// from async or sync. The return interface{} and error value will be returned back to Sync
// or be returned by the Callback function from Async.
type Handler interface {
	Do(context.Context, interface{}, bool) (interface{}, error)
}

// HandlerFunc is the work routine. ctx references the working context and job references the job.
// !!! pool.Scale/pool.Stop MUST NOT be called in the same goroutine of the handler
type HandlerFunc func(ctx context.Context, job interface{}, async bool) (interface{}, error)

func (f HandlerFunc) Do(ctx context.Context, job interface{}, async bool) (interface{}, error) {
	return f(ctx, job, async)
}

// Callback returns the result and error back to async caller.
type Callback func(ctx context.Context, result interface{}, err error)

// Pool is the auto-scale worker pool with non-return value handler
type Pool interface {
	// Start creates required number of workers
	Start()

	// Sync calls handler function to do the job
	Sync(context.Context, interface{}) (interface{}, error)

	// Async add one job and callback function into the pool and return
	Async(context.Context, interface{}, Callback) error

	// Scale changes the worker pool size
	Scale(count int, force bool) error

	// Stop the worker pool. Block until all the worker routine quit.
	Stop()

	// Count returns current worker routine number.
	Count() int
}

var (
	ErrContextCanel     = errors.New("context cancelled")
	ErrPoolIsNotRunning = errors.New("pool is not running")
	ErrPoolFull         = errors.New("workpool is full")
	ErrScaleConflict    = errors.New("scale conflict")
)

type Result struct {
	Context  context.Context
	Callback Callback
	Value    interface{}
	Error    error
}

type Task struct {
	Async    bool
	Context  context.Context
	Value    interface{}
	Callback Callback
	Result   chan Result
}

type Controller struct {
	Cancel context.CancelFunc
	Done   chan struct{}
}

type pool struct {
	sync.RWMutex
	wg          sync.WaitGroup
	count       int64
	async_block bool
	sync_queue  chan Task
	async_queue chan Task
	callback    chan Result
	handler     Handler
	controllers []Controller
	scaling     int64
	running     int64
}

// NewPool creates an instance of Pool
// 	count       - worker number
// 	aysnc_block - if async calls block
//  handler 	- worker handle function
func NewPool(count int, async_block bool, handler HandlerFunc) Pool {
	return &pool{
		count:       int64(count),
		handler:     handler,
		async_block: async_block,
	}
}

func (p *pool) Sync(ctx context.Context, job interface{}) (interface{}, error) {
	// TODO check if the pool is closed
	if atomic.LoadInt64(&p.running) == 0 {
		return nil, ErrPoolIsNotRunning
	}
	task := Task{
		Context: ctx,
		Value:   job,
		Result:  make(chan Result, 1),
	}
	select {
	case <-ctx.Done():
		return nil, ErrContextCanel
	case p.sync_queue <- task:
	}
	select {
	case <-ctx.Done():
		return nil, ErrContextCanel
	case result := <-task.Result:
		return result.Value, result.Error
	}
}

func (p *pool) Async(ctx context.Context, job interface{}, callback Callback) error {
	if atomic.LoadInt64(&p.running) == 0 {
		return ErrPoolIsNotRunning
	}
	task := Task{
		Async:    true,
		Context:  ctx,
		Value:    job,
		Callback: callback,
	}
	if p.async_block {
		select {
		case <-ctx.Done():
			return ErrContextCanel
		case p.async_queue <- task:
			return nil
		}
	} else {
		select {
		case <-ctx.Done():
			return ErrContextCanel
		case p.async_queue <- task:
			return nil
		default:
			return ErrPoolFull
		}
	}
}

func (p *pool) Callback() {
	for result := range p.callback {
		if result.Callback != nil {
			result.Callback(result.Context, result.Value, result.Error)
		}
	}
}

// TODO force scale
func (p *pool) Scale(count int, force bool) error {
	if atomic.LoadInt64(&p.scaling) == 1 {
		return ErrScaleConflict
	}
	if atomic.LoadInt64(&p.running) == 0 {
		return ErrPoolIsNotRunning
	}
	p.Lock()
	atomic.StoreInt64(&p.scaling, 1)
	defer func() {
		atomic.StoreInt64(&p.scaling, 0)
		p.Unlock()
	}()

	if count > len(p.controllers) {
		// add worker
		for i := len(p.controllers); i < count; i++ {
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			p.controllers = append(p.controllers, Controller{
				Cancel: cancel,
				Done:   done,
			})
			atomic.AddInt64(&p.count, 1)
			go p.do(ctx, i, done)
			p.wg.Add(1)
		}
	} else if count < len(p.controllers) {
		// reduce worker
		for _, controller := range p.controllers[count:] {
			controller.Cancel()
			<-controller.Done
			atomic.AddInt64(&p.count, -1)
		}
		p.controllers = p.controllers[:count]
	}
	return nil
}

func (p *pool) Stop() {
	atomic.StoreInt64(&p.scaling, 1)
	atomic.StoreInt64(&p.running, 0)
	p.Lock()
	for _, controller := range p.controllers {
		controller.Cancel()
		<-controller.Done
	}
	p.controllers = []Controller{}
	p.Unlock()
	close(p.sync_queue)
	close(p.async_queue)
	close(p.callback)
	p.wg.Wait()
}

func (p *pool) Count() int {
	return int(atomic.LoadInt64(&p.count))
}

func (p *pool) Start() {
	atomic.StoreInt64(&p.scaling, 1)
	p.Lock()
	defer func() {
		atomic.StoreInt64(&p.scaling, 0)
		p.Unlock()
	}()

	p.sync_queue = make(chan Task, 1)
	p.async_queue = make(chan Task, p.count)
	p.callback = make(chan Result)

	for i := 0; i < int(p.count); i++ {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{}, 1)
		p.controllers = append(p.controllers, Controller{
			Cancel: cancel,
			Done:   done,
		})
		go p.do(ctx, i, done)
		p.wg.Add(1)
	}
	go p.Callback()
	atomic.StoreInt64(&p.running, 1)
}

func (p *pool) do(ctx context.Context, index int, done chan struct{}) {
	defer func() {
		close(done)
		p.wg.Done()
	}()
	for {
		var (
			task   Task
			result Result
		)
		select {
		case <-ctx.Done():
			return
		case task = <-p.sync_queue:
		default:
			select {
			case <-ctx.Done():
				return
			case task = <-p.sync_queue:
			case task = <-p.async_queue:
			}
		}
		select {
		case <-ctx.Done():
			if atomic.LoadInt64(&p.running) == 1 {
				if task.Async {
					p.async_queue <- task
				} else {
					p.sync_queue <- task
				}
			}
			return
		case <-task.Context.Done():
			result.Error = ErrContextCanel
		default:
		}
		result.Context = task.Context
		result.Value, result.Error = p.handler.Do(task.Context, task.Value, task.Async)
		if task.Async {
			result.Callback = task.Callback
			p.callback <- result
		} else {
			task.Result <- result
		}
	}
}
