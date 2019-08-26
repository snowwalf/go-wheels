package workpool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPool(t *testing.T) {
	var index int64
	handler := func(ctx context.Context, job interface{}, async bool) (interface{}, error) {
		value := job.(int64)
		time.Sleep(time.Duration(value*50) * time.Millisecond)
		atomic.AddInt64(&index, 1)
		return value, nil
	}
	pool := NewPool(3, true, handler)
	pool.Start()

	// sync call
	var wg sync.WaitGroup
	for i := 1; i < 6; i++ {
		wg.Add(1)
		go func(index int64) {
			defer wg.Done()
			value, err := pool.Sync(context.Background(), index)
			assert.Nil(t, err)
			assert.Equal(t, index, value.(int64))
		}(int64(i))
	}
	wg.Wait()

	// async call
	index = 0
	callback := func(ctx context.Context, result interface{}, err error) {
		defer wg.Done()
		assert.Nil(t, err)
		assert.Equal(t, atomic.LoadInt64(&index), result.(int64))
	}
	for i := 1; i < 6; i++ {
		wg.Add(1)
		err := pool.Async(context.Background(), int64(i), callback)
		assert.Nil(t, err)
	}
	wg.Wait()

	// sync + async call: sync (1-5) + async(6-10)
	index = 0
	for i := 1; i < 11; i++ {
		if i > 5 {
			//async
			if i == 6 {
				// first async call sleep 10ms to ensure sync call reach
				time.Sleep(time.Duration(10) * time.Millisecond)
			}
			wg.Add(1)
			assert.Nil(t, pool.Async(context.Background(), int64(i), callback))
		} else {
			// sync
			wg.Add(1)
			go func(index int64) {
				defer wg.Done()
				value, err := pool.Sync(context.Background(), index)
				assert.Equal(t, index, value.(int64))
				assert.Nil(t, err)
			}(int64(i))
		}
	}
	wg.Wait()
}

func TestPoolScale(t *testing.T) {
	// worker=3, aysnc call 1, 2, 3(scale to 5),4,5,6(count=5?)
	var (
		pool Pool
		wg   sync.WaitGroup
	)
	var scaled int64
	handler := func(ctx context.Context, job interface{}, async bool) (interface{}, error) {
		value := job.(int)
		if atomic.LoadInt64(&scaled) != 1 {
			count := pool.Count()
			assert.Equal(t, 3, count)
		} else {
			assert.Equal(t, 5, pool.Count())
		}
		time.Sleep(time.Duration(value*20) * time.Millisecond)
		return value, nil
	}
	callback := func(ctx context.Context, result interface{}, err error) {
		wg.Done()
	}
	pool = NewPool(3, true, handler)
	pool.Start()
	for i := 1; i <= 6; i++ {
		wg.Add(1)
		assert.Nil(t, pool.Async(context.Background(), i, callback))
	}

	pool.Scale(5, false)
	atomic.StoreInt64(&scaled, 1)
	wg.Wait()
	pool.Stop()

	// worker=3, aysnc call 1,2,3(scale to 2),4,5(count=2?)
	atomic.StoreInt64(&scaled, 0)
	handler = func(ctx context.Context, job interface{}, async bool) (interface{}, error) {
		value := job.(int)
		if atomic.LoadInt64(&scaled) != 1 {
			count := pool.Count()
			assert.Equal(t, 3, count)
		} else {
			assert.Equal(t, 2, pool.Count())
		}

		time.Sleep(time.Duration(value*20) * time.Millisecond)
		return value, nil
	}
	pool = NewPool(3, true, handler)
	pool.Start()
	for i := 1; i <= 6; i++ {
		wg.Add(1)
		assert.Nil(t, pool.Async(context.Background(), i, callback))
	}
	pool.Scale(2, false)
	atomic.StoreInt64(&scaled, 1)
	wg.Wait()
}

func TestHandlerContextCancel(t *testing.T) {
	handler := func(ctx context.Context, job interface{}, async bool) (interface{}, error) {
		select {
		case <-ctx.Done():

			return nil, ErrContextCanel
		case <-time.NewTicker(50 * time.Millisecond).C:
			return job, nil
		}
	}
	pool := NewPool(1, true, handler)
	pool.Start()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(20 * time.Millisecond)
			cancel()
		}()
		_, err := pool.Sync(ctx, 1)
		assert.Equal(t, ErrContextCanel, err)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		result, err := pool.Sync(context.Background(), 1)
		assert.Nil(t, err)
		assert.Equal(t, 1, result.(int))
	}()
	wg.Wait()

	callback := func(ctx context.Context, result interface{}, err error) {
		defer wg.Done()
		if result == nil {
			assert.Equal(t, ErrContextCanel, err)
			return
		}
		assert.Nil(t, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(40 * time.Millisecond)
		cancel()
	}()
	wg.Add(1)
	assert.Nil(t, pool.Async(ctx, 1, callback))
	wg.Add(1)
	assert.Nil(t, pool.Async(context.Background(), 2, callback))
	wg.Wait()
}

func TestPoolScaleConflict(t *testing.T) {
	var wg sync.WaitGroup
	pool := NewPool(2, true, func(ctx context.Context, job interface{}, async bool) (interface{}, error) {
		time.Sleep(100 * time.Millisecond)
		wg.Done()
		return nil, nil
	})
	_, err := pool.Sync(context.Background(), 1)
	assert.Equal(t, ErrPoolIsNotRunning, err)
	assert.Equal(t, ErrPoolIsNotRunning, pool.Async(context.Background(), 1, nil))
	pool.Start()
	wg.Add(1)
	assert.Nil(t, pool.Async(context.Background(), 1, nil))
	wg.Add(1)
	assert.Nil(t, pool.Async(context.Background(), 2, nil))
	go func() {
		time.Sleep(50 * time.Millisecond)
		err := pool.Scale(2, false)
		assert.Equal(t, ErrScaleConflict, err)
	}()
	time.Sleep(20 * time.Millisecond)
	assert.Nil(t, pool.Scale(1, false))
	wg.Wait()
	wg.Add(1)
	assert.Nil(t, pool.Async(context.Background(), 1, nil))
	go func() {
		time.Sleep(50 * time.Millisecond)
		assert.Equal(t, ErrScaleConflict, pool.Scale(2, false))
	}()
	time.Sleep(30 * time.Millisecond)
	pool.Stop()
	wg.Wait()
	time.Sleep(100 * time.Millisecond)
}

func TestPoolAsyncFull(t *testing.T) {
	pool := NewPool(1, false, func(ctx context.Context, job interface{}, async bool) (interface{}, error) {
		time.Sleep(50 * time.Millisecond)
		return nil, nil
	})
	pool.Start()
	assert.Nil(t, pool.Async(context.Background(), 1, nil))
	time.Sleep(10 * time.Millisecond)
	assert.Nil(t, pool.Async(context.Background(), 1, nil))
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, ErrPoolFull, pool.Async(context.Background(), 1, nil))
	time.Sleep(100 * time.Millisecond)
}
