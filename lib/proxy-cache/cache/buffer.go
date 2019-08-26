package cache

import (
	"errors"
	"io"
	"io/ioutil"
	"sync"
	"sync/atomic"

	"github.com/snowwalf/go-wheels/lib/math"
)

const (
	maxConflictRetry = 3
	MinRead          = 512
)

///////////////////////////////////////////////////////////////////////////////////////////////////////
var pools sync.Map

func QueryPool(capacity int64) *sync.Pool {
	pool, _ := pools.LoadOrStore(capacity, &sync.Pool{
		New: func() interface{} {
			return &Block{
				data: make([]byte, capacity),
			}
		},
	})
	return pool.(*sync.Pool)
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
type Block struct {
	data     []byte
	index    int64
	size     int64
	offset   int64
	capacity int64
}

func NewBlock(index, size, blockCapacity int64) *Block {
	b := QueryPool(blockCapacity).Get().(*Block)
	b.index = index
	b.size = size
	b.offset = 0
	b.capacity = blockCapacity
	b.data = b.data[:0]
	return b
}

func (b *Block) Offset() int64 { return atomic.LoadInt64(&b.offset) }

func (b *Block) AbsOffset() int64 { return b.index*b.capacity + b.Offset() }

func (b *Block) Remain() int64 { return b.size - atomic.LoadInt64(&b.offset) }

func (b *Block) BlockReadWriteSeeker() *BlockReadWriteSeeker {
	return &BlockReadWriteSeeker{
		Block: b,
	}
}

type BlockReadWriteSeeker struct {
	*Block
	seek     int64
	upstream io.ReadCloser
}

func (b *BlockReadWriteSeeker) Read(p []byte) (n int, err error) {
	offset := atomic.LoadInt64(&b.offset)
	if offset <= b.seek {
		if len(p) == 0 {
			return 0, nil
		}
		return 0, io.EOF
	}
	n = copy(p, b.data[b.seek:offset])
	b.seek += int64(n)
	return n, nil
}

func (b *BlockReadWriteSeeker) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	size := atomic.LoadInt64(&b.size)
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = b.seek + offset
	default:
		return 0, ErrInvalidSeekWhence
	}
	if abs < 0 || abs > size {
		return 0, ErrNagativeSeekOffset
	}
	b.seek = abs
	return abs, nil
}

func (b *BlockReadWriteSeeker) Close() error {
	if b.upstream != nil {
		if offset, size := atomic.LoadInt64(&b.offset), atomic.LoadInt64(&b.size); offset < size {
			_, _ = io.CopyN(b, b.upstream, size-offset)
		}
		b.upstream.Close()
	}
	return nil
}

func (b *BlockReadWriteSeeker) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	from, to := b.seek, b.seek+int64(len(p))
	i := maxConflictRetry
	for ; i > 0; i-- {
		offset := atomic.LoadInt64(&b.offset)
		if to <= offset {
			return len(p), nil
		}
		size := atomic.LoadInt64((&b.size))
		if from > offset && size < to {
			return 0, ErrWritePosition
		}
		if !atomic.CompareAndSwapInt64(&b.offset, offset, to) {
			continue
		}
		copy(b.data[offset:to], p[offset-from:])
		break
	}
	if i == 0 {
		return 0, ErrDataConflict
	}
	b.seek += int64(len(p))
	return len(p), nil
}

func (b *BlockReadWriteSeeker) ReadFrom(r io.Reader) (n int64, err error) {
	offset := atomic.LoadInt64(&b.offset)
	if b.seek < offset {
		n, err = io.CopyN(ioutil.Discard, r, offset-b.seek)
		if err == io.EOF {
			return 0, nil
		}
		if err != nil {
			return n, err
		}
		b.seek += n
	}
	buf := make([]byte, MinRead)
	for {
		m, err0 := r.Read(buf)
		if m < 0 {
			panic(errors.New("nagative read"))
		}
		if _, err1 := b.Write(buf[:m]); err != nil {
			return 0, err1
		}
		n += int64(m)
		if err0 == io.EOF {
			return n, nil
		}
		if err0 != nil {
			return n, err0
		}
	}
}

func (b *BlockReadWriteSeeker) UpStream(up io.ReadCloser) { b.upstream = up }

///////////////////////////////////////////////////////////////////////////////////////////////////////
// 内存缓存链表

var (
	ErrNagativeSeekOffset = errors.New("buffer.seek: nagative offset")
	ErrInvalidSeekWhence  = errors.New("buffer.seek: invalid whence")
	ErrWritePosition      = errors.New(("buffer write: invalid position"))
)

type UpStreamHandler func(start, end int64) (io.ReadCloser, int64, error)

type Buffer struct {
	sync.RWMutex
	size          int64
	blocks        []*Block
	blockCapacity int64
}

func NewBuffer(blockCapacity int64) *Buffer {
	return &Buffer{
		blockCapacity: blockCapacity,
	}
}

func (b *Buffer) Reset(size int64) {
	for _, block := range b.blocks {
		QueryPool(b.blockCapacity).Put(block)
	}
	atomic.StoreInt64(&b.size, size)
	b.blocks = make([]*Block, (size+b.blockCapacity-1)/b.blockCapacity)
	var index int64
	for {
		if size <= b.blockCapacity {
			b.blocks[index] = NewBlock(index, size, b.blockCapacity)
			break
		}
		b.blocks[index] = NewBlock(index, b.blockCapacity, b.blockCapacity)
		index++
		size -= b.blockCapacity
	}
}

func (b *Buffer) ReadSeekCloser(up UpStreamHandler) ReadSeekCloser {
	return &readSeekCloser{
		Buffer:          b,
		upstreamHandler: up,
	}
}

func (b *Buffer) ReadSeekCloserWithUpstream(up UpStreamHandler, stream io.ReadCloser) ReadSeekCloser {
	return &readSeekCloser{
		Buffer:          b,
		upstreamHandler: up,
		upstream:        stream,
	}
}

func (b *Buffer) Size() int64 {
	return atomic.LoadInt64(&b.size)
}

// read-seeker
var _ ReadSeekCloser = &readSeekCloser{}

type ReadSeekCloser interface {
	io.Reader
	io.Seeker
	io.Closer
}

type readSeekCloser struct {
	*Buffer
	writer          *BlockReadWriteSeeker
	seek            int64
	upstreamHandler UpStreamHandler
	upstream        io.ReadCloser
}

func (b *readSeekCloser) Read(p []byte) (n int, err error) {
	for {
		if len(p) == 0 {
			return n, nil
		}
		if b.seek >= b.size {
			return n, io.EOF
		}
		block := b.blocks[b.seek/b.blockCapacity]
		offset := b.seek % b.blockCapacity
		reader := block.BlockReadWriteSeeker()
		if _, err := reader.Seek(offset, io.SeekStart); err != nil {
			return 0, err
		}
		s, e := reader.Read(p)
		if e != nil && e != io.EOF {
			return n, e
		}
		n += s
		p = p[s:]
		b.seek += int64(s)
		if e == io.EOF {
			if b.upstream == nil {
				if b.upstreamHandler == nil {
					return n, e
				}
				if b.upstream, _, err = b.upstreamHandler(block.AbsOffset(), block.index*b.blockCapacity+block.size); err != nil {
					return n, err
				}
			}
			b.writer = block.BlockReadWriteSeeker()
			_, _ = b.writer.Seek(block.Offset(), io.SeekStart)
			if off := offset - block.Offset(); off > 0 {
				_, e0 := io.CopyN(b.writer, b.upstream, off)
				if err != nil {
					return n, e0
				}
			}
			remain := block.Remain()
			s, e = io.TeeReader(b.upstream, b.writer).Read(p[:math.MinInt(len(p), int(remain))])
			n += s
			p = p[s:]
			b.seek += int64(s)
			if e != nil && e != io.EOF {
				return n, e
			}
			if e == io.EOF && int64(s) < remain {
				return n, e
			}
			if int64(s) == remain {
				b.writer.UpStream(b.upstream)
				b.writer.Close()
				b.upstream = nil
			}
			if len(p) == 0 && block.Remain() > 0 && b.upstream != nil {
				b.writer.UpStream(b.upstream)
				b.writer.Close()
				b.upstream = nil
			}
		}
	}
}

func (b *readSeekCloser) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = b.seek + offset
	default:
		return 0, ErrInvalidSeekWhence
	}
	if abs < 0 || abs > b.Buffer.size {
		return 0, ErrNagativeSeekOffset
	}
	b.seek = abs
	return abs, nil
}

func (b *readSeekCloser) Close() error {
	if b.writer != nil {
		b.writer.UpStream(b.upstream)
		return b.writer.Close()
	}
	return nil
}

//////////////////////////////////////////////////////////////////////////////////////////////////
type wrapCloser struct {
	io.Reader
	io.Closer
}

func WrapCloser(r io.Reader, c io.Closer) io.ReadCloser {
	return wrapCloser{Reader: r, Closer: c}
}
