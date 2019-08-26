package cache

import (
	"errors"
	"io"
	"io/ioutil"
	"strconv"
	"sync"
)

var (
	ErrNagativeRead = errors.New("reader returned negative count from Read")
	ErrWriteCache   = errors.New("write cache")
	ErrDataConflict = errors.New("data conflict")
	ErrFromOrTo     = errors.New("error from or to param")
	ErrSkipData     = errors.New("skip data")
)

type memoryCache struct {
	kvs           sync.Map
	ring          chan *Buffer
	blockCapacity int64
}

var _ Cache = &memoryCache{}

func NewMemoryCache(count int, blockCapacity int) *memoryCache {
	mem := &memoryCache{ring: make(chan *Buffer, count), blockCapacity: int64(blockCapacity)}

	for i := 0; i < count; i++ {
		buf := NewBuffer(int64(blockCapacity))
		mem.ring <- buf
		mem.kvs.Store(strconv.Itoa(i), buf)
	}
	return mem
}

func (mc *memoryCache) get(key []byte) (*Buffer, bool) {
	item, ok := mc.kvs.Load(string(key))
	if !ok {
		return nil, false
	}
	buf, ok := item.(*Buffer)
	return buf, ok
}

func (mc *memoryCache) create(key []byte, size int64) *Buffer {
	buf := <-mc.ring
	if buf == nil {
		buf = NewBuffer(mc.blockCapacity)
	}
	buf.Reset(size)
	mc.kvs.Store(string(key), buf)
	mc.ring <- buf
	return buf
}

func (mc *memoryCache) Get(key []byte, up UpStreamHandler) (rc io.ReadCloser, err error) {
	buf, ok := mc.get(key)
	if !ok {
		reader, size, err := up(0, 0)
		if err != nil {
			return nil, err
		}
		buf = mc.create(key, size)
		return buf.ReadSeekCloserWithUpstream(up, reader), nil
	}
	return buf.ReadSeekCloser(up), nil
}

func (mc *memoryCache) RangeGet(key []byte, from, to int64, up UpStreamHandler) (rc io.ReadCloser, err error) {
	buf, ok := mc.get(key)
	if !ok {
		from1 := from / mc.blockCapacity * mc.blockCapacity
		reader, size, err := up(from1, to)
		if err != nil {
			return nil, err
		}
		if to == 0 {
			to = size
		}
		buf = mc.create(key, size)
		r0 := buf.ReadSeekCloserWithUpstream(up, reader)
		_, _ = r0.Seek(from1, io.SeekStart)
		if _, err = io.CopyN(ioutil.Discard, r0, from-from1); err != nil {
			return nil, err
		}
		return WrapCloser(io.LimitReader(r0, to-from), reader), nil
	}
	reader := buf.ReadSeekCloser(up)
	_, err = reader.Seek(from, io.SeekStart)
	if err != nil {
		return nil, err
	}
	if to == 0 {
		to = buf.Size()
	}
	return WrapCloser(io.LimitReader(reader, to-from), reader), nil
}

func (mc *memoryCache) Exist(key []byte) bool {
	_, ok := mc.get(key)
	return ok
}

func (mc *memoryCache) Size(key []byte) (int64, error) {
	buf, ok := mc.get(key)
	if !ok {
		return 0, ErrNoExist
	}
	return buf.Size(), nil
}
