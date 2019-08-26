package cache

import (
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"testing"
	"time"
)

func BenchmarkGetAndSet(b *testing.B) {
	rand.Seed(time.Now().UnixNano())
	cache := NewMemoryCache(200, 1024*1024)
	tokens := make(chan struct{}, 10)
	for i := 0; i < 10; i++ {
		tokens <- struct{}{}
	}
	b.RunParallel(func(pb *testing.PB) {
		magic := make([]byte, 4)
		var index uint32 = 1
		for pb.Next() {
			up := NewUpStream(1024 * 1024)
			buf := up.Buffer()
			<-tokens
			size := 512*1024 + rand.Int63n(512*1024) // 512k-1024k
			key := md5.Sum(buf[:size])
			binary.LittleEndian.PutUint32(buf[:4], index)
			reader, err := cache.Get(key[:], up.UpStream)
			if err != nil {
				panic(err)
			}
			len1, err := reader.Read(magic[:4])
			if len1 != 4 || err != nil {
				panic(err)
			}
			if binary.LittleEndian.Uint32(magic) != index {
				panic(fmt.Errorf("bad magic: %d, %d", index, binary.LittleEndian.Uint32(magic)))
			}
			len2, err := io.CopyN(ioutil.Discard, reader, size-4)
			if err != nil || len2 != size-4 {
				fmt.Println(len2, size-4)
				panic(err)
			}
			reader.Close()
			index++
			tokens <- struct{}{}
		}
	})
}

func BenchmarkRangeGet(b *testing.B) {
	rand.Seed(time.Now().UnixNano())
	cache := NewMemoryCache(100, 1024*1024)
	tokens := make(chan struct{}, 10)
	for i := 0; i < 10; i++ {
		tokens <- struct{}{}
	}
	b.RunParallel(func(pb *testing.PB) {
		var index uint32 = 1
		for pb.Next() {
			up := NewUpStream(1024 * 1024)
			buf := up.Buffer()
			<-tokens
			magic := make([]byte, 4)
			size := 512*1024 + rand.Int63n(512*1024) // 512k-1024k
			key := md5.Sum(buf[:size])
			binary.LittleEndian.PutUint32(buf[:4], index)
			binary.LittleEndian.PutUint32(buf[512*1024-4:512*1024], index)
			// fisrt part
			offset := 256*1024 + rand.Int63n(256*1024-4) // [256k:512k-4)
			reader, err := cache.RangeGet(key[:], 0, offset, up.UpStream)
			if err != nil {
				panic(err)
			}
			len1, err := reader.Read(magic)
			if err != nil || len1 != 4 {
				panic(err)
			}
			if binary.LittleEndian.Uint32(magic) != index {
				panic(errors.New("invalid magic"))
			}
			len2, err := io.CopyN(ioutil.Discard, reader, offset-4)
			if err != nil || len2 != offset-4 {
				panic(err)
			}
			reader.Close()

			// second part
			reader, err = cache.RangeGet(key[:], offset, size, up.UpStream)
			if err != nil {
				panic(err)
			}
			len3, err := io.CopyN(ioutil.Discard, reader, 512*1024-4-offset)
			if err != nil || len3 != 512*1024-4-offset {
				panic(err)
			}
			len1, err = reader.Read(magic)
			if err != nil || len1 != 4 {
				panic(err)
			}
			if binary.LittleEndian.Uint32(magic) != index {
				panic(errors.New("invalid magic"))
			}
			len2, err = io.CopyN(ioutil.Discard, reader, size-512*1024)
			if err != nil || len2 != size-512*1024 {
				panic(err)
			}
			reader.Close()
			index++
			tokens <- struct{}{}
		}
	})
}
