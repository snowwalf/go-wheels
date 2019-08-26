package cache

import (
	"bytes"
	"io"
	"io/ioutil"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type upstream struct {
	buffer []byte
}

func NewUpStream(size int64) *upstream {
	rand.Seed(time.Now().UnixNano())
	buf := make([]byte, size)
	rand.Read(buf)
	return &upstream{
		buffer: buf,
	}
}

func (up *upstream) UpStream(start, end int64) (io.ReadCloser, int64, error) {
	if end == 0 {
		return ioutil.NopCloser(bytes.NewReader(up.buffer[start:])), int64(len(up.buffer)), nil
	}
	return ioutil.NopCloser(bytes.NewReader(up.buffer[start:end])), int64(len(up.buffer)), nil
}

func (up *upstream) Buffer() []byte { return up.buffer }

func TestBlock(t *testing.T) {
	var blockCapacity int64 = 1024 * 1024
	size := 4*blockCapacity + 1024
	up := NewUpStream(size)

	t.Run("read from 0-", func(t *testing.T) {
		buf := NewBuffer(blockCapacity)
		buf.Reset(size)
		reader := buf.ReadSeekCloser(up.UpStream)
		defer reader.Close()
		b1 := make([]byte, size)
		n, err := reader.Read(b1)
		assert.Nil(t, err)
		assert.Equal(t, size, int64(n))
		assert.Equal(t, up.Buffer(), b1)
	})

	t.Run("reader 0-1024 and 1023*10234-1025*1024", func(t *testing.T) {
		buf := NewBuffer(blockCapacity)
		buf.Reset(size)
		reader := buf.ReadSeekCloser(up.UpStream)
		b1 := make([]byte, 1024)
		n, err := reader.Read(b1)
		assert.NoError(t, err)
		assert.Equal(t, 1024, n)
		assert.Equal(t, up.Buffer()[0:1024], b1)
		reader.Close()

		reader = buf.ReadSeekCloser(up.UpStream)
		n1, err := reader.Seek(int64(1023*1024), io.SeekStart)
		assert.NoError(t, err)
		assert.Equal(t, int64(1023*1024), n1)
		b2 := make([]byte, 2*1024)
		n, err = reader.Read(b2)
		assert.NoError(t, err)
		assert.Equal(t, 2*1024, n)
		assert.Equal(t, up.Buffer()[1023*1024:1025*1024], b2)
		reader.Close()
	})

	t.Run("reader 0-1024, (2*1024*1024)-(2*1024*1024+1024), 0-", func(t *testing.T) {
		buf := NewBuffer(blockCapacity)
		buf.Reset(size)
		reader := buf.ReadSeekCloser(up.UpStream)
		b1 := make([]byte, 1024)
		n, err := reader.Read(b1)
		assert.NoError(t, err)
		assert.Equal(t, 1024, n)
		assert.Equal(t, up.Buffer()[0:1024], b1)
		reader.Close()

		reader = buf.ReadSeekCloser(up.UpStream)
		n1, err := reader.Seek(int64(2*1024*1024), io.SeekStart)
		assert.NoError(t, err)
		assert.Equal(t, int64(2*1024*1024), n1)
		b2 := make([]byte, 1024)
		n, err = reader.Read(b2)
		assert.NoError(t, err)
		assert.Equal(t, 1024, n)
		assert.Equal(t, up.Buffer()[2*1024*1024:2*1024*1024+1024], b2)
		reader.Close()

		reader = buf.ReadSeekCloser(up.UpStream)
		b3 := make([]byte, size)
		n, err = reader.Read(b3)
		assert.NoError(t, err)
		assert.Equal(t, size, int64(n))
		assert.Equal(t, up.Buffer(), b3)
		reader.Close()
	})

}
