package cache

import (
	"errors"
	"io"
)

var (
	ErrNoExist       = errors.New("file not exist")
	ErrNotEnoughData = errors.New("not enough data")
	ErrFileTooLarge  = errors.New("file size is too large")
)

type Cache interface {
	Exist(key []byte) bool
	Get(key []byte, up UpStreamHandler) (rc io.ReadCloser, err error)
	RangeGet(key []byte, from, to int64, up UpStreamHandler) (rc io.ReadCloser, err error)
	Size(key []byte) (int64, error)
}
