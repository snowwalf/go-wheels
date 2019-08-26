package transport

import (
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/snowwalf/go-wheels/lib/proxy-cache/cache"
)

const (
	headerRange         = "Range"
	headerContentRange  = "Content-Range"
	headerContentLength = "Content-Length"
)

type cacheTransport struct {
	http.RoundTripper
	cache.Cache
}

var _ http.RoundTripper = &cacheTransport{}

func NewCacheTransport(transport http.RoundTripper, c cache.Cache) *cacheTransport {
	return &cacheTransport{
		RoundTripper: transport,
		Cache:        c,
	}
}

func parseReq(req *http.Request) (key []byte, ranges HTTPRanges, err error) {
	// 根据url计算出key
	key = []byte(req.URL.String())

	ranges, err = ParseRange(req.Header.Get(headerRange))
	return
}

func (ct *cacheTransport) upStreamHandleFunc(req *http.Request, resp *http.Response) cache.UpStreamHandler {
	return cache.UpStreamHandler(func(start, end int64) (io.ReadCloser, int64, error) {
		transport := ct.RoundTripper
		if transport == nil {
			transport = http.DefaultTransport
		}
		ranges := HTTPRanges{HTTPRange{Start: start, End: end}}
		req.Header.Set(headerRange, ranges.String())
		resp0, err := transport.RoundTrip(req)

		if err != nil {
			return nil, 0, err
		}
		if resp != nil && resp0.StatusCode/100 != 2 {
			return nil, 0, fmt.Errorf("range get code: %d", resp0.StatusCode)
		}
		_, size, err := ParseContentRange(resp0.Header.Get(headerContentRange))
		if err != nil {
			return nil, size, err
		}
		for k, v := range resp0.Header {
			resp.Header[k] = v
		}
		return resp0.Body, size, nil
	})
}

func (ct *cacheTransport) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	transport := ct.RoundTripper
	if transport == nil {
		transport = http.DefaultTransport
	}
	if req.Method != http.MethodGet {
		return transport.RoundTrip(req)
	}
	key, ranges, err := parseReq(req)

	if err != nil {
		// Range Header错误，透传
		return transport.RoundTrip(req)
	}
	if len(ranges) > 1 {
		// TODO multipart/byteranges
		return transport.RoundTrip(req)
	}
	var (
		rc          io.ReadCloser
		size, total int64
	)
	resp = &http.Response{
		Request: req,
		Header:  make(http.Header),
	}
	switch len(ranges) {
	// 读整个文件
	case 0:
		rc, err = ct.Cache.Get(key, ct.upStreamHandleFunc(req, resp))
		resp.StatusCode = http.StatusOK
		resp.Status = "200 OK"
	// 单一范围range get
	case 1:
		rc, err = ct.Cache.RangeGet(key, ranges[0].Start, ranges[0].End, ct.upStreamHandleFunc(req, resp))
		resp.StatusCode = http.StatusPartialContent
		resp.Status = "206 Partial Content"
	default:
		// TODO multipart/byteranges
		return transport.RoundTrip(req)
	}
	if err != nil {
		return nil, err
	}
	total, _ = ct.Cache.Size(key)
	size = total
	if ranges != nil {
		if ranges[0].End == 0 {
			ranges[0].End = size
		}
		resp.Header.Set(headerContentRange, fmt.Sprintf("bytes %d-%d/%d", ranges[0].Start, ranges[0].End-1, total))
		size = ranges[0].End - ranges[0].Start
	}
	resp.Header.Set(headerContentLength, strconv.FormatInt(size, 10))
	resp.Body = rc
	return resp, nil
}
