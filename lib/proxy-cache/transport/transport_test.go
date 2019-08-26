package transport

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/snowwalf/go-wheels/lib/proxy-cache/cache"
	"github.com/stretchr/testify/assert"
)

var (
	testDir = "../../../test"
	txt     = "test.txt"
	gw      = httptest.NewServer(http.FileServer(http.Dir(testDir)))
)

func TestCacheTransport(t *testing.T) {
	ch := cache.NewMemoryCache(10, 5)
	transport := NewCacheTransport(nil, ch)

	cli := &http.Client{
		Transport: transport,
	}
	req, _ := http.NewRequest(http.MethodGet, gw.URL+"/"+txt, nil)
	ranges := HTTPRanges{
		HTTPRange{Start: 0, End: 10},
	}

	// Range: bytes=0-9
	req.Header.Set(headerRange, ranges.String())
	resp, err := cli.Do(req)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, http.StatusPartialContent, resp.StatusCode)
	assert.Equal(t, "206 Partial Content", resp.Status)
	assert.Equal(t, "10", resp.Header.Get(headerContentLength))
	rg, size, err := ParseContentRange(resp.Header.Get(headerContentRange))
	assert.NoError(t, err)
	assert.Equal(t, int64(20), size)
	assert.NotNil(t, rg)
	assert.Equal(t, int64(0), rg.Start)
	assert.Equal(t, int64(10), rg.End)
	buf, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	assert.NoError(t, err)
	assert.Equal(t, "0123456789", string(buf))

	// Range: bytes=0-5
	ranges[0].End = 6
	req.Header.Set(headerRange, ranges.String())
	resp, err = cli.Do(req)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, http.StatusPartialContent, resp.StatusCode)
	assert.Equal(t, "206 Partial Content", resp.Status)
	assert.Equal(t, "6", resp.Header.Get(headerContentLength))
	rg, size, err = ParseContentRange(resp.Header.Get(headerContentRange))
	assert.NoError(t, err)
	assert.Equal(t, int64(20), size)
	assert.NotNil(t, rg)
	assert.Equal(t, int64(0), rg.Start)
	assert.Equal(t, int64(6), rg.End)
	buf, err = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	assert.NoError(t, err)
	assert.Equal(t, "012345", string(buf))

	// Range: bytes=12-14
	ranges[0].Start, ranges[0].End = 12, 15
	req.Header.Set(headerRange, ranges.String())
	resp, err = cli.Do(req)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, http.StatusPartialContent, resp.StatusCode)
	assert.Equal(t, "206 Partial Content", resp.Status)
	assert.Equal(t, "3", resp.Header.Get(headerContentLength))
	rg, size, err = ParseContentRange(resp.Header.Get(headerContentRange))
	assert.NoError(t, err)
	assert.Equal(t, int64(20), size)
	assert.NotNil(t, rg)
	assert.Equal(t, int64(12), rg.Start)
	assert.Equal(t, int64(15), rg.End)
	buf, err = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	assert.NoError(t, err)
	assert.Equal(t, "234", string(buf))

	// Range: bytes=5-
	ranges[0].Start, ranges[0].End = 5, 0
	req.Header.Set(headerRange, ranges.String())
	resp, err = cli.Do(req)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, http.StatusPartialContent, resp.StatusCode)
	assert.Equal(t, "206 Partial Content", resp.Status)
	assert.Equal(t, "15", resp.Header.Get(headerContentLength))
	rg, size, err = ParseContentRange(resp.Header.Get(headerContentRange))
	assert.NoError(t, err)
	assert.Equal(t, int64(20), size)
	assert.NotNil(t, rg)
	assert.Equal(t, int64(5), rg.Start)
	assert.Equal(t, int64(20), rg.End)
	buf, err = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	assert.NoError(t, err)
	assert.Equal(t, "567890123456789", string(buf))

	// cache get/cache range get
	rc, err := ch.RangeGet([]byte(req.URL.String()), 0, 16, nil)
	assert.NoError(t, err)
	buf, err = ioutil.ReadAll(rc)
	rc.Close()
	assert.NoError(t, err)
	assert.Equal(t, "0123456789012345", string(buf))

	// Get
	req.Header.Del(headerRange)
	resp, err = cli.Do(req)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "20", resp.Header.Get(headerContentLength))
	buf, err = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	assert.NoError(t, err)
	assert.Equal(t, "01234567890123456789", string(buf))

	// cache get
	rc, err = ch.Get([]byte(req.URL.String()), nil)
	assert.NoError(t, err)
	buf, err = ioutil.ReadAll(rc)
	rc.Close()
	assert.NoError(t, err)
	assert.Equal(t, "01234567890123456789", string(buf))
}
