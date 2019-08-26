package transport

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const (
	rangePrefix        = "bytes="
	contentRangePrefix = "bytes"
)

var (
	ErrInvalidRangeHeader        = errors.New("invalid range header")
	ErrInvalidContentRangeHeader = errors.New("invalid content range header")
)

type HTTPRange struct {
	// start-end: [start,end)
	// -end: [0, size-end)
	Start int64
	End   int64
}

type HTTPRanges []HTTPRange

func (ranges HTTPRanges) String() string {
	var rs []string
	for _, rg := range ranges {
		if rg.Start == 0 && rg.End < 0 {
			rs = append(rs, fmt.Sprintf("-%d", 0-rg.End))
		} else if rg.Start >= 0 && rg.End == 0 {
			rs = append(rs, fmt.Sprintf("%d-", rg.Start))
		} else {
			rs = append(rs, fmt.Sprintf("%d-%d", rg.Start, rg.End-1))
		}
	}
	return rangePrefix + strings.Join(rs, ",")
}

// Example:
//   "Range": "bytes=100-200"
//   "Range": "bytes=-50"
//   "Range": "bytes=150-"
//   "Range": "bytes=0-0,-1"
func ParseRange(s string) (ranges HTTPRanges, err error) {
	if s == "" {
		return nil, nil
	}
	if !strings.HasPrefix(s, rangePrefix) {
		return nil, ErrInvalidRangeHeader
	}
	s = strings.TrimPrefix(s, rangePrefix)
	for _, rg := range strings.Split(s, ",") {
		if rg = strings.TrimSpace(rg); rg == "" {
			continue
		}
		index := strings.Index(rg, "-")
		if index < 0 {
			return nil, ErrInvalidRangeHeader
		}
		start, end := strings.TrimSpace(rg[:index]), strings.TrimSpace(rg[(index+1):])
		if start == "" {
			end0, err := strconv.ParseInt(end, 10, 64)
			if err != nil {
				return nil, ErrInvalidRangeHeader
			}
			ranges = append(ranges, HTTPRange{End: 0 - end0})
			continue
		}
		var r HTTPRange
		r.Start, err = strconv.ParseInt(start, 10, 64)
		if err != nil || r.Start < 0 {
			return nil, ErrInvalidRangeHeader
		}
		if end == "" {
			r.End = 0
			ranges = append(ranges, r)
			continue
		}
		end0, err := strconv.ParseInt(end, 10, 64)
		if err != nil || r.Start > end0 {
			return nil, ErrInvalidRangeHeader
		}
		r.End = end0 + 1
		ranges = append(ranges, r)
	}
	return ranges, nil
}

// Content-Range: bytes 0-1023/146515
func ParseContentRange(s string) (*HTTPRange, int64, error) {
	if s == "" {
		return nil, 0, nil
	}
	if !strings.HasPrefix(s, contentRangePrefix) {
		return nil, 0, ErrInvalidContentRangeHeader
	}
	s = strings.TrimSpace(strings.TrimPrefix(s, contentRangePrefix))
	index := strings.Index(s, "/")
	if index < 0 {
		return nil, 0, ErrInvalidContentRangeHeader
	}
	rg, size0 := s[:index], s[index+1:]
	ranges := strings.Split(rg, "-")
	if len(ranges) != 2 {
		return nil, 0, ErrInvalidContentRangeHeader
	}
	var (
		ret  = &HTTPRange{}
		size int64
		err  error
	)
	if ret.Start, err = strconv.ParseInt(ranges[0], 10, 64); err != nil {
		return nil, 0, ErrInvalidContentRangeHeader
	}
	if ret.End, err = strconv.ParseInt(ranges[1], 10, 64); err != nil {
		return nil, 0, ErrInvalidContentRangeHeader
	}
	ret.End += 1
	if size, err = strconv.ParseInt(size0, 10, 64); err != nil {
		return nil, 0, ErrInvalidContentRangeHeader
	}
	return ret, size, nil
}
