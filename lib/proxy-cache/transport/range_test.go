package transport

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRange(t *testing.T) {
	var ParseRangeTests = []struct {
		s   string
		r   HTTPRanges
		err error
	}{
		{"", nil, nil},
		{"bytes=", nil, nil},
		{"bytes=         ", nil, nil},
		{"bytes= , , ,   ", nil, nil},
		{"foo", nil, ErrInvalidRangeHeader},
		{"bytes=7", nil, ErrInvalidRangeHeader},
		{"bytes= 7 ", nil, ErrInvalidRangeHeader},
		{"bytes=5-4", nil, ErrInvalidRangeHeader},
		{"bytes=0-2,5-4", nil, ErrInvalidRangeHeader},
		{"bytes=2-5,4-3", nil, ErrInvalidRangeHeader},
		{"bytes=--5,4--3", nil, ErrInvalidRangeHeader},
		{"bytes=A-", nil, ErrInvalidRangeHeader},
		{"bytes=A- ", nil, ErrInvalidRangeHeader},
		{"bytes=A-Z", nil, ErrInvalidRangeHeader},
		{"bytes= -Z", nil, ErrInvalidRangeHeader},
		{"bytes=5-Z", nil, ErrInvalidRangeHeader},
		{"bytes=Ran-dom, garbage", nil, ErrInvalidRangeHeader},
		{"bytes=0x01-0x02", nil, ErrInvalidRangeHeader},

		{"bytes=0-9", []HTTPRange{{0, 10}}, nil},
		{"bytes=0-", []HTTPRange{{0, 0}}, nil},
		{"bytes=5-", []HTTPRange{{5, 0}}, nil},
		{"bytes=0-20", []HTTPRange{{0, 21}}, nil},
		{"bytes=1-2,5-", []HTTPRange{{1, 3}, {5, 0}}, nil},
		{"bytes=-2 , 7-", []HTTPRange{{0, -2}, {7, 0}}, nil},
		{"bytes=0-0 ,2-2, 7-", []HTTPRange{{0, 1}, {2, 3}, {7, 0}}, nil},
		{"bytes=-5", []HTTPRange{{0, -5}}, nil},
		{"bytes=0-499", []HTTPRange{{0, 500}}, nil},
		{"bytes=500-999", []HTTPRange{{500, 1000}}, nil},
		{"bytes=9500-", []HTTPRange{{9500, 0}}, nil},
		{"bytes=15-,0-5", []HTTPRange{{15, 0}, {0, 6}}, nil},
		{"bytes=0-0,-1", []HTTPRange{{0, 1}, {0, -1}}, nil},
		{"bytes=500-600,601-999", []HTTPRange{{500, 601}, {601, 1000}}, nil},
		{"bytes=500-700,601-999", []HTTPRange{{500, 701}, {601, 1000}}, nil},
		// Match Apache laxity:
		{"bytes=   1 -2   ,  4- 5, 7 - 8 , ,,", []HTTPRange{{1, 3}, {4, 6}, {7, 9}}, nil},
	}

	for _, test := range ParseRangeTests {
		t.Run("Range: "+test.s, func(tt *testing.T) {
			r := test.r
			ranges, err := ParseRange(test.s)
			assert.Equal(tt, test.err, err)
			assert.Equal(tt, r, ranges)
			assert.Equal(tt, len(ranges), len(r))
			for i := range r {
				assert.Equal(tt, ranges[i].Start, r[i].Start)
				assert.Equal(tt, ranges[i].End, r[i].End)
			}
		})
	}
}
