// Copyright (c) 2018-2019 KIDTSUNAMI
// Author: alex@kidtsunami.com
//
// Original from: InfluxData, MIT
// https://github.com/influxdata/influxdb
package compress

import (
	"encoding/binary"
	"fmt"
	"io"
	"unsafe"

	"blockwatch.cc/packdb/util"
)

const (
	stringUncompressed = 0
)

var (
	errStringBatchDecodeInvalidStringLength = fmt.Errorf("pack: StringArrayDecodeAll invalid encoded string length")
	errStringBatchDecodeLengthOverflow      = fmt.Errorf("pack: StringArrayDecodeAll length overflow")
	errStringBatchDecodeShortBuffer         = fmt.Errorf("pack: StringArrayDecodeAll short buffer")
)

func StringArrayEncodedSize(src []string) int {
	var sz int
	for _, v := range src {
		l := len(v)
		sz += l + uvarIntLen(l)
	}
	return sz + 1
}

func StringArrayEncodeAll(src []string, w io.Writer) (string, string, error) {
	w.Write([]byte{bytesUncompressed << 4})
	if len(src) == 0 {
		return "", "", nil
	}

	var buf [binary.MaxVarintLen64]byte
	min := src[0]
	max := src[0]
	for i := range src {
		l := binary.PutUvarint(buf[:], uint64(len(src[i])))
		w.Write(buf[:l])
		w.Write([]byte(src[i]))
		min = util.MinString(min, src[i])
		max = util.MaxString(max, src[i])
	}

	return min, max, nil
}

func StringArrayDecodeAll(b []byte, dst []string) ([]string, error) {
	if len(b) == 0 {
		return []string{}, nil
	}
	b = b[1:]
	var i, l int
	sz := cap(dst)
	if sz == 0 {
		sz = DefaultMaxPointsPerBlock
		dst = make([]string, sz)
	} else {
		dst = dst[:sz]
	}
	j := 0

	for i < len(b) {
		length, n := binary.Uvarint(b[i:])
		if n <= 0 {
			return []string{}, errStringBatchDecodeInvalidStringLength
		}
		l = int(length) + n
		lower := i + n
		upper := lower + int(length)
		if upper < lower {
			return []string{}, errStringBatchDecodeLengthOverflow
		}
		if upper > len(b) {
			return []string{}, errStringBatchDecodeShortBuffer
		}
		s := b[lower:upper]
		val := *(*string)(unsafe.Pointer(&s))
		if j < len(dst) {
			dst[j] = val
		} else {
			dst = append(dst, val)
			dst = dst[:cap(dst)]
		}
		i += l
		j++
	}

	return dst[:j], nil
}
