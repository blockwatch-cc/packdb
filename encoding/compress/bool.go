// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//
// Original from: InfluxData, MIT
// https://github.com/influxdata/influxdb
package compress

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	booleanUncompressed        = 0 // unused
	booleanCompressedBitPacked = 1
)

func BooleanArrayEncodedSize(src []bool) int {
	n := len(src)
	sz := n / 8
	if n&7 > 0 {
		sz++
	}
	return sz + 1
}

func BooleanArrayEncodeAll(src []bool, w io.Writer) (bool, bool, error) {
	w.Write([]byte{booleanCompressedBitPacked << 4})
	var b [binary.MaxVarintLen64]byte
	i := binary.PutUvarint(b[:], uint64(len(src)))
	w.Write(b[:i])
	b[0] = 0

	n := uint64(0)
	var min, max bool
	if len(src) > 0 {
		min = src[0]
		max = src[0]
	}
	for _, v := range src {
		min = min && v
		max = max || v
		if v {
			b[0] |= 128 >> (n & 7)
		} else {
			b[0] &^= 128 >> (n & 7)
		}
		n++
		if n&7 == 0 {
			w.Write(b[0:1])
			b[0] = 0
		}
	}

	if n&7 > 0 {
		w.Write(b[0:1])
	}
	return min, max, nil
}

func BooleanArrayDecodeAll(b []byte, dst []bool) ([]bool, error) {
	if len(b) == 0 {
		return nil, nil
	}

	b = b[1:]
	val, n := binary.Uvarint(b)
	if n <= 0 {
		return nil, fmt.Errorf("pack: BooleanBatchDecoder invalid count")
	}

	count := int(val)

	b = b[n:]
	if min := len(b) * 8; min < count {
		count = min
	}

	if cap(dst) < count {
		dst = make([]bool, count)
	} else {
		dst = dst[:count]
	}

	j := 0
	for _, v := range b {
		for i := byte(128); i > 0 && j < len(dst); i >>= 1 {
			dst[j] = v&i != 0
			j++
		}
	}
	return dst, nil
}
