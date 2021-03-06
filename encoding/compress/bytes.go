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

	"blockwatch.cc/packdb/util"
)

const (
	bytesUncompressed = 0
)

var (
	errBytesBatchDecodeInvalidLength  = fmt.Errorf("pack: BytesArrayDecodeAll invalid encoded length")
	errBytesBatchDecodeLengthOverflow = fmt.Errorf("pack: BytesArrayDecodeAll length overflow")
	errBytesBatchDecodeShortBuffer    = fmt.Errorf("pack: BytesArrayDecodeAll short buffer")
)

func BytesArrayEncodedSize(src [][]byte) int {
	var sz int
	for _, v := range src {
		l := len(v)
		sz += l + uvarIntLen(l)
	}
	return sz + 1
}

func BytesArrayEncodeAll(src [][]byte, w io.Writer) ([]byte, []byte, error) {
	w.Write([]byte{bytesUncompressed << 4})
	if len(src) == 0 {
		return nil, nil, nil
	}

	var buf [binary.MaxVarintLen64]byte
	min := src[0]
	max := src[0]
	for i := range src {
		l := binary.PutUvarint(buf[:], uint64(len(src[i])))
		w.Write(buf[:l])
		w.Write(src[i])
		min = util.MinBytes(min, src[i])
		max = util.MaxBytes(max, src[i])
	}

	return min, max, nil
}

func BytesArrayDecodeAll(b []byte, dst [][]byte) ([][]byte, error) {
	if len(b) == 0 {
		return [][]byte{}, nil
	}
	b = b[1:]
	var i, l int

	sz := cap(dst)
	if sz == 0 {
		sz = DefaultMaxPointsPerBlock
		dst = make([][]byte, sz)
	} else {
		dst = dst[:sz]
	}

	j := 0

	for i < len(b) {
		length, n := binary.Uvarint(b[i:])
		if n <= 0 {
			return [][]byte{}, errBytesBatchDecodeInvalidLength
		}

		l = int(length) + n

		lower := i + n
		upper := lower + int(length)
		if upper < lower {
			return [][]byte{}, errBytesBatchDecodeLengthOverflow
		}
		if upper > len(b) {
			return [][]byte{}, errBytesBatchDecodeShortBuffer
		}

		val := b[lower:upper]
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
