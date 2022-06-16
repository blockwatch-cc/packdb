// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build go1.7 && amd64 && !gccgo && !appengine
// +build go1.7,amd64,!gccgo,!appengine

package vec

import (
// "fmt"
)

//go:noescape
func bitsetAndAVX2(dst, src []byte)

//go:noescape
func bitsetAndNotAVX2(dst, src []byte)

//go:noescape
func bitsetOrAVX2(dst, src []byte)

//go:noescape
func bitsetXorAVX2(dst, src []byte)

//go:noescape
func bitsetNegAVX2(src []byte)

//go:noescape
func bitsetPopCountAVX2(src []byte) int64

//go:noescape
func bitsetNextOneBitAVX2(src []byte, index uint64) uint64

//go:noescape
func bitsetNextZeroBitAVX2(src []byte, index uint64) uint64

func bitsetAnd(dst, src []byte, size int) {
	switch {
	case useAVX2:
		bitsetAndAVX2(dst, src)
		dst[len(dst)-1] &= bitmask(size)
	default:
		bitsetAndGeneric(dst, src, size)
	}
}

func bitsetAndNot(dst, src []byte, size int) {
	switch {
	case useAVX2:
		bitsetAndNotAVX2(dst, src)
		dst[len(dst)-1] &= bitmask(size)
	default:
		bitsetAndNotGeneric(dst, src, size)
	}
}

func bitsetOr(dst, src []byte, size int) {
	switch {
	case useAVX2:
		bitsetOrAVX2(dst, src)
		dst[len(dst)-1] &= bitmask(size)
	default:
		bitsetOrGeneric(dst, src, size)
	}
}

func bitsetXor(dst, src []byte, size int) {
	switch {
	case useAVX2:
		bitsetXorAVX2(dst, src)
		dst[len(dst)-1] &= bitmask(size)
	default:
		bitsetXorGeneric(dst, src, size)
	}
}

func bitsetNeg(src []byte, size int) {
	switch {
	case useAVX2:
		bitsetNegAVX2(src)
		src[len(src)-1] &= bitmask(size)
	default:
		bitsetNegGeneric(src, size)
	}
}

// no AVX2 algorithmen yet
func bitsetReverse(src []byte) {
	switch {
	case useAVX2:
		bitsetReverseGeneric(src)
	default:
		bitsetReverseGeneric(src)
	}
}

func bitsetPopCount(src []byte, size int) int64 {
	switch {
	case useAVX2:
		switch true {
		case size == 0:
			return 0
		case size <= 8:
			return int64(bitsetLookup[src[0]&bitmask(size)])
		case size&0x7 == 0:
			return bitsetPopCountAVX2(src)
		default:
			cnt := bitsetPopCountAVX2(src[:len(src)-1])
			return cnt + int64(bitsetLookup[src[len(src)-1]&bitmask(size)])
		}
	default:
		return bitsetPopCountGeneric(src, size)
	}

}

func bitsetRun(src []byte, index, size int) (int, int) {
	switch {
	case useAVX2:
		return bitsetRunAVX2Wrapper(src, index, size)
	default:
		return bitsetRunGeneric(src, index, size)
	}
}

func bitsetRunAVX2Wrapper(src []byte, index, size int) (int, int) {
	if len(src) == 0 || index < 0 || index >= size {
		return -1, 0
	}
	var (
		start  int = -1
		length int
	)
	i := index >> 3

	offset := index & 0x7
	mask := byte(0xff) >> uint(offset)
	first := src[i] & mask
	if first > 0 {
		start = index - offset + bitsetLeadingZeros[first]
		length = -bitsetLeadingZeros[first]
	} else {
		i++
		i = int(bitsetNextOneBitAVX2(src, uint64(i)))
		if i == len(src) {
			return -1, 0
		}
		start = i<<3 + bitsetLeadingZeros[src[i]]
		length = -bitsetLeadingZeros[src[i]]
	}

	if pos := bitsetLeadingZeros[(^src[i])&(byte(0xff)>>uint((start&0x7)+1))]; pos < 8 {
		length += pos
		return start, length
	}

	i++
	length += 8
	j := int(bitsetNextZeroBitAVX2(src, uint64(i)))
	length += 8 * (j - i)
	i = j
	if i == len(src) {
		i--
	}

	if src[i] != 0xff {
		length += bitsetLeadingZeros[^src[i]]
		if start+length > size {
			length = size - start
		}
	}

	return start, length
}
