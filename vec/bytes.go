// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package vec

import (
	"bytes"
	"sort"
)

func MatchBytesEqual(src [][]byte, val []byte, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchBytesEqualGeneric(src, val, bits.Bytes())
	return bits
}

func MatchBytesNotEqual(src [][]byte, val []byte, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchBytesNotEqualGeneric(src, val, bits.Bytes())
	return bits
}

func MatchBytesLessThan(src [][]byte, val []byte, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchBytesLessThanGeneric(src, val, bits.Bytes())
	return bits
}

func MatchBytesLessThanEqual(src [][]byte, val []byte, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchBytesLessThanEqualGeneric(src, val, bits.Bytes())
	return bits
}

func MatchBytesGreaterThan(src [][]byte, val []byte, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchBytesGreaterThanGeneric(src, val, bits.Bytes())
	return bits
}

func MatchBytesGreaterThanEqual(src [][]byte, val []byte, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchBytesGreaterThanEqualGeneric(src, val, bits.Bytes())
	return bits
}

func MatchBytesBetween(src [][]byte, a, b []byte, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchBytesBetweenGeneric(src, a, b, bits.Bytes())
	return bits
}

type ByteSlice [][]byte

func (s ByteSlice) Sort() ByteSlice {
	sort.Slice(s, func(i, j int) bool { return bytes.Compare(s[i], s[j]) < 0 })
	return s
}

func (s ByteSlice) Less(i, j int) bool { return bytes.Compare(s[i], s[j]) < 0 }
func (s ByteSlice) Len() int           { return len(s) }
func (s ByteSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func (s *ByteSlice) AddUnique(val []byte) bool {
	idx := s.Index(val, 0)
	if idx > -1 {
		return false
	}
	*s = append(*s, val)
	s.Sort()
	return true
}

func (s *ByteSlice) Remove(val []byte) bool {
	idx := s.Index(val, 0)
	if idx < 0 {
		return false
	}
	*s = append((*s)[:idx], (*s)[idx+1:]...)
	return true
}

func (s ByteSlice) Contains(val []byte) bool {
	if len(s) == 0 {
		return false
	}

	if bytes.Compare(s[0], val) > 0 {
		return false
	}
	if bytes.Compare(s[len(s)-1], val) < 0 {
		return false
	}

	i := sort.Search(len(s), func(i int) bool { return bytes.Compare(s[i], val) >= 0 })
	if i < len(s) && bytes.Compare(s[i], val) == 0 {
		return true
	}

	return false
}

func (s ByteSlice) Index(val []byte, last int) int {
	if len(s) <= last {
		return -1
	}

	slice := s[last:]
	l := len(slice)
	min, max := slice[0], slice[l-1]
	if bytes.Compare(min, val) > 0 {
		return -1
	}
	if bytes.Compare(max, val) < 0 {
		return -1
	}

	idx := sort.Search(l, func(i int) bool { return bytes.Compare(s[i], val) >= 0 })
	if idx < l && bytes.Compare(s[idx], val) == 0 {
		return idx + last
	}
	return -1
}

func (s ByteSlice) MinMax() ([]byte, []byte) {
	var min, max []byte

	switch l := len(s); l {
	case 0:
	case 1:
		min, max = s[0], s[0]
	default:
		if bytes.Compare(s[0], s[1]) > 0 {
			max = s[0]
			min = s[1]
		} else {
			max = s[1]
			min = s[0]
		}

		for i := 2; i < l; i++ {
			if bytes.Compare(s[i], max) > 0 {
				max = s[i]
			} else if bytes.Compare(s[i], min) < 0 {
				min = s[i]
			}
		}
	}

	return min, max
}

func (s ByteSlice) ContainsRange(from, to []byte) bool {
	n := len(s)
	if n == 0 {
		return false
	}
	if v := bytes.Compare(to, s[0]); v < 0 {
		return false
	} else if v == 0 {
		return true
	}
	if v := bytes.Compare(from, s[n-1]); v > 0 {
		return false
	} else if v == 0 {
		return true
	}
	min := sort.Search(n, func(i int) bool {
		return bytes.Compare(s[i], from) >= 0
	})
	if bytes.Compare(s[min], from) == 0 {
		return true
	}
	max := sort.Search(n-min, func(i int) bool {
		return bytes.Compare(s[i+min], to) >= 0
	})
	max = max + min
	if max < n && bytes.Compare(s[max], to) == 0 {
		return true
	}
	return min < max
}
