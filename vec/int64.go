// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package vec

import (
	"sort"
)

func MatchInt64Equal(src []int64, val int64, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchInt64Equal(src, val, bits.Bytes())
	return bits
}

func MatchInt64NotEqual(src []int64, val int64, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchInt64NotEqual(src, val, bits.Bytes())
	return bits
}

func MatchInt64LessThan(src []int64, val int64, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchInt64LessThan(src, val, bits.Bytes())
	return bits
}

func MatchInt64LessThanEqual(src []int64, val int64, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchInt64LessThanEqual(src, val, bits.Bytes())
	return bits
}

func MatchInt64GreaterThan(src []int64, val int64, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchInt64GreaterThan(src, val, bits.Bytes())
	return bits
}

func MatchInt64GreaterThanEqual(src []int64, val int64, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchInt64GreaterThanEqual(src, val, bits.Bytes())
	return bits
}

func MatchInt64Between(src []int64, a, b int64, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchInt64Between(src, a, b, bits.Bytes())
	return bits
}

type Int64Slice []int64

func (s Int64Slice) Sort() Int64Slice {
	Int64Sorter(s).Sort()
	return s
}

func (s Int64Slice) Less(i, j int) bool { return s[i] < s[j] }
func (s Int64Slice) Len() int           { return len(s) }
func (s Int64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func (s Int64Slice) Contains(val int64) bool {
	if len(s) == 0 {
		return false
	}

	if s[0] > val || s[len(s)-1] < val {
		return false
	}

	if ofs := int(val - s[0]); ofs >= 0 && ofs < len(s) && s[ofs] == val {
		return true
	}

	i := sort.Search(len(s), func(i int) bool { return s[i] >= val })
	if i < len(s) && s[i] == val {
		return true
	}

	return false
}

func (s Int64Slice) Index(val int64, last int) int {
	if len(s) <= last {
		return -1
	}

	slice := s[last:]
	l := len(slice)
	min, max := slice[0], slice[l-1]
	if val < min || val > max {
		return -1
	}

	if l == int(max-min)+1 {
		return int(val-min) + last
	}

	idx := sort.Search(l, func(i int) bool { return slice[i] >= val })
	if idx < l && slice[idx] == val {
		return idx + last
	}
	return -1
}

func (s Int64Slice) MinMax() (int64, int64) {
	var min, max int64

	switch l := len(s); l {
	case 0:
	case 1:
		min, max = s[0], s[0]
	default:
		if s[0] > s[1] {
			max = s[0]
			min = s[1]
		} else {
			max = s[1]
			min = s[0]
		}

		for i := 2; i < l; i++ {
			if s[i] > max {
				max = s[i]
			} else if s[i] < min {
				min = s[i]
			}
		}
	}

	return min, max
}

func (s Int64Slice) ContainsRange(from, to int64) bool {
	n := len(s)
	if n == 0 {
		return false
	}
	if to < s[0] {
		return false
	}
	if to == s[0] {
		return true
	}
	if from > s[n-1] {
		return false
	}
	if from == s[n-1] {
		return true
	}
	min := sort.Search(n, func(i int) bool {
		return s[i] >= from
	})
	if s[min] == from {
		return true
	}
	max := sort.Search(n-min, func(i int) bool {
		return s[i+min] >= to
	})
	max = max + min

	if max < n && s[max] == to {
		return true
	}

	return min < max
}

func (s Int64Slice) MatchEqual(val int64, bits *BitSet) *BitSet {
	return MatchInt64Equal(s, val, bits)
}
