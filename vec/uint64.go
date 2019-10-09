// Copyright (c) 2019 KIDTSUNAMI
// Author: alex@kidtsunami.com

package vec

import (
	"sort"
)

func MatchUint64Equal(src []uint64, val uint64, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchUint64Equal(src, val, bits.Bytes())
	return bits
}

func MatchUint64NotEqual(src []uint64, val uint64, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchUint64NotEqual(src, val, bits.Bytes())
	return bits
}

func MatchUint64LessThan(src []uint64, val uint64, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchUint64LessThan(src, val, bits.Bytes())
	return bits
}

func MatchUint64LessThanEqual(src []uint64, val uint64, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchUint64LessThanEqual(src, val, bits.Bytes())
	return bits
}

func MatchUint64GreaterThan(src []uint64, val uint64, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchUint64GreaterThan(src, val, bits.Bytes())
	return bits
}

func MatchUint64GreaterThanEqual(src []uint64, val uint64, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchUint64GreaterThanEqual(src, val, bits.Bytes())
	return bits
}

func MatchUint64Between(src []uint64, a, b uint64, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchUint64Between(src, a, b, bits.Bytes())
	return bits
}

type Uint64Slice []uint64

func (s Uint64Slice) Sort() Uint64Slice {
	Uint64Sorter(s).Sort()
	return s
}

func (s Uint64Slice) Less(i, j int) bool { return s[i] < s[j] }
func (s Uint64Slice) Len() int           { return len(s) }
func (s Uint64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func (s Uint64Slice) Unique() Uint64Slice {
	return UniqueUint64Slice(s)
}

func (s Uint64Slice) Contains(val uint64) bool {
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

func (s Uint64Slice) Index(val uint64, last int) int {
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

func (s Uint64Slice) MinMax() (uint64, uint64) {
	var min, max uint64

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

func (s Uint64Slice) ContainsRange(from, to uint64) bool {
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

func (s Uint64Slice) MatchEqual(val uint64, bits *BitSet) *BitSet {
	return MatchUint64Equal(s, val, bits)
}
