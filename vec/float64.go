// Copyright (c) 2019 KIDTSUNAMI
// Author: alex@kidtsunami.com

package vec

import (
	"sort"
)

func MatchFloat64Equal(src []float64, val float64, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchFloat64Equal(src, val, bits.Bytes())
	return bits
}

func MatchFloat64NotEqual(src []float64, val float64, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchFloat64NotEqual(src, val, bits.Bytes())
	return bits
}

func MatchFloat64LessThan(src []float64, val float64, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchFloat64LessThan(src, val, bits.Bytes())
	return bits
}

func MatchFloat64LessThanEqual(src []float64, val float64, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchFloat64LessThanEqual(src, val, bits.Bytes())
	return bits
}

func MatchFloat64GreaterThan(src []float64, val float64, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchFloat64GreaterThan(src, val, bits.Bytes())
	return bits
}

func MatchFloat64GreaterThanEqual(src []float64, val float64, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchFloat64GreaterThanEqual(src, val, bits.Bytes())
	return bits
}

func MatchFloat64Between(src []float64, a, b float64, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchFloat64Between(src, a, b, bits.Bytes())
	return bits
}

type Float64Slice []float64

func (s Float64Slice) Sort() Float64Slice {
	Float64Sorter(s).Sort()
	return s
}

func (s Float64Slice) Less(i, j int) bool { return s[i] < s[j] }
func (s Float64Slice) Len() int           { return len(s) }
func (s Float64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func (s Float64Slice) Contains(val float64) bool {
	if len(s) == 0 {
		return false
	}

	if s[0] > val || s[len(s)-1] < val {
		return false
	}

	i := sort.Search(len(s), func(i int) bool { return s[i] >= val })
	if i < len(s) && s[i] == val {
		return true
	}

	return false
}

func (s Float64Slice) Index(val float64, last int) int {
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

func (s Float64Slice) MinMax() (float64, float64) {
	var min, max float64

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

func (s Float64Slice) ContainsRange(from, to float64) bool {
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

func (s Float64Slice) MatchEqual(val float64, bits *BitSet) *BitSet {
	return MatchFloat64Equal(s, val, bits)
}
