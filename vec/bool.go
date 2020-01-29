// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package vec

import (
	"sort"
)

func MatchBoolEqual(src []bool, val bool, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchBoolEqualGeneric(src, val, bits.Bytes())
	return bits
}

func MatchBoolNotEqual(src []bool, val bool, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchBoolNotEqualGeneric(src, val, bits.Bytes())
	return bits
}

func MatchBoolLessThan(src []bool, val bool, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchBoolLessThanGeneric(src, val, bits.Bytes())
	return bits
}

func MatchBoolLessThanEqual(src []bool, val bool, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchBoolLessThanEqualGeneric(src, val, bits.Bytes())
	return bits
}

func MatchBoolGreaterThan(src []bool, val bool, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchBoolGreaterThanGeneric(src, val, bits.Bytes())
	return bits
}

func MatchBoolGreaterThanEqual(src []bool, val bool, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchBoolGreaterThanEqualGeneric(src, val, bits.Bytes())
	return bits
}

func MatchBoolBetween(src []bool, a, b bool, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchBoolBetweenGeneric(src, a, b, bits.Bytes())
	return bits
}

type BoolSlice []bool

func (s BoolSlice) Sort() BoolSlice {
	sort.Slice(s, func(i, j int) bool { return !s[i] && s[j] })
	return s
}

func (s BoolSlice) Less(i, j int) bool { return !s[i] && s[j] }
func (s BoolSlice) Len() int           { return len(s) }
func (s BoolSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func (s BoolSlice) Contains(val bool) bool {
	// empty s cannot contain values
	if len(s) == 0 {
		return false
	}

	if s[0] == val || s[len(s)-1] == val {
		return true
	}

	return false
}

func (s BoolSlice) Index(val bool, last int) int {
	if len(s) <= last {
		return -1
	}

	slice := s[last:]
	l := len(slice)
	min, max := slice[0], slice[l-1]
	if min && !val {
		return -1
	}
	if !min && !max && val {
		return -1
	}

	idx := sort.Search(l, func(i int) bool { return s[i] })
	if idx < l && s[idx] == val {
		return idx + last
	}
	return -1
}

func (s BoolSlice) MinMax() (bool, bool) {
	var min, max bool

	switch l := len(s); l {
	case 0:
	case 1:
		min, max = s[0], s[0]
	default:
		min, max = s[0], s[0]
		for i := 1; i < l; i++ {
			if min != max {
				break
			}
			min = min && s[i]
			max = max || s[i]
		}
	}

	return min, max
}

func (s BoolSlice) ContainsRange(from, to bool) bool {
	n := len(s)
	if n == 0 {
		return false
	}
	if !to && s[0] {
		return false
	}
	if from && !s[n-1] {
		return false
	}
	return true
}
