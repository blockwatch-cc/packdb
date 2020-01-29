// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package vec

import (
	"sort"
	"strings"
)

func MatchStringsEqual(src []string, val string, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchStringsEqualGeneric(src, val, bits.Bytes())
	return bits
}

func MatchStringsNotEqual(src []string, val string, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchStringsNotEqualGeneric(src, val, bits.Bytes())
	return bits
}

func MatchStringsLessThan(src []string, val string, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchStringsLessThanGeneric(src, val, bits.Bytes())
	return bits
}

func MatchStringsLessThanEqual(src []string, val string, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchStringsLessThanEqualGeneric(src, val, bits.Bytes())
	return bits
}

func MatchStringsGreaterThan(src []string, val string, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchStringsGreaterThanGeneric(src, val, bits.Bytes())
	return bits
}

func MatchStringsGreaterThanEqual(src []string, val string, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchStringsGreaterThanEqualGeneric(src, val, bits.Bytes())
	return bits
}

func MatchStringsBetween(src []string, a, b string, bits *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchStringsBetweenGeneric(src, a, b, bits.Bytes())
	return bits
}

type StringSlice []string

func (s StringSlice) Sort() StringSlice {
	sort.Slice(s, func(i, j int) bool { return strings.Compare(s[i], s[j]) < 0 })
	return s
}

func (s StringSlice) Less(i, j int) bool { return strings.Compare(s[i], s[j]) < 0 }
func (s StringSlice) Len() int           { return len(s) }
func (s StringSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func (s StringSlice) Contains(val string) bool {
	if len(s) == 0 {
		return false
	}

	if strings.Compare(s[0], val) > 0 {
		return false
	}
	if strings.Compare(s[len(s)-1], val) < 0 {
		return false
	}
	i := sort.Search(len(s), func(i int) bool { return strings.Compare(s[i], val) >= 0 })
	if i < len(s) && strings.Compare(s[i], val) == 0 {
		return true
	}

	return false
}

func (s StringSlice) Index(val string, last int) int {
	if len(s) <= last {
		return -1
	}

	slice := s[last:]
	l := len(slice)
	min, max := slice[0], slice[l-1]
	if strings.Compare(min, val) > 0 {
		return -1
	}
	if strings.Compare(max, val) < 0 {
		return -1
	}

	idx := sort.Search(l, func(i int) bool { return strings.Compare(s[i], val) >= 0 })
	if idx < l && strings.Compare(s[idx], val) == 0 {
		return idx + last
	}
	return -1
}

func (s StringSlice) MinMax() (string, string) {
	var min, max string

	switch l := len(s); l {
	case 0:
	case 1:
		min, max = s[0], s[0]
	default:
		if strings.Compare(s[0], s[1]) > 0 {
			max = s[0]
			min = s[1]
		} else {
			max = s[1]
			min = s[0]
		}

		for i := 2; i < l; i++ {
			if strings.Compare(s[i], max) > 0 {
				max = s[i]
			} else if strings.Compare(s[i], min) < 0 {
				min = s[i]
			}
		}
	}

	return min, max
}

func (s StringSlice) ContainsRange(from, to string) bool {
	n := len(s)
	if n == 0 {
		return false
	}
	if v := strings.Compare(to, s[0]); v < 0 {
		return false
	} else if v == 0 {
		return true
	}
	if v := strings.Compare(from, s[n-1]); v > 0 {
		return false
	} else if v == 0 {
		return true
	}
	min := sort.Search(n, func(i int) bool {
		return strings.Compare(s[i], from) >= 0
	})
	if strings.Compare(s[min], from) == 0 {
		return true
	}
	max := sort.Search(n-min, func(i int) bool {
		return strings.Compare(s[i+min], to) >= 0
	})
	max = max + min
	if max < n && strings.Compare(s[max], to) == 0 {
		return true
	}
	return min < max
}
