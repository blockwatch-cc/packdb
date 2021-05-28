// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package vec

import (
	"sort"
)

type Uint64Sorter []uint64

func (s Uint64Sorter) Sort() {
	if !sort.IsSorted(s) {
		sort.Sort(s)
	}
}

func (s Uint64Sorter) Len() int           { return len(s) }
func (s Uint64Sorter) Less(i, j int) bool { return s[i] < s[j] }
func (s Uint64Sorter) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func UniqueUint64Slice(a []uint64) []uint64 {
	if len(a) == 0 {
		return a
	}
	b := make([]uint64, len(a))
	copy(b, a)
	Uint64Sorter(b).Sort()
	j := 0
	for i := 1; i < len(b); i++ {
		if b[j] == b[i] {
			continue
		}
		j++
		b[j] = b[i]
	}
	return b[:j+1]
}

func Uint64RemoveZeros(s []uint64) ([]uint64, int) {
	var n int
	for i, v := range s {
		if v == 0 {
			continue
		}
		s[n] = s[i]
		n++
	}
	s = s[:n]
	return s, n
}

func IntersectSortedUint64(x, y, out []uint64) []uint64 {
	if out == nil {
		out = make([]uint64, 0, min(len(x), len(y)))
	}
	count := 0
	for i, j, il, jl := 0, 0, len(x), len(y); i < il && j < jl; {
		if x[i] < y[j] {
			i++
			continue
		}
		if x[i] > y[j] {
			j++
			continue
		}
		if count > 0 {
			last := out[count-1]
			if last == x[i] {
				i++
				continue
			}
			if last == y[j] {
				j++
				continue
			}
		}
		if i == il || j == jl {
			break
		}
		if x[i] == y[j] {
			out = append(out, x[i])
			count++
			i++
			j++
		}
	}
	return out
}

type Uint8Sorter []uint8

func (s Uint8Sorter) Sort() []uint8 {
	if !sort.IsSorted(s) {
		sort.Sort(s)
	}
	return s
}

func (s Uint8Sorter) Len() int           { return len(s) }
func (s Uint8Sorter) Less(i, j int) bool { return s[i] < s[j] }
func (s Uint8Sorter) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func UniqueUint8Slice(a []uint8) []uint8 {
	if len(a) == 0 {
		return a
	}
	b := make([]uint8, len(a))
	copy(b, a)
	Uint8Sorter(b).Sort()
	j := 0
	for i := 1; i < len(b); i++ {
		if b[j] == b[i] {
			continue
		}
		j++
		b[j] = b[i]
	}
	return b[:j+1]
}
