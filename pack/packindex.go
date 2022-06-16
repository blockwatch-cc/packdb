// Copyright (c) 2018-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"encoding/json"

	"sort"
)

type PackIndex struct {
	packs   PackInfoList
	minpks  []uint64
	maxpks  []uint64
	removed []uint32
	pos     []int32
	pkidx   int
}

func (i *PackIndex) MarshalJSON() ([]byte, error) {
	type M struct {
		Packs   PackInfoList
		Minpks  []uint64
		Maxpks  []uint64
		Removed []uint32
		Pos     []int32
		Pkidx   int
	}
	m := &M{
		Packs:   i.packs,
		Minpks:  i.minpks,
		Maxpks:  i.maxpks,
		Removed: i.removed,
		Pos:     i.pos,
		Pkidx:   i.pkidx,
	}
	return json.Marshal(m)
}

func NewPackIndex(packs PackInfoList, pkidx int) *PackIndex {
	if packs == nil {
		packs = make(PackInfoList, 0)
	}
	l := &PackIndex{
		packs:   packs,
		minpks:  make([]uint64, len(packs), cap(packs)),
		maxpks:  make([]uint64, len(packs), cap(packs)),
		removed: make([]uint32, 0),
		pkidx:   pkidx,
		pos:     make([]int32, len(packs), cap(packs)),
	}
	sort.Sort(l.packs)
	for i := range l.packs {
		l.minpks[i] = l.packs[i].Blocks[l.pkidx].MinValue.(uint64)
		l.maxpks[i] = l.packs[i].Blocks[l.pkidx].MaxValue.(uint64)
		l.pos[i] = int32(i)
	}
	l.Sort()
	return l
}

func (l *PackIndex) Clear() {
	for _, v := range l.packs {
		l.removed = append(l.removed, v.Key)
	}
	l.packs = l.packs[:0]
	l.minpks = l.minpks[:0]
	l.maxpks = l.maxpks[:0]
	l.pos = l.pos[:0]
}

func (l PackIndex) NextKey() uint32 {
	if len(l.packs) == 0 {
		return 0
	}
	return l.packs[len(l.packs)-1].Key + 1
}

func (l *PackIndex) Len() int {
	return len(l.packs)
}

func (l *PackIndex) Count() int {
	var count int
	for i := range l.packs {
		count += l.packs[i].NValues
	}
	return count
}

func (l *PackIndex) HeapSize() int {
	sz := szPackIndex
	sz += len(l.minpks) * 8
	sz += len(l.maxpks) * 8
	sz += len(l.removed) * 8
	sz += len(l.pos) * 4
	for i := range l.packs {
		sz += l.packs[i].HeapSize()
	}
	return sz
}

func (l *PackIndex) TableSize() int {
	var sz int
	for i := range l.packs {
		sz += l.packs[i].Packsize
	}
	return sz
}

func (l *PackIndex) Sort() {
	sort.Slice(l.pos, func(i, j int) bool {
		posi, posj := l.pos[i], l.pos[j]
		mini, maxi := l.minpks[posi], l.maxpks[posi]
		minj, maxj := l.minpks[posj], l.maxpks[posj]
		return mini < minj || (mini == minj && maxi < maxj) || (mini == minj && maxi == maxj && i < j)
	})
}

func (l *PackIndex) MinMax(n int) (uint64, uint64) {
	if n >= l.Len() {
		return 0, 0
	}
	return l.minpks[n], l.maxpks[n]
}

func (l *PackIndex) GlobalMinMax() (uint64, uint64) {
	if l.Len() == 0 {
		return 0, 0
	}
	pos := l.pos[len(l.pos)-1]
	return l.minpks[pos], l.maxpks[pos]
}

func (l *PackIndex) MinMaxSlices() ([]uint64, []uint64) {
	return l.minpks, l.maxpks
}

func (l *PackIndex) Get(i int) PackInfo {
	if i < 0 || i >= l.Len() {
		return PackInfo{}
	}
	return l.packs[i]
}

func (l *PackIndex) GetSorted(i int) PackInfo {
	if i < 0 || i >= l.Len() {
		return PackInfo{}
	}
	return l.packs[l.pos[i]]
}

func (l *PackIndex) AddOrUpdate(head PackInfo) {
	head.dirty = true
	l.removed = uint32Remove(l.removed, head.Key)
	old, pos, isAdd := l.packs.Add(head)
	var needsort bool

	if isAdd {
		if pos > 0 && pos == l.Len()-1 {
			newmin := l.packs[pos].Blocks[l.pkidx].MinValue.(uint64)
			newmax := l.packs[pos].Blocks[l.pkidx].MaxValue.(uint64)
			lastmax := l.packs[l.pos[len(l.pos)-1]].Blocks[l.pkidx].MaxValue.(uint64)
			needsort = newmin < lastmax
			l.pos = append(l.pos, int32(pos))
			l.minpks = append(l.minpks, newmin)
			l.maxpks = append(l.maxpks, newmax)

		} else {
			if cap(l.pos) < len(l.packs) {
				l.pos = make([]int32, len(l.packs))
				l.minpks = make([]uint64, len(l.packs))
				l.maxpks = make([]uint64, len(l.packs))
			}
			l.pos = l.pos[:len(l.packs)]
			l.minpks = l.minpks[:len(l.packs)]
			l.maxpks = l.maxpks[:len(l.packs)]
			for i := range l.packs {
				l.minpks[i] = l.packs[i].Blocks[l.pkidx].MinValue.(uint64)
				l.maxpks[i] = l.packs[i].Blocks[l.pkidx].MaxValue.(uint64)
				l.pos[i] = int32(i)
			}
			needsort = true
		}
	} else {
		newmin := l.packs[pos].Blocks[l.pkidx].MinValue.(uint64)
		newmax := l.packs[pos].Blocks[l.pkidx].MaxValue.(uint64)
		l.minpks[pos] = newmin
		l.maxpks[pos] = newmax

		oldmin := old.Blocks[l.pkidx].MinValue.(uint64)
		oldmax := old.Blocks[l.pkidx].MaxValue.(uint64)
		needsort = oldmin != newmin || oldmax != newmax
	}
	if needsort {
		l.Sort()
	}
}

func (l *PackIndex) Remove(key uint32) {
	oldhead, pos := l.packs.RemoveKey(key)
	if pos < 0 {
		return
	}
	l.removed = uint32AddUnique(l.removed, key)

	if pos > 0 && pos == l.Len() {
		min := oldhead.Blocks[l.pkidx].MinValue.(uint64)
		i := sort.Search(l.Len(), func(i int) bool { return l.minpks[l.pos[i]] >= min })
		for i < l.Len() && l.pos[i] != int32(pos) && l.minpks[l.pos[i]] == min {
			i++
		}
		l.pos = append(l.pos[:i], l.pos[i+1:]...)
		l.minpks = append(l.minpks[:pos], l.minpks[pos+1:]...)
		l.maxpks = append(l.maxpks[:pos], l.maxpks[pos+1:]...)
	} else {
		l.minpks = append(l.minpks[:pos], l.minpks[pos+1:]...)
		l.maxpks = append(l.maxpks[:pos], l.maxpks[pos+1:]...)
		l.pos = l.pos[:len(l.packs)]
		for i := range l.packs {
			l.pos[i] = int32(i)
		}
		l.Sort()
	}
}

func (l *PackIndex) Best(val uint64) (pos int, packmin uint64, packmax uint64, nextmin uint64) {
	count := l.Len()
	if count == 0 {
		return
	}

	i := sort.Search(count, func(i int) bool { return l.minpks[l.pos[i]] > val })
	if i > 0 {
		i--
	}

	if i+1 < count {
		nextmin = l.minpks[l.pos[i+1]]
	}

	pos = int(l.pos[i])
	packmin, packmax = l.minpks[pos], l.maxpks[pos]
	return
}

func (l *PackIndex) Next(last int) (pos int, packmin uint64, packmax uint64, nextmin uint64) {
	next := last + 1
	count := l.Len()
	if next >= count {
		return
	}
	if next+1 < count {
		nextmin = l.minpks[l.pos[next+1]]
	}
	pos = int(l.pos[next])
	packmin, packmax = l.minpks[pos], l.maxpks[pos]
	return
}

func uint32Remove(s []uint32, val uint32) []uint32 {
	l := len(s)
	idx := sort.Search(l, func(i int) bool { return s[i] >= val })
	if idx < l && s[idx] == val {
		s = append(s[:idx], s[idx+1:]...)
	}
	return s
}

func uint32AddUnique(s []uint32, val uint32) []uint32 {
	l := len(s)
	idx := sort.Search(l, func(i int) bool { return s[i] >= val })
	if idx < l && s[idx] == val {
		return s
	}
	s = append(s, val)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s
}
