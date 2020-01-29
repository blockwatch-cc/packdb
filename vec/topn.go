// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package vec

import (
	"container/heap"
	"sort"
)

type TopFloat64Heap struct {
	vals     []float64
	calls    uint64
	sum      float64
	total    float64
	issorted bool
}

func (h TopFloat64Heap) Len() int           { return len(h.vals) }
func (h TopFloat64Heap) Less(i, j int) bool { return h.vals[i] < h.vals[j] }
func (h TopFloat64Heap) Swap(i, j int)      { h.vals[i], h.vals[j] = h.vals[j], h.vals[i] }

func (h *TopFloat64Heap) Push(x interface{}) {
	// Push and Pop modify the slice's length, not just its contents.
	h.vals = append(h.vals, x.(float64))
}

func (h *TopFloat64Heap) Pop() interface{} {
	old := h.vals
	n := len(old)
	x := old[n-1]
	h.vals = old[0 : n-1]
	return x
}

func NewTopFloat64Heap(size int) *TopFloat64Heap {
	h := &TopFloat64Heap{
		vals: make([]float64, 0, size),
	}
	heap.Init(h)
	return h
}

func (h *TopFloat64Heap) Add(val float64) {
	l, c := h.Len(), cap(h.vals)
	h.total += val
	h.calls++

	// restore heap invariant after sort
	if h.issorted {
		heap.Init(h)
	}

	// maybe add value
	if l < c || val > h.vals[0] {
		if l == c {
			removed := heap.Pop(h)
			h.sum -= removed.(float64)
		}
		heap.Push(h, val)
	}
}

func (h *TopFloat64Heap) Total() float64 {
	return h.total
}

func (h *TopFloat64Heap) Calls() uint64 {
	return h.calls
}

func (h *TopFloat64Heap) Sum() float64 {
	return h.sum
}

func (h *TopFloat64Heap) sorted() []float64 {
	if !h.issorted {
		sort.Slice(h.vals, func(i, j int) bool { return h.vals[i] > h.vals[j] })
		h.issorted = true
	}
	return h.vals
}

func (h *TopFloat64Heap) TopN(n int) []float64 {
	n = min(n, len(h.vals))
	return h.sorted()[:n]
}

func (h *TopFloat64Heap) SumN(n int) float64 {
	n = min(n, len(h.vals))
	var sum float64
	for _, v := range h.sorted()[:n] {
		sum += v
	}
	return sum
}

type TopUint64Heap struct {
	vals     []uint64
	calls    uint64
	sum      uint64
	total    uint64
	issorted bool
}

func (h TopUint64Heap) Len() int           { return len(h.vals) }
func (h TopUint64Heap) Less(i, j int) bool { return h.vals[i] < h.vals[j] }
func (h TopUint64Heap) Swap(i, j int)      { h.vals[i], h.vals[j] = h.vals[j], h.vals[i] }

func (h *TopUint64Heap) Push(x interface{}) {
	// Push and Pop modify the slice's length, not just its contents.
	h.vals = append(h.vals, x.(uint64))
}

func (h *TopUint64Heap) Pop() interface{} {
	old := h.vals
	n := len(old)
	x := old[n-1]
	h.vals = old[0 : n-1]
	return x
}

func NewTopUint64Heap(size int) *TopUint64Heap {
	h := &TopUint64Heap{
		vals: make([]uint64, 0, size),
	}
	heap.Init(h)
	return h
}

func (h *TopUint64Heap) Add(val uint64) {
	l, c := h.Len(), cap(h.vals)
	h.total += val
	h.calls++

	// restore heap invariant after sort
	if h.issorted {
		heap.Init(h)
	}

	// maybe add value
	if l < c || val > h.vals[0] {
		if l == c {
			removed := heap.Pop(h)
			h.sum -= removed.(uint64)
		}
		heap.Push(h, val)
	}
}

func (h *TopUint64Heap) Total() uint64 {
	return h.total
}

func (h *TopUint64Heap) Calls() uint64 {
	return h.calls
}

func (h *TopUint64Heap) Sum() uint64 {
	return h.sum
}

func (h *TopUint64Heap) sorted() []uint64 {
	if !h.issorted {
		sort.Slice(h.vals, func(i, j int) bool { return h.vals[i] > h.vals[j] })
		h.issorted = true
	}
	return h.vals
}

func (h *TopUint64Heap) TopN(n int) []uint64 {
	n = min(n, len(h.vals))
	return h.sorted()[:n]
}

func (h *TopUint64Heap) SumN(n int) uint64 {
	n = min(n, len(h.vals))
	var sum uint64
	for _, v := range h.sorted()[:n] {
		sum += v
	}
	return sum
}
