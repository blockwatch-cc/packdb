// Copyright (c) 2018-2019 KIDTSUNAMI
// Author: alex@kidtsunami.com

package vec

import (
	"blockwatch.cc/packdb/util"
	"math"
)

type IntegerReducer struct {
	n    int
	sum  int64
	min  int64
	max  int64
	mean float64 // mean aggregator
	m2   float64 // variance aggregator
}

func (b IntegerReducer) Len() int {
	return b.n
}

func (b IntegerReducer) Sum() int64 {
	return b.sum
}

func (b IntegerReducer) Min() int64 {
	return b.min
}

func (b IntegerReducer) Max() int64 {
	return b.max
}

func (b *IntegerReducer) Add(val int64) {
	if b.n == 0 {
		b.min = val
		b.max = val
	} else {
		b.min = util.Min64(b.min, val)
		b.max = util.Max64(b.max, val)
	}
	b.sum += val
	b.n++
	// summarize means and squared distances from mean using
	// Welford's Online algorithm, see
	// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
	delta := float64(val) - b.mean
	b.mean += delta / float64(b.n)
	b.m2 += delta * (float64(val) - b.mean)
}

func (b *IntegerReducer) AddN(val ...int64) {
	b.AddSlice(val)
}

func (b *IntegerReducer) AddSlice(val []int64) {
	for _, v := range val {
		b.Add(v)
	}
}

// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
func (b IntegerReducer) Stddev() float64 {
	v := b.Var()
	if math.IsNaN(v) {
		return v
	}
	return math.Sqrt(v)
}

func (b IntegerReducer) Var() float64 {
	if b.n < 2 {
		return math.NaN()
	}
	return b.m2 / float64(b.n-1)
}

func (b IntegerReducer) Mean() float64 {
	return b.mean
}

type UnsignedReducer struct {
	n    int
	sum  uint64
	min  uint64
	max  uint64
	mean float64 // mean aggregator
	m2   float64 // variance aggregator
}

func (b UnsignedReducer) Len() int {
	return b.n
}

func (b UnsignedReducer) Sum() uint64 {
	return b.sum
}

func (b UnsignedReducer) Min() uint64 {
	return b.min
}

func (b UnsignedReducer) Max() uint64 {
	return b.max
}

func (b *UnsignedReducer) Add(val uint64) {
	if b.n == 0 {
		b.min = val
		b.max = val
	} else {
		b.min = util.MinU64(b.min, val)
		b.max = util.MaxU64(b.max, val)
	}
	b.sum += val
	b.n++
	// summarize means and squared distances from mean using
	// Welford's Online algorithm, see
	// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
	delta := float64(val) - b.mean
	b.mean += delta / float64(b.n)
	b.m2 += delta * (float64(val) - b.mean)
}

func (b *UnsignedReducer) AddN(val ...uint64) {
	b.AddSlice(val)
}

func (b *UnsignedReducer) AddSlice(val []uint64) {
	for _, v := range val {
		b.Add(v)
	}
}

// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
func (b UnsignedReducer) Stddev() float64 {
	v := b.Var()
	if math.IsNaN(v) {
		return v
	}
	return math.Sqrt(v)
}

func (b UnsignedReducer) Var() float64 {
	if b.n < 2 {
		return math.NaN()
	}
	return b.m2 / float64(b.n-1)
}

func (b UnsignedReducer) Mean() float64 {
	return b.mean
}

type FloatReducer struct {
	n    int
	sum  float64
	min  float64
	max  float64
	mean float64 // mean aggregator
	m2   float64 // variance aggregator
}

func (b FloatReducer) Len() int {
	return b.n
}

func (b FloatReducer) Sum() float64 {
	return b.sum
}

func (b FloatReducer) Min() float64 {
	return b.min
}

func (b FloatReducer) Max() float64 {
	return b.max
}

func (b *FloatReducer) Add(val float64) {
	if b.n == 0 {
		b.min = val
		b.max = val
	} else {
		b.min = util.MinFloat64(b.min, val)
		b.max = util.MaxFloat64(b.max, val)
	}
	b.sum += val
	b.n++
	// summarize means and squared distances from mean using
	// Welford's Online algorithm, see
	// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
	delta := val - b.mean
	b.mean += delta / float64(b.n)
	b.m2 += delta * (val - b.mean)
}

func (b *FloatReducer) AddN(val ...float64) {
	for _, v := range val {
		b.Add(v)
	}
}

func (b *FloatReducer) AddSlice(val []float64) {
	for _, v := range val {
		b.Add(v)
	}
}

// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
func (b FloatReducer) Stddev() float64 {
	v := b.Var()
	if math.IsNaN(v) {
		return v
	}
	return math.Sqrt(v)
}

func (b FloatReducer) Var() float64 {
	if b.n < 2 {
		return math.NaN()
	}
	return b.m2 / float64(b.n-1)
}

func (b FloatReducer) Mean() float64 {
	return b.mean
}

type WindowIntegerReducer struct {
	IntegerReducer
	values   []int64
	isSorted bool
}

func NewWindowIntegerReducer(size int) *WindowIntegerReducer {
	return &WindowIntegerReducer{
		values:   make([]int64, 0, size),
		isSorted: true,
	}
}

func (b *WindowIntegerReducer) UseSlice(val []int64) {
	b.values = make([]int64, len(val))
	copy(b.values, val)
	b.isSorted = false
	for _, v := range b.values {
		b.IntegerReducer.Add(v)
	}
}

func (b *WindowIntegerReducer) Add(val int64) {
	b.isSorted = b.isSorted && val >= b.Max()
	b.IntegerReducer.Add(val)
	b.values = append(b.values, val)
}

func (b *WindowIntegerReducer) AddN(val ...int64) {
	b.AddSlice(val)
}

func (b *WindowIntegerReducer) AddSorted(val int64) {
	b.IntegerReducer.Add(val)
	b.values = append(b.values, val)
}

func (b *WindowIntegerReducer) AddSortedN(val ...int64) {
	for _, v := range val {
		b.IntegerReducer.Add(v)
	}
	b.values = append(b.values, val...)
}

func (b *WindowIntegerReducer) AddSortedSlice(val []int64) {
	for _, v := range val {
		b.IntegerReducer.Add(v)
	}
	b.values = append(b.values, val...)
}

func (b *WindowIntegerReducer) Median() float64 {
	l := len(b.values)
	if l == 0 {
		return 0
	}
	if !b.isSorted {
		Int64Sorter(b.values).Sort()
		b.isSorted = true
	}
	if l%2 == 0 {
		lo, hi := b.values[l/2-1], b.values[(l/2)]
		return float64(lo) + float64((hi-lo)/2)
	} else {
		return float64(b.values[l/2])
	}
}

type WindowUnsignedReducer struct {
	UnsignedReducer
	values   []uint64
	isSorted bool
}

func NewWindowUnsignedReducer(size int) *WindowUnsignedReducer {
	return &WindowUnsignedReducer{
		values:   make([]uint64, 0, size),
		isSorted: true,
	}
}

func (b *WindowUnsignedReducer) UseSlice(val []uint64) {
	b.values = make([]uint64, len(val))
	copy(b.values, val)
	b.isSorted = false
	for _, v := range b.values {
		b.UnsignedReducer.Add(v)
	}
}

func (b *WindowUnsignedReducer) Add(val uint64) {
	b.isSorted = b.isSorted && val >= b.Max()
	b.UnsignedReducer.Add(val)
	b.values = append(b.values, val)
}

func (b *WindowUnsignedReducer) AddN(val ...uint64) {
	b.AddSlice(val)
}

func (b *WindowUnsignedReducer) AddSorted(val uint64) {
	b.UnsignedReducer.Add(val)
	b.values = append(b.values, val)
}

func (b *WindowUnsignedReducer) AddSortedN(val ...uint64) {
	b.AddSortedSlice(val)
}

func (b *WindowUnsignedReducer) AddSortedSlice(val []uint64) {
	for _, v := range val {
		b.UnsignedReducer.Add(v)
	}
	b.values = append(b.values, val...)
}

func (b *WindowUnsignedReducer) Median() float64 {
	l := len(b.values)
	if l == 0 {
		return 0
	}
	if !b.isSorted {
		Uint64Sorter(b.values).Sort()
		b.isSorted = true
	}
	if l%2 == 0 {
		lo, hi := b.values[l/2-1], b.values[(l/2)]
		return float64(lo) + float64((hi-lo)/2)
	} else {
		return float64(b.values[l/2])
	}
}

type WindowFloatReducer struct {
	FloatReducer
	values   []float64
	isSorted bool
}

func NewWindowFloatReducer(size int) *WindowFloatReducer {
	return &WindowFloatReducer{
		values:   make([]float64, 0, size),
		isSorted: true,
	}
}

func (b *WindowFloatReducer) UseSlice(val []float64) {
	b.values = make([]float64, len(val))
	copy(b.values, val)
	b.isSorted = false
	for _, v := range b.values {
		b.FloatReducer.Add(v)
	}
}

func (b *WindowFloatReducer) Add(val float64) {
	b.isSorted = b.isSorted && val >= b.Max()
	b.FloatReducer.Add(val)
	b.values = append(b.values, val)
}

func (b *WindowFloatReducer) AddN(val ...float64) {
	b.AddSlice(val)
}

func (b *WindowFloatReducer) AddSorted(val float64) {
	b.FloatReducer.Add(val)
	b.values = append(b.values, val)
}

func (b *WindowFloatReducer) AddSortedN(val ...float64) {
	for _, v := range val {
		b.FloatReducer.Add(v)
	}
	b.values = append(b.values, val...)
}

func (b *WindowFloatReducer) AddSortedSlice(val []float64) {
	for _, v := range val {
		b.FloatReducer.Add(v)
	}
	b.values = append(b.values, val...)
}

func (b *WindowFloatReducer) Median() float64 {
	l := len(b.values)
	if l == 0 {
		return 0
	}
	if !b.isSorted {
		Float64Sorter(b.values).Sort()
		b.isSorted = true
	}
	if l%2 == 0 {
		lo, hi := b.values[l/2-1], b.values[(l/2)]
		return lo + (hi-lo)/2
	} else {
		return b.values[l/2]
	}
}
