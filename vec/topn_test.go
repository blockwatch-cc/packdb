// Copyright (c) 2018 KIDTSUNAMI
// Author: alex@kidtsunami.com
//
// go test -bench=. -benchmem

package vec

import (
	"math/rand"
	"testing"
)

const n = 100000

func generateFloatHeap(n int) *TopFloat64Heap {
	h := NewTopFloat64Heap(n)
	for i := 0; i < n; i++ {
		h.Add(rand.Float64() * 21000000 / 2)
	}
	return h
}

func BenchmarkFloatHeapInsert1k(B *testing.B) {
	h := generateFloatHeap(1000)
	for i := 0; i < B.N; i++ {
		h.Add(rand.Float64() * 21000000)
	}
}

func BenchmarkFloatHeapInsert10k(B *testing.B) {
	h := generateFloatHeap(10000)
	for i := 0; i < B.N; i++ {
		h.Add(rand.Float64() * 21000000)
	}
}

func BenchmarkFloatHeapInsert100k(B *testing.B) {
	h := generateFloatHeap(100000)
	for i := 0; i < B.N; i++ {
		h.Add(rand.Float64() * 21000000)
	}
}

func BenchmarkFloatHeapTop1k(B *testing.B) {
	for i := 0; i < B.N; i++ {
		B.StopTimer()
		h := generateFloatHeap(1000)
		B.StartTimer()
		_ = h.TopN(1000)
	}
}

func BenchmarkFloatHeapTop10k(B *testing.B) {
	for i := 0; i < B.N; i++ {
		B.StopTimer()
		h := generateFloatHeap(10000)
		B.StartTimer()
		_ = h.TopN(10000)
	}
}

func BenchmarkFloatHeapTop100k(B *testing.B) {
	for i := 0; i < B.N; i++ {
		B.StopTimer()
		h := generateFloatHeap(100000)
		B.StartTimer()
		_ = h.TopN(100000)
	}
}

func BenchmarkFloatHeapSum1k(B *testing.B) {
	for i := 0; i < B.N; i++ {
		B.StopTimer()
		h := generateFloatHeap(1000)
		B.StartTimer()
		_ = h.SumN(1000)
	}
}

func BenchmarkFloatHeapSum10k(B *testing.B) {
	for i := 0; i < B.N; i++ {
		B.StopTimer()
		h := generateFloatHeap(10000)
		B.StartTimer()
		_ = h.SumN(10000)
	}
}

func BenchmarkFloatHeapSum100k(B *testing.B) {
	for i := 0; i < B.N; i++ {
		B.StopTimer()
		h := generateFloatHeap(100000)
		B.StartTimer()
		_ = h.SumN(100000)
	}
}

func generateUint64Heap(n int) *TopUint64Heap {
	h := NewTopUint64Heap(n)
	for i := 0; i < n; i++ {
		h.Add(uint64(rand.Int63n(21000000 / 2)))
	}
	return h
}

func BenchmarkUint64HeapInsert1k(B *testing.B) {
	h := generateUint64Heap(1000)
	for i := 0; i < B.N; i++ {
		h.Add(uint64(rand.Int63n(21000000)))
	}
}

func BenchmarkUint64HeapInsert10k(B *testing.B) {
	h := generateUint64Heap(10000)
	for i := 0; i < B.N; i++ {
		h.Add(uint64(rand.Int63n(21000000)))
	}
}

func BenchmarkUint64HeapInsert100k(B *testing.B) {
	h := generateUint64Heap(100000)
	for i := 0; i < B.N; i++ {
		h.Add(uint64(rand.Int63n(21000000)))
	}
}

func BenchmarkUint64HeapTop1k(B *testing.B) {
	for i := 0; i < B.N; i++ {
		B.StopTimer()
		h := generateUint64Heap(1000)
		B.StartTimer()
		_ = h.TopN(1000)
	}
}

func BenchmarkUint64HeapTop10k(B *testing.B) {
	for i := 0; i < B.N; i++ {
		B.StopTimer()
		h := generateUint64Heap(10000)
		B.StartTimer()
		_ = h.TopN(10000)
	}
}

func BenchmarkUint64HeapTop100k(B *testing.B) {
	for i := 0; i < B.N; i++ {
		B.StopTimer()
		h := generateUint64Heap(100000)
		B.StartTimer()
		_ = h.TopN(100000)
	}
}

func BenchmarkUint64HeapSum1k(B *testing.B) {
	for i := 0; i < B.N; i++ {
		B.StopTimer()
		h := generateUint64Heap(1000)
		B.StartTimer()
		_ = h.SumN(1000)
	}
}

func BenchmarkUint64HeapSum10k(B *testing.B) {
	for i := 0; i < B.N; i++ {
		B.StopTimer()
		h := generateUint64Heap(10000)
		B.StartTimer()
		_ = h.SumN(10000)
	}
}

func BenchmarkUint64HeapSum100k(B *testing.B) {
	for i := 0; i < B.N; i++ {
		B.StopTimer()
		h := generateUint64Heap(100000)
		B.StartTimer()
		_ = h.SumN(100000)
	}
}
