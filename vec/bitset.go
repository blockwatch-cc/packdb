// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package vec

import (
	"sync"
)

const defaultBitSetSize = 16

var bitSetPool = &sync.Pool{
	New: func() interface{} { return makeBitSet(1 << defaultBitSetSize) },
}

type BitSet struct {
	buf       []byte
	cnt       int64
	size      int
	isReverse bool
}

func NewBitSet(size int) *BitSet {
	s := bitSetPool.Get().(*BitSet)
	s.Resize(size)
	return s
}

func NewBitSetFromBytes(buf []byte, size int) *BitSet {
	s := &BitSet{
		buf:  make([]byte, bitFieldLen(size)),
		cnt:  -1,
		size: size,
	}
	copy(s.buf, buf)
	if l := bitFieldLen(size); cap(buf) < l {
		s.buf = make([]byte, l)
		copy(s.buf, buf)
	}
	if size%8 > 0 {
		s.buf[len(s.buf)-1] &= bitmask(size)
	}
	return s
}

func makeBitSet(size int) *BitSet {
	return &BitSet{
		buf:  make([]byte, bitFieldLen(size)),
		cnt:  0,
		size: size,
	}
}

func (s *BitSet) Resize(size int) *BitSet {
	if size < 0 {
		return s
	}
	sz := bitFieldLen(size)
	if s.buf == nil || cap(s.buf) < sz {
		buf := make([]byte, sz)
		copy(buf, s.buf)
		s.buf = buf
	} else {
		if size < s.size {
			if len(s.buf) > sz {
				s.buf[sz] = 0
				for bp := 1; sz+bp < len(s.buf); bp *= 2 {
					copy(s.buf[sz+bp:], s.buf[sz:sz+bp])
				}
			}
			if sz > 0 {
				s.buf[sz-1] &= bitmask(size)
			}
			s.cnt = -1
		}
		s.buf = s.buf[:sz]
	}
	s.size = size
	return s
}

func (s *BitSet) Reset() {
	if len(s.buf) > 0 {
		s.buf[0] = 0
		for bp := 1; bp < len(s.buf); bp *= 2 {
			copy(s.buf[bp:], s.buf[:bp])
		}
	}
	s.size = 0
	s.cnt = 0
	s.buf = s.buf[:0]
	s.isReverse = false
}

func (s *BitSet) Close() {
	s.Zero()
	bitSetPool.Put(s)
}

func (s *BitSet) And(r *BitSet) *BitSet {
	if s.size == 0 || s.cnt == 0 {
		return s
	}
	if r.Count() == 0 {
		s.Zero()
		return s
	}
	bitsetAnd(s.Bytes(), r.Bytes(), min(s.size, r.size))
	s.cnt = -1
	return s
}

func (s *BitSet) AndNot(r *BitSet) *BitSet {
	if s.size == 0 || s.cnt == 0 {
		return s
	}
	bitsetAndNot(s.Bytes(), r.Bytes(), min(s.size, r.size))
	s.cnt = -1
	return s
}

func (s *BitSet) Or(r *BitSet) *BitSet {
	if s.size == 0 {
		return s
	}
	if s.cnt == 0 {
		copy(s.buf, r.buf)
		s.cnt = r.cnt
		return s
	}
	bitsetOr(s.Bytes(), r.Bytes(), min(s.size, r.size))
	s.cnt = -1
	return s
}

func (s *BitSet) Xor(r *BitSet) *BitSet {
	if s.size == 0 {
		return s
	}
	bitsetXor(s.Bytes(), r.Bytes(), min(s.size, r.size))
	s.cnt = -1
	return s
}

func (s *BitSet) Neg() *BitSet {
	if s.size == 0 {
		return s
	}
	bitsetNeg(s.Bytes(), s.size)
	if s.cnt >= 0 {
		s.cnt = int64(s.size) - s.cnt
	}
	return s
}

func (s *BitSet) One() *BitSet {
	if s.size == 0 {
		return s
	}
	s.cnt = int64(s.size)
	s.buf[0] = 0xff
	for bp := 1; bp < len(s.buf); bp *= 2 {
		copy(s.buf[bp:], s.buf[:bp])
	}
	s.buf[len(s.buf)-1] = 0xff << (7 - uint(s.size-1)&0x7)
	return s
}

func (s *BitSet) Zero() *BitSet {
	s.isReverse = false
	if s.size == 0 || s.cnt == 0 {
		return s
	}
	s.cnt = 0
	s.buf[0] = 0
	for bp := 1; bp < len(s.buf); bp *= 2 {
		copy(s.buf[bp:], s.buf[:bp])
	}
	return s
}

func (s *BitSet) Fill(b byte) *BitSet {
	s.buf[0] = b
	for bp := 1; bp < len(s.buf); bp *= 2 {
		copy(s.buf[bp:], s.buf[:bp])
	}
	if s.isReverse {
		s.buf[0] &= bitsetReverseLut256[bitmask(s.size)]
	} else {
		s.buf[len(s.buf)-1] &= bitmask(s.size)
	}
	s.cnt = -1
	return s
}

func (s *BitSet) Set(i int) *BitSet {
	if i < 0 || i >= s.size {
		return s
	}
	if s.isReverse {
		pad := int(7 - uint(s.size-1)&0x7)
		i = s.size - i + pad - 1
	}
	mask := byte(1 << uint(7-i&0x7))
	if s.cnt >= 0 && s.buf[i>>3]&mask == 0 {
		s.cnt++
	}
	s.buf[i>>3] |= mask
	return s
}

func (s *BitSet) Clear(i int) *BitSet {
	if i < 0 || i >= s.size {
		return s
	}
	if s.isReverse {
		pad := int(7 - uint(s.size-1)&0x7)
		i = s.size - i + pad - 1
	}
	mask := byte(1 << uint(7-i&0x7))
	if s.cnt > 0 && s.buf[i>>3]&mask > 0 {
		s.cnt--
	}
	s.buf[i>>3] &^= mask
	return s
}

func (s *BitSet) IsSet(i int) bool {
	if i < 0 || i >= s.size {
		return false
	}
	if s.isReverse {
		pad := int(7 - uint(s.size-1)&0x7)
		i = s.size - i + pad - 1
	}
	mask := byte(1 << uint(7-i&0x7))
	return (s.buf[i>>3] & mask) > 0
}

func (s *BitSet) Reverse() *BitSet {
	bitsetReverse(s.buf)
	s.isReverse = !s.isReverse
	return s
}

func (s BitSet) Bytes() []byte {
	return s.buf
}

func (s *BitSet) Count() int64 {
	if s.cnt < 0 {
		if s.isReverse {
			s.cnt = bitsetPopCount(s.buf, len(s.buf)*8)
		} else {
			s.cnt = bitsetPopCount(s.buf, s.size)
		}
	}
	return s.cnt
}

func (s BitSet) Size() int {
	return s.size
}

func (b BitSet) Run(index int) (int, int) {
	if b.isReverse {
		if b.size == 0 || index < 0 || index > b.size {
			return -1, 0
		}
		pad := int(7 - uint(b.size-1)&0x7)
		index = b.size - index + pad - 1
		start, length := bitsetRun(b.buf, index, len(b.buf)*8)
		if start < 0 {
			return -1, 0
		}
		start = b.size - start + pad - 1
		return start, length
	}
	return bitsetRun(b.buf, index, b.size)
}

func (s BitSet) Indexes(slice []int) []int {
	cnt := s.Count()
	if slice == nil || cap(slice) < int(cnt) {
		slice = make([]int, cnt)
	} else {
		slice = slice[:cnt]
	}
	var j int
	for i, l := 0, s.size-s.size%8; i < l; i += 8 {
		b := s.buf[i>>3]
		for l := 0; b > 0; b, l = b<<1, l+1 {
			if b&0x80 == 0 {
				continue
			}
			slice[j] = i + l
			j++
		}
	}
	for i := s.size & ^0x7; i < s.size; i++ {
		mask := byte(1 << uint(7-i&0x7))
		if s.buf[i>>3]&mask == 0 {
			continue
		}
		slice[j] = i
		j++
	}
	return slice
}
