// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"time"

	"blockwatch.cc/packdb/filter/bloom"
	"blockwatch.cc/packdb/filter/loglogbeta"
	"blockwatch.cc/packdb/util"
)

const (
	blockHeaderVersion   byte = 2
	blockTypeMask        byte = 0x1f
	blockCompressionMask byte = 0x03
	blockFlagMask        byte = 0x03
	blockPrecisionMask   byte = 0x0f
	blockFilterMask      byte = 0x03
)

type BlockHeader struct {
	Type        BlockType
	Compression Compression
	Precision   int
	Flags       BlockFlags
	MinValue    interface{}
	MaxValue    interface{}
	Bloom       *bloom.Filter
	Cardinality int
	dirty       bool
}

func (h BlockHeader) IsValid() bool {
	return h.Type != BlockIgnore && h.MinValue != nil && h.MaxValue != nil
}

func (h BlockHeader) IsDirty() bool {
	return h.dirty
}

func (h BlockHeader) SetDirty() {
	h.dirty = true
}

func (h BlockHeader) ResetDirty() {
	h.dirty = false
}

type BlockHeaderList []BlockHeader

func (h BlockHeaderList) Encode(buf *bytes.Buffer) error {
	buf.WriteByte(blockHeaderVersion)
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(len(h)))
	buf.Write(b[:])
	for _, v := range h {
		if err := v.Encode(buf); err != nil {
			return err
		}
	}
	return nil
}

func (h *BlockHeaderList) Decode(buf *bytes.Buffer) error {
	if buf.Len() < 5 {
		return fmt.Errorf("pack: short block header list, length %d", buf.Len())
	}

	b, _ := buf.ReadByte()
	if b != blockHeaderVersion {
		return fmt.Errorf("pack: invalid block header list version %d", b)
	}

	l := int(binary.BigEndian.Uint32(buf.Next(4)))
	*h = make(BlockHeaderList, l)
	for i := range *h {
		if err := (*h)[i].Decode(buf); err != nil {
			return err
		}
	}
	return nil
}

func (b *Block) MakeHeader() BlockHeader {
	if b.MinValue == nil || b.MaxValue == nil {
		return BlockHeader{}
	}
	bh := BlockHeader{
		Type:        b.Type,
		Compression: b.Compression,
		Precision:   b.Precision,
		Flags:       b.Flags,
		dirty:       true,
	}
	switch b.Type {
	case BlockTime:
		min, max := b.MinValue.(time.Time), b.MaxValue.(time.Time)
		bh.MinValue = min
		bh.MaxValue = max
	case BlockFloat:
		min, max := b.MinValue.(float64), b.MaxValue.(float64)
		bh.MinValue = min
		bh.MaxValue = max
	case BlockInteger:
		min, max := b.MinValue.(int64), b.MaxValue.(int64)
		bh.MinValue = min
		bh.MaxValue = max
	case BlockUnsigned:
		min, max := b.MinValue.(uint64), b.MaxValue.(uint64)
		if b.Flags&BlockFlagConvert > 0 {
			bh.MinValue = ConvertValue(DecompressAmount(min), b.Precision)
			bh.MaxValue = ConvertValue(DecompressAmount(max), b.Precision)
		} else if b.Flags&BlockFlagCompress > 0 {
			bh.MinValue = DecompressAmount(min)
			bh.MaxValue = DecompressAmount(max)
		} else {
			bh.MinValue = min
			bh.MaxValue = max
		}
	case BlockBool:
		min, max := b.MinValue.(bool), b.MaxValue.(bool)
		bh.MinValue = min
		bh.MaxValue = max
	case BlockString:
		min, max := b.MinValue.(string), b.MaxValue.(string)
		bh.MinValue = min
		bh.MaxValue = max
	case BlockBytes:
		min, max := b.MinValue.([]byte), b.MaxValue.([]byte)
		mincopy := make([]byte, len(min))
		copy(mincopy, min)
		maxcopy := make([]byte, len(max))
		copy(maxcopy, max)
		bh.MinValue = mincopy
		bh.MaxValue = maxcopy
	}
	return bh
}

func (h BlockHeader) Encode(buf *bytes.Buffer) error {
	buf.WriteByte(byte(h.Type)&blockTypeMask | (byte(h.Compression)&blockCompressionMask)<<5 | 0x80)
	buf.WriteByte((byte(h.Flags)&blockFlagMask)<<4 | byte(h.Precision)&blockPrecisionMask)
	var b [4]byte
	bigEndian.PutUint32(b[0:], uint32(h.Cardinality))
	_, _ = buf.Write(b[:])

	switch h.Type {
	case BlockTime:
		var v [16]byte
		min, max := h.MinValue.(time.Time), h.MaxValue.(time.Time)
		vmin, vmax := min.UnixNano(), max.UnixNano()
		bigEndian.PutUint64(v[0:], uint64(vmin))
		bigEndian.PutUint64(v[8:], uint64(vmax))
		_, _ = buf.Write(v[:])

	case BlockFloat:
		var v [16]byte
		min, max := h.MinValue.(float64), h.MaxValue.(float64)
		bigEndian.PutUint64(v[0:], math.Float64bits(min))
		bigEndian.PutUint64(v[8:], math.Float64bits(max))
		_, _ = buf.Write(v[:])

	case BlockInteger:
		var v [16]byte
		min, max := h.MinValue.(int64), h.MaxValue.(int64)
		bigEndian.PutUint64(v[0:], uint64(min))
		bigEndian.PutUint64(v[8:], uint64(max))
		_, _ = buf.Write(v[:])

	case BlockUnsigned:
		var v [16]byte
		if h.Flags&BlockFlagConvert > 0 {
			min, max := h.MinValue.(float64), h.MaxValue.(float64)
			bigEndian.PutUint64(v[0:], math.Float64bits(min))
			bigEndian.PutUint64(v[8:], math.Float64bits(max))
		} else {
			min, max := h.MinValue.(uint64), h.MaxValue.(uint64)
			bigEndian.PutUint64(v[0:], min)
			bigEndian.PutUint64(v[8:], max)
		}
		_, _ = buf.Write(v[:])

	case BlockBool:
		var v byte
		min, max := h.MinValue.(bool), h.MaxValue.(bool)
		if min {
			v = 1
		}
		if max {
			v += 2
		}
		buf.WriteByte(v)

	case BlockString:
		min, max := h.MinValue.(string), h.MaxValue.(string)
		_, _ = buf.WriteString(min)
		buf.WriteByte(0)
		_, _ = buf.WriteString(max)
		buf.WriteByte(0)

	case BlockBytes:
		min, max := h.MinValue.([]byte), h.MaxValue.([]byte)
		var v [8]byte
		i := binary.PutUvarint(v[:], uint64(len(min)))
		_, _ = buf.Write(v[:i])
		_, _ = buf.Write(min)

		i = binary.PutUvarint(v[:], uint64(len(max)))
		_, _ = buf.Write(v[:i])
		_, _ = buf.Write(max)

	case BlockIgnore:
		return nil

	default:
		return fmt.Errorf("pack: invalid data type %d", h.Type)
	}

	if h.Bloom != nil && h.Flags&BlockFlagBloom > 0 {
		buf.Write(h.Bloom.Bytes())
	}

	return nil
}

func (h *BlockHeader) Decode(buf *bytes.Buffer) error {
	val := buf.Next(1)
	var err error
	h.Type, err = readBlockType(val)
	if err != nil {
		return err
	}
	h.Compression, err = readBlockCompression(val)
	if err != nil {
		return err
	}

	if val[0]&0x80 > 0 {
		val = buf.Next(1)
		h.Precision = readBlockPrecision(val)
		h.Flags = readBlockFlags(val)
	}

	h.Cardinality = int(bigEndian.Uint32(buf.Next(4)))

	switch h.Type {
	case BlockTime:
		v := buf.Next(16)
		vmin := bigEndian.Uint64(v[0:])
		vmax := bigEndian.Uint64(v[8:])
		h.MinValue = time.Unix(0, int64(vmin)).UTC()
		h.MaxValue = time.Unix(0, int64(vmax)).UTC()

	case BlockFloat:
		v := buf.Next(16)
		h.MinValue = math.Float64frombits(bigEndian.Uint64(v[0:]))
		h.MaxValue = math.Float64frombits(bigEndian.Uint64(v[8:]))

	case BlockInteger:
		v := buf.Next(16)
		h.MinValue = int64(bigEndian.Uint64(v[0:]))
		h.MaxValue = int64(bigEndian.Uint64(v[8:]))

	case BlockUnsigned:
		v := buf.Next(16)
		if h.Flags&BlockFlagConvert > 0 {
			h.MinValue = math.Float64frombits(bigEndian.Uint64(v[0:]))
			h.MaxValue = math.Float64frombits(bigEndian.Uint64(v[8:]))
		} else {
			h.MinValue = bigEndian.Uint64(v[0:])
			h.MaxValue = bigEndian.Uint64(v[8:])
		}

	case BlockBool:
		v := buf.Next(1)
		h.MinValue = v[0]&1 > 0
		h.MaxValue = v[0]&2 > 0

	case BlockString:
		min, err := buf.ReadString(0)
		if err != nil {
			return fmt.Errorf("pack: reading min string block header: %v", err)
		}
		max, err := buf.ReadString(0)
		if err != nil {
			return fmt.Errorf("pack: reading max string block header: %v", err)
		}
		mincopy := min[:len(min)-1]
		maxcopy := max[:len(max)-1]
		h.MinValue = mincopy
		h.MaxValue = maxcopy

	case BlockBytes:
		length, err := binary.ReadUvarint(buf)
		if err != nil {
			return fmt.Errorf("pack: reading min []byte block header: %v", err)
		}
		min := buf.Next(int(length))
		length, err = binary.ReadUvarint(buf)
		if err != nil {
			return fmt.Errorf("pack: reading max []byte block header: %v", err)
		}
		max := buf.Next(int(length))
		mincopy := make([]byte, len(min))
		maxcopy := make([]byte, len(max))
		copy(mincopy, min)
		copy(maxcopy, max)
		h.MinValue = mincopy
		h.MaxValue = maxcopy

	default:
		return fmt.Errorf("pack: invalid data type %d", h.Type)
	}

	if h.Flags&BlockFlagBloom > 0 {
		// filter size is cardinality rounded up to next pow-2 times scale factor
		sz := h.Precision * int(pow2(int64(h.Cardinality)))
		b := buf.Next(sz)
		if len(b) < sz {
			return fmt.Errorf("pack: reading bloom filter: %w", io.ErrShortBuffer)
		}
		// we use a fixed number of 4 hash locations
		bCopy := make([]byte, sz)
		copy(bCopy, b)
		var err error
		h.Bloom, err = bloom.NewFilterBuffer(bCopy, 4)
		if err != nil {
			return fmt.Errorf("pack: reading bloom filter: %w", err)
		}
	}

	return nil
}

func (h *BlockHeader) EstimateCardinality(b *Block) {
	switch b.Len() {
	case 0:
		h.Cardinality = 0
		return
	case 1:
		h.Cardinality = 1
		return
	}
	filter := loglogbeta.NewFilterWithPrecision(12)
	var buf [8]byte
	switch h.Type {
	case BlockBytes:
		for i := range b.Bytes {
			filter.Add(b.Bytes[i])
		}
	case BlockString:
		for i := range b.Strings {
			filter.Add([]byte(b.Strings[i]))
		}
	case BlockTime:
		for _, v := range b.Timestamps {
			bigEndian.PutUint64(buf[:], uint64(v))
			filter.Add(buf[:])
		}
	case BlockBool:
		if !h.MinValue.(bool) {
			filter.Add([]byte{0})
		}
		if h.MaxValue.(bool) {
			filter.Add([]byte{1})
		}
	case BlockInteger:
		for _, v := range b.Integers {
			bigEndian.PutUint64(buf[:], uint64(v))
			filter.Add(buf[:])
		}
	case BlockUnsigned:
		for _, v := range b.Unsigneds {
			bigEndian.PutUint64(buf[:], v)
			filter.Add(buf[:])
		}
	case BlockFloat:
		for _, v := range b.Floats {
			bigEndian.PutUint64(buf[:], math.Float64bits(v))
			filter.Add(buf[:])
		}
	}
	h.Cardinality = util.Min(b.Len(), int(filter.Cardinality()))
}

func (h *BlockHeader) BuildBloomFilter(b *Block) {
	if h.Precision <= 0 {
		h.Precision = 1
	}
	h.EstimateCardinality(b)
	if h.Cardinality <= 0 {
		return
	}
	m := uint64(h.Cardinality * h.Precision * 8)
	h.Bloom = bloom.NewFilter(m, 4)
	var buf [8]byte
	switch h.Type {
	case BlockBytes:
		for i := range b.Bytes {
			h.Bloom.Add(b.Bytes[i])
		}
	case BlockString:
		for i := range b.Strings {
			h.Bloom.Add([]byte(b.Strings[i]))
		}
	case BlockTime:
		for _, v := range b.Timestamps {
			bigEndian.PutUint64(buf[:], uint64(v))
			h.Bloom.Add(buf[:])
		}
	case BlockBool:
		var (
			count int
			last  bool
		)
		for _, v := range b.Bools {
			if count == 2 {
				break
			}
			if count > 0 && v == last {
				continue
			}
			if v {
				h.Bloom.Add([]byte{1})
				count++
				last = v
			} else {
				h.Bloom.Add([]byte{0})
				count++
				last = v
			}
		}
	case BlockInteger:
		for _, v := range b.Integers {
			bigEndian.PutUint64(buf[:], uint64(v))
			h.Bloom.Add(buf[:])
		}
	case BlockUnsigned:
		for _, v := range b.Unsigneds {
			bigEndian.PutUint64(buf[:], v)
			h.Bloom.Add(buf[:])
		}
	case BlockFloat:
		for _, v := range b.Floats {
			bigEndian.PutUint64(buf[:], math.Float64bits(v))
			h.Bloom.Add(buf[:])
		}
	}
}
