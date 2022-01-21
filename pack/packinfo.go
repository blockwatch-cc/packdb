// Copyright (c) 2018-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	"blockwatch.cc/packdb/encoding/block"
)

type PackInfo struct {
	Key      uint32
	NValues  int
	Packsize int
	Blocks   block.BlockHeaderList

	dirty bool
}

func (p *Package) Info() PackInfo {
	h := PackInfo{
		Key:      p.key,
		NValues:  p.nValues,
		Packsize: p.packedsize,
		Blocks:   make(block.BlockHeaderList, 0, p.nFields),
		dirty:    true,
	}
	for _, v := range p.blocks {
		h.Blocks = append(h.Blocks, v.MakeHeader())
	}
	return h
}

func (h PackInfo) EncodedKey() []byte {
	return encodePackKey(h.Key)
}

func (h PackInfo) HeapSize() int {
	sz := szPackInfo + len(h.Blocks)*(szBlockInfo+16)
	for i := range h.Blocks {
		if h.Blocks[i].Bloom != nil {
			sz += szBloomFilter + len(h.Blocks[i].Bloom.Bytes())
		}
	}
	return sz
}

func (h *PackInfo) UpdateStats(pkg *Package) error {
	if pkg.IsJournal() || pkg.IsTomb() || pkg.Len() <= 2 {
		return nil
	}
	for i := range h.Blocks {
		if !h.Blocks[i].IsDirty() {
			continue
		}
		h.Blocks[i].ResetDirty()
		if h.Blocks[i].Flags&block.BlockFlagBloom == 0 {
			continue
		}
		h.Blocks[i].BuildBloomFilter(pkg.blocks[i])
	}
	h.dirty = true
	return nil
}

func (h PackInfo) MarshalBinary() ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := h.Encode(buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (h *PackInfo) UnmarshalBinary(data []byte) error {
	return h.Decode(bytes.NewBuffer(data))
}

func (h PackInfo) Encode(buf *bytes.Buffer) error {
	var b [4]byte
	bigEndian.PutUint32(b[:], uint32(h.Key))
	buf.Write(b[:])
	bigEndian.PutUint32(b[:], uint32(h.NValues))
	buf.Write(b[:])
	bigEndian.PutUint32(b[:], uint32(h.Packsize))
	buf.Write(b[:])
	return h.Blocks.Encode(buf)
}

func (h *PackInfo) Decode(buf *bytes.Buffer) error {
	h.Key = bigEndian.Uint32(buf.Next(4))
	h.NValues = int(bigEndian.Uint32(buf.Next(4)))
	h.Packsize = int(bigEndian.Uint32(buf.Next(4)))
	return h.Blocks.Decode(buf)
}

type PackInfoList []PackInfo

func (l PackInfoList) Len() int           { return len(l) }
func (l PackInfoList) Less(i, j int) bool { return l[i].Key < l[j].Key }
func (l PackInfoList) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }

func (l *PackInfoList) Add(info PackInfo) (PackInfo, int, bool) {
	i := sort.Search(l.Len(), func(i int) bool {
		return (*l)[i].Key >= info.Key
	})
	if i < len(*l) && (*l)[i].Key == info.Key {
		oldhead := (*l)[i]
		(*l)[i] = info
		return oldhead, i, false
	}
	*l = append(*l, PackInfo{})
	copy((*l)[i+1:], (*l)[i:])
	(*l)[i] = info
	return PackInfo{}, i, true
}

func (l *PackInfoList) Remove(head PackInfo) (PackInfo, int) {
	return l.RemoveKey(head.Key)
}

func (l *PackInfoList) RemoveKey(key uint32) (PackInfo, int) {
	i := sort.Search(l.Len(), func(i int) bool {
		return (*l)[i].Key >= key
	})
	if i < len(*l) && (*l)[i].Key == key {
		oldhead := (*l)[i]
		*l = append((*l)[:i], (*l)[i+1:]...)
		return oldhead, i
	}
	return PackInfo{}, -1
}

func (h PackInfoList) MarshalBinary() ([]byte, error) {
	buf := &bytes.Buffer{}
	buf.WriteByte(packageStorageFormatVersionV1)
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(len(h)))
	buf.Write(b[:])
	for _, v := range h {
		if err := v.Encode(buf); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func (h *PackInfoList) UnmarshalBinary(data []byte) error {
	if len(data) < 5 {
		return fmt.Errorf("pack: short package list header, length %d", len(data))
	}
	buf := bytes.NewBuffer(data)

	b, _ := buf.ReadByte()
	if b != packageStorageFormatVersionV1 {
		return fmt.Errorf("pack: invalid package list header version %d", b)
	}

	l := int(binary.BigEndian.Uint32(buf.Next(4)))

	*h = make(PackInfoList, l)

	for i := range *h {
		if err := (*h)[i].Decode(buf); err != nil {
			return err
		}
	}
	return nil
}
