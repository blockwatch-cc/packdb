// Copyright (c) 2018-2019 KIDTSUNAMI
// Author: alex@kidtsunami.com

package pack

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"blockwatch.cc/packdb/encoding/block"
)

func (p *Package) MarshalBinary() ([]byte, error) {
	var (
		bodySize int
		err      error
	)

	buf := bytes.NewBuffer(make([]byte, 0, p.nFields*block.BlockSizeHint))
	buf.WriteByte(byte(p.version))

	var b [8]byte
	binary.BigEndian.PutUint32(b[0:], uint32(p.nFields))
	binary.BigEndian.PutUint32(b[4:], uint32(p.nValues))
	buf.Write(b[:])

	for _, v := range p.names {
		_, _ = buf.WriteString(v)
		buf.WriteByte(0)
	}

	headers := make([][]byte, p.nFields)
	encoded := make([][]byte, p.nFields)
	if p.offsets == nil {
		p.offsets = make([]int, p.nFields)
	}
	for i, b := range p.blocks {
		headers[i], encoded[i], err = b.Encode()
		p.offsets[i] = bodySize
		bodySize += len(encoded[i])
		if err != nil {
			return nil, err
		}
	}

	var offs [4]byte
	for _, v := range p.offsets {
		binary.BigEndian.PutUint32(offs[:], uint32(v))
		buf.Write(offs[:])
	}

	for _, v := range headers {
		buf.Write(v)
		block.BlockEncoderPool.Put(v[:0])
	}

	for _, v := range encoded {
		buf.Write(v)
		if cap(v) == block.BlockSizeHint {
			block.BlockEncoderPool.Put(v[:0])
		}
	}

	p.rawsize = bodySize
	p.packedsize = buf.Len()
	return buf.Bytes(), nil
}

func (p *Package) UnmarshalHeader(data []byte) (PackageHeader, error) {
	buf := bytes.NewBuffer(data)
	if err := p.unmarshalHeader(buf); err != nil {
		return PackageHeader{}, err
	}
	return p.Header(), nil
}

func (p *Package) unmarshalHeader(buf *bytes.Buffer) error {
	blen := buf.Len()
	if blen < 9 {
		return io.ErrShortBuffer
	}
	p.version, _ = buf.ReadByte()
	if p.version > packageStorageFormatVersionV1 {
		return fmt.Errorf("pack: invalid storage format version %d", p.version)
	}
	p.packedsize = blen

	p.nFields = int(binary.BigEndian.Uint32(buf.Next(4)))
	p.nValues = int(binary.BigEndian.Uint32(buf.Next(4)))

	p.names = make([]string, p.nFields)
	for i := 0; i < p.nFields; i++ {
		str, err := buf.ReadString(0)
		if err != nil {
			return err
		}
		strcopy := str[:len(str)-1]
		p.names[i] = strcopy
		p.namemap[strcopy] = i
	}

	p.offsets = make([]int, p.nFields)
	offs := buf.Next(4 * p.nFields)
	for i := 0; i < p.nFields; i++ {
		p.offsets[i] = int(binary.BigEndian.Uint32(offs[i*4:]))
	}

	if len(p.blocks) != p.nFields {
		p.blocks = make([]*block.Block, p.nFields)
	}
	for i := 0; i < p.nFields; i++ {
		if p.blocks[i] == nil {
			p.blocks[i] = &block.Block{}
		}
		if err := p.blocks[i].DecodeHeader(buf); err != nil {
			return err
		}
	}

	p.rawsize = buf.Len()
	return nil
}

func (p *Package) UnmarshalBinary(data []byte) error {
	buf := bytes.NewBuffer(data)
	err := p.unmarshalHeader(buf)
	if err != nil {
		return err
	}
	for i := 0; i < p.nFields; i++ {
		sz := buf.Len()
		if i+1 < p.nFields {
			sz = p.offsets[i+1] - p.offsets[i]
		}
		if err := p.blocks[i].DecodeBody(buf.Next(sz), p.nValues); err != nil {
			return err
		}
	}
	return nil
}
