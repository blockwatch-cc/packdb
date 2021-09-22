// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"bytes"
	"encoding"
	"fmt"
	"math"
	"reflect"
	"sort"
	"time"

	. "blockwatch.cc/packdb/encoding/block"
	"blockwatch.cc/packdb/util"
)

const (
	packageStorageFormatVersionV1 = 1
	maxPrecision                  = 12
)

type Package struct {
	used       int64
	version    byte
	nFields    int
	nValues    int
	offsets    []int
	names      []string
	blocks     []*Block
	namemap    map[string]int
	key        []byte
	tinfo      *typeInfo
	pkindex    int
	pkmap      map[uint64]int
	packedsize int
	rawsize    int
	dirty      bool
	cached     bool
	stripped   bool
}

func (p *Package) Key() []byte {
	return p.key
}

func (p *Package) SetKey(key []byte) {
	p.key = key
}

func (p *Package) IsJournal() bool {
	return bytes.Compare(journalKey, p.key) == 0
}

func (p *Package) PkMap() map[uint64]int {
	if p.pkmap != nil {
		return p.pkmap
	}
	if p.pkindex < 0 {
		return nil
	}
	p.pkmap = make(map[uint64]int, p.nValues)
	for i, v := range p.blocks[p.pkindex].Unsigneds {
		p.pkmap[v] = i
	}
	return p.pkmap
}

func NewPackage() *Package {
	return &Package{
		version: packageStorageFormatVersionV1,
		pkindex: -1,
		namemap: make(map[string]int),
	}
}

func (p *Package) HasNames() bool {
	return len(p.names) > 0
}

func (p *Package) IsDirty() bool {
	return p.dirty
}

func (p *Package) Len() int {
	return p.nValues
}

func (p *Package) Cap() int {
	if p.pkindex < 0 {
		return -1
	}
	return cap(p.blocks[p.pkindex].Unsigneds)
}

func (p *Package) FieldIndex(name string) int {
	if i, ok := p.namemap[name]; ok {
		return int(i)
	}
	return -1
}

func (p *Package) Field(name string) Field {
	idx := p.FieldIndex(name)
	if idx < 0 {
		return Field{Index: -1}
	}
	var flags FieldFlags
	switch p.blocks[idx].Compression {
	case SnappyCompression:
		flags = FlagCompressSnappy
	case LZ4Compression:
		flags = FlagCompressLZ4
	}
	return Field{
		Index:     idx,
		Name:      name,
		Type:      FieldTypeFromBlock(p.blocks[idx].Type),
		Flags:     flags,
		Precision: p.blocks[idx].Precision,
	}
}

func (p *Package) Contains(fields FieldList) bool {
	for _, v := range fields {
		if _, ok := p.namemap[v.Name]; !ok {
			return false
		}
	}
	return true
}

func (p *Package) PkColumn() []uint64 {
	if p.pkindex < 0 {
		return []uint64{}
	}
	return p.blocks[p.pkindex].Unsigneds
}

func (p *Package) initType(v interface{}) error {
	if p.tinfo != nil && p.tinfo.gotype {
		return nil
	}
	tinfo, err := getTypeInfo(v)
	if err != nil {
		return err
	}
	p.tinfo = tinfo
	if p.pkindex < 0 {
		p.pkindex = tinfo.PkColumn()
	}
	return nil
}

func (p *Package) Init(v interface{}, sz int) error {
	if err := p.initType(v); err != nil {
		return err
	}

	if len(p.tinfo.fields) > 256 {
		return fmt.Errorf("pack: cannot handle more than 256 fields")
	}

	p.nFields = len(p.tinfo.fields)
	p.blocks = make([]*Block, p.nFields)
	p.offsets = make([]int, p.nFields)
	p.names = make([]string, p.nFields)
	p.namemap = make(map[string]int)
	p.dirty = true
	val := reflect.Indirect(reflect.ValueOf(v))
	for i, finfo := range p.tinfo.fields {
		f := finfo.value(val)
		p.names[i] = finfo.name
		p.namemap[finfo.name] = i
		p.namemap[finfo.alias] = i
		switch f.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			p.blocks[i] = NewBlock(BlockInteger, sz, finfo.flags.Compression(), 0, 0)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if finfo.flags&FlagConvert > 0 {
				p.blocks[i] = NewBlock(
					BlockUnsigned,
					sz,
					finfo.flags.Compression(),
					finfo.precision,
					BlockFlagCompress,
				)
			} else {
				p.blocks[i] = NewBlock(BlockUnsigned, sz, finfo.flags.Compression(), 0, 0)
			}
		case reflect.Float32, reflect.Float64:
			if finfo.flags&FlagConvert > 0 {
				p.blocks[i] = NewBlock(
					BlockUnsigned,
					sz,
					finfo.flags.Compression(),
					finfo.precision,
					BlockFlagConvert|BlockFlagCompress,
				)
			} else {
				p.blocks[i] = NewBlock(
					BlockFloat,
					sz,
					finfo.flags.Compression(),
					finfo.precision,
					0,
				)
			}
		case reflect.String:
			p.blocks[i] = NewBlock(BlockString, sz, finfo.flags.Compression(), 0, 0)
		case reflect.Slice:
			if f.CanInterface() && f.Type().Implements(binaryMarshalerType) {
				p.blocks[i] = NewBlock(BlockBytes, sz, finfo.flags.Compression(), 0, 0)
				break
			}
			if f.CanInterface() && f.Type().Implements(textMarshalerType) {
				p.blocks[i] = NewBlock(BlockString, sz, finfo.flags.Compression(), 0, 0)
				break
			}
			if f.CanInterface() && f.Type().Implements(stringerType) {
				p.blocks[i] = NewBlock(BlockString, sz, finfo.flags.Compression(), 0, 0)
				break
			}
			if f.Type() != byteSliceType {
				return fmt.Errorf("pack: unsupported slice type %s", f.Type().String())
			}
			p.blocks[i] = NewBlock(BlockBytes, sz, finfo.flags.Compression(), 0, 0)
		case reflect.Bool:
			p.blocks[i] = NewBlock(BlockBool, sz, finfo.flags.Compression(), 0, 0)
		case reflect.Struct:
			if f.Type().String() == "time.Time" {
				p.blocks[i] = NewBlock(BlockTime, sz, finfo.flags.Compression(), 0, 0)
			} else if f.CanInterface() && f.Type().Implements(binaryMarshalerType) {
				p.blocks[i] = NewBlock(BlockBytes, sz, finfo.flags.Compression(), 0, 0)
			} else {
				return fmt.Errorf("pack: unsupported embedded struct type %s", f.Type().String())
			}
		case reflect.Array:
			if f.CanInterface() && f.Type().Implements(binaryMarshalerType) {
				p.blocks[i] = NewBlock(BlockBytes, sz, finfo.flags.Compression(), 0, 0)
				break
			}
			return fmt.Errorf("pack: unsupported array type %s", f.Type().String())
		default:
			return fmt.Errorf("pack: unsupported type %s (%v)", f.Type().String(), f.Kind())
		}
	}
	return nil
}

func (p *Package) InitFields(fields FieldList, sz int) error {
	if len(fields) > 256 {
		return fmt.Errorf("pack: cannot handle more than 256 fields")
	}

	p.nFields = len(fields)
	p.blocks = make([]*Block, p.nFields)
	p.offsets = make([]int, p.nFields)
	p.names = make([]string, p.nFields)
	p.namemap = make(map[string]int)
	p.dirty = true
	p.tinfo = &typeInfo{
		fields: make([]fieldInfo, p.nFields),
		gotype: false,
	}
	for i, field := range fields {
		if field.Flags&FlagPrimary > 0 {
			p.pkindex = i
		}
		p.tinfo.fields[i].name = field.Name
		p.tinfo.fields[i].alias = field.Alias
		p.tinfo.fields[i].flags = field.Flags
		p.names[i] = field.Name
		p.namemap[field.Name] = i
		p.namemap[field.Alias] = i
		switch field.Type {
		case FieldTypeInt64:
			p.blocks[i] = NewBlock(BlockInteger, sz, field.Flags.Compression(), 0, 0)
		case FieldTypeUint64:
			if field.Flags&FlagConvert > 0 {
				p.blocks[i] = NewBlock(
					BlockUnsigned,
					sz,
					field.Flags.Compression(),
					field.Precision,
					BlockFlagConvert|BlockFlagCompress,
				)
			} else {
				p.blocks[i] = NewBlock(
					BlockUnsigned,
					sz,
					field.Flags.Compression(),
					0,
					0,
				)
			}
		case FieldTypeFloat64:
			if field.Flags&FlagConvert > 0 {
				p.blocks[i] = NewBlock(
					BlockUnsigned,
					sz,
					field.Flags.Compression(),
					field.Precision,
					BlockFlagConvert|BlockFlagCompress,
				)
			} else {
				p.blocks[i] = NewBlock(BlockFloat, sz, field.Flags.Compression(), 0, 0)
			}
		case FieldTypeString:
			p.blocks[i] = NewBlock(BlockString, sz, field.Flags.Compression(), 0, 0)
		case FieldTypeBytes:
			p.blocks[i] = NewBlock(BlockBytes, sz, field.Flags.Compression(), 0, 0)
		case FieldTypeBoolean:
			p.blocks[i] = NewBlock(BlockBool, sz, field.Flags.Compression(), 0, 0)
		case FieldTypeDatetime:
			p.blocks[i] = NewBlock(BlockTime, sz, field.Flags.Compression(), 0, 0)
		default:
			return fmt.Errorf("pack: unsupported field type %s", field.Type)
		}
	}
	return nil
}

func (p *Package) Clone(copydata bool, sz int) *Package {
	np := &Package{
		version:  p.version,
		nFields:  p.nFields,
		nValues:  0,
		offsets:  make([]int, p.nFields),
		names:    p.names,
		namemap:  make(map[string]int),
		blocks:   make([]*Block, p.nFields),
		key:      nil,
		dirty:    true,
		stripped: p.stripped,
		tinfo:    p.tinfo,
		pkindex:  p.pkindex,
	}

	for i, b := range p.blocks {
		np.blocks[i] = b.Clone(sz, copydata)
		np.namemap[np.names[i]] = i
	}

	if copydata {
		np.nValues = p.nValues
	}
	return np
}

func (p *Package) KeepFields(fields FieldList) *Package {
	if len(fields) == 0 {
		return p
	}
	for i, v := range p.names {
		if !fields.Contains(v) {
			p.blocks[i].Release()
			p.blocks[i].Type = BlockIgnore
			p.stripped = true
		}
	}
	return p
}

func (p *Package) UpdateAliasesFrom(fields FieldList) *Package {
	if len(fields) == 0 {
		return p
	}
	for _, v := range fields {
		if v.Index < 0 || v.Index-1 > p.nFields {
			continue
		}
		delete(p.namemap, v.Alias)
		p.namemap[v.Alias] = v.Index
	}
	return p
}

func (p *Package) UpdateAliases(aliases []string) *Package {
	if len(aliases) == 0 {
		return p
	}
	for i, v := range aliases {
		if i >= p.nFields {
			continue
		}
		p.namemap[v] = i
	}
	return p
}

func (p *Package) Push(v interface{}) error {
	if err := p.initType(v); err != nil {
		return err
	}
	val := reflect.Indirect(reflect.ValueOf(v))
	if !val.IsValid() {
		return fmt.Errorf("pack: invalid value of type %T", v)
	}
	for i, finfo := range p.tinfo.fields {
		blockId := i
		if p.HasNames() {
			if v, ok := p.namemap[finfo.name]; ok {
				blockId = v
			} else {
				continue
			}
		}
		f := finfo.value(val)
		switch p.blocks[blockId].Type {
		case BlockInteger:
			p.blocks[blockId].Integers = append(p.blocks[blockId].Integers, f.Int())

		case BlockUnsigned:
			var amount uint64
			if p.blocks[blockId].Flags&(BlockFlagConvert|BlockFlagCompress) > 0 || finfo.flags&FlagConvert > 0 {
				if f.Type().String() == "float64" {
					amount = CompressAmount(ConvertAmount(f.Float(), p.blocks[blockId].Precision))
				} else {
					amount = CompressAmount(f.Uint())
				}
			} else {
				amount = f.Uint()
			}
			p.blocks[blockId].Unsigneds = append(p.blocks[blockId].Unsigneds, amount)

		case BlockFloat:
			p.blocks[blockId].Floats = append(p.blocks[blockId].Floats, f.Float())

		case BlockString:
			if f.CanInterface() && f.Type().Implements(textMarshalerType) {
				txt, err := f.Interface().(encoding.TextMarshaler).MarshalText()
				if err != nil {
					return err
				}
				p.blocks[blockId].Strings = append(p.blocks[blockId].Strings, string(txt))
			} else if f.CanInterface() && f.Type().Implements(stringerType) {
				p.blocks[blockId].Strings = append(p.blocks[blockId].Strings, f.Interface().(fmt.Stringer).String())
			} else {
				p.blocks[blockId].Strings = append(p.blocks[blockId].Strings, f.String())
			}

		case BlockBytes:
			var amount []byte
			if f.CanInterface() && f.Type().Implements(binaryMarshalerType) {
				if b, err := f.Interface().(encoding.BinaryMarshaler).MarshalBinary(); err != nil {
					return err
				} else {
					amount = b
				}
			} else {
				buf := f.Bytes()
				amount = make([]byte, len(buf))
				copy(amount, buf)
			}
			p.blocks[blockId].Bytes = append(p.blocks[blockId].Bytes, amount)

		case BlockBool:
			p.blocks[blockId].Bools = append(p.blocks[blockId].Bools, f.Bool())

		case BlockTime:
			p.blocks[blockId].Timestamps = append(p.blocks[blockId].Timestamps, f.Interface().(time.Time).UnixNano())

		case BlockIgnore:

		default:
			return fmt.Errorf("pack: unsupported type %s (%v)", f.Type().String(), f.Kind())
		}
		p.blocks[blockId].Dirty = true
	}
	p.nValues++
	p.dirty = true
	p.pkmap = nil
	return nil
}

func (p *Package) ReplaceAt(pos int, v interface{}) error {
	if err := p.initType(v); err != nil {
		return err
	}
	if p.nValues <= pos {
		return fmt.Errorf("pack: invalid pack offset %d (max %d)", pos, p.nValues)
	}
	val := reflect.Indirect(reflect.ValueOf(v))
	if !val.IsValid() {
		return fmt.Errorf("pack: invalid value of type %T", v)
	}
	for i, finfo := range p.tinfo.fields {
		blockId := i
		if p.HasNames() {
			if v, ok := p.namemap[finfo.name]; ok {
				blockId = v
			} else {
				continue
			}
		}
		f := finfo.value(val)
		switch p.blocks[blockId].Type {
		case BlockInteger:
			amount := f.Int()
			p.blocks[blockId].Integers[pos] = amount

		case BlockUnsigned:
			var amount uint64
			if p.blocks[blockId].Flags&(BlockFlagConvert|BlockFlagCompress) > 0 ||
				finfo.flags&FlagConvert > 0 {
				if f.Type().String() == "float64" {
					amount = CompressAmount(ConvertAmount(f.Float(), p.blocks[blockId].Precision))
				} else {
					amount = CompressAmount(f.Uint())
				}
			} else {
				amount = f.Uint()
			}
			p.blocks[blockId].Unsigneds[pos] = amount

		case BlockFloat:
			amount := f.Float()
			p.blocks[blockId].Floats[pos] = amount

		case BlockString:
			if f.CanInterface() && f.Type().Implements(textMarshalerType) {
				txt, err := f.Interface().(encoding.TextMarshaler).MarshalText()
				if err != nil {
					return err
				}
				p.blocks[blockId].Strings[pos] = string(txt)
			} else if f.CanInterface() && f.Type().Implements(stringerType) {
				p.blocks[blockId].Strings[pos] = f.Interface().(fmt.Stringer).String()
			} else {
				p.blocks[blockId].Strings[pos] = f.String()
			}

		case BlockBytes:
			var amount []byte
			if f.CanInterface() && f.Type().Implements(binaryMarshalerType) {
				if b, err := f.Interface().(encoding.BinaryMarshaler).MarshalBinary(); err != nil {
					return err
				} else {
					amount = b
				}
			} else {
				buf := f.Bytes()
				amount = make([]byte, len(buf))
				copy(amount, buf)
			}
			p.blocks[blockId].Bytes[pos] = amount

		case BlockBool:
			amount := f.Bool()
			p.blocks[blockId].Bools[pos] = amount

		case BlockTime:
			amount := f.Interface().(time.Time)
			p.blocks[blockId].Timestamps[pos] = amount.UnixNano()

		case BlockIgnore:

		default:
			return fmt.Errorf("pack: unsupported type %s (%v)", f.Type().String(), f.Kind())
		}
		p.blocks[blockId].Dirty = true
	}
	p.dirty = true
	p.pkmap = nil
	return nil
}

func (p *Package) ReadAt(pos int, v interface{}) error {
	if p.tinfo == nil || !p.tinfo.gotype {
		tinfo, err := getTypeInfo(v)
		if err != nil {
			return err
		}
		p.tinfo = tinfo
	}
	return p.ReadAtWithInfo(pos, v, p.tinfo)
}

func (p *Package) ReadAtWithInfo(pos int, v interface{}, tinfo *typeInfo) error {
	if p.nValues <= pos {
		return nil
	}
	val := derefValue(reflect.ValueOf(v))
	if !val.IsValid() {
		return fmt.Errorf("pack: invalid value of type %T", v)
	}
	for i, finfo := range tinfo.fields {
		blockId := i
		if p.HasNames() {
			if v, ok := p.namemap[finfo.name]; ok {
				blockId = v
			} else {
				continue
			}
		}
		dst := finfo.value(val)
		if !dst.IsValid() {
			continue
		}
		dst0 := dst
		if dst.Kind() == reflect.Ptr {
			if dst.IsNil() && dst.CanSet() {
				dst.Set(reflect.New(dst.Type().Elem()))
			}
			dst = dst.Elem()
		}
		b := p.blocks[blockId]
		switch b.Type {
		case BlockInteger:
			dst.SetInt(b.Integers[pos])

		case BlockUnsigned:
			value := b.Unsigneds[pos]
			if b.Flags&(BlockFlagConvert|BlockFlagCompress) > 0 || finfo.flags&FlagConvert > 0 {
				if dst.Type().String() == "float64" {
					dst.SetFloat(ConvertValue(DecompressAmount(value), b.Precision))
				} else {
					dst.SetUint(DecompressAmount(value))
				}
			} else {
				dst.SetUint(value)
			}

		case BlockFloat:
			dst.SetFloat(b.Floats[pos])

		case BlockString:
			if dst.CanAddr() {
				pv := dst.Addr()
				if pv.CanInterface() && pv.Type().Implements(textUnmarshalerType) {
					if err := pv.Interface().(encoding.TextUnmarshaler).UnmarshalText([]byte(b.Strings[pos])); err != nil {
						return err
					}
					break
				}
			}
			dst.SetString(b.Strings[pos])

		case BlockBytes:
			if dst.CanAddr() {
				pv := dst.Addr()
				if pv.CanInterface() && pv.Type().Implements(binaryUnmarshalerType) {
					if err := pv.Interface().(encoding.BinaryUnmarshaler).UnmarshalBinary(b.Bytes[pos]); err != nil {
						return err
					}
					break
				}
			}
			buf := make([]byte, len(b.Bytes[pos]))
			copy(buf, b.Bytes[pos])
			dst.SetBytes(buf)

		case BlockBool:
			dst.SetBool(b.Bools[pos])

		case BlockTime:
			dst.Set(reflect.ValueOf(time.Unix(0, b.Timestamps[pos]).UTC()))

		case BlockIgnore:

		default:
			return fmt.Errorf("pack: unsupported type %s (%v)", dst0.Type().String(), dst0.Kind())
		}
	}
	return nil
}

func (p *Package) ForEach(proto interface{}, fn func(i int, val interface{}) error) error {
	if p.tinfo == nil || !p.tinfo.gotype {
		tinfo, err := getTypeInfo(proto)
		if err != nil {
			return err
		}
		p.tinfo = tinfo
	}
	typ := derefIndirect(proto).Type()
	for i := 0; i < p.nValues; i++ {
		val := reflect.New(typ)
		if err := p.ReadAtWithInfo(i, val.Interface(), p.tinfo); err != nil {
			return err
		}
		if err := fn(i, val.Interface()); err != nil {
			return err
		}
	}
	return nil
}

func (p *Package) FieldAt(index, pos int) (interface{}, error) {
	if p.nFields <= index {
		return nil, fmt.Errorf("pack: invalid field index %d (max=%d)", index, p.nFields)
	}
	if p.nValues <= pos {
		return nil, fmt.Errorf("pack: invalid pos index %d (max=%d)", pos, p.nValues)
	}
	switch p.blocks[index].Type {
	case BlockInteger:
		val := p.blocks[index].Integers[pos]
		return val, nil
	case BlockUnsigned:
		val := p.blocks[index].Unsigneds[pos]
		if p.blocks[index].Flags&BlockFlagConvert > 0 {
			return ConvertValue(DecompressAmount(val), p.blocks[index].Precision), nil
		}
		if p.blocks[index].Flags&BlockFlagCompress > 0 {
			return DecompressAmount(val), nil
		}
		return val, nil
	case BlockFloat:
		val := p.blocks[index].Floats[pos]
		return val, nil
	case BlockString:
		val := p.blocks[index].Strings[pos]
		return val, nil
	case BlockBytes:
		val := p.blocks[index].Bytes[pos]
		return val, nil
	case BlockBool:
		val := p.blocks[index].Bools[pos]
		return val, nil
	case BlockTime:
		val := time.Unix(0, p.blocks[index].Timestamps[pos]).UTC()
		return val, nil
	default:
		return nil, fmt.Errorf("pack: invalid data type %d", p.blocks[index].Type)
	}
}

func (p *Package) SetFieldAt(index, pos int, v interface{}) error {
	if p.nFields <= index {
		return fmt.Errorf("pack: invalid field index %d (max=%d)", index, p.nFields)
	}
	if p.nValues <= pos {
		return fmt.Errorf("pack: invalid pos index %d (max=%d)", pos, p.nValues)
	}
	val := reflect.Indirect(reflect.ValueOf(v))
	if !val.IsValid() {
		return fmt.Errorf("pack: invalid value of type %T", v)
	}
	switch p.blocks[index].Type {
	case BlockInteger:
		p.blocks[index].Integers[pos] = val.Int()
	case BlockUnsigned:
		if p.blocks[index].Flags&BlockFlagConvert > 0 {
			p.blocks[index].Unsigneds[pos] = CompressAmount(ConvertAmount(val.Float(), p.blocks[index].Precision))
		} else if p.blocks[index].Flags&BlockFlagCompress > 0 {
			p.blocks[index].Unsigneds[pos] = CompressAmount(val.Uint())
		} else {
			p.blocks[index].Unsigneds[pos] = val.Uint()
		}
	case BlockFloat:
		amount := val.Float()
		p.blocks[index].Floats[pos] = amount
	case BlockString:
		if val.CanInterface() && val.Type().Implements(textMarshalerType) {
			txt, err := val.Interface().(encoding.TextMarshaler).MarshalText()
			if err != nil {
				return err
			}
			p.blocks[index].Strings[pos] = string(txt)
		} else if val.CanInterface() && val.Type().Implements(stringerType) {
			p.blocks[index].Strings[pos] = val.Interface().(fmt.Stringer).String()
		} else {
			p.blocks[index].Strings[pos] = val.String()
		}

	case BlockBytes:
		var amount []byte
		if val.CanInterface() && val.Type().Implements(binaryMarshalerType) {
			if b, err := val.Interface().(encoding.BinaryMarshaler).MarshalBinary(); err != nil {
				return err
			} else {
				amount = b
			}
		} else {
			buf := val.Bytes()
			amount = make([]byte, len(buf))
			copy(amount, buf)
		}
		p.blocks[index].Bytes[pos] = amount
	case BlockBool:
		amount := val.Bool()
		p.blocks[index].Bools[pos] = amount
	case BlockTime:
		amount := val.Interface().(time.Time)
		p.blocks[index].Timestamps[pos] = amount.UnixNano()
	default:
		return fmt.Errorf("pack: invalid data type %d", p.blocks[index].Type)
	}
	p.blocks[index].Dirty = true
	p.dirty = true
	if p.pkindex == index {
		p.pkmap = nil
	}
	return nil
}

func (p *Package) isValidAt(index, pos int, typ BlockType) error {
	if index < 0 || p.nFields <= index {
		return ErrNoField
	}
	if pos < 0 || p.nValues <= pos {
		return ErrNoColumn
	}
	if p.blocks[index].Type != typ {
		return ErrInvalidType
	}
	return nil
}

func (p *Package) Uint64At(index, pos int) (uint64, error) {
	if err := p.isValidAt(index, pos, BlockUnsigned); err != nil {
		return 0, err
	}
	if p.blocks[index].Flags&BlockFlagCompress > 0 {
		return DecompressAmount(p.blocks[index].Unsigneds[pos]), nil
	}
	return p.blocks[index].Unsigneds[pos], nil
}

func (p *Package) Int64At(index, pos int) (int64, error) {
	if err := p.isValidAt(index, pos, BlockInteger); err != nil {
		return 0, err
	}
	return p.blocks[index].Integers[pos], nil
}

func (p *Package) Float64At(index, pos int) (float64, error) {
	if p.blocks[index].Flags&BlockFlagConvert > 0 {
		if err := p.isValidAt(index, pos, BlockUnsigned); err != nil {
			return 0.0, err
		}
		val := DecompressAmount(p.blocks[index].Unsigneds[pos])
		return ConvertValue(val, p.blocks[index].Precision), nil
	}
	if err := p.isValidAt(index, pos, BlockFloat); err != nil {
		return 0.0, err
	}
	return p.blocks[index].Floats[pos], nil
}

func (p *Package) StringAt(index, pos int) (string, error) {
	if err := p.isValidAt(index, pos, BlockString); err != nil {
		return "", err
	}
	return p.blocks[index].Strings[pos], nil
}

func (p *Package) BytesAt(index, pos int) ([]byte, error) {
	if err := p.isValidAt(index, pos, BlockBytes); err != nil {
		return nil, err
	}
	return p.blocks[index].Bytes[pos], nil
}

func (p *Package) BoolAt(index, pos int) (bool, error) {
	if err := p.isValidAt(index, pos, BlockBool); err != nil {
		return false, err
	}
	return p.blocks[index].Bools[pos], nil
}

func (p *Package) TimeAt(index, pos int) (time.Time, error) {
	if err := p.isValidAt(index, pos, BlockTime); err != nil {
		return zeroTime, err
	}
	return time.Unix(0, p.blocks[index].Timestamps[pos]).UTC(), nil
}

func (p *Package) IsZeroAt(index, pos int) bool {
	if p.nFields <= index || p.nValues <= pos {
		return false
	}
	switch p.blocks[index].Type {
	case BlockInteger, BlockUnsigned, BlockBool:
		return false
	case BlockFloat:
		v := p.blocks[index].Floats[pos]
		return math.IsNaN(v) || math.IsInf(v, 0)
	case BlockString:
		return len(p.blocks[index].Strings[pos]) == 0
	case BlockBytes:
		return len(p.blocks[index].Bytes[pos]) == 0
	case BlockTime:
		val := p.blocks[index].Timestamps[pos]
		return val == 0 || time.Unix(0, val).IsZero()
	}
	return true
}

func (p *Package) Column(index int) (interface{}, error) {
	if index < 0 || p.nFields <= index {
		return nil, ErrNoField
	}
	switch p.blocks[index].Type {
	case BlockInteger:
		return p.blocks[index].Integers, nil
	case BlockUnsigned:
		val := p.blocks[index].Unsigneds
		if p.blocks[index].Flags&BlockFlagConvert > 0 {
			resp := make([]float64, len(val))
			for i, v := range val {
				resp[i] = ConvertValue(DecompressAmount(v), p.blocks[index].Precision)
			}
			return resp, nil
		}
		if p.blocks[index].Flags&BlockFlagCompress > 0 {
			resp := make([]uint64, len(val))
			for i, v := range val {
				resp[i] = DecompressAmount(v)
			}
			return resp, nil
		}
		return val, nil
	case BlockFloat:
		return p.blocks[index].Floats, nil
	case BlockString:
		return p.blocks[index].Strings, nil
	case BlockBytes:
		return p.blocks[index].Bytes, nil
	case BlockBool:
		return p.blocks[index].Bools, nil
	case BlockTime:
		return p.blocks[index].Timestamps, nil
	default:
		return nil, fmt.Errorf("pack: invalid data type %d", p.blocks[index].Type)
	}
}

func (p *Package) RowAt(pos int) ([]interface{}, error) {
	if p.nValues <= pos {
		return nil, fmt.Errorf("pack: invalid pack offset %d (max %d)", pos, p.nValues)
	}
	out := make([]interface{}, p.nFields)
	for i, b := range p.blocks {
		switch b.Type {
		case BlockInteger:
			out[i] = b.Integers[pos]
		case BlockUnsigned:
			out[i] = b.Unsigneds[pos]
		case BlockFloat:
			out[i] = b.Floats[pos]
		case BlockString:
			str := b.Strings[pos]
			out[i] = str
		case BlockBytes:
			buf := make([]byte, len(b.Bytes[pos]))
			copy(buf, b.Bytes[pos])
			out[i] = buf
		case BlockBool:
			out[i] = b.Bools[pos]
		case BlockTime:
			out[i] = b.Timestamps[pos]
		case BlockIgnore:
		default:
			return nil, fmt.Errorf("pack: invalid data type %d", b.Type)
		}
	}
	return out, nil
}

func (p *Package) RangeAt(index, start, end int) (interface{}, error) {
	if p.nFields <= index {
		return nil, fmt.Errorf("pack: invalid field index %d (max=%d)", index, p.nFields)
	}
	if p.nValues <= start || p.nValues <= end {
		return nil, fmt.Errorf("pack: invalid range %d:%d (max=%d)", start, end, p.nValues)
	}
	switch p.blocks[index].Type {
	case BlockInteger:
		return p.blocks[index].Integers[start:end], nil
	case BlockUnsigned:
		val := p.blocks[index].Unsigneds[start:end]
		if p.blocks[index].Flags&BlockFlagConvert > 0 {
			resp := make([]float64, len(val))
			for i, v := range val {
				resp[i] = ConvertValue(DecompressAmount(v), p.blocks[index].Precision)
			}
			return resp, nil
		}
		if p.blocks[index].Flags&BlockFlagCompress > 0 {
			resp := make([]uint64, len(val))
			for i, v := range val {
				resp[i] = DecompressAmount(v)
			}
			return resp, nil
		}
		return val, nil
	case BlockFloat:
		return p.blocks[index].Floats[start:end], nil
	case BlockString:
		return p.blocks[index].Strings[start:end], nil
	case BlockBytes:
		return p.blocks[index].Bytes[start:end], nil
	case BlockBool:
		return p.blocks[index].Bools[start:end], nil
	case BlockTime:
		return p.blocks[index].Timestamps[start:end], nil
	default:
		return nil, fmt.Errorf("pack: invalid data type %d", p.blocks[index].Type)
	}
}

func (p *Package) CopyFrom(src *Package, dstPos, srcPos, srcLen int) error {
	if src.nFields != p.nFields {
		return fmt.Errorf("pack: invalid src/dst field count %d/%d", src.nFields, p.nFields)
	}
	if src.nValues <= srcPos {
		return fmt.Errorf("pack: invalid source pack offset %d (max %d)", srcPos, src.nValues)
	}
	if src.nValues < srcPos+srcLen {
		return fmt.Errorf("pack: invalid source pack offset %d len %d (max %d)", srcPos, srcLen, src.nValues)
	}
	if p.nValues <= dstPos {
		return fmt.Errorf("pack: invalid dest pack offset %d (max %d)", dstPos, p.nValues)
	}
	n := util.Min(p.Len()-dstPos, srcLen)
	for i, _ := range p.blocks {
		switch src.blocks[i].Type {
		case BlockInteger:
			copy(p.blocks[i].Integers[dstPos:], src.blocks[i].Integers[srcPos:srcPos+n])
		case BlockUnsigned:
			copy(p.blocks[i].Unsigneds[dstPos:], src.blocks[i].Unsigneds[srcPos:srcPos+n])
		case BlockFloat:
			copy(p.blocks[i].Floats[dstPos:], src.blocks[i].Floats[srcPos:srcPos+n])
		case BlockString:
			copy(p.blocks[i].Strings[dstPos:], src.blocks[i].Strings[srcPos:srcPos+n])
		case BlockBytes:
			for j, v := range src.blocks[i].Bytes[srcPos : srcPos+n] {
				if len(p.blocks[i].Bytes[dstPos+j]) < len(v) {
					buf := make([]byte, len(v))
					copy(buf, v)
					p.blocks[i].Bytes[dstPos+j] = buf
				} else {
					p.blocks[i].Bytes[dstPos+j] = p.blocks[i].Bytes[dstPos+j][:len(v)]
					copy(p.blocks[i].Bytes[dstPos+j], v)
				}
			}
		case BlockBool:
			copy(p.blocks[i].Bools[dstPos:], src.blocks[i].Bools[srcPos:srcPos+n])
		case BlockTime:
			copy(p.blocks[i].Timestamps[dstPos:], src.blocks[i].Timestamps[srcPos:srcPos+n])
		case BlockIgnore:
		default:
			return fmt.Errorf("pack: invalid data type %d", p.blocks[i].Type)
		}
		p.blocks[i].Dirty = true
	}
	p.dirty = true
	p.pkmap = nil
	return nil
}

func (p *Package) AppendFrom(src *Package, srcPos, srcLen int, safecopy bool) error {
	if src.nFields != p.nFields {
		return fmt.Errorf("pack: invalid src/dst field count %d/%d", src.nFields, p.nFields)
	}
	if src.nValues <= srcPos {
		return fmt.Errorf("pack: invalid source pack offset %d (max %d)", srcPos, src.nValues)
	}
	if src.nValues < srcPos+srcLen {
		return fmt.Errorf("pack: invalid source pack offset %d len %d (max %d)", srcPos, srcLen, src.nValues)
	}
	for i, _ := range p.blocks {
		switch src.blocks[i].Type {
		case BlockInteger:
			p.blocks[i].Integers = append(p.blocks[i].Integers, src.blocks[i].Integers[srcPos:srcPos+srcLen]...)
		case BlockUnsigned:
			p.blocks[i].Unsigneds = append(p.blocks[i].Unsigneds, src.blocks[i].Unsigneds[srcPos:srcPos+srcLen]...)
		case BlockFloat:
			p.blocks[i].Floats = append(p.blocks[i].Floats, src.blocks[i].Floats[srcPos:srcPos+srcLen]...)
		case BlockString:
			p.blocks[i].Strings = append(p.blocks[i].Strings, src.blocks[i].Strings[srcPos:srcPos+srcLen]...)
		case BlockBytes:
			if safecopy {
				for _, v := range src.blocks[i].Bytes[srcPos : srcPos+srcLen] {
					buf := make([]byte, len(v))
					copy(buf, v)
					p.blocks[i].Bytes = append(p.blocks[i].Bytes, buf)
				}
			} else {
				p.blocks[i].Bytes = append(p.blocks[i].Bytes, src.blocks[i].Bytes[srcPos:srcPos+srcLen]...)
			}
		case BlockBool:
			p.blocks[i].Bools = append(p.blocks[i].Bools, src.blocks[i].Bools[srcPos:srcPos+srcLen]...)
		case BlockTime:
			p.blocks[i].Timestamps = append(p.blocks[i].Timestamps, src.blocks[i].Timestamps[srcPos:srcPos+srcLen]...)
		case BlockIgnore:
		default:
			return fmt.Errorf("pack: invalid data type %d", p.blocks[i].Type)
		}
		p.blocks[i].Dirty = true
	}
	p.nValues += srcLen
	p.dirty = true
	p.pkmap = nil
	return nil
}

func (p *Package) Append() error {
	for i, _ := range p.blocks {
		switch p.blocks[i].Type {
		case BlockInteger:
			p.blocks[i].Integers = append(p.blocks[i].Integers, 0)
		case BlockUnsigned:
			p.blocks[i].Unsigneds = append(p.blocks[i].Unsigneds, 0)
		case BlockFloat:
			p.blocks[i].Floats = append(p.blocks[i].Floats, 0)
		case BlockString:
			p.blocks[i].Strings = append(p.blocks[i].Strings, "")
		case BlockBytes:
			p.blocks[i].Bytes = append(p.blocks[i].Bytes, []byte{})
		case BlockBool:
			p.blocks[i].Bools = append(p.blocks[i].Bools, false)
		case BlockTime:
			p.blocks[i].Timestamps = append(p.blocks[i].Timestamps, 0)
		case BlockIgnore:
		default:
			return fmt.Errorf("pack: invalid data type %d", p.blocks[i].Type)
		}
		p.blocks[i].Dirty = true
	}
	p.nValues++
	p.dirty = true
	p.pkmap = nil
	return nil
}

func (p *Package) Grow(n int) error {
	if n <= 0 {
		return fmt.Errorf("pack: grow requires positive value")
	}
	for i, _ := range p.blocks {
		switch p.blocks[i].Type {
		case BlockInteger:
			p.blocks[i].Integers = append(p.blocks[i].Integers, make([]int64, n)...)
		case BlockUnsigned:
			p.blocks[i].Unsigneds = append(p.blocks[i].Unsigneds, make([]uint64, n)...)
		case BlockFloat:
			p.blocks[i].Floats = append(p.blocks[i].Floats, make([]float64, n)...)
		case BlockString:
			p.blocks[i].Strings = append(p.blocks[i].Strings, make([]string, n)...)
		case BlockBytes:
			p.blocks[i].Bytes = append(p.blocks[i].Bytes, make([][]byte, n)...)
		case BlockBool:
			p.blocks[i].Bools = append(p.blocks[i].Bools, make([]bool, n)...)
		case BlockTime:
			p.blocks[i].Timestamps = append(p.blocks[i].Timestamps, make([]int64, n)...)
		case BlockIgnore:
		default:
			return fmt.Errorf("pack: invalid data type %d", p.blocks[i].Type)
		}
		p.blocks[i].Dirty = true
	}
	p.nValues += n
	p.dirty = true
	p.pkmap = nil
	return nil
}

func (p *Package) Delete(pos, n int) error {
	if n <= 0 {
		return nil
	}
	if p.nValues <= pos {
		return fmt.Errorf("pack: invalid pack offset %d (max %d)", pos, p.nValues)
	}
	n = util.Min(p.Len()-pos, n)
	for i, _ := range p.blocks {
		switch p.blocks[i].Type {
		case BlockInteger:
			p.blocks[i].Integers = append(p.blocks[i].Integers[:pos], p.blocks[i].Integers[pos+n:]...)
		case BlockUnsigned:
			p.blocks[i].Unsigneds = append(p.blocks[i].Unsigneds[:pos], p.blocks[i].Unsigneds[pos+n:]...)
		case BlockFloat:
			p.blocks[i].Floats = append(p.blocks[i].Floats[:pos], p.blocks[i].Floats[pos+n:]...)
		case BlockString:
			for j, l := pos, pos+n; j < l; j++ {
				p.blocks[i].Strings[j] = ""
			}
			p.blocks[i].Strings = append(p.blocks[i].Strings[:pos], p.blocks[i].Strings[pos+n:]...)
		case BlockBytes:
			for j, l := pos, pos+n; j < l; j++ {
				p.blocks[i].Bytes[j] = nil
			}
			p.blocks[i].Bytes = append(p.blocks[i].Bytes[:pos], p.blocks[i].Bytes[pos+n:]...)
		case BlockBool:
			p.blocks[i].Bools = append(p.blocks[i].Bools[:pos], p.blocks[i].Bools[pos+n:]...)
		case BlockTime:
			p.blocks[i].Timestamps = append(p.blocks[i].Timestamps[:pos], p.blocks[i].Timestamps[pos+n:]...)
		case BlockIgnore:
		default:
			return fmt.Errorf("pack: invalid data type %d", p.blocks[i].Type)
		}
		p.blocks[i].Dirty = true
	}
	p.nValues -= n
	p.dirty = true
	p.pkmap = nil
	return nil
}

func (p *Package) Clear() {
	for _, v := range p.blocks {
		v.Clear()
	}
	p.version = packageStorageFormatVersionV1
	p.nValues = 0
	p.pkmap = nil
	p.offsets = nil
	p.dirty = true
	p.cached = false
	p.packedsize = 0
	p.rawsize = 0
}

func (p *Package) Release() {
	for _, v := range p.blocks {
		v.Release()
	}
	p.version = 0
	p.nFields = 0
	p.nValues = 0
	p.offsets = nil
	p.names = nil
	p.blocks = nil
	p.namemap = nil
	p.key = nil
	p.tinfo = nil
	p.pkindex = -1
	p.pkmap = nil
	p.packedsize = 0
	p.rawsize = 0
	p.dirty = false
	p.cached = false
	p.stripped = false
}

func (p *Package) HeapSize() int {
	var sz int
	for _, v := range p.blocks {
		sz += v.HeapSize()
	}
	return sz
}

func (p *Package) PkIndex(id uint64, last int) int {
	if p.pkindex < 0 || p.Len() <= last {
		return -1
	}
	slice := p.blocks[p.pkindex].Unsigneds[last:]
	l := len(slice)
	min, max := slice[0], slice[l-1]
	if id < min || id > max {
		return -1
	}
	if l == int(max-min)+1 {
		return int(id-min) + last
	}
	if p.pkmap != nil {
		idx, ok := p.pkmap[id]
		if ok {
			return idx
		}
		return -1
	}
	idx := sort.Search(l, func(i int) bool { return slice[i] >= id })
	if idx < l && slice[idx] == id {
		return idx + last
	}
	return -1
}

func (p *Package) PkIndexUnsorted(id uint64, last int) int {
	if p.pkindex < 0 || p.Len() <= last {
		return -1
	}
	if p.pkmap != nil {
		idx, ok := p.pkmap[id]
		if ok {
			return idx
		}
		return -1
	}
	slice := p.blocks[p.pkindex].Unsigneds[last:]
	for i, v := range slice {
		if v == id {
			return i + last
		}
	}
	return -1
}

type PackageSorter struct {
	*Package
	col int
}

func (p *PackageSorter) Len() int { return p.Package.Len() }

func (p *PackageSorter) Less(i, j int) bool {
	switch p.Package.blocks[p.col].Type {
	case BlockInteger:
		return p.Package.blocks[p.col].Integers[i] < p.Package.blocks[p.col].Integers[j]
	case BlockUnsigned:
		return p.Package.blocks[p.col].Unsigneds[i] < p.Package.blocks[p.col].Unsigneds[j]
	case BlockFloat:
		return p.Package.blocks[p.col].Floats[i] < p.Package.blocks[p.col].Floats[j]
	case BlockString:
		return p.Package.blocks[p.col].Strings[i] < p.Package.blocks[p.col].Strings[j]
	case BlockBytes:
		return bytes.Compare(p.Package.blocks[p.col].Bytes[i], p.Package.blocks[p.col].Bytes[j]) < 0
	case BlockBool:
		return !p.Package.blocks[p.col].Bools[i] && p.Package.blocks[p.col].Bools[j]
	case BlockTime:
		return p.Package.blocks[p.col].Timestamps[i] < p.Package.blocks[p.col].Timestamps[j]
	case BlockIgnore:
		return true
	default:
		return false
	}
}

func (p *PackageSorter) Swap(i, j int) {
	for n := 0; n < p.Package.nFields; n++ {
		switch p.Package.blocks[n].Type {
		case BlockInteger:
			p.Package.blocks[n].Integers[i], p.Package.blocks[n].Integers[j] =
				p.Package.blocks[n].Integers[j], p.Package.blocks[n].Integers[i]
		case BlockUnsigned:
			p.Package.blocks[n].Unsigneds[i], p.Package.blocks[n].Unsigneds[j] =
				p.Package.blocks[n].Unsigneds[j], p.Package.blocks[n].Unsigneds[i]
		case BlockFloat:
			p.Package.blocks[n].Floats[i], p.Package.blocks[n].Floats[j] =
				p.Package.blocks[n].Floats[j], p.Package.blocks[n].Floats[i]
		case BlockString:
			p.Package.blocks[n].Strings[i], p.Package.blocks[n].Strings[j] =
				p.Package.blocks[n].Strings[j], p.Package.blocks[n].Strings[i]
		case BlockBytes:
			p.Package.blocks[n].Bytes[i], p.Package.blocks[n].Bytes[j] =
				p.Package.blocks[n].Bytes[j], p.Package.blocks[n].Bytes[i]
		case BlockBool:
			p.Package.blocks[n].Bools[i], p.Package.blocks[n].Bools[j] =
				p.Package.blocks[n].Bools[j], p.Package.blocks[n].Bools[i]
		case BlockTime:
			p.Package.blocks[n].Timestamps[i], p.Package.blocks[n].Timestamps[j] =
				p.Package.blocks[n].Timestamps[j], p.Package.blocks[n].Timestamps[i]
		}
	}
}

func (p *Package) PkSort() error {
	if p.pkindex < 0 {
		return fmt.Errorf("pack: missing primary key field")
	}

	if p.Len() == 0 {
		return nil
	}

	spkg := &PackageSorter{Package: p, col: p.pkindex}
	if !sort.IsSorted(spkg) {
		sort.Sort(spkg)
		p.dirty = true
		p.pkmap = nil
	}
	return nil
}
