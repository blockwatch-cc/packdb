// Copyright (c) 2018-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// TODO
// - support expressions in fields and condition lists

package pack

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"time"

	"blockwatch.cc/packdb/filter/bloom"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/packdb/vec"
	"github.com/cespare/xxhash"
)

const (
	filterThreshold = 2
	COND_OR         = true
	COND_AND        = false
)

type Condition struct {
	Field    Field
	Mode     FilterMode
	Raw      string
	Value    interface{}
	From     interface{}
	To       interface{}
	IsSorted bool

	processed    bool
	nomatch      bool
	hashmap      map[uint64]int
	hashoverflow []hashvalue
	int64map     map[int64]struct{}
	uint64map    map[uint64]struct{}
	numValues    int
	bloomHashes  [][2]uint64
}

type hashvalue struct {
	hash uint64
	pos  int
}

func (c Condition) NValues() int {
	return c.numValues
}

func (c *Condition) EnsureTypes() error {
	var err error
	switch c.Mode {
	case FilterModeRange:
		if c.From == nil || c.To == nil {
			return fmt.Errorf("range condition expects From and To values")
		}
		if c.From, err = c.Field.Type.CastType(c.From, c.Field); err != nil {
			return err
		}
		if c.To, err = c.Field.Type.CastType(c.To, c.Field); err != nil {
			return err
		}
		if c.Field.Type.Gt(c.From, c.To) {
			return fmt.Errorf("range condition mismatch: from > to")
		}
	case FilterModeIn, FilterModeNotIn:
		if c.Value, err = c.Field.Type.CastSliceType(c.Value, c.Field); err != nil {
			return err
		}
	case FilterModeRegexp:
		if err := FieldTypeString.CheckType(c.Value); err != nil {
			return err
		}
	default:
		if c.Value, err = c.Field.Type.CastType(c.Value, c.Field); err != nil {
			return err
		}
	}
	return nil
}

func (c *Condition) sortValueSlice() {
	if c.IsSorted {
		return
	}
	switch c.Field.Type {
	case FieldTypeBytes:
		if slice := c.Value.([][]byte); slice != nil {
			c.numValues = len(slice)
			sort.Slice(slice, func(i, j int) bool {
				return bytes.Compare(slice[i], slice[j]) < 0
			})
		}
	case FieldTypeString:
		if slice := c.Value.([]string); slice != nil {
			c.numValues = len(slice)
			sort.Slice(slice, func(i, j int) bool {
				return slice[i] < slice[j]
			})
		}
	case FieldTypeDatetime:
		if slice := c.Value.([]time.Time); slice != nil {
			c.numValues = len(slice)
			sort.Slice(slice, func(i, j int) bool {
				return slice[i].Before(slice[j])
			})
		}
	case FieldTypeBoolean:
		if slice := c.Value.([]bool); slice != nil {
			c.numValues = len(slice)
			sort.Slice(slice, func(i, j int) bool {
				return !slice[i] && slice[j]
			})
		}
	case FieldTypeInt64:
		if slice := c.Value.([]int64); slice != nil {
			c.numValues = len(slice)
			sort.Slice(slice, func(i, j int) bool {
				return slice[i] < slice[j]
			})
		}
	case FieldTypeUint64:
		if slice := c.Value.([]uint64); slice != nil {
			c.numValues = len(slice)
			sort.Slice(slice, func(i, j int) bool {
				return slice[i] < slice[j]
			})
		}
	case FieldTypeFloat64:
		if slice := c.Value.([]float64); slice != nil {
			c.numValues = len(slice)
			sort.Slice(slice, func(i, j int) bool {
				return slice[i] < slice[j]
			})
		}
	}
	c.IsSorted = true
}

func (c *Condition) buildValueMap() {
	switch c.Field.Type {
	case FieldTypeInt64:
		slice := c.Value.([]int64)
		c.int64map = make(map[int64]struct{}, len(slice))
		for _, v := range slice {
			c.int64map[v] = struct{}{}
		}
	case FieldTypeUint64:
		if c.Field.Flags&FlagPrimary == 0 {
			slice := c.Value.([]uint64)
			c.uint64map = make(map[uint64]struct{}, len(slice))
			for _, v := range slice {
				c.uint64map[v] = struct{}{}
			}
		}
	}
}

func (c *Condition) buildHashMap() {
	var vals [][]byte
	switch c.Field.Type {
	case FieldTypeBytes:
		vals = c.Value.([][]byte)
		if vals == nil {
			return
		}
	case FieldTypeString:
		strs := c.Value.([]string)
		if strs == nil {
			return
		}
		vals = make([][]byte, len(strs))
		for i, v := range strs {
			vals[i] = []byte(v)
		}
	default:
		return
	}

	if len(vals) < filterThreshold {
		return
	}

	c.hashmap = make(map[uint64]int)
	for i, v := range vals {
		sum := xxhash.Sum64(v)
		if mapval, ok := c.hashmap[sum]; !ok {
			c.hashmap[sum] = i
		} else {
			if mapval != 0xFFFFFFFF {
				log.Warnf("pack: condition hash collision %0x / %0x == %0x", v, vals[mapval], sum)
				c.hashoverflow = append(c.hashoverflow, hashvalue{
					hash: sum,
					pos:  mapval,
				})
			} else {
				log.Warnf("pack: condition double hash collision %0x == %0x", v, sum)
			}
			c.hashoverflow = append(c.hashoverflow, hashvalue{
				hash: sum,
				pos:  i,
			})
			c.hashmap[sum] = 0xFFFFFFFF
		}
	}
}

func (c *Condition) buildBloomData() {
	if !c.Field.Flags.Contains(FlagBloom) {
		return
	}
	c.bloomHashes = make([][2]uint64, 0)
	var buf [8]byte
	switch c.Field.Type {
	case FieldTypeBytes:
		for _, val := range c.Value.([][]byte) {
			c.bloomHashes = append(c.bloomHashes, bloom.Hash(val))
		}
	case FieldTypeString:
		for _, val := range c.Value.([]string) {
			c.bloomHashes = append(c.bloomHashes, bloom.Hash([]byte(val)))
		}
	case FieldTypeInt64:
		for _, val := range c.Value.([]int64) {
			bigEndian.PutUint64(buf[:], uint64(val))
			c.bloomHashes = append(c.bloomHashes, bloom.Hash(buf[:]))
		}
	case FieldTypeUint64:
		for _, val := range c.Value.([]uint64) {
			bigEndian.PutUint64(buf[:], val)
			c.bloomHashes = append(c.bloomHashes, bloom.Hash(buf[:]))
		}
	case FieldTypeFloat64:
		for _, val := range c.Value.([]float64) {
			bigEndian.PutUint64(buf[:], math.Float64bits(val))
			c.bloomHashes = append(c.bloomHashes, bloom.Hash(buf[:]))
		}
	}
}

func (c *Condition) Compile() (err error) {
	if !c.Field.IsValid() {
		err = fmt.Errorf("invalid field in cond %s", c.String())
		return
	}
	if err = c.EnsureTypes(); err != nil {
		err = fmt.Errorf("%s cond %s: %v", c.Field.Name, c.String(), err)
		return
	}

	if c.Value != nil {
		c.numValues++
	}
	if c.From != nil {
		c.numValues++
	}
	if c.To != nil {
		c.numValues++
	}

	switch c.Mode {
	case FilterModeIn, FilterModeNotIn:
		c.sortValueSlice()
		c.buildValueMap()
		c.buildHashMap()
		c.buildBloomData()
	default:
		if c.Field.Flags.Contains(FlagBloom) {
			c.bloomHashes = [][2]uint64{bloom.Hash(c.Field.Type.Bytes(c.Value))}
		}
	}
	return nil
}

func (c Condition) MaybeMatchPack(info PackInfo) bool {
	idx := c.Field.Index
	min := info.Blocks[idx].MinValue
	max := info.Blocks[idx].MaxValue
	filter := info.Blocks[idx].Bloom
	typ := c.Field.Type
	switch c.Mode {
	case FilterModeEqual:
		res := typ.Between(c.Value, min, max)
		if res && filter != nil {
			return filter.ContainsHash(c.bloomHashes[0])
		}
		return res
	case FilterModeNotEqual:
		return true
	case FilterModeRange:
		return !(typ.Lt(max, c.From) || typ.Gt(min, c.To))
	case FilterModeIn:
		res := typ.InBetween(c.Value, min, max)
		if res && filter != nil {
			return filter.ContainsAnyHash(c.bloomHashes)
		}
		return res
	case FilterModeNotIn:
		return true
	case FilterModeRegexp:
		return true
	case FilterModeGt:
		return typ.Gt(min, c.Value) || typ.Gt(max, c.Value)
	case FilterModeGte:
		return typ.Gte(min, c.Value) || typ.Gte(max, c.Value)
	case FilterModeLt:
		return typ.Lt(min, c.Value) || typ.Lt(max, c.Value)
	case FilterModeLte:
		return typ.Lte(min, c.Value) || typ.Lte(max, c.Value)
	default:
		return false
	}
}

func (c Condition) String() string {
	switch c.Mode {
	case FilterModeRange:
		return fmt.Sprintf("%s %s [%s, %s]", c.Field.Name, c.Mode.Op(),
			util.ToString(c.From), util.ToString(c.To))
	case FilterModeIn, FilterModeNotIn:
		size := c.numValues
		if size == 0 {
			size = reflect.ValueOf(c.Value).Len()
		}
		if size > 16 {
			return fmt.Sprintf("%s %s [%d values]", c.Field.Name, c.Mode.Op(), size)
		} else {
			return fmt.Sprintf("%s %s [%v]", c.Field.Name, c.Mode.Op(), c.Field.Type.ToString(c.Value))
		}
	default:
		s := fmt.Sprintf("%s %s %s", c.Field.Name, c.Mode.Op(), util.ToString(c.Value))
		if len(c.Raw) > 0 {
			s += " [" + c.Raw + "]"
		}
		return s
	}
}

func ParseCondition(key, val string, fields FieldList) (UnboundCondition, error) {
	var (
		c    UnboundCondition
		f, m string
		err  error
	)
	if ff := strings.Split(key, "."); len(ff) == 2 {
		f, m = ff[0], ff[1]
	} else {
		f = ff[0]
		m = "eq"
	}
	field := fields.Find(f)
	if !field.IsValid() {
		return c, fmt.Errorf("unknown column '%s'", f)
	}
	c.Name = field.Name
	c.Mode = ParseFilterMode(m)
	if !c.Mode.IsValid() {
		return c, fmt.Errorf("invalid filter mode '%s'", m)
	}
	c.Raw = val
	switch c.Mode {
	case FilterModeRange:
		vv := strings.Split(val, ",")
		if len(vv) != 2 {
			return c, fmt.Errorf("range conditions require exactly two arguments")
		}
		c.From, err = field.Type.ParseAs(vv[0])
		if err != nil {
			return c, fmt.Errorf("error parsing condition value '%s': %v", val, err)
		}
		c.To, err = field.Type.ParseAs(vv[1])
		if err != nil {
			return c, fmt.Errorf("error parsing condition value '%s': %v", val, err)
		}
	case FilterModeIn, FilterModeNotIn:
		c.Value, err = field.Type.ParseSliceAs(val)
		if err != nil {
			return c, fmt.Errorf("error parsing condition value '%s': %v", val, err)
		}
	default:
		c.Value, err = field.Type.ParseAs(val)
		if err != nil {
			return c, fmt.Errorf("error parsing condition value '%s': %v", val, err)
		}
	}
	return c, nil
}

func (c Condition) MatchPack(pkg *Package, mask *vec.BitSet) *vec.BitSet {
	bits := vec.NewBitSet(pkg.Len())
	slice, _ := pkg.Column(c.Field.Index)
	switch c.Mode {
	case FilterModeEqual:
		return c.Field.Type.EqualSlice(slice, c.Value, bits)
	case FilterModeNotEqual:
		return c.Field.Type.NotEqualSlice(slice, c.Value, bits)
	case FilterModeGt:
		return c.Field.Type.GtSlice(slice, c.Value, bits)
	case FilterModeGte:
		return c.Field.Type.GteSlice(slice, c.Value, bits)
	case FilterModeLt:
		return c.Field.Type.LtSlice(slice, c.Value, bits)
	case FilterModeLte:
		return c.Field.Type.LteSlice(slice, c.Value, bits)
	case FilterModeRange:
		return c.Field.Type.BetweenSlice(slice, c.From, c.To, bits)
	case FilterModeRegexp:
		return c.Field.Type.RegexpSlice(slice, c.Value.(string), bits)
	case FilterModeIn:
		switch c.Field.Type {
		case FieldTypeInt64:
			for i, v := range slice.([]int64) {
				// skip masked values
				if mask != nil && !mask.IsSet(i) {
					continue
				}
				if _, ok := c.int64map[v]; ok {
					bits.Set(i)
				}
			}
		case FieldTypeUint64:
			pk := slice.([]uint64)
			in := c.Value.([]uint64)
			if pkg.IsJournal() && c.uint64map == nil {
				c.uint64map = make(map[uint64]struct{}, len(in))
				for _, v := range in {
					c.uint64map[v] = struct{}{}
				}
			}
			if !pkg.IsJournal() && c.Field.Flags&FlagPrimary > 0 && len(in) > 0 {
				maxin := in[len(in)-1]
				maxpk := pk[len(pk)-1]
				for i, p, il, pl := 0, 0, len(in), len(pk); i < il && p < pl; {
					if pk[p] > maxin || maxpk < in[i] {
						break
					}
					for p < pl && pk[p] < in[i] {
						p++
					}
					if p == pl {
						break
					}
					for i < il && pk[p] > in[i] {
						i++
					}
					if i == il {
						break
					}
					if pk[p] == in[i] {
						// blend masked values
						if mask == nil || mask.IsSet(p) {
							bits.Set(p)
						}
						i++
					}
				}
			} else {
				for i, v := range pk {
					// skip masked values
					if mask != nil && !mask.IsSet(i) {
						continue
					}
					if _, ok := c.uint64map[v]; ok {
						bits.Set(i)
					}
				}
			}
		case FieldTypeBytes:
			vals := c.Value.([][]byte)
			for i, v := range slice.([][]byte) {
				// skip masked values
				if mask != nil && !mask.IsSet(i) {
					continue
				}
				if c.hashmap != nil {
					sum := xxhash.Sum64(v)
					if pos, ok := c.hashmap[sum]; ok {
						if pos != 0xFFFFFFFF {
							if bytes.Compare(v, vals[pos]) == 0 {
								bits.Set(i)
							}
						} else {
							for _, oflow := range c.hashoverflow {
								if oflow.hash != sum {
									continue
								}
								if bytes.Compare(v, vals[oflow.pos]) != 0 {
									continue
								}
								bits.Set(i)
								break
							}
						}
					}
				} else {
					if c.Field.Type.In(v, c.Value) {
						bits.Set(i)
					}
				}
			}

		case FieldTypeString:
			strs := c.Value.([]string)
			for i, v := range slice.([]string) {
				// skip masked values
				if mask != nil && !mask.IsSet(i) {
					continue
				}
				if c.hashmap != nil {
					sum := xxhash.Sum64([]byte(v))
					if pos, ok := c.hashmap[sum]; ok {
						if pos != 0xFFFFFFFF {
							if strings.Compare(v, strs[pos]) == 0 {
								bits.Set(i)
							}
						} else {
							for _, oflow := range c.hashoverflow {
								if oflow.hash != sum {
									continue
								}
								if strings.Compare(v, strs[oflow.pos]) != 0 {
									continue
								}
								bits.Set(i)
								break
							}
						}
					}
				} else {
					if c.Field.Type.In(v, c.Value) {
						bits.Set(i)
					}
				}
			}
		}

		return bits

	case FilterModeNotIn:
		switch c.Field.Type {
		case FieldTypeInt64:
			for i, v := range slice.([]int64) {
				// skip masked values
				if mask != nil && !mask.IsSet(i) {
					continue
				}
				if _, ok := c.int64map[v]; !ok {
					bits.Set(i)
				}
			}

		case FieldTypeUint64:
			pk := slice.([]uint64)
			in := c.Value.([]uint64)
			if pkg.IsJournal() && c.uint64map == nil {
				c.uint64map = make(map[uint64]struct{}, len(in))
				for _, v := range in {
					c.uint64map[v] = struct{}{}
				}
			}
			if !pkg.IsJournal() && c.Field.Flags&FlagPrimary > 0 && len(in) > 0 {
				maxin := in[len(in)-1]
				maxpk := pk[len(pk)-1]
				for i, p, il, pl := 0, 0, len(in), len(pk); i < il && p < pl; {
					if pk[p] > maxin || maxpk < in[i] {
						break
					}
					for p < pl && pk[p] < in[i] {
						p++
					}
					if p == pl {
						break
					}
					for i < il && pk[p] > in[i] {
						i++
					}
					if i == il {
						break
					}
					if pk[p] == in[i] {
						bits.Set(p)
						i++
					}
				}
				bits.Neg()
			} else {
				for i, v := range pk {
					// skip masked values
					if mask != nil && !mask.IsSet(i) {
						continue
					}
					if _, ok := c.uint64map[v]; !ok {
						bits.Set(i)
					}
				}
			}

		case FieldTypeBytes:
			vals := c.Value.([][]byte)
			for i, v := range slice.([][]byte) {
				// skip masked values
				if mask != nil && !mask.IsSet(i) {
					continue
				}
				if c.hashmap != nil {
					sum := xxhash.Sum64(v)
					if pos, ok := c.hashmap[sum]; !ok {
						bits.Set(i)
					} else {
						if pos != 0xFFFFFFFF {
							if bytes.Compare(v, vals[pos]) != 0 {
								bits.Set(i)
							}
						} else {
							var found bool
							for _, oflow := range c.hashoverflow {
								if oflow.hash != sum {
									continue
								}
								if bytes.Compare(v, vals[oflow.pos]) == 0 {
									found = true
									break
								}
							}
							if !found {
								bits.Set(i)
							}
						}
					}
				} else {
					if !c.Field.Type.In(v, c.Value) {
						bits.Set(i)
					}
				}
			}

		case FieldTypeString:
			strs := c.Value.([]string)
			for i, v := range slice.([]string) {
				// skip masked values
				if mask != nil && !mask.IsSet(i) {
					continue
				}
				if c.hashmap != nil {
					sum := xxhash.Sum64([]byte(v))
					if pos, ok := c.hashmap[sum]; !ok {
						bits.Set(i)
					} else {
						if pos != 0xFFFFFFFF {
							if strings.Compare(v, strs[pos]) != 0 {
								bits.Set(i)
							}
						} else {
							var found bool
							for _, oflow := range c.hashoverflow {
								if oflow.hash != sum {
									continue
								}
								if strings.Compare(v, strs[oflow.pos]) == 0 {
									found = true
									break
								}
							}
							if !found {
								bits.Set(i)
							}
						}
					}
				} else {
					if !c.Field.Type.In(v, c.Value) {
						bits.Set(i)
					}
				}
			}
		}
		return bits
	default:
		return bits
	}
}

func (c Condition) MatchAt(pkg *Package, pos int) bool {
	index := c.Field.Index
	switch c.Mode {
	case FilterModeEqual:
		return c.Field.Type.EqualAt(pkg, index, pos, c.Value)
	case FilterModeNotEqual:
		return !c.Field.Type.EqualAt(pkg, index, pos, c.Value)
	case FilterModeRange:
		return c.Field.Type.BetweenAt(pkg, index, pos, c.From, c.To)
	case FilterModeIn:
		switch c.Field.Type {
		case FieldTypeInt64:
			val, _ := pkg.Int64At(index, pos)
			_, ok := c.int64map[val]
			return ok
		case FieldTypeUint64:
			val, _ := pkg.Uint64At(index, pos)
			_, ok := c.uint64map[val]
			return ok
		}

		var buf []byte
		if c.Field.Type == FieldTypeBytes {
			buf, _ = pkg.BytesAt(index, pos)
		} else if c.Field.Type == FieldTypeString {
			str, _ := pkg.StringAt(index, pos)
			buf = []byte(str)
		}
		if buf != nil && c.hashmap != nil {
			if _, ok := c.hashmap[xxhash.Sum64(buf)]; !ok {
				return false
			}
		}
		return c.Field.Type.InAt(pkg, index, pos, c.Value)

	case FilterModeNotIn:
		switch c.Field.Type {
		case FieldTypeInt64:
			val, _ := pkg.Int64At(index, pos)
			_, ok := c.int64map[val]
			return !ok
		case FieldTypeUint64:
			val, _ := pkg.Uint64At(index, pos)
			_, ok := c.uint64map[val]
			return !ok
		}

		var buf []byte
		if c.Field.Type == FieldTypeBytes {
			buf, _ = pkg.BytesAt(index, pos)
		} else if c.Field.Type == FieldTypeString {
			str, _ := pkg.StringAt(index, pos)
			buf = []byte(str)
		}
		if buf != nil && c.hashmap != nil {
			if _, ok := c.hashmap[xxhash.Sum64(buf)]; !ok {
				return true
			}
		}
		return !c.Field.Type.InAt(pkg, index, pos, c.Value)

	case FilterModeRegexp:
		return c.Field.Type.RegexpAt(pkg, index, pos, c.Value.(string))
	case FilterModeGt:
		return c.Field.Type.GtAt(pkg, index, pos, c.Value)
	case FilterModeGte:
		return c.Field.Type.GteAt(pkg, index, pos, c.Value)
	case FilterModeLt:
		return c.Field.Type.LtAt(pkg, index, pos, c.Value)
	case FilterModeLte:
		return c.Field.Type.LteAt(pkg, index, pos, c.Value)
	default:
		return false
	}
}
