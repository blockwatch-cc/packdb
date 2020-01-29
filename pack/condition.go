// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// TODO
// - support expressions in fields and condition lists

package pack

import (
	"bytes"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"blockwatch.cc/packdb/util"
	"blockwatch.cc/packdb/vec"
	"github.com/cespare/xxhash"
)

const (
	filterThreshold = 2
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
	hashmap      map[uint64]int
	hashoverflow []hashvalue
	int64map     map[int64]struct{}
	uint64map    map[uint64]struct{}
	numValues    int
}

type hashvalue struct {
	hash uint64
	pos  int
}

func (c Condition) NValues() int {
	return c.numValues
}

func (c Condition) Check() error {
	switch c.Mode {
	case FilterModeRange:
		if c.From == nil || c.To == nil {
			return fmt.Errorf("range condition expects From and To values")
		}
		if err := c.Field.Type.CheckType(c.From); err != nil {
			return err
		}
		if err := c.Field.Type.CheckType(c.To); err != nil {
			return err
		}
	case FilterModeIn, FilterModeNotIn:
		if err := c.Field.Type.CheckSliceType(c.Value); err != nil {
			return err
		}
	case FilterModeRegexp:
		if err := FieldTypeString.CheckType(c.Value); err != nil {
			return err
		}
	default:
		if err := c.Field.Type.CheckType(c.Value); err != nil {
			return err
		}
	}
	return nil
}

func (c *Condition) Compile() {
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
	default:
		return
	}

	switch c.Field.Type {
	case FieldTypeBytes:
		if slice := c.Value.([][]byte); slice != nil {
			c.numValues = len(slice)
			if !c.IsSorted {
				sort.Slice(slice, func(i, j int) bool {
					return bytes.Compare(slice[i], slice[j]) < 0
				})
				c.IsSorted = true
			}
		}
	case FieldTypeString:
		if slice := c.Value.([]string); slice != nil {
			c.numValues = len(slice)
			if !c.IsSorted {
				sort.Slice(slice, func(i, j int) bool {
					return slice[i] < slice[j]
				})
				c.IsSorted = true
			}
		}
	case FieldTypeDatetime:
		if slice := c.Value.([]time.Time); slice != nil {
			c.numValues = len(slice)
			if !c.IsSorted {
				sort.Slice(slice, func(i, j int) bool {
					return slice[i].Before(slice[j])
				})
				c.IsSorted = true
			}
		}
	case FieldTypeBoolean:
		if slice := c.Value.([]bool); slice != nil {
			c.numValues = len(slice)
			if !c.IsSorted {
				sort.Slice(slice, func(i, j int) bool {
					return !slice[i] && slice[j]
				})
				c.IsSorted = true
			}
		}
	case FieldTypeInt64:
		if slice := c.Value.([]int64); slice != nil {
			c.numValues = len(slice)
			if !c.IsSorted {
				sort.Slice(slice, func(i, j int) bool {
					return slice[i] < slice[j]
				})
				c.IsSorted = true
			}
		}
	case FieldTypeUint64:
		if slice := c.Value.([]uint64); slice != nil {
			c.numValues = len(slice)
			if !c.IsSorted {
				sort.Slice(slice, func(i, j int) bool {
					return slice[i] < slice[j]
				})
				c.IsSorted = true
			}
		}
	case FieldTypeFloat64:
		if slice := c.Value.([]float64); slice != nil {
			c.numValues = len(slice)
			if !c.IsSorted {
				sort.Slice(slice, func(i, j int) bool {
					return slice[i] < slice[j]
				})
				c.IsSorted = true
			}
		}
	}

	var vals [][]byte

	switch c.Field.Type {
	case FieldTypeInt64:
		slice := c.Value.([]int64)
		c.int64map = make(map[int64]struct{}, len(slice))
		for _, v := range slice {
			c.int64map[v] = struct{}{}
		}
		return
	case FieldTypeUint64:
		if c.Field.Flags&FlagPrimary == 0 {
			slice := c.Value.([]uint64)
			c.uint64map = make(map[uint64]struct{}, len(slice))
			for _, v := range slice {
				c.uint64map[v] = struct{}{}
			}
		}
		return
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

func (c Condition) MaybeMatchPack(head PackageHeader) bool {
	min, max := head.BlockHeaders[c.Field.Index].MinValue, head.BlockHeaders[c.Field.Index].MaxValue
	switch c.Mode {
	case FilterModeEqual:
		return c.Field.Type.Lte(min, c.Value) && c.Field.Type.Gte(max, c.Value)
	case FilterModeNotEqual:
		return c.Field.Type.Lt(min, c.Value) || c.Field.Type.Gt(max, c.Value)
	case FilterModeRange:
		return !(c.Field.Type.Lt(max, c.From) || c.Field.Type.Gt(min, c.To))
	case FilterModeIn:
		return c.Field.Type.InBetween(c.Value, min, max)
	case FilterModeNotIn:
		return true
	case FilterModeRegexp:
		return true
	case FilterModeGt:
		return c.Field.Type.Gt(min, c.Value) || c.Field.Type.Gt(max, c.Value)
	case FilterModeGte:
		return c.Field.Type.Gte(min, c.Value) || c.Field.Type.Gte(max, c.Value)
	case FilterModeLt:
		return c.Field.Type.Lt(min, c.Value) || c.Field.Type.Lt(max, c.Value)
	case FilterModeLte:
		return c.Field.Type.Lte(min, c.Value) || c.Field.Type.Lte(max, c.Value)
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
		return fmt.Sprintf("%s %s %s [%s]", c.Field.Name, c.Mode.Op(), util.ToString(c.Value), c.Raw)
	}
}

func ParseCondition(key, val string, fields FieldList) (Condition, error) {
	var (
		c    Condition
		f, m string
		err  error
	)
	if ff := strings.Split(key, "."); len(ff) == 2 {
		f, m = ff[0], ff[1]
	} else {
		f = ff[0]
		m = "eq"
	}
	c.Field = fields.Find(f)
	if !c.Field.IsValid() {
		return c, fmt.Errorf("unknown column '%s'", f)
	}
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
		c.From, err = c.Field.Type.ParseAs(vv[0])
		if err != nil {
			return c, fmt.Errorf("error parsing condition value '%s': %v", val, err)
		}
		c.To, err = c.Field.Type.ParseAs(vv[1])
		if err != nil {
			return c, fmt.Errorf("error parsing condition value '%s': %v", val, err)
		}
	case FilterModeIn, FilterModeNotIn:
		c.Value, err = c.Field.Type.ParseSliceAs(val)
		if err != nil {
			return c, fmt.Errorf("error parsing condition value '%s': %v", val, err)
		}
	default:
		c.Value, err = c.Field.Type.ParseAs(val)
		if err != nil {
			return c, fmt.Errorf("error parsing condition value '%s': %v", val, err)
		}
	}
	return c, nil
}

type ConditionList []Condition

func (l *ConditionList) Compile(t *Table) error {
	for i, _ := range *l {
		if err := (*l)[i].Check(); err != nil {
			return fmt.Errorf("cond %d on table field '%s.%s': %v", i, t.name, (*l)[i].Field.Name, err)
		}
		(*l)[i].Compile()
	}
	return nil
}

func (l ConditionList) Fields() FieldList {
	if len(l) == 0 {
		return nil
	}
	fl := make(FieldList, 0, len(l))
	for i, _ := range l {
		fl = fl.AddUnique(l[i].Field)
	}
	return fl
}

func (l ConditionList) MaybeMatchPack(head PackageHeader) bool {
	if head.NValues == 0 {
		return false
	}
	if len(l) == 0 {
		return true
	}
	for i, _ := range l {
		if l[i].MaybeMatchPack(head) {
			continue
		}
		return false
	}
	return true
}

func (l ConditionList) MatchPack(pkg *Package) *vec.BitSet {
	if len(l) == 0 || pkg.Len() == 0 {
		allOnes := vec.NewBitSet(pkg.Len())
		allOnes.One()
		return allOnes
	}
	var bits *vec.BitSet
	for _, c := range l {
		b := c.MatchPack(pkg)
		if bits == nil {
			if b.Count() == 0 {
				return b
			}
			bits = b
			continue
		}
		if b.Count() == 0 {
			bits.Close()
			return b
		}
		bits.And(b)
		b.Close()
	}
	return bits
}

func (c Condition) MatchPack(pkg *Package) *vec.BitSet {
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
				if _, ok := c.int64map[v]; ok {
					bits.Set(i)
				}
			}
		case FieldTypeUint64:
			pk := slice.([]uint64)
			in := c.Value.([]uint64)
			if c.Field.Flags&FlagPrimary > 0 && len(in) > 0 {
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
			} else {
				for i, v := range pk {
					if _, ok := c.uint64map[v]; ok {
						bits.Set(i)
					}
				}
			}
		case FieldTypeBytes:
			vals := c.Value.([][]byte)
			for i, v := range slice.([][]byte) {
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
				if _, ok := c.int64map[v]; !ok {
					bits.Set(i)
				}
			}

		case FieldTypeUint64:
			pk := slice.([]uint64)
			in := c.Value.([]uint64)
			if c.Field.Flags&FlagPrimary > 0 && len(in) > 0 {
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
					if _, ok := c.uint64map[v]; !ok {
						bits.Set(i)
					}
				}
			}

		case FieldTypeBytes:
			vals := c.Value.([][]byte)
			for i, v := range slice.([][]byte) {
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
					if c.Field.Type.In(v, c.Value) {
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

func (l ConditionList) MatchAt(pkg *Package, pos int) bool {
	if len(l) == 0 {
		return true
	}
	if pkg.Len() <= pos {
		return false
	}
	for _, c := range l {
		if !c.MatchAt(pkg, pos) {
			return false
		}
	}
	return true
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
