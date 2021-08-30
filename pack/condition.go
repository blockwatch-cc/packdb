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
}

type UnboundCondition struct {
	Name     string
	Mode     FilterMode
	Raw      string
	Value    interface{}
	From     interface{}
	To       interface{}
	OrKind   bool
	Children []UnboundCondition
}

func (u UnboundCondition) Bind(table *Table) ConditionTreeNode {
	if u.Name != "" {
		return ConditionTreeNode{
			Cond: &Condition{
				Field: table.Fields().Find(u.Name),
				Mode:  u.Mode,
				Raw:   u.Raw,
				Value: u.Value,
				From:  u.From,
				To:    u.To,
			},
		}
	}

	node := ConditionTreeNode{
		OrKind:   u.OrKind,
		Children: make([]ConditionTreeNode, 0),
	}
	for _, v := range u.Children {
		node.Children = append(node.Children, v.Bind(table))
	}
	return node
}

func And(conds ...UnboundCondition) UnboundCondition {
	return UnboundCondition{
		Mode:     FilterModeInvalid,
		OrKind:   COND_AND,
		Children: conds,
	}
}

func Or(conds ...UnboundCondition) UnboundCondition {
	return UnboundCondition{
		Mode:     FilterModeInvalid,
		OrKind:   COND_OR,
		Children: conds,
	}
}

func Equal(field string, val interface{}) UnboundCondition {
	return UnboundCondition{Name: field, Mode: FilterModeEqual, Value: val}
}

func NotEqual(field string, val interface{}) UnboundCondition {
	return UnboundCondition{Name: field, Mode: FilterModeNotEqual, Value: val}
}

func In(field string, value interface{}) UnboundCondition {
	return UnboundCondition{Name: field, Mode: FilterModeIn, Value: value}
}

func NotIn(field string, value interface{}) UnboundCondition {
	return UnboundCondition{Name: field, Mode: FilterModeNotIn, Value: value}
}

func Lt(field string, value interface{}) UnboundCondition {
	return UnboundCondition{Name: field, Mode: FilterModeLt, Value: value}
}

func Lte(field string, value interface{}) UnboundCondition {
	return UnboundCondition{Name: field, Mode: FilterModeLte, Value: value}
}

func Gt(field string, value interface{}) UnboundCondition {
	return UnboundCondition{Name: field, Mode: FilterModeGt, Value: value}
}

func Gte(field string, value interface{}) UnboundCondition {
	return UnboundCondition{Name: field, Mode: FilterModeGte, Value: value}
}

func Regexp(field string, value interface{}) UnboundCondition {
	return UnboundCondition{Name: field, Mode: FilterModeRegexp, Value: value}
}

func Range(field string, from, to interface{}) UnboundCondition {
	return UnboundCondition{Name: field, Mode: FilterModeRange, From: from, To: to}
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
	return
}

func (c Condition) MaybeMatchPack(head PackageHeader) bool {
	min, max := head.BlockHeaders[c.Field.Index].MinValue, head.BlockHeaders[c.Field.Index].MaxValue
	switch c.Mode {
	case FilterModeEqual:
		return c.Field.Type.Lte(min, c.Value) && c.Field.Type.Gte(max, c.Value)
	case FilterModeNotEqual:
		return true
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
		s := fmt.Sprintf("%s %s %s", c.Field.Name, c.Mode.Op(), util.ToString(c.Value))
		if len(c.Raw) > 0 {
			s += " [" + c.Raw + "]"
		}
		return s
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

type ConditionTreeNode struct {
	OrKind   bool
	Children []ConditionTreeNode
	Cond     *Condition
}

func (n ConditionTreeNode) Empty() bool {
	return len(n.Children) == 0 && n.Cond == nil
}

func (n ConditionTreeNode) Leaf() bool {
	return n.Cond != nil
}

func (n ConditionTreeNode) NoMatch() bool {
	if n.Empty() {
		return false
	}

	if n.Leaf() {
		return n.Cond.nomatch
	}

	if n.OrKind {
		for _, v := range n.Children {
			if !v.NoMatch() {
				return false
			}
		}
		return true
	} else {
		for _, v := range n.Children {
			if v.NoMatch() {
				return true
			}
		}
		return false
	}
}

func (n ConditionTreeNode) Compile() error {
	if n.Leaf() {
		if err := n.Cond.Compile(); err != nil {
			return nil
		}
	} else {
		for _, v := range n.Children {
			if err := v.Compile(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (n ConditionTreeNode) Fields() FieldList {
	if n.Empty() {
		return nil
	}
	if n.Leaf() {
		return FieldList{n.Cond.Field}
	}
	fl := make(FieldList, 0)
	for _, v := range n.Children {
		fl = fl.AddUnique(v.Fields()...)
	}
	return fl
}

func (n ConditionTreeNode) Len() int {
	if n.Leaf() {
		return 1
	}
	l := 0
	for _, v := range n.Children {
		l += v.Len()
	}
	return l
}

func (n ConditionTreeNode) Weight() int {
	if n.Leaf() {
		return n.Cond.NValues()
	}
	w := 0
	for _, v := range n.Children {
		w += v.Weight()
	}
	return w
}

func (n ConditionTreeNode) Cost(info PackageHeader) int {
	return n.Weight() * info.NValues
}

func (n ConditionTreeNode) Conditions() []*Condition {
	if n.Leaf() {
		return []*Condition{n.Cond}
	}
	cond := make([]*Condition, 0)
	for _, v := range n.Children {
		cond = append(cond, v.Conditions()...)
	}
	return cond
}

func (n *ConditionTreeNode) AddAndCondition(c *Condition) {
	if n.OrKind || n.Leaf() {
		clone := ConditionTreeNode{
			OrKind:   n.OrKind,
			Children: n.Children,
			Cond:     n.Cond,
		}
		n.OrKind = COND_AND
		n.Children = []ConditionTreeNode{clone}
	}

	n.Children = append(n.Children, ConditionTreeNode{Cond: c})
}

func (n *ConditionTreeNode) AddOrCondition(c *Condition) {
	if !n.OrKind || n.Leaf() {
		clone := ConditionTreeNode{
			OrKind:   n.OrKind,
			Children: n.Children,
			Cond:     n.Cond,
		}
		n.OrKind = COND_OR
		n.Children = []ConditionTreeNode{clone}
	}

	n.Children = append(n.Children, ConditionTreeNode{Cond: c})
}

func (n *ConditionTreeNode) ReplaceNode(node ConditionTreeNode) {
	n.Cond = node.Cond
	n.OrKind = node.OrKind
	n.Children = node.Children
}

func (n *ConditionTreeNode) AddNode(node ConditionTreeNode) {
	if n.Leaf() || (!node.Leaf() && n.OrKind != node.OrKind) {
		clone := ConditionTreeNode{
			OrKind:   n.OrKind,
			Children: n.Children,
			Cond:     n.Cond,
		}
		n.OrKind = node.OrKind
		n.Children = []ConditionTreeNode{clone}
	}

	if node.Leaf() {
		n.Children = append(n.Children, node)
	} else {
		n.Children = append(n.Children, node.Children...)
	}
}

func (n ConditionTreeNode) MaybeMatchPack(info PackageHeader) bool {
	if info.NValues == 0 {
		return false
	}

	if n.Empty() {
		return true
	}

	if n.Leaf() {
		return n.Cond.MaybeMatchPack(info)
	}

	for _, v := range n.Children {
		if n.OrKind {
			if v.MaybeMatchPack(info) {
				return true
			}
		} else {
			if !v.MaybeMatchPack(info) {
				return false
			}
		}
	}

	// no OR nodes match
	if n.OrKind {
		return false
	}
	// all AND nodes match
	return true
}

func (n ConditionTreeNode) MatchPack(pkg *Package, info PackageHeader) *vec.BitSet {
	if n.Leaf() {
		return n.Cond.MatchPack(pkg, nil)
	}

	if n.Empty() {
		return vec.NewBitSet(pkg.Len()).One()
	}

	if n.OrKind {
		return n.MatchPackOr(pkg, info)
	} else {
		return n.MatchPackAnd(pkg, info)
	}
}

func (n ConditionTreeNode) MatchPackAnd(pkg *Package, info PackageHeader) *vec.BitSet {
	bits := vec.NewBitSet(pkg.Len()).One()

	for _, cn := range n.Children {
		var b *vec.BitSet
		if !cn.Leaf() {
			b = cn.MatchPack(pkg, info)
		} else {
			c := cn.Cond
			if !pkg.IsJournal() && len(info.BlockHeaders) > c.Field.Index {
				min, max := info.BlockHeaders[c.Field.Index].MinValue, info.BlockHeaders[c.Field.Index].MaxValue
				switch c.Mode {
				case FilterModeEqual:
					if c.Field.Type.Equal(min, c.Value) && c.Field.Type.Equal(max, c.Value) {
						continue
					}
				case FilterModeNotEqual:
					if c.Field.Type.Lt(c.Value, min) || c.Field.Type.Gt(c.Value, max) {
						continue
					}
				case FilterModeRange:
					if c.Field.Type.Lte(c.From, min) && c.Field.Type.Gte(c.To, max) {
						continue
					}
				case FilterModeGt:
					if c.Field.Type.Gt(min, c.Value) {
						continue
					}
				case FilterModeGte:
					if c.Field.Type.Gte(min, c.Value) {
						continue
					}
				case FilterModeLt:
					if c.Field.Type.Lt(max, c.Value) {
						continue
					}
				case FilterModeLte:
					if c.Field.Type.Lte(max, c.Value) {
						continue
					}
				}
			}
			b = c.MatchPack(pkg, bits)
		}

		if bits.Count() == int64(bits.Size()) {
			bits.Close()
			bits = b
			continue
		}

		bits.And(b)
		b.Close()

		if bits.Count() == 0 {
			break
		}
	}
	return bits
}

func (n ConditionTreeNode) MatchPackOr(pkg *Package, info PackageHeader) *vec.BitSet {
	bits := vec.NewBitSet(pkg.Len())
	for _, cn := range n.Children {
		var b *vec.BitSet
		if !cn.Leaf() {
			b = cn.MatchPack(pkg, info)
		} else {
			c := cn.Cond
			if !pkg.IsJournal() && len(info.BlockHeaders) > c.Field.Index {
				min, max := info.BlockHeaders[c.Field.Index].MinValue, info.BlockHeaders[c.Field.Index].MaxValue
				skipEarly := false
				switch c.Mode {
				case FilterModeEqual:
					if c.Field.Type.Equal(min, c.Value) && c.Field.Type.Equal(max, c.Value) {
						skipEarly = true
					}
				case FilterModeNotEqual:
					if c.Field.Type.Lt(c.Value, min) || c.Field.Type.Gt(c.Value, max) {
						skipEarly = true
					}
				case FilterModeRange:
					if c.Field.Type.Lte(c.From, min) && c.Field.Type.Gte(c.To, max) {
						skipEarly = true
					}
				case FilterModeGt:
					if c.Field.Type.Gt(min, c.Value) {
						skipEarly = true
					}
				case FilterModeGte:
					if c.Field.Type.Gte(min, c.Value) {
						skipEarly = true
					}
				case FilterModeLt:
					if c.Field.Type.Lt(max, c.Value) {
						skipEarly = true
					}
				case FilterModeLte:
					if c.Field.Type.Lte(max, c.Value) {
						skipEarly = true
					}
				}
				if skipEarly {
					bits.Close()
					return vec.NewBitSet(pkg.Len()).One()
				}
			}
			b = c.MatchPack(pkg, bits)
		}

		if b.Count() == 0 {
			b.Close()
			continue
		}

		bits.Or(b)
		b.Close()

		if bits.Count() == int64(bits.Size()) {
			break
		}
	}
	return bits
}

func (n ConditionTreeNode) MatchAt(pkg *Package, pos int) bool {
	if n.Leaf() {
		return n.Cond.MatchAt(pkg, pos)
	}

	if n.Empty() {
		return true
	}

	if n.OrKind {
		for _, c := range n.Children {
			if c.MatchAt(pkg, pos) {
				return true
			}
		}
	} else {
		for _, c := range n.Children {
			if !c.MatchAt(pkg, pos) {
				return false
			}
		}
	}
	return true
}
