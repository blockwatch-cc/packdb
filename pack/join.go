// Copyright (c) 2018-2019 KIDTSUNAMI
// Author: alex@kidtsunami.com

// TODO
// - complex predicates "JOIN ON a.f = b.f AND a.id = b.id"
// - scalar predicates  "JOIN ON a.f = 42"
// - GetTotalRowCount() for all join types

package pack

import (
	"context"
	"fmt"
	"strings"

	"blockwatch.cc/packdb/util"
)

type JoinType int

const (
	InnerJoin  JoinType = iota // INNER JOIN (maybe EQUI JOIN)
	LeftJoin                   // LEFT OUTER JOIN
	RightJoin                  // RIGHT OUTER JOIN
	FullJoin                   // FULL OUTER JOIN
	CrossJoin                  // CROSS JOIN
	SelfJoin                   // unused
	AsOfJoin                   // see https://code.kx.com/q4m3/9_Queries_q-sql/#998-as-of-joins
	WindowJoin                 // see https://code.kx.com/q4m3/9_Queries_q-sql/#999-window-join
)

func (t JoinType) String() string {
	switch t {
	case InnerJoin:
		return "inner_join"
	case LeftJoin:
		return "left_join"
	case RightJoin:
		return "right_join"
	case FullJoin:
		return "full_join"
	case CrossJoin:
		return "cross_join"
	case SelfJoin:
		return "self_join"
	case AsOfJoin:
		return "as_of_join"
	case WindowJoin:
		return "window_join"
	default:
		return "invalid_join"
	}
}

type JoinTable struct {
	Table    *Table
	Where    ConditionList
	Fields   FieldList
	FieldsAs util.StringList
	Limit    int
}

type Join struct {
	Type      JoinType
	Predicate BinaryCondition
	Left      JoinTable
	Right     JoinTable

	limit   int
	fields  FieldList
	aliases map[string]string
}

func (j Join) Fields() FieldList {
	return j.fields
}

func (j Join) IsEquiJoin() bool {
	return j.Predicate.Mode == FilterModeEqual
}

func (j Join) Check() error {
	if j.Type < InnerJoin || j.Type > WindowJoin {
		return fmt.Errorf("pack: invalid join type %d", j.Type)
	}

	if err := j.Predicate.Check(); err != nil {
		return err
	}

	if j.Left.Table == nil {
		return fmt.Errorf("pack: left join table is nil")
	}
	if j.Right.Table == nil {
		return fmt.Errorf("pack: right join table is nil")
	}

	if f, a := len(j.Left.Fields), len(j.Left.FieldsAs); f != a && a != 0 {
		return fmt.Errorf("pack: left join table has %d fields and %d alias names", f, a)
	}
	if f, a := len(j.Right.Fields), len(j.Right.FieldsAs); f != a && a != 0 {
		return fmt.Errorf("pack: right join table has %d fields and %d alias names", f, a)
	}

	lfields := j.Left.Table.Fields()
	lname := j.Left.Table.Name()
	rfields := j.Right.Table.Fields()
	rname := j.Right.Table.Name()
	if !lfields.Contains(j.Predicate.Left.Name) {
		return fmt.Errorf("pack: missing predicate field '%s' in left table '%s'",
			j.Predicate.Left.Name, lname)
	}
	if !rfields.Contains(j.Predicate.Right.Name) {
		return fmt.Errorf("pack: missing predicate field '%s' in right table '%s'",
			j.Predicate.Right.Name, rname)
	}

	for _, v := range j.Left.Fields {
		if !lfields.Contains(v.Name) {
			return fmt.Errorf("pack: undefined field '%s.%s' used in join output",
				lname, v.Name)
		}
		if lfields.Find(v.Name).Type != v.Type {
			return fmt.Errorf("pack: mismatched type %s for field '%s.%s' used in join output",
				v.Type, lname, v.Name)
		}
		if v.Index < 0 || v.Index >= len(lfields) {
			return fmt.Errorf("pack: illegal index %d for field '%s.%s' used in join output",
				v.Index, lname, v.Name)
		}
	}

	for _, v := range j.Right.Fields {
		if !rfields.Contains(v.Name) {
			return fmt.Errorf("pack: undefined field '%s.%s' used in join output",
				rname, v.Name)
		}
		if rfields.Find(v.Name).Type != v.Type {
			return fmt.Errorf("pack: mismatched type %s for field '%s.%s' used in join output",
				v.Type, rname, v.Name)
		}
		if v.Index < 0 || v.Index >= len(lfields) {
			return fmt.Errorf("pack: illegal index %d for field '%s.%s' used in join output",
				v.Index, rname, v.Name)
		}
	}

	for i, c := range j.Left.Where {
		if err := j.Left.Where[i].Check(); err != nil {
			return fmt.Errorf("pack: invalid cond %d in join field '%s.%s': %v",
				i, lname, c.Field.Name, err)
		}
		if !lfields.Contains(c.Field.Name) {
			return fmt.Errorf("pack: undefined field '%s.%s' used in join cond %d",
				lname, c.Field.Name, i)
		}
		if lfields.Find(c.Field.Name).Type != c.Field.Type {
			return fmt.Errorf("pack: mismatched type %s for field '%s.%s' used in join cond %d",
				c.Field.Type, lname, c.Field.Name, i)
		}
		if c.Field.Index < 0 || c.Field.Index >= len(lfields) {
			return fmt.Errorf("pack: illegal index %d for field '%s.%s' used in join cond %d",
				c.Field.Index, lname, c.Field.Name, i)
		}
	}

	for i, c := range j.Right.Where {
		if err := j.Right.Where[i].Check(); err != nil {
			return fmt.Errorf("pack: invalid cond %d in join field '%s.%s': %v",
				i, rname, c.Field.Name, err)
		}
		if !rfields.Contains(c.Field.Name) {
			return fmt.Errorf("pack: undefined field '%s.%s' used in join cond %d",
				rname, c.Field.Name, i)
		}
		if rfields.Find(c.Field.Name).Type != c.Field.Type {
			return fmt.Errorf("pack: mismatched type %s for field '%s.%s' used in join cond %d",
				c.Field.Type, rname, c.Field.Name, i)
		}
		if c.Field.Index < 0 || c.Field.Index >= len(rfields) {
			return fmt.Errorf("pack: illegal index %d for field '%s.%s' used in join cond %d",
				c.Field.Index, rname, c.Field.Name, i)
		}
	}
	return nil
}

func (j *Join) Compile() error {
	if len(j.fields) > 0 {
		return nil
	}

	if err := j.Check(); err != nil {
		return err
	}

	j.aliases = make(map[string]string)

	if len(j.Left.Fields) == 0 {
		j.Left.Fields = j.Left.Table.Fields()
	}
	if len(j.Right.Fields) == 0 {
		j.Right.Fields = j.Right.Table.Fields()
	}

	for i, v := range j.Left.Fields {
		joinname := j.Left.Table.Name() + "." + v.Name
		alias := joinname
		if len(j.Left.FieldsAs) > i+1 {
			alias = j.Left.FieldsAs[i]
		}
		j.aliases[joinname] = alias
		j.fields = append(j.fields, Field{
			Index: len(j.fields),
			Name:  joinname,
			Alias: alias,
			Type:  v.Type,
			Flags: 0,
		})
	}

	for i, v := range j.Right.Fields {
		joinname := j.Right.Table.Name() + "." + v.Name
		alias := joinname
		if len(j.Right.FieldsAs) > i+1 {
			alias = j.Right.FieldsAs[i]
		}
		j.aliases[joinname] = alias
		j.fields = append(j.fields, Field{
			Index: len(j.fields),
			Name:  joinname,
			Alias: alias,
			Type:  v.Type,
			Flags: 0,
		})
	}
	return nil
}

func (j Join) AppendResult(out, left *Package, l int, right *Package, r int) error {
	if err := out.Grow(1); err != nil {
		return err
	}
	ins := out.Len() - 1
	if left != nil {
		for i, v := range j.Left.Fields {
			f, err := left.FieldAt(v.Index, l)
			if err != nil {
				return err
			}
			if err := out.SetFieldAt(i, ins, f); err != nil {
				return err
			}
		}
	}
	offs := len(j.Left.Fields)
	if right != nil {
		for i, v := range j.Right.Fields {
			f, err := right.FieldAt(v.Index, r)
			if err != nil {
				return err
			}
			if err := out.SetFieldAt(i+offs, ins, f); err != nil {
				return err
			}
		}
	}
	return nil
}

// TODO
func (j Join) Stream(ctx context.Context, q Query, fn func(r Row) error) error {
	return nil
}

func (j Join) Query(ctx context.Context, q Query) (*Result, error) {
	// ------------------------------------------------------------
	// PREPARE
	// ------------------------------------------------------------
	if err := j.Compile(); err != nil {
		return nil, err
	}

	if err := q.Compile(&Table{
		name: strings.Join([]string{
			j.Type.String(),
			j.Left.Table.Name(),
			j.Right.Table.Name(),
			"on",
			j.Predicate.Left.Name,
			j.Predicate.Mode.String(),
			j.Predicate.Right.Name,
		}, "."),
		fields: j.fields,
	}); err != nil {
		return nil, err
	}
	defer q.Close()

	havePostFilter := len(q.Conditions) > 0
	if !havePostFilter {
		j.limit = q.Limit
	}

	var out, agg *Result
	maxPackSize := 1 << defaultPackSizeLog2

	out = &Result{
		fields: j.fields,
		pkg:    NewPackage(),
	}
	if err := out.pkg.InitFields(j.fields, util.NonZero(q.Limit, maxPackSize)); err != nil {
		return nil, err
	}

	if havePostFilter {
		agg = &Result{
			fields: j.fields,
			pkg:    out.pkg.Clone(false, util.NonZero(j.limit, maxPackSize)),
		}
		defer agg.Close()
	} else {
		agg = out
	}

	// ------------------------------------------------------------
	// PROCESS
	// ------------------------------------------------------------
	queryOrderLR := !(j.Type == RightJoin || j.Type == FullJoin || j.Left.Limit == 0 && j.Right.Limit > 0)
	var (
		lRes, rRes *Result
		err        error
	)
	defer func() {
		if lRes != nil {
			lRes.Close()
		}
		if rRes != nil {
			rRes.Close()
		}
	}()

	var pkcursor uint64
	for {
		// ------------------------------------------------------------
		// QUERY
		// ------------------------------------------------------------
		if queryOrderLR {
			lQ := Query{
				Name:       q.Name + ".join_left",
				Fields:     j.Left.Fields.AddUnique(j.Predicate.Left),
				Conditions: j.Left.Where,
				Limit:      util.NonZeroMin(j.Left.Limit, maxPackSize),
			}
			if pkcursor > 0 {
				lQ.Conditions = append(lQ.Conditions, Condition{
					Field: j.Left.Fields.Pk(),
					Mode:  FilterModeGt,
					Value: pkcursor,
					Raw:   "left_join_cursor",
				})
			}
			log.Debugf("join: left table query with %d cond, cursor=%d limit=%d",
				len(lQ.Conditions), pkcursor, lQ.Limit)
			for i, c := range lQ.Conditions {
				log.Debugf("cond %d: %s", i, c.String())
			}
			lRes, err = j.Left.Table.Query(ctx, lQ)
			if err != nil {
				return nil, err
			}
			log.Debugf("join: left table result %d rows", lRes.Rows())

			if lRes.Rows() == 0 {
				log.Debugf("join: final result contains %d rows", out.Rows())
				return out, nil
			}

			pkcursor, err = lRes.pkg.Uint64At(lRes.Fields().PkIndex(), lRes.Rows()-1)
			if err != nil {
				log.Errorf("join: no pk column in query result %s: %v", lQ.Name, err)
				return nil, err
			}

			rConds := j.Right.Where
			if j.IsEquiJoin() && lRes.Rows() > 0 {
				lPredCol, err := lRes.Column(j.Predicate.Left.Name)
				if err != nil {
					return nil, err
				}
				lPredColCopy, err := j.Predicate.Left.Type.CopySliceType(lPredCol)
				if err != nil {
					return nil, err
				}
				rConds = append(rConds, Condition{
					Field:    j.Predicate.Right,
					Mode:     FilterModeIn,
					Value:    lPredColCopy,
					IsSorted: j.Predicate.Left.Flags&FlagPrimary > 0,
					Raw:      "join_predicate." + j.Left.Table.Name() + "." + j.Predicate.Left.Name,
				})
			}

			rQ := Query{
				Name:       q.Name + ".join_right",
				Fields:     j.Right.Fields.AddUnique(j.Predicate.Right),
				Conditions: rConds,
			}
			log.Debugf("join: right table query with %d cond and limit %d", len(rQ.Conditions), rQ.Limit)
			for i, c := range rQ.Conditions {
				log.Debugf("cond %d: %s", i, c.String())
			}

			rRes, err = j.Right.Table.Query(ctx, rQ)
			if err != nil {
				return nil, err
			}
			log.Debugf("join: right table result %d rows", rRes.Rows())

		} else {
			rQ := Query{
				Name:       q.Name + ".join_right",
				Fields:     j.Right.Fields.AddUnique(j.Predicate.Right),
				Conditions: j.Right.Where,
				Limit:      util.NonZeroMin(j.Right.Limit, maxPackSize),
			}
			if pkcursor > 0 {
				rQ.Conditions = append(rQ.Conditions, Condition{
					Field: j.Right.Fields.Pk(),
					Mode:  FilterModeGt,
					Value: pkcursor,
					Raw:   "right_join_cursor",
				})
			}
			log.Debugf("join: right table query with %d cond, cursor=%d limit=%d",
				len(rQ.Conditions), pkcursor, rQ.Limit)
			for i, c := range rQ.Conditions {
				log.Debugf("cond %d: %s", i, c.String())
			}
			rRes, err = j.Right.Table.Query(ctx, rQ)
			if err != nil {
				return nil, err
			}
			log.Debugf("join: right table result %d rows", rRes.Rows())

			if rRes.Rows() == 0 {
				log.Debugf("join: final result contains %d rows", out.Rows())
				return out, nil
			}

			pkcursor, err = rRes.pkg.Uint64At(rRes.Fields().PkIndex(), rRes.Rows()-1)
			if err != nil {
				log.Errorf("join: no pk column in query result %s: %v", rQ.Name, err)
				return nil, err
			}

			lConds := j.Left.Where
			if j.IsEquiJoin() && rRes.Rows() > 0 {
				rPredCol, err := rRes.Column(j.Predicate.Right.Name)
				if err != nil {
					return nil, err
				}
				rPredColCopy, err := j.Predicate.Right.Type.CopySliceType(rPredCol)
				if err != nil {
					return nil, err
				}
				lConds = append(lConds, Condition{
					Field:    j.Predicate.Left,
					Mode:     FilterModeIn,
					Value:    rPredColCopy,
					IsSorted: j.Predicate.Right.Flags&FlagPrimary > 0,
					Raw:      "join_predicate." + j.Right.Table.Name() + "." + j.Predicate.Right.Name,
				})
			}
			lQ := Query{
				Name:       q.Name + ".join_left",
				Fields:     j.Left.Fields.AddUnique(j.Predicate.Left),
				Conditions: lConds,
			}
			log.Debugf("join: left table query with %d cond and limit %d", len(lQ.Conditions), lQ.Limit)
			for i, c := range lQ.Conditions {
				log.Debugf("cond %d: %s", i, c.String())
			}
			lRes, err = j.Left.Table.Query(ctx, lQ)
			if err != nil {
				return nil, err
			}
			log.Debugf("join: left table result %d rows", lRes.Rows())
		}

		// ------------------------------------------------------------
		// JOIN
		// ------------------------------------------------------------
		switch j.Type {
		case InnerJoin:
			if j.IsEquiJoin() {
				err = mergeJoinInner(j, lRes, rRes, agg)
			} else {
				err = loopJoinInner(j, lRes, rRes, agg)
			}
		case LeftJoin:
			if j.IsEquiJoin() {
				err = mergeJoinLeft(j, lRes, rRes, agg)
			} else {
				err = loopJoinLeft(j, lRes, rRes, agg)
			}
		case RightJoin:
			if j.IsEquiJoin() {
				err = mergeJoinRight(j, lRes, rRes, agg)
			} else {
				err = loopJoinRight(j, lRes, rRes, agg)
			}
		case CrossJoin:
			err = loopJoinCross(j, lRes, rRes, agg)
		// case FullJoin:
		//  // does not work with the loop algorithm above
		// 	if j.IsEquiJoin() {
		// 		n, err = mergeJoinFull(j, lRes, rRes, agg)
		// 	} else {
		// 		n, err = loopJoinFull(j, lRes, rRes, agg)
		// 	}
		// case SelfJoin:
		// TODO
		// case AsOfJoin:
		// TODO
		// case WindowJoin:
		// TODO
		default:
			return nil, fmt.Errorf("%s is not implemented yet", j.Type)
		}
		if err != nil {
			return nil, err
		}

		lRes.Close()
		rRes.Close()

		// ------------------------------------------------------------
		// POST-PROCESS
		// ------------------------------------------------------------
		if havePostFilter {
			log.Debugf("join: filtering result with %d rows against %d conds", agg.Rows(), len(q.Conditions))
			bits := q.Conditions.MatchPack(agg.pkg)
			for idx, length := bits.Run(0); idx >= 0; idx, length = bits.Run(idx + length) {
				n := length
				if q.Limit > 0 {
					n = util.Min(n, q.Limit-out.pkg.Len())
				}

				if err := out.pkg.AppendFrom(agg.pkg, idx, n, true); err != nil {
					return nil, err
				}

				if q.Limit > 0 && out.pkg.Len() >= q.Limit {
					log.Debugf("join: final result clipped at limit %d/%d", q.Limit, out.pkg.Len())
					return out, nil
				}

			}
			bits.Close()
			agg.pkg.Clear()
		}

		if q.Limit > 0 && out.pkg.Len() >= q.Limit {
			log.Debugf("join: final result clipped at limit %d", q.Limit)
			return out, nil
		}
	}
	return out, nil
}

func loopJoinInner(join Join, left, right, out *Result) error {
	log.Debugf("join: inner join on %d/%d rows using loop", left.Rows(), right.Rows())
	for i, il := 0, left.Rows(); i < il; i++ {
		for j, jl := 0, right.Rows(); j < jl; j++ {
			if join.Predicate.MatchPacksAt(left.pkg, i, right.pkg, j) {
				if err := join.AppendResult(out.pkg, left.pkg, i, right.pkg, j); err != nil {
					return err
				}
				if join.limit > 0 && out.Rows() == join.limit {
					return nil
				}
			}
		}
	}
	return nil
}

// equi-joins only, |l| ~ |r| (close set sizes)
// TODO: never match NULL values (i.e. pkg.IsZeroAt(index,pos) == true)
func mergeJoinInner(join Join, left, right, out *Result) error {
	log.Debugf("join: inner join on %d/%d rows using merge", left.Rows(), right.Rows())
	var (
		currBlockStart, currBlockEnd int
		haveBlockMatch               bool
		forceMatch                   bool
	)

	if join.Predicate.Left.Flags&FlagPrimary == 0 {
		if err := left.SortByField(join.Predicate.Left.Name); err != nil {
			return err
		}
	}

	if join.Predicate.Right.Flags&FlagPrimary == 0 {
		if err := right.SortByField(join.Predicate.Right.Name); err != nil {
			return err
		}
	}

	i, j, il, jl := 0, 0, left.Rows(), right.Rows()
	for i < il && j < jl {
		var cmp int
		if !haveBlockMatch || forceMatch || j > currBlockEnd {
			cmp = join.Predicate.ComparePacksAt(left.pkg, i, right.pkg, j)
			forceMatch = false
		}
		switch cmp {
		case -1:
			i++
			j = currBlockStart
			haveBlockMatch = currBlockEnd-currBlockStart > 1
			forceMatch = true
		case 1:
			j = currBlockEnd + 1
			currBlockStart = j
			currBlockEnd = j
			haveBlockMatch = false
		case 0:
			if err := join.AppendResult(out.pkg, left.pkg, i, right.pkg, j); err != nil {
				return err
			}

			if join.limit > 0 && out.Rows() == join.limit {
				return nil
			}

			if !haveBlockMatch {
				currBlockEnd = j
			}
			if j+1 < jl {
				j++
			} else {
				i++
				j = currBlockStart
				haveBlockMatch = currBlockEnd-currBlockStart > 1
				forceMatch = true
			}
		}
	}
	return nil
}

// equi-joins only, |l| << >> |r| (widely different set sizes)
func hashJoinInner(join Join, left, right, out *Result) error {
	log.Debugf("join: inner join on %d/%d rows using hash", left.Rows(), right.Rows())
	return nil
}

// TODO: never match NULL values (i.e. pkg.IsZeroAt(index,pos) == true)
func loopJoinLeft(join Join, left, right, out *Result) error {
	log.Debugf("join: left join on %d/%d rows using loop", left.Rows(), right.Rows())
	return nil
}

// TODO: never match NULL values (i.e. pkg.IsZeroAt(index,pos) == true)
func mergeJoinLeft(join Join, left, right, out *Result) error {
	log.Debugf("join: left join on %d/%d rows using merge", left.Rows(), right.Rows())
	var (
		currBlockStart, currBlockEnd int
		wasMatch                     bool
	)

	if join.Predicate.Left.Flags&FlagPrimary == 0 {
		if err := left.SortByField(join.Predicate.Left.Name); err != nil {
			return err
		}
	}

	if join.Predicate.Right.Flags&FlagPrimary == 0 {
		if err := right.SortByField(join.Predicate.Right.Name); err != nil {
			return err
		}
	}

	i, j, il, jl := 0, 0, left.Rows(), right.Rows()
	for i < il {
		cmp := join.Predicate.ComparePacksAt(left.pkg, i, right.pkg, j)
		switch cmp {
		case -1:
			if !wasMatch {
				if err := join.AppendResult(out.pkg, left.pkg, i, nil, -1); err != nil {
					return err
				}
				if join.limit > 0 && out.Rows() == join.limit {
					return nil
				}
			}
			i++
			j = currBlockStart
			wasMatch = false
		case 1:
			if j+1 < jl {
				j = currBlockEnd + 1
				currBlockStart = j
				currBlockEnd = j
			} else {
				if !wasMatch {
					if err := join.AppendResult(out.pkg, left.pkg, i, nil, -1); err != nil {
						return err
					}
					if join.limit > 0 && out.Rows() == join.limit {
						return nil
					}
				}
				i++
			}
			wasMatch = false
		case 0:
			if err := join.AppendResult(out.pkg, left.pkg, i, right.pkg, j); err != nil {
				return err
			}
			if join.limit > 0 && out.Rows() == join.limit {
				return nil
			}
			if j+1 < jl {
				j++
				wasMatch = true
			} else {
				i++
				j = currBlockStart
				wasMatch = false
			}
		}
	}
	return nil
}

// TODO: need hash table to remember whether a row was joined already
// process inner join first, then add missing left, then missing right rows
func loopJoinRight(join Join, left, right, out *Result) error {
	log.Debugf("join: right join on %d/%d rows using loop", left.Rows(), right.Rows())
	return nil
}

func mergeJoinRight(join Join, left, right, out *Result) error {
	log.Debugf("join: right join on %d/%d rows using merge", left.Rows(), right.Rows())
	return nil
}

func loopJoinFull(join Join, left, right, out *Result) error {
	log.Debugf("join: full loop join on %d/%d rows", left.Rows(), right.Rows())
	return nil
}

func mergeJoinFull(join Join, left, right, out *Result) error {
	log.Debugf("join: full join on %d/%d rows using merge", left.Rows(), right.Rows())
	return nil
}

func loopJoinCross(join Join, left, right, out *Result) error {
	log.Debugf("join: cross join on %d/%d rows using loop", left.Rows(), right.Rows())
	for i, il := 0, left.Rows(); i < il; i++ {
		for j, jl := 0, right.Rows(); j < jl; j++ {
			if err := join.AppendResult(out.pkg, left.pkg, i, right.pkg, j); err != nil {
				return err
			}
			if join.limit > 0 && out.Rows() == join.limit {
				return nil
			}
		}
	}
	return nil
}
