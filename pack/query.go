// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// TODO
// - support expressions in fields and condition lists

package pack

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"blockwatch.cc/packdb/vec"
)

var QueryLogMinDuration time.Duration = 500 * time.Millisecond

type Query struct {
	Name       string           // optional, used for query stats
	NoCache    bool             // explicitly disable pack caching for this query
	NoIndex    bool             // explicitly disable index query (use for many known duplicates)
	Fields     []string         // SELECT ...
	Conditions UnboundCondition // WHERE ... AND / OR tree
	Order      OrderType        // ASC|DESC
	Limit      int              // LIMIT ...
	Offset     int              // OFFSET ...

	table   *Table
	conds   ConditionTreeNode
	outcols FieldList
	reqcols FieldList
	idxcols FieldList

	start          time.Time
	lap            time.Time
	compileTime    time.Duration
	journalTime    time.Duration
	indexTime      time.Duration
	scanTime       time.Duration
	totalTime      time.Duration
	packsScheduled int
	packsScanned   int
	rowsMatched    int
	indexLookups   int
}

func (q Query) IsEmptyMatch() bool {
	return q.conds.NoMatch()
}

func NewQuery(name string) Query {
	return Query{
		Name:  name,
		Order: OrderAsc,
	}
}

func (t *Table) NewQuery(name string) Query {
	return NewQuery(name).WithTable(t)
}

func (q *Query) Close() {
	if q.table != nil {
		q.totalTime = time.Since(q.start)
		if q.totalTime > QueryLogMinDuration {
			log.Warnf("%s", newLogClosure(func() string {
				return q.PrintTiming()
			}))
		}
		q.table = nil
	}
	q.Fields = nil
	q.outcols = nil
	q.reqcols = nil
	q.idxcols = nil
}

func (q Query) IsBound() bool {
	return q.table != nil && !q.conds.Empty()
}

func (q Query) Runtime() time.Duration {
	return time.Since(q.start)
}

func (q Query) PrintTiming() string {
	return fmt.Sprintf("query: %s compile=%s journal=%s index=%s scan=%s total=%s matched=%d rows, scheduled=%d packs, scanned=%d packs, searched=%d index rows",
		q.Name,
		q.compileTime,
		q.journalTime,
		q.indexTime,
		q.scanTime,
		q.totalTime,
		q.rowsMatched,
		q.packsScheduled,
		q.packsScanned,
		q.indexLookups)
}

func (q *Query) Compile(t *Table) error {
	if t == nil {
		return ErrNoTable
	}
	if !strings.HasPrefix(q.Name, t.Name()) {
		q.Name = t.Name() + "." + q.Name
	}
	q.table = t
	q.start = time.Now()
	q.lap = q.start
	if q.conds.Size() == 0 {
		q.conds = q.Conditions.Bind(q.table)
		if err := q.conds.Compile(); err != nil {
			return fmt.Errorf("pack: %s %v", q.Name, err)
		}
	}
	if len(q.outcols) == 0 {
		if len(q.Fields) == 0 {
			q.outcols = t.fields
		} else {
			q.outcols = q.table.fields.Select(q.Fields...)
			q.outcols = q.outcols.MergeUnique(t.fields.Pk())
		}
	}
	if len(q.reqcols) == 0 {
		q.reqcols = q.outcols.MergeUnique(q.conds.Fields()...)
		q.reqcols = q.reqcols.MergeUnique(t.fields.Pk())
	}
	if len(q.idxcols) == 0 {
		q.idxcols = t.fields.Indexed()
	}
	if err := q.check(); err != nil {
		q.totalTime = time.Since(q.lap)
		return err
	}
	q.compileTime = time.Since(q.lap)

	log.Debug(newLogClosure(func() string {
		return q.Dump()
	}))

	return nil
}

func (q Query) check() error {
	tfields := q.table.fields
	for _, v := range q.reqcols {
		if !tfields.Contains(v.Name) {
			return fmt.Errorf("undefined column '%s.%s' in query %s", q.table.name, v.Name, q.Name)
		}
		if tfields.Find(v.Name).Type != v.Type {
			return fmt.Errorf("mismatched type %s for column '%s.%s' in query %s", v.Type, q.table.name, v.Name, q.Name)
		}
		if v.Index < 0 || v.Index >= len(tfields) {
			return fmt.Errorf("illegal index %d for column '%s.%s' in query %s", v.Index, q.table.name, v.Name, q.Name)
		}
	}
	if q.conds.Leaf() {
		return fmt.Errorf("unexpected simple condition tree in query %s", q.Name)
	}
	if q.Limit < 0 {
		return fmt.Errorf("invalid limit %d", q.Limit)
	}
	if q.Offset < 0 {
		return fmt.Errorf("invalid offset %d", q.Offset)
	}
	return nil
}

func (q *Query) queryIndexNode(ctx context.Context, tx *Tx, node *ConditionTreeNode) error {
	ins := make([]ConditionTreeNode, 0)
	for i, v := range node.Children {
		if v.Leaf() {
			if !q.idxcols.Contains(v.Cond.Field.Name) {
				continue
			}
			idx := q.table.indexes.FindField(v.Cond.Field.Name)
			if idx == nil {
				continue
			}
			if !idx.CanMatch(*v.Cond) {
				continue
			}

			pkmatch, err := idx.LookupTx(ctx, tx, *v.Cond)
			if err != nil {
				log.Errorf("%s index scan: %v", q.Name, err)
				return err
			}
			q.indexLookups += len(pkmatch)

			if !idx.Type.MayHaveCollisions() {
				v.Cond.processed = true
			}
			log.Debugf("query: %s index scan found %d matches", q.Name, len(pkmatch))

			if len(pkmatch) == 0 {
				v.Cond.nomatch = true
				continue
			}

			c := &Condition{
				Field:    q.table.fields.Pk(),
				Mode:     FilterModeIn,
				Value:    pkmatch,
				IsSorted: true,
				Raw:      v.Cond.Raw + "/index_lookup",
			}

			if err := c.Compile(); err != nil {
				return fmt.Errorf("pack: %s %v", q.Name, err)
			}

			ins = append(ins, ConditionTreeNode{Cond: c})
		} else {
			if err := q.queryIndexNode(ctx, tx, &node.Children[i]); err != nil {
				return err
			}
		}
	}

	if len(ins) > 0 {
		for _, v := range node.Children {
			if v.Leaf() && v.Cond.processed && !v.Cond.nomatch {
				continue
			}
			ins = append(ins, v)
		}
		node.Children = ins
		log.Debug(newLogClosure(func() string {
			return "Updated query:\n" + q.Dump()
		}))
	}

	return nil
}

func (q *Query) QueryIndexes(ctx context.Context, tx *Tx) error {
	q.lap = time.Now()
	if q.NoIndex || q.conds.Empty() {
		q.indexTime = time.Since(q.lap)
		return nil
	}
	if err := q.queryIndexNode(ctx, tx, &q.conds); err != nil {
		return err
	}
	q.indexTime = time.Since(q.lap)
	return nil
}

func (q *Query) MakePackSchedule(reverse bool) []int {
	schedule := make([]int, 0, q.table.packs.Len())
	for _, p := range q.table.packs.pos {
		if q.conds.MaybeMatchPack(q.table.packs.packs[p]) {
			schedule = append(schedule, int(p))
		}
	}
	q.packsScheduled = len(schedule)
	if reverse {
		for l, r := 0, len(schedule)-1; l < r; l, r = l+1, r-1 {
			schedule[l], schedule[r] = schedule[r], schedule[l]
		}
	}
	return schedule
}

func (q *Query) MakePackLookupSchedule(ids []uint64, reverse bool) []int {
	schedule := make([]int, 0, q.table.packs.Len())
	slice := vec.Uint64Slice(ids)

	mins, maxs := q.table.packs.MinMaxSlices()

	for i := range mins {
		if !slice.ContainsRange(mins[i], maxs[i]) {
			continue
		}
		schedule = append(schedule, i)
	}

	sort.Slice(schedule, func(i, j int) bool { return mins[schedule[i]] < mins[schedule[j]] })

	if reverse {
		for l, r := 0, len(schedule)-1; l < r; l, r = l+1, r-1 {
			schedule[l], schedule[r] = schedule[r], schedule[l]
		}
	}
	q.packsScheduled = len(schedule)
	return schedule
}

func (q Query) WithTable(table *Table) Query {
	q.table = table
	return q
}

func (q Query) WithFields(names ...string) Query {
	q.Fields = append(q.Fields, names...)
	return q
}

func (q Query) WithOrder(o OrderType) Query {
	q.Order = o
	return q
}

func (q Query) WithDesc() Query {
	q.Order = OrderDesc
	return q
}

func (q Query) WithAsc() Query {
	q.Order = OrderAsc
	return q
}

func (q Query) WithLimit(l int) Query {
	q.Limit = l
	return q
}

func (q Query) WithOffset(o int) Query {
	q.Offset = o
	return q
}

func (q Query) WithoutIndex() Query {
	q.NoIndex = true
	return q
}

func (q Query) WithoutCache() Query {
	q.NoCache = true
	return q
}

func (q Query) AndCondition(conds ...UnboundCondition) Query {
	if len(conds) == 0 {
		return q
	}
	q.Conditions.Add(And(conds...))
	return q
}

func (q Query) OrCondition(conds ...UnboundCondition) Query {
	if len(conds) == 0 {
		return q
	}
	q.Conditions.Add(Or(conds...))
	return q
}

func (q Query) And(col string, mode FilterMode, value interface{}) Query {
	q.Conditions.And(col, mode, value)
	return q
}

func (q Query) Or(col string, mode FilterMode, value interface{}) Query {
	q.Conditions.Or(col, mode, value)
	return q
}

func (q Query) AndEqual(col string, value interface{}) Query {
	return q.And(col, FilterModeEqual, value)
}

func (q Query) AndNotEqual(col string, value interface{}) Query {
	return q.And(col, FilterModeNotEqual, value)
}

func (q Query) AndIn(col string, value interface{}) Query {
	return q.And(col, FilterModeIn, value)
}

func (q Query) AndNotIn(col string, value interface{}) Query {
	return q.And(col, FilterModeNotIn, value)
}

func (q Query) AndLt(col string, value interface{}) Query {
	return q.And(col, FilterModeLt, value)
}

func (q Query) AndLte(col string, value interface{}) Query {
	return q.And(col, FilterModeLte, value)
}

func (q Query) AndGt(col string, value interface{}) Query {
	return q.And(col, FilterModeGt, value)
}

func (q Query) AndGte(col string, value interface{}) Query {
	return q.And(col, FilterModeGte, value)
}

func (q Query) AndRegexp(col string, value interface{}) Query {
	return q.And(col, FilterModeRegexp, value)
}

func (q Query) AndRange(col string, from, to interface{}) Query {
	q.Conditions.AndRange(col, from, to)
	return q
}

func (q Query) OrRange(col string, from, to interface{}) Query {
	q.Conditions.OrRange(col, from, to)
	return q
}

func (q Query) Execute(ctx context.Context, val interface{}) error {
	v := reflect.ValueOf(val)
	if v.Kind() != reflect.Ptr {
		return fmt.Errorf("pack: non-pointer passed to Execute")
	}
	v = reflect.Indirect(v)
	switch v.Kind() {
	case reflect.Slice:
		elem := v.Type().Elem()
		return q.table.Stream(ctx, q, func(r Row) error {
			e := reflect.New(elem)
			ev := e
			if e.Elem().Kind() == reflect.Ptr {
				ev.Elem().Set(reflect.New(e.Elem().Type().Elem()))
				ev = reflect.Indirect(e)
			}

			if err := r.Decode(ev.Interface()); err != nil {
				return err
			}

			v.Set(reflect.Append(v, e.Elem()))
			return nil
		})
	case reflect.Struct:
		return q.table.Stream(ctx, q.WithLimit(1), func(r Row) error {
			return r.Decode(val)
		})
	default:
		return fmt.Errorf("pack: non-slice/struct passed to Execute")
	}
}

func (q Query) Stream(ctx context.Context, fn func(r Row) error) error {
	return q.table.Stream(ctx, q, fn)
}

func (q Query) Delete(ctx context.Context) (int64, error) {
	return q.table.Delete(ctx, q)
}

func (q Query) Count(ctx context.Context) (int64, error) {
	return q.table.Count(ctx, q)
}
