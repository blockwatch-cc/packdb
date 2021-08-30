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
	Name       string            // optional, used for query stats
	NoCache    bool              // explicitly disable pack caching for this query
	NoIndex    bool              // explicitly disable index query (use for many known duplicates)
	Fields     FieldList         // SELECT ...
	Conditions ConditionTreeNode // WHERE ... AND / OR tree
	Order      OrderType         // ASC|DESC
	Limit      int               // LIMIT ...
	Offset     int               // OFFSET ...

	table     *Table
	reqfields FieldList
	idxFields FieldList

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
	return q.Conditions.NoMatch()
}

func NewQuery(name string, table *Table) Query {
	now := time.Now()
	f := table.Fields()
	return Query{
		Name:      name,
		table:     table,
		start:     now,
		lap:       now,
		Fields:    f,
		Order:     OrderAsc,
		reqfields: f,
		idxFields: table.fields.Indexed(),
	}
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
	q.reqfields = nil
}

func (q *Query) Runtime() time.Duration {
	return time.Since(q.start)
}

func (q *Query) PrintTiming() string {
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
	if err := q.Conditions.Compile(); err != nil {
		return fmt.Errorf("pack: %s %v", q.Name, err)
	}
	if len(q.Fields) == 0 {
		q.Fields = t.Fields()
	} else {
		q.Fields = q.Fields.MergeUnique(t.Fields().Pk())
	}
	q.reqfields = q.Fields.MergeUnique(q.Conditions.Fields()...)
	if len(q.idxFields) == 0 {
		q.idxFields = t.fields.Indexed()
	}
	if err := q.Check(); err != nil {
		q.totalTime = time.Since(q.lap)
		return err
	}
	q.compileTime = time.Since(q.lap)
	return nil
}

func (q Query) Check() error {
	tfields := q.table.Fields()
	for _, v := range q.reqfields {
		if !tfields.Contains(v.Name) {
			return fmt.Errorf("undefined field '%s.%s' in query %s", q.table.name, v.Name, q.Name)
		}
		if tfields.Find(v.Name).Type != v.Type {
			return fmt.Errorf("mismatched type %s for field '%s.%s' in query %s", v.Type, q.table.name, v.Name, q.Name)
		}
		if v.Index < 0 || v.Index >= len(tfields) {
			return fmt.Errorf("illegal index %d for field '%s.%s' in query %s", v.Index, q.table.name, v.Name, q.Name)
		}
	}
	if q.Conditions.Leaf() {
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
			if !q.idxFields.Contains(v.Cond.Field.Name) {
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

			if len(pkmatch) == 0 {
				v.Cond.nomatch = true
				continue
			}

			c := &Condition{
				Field:    q.table.Fields().Pk(),
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
	}

	return nil
}

func (q *Query) QueryIndexes(ctx context.Context, tx *Tx) error {
	q.lap = time.Now()
	if q.NoIndex || q.Conditions.Empty() {
		q.indexTime = time.Since(q.lap)
		return nil
	}
	if err := q.queryIndexNode(ctx, tx, &q.Conditions); err != nil {
		return err
	}
	q.indexTime = time.Since(q.lap)
	return nil
}

func (q *Query) MakePackSchedule(reverse bool) []int {
	schedule := make([]int, 0, q.table.packs.Len())
	for _, p := range q.table.packs.pairs {
		if q.Conditions.MaybeMatchPack(q.table.packs.heads[p.pos]) {
			schedule = append(schedule, p.pos)
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

func (q Query) WithFields(names ...string) Query {
	q.Fields = q.table.Fields().Select(names...)
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

func (q Query) And(conds ...UnboundCondition) Query {
	if len(conds) == 0 {
		return q
	}

	node := ConditionTreeNode{
		OrKind:   COND_AND,
		Children: make([]ConditionTreeNode, 0),
	}

	for _, v := range conds {
		node.AddNode(v.Bind(q.table))
	}

	if q.Conditions.Empty() {
		q.Conditions.ReplaceNode(node)
	} else {
		q.Conditions.AddNode(node)
	}

	return q
}

func (q Query) Or(conds ...UnboundCondition) Query {
	if len(conds) == 0 {
		return q
	}

	node := ConditionTreeNode{
		OrKind:   COND_OR,
		Children: make([]ConditionTreeNode, 0),
	}

	for _, v := range conds {
		node.AddNode(v.Bind(q.table))
	}

	if q.Conditions.Empty() {
		q.Conditions.ReplaceNode(node)
	} else {
		q.Conditions.AddNode(node)
	}

	return q
}

func (q Query) AndCondition(field string, mode FilterMode, value interface{}) Query {
	q.Conditions.AddAndCondition(&Condition{
		Field: q.table.Fields().Find(field),
		Mode:  mode,
		Value: value,
	})
	return q
}

func (q Query) OrCondition(field string, mode FilterMode, value interface{}) Query {
	q.Conditions.AddOrCondition(&Condition{
		Field: q.table.Fields().Find(field),
		Mode:  mode,
		Value: value,
	})
	return q
}

func (q Query) AndEqual(field string, value interface{}) Query {
	return q.AndCondition(field, FilterModeEqual, value)
}

func (q Query) AndNotEqual(field string, value interface{}) Query {
	return q.AndCondition(field, FilterModeNotEqual, value)
}

func (q Query) AndIn(field string, value interface{}) Query {
	return q.AndCondition(field, FilterModeIn, value)
}

func (q Query) AndNotIn(field string, value interface{}) Query {
	return q.AndCondition(field, FilterModeNotIn, value)
}

func (q Query) AndLt(field string, value interface{}) Query {
	return q.AndCondition(field, FilterModeLt, value)
}

func (q Query) AndLte(field string, value interface{}) Query {
	return q.AndCondition(field, FilterModeLte, value)
}

func (q Query) AndGt(field string, value interface{}) Query {
	return q.AndCondition(field, FilterModeGt, value)
}

func (q Query) AndGte(field string, value interface{}) Query {
	return q.AndCondition(field, FilterModeGte, value)
}

func (q Query) AndRegexp(field string, value interface{}) Query {
	return q.AndCondition(field, FilterModeRegexp, value)
}

func (q Query) AndRange(field string, from, to interface{}) Query {
	if q.Conditions.OrKind {
		q.Conditions = ConditionTreeNode{
			OrKind:   false,
			Children: []ConditionTreeNode{q.Conditions},
		}
	}
	q.Conditions.Children = append(q.Conditions.Children,
		ConditionTreeNode{
			Cond: &Condition{
				Field: q.table.Fields().Find(field),
				Mode:  FilterModeRange,
				From:  from,
				To:    to,
			},
		})
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
