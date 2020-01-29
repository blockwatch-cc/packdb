// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// TODO
// - support expressions in fields and condition lists

package pack

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"blockwatch.cc/packdb/vec"
)

var QueryLogMinDuration time.Duration = 500 * time.Millisecond

type Query struct {
	Name       string // optional, used for query stats
	NoCache    bool
	Fields     FieldList     // SELECT ...
	Conditions ConditionList // WHERE ... AND (TODO: OR)
	Order      OrderType     // ASC|DESC
	Limit      int           // LIMIT ...

	// GroupBy   FieldList   // GROUP BY ... - COLLATE/COLLAPSE
	// OrderBy   FieldList    // ORDER BY ...
	// Aggregate AggregateList // sum, mean, ...

	// internal
	table     *Table
	pkids     []uint64
	reqfields FieldList

	// metrics
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

// type AggregateList []Aggregate
// type Aggregate struct {
// 	Field Field
// 	Func  AggFunction
// }

func (q Query) IsEmptyMatch() bool {
	return q.pkids != nil && len(q.pkids) == 0
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
	}
}

func (q *Query) Close() {
	if q.pkids != nil {
		q.pkids = q.pkids[:0]
		q.table.pkPool.Put(q.pkids)
		q.pkids = nil
	}
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
	if err := q.Conditions.Compile(q.table); err != nil {
		return err
	}
	if len(q.Fields) == 0 {
		q.Fields = t.Fields()
	} else {
		q.Fields = q.Fields.MergeUnique(t.Fields().Pk())
	}
	q.reqfields = q.Fields.MergeUnique(q.Conditions.Fields()...)
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
	return nil
}

func (q *Query) QueryIndexes(ctx context.Context, tx *Tx) error {
	q.lap = time.Now()
	idxFields := q.table.fields.Indexed()
	for i, cond := range q.Conditions {
		if !idxFields.Contains(cond.Field.Name) {
			log.Tracef("query: %s table non-indexed field '%s' for cond %d, fallback to table scan",
				q.table.name, cond.Field.Name, i)
			continue
		}
		idx := q.table.indexes.FindField(cond.Field.Name)
		if idx == nil {
			log.Tracef("query: %s table missing index on field %s for cond %d, fallback to table scan",
				q.table.name, cond.Field.Name, i)
			continue
		}
		if !idx.CanMatch(cond) {
			log.Tracef("query: index %s cannot match cond %d, fallback to table scan", idx.Name, i)
			continue
		}
		pkmatch, err := idx.LookupTx(ctx, tx, cond)
		if err != nil {
			q.Close()
			return err
		}
		if q.pkids == nil {
			q.pkids = pkmatch
		} else {
			q.pkids = vec.IntersectSortedUint64(q.pkids, pkmatch, q.table.pkPool.Get().([]uint64))
			pkmatch = pkmatch[:0]
			q.table.pkPool.Put(pkmatch)
		}
		if !idx.Type.MayHaveCollisions() {
			q.Conditions[i].processed = true
		}
	}
	q.indexLookups = len(q.pkids)

	if len(q.pkids) > 0 {
		conds := ConditionList{
			Condition{
				Field:    q.table.Fields().Pk(),
				Mode:     FilterModeIn,
				Value:    q.pkids,
				IsSorted: true,
				Raw:      "pkid's from index lookup",
			},
		}
		for _, v := range q.Conditions {
			if !v.processed {
				conds = append(conds, v)
			}
		}
		q.Conditions = conds
		q.Conditions[0].Compile()
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
