// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// TODO
// - materialized views for storing expensive query results
// - complex conditions using AND/OR and brackets

// Query features
// - GROUP BY and HAVING (special condition to filter groups after aggregation)
// - aggregate functions sum, mean, median, std,
// - selectors (first, last, min, max, topN, bottomN)
// - arithmetic expressions (simple math)
// - PARTITION BY analytics (keep rows unlike GROUP BY which aggregates)

// Performance and safety
// - thread safety
// - concurrent/background pack compaction/storage using shadow journal
// - concurrent index build
// - flush journal after each insert/update/delete for data persistence is sloooow
// - auto-create indexes when index keyword is used in struct tag for CreateTable
// - more indexes (b-tree?)

// Design concepts
// - columnar design with type-specific multi-level compression
// - column groups (i.e. matrix to keep relation within a row)
// - zonemaps keeping min/max for each column in a partition
// - buffer pools for packs and slices
// - pack caches and cache-sensitive pack query scheduler

package pack

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"blockwatch.cc/packdb/cache"
	"blockwatch.cc/packdb/cache/lru"
	"blockwatch.cc/packdb/store"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/packdb/vec"
)

const (
	idFieldName             = "I"
	defaultCacheSize        = 128 // keep 128 unpacked partitions in memory (per table/index)
	defaultPackSizeLog2     = 16  // 64k entries per partition
	defaultJournalFillLevel = 50  // keep space for extension
)

var (
	optsKey      = []byte("_options")
	fieldsKey    = []byte("_fields")
	metaKey      = []byte("_meta")
	headerKey    = []byte("_headers")
	indexesKey   = []byte("_indexes")
	journalKey   = []byte("_journal")
	tombstoneKey = []byte("_tombstone")

	DefaultOptions = Options{
		PackSizeLog2:    defaultPackSizeLog2, // 64k entries
		JournalSizeLog2: 17,                  // 128k entries
		CacheSize:       defaultCacheSize,    // packs
		FillLevel:       90,                  // boltdb fill level to limit reallocations
	}
	NoOptions = Options{}
)

type Tombstone struct {
	Id uint64 `pack:"I,pk,snappy"`
}

type Options struct {
	PackSizeLog2    int `json:"pack_size_log2"`
	JournalSizeLog2 int `json:"journal_size_log2"`
	CacheSize       int `json:"cache_size"`
	FillLevel       int `json:"fill_level"`
}

func (o *Options) MergeDefaults() {
	o.CacheSize = util.NonZero(o.CacheSize, DefaultOptions.CacheSize)
	o.PackSizeLog2 = util.NonZero(o.PackSizeLog2, DefaultOptions.PackSizeLog2)
	o.JournalSizeLog2 = util.NonZero(o.JournalSizeLog2, DefaultOptions.JournalSizeLog2)
	o.FillLevel = util.NonZero(o.FillLevel, DefaultOptions.FillLevel)
}

func (o Options) Check() error {
	if o.PackSizeLog2 < 10 || o.PackSizeLog2 > 22 {
		return fmt.Errorf("PackSizeLog2 %d out of range [10, 22]", o.PackSizeLog2)
	}
	if o.JournalSizeLog2 < 10 || o.JournalSizeLog2 > 22 {
		return fmt.Errorf("JournalSizeLog2 %d out of range [10, 22]", o.JournalSizeLog2)
	}
	if o.CacheSize > 64*1024 {
		return fmt.Errorf("CacheSize %d out of range [0, 64k]", o.CacheSize)
	}
	if o.FillLevel < 10 || o.FillLevel > 100 {
		return fmt.Errorf("FillLevel %d out of range [10, 100]", o.FillLevel)
	}
	return nil
}

type TableMeta struct {
	Sequence uint64 `json:"sequence"`
	Rows     int64  `json:"rows"`
	dirty    bool   `json:"-"`
}

type Table struct {
	name      string
	opts      Options
	fields    FieldList
	indexes   IndexList
	meta      TableMeta
	db        *DB
	cache     cache.Cache // keep decoded packs for query/updates
	journal   *Package    // insert/update log
	tombstone *Package    // delete log
	packs     *PackIndex  // in-memory list of pack and block headers
	key       []byte
	metakey   []byte
	packPool  *sync.Pool // buffer pool for new packages
	pkPool    *sync.Pool
	stats     TableStats
	mu        sync.RWMutex
}

func (d *DB) CreateTable(name string, fields FieldList, opts Options) (*Table, error) {
	opts.MergeDefaults()
	if err := opts.Check(); err != nil {
		return nil, err
	}
	t := &Table{
		name:   name,
		opts:   opts,
		fields: fields,
		meta: TableMeta{
			Sequence: 0,
		},
		db:      d,
		indexes: make(IndexList, 0),
		packs:   NewPackIndex(nil, fields.PkIndex()),
		key:     []byte(name),
		metakey: []byte(name + "_meta"),
		pkPool: &sync.Pool{
			New: func() interface{} { return make([]uint64, 0) },
		},
	}
	t.packPool = &sync.Pool{
		New: t.makePackage,
	}
	err := d.db.Update(func(dbTx store.Tx) error {
		b := dbTx.Bucket(t.key)
		if b != nil {
			return ErrTableExists
		}
		_, err := dbTx.Root().CreateBucketIfNotExists(t.key)
		if err != nil {
			return err
		}
		meta, err := dbTx.Root().CreateBucketIfNotExists(t.metakey)
		if err != nil {
			return err
		}
		_, err = meta.CreateBucketIfNotExists(headerKey)
		if err != nil {
			return err
		}
		buf, err := json.Marshal(t.opts)
		if err != nil {
			return err
		}
		err = meta.Put(optsKey, buf)
		if err != nil {
			return err
		}
		buf, err = json.Marshal(t.fields)
		if err != nil {
			return err
		}
		err = meta.Put(fieldsKey, buf)
		if err != nil {
			return err
		}
		buf, err = json.Marshal(t.indexes)
		if err != nil {
			return err
		}
		err = meta.Put(indexesKey, buf)
		if err != nil {
			return err
		}
		buf, err = json.Marshal(t.meta)
		if err != nil {
			return err
		}
		err = meta.Put(metaKey, buf)
		if err != nil {
			return err
		}
		t.journal = NewPackage()
		if err := t.journal.InitFields(fields, 1<<uint(t.opts.JournalSizeLog2)); err != nil {
			return err
		}
		_, err = storePackTx(dbTx, t.metakey, journalKey, t.journal, defaultJournalFillLevel)
		if err != nil {
			return err
		}
		t.tombstone = NewPackage()
		if err := t.tombstone.Init(Tombstone{}, 1<<uint(t.opts.JournalSizeLog2)); err != nil {
			return err
		}
		_, err = storePackTx(dbTx, t.metakey, tombstoneKey, t.tombstone, defaultJournalFillLevel)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if t.opts.CacheSize > 0 {
		t.cache, err = lru.New2QWithEvict(int(t.opts.CacheSize), t.onEvictedPackage)
		if err != nil {
			return nil, err
		}
	} else {
		t.cache = cache.NewNoCache()
	}
	log.Debugf("Created table %s", name)
	d.tables[name] = t
	return t, nil
}

func (d *DB) CreateTableIfNotExists(name string, fields FieldList, opts Options) (*Table, error) {
	t, err := d.CreateTable(name, fields, opts)
	if err != nil {
		if err != ErrTableExists {
			return nil, err
		}
		t, err = d.Table(name, opts)
		if err != nil {
			return nil, err
		}
	}
	return t, nil
}

func (d *DB) DropTable(name string) error {
	t, err := d.Table(name)
	if err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	idxnames := make([]string, len(t.indexes))
	for i, idx := range t.indexes {
		idxnames[i] = idx.Name
	}
	for _, v := range idxnames {
		if err := t.DropIndex(v); err != nil {
			return err
		}
	}
	t.cache.Purge()
	err = d.db.Update(func(dbTx store.Tx) error {
		err = dbTx.Root().DeleteBucket([]byte(name))
		if err != nil {
			return err
		}
		return dbTx.Root().DeleteBucket([]byte(name + "_meta"))
	})
	if err != nil {
		return err
	}
	delete(d.tables, t.name)
	t = nil
	return nil
}

func (d *DB) Table(name string, opts ...Options) (*Table, error) {
	if t, ok := d.tables[name]; ok {
		return t, nil
	}
	if len(opts) > 0 {
		log.Debugf("Opening table %s with opts %#v", name, opts[0])
	} else {
		log.Debugf("Opening table %s with default opts", name)
	}
	t := &Table{
		name:    name,
		db:      d,
		key:     []byte(name),
		metakey: []byte(name + "_meta"),
		pkPool: &sync.Pool{
			New: func() interface{} { return make([]uint64, 0) },
		},
	}
	t.packPool = &sync.Pool{
		New: t.makePackage,
	}
	err := d.db.View(func(dbTx store.Tx) error {
		b := dbTx.Bucket(t.metakey)
		if b == nil {
			return ErrNoTable
		}
		buf := b.Get(optsKey)
		if buf == nil {
			return fmt.Errorf("pack: missing options for table %s", name)
		}
		err := json.Unmarshal(buf, &t.opts)
		if err != nil {
			return err
		}
		buf = b.Get(fieldsKey)
		if buf == nil {
			return fmt.Errorf("pack: missing fields for table %s", name)
		}
		err = json.Unmarshal(buf, &t.fields)
		if err != nil {
			return fmt.Errorf("pack: cannot read fields for table %s: %v", name, err)
		}
		buf = b.Get(indexesKey)
		if buf == nil {
			return fmt.Errorf("pack: missing indexes for table %s", name)
		}
		err = json.Unmarshal(buf, &t.indexes)
		if err != nil {
			return fmt.Errorf("pack: cannot read indexes for table %s: %v", name, err)
		}
		buf = b.Get(metaKey)
		if buf == nil {
			return fmt.Errorf("pack: missing metadata for table %s", name)
		}
		err = json.Unmarshal(buf, &t.meta)
		if err != nil {
			return fmt.Errorf("pack: cannot read metadata for table %s: %v", name, err)
		}
		t.journal, err = loadPackTx(dbTx, t.metakey, journalKey, nil)
		if err != nil {
			return fmt.Errorf("pack: cannot open journal for table %s: %v", name, err)
		}
		t.journal.pkindex = t.fields.PkIndex()
		log.Debugf("pack: %s table loaded journal with %d entries", name, t.journal.Len())
		t.tombstone, err = loadPackTx(dbTx, t.metakey, tombstoneKey, nil)
		if err != nil {
			return fmt.Errorf("pack: cannot open tombstone for table %s: %v", name, err)
		}
		t.tombstone.pkindex = 0
		log.Debugf("pack: %s table loaded tombstone with %d entries", name, t.tombstone.Len())

		return t.loadPackHeaders(dbTx)
	})
	if err != nil {
		return nil, err
	}
	cacheSize := t.opts.CacheSize
	if len(opts) > 0 {
		cacheSize = opts[0].CacheSize
		if opts[0].JournalSizeLog2 > 0 {
			t.opts.JournalSizeLog2 = opts[0].JournalSizeLog2
		}
	}
	if cacheSize > 0 {
		t.cache, err = lru.New2QWithEvict(int(cacheSize), t.onEvictedPackage)
		if err != nil {
			return nil, err
		}
	} else {
		t.cache = cache.NewNoCache()
	}

	needFlush := make([]*Index, 0)
	for _, idx := range t.indexes {
		if len(opts) > 1 {
			if err := t.OpenIndex(idx, opts[1]); err != nil {
				return nil, err
			}
		} else {
			if err := t.OpenIndex(idx); err != nil {
				return nil, err
			}
		}
		if idx.journal.Len() > 0 {
			needFlush = append(needFlush, idx)
		}
	}

	// FIXME: change index lookups to also use index journal
	// flush any previously stored index data; this is necessary because
	// index lookups are only implemented for non-journal packs
	if len(needFlush) > 0 {
		tx, err := t.db.Tx(true)
		if err != nil {
			return nil, err
		}

		defer tx.Rollback()
		for _, idx := range needFlush {
			if err := idx.FlushTx(context.Background(), tx); err != nil {
				return nil, err
			}
		}

		tx.Commit()
	}
	d.tables[name] = t
	return t, nil
}

func (t *Table) loadPackHeaders(dbTx store.Tx) error {
	b := dbTx.Bucket(t.metakey)
	if b == nil {
		return ErrNoTable
	}
	heads := make(PackageHeaderList, 0)
	bh := b.Bucket(headerKey)
	if bh != nil {
		log.Debugf("pack: %s table loading package headers from bucket", t.name)
		c := bh.Cursor()
		var err error
		for ok := c.First(); ok; ok = c.Next() {
			head := PackageHeader{}
			err = head.UnmarshalBinary(c.Value())
			if err != nil {
				break
			}
			heads = append(heads, head)
			atomic.AddInt64(&t.stats.MetaBytesRead, int64(len(c.Value())))
		}
		if err != nil {
			heads = heads[:0]
			log.Errorf("pack: header decode for table %s pack %x: %v", t.name, c.Key(), err)
		} else {
			t.packs = NewPackIndex(heads, t.fields.PkIndex())
			log.Debugf("pack: %s table loaded %d package headers", t.name, t.packs.Len())
			return nil
		}
	}
	log.Infof("pack: scanning headers for table %s...", t.name)
	c := dbTx.Bucket(t.key).Cursor()
	pkg := t.journal.Clone(false, 0)
	for ok := c.First(); ok; ok = c.Next() {
		ph, err := pkg.UnmarshalHeader(c.Value())
		if err != nil {
			return fmt.Errorf("pack: cannot scan table pack %x: %v", c.Key(), err)
		}
		ph.dirty = true
		ph.Key = make([]byte, len(c.Key()))
		copy(ph.Key, c.Key())
		heads = append(heads, ph)
		atomic.AddInt64(&t.stats.MetaBytesRead, int64(len(c.Value())))
	}
	t.packs = NewPackIndex(heads, t.fields.PkIndex())
	log.Debugf("pack: %s table scanned %d package headers", t.name, t.packs.Len())
	return nil
}

func (t *Table) storePackHeaders(dbTx store.Tx) error {
	b := dbTx.Bucket(t.metakey)
	if b == nil {
		return ErrNoTable
	}
	hb := b.Bucket(headerKey)
	for _, v := range t.packs.deads {
		log.Debugf("pack: %s table removing dead header %x", t.name, v)
		hb.Delete(v)
	}
	t.packs.deads = t.packs.deads[:0]

	for i := range t.packs.heads {
		if !t.packs.heads[i].dirty {
			continue
		}
		buf, err := t.packs.heads[i].MarshalBinary()
		if err != nil {
			return err
		}
		if err := hb.Put(t.packs.heads[i].Key, buf); err != nil {
			return err
		}
		t.packs.heads[i].dirty = false
		atomic.AddInt64(&t.stats.MetaBytesWritten, int64(len(buf)))
	}
	return nil
}

func (t *Table) Fields() FieldList {
	return t.fields
}

func (t *Table) Name() string {
	return t.name
}

func (t *Table) Database() *DB {
	return t.db
}

func (t *Table) Options() Options {
	return t.opts
}

func (t *Table) Indexes() IndexList {
	return t.indexes
}

func (t *Table) Lock() {
	t.mu.Lock()
}

func (t *Table) Unlock() {
	t.mu.Unlock()
}

func (t *Table) Stats() TableStats {
	var s TableStats = t.stats
	s.TupleCount = t.meta.Rows
	s.PacksCount = int64(t.packs.Len())
	s.PacksCached = int64(t.cache.Len())
	for _, idx := range t.indexes {
		s.IndexPacksCount += int64(idx.packs.Len())
		s.IndexPacksCached += int64(idx.cache.Len())
	}
	return s
}

func (t *Table) NextSequence() uint64 {
	t.meta.Sequence++
	t.meta.dirty = true
	return t.meta.Sequence
}

func (t *Table) Insert(ctx context.Context, val interface{}) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if util.InterruptRequested(ctx) {
		return ctx.Err()
	}

	if err := t.insertJournal(val); err != nil {
		return err
	}

	if t.journal.Len()+t.tombstone.Len() >= 1<<uint(t.opts.JournalSizeLog2) {
		tx, err := t.db.Tx(true)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := t.flushTx(ctx, tx); err != nil {
			return err
		}
		return tx.Commit()
	}
	// FIXME: flushing packed journal after every insert slows down by 10-20x
	// else {
	// 	// save journal and tombstone
	// 	if t.journal.IsDirty() {
	// 		tx, err := t.db.Tx(true)
	// 		if err != nil {
	// 			return err
	// 		}
	// 		// be panic safe
	// 		defer tx.Rollback()
	// 		if err := t.flushJournalTx(ctx, tx); err != nil {
	// 			return err
	// 		}
	// 		// commit storage transaction
	// 		return tx.Commit()
	// 	}
	// }
	return nil
}

// unsafe when used concurrently, need to obtain lock _before_ starting bolt tx
func (t *Table) InsertTx(ctx context.Context, tx *Tx, val interface{}) error {
	if err := t.insertJournal(val); err != nil {
		return err
	}

	if t.journal.Len()+t.tombstone.Len() >= 1<<uint(t.opts.JournalSizeLog2) {
		if err := t.flushTx(ctx, tx); err != nil {
			return err
		}
	}
	// FIXME: flushing packed journal after every insert slows down by 10-20x
	//  else {
	// 	if err := t.flushJournalTx(ctx, tx); err != nil {
	// 		return err
	// 	}
	// }

	return nil
}

func (t *Table) insertJournal(val interface{}) error {
	var batch []Item
	if v, ok := val.([]Item); ok {
		batch = v
	} else if i, ok := val.(Item); ok {
		batch = []Item{i}
	} else {
		return fmt.Errorf("pack: type %T does not implement Item interface", val)
	}
	atomic.AddInt64(&t.stats.InsertCalls, 1)

	var (
		lastmax  uint64
		needSort bool
		count    int64
	)
	if t.journal.Len() > 0 {
		lastmax, _ = t.journal.Uint64At(t.journal.pkindex, t.journal.Len()-1)
	}

	for _, v := range batch {
		id := v.ID()
		if id == 0 {
			id = t.NextSequence()
			v.SetID(id)
		} else {
			if id > t.meta.Sequence {
				t.meta.Sequence = id
				t.meta.dirty = true
			}
		}
		needSort = needSort || id < lastmax
		lastmax = util.MaxU64(lastmax, id)

		if err := t.journal.Push(v); err != nil {
			return err
		}
		count++
	}
	t.meta.Rows += count
	t.meta.dirty = true
	atomic.AddInt64(&t.stats.InsertedTuples, count)

	if needSort {
		if err := t.journal.PkSort(); err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) InsertRow(ctx context.Context, row Row) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if util.InterruptRequested(ctx) {
		return ctx.Err()
	}

	if err := t.appendPackIntoJournal(ctx, row.res.pkg, row.n, 1); err != nil {
		return err
	}

	if t.journal.Len()+t.tombstone.Len() >= 1<<uint(t.opts.JournalSizeLog2) {
		tx, err := t.db.Tx(true)
		if err != nil {
			return err
		}

		defer tx.Rollback()
		if err := t.flushTx(ctx, tx); err != nil {
			return err
		}

		return tx.Commit()
	}
	// FIXME: flushing packed journal after every insert slows down by 10-20x
	// else {
	// 	// FIXME
	// 	// in-memory journal inserts are fast, but unsafe for data durability
	// 	//
	// 	// save journal and tombstone
	// 	if t.journal.IsDirty() {
	// 		tx, err := t.db.Tx(true)
	// 		if err != nil {
	// 			return err
	// 		}
	// 		// be panic safe
	// 		defer tx.Rollback()
	// 		if err := t.flushJournalTx(ctx, tx); err != nil {
	// 			return err
	// 		}
	// 		// commit storage transaction
	// 		return tx.Commit()
	// 	}
	// }
	return nil
}

func (t *Table) InsertResult(ctx context.Context, res *Result) error {
	if res == nil {
		return nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if util.InterruptRequested(ctx) {
		return ctx.Err()
	}

	if err := t.appendPackIntoJournal(ctx, res.pkg, 0, res.pkg.Len()); err != nil {
		return err
	}

	if t.journal.Len()+t.tombstone.Len() >= 1<<uint(t.opts.JournalSizeLog2) {
		tx, err := t.db.Tx(true)
		if err != nil {
			return err
		}

		defer tx.Rollback()
		if err := t.flushTx(ctx, tx); err != nil {
			return err
		}

		return tx.Commit()
	}
	// FIXME: flushing packed journal after every insert slows down by 10-20x
	// else {
	// 		// save journal and tombstone
	// 		if t.journal.IsDirty() {
	// 			tx, err := t.db.Tx(true)
	// 			if err != nil {
	// 				return err
	// 			}
	// 			// be panic safe
	// 			defer tx.Rollback()
	// 			if err := t.flushJournalTx(ctx, tx); err != nil {
	// 				return err
	// 			}
	// 			// commit storage transaction
	// 			return tx.Commit()
	// 		}
	// 	}
	return nil
}

func (t *Table) appendPackIntoJournal(ctx context.Context, pkg *Package, pos, n int) error {
	if pkg.Len() == 0 {
		return nil
	}

	firstid, _ := pkg.Uint64At(pkg.pkindex, 0)
	lastid, _ := pkg.Uint64At(pkg.pkindex, pkg.Len()-1)

	if firstid < t.meta.Sequence {
		return fmt.Errorf("pack: out-of-order pack insert is not supported %d < %d",
			firstid, t.meta.Sequence)
	}

	if err := t.journal.AppendFrom(pkg, pos, n, true); err != nil {
		return err
	}

	t.meta.Sequence = lastid
	t.meta.Rows += int64(n)
	t.meta.dirty = true
	atomic.AddInt64(&t.stats.InsertCalls, 1)
	atomic.AddInt64(&t.stats.InsertedTuples, int64(n))
	return nil

}

func (t *Table) Update(ctx context.Context, val interface{}) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if util.InterruptRequested(ctx) {
		return ctx.Err()
	}

	if err := t.updateJournal(val); err != nil {
		return err
	}

	if t.journal.Len()+t.tombstone.Len() >= 1<<uint(t.opts.JournalSizeLog2) {
		tx, err := t.db.Tx(true)
		if err != nil {
			return err
		}

		defer tx.Rollback()
		if err := t.flushTx(ctx, tx); err != nil {
			return err
		}

		return tx.Commit()
	}
	// FIXME: flushing packed journal after every insert slows down by 10-20x
	// else {
	// 	// save journal and tombstone
	// 	if t.journal.IsDirty() {
	// 		tx, err := t.db.Tx(true)
	// 		if err != nil {
	// 			return err
	// 		}
	// 		// be panic safe
	// 		defer tx.Rollback()
	// 		if err := t.flushJournalTx(ctx, tx); err != nil {
	// 			return err
	// 		}
	// 		// commit storage transaction
	// 		return tx.Commit()
	// 	}
	// }
	return nil
}

func (t *Table) UpdateTx(ctx context.Context, tx *Tx, val interface{}) error {
	if err := t.updateJournal(val); err != nil {
		return err
	}

	if t.journal.Len()+t.tombstone.Len() >= 1<<uint(t.opts.JournalSizeLog2) {
		if err := t.flushTx(ctx, tx); err != nil {
			return err
		}
	}
	// FIXME: flushing packed journal after every insert slows down by 10-20x
	// else {
	// 	if err := t.flushJournalTx(ctx, tx); err != nil {
	// 		return err
	// 	}
	// }
	return nil
}

func (t *Table) updateJournal(val interface{}) error {
	var batch []Item
	if v, ok := val.([]Item); ok {
		batch = v
	} else if i, ok := val.(Item); ok {
		batch = []Item{i}
	} else {
		return fmt.Errorf("type %T does not implement Item interface", val)
	}

	atomic.AddInt64(&t.stats.UpdateCalls, 1)
	atomic.AddInt64(&t.stats.UpdatedTuples, int64(len(batch)))

	SortItems(batch)

	var (
		lastmax  uint64
		needSort bool
	)

	col, _ := t.journal.Column(t.journal.pkindex)
	pk, _ := col.([]uint64)

	if len(pk) > 0 {
		lastmax = pk[len(pk)-1]
	}

	for i, v := range batch {
		id := v.ID()
		if id == 0 {
			return fmt.Errorf("pack: missing primary key on item %d %#v", i, v)
		}

		if off := vec.Uint64Slice(pk).Index(id, 0); off > -1 {
			if err := t.journal.ReplaceAt(off, v); err != nil {
				return err
			}
		} else {
			needSort = needSort || id < lastmax
			lastmax = util.MaxU64(lastmax, id)
			if err := t.journal.Push(v); err != nil {
				return err
			}
		}
	}

	if needSort {
		if err := t.journal.PkSort(); err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) Delete(ctx context.Context, q Query) (int64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if util.InterruptRequested(ctx) {
		return 0, ctx.Err()
	}

	tx, err := t.db.Tx(true)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	q.Fields = FieldList{t.Fields().Pk()}
	res, err := t.QueryTx(ctx, tx, q)
	if err != nil {
		return 0, err
	}
	defer res.Close()

	col, err := res.Uint64Column(t.Fields().Pk().Name)
	if err != nil {
		return 0, err
	}

	if err := t.DeleteIdsTx(ctx, tx, col); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}

	return int64(len(col)), nil
}

func (t *Table) DeleteIds(ctx context.Context, val []uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if util.InterruptRequested(ctx) {
		return ctx.Err()
	}

	if err := t.deleteJournal(val); err != nil {
		return err
	}

	if t.journal.Len()+t.tombstone.Len() >= 1<<uint(t.opts.JournalSizeLog2) {
		tx, err := t.db.Tx(true)
		if err != nil {
			return err
		}

		defer tx.Rollback()
		if err := t.flushTx(ctx, tx); err != nil {
			return err
		}

		return tx.Commit()
	}
	// FIXME: flushing packed journal after every insert slows down by 10-20x
	// else {
	// 	// save journal and tombstone
	// 	if t.journal.IsDirty() {
	// 		tx, err := t.db.Tx(true)
	// 		if err != nil {
	// 			return err
	// 		}
	// 		// be panic safe
	// 		defer tx.Rollback()
	// 		if err := t.flushJournalTx(ctx, tx); err != nil {
	// 			return err
	// 		}
	// 		// commit storage transaction
	// 		return tx.Commit()
	// 	}
	// }

	return nil
}

func (t *Table) DeleteIdsTx(ctx context.Context, tx *Tx, val []uint64) error {
	if err := t.deleteJournal(val); err != nil {
		return err
	}

	if t.journal.Len()+t.tombstone.Len() >= 1<<uint(t.opts.JournalSizeLog2) {
		if err := t.flushTx(ctx, tx); err != nil {
			return err
		}
	}
	// FIXME: flushing packed journal after every insert slows down by 10-20x
	// else {
	// 	if err := t.flushJournalTx(ctx, tx); err != nil {
	// 		return err
	// 	}
	// }

	return nil
}

func (t *Table) deleteJournal(ids []uint64) error {
	atomic.AddInt64(&t.stats.DeleteCalls, 1)

	var count int64
	for _, v := range ids {
		if v == 0 {
			continue
		}

		if err := t.tombstone.Push(Tombstone{Id: v}); err != nil {
			return err
		}
		count++
	}

	atomic.AddInt64(&t.stats.DeletedTuples, count)
	t.meta.Rows -= count
	t.meta.dirty = true

	if err := t.tombstone.PkSort(); err != nil {
		return err
	}
	return nil
}

func (t *Table) Close() error {
	log.Debugf("pack: closing %s table with %d/%d records", t.name,
		t.journal.Len(), t.tombstone.Len())
	t.mu.Lock()
	defer t.mu.Unlock()

	tx, err := t.db.Tx(true)
	if err != nil {
		return err
	}

	defer tx.Rollback()

	if t.meta.dirty {
		buf, err := json.Marshal(t.meta)
		if err != nil {
			return err
		}
		err = tx.tx.Bucket(t.metakey).Put(metaKey, buf)
		if err != nil {
			return err
		}
		t.meta.dirty = false
	}

	if t.journal.IsDirty() {
		_, err = tx.storePack(t.metakey, journalKey, t.journal, defaultJournalFillLevel)
		if err != nil {
			return err
		}
	}
	if t.tombstone.IsDirty() {
		_, err = tx.storePack(t.metakey, tombstoneKey, t.tombstone, defaultJournalFillLevel)
		if err != nil {
			return err
		}
	}

	if err := t.storePackHeaders(tx.tx); err != nil {
		return err
	}

	for _, idx := range t.indexes {
		if err := idx.CloseTx(tx); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (t *Table) FlushJournal(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if util.InterruptRequested(ctx) {
		return ctx.Err()
	}

	tx, err := t.db.Tx(true)
	if err != nil {
		return err
	}

	defer tx.Rollback()
	if err := t.flushJournalTx(ctx, tx); err != nil {
		return err
	}

	// store table metadata
	if t.meta.dirty {
		buf, err := json.Marshal(t.meta)
		if err != nil {
			return err
		}
		err = tx.tx.Bucket(t.metakey).Put(metaKey, buf)
		if err != nil {
			return err
		}
		t.meta.dirty = false
	}

	return tx.Commit()
}

func (t *Table) flushJournalTx(ctx context.Context, tx *Tx) error {
	if t.journal.IsDirty() {
		n, err := tx.storePack(t.metakey, journalKey, t.journal, defaultJournalFillLevel)
		if err != nil {
			return err
		}
		atomic.AddInt64(&t.stats.JournalFlushedTuples, int64(t.journal.Len()))
		atomic.AddInt64(&t.stats.JournalPacksStored, 1)
		atomic.AddInt64(&t.stats.JournalBytesWritten, int64(n))
	}
	if t.tombstone.IsDirty() {
		n, err := tx.storePack(t.metakey, tombstoneKey, t.tombstone, defaultJournalFillLevel)
		if err != nil {
			return err
		}
		atomic.AddInt64(&t.stats.TombstoneFlushedTuples, int64(t.tombstone.Len()))
		atomic.AddInt64(&t.stats.TombstonePacksStored, 1)
		atomic.AddInt64(&t.stats.TombstoneBytesWritten, int64(n))
	}
	return nil
}

func (t *Table) Flush(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if util.InterruptRequested(ctx) {
		return ctx.Err()
	}

	tx, err := t.db.Tx(true)
	if err != nil {
		return err
	}

	defer tx.Rollback()
	if err := t.flushTx(ctx, tx); err != nil {
		return err
	}

	return tx.Commit()
}

func (t *Table) flushTx(ctx context.Context, tx *Tx) error {
	var (
		nParts, nBytes, nUpd, nAdd, nDel int
		pUpd, pAdd, pDel                 int
		start                            time.Time = time.Now()
	)

	col, _ := t.tombstone.Column(0)
	dead, _ := col.([]uint64)
	col, _ = t.journal.Column(t.journal.pkindex)
	pk, _ := col.([]uint64)

	atomic.AddInt64(&t.stats.FlushCalls, 1)
	atomic.AddInt64(&t.stats.FlushedTuples, int64(t.journal.Len()+t.tombstone.Len()))

	log.Debugf("flush: %s table %d journal and %d tombstone records", t.name, len(pk), len(dead))
	for j, d, jl, dl := 0, 0, len(pk), len(dead); j < jl && d < dl; {
		for d < dl && dead[d] < pk[j] {
			d++
		}
		if d == dl {
			break
		}
		for j < jl && dead[d] > pk[j] {
			j++
		}
		if j == jl {
			break
		}
		if dead[d] == pk[j] {
			pk[j] = 0
			dead[d] = 0
			d++
			j++
			nDel++
		}
	}

	var (
		pkg                            *Package
		pkgsz                          int = 1 << uint(t.opts.PackSizeLog2)
		jpos, tpos, nextpack, lastpack int
		jlen, tlen                     int = len(pk), len(dead)
		needSort                       bool
		nextmax, lastmax               uint64
		err                            error
	)

	for {
		if jpos >= jlen && tpos >= tlen {
			break
		}

		for ; jpos < jlen && pk[jpos] == 0; jpos++ {
		}

		for ; tpos < tlen && dead[tpos] == 0; tpos++ {
		}

		var nextid uint64
		switch true {
		case jpos < jlen && tpos < tlen:
			nextid = util.MinU64(pk[jpos], dead[tpos])
		case jpos < jlen && tpos >= tlen:
			nextid = pk[jpos]
		case jpos >= jlen && tpos < tlen:
			nextid = dead[tpos]
		default:
			break
		}

		if lastpack < t.packs.Len() {
			nextpack, _, nextmax = t.findBestPack(nextid)
		}

		if lastpack != nextpack && pkg != nil {
			if pkg.IsDirty() {
				if needSort {
					pkg.PkSort()
				}
				n, err := t.storePack(tx, pkg)
				if err != nil {
					return err
				}
				nParts++
				nBytes += n
				if tx.Pending() >= txMaxSize {
					if err := t.storePackHeaders(tx.tx); err != nil {
						return err
					}
					if err := tx.CommitAndContinue(); err != nil {
						return err
					}
				}
				nextpack, _, nextmax = t.findBestPack(nextid)
			}
			lastpack = nextpack
			needSort = false
			lastmax = 0
			pkg = nil
			pAdd = 0
			pDel = 0
			pUpd = 0
		}

		if pkg == nil {
			if nextpack < t.packs.Len() {
				pkg, err = t.loadPack(tx, t.packs.heads[nextpack].Key, true, nil)
				if err != nil && err != ErrPackNotFound {
					return err
				}
				lastmax = nextmax
			}
			if pkg == nil {
				lastpack = t.packs.Len()
				lastmax = 0
				pkg = t.packPool.Get().(*Package)
				pkg.SetKey(t.nextPackKey())
				pkg.cached = false
			}
		}

		if tpos < tlen && lastmax > 0 {
			var ppos int
			for ; tpos < tlen; tpos++ {
				pkid := dead[tpos]

				if pkid == 0 {
					continue
				}

				if pkid > lastmax {
					break
				}

				for plen := pkg.Len(); ppos < plen; ppos++ {
					rowid, _ := pkg.Uint64At(pkg.pkindex, ppos)
					if rowid > pkid {
						dead[tpos] = 0
						break
					}
					if rowid == pkid {
						for _, idx := range t.indexes {
							if err := idx.RemoveTx(tx, pkg, ppos, 1); err != nil {
								return err
							}
						}
						pkg.Delete(ppos, 1)
						dead[tpos] = 0
						nDel++
						pDel++
						break
					}
				}
			}
		}

		for lastoffset := 0; jpos < jlen; jpos++ {
			pkid := pk[jpos]

			if pkid == 0 {
				continue
			}

			if best, _, _ := t.findBestPack(pkid); best != lastpack {
				break
			}

			if offs := pkg.PkIndex(pkid, lastoffset); offs > -1 {
				lastoffset = offs
				for _, idx := range t.indexes {
					if !idx.Field.Type.EqualPacksAt(
						pkg, idx.Field.Index, offs,
						t.journal, idx.Field.Index, jpos,
					) {
						if err := idx.RemoveTx(tx, pkg, offs, 1); err != nil {
							return err
						}
						if err := idx.AddTx(tx, t.journal, jpos, 1); err != nil {
							return err
						}
					}
				}
				if err := pkg.CopyFrom(t.journal, offs, jpos, 1); err != nil {
					return err
				}
				nUpd++
				pUpd++
			} else {
				if pkg.Len() >= pkgsz {
					bmin, bmax := t.packs.MinMax(lastpack)
					if lastpack < t.packs.Len() && pkid > bmin && pkid < bmax {
						log.Warnf("flush: %s table splitting full pack %x (%d/%d) with min=%d max=%d on out-of-order insert pk %d",
							t.name, pkg.Key(), lastpack, t.packs.Len(), bmin, bmax, pkid)
						if needSort {
							pkg.PkSort()
							needSort = false
						}
						n, err := t.splitPack(tx, pkg)
						if err != nil {
							return err
						}
						nParts++
						nBytes += n
						if tx.Pending() >= txMaxSize {
							if err := t.storePackHeaders(tx.tx); err != nil {
								return err
							}
							if err := tx.CommitAndContinue(); err != nil {
								return err
							}
						}
						break

					} else {
						if needSort {
							pkg.PkSort()
							needSort = false
						}
						n, err := t.storePack(tx, pkg)
						if err != nil {
							return err
						}
						nParts++
						nBytes += n
						if tx.Pending() >= txMaxSize {
							if err := t.storePackHeaders(tx.tx); err != nil {
								return err
							}
							if err := tx.CommitAndContinue(); err != nil {
								return err
							}
						}
						break
					}
				}
				if err := pkg.AppendFrom(t.journal, jpos, 1, false); err != nil {
					return err
				}
				needSort = needSort || pkid < lastmax
				lastmax = util.MaxU64(lastmax, pkid)
				lastoffset = pkg.Len() - 1
				nAdd++
				pAdd++
				for _, idx := range t.indexes {
					if err := idx.AddTx(tx, pkg, lastoffset, 1); err != nil {
						return err
					}
				}
			}
		}
	}

	if pkg != nil && pkg.IsDirty() {
		if needSort {
			pkg.PkSort()
		}
		n, err := t.storePack(tx, pkg)
		if err != nil {
			return err
		}
		nParts++
		nBytes += n
	}

	log.Debugf("flush: %s table %d packs add=%d del=%d total_size=%s in %s",
		t.name, nParts, nAdd, nDel, util.ByteSize(nBytes), time.Since(start))

	for _, idx := range t.indexes {
		if err := idx.FlushTx(ctx, tx); err != nil {
			return err
		}
	}

	if tlen > nDel {
		t.meta.Rows += int64(tlen - nDel)
		t.meta.dirty = true
	}

	if t.meta.dirty {
		buf, err := json.Marshal(t.meta)
		if err != nil {
			return err
		}
		err = tx.tx.Bucket(t.metakey).Put(metaKey, buf)
		if err != nil {
			return err
		}
		t.meta.dirty = false
	}

	if err := t.storePackHeaders(tx.tx); err != nil {
		return err
	}

	t.journal.Clear()
	t.tombstone.Clear()

	return t.flushJournalTx(ctx, tx)
}

func (t Table) findBestPack(pkval uint64) (int, uint64, uint64) {
	bestpack, min, max := t.packs.Best(pkval)

	if t.packs.Len() == 0 || pkval-min <= max-min {
		return bestpack, min, max
	}

	if t.packs.heads[bestpack].NValues >= 1<<uint(t.opts.PackSizeLog2) {
		return t.packs.Len(), 0, 0
	}

	return bestpack, min, max
}

func (t *Table) Lookup(ctx context.Context, ids []uint64) (*Result, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if util.InterruptRequested(ctx) {
		return nil, ctx.Err()
	}

	tx, err := t.db.Tx(false)
	if err != nil {
		return nil, err
	}
	res, err := t.LookupTx(ctx, tx, ids)
	tx.Rollback()
	return res, err
}

func (t *Table) LookupTx(ctx context.Context, tx *Tx, ids []uint64) (*Result, error) {
	res := &Result{
		fields: t.Fields(),
		pkg:    t.packPool.Get().(*Package),
		table:  t,
	}

	atomic.AddInt64(&t.stats.QueryCalls, 1)

	q := NewQuery(t.name+".lookup", t)

	ids = vec.UniqueUint64Slice(ids)
	if len(ids) > 0 && ids[0] == 0 {
		ids = ids[1:]
	}
	maxRows := len(ids)

	var maxNonZeroId uint64
	if maxRows > 0 {
		maxNonZeroId = ids[maxRows-1]
	} else {
		return res, nil
	}

	defer func() {
		atomic.AddInt64(&t.stats.QueriedTuples, int64(q.rowsMatched))
		q.Close()
	}()

	pkidx := t.journal.pkindex
	col, _ := t.journal.Column(pkidx)
	pk, _ := col.([]uint64)

	if t.tombstone.Len() > 0 {
		for i, v := range ids {
			if v == 0 || t.tombstone.PkIndex(v, 0) >= 0 {
				ids[i] = 0
			} else {
				maxNonZeroId = v
			}
		}
	}

	if t.journal.Len() > 0 {
		var last int
		for i, v := range ids {
			if pk[len(pk)-1] < v || pk[last] > maxNonZeroId {
				break
			}
			j := t.journal.PkIndex(v, last)
			if j < 0 {
				continue
			}
			if err := res.pkg.AppendFrom(t.journal, j, 1, true); err != nil {
				res.Close()
				return nil, err
			}
			q.rowsMatched++

			ids[i] = 0
			last = j
		}
	}
	q.journalTime = time.Since(q.lap)

	if maxRows == q.rowsMatched {
		return res, nil
	}

	if q.rowsMatched > 0 || t.tombstone.Len() > 0 {
		clean := make([]uint64, 0, len(ids))
		for _, v := range ids {
			if v != 0 {
				clean = append(clean, v)
				maxNonZeroId = v
			}
		}
		ids = clean
	}

	if len(ids) == 0 {
		return res, nil
	}

	q.lap = time.Now()
	var nextid int
	for _, nextpack := range q.MakePackLookupSchedule(ids, false) {
		if maxRows == q.rowsMatched {
			break
		}

		if util.InterruptRequested(ctx) {
			res.Close()
			return nil, ctx.Err()
		}

		_, max := t.packs.MinMax(nextpack)
		pkg, err := t.loadPack(tx, t.packs.heads[nextpack].Key, true, q.reqfields)
		if err != nil {
			res.Close()
			return nil, err
		}
		q.packsScanned++

		col, _ := pkg.Column(pkidx)
		pk, _ := col.([]uint64)

		last := 0
		for i, v := range ids[nextid:] {
			if max < v || pk[last] > maxNonZeroId {
				break
			}
			j := pkg.PkIndex(v, last)
			if j < 0 {
				continue
			}
			if err := res.pkg.AppendFrom(pkg, j, 1, true); err != nil {
				res.Close()
				return nil, err
			}
			nextid = i
			q.rowsMatched++
			last = j
		}
	}
	q.scanTime = time.Since(q.lap)
	return res, nil
}

func (t *Table) Query(ctx context.Context, q Query) (*Result, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if util.InterruptRequested(ctx) {
		return nil, ctx.Err()
	}

	tx, err := t.db.Tx(false)
	if err != nil {
		return nil, err
	}

	defer tx.Rollback()
	if q.Order == OrderAsc {
		return t.QueryTx(ctx, tx, q)
	} else {
		return t.QueryTxDesc(ctx, tx, q)
	}
}

func (t *Table) QueryTx(ctx context.Context, tx *Tx, q Query) (*Result, error) {
	atomic.AddInt64(&t.stats.QueryCalls, 1)

	if err := q.Compile(t); err != nil {
		return nil, err
	}

	var jbits *vec.BitSet

	defer func() {
		atomic.AddInt64(&t.stats.QueriedTuples, int64(q.rowsMatched))
		q.Close()
		if jbits != nil {
			jbits.Close()
		}
	}()

	jbits = q.Conditions.MatchPack(t.journal, PackageHeader{})
	q.journalTime = time.Since(q.lap)

	if err := q.QueryIndexes(ctx, tx); err != nil {
		return nil, err
	}

	pkg := t.packPool.Get().(*Package)
	pkg.KeepFields(q.reqfields)
	pkg.UpdateAliasesFrom(q.reqfields)

	res := &Result{
		fields: q.reqfields,
		pkg:    pkg,
		table:  t,
	}

	if jbits.Count() == 0 && q.IsEmptyMatch() {
		return res, nil
	}

	if !q.IsEmptyMatch() {
		q.lap = time.Now()
	packloop:
		for _, p := range q.MakePackSchedule(false) {
			if util.InterruptRequested(ctx) {
				res.Close()
				return nil, ctx.Err()
			}

			pkg, err := t.loadPack(tx, t.packs.heads[p].Key, !q.NoCache, q.reqfields)
			if err != nil {
				res.Close()
				return nil, err
			}
			q.packsScanned++

			bits := q.Conditions.MatchPack(pkg, t.packs.heads[p])
			for idx, length := bits.Run(0); idx >= 0; idx, length = bits.Run(idx + length) {
				for i := idx; i < idx+length; i++ {
					pkid, err := pkg.Uint64At(pkg.pkindex, i)
					if err != nil {
						continue
					}

					if t.tombstone.PkIndex(pkid, 0) >= 0 {
						continue
					}

					src := pkg
					index := i

					if j := t.journal.PkIndex(pkid, 0); j >= 0 {
						if !jbits.IsSet(j) {
							continue
						}

						jbits.Clear(j)
						src = t.journal
						index = j
					}

					if err := res.pkg.AppendFrom(src, index, 1, true); err != nil {
						bits.Close()
						res.Close()
						return nil, err
					}
					q.rowsMatched++

					if q.Limit > 0 && q.rowsMatched == q.Limit {
						bits.Close()
						break packloop
					}
				}
			}
			bits.Close()
		}
		q.scanTime = time.Since(q.lap)
		q.lap = time.Now()
	}

	if q.Limit > 0 && q.rowsMatched >= q.Limit {
		return res, nil
	}

	for idx, length := jbits.Run(0); idx >= 0; idx, length = jbits.Run(idx + length) {
		for i := idx; i < idx+length; i++ {
			pkid, err := t.journal.Uint64At(t.journal.pkindex, i)
			if err != nil {
				continue
			}

			if t.tombstone.PkIndex(pkid, 0) >= 0 {
				continue
			}

			if err := res.pkg.AppendFrom(t.journal, i, 1, true); err != nil {
				res.Close()
				return nil, err
			}
			q.rowsMatched++

			if q.Limit > 0 && q.rowsMatched == q.Limit {
				break
			}
		}
	}
	q.journalTime = time.Since(q.lap)

	return res, nil
}

func (t *Table) QueryTxDesc(ctx context.Context, tx *Tx, q Query) (*Result, error) {
	atomic.AddInt64(&t.stats.QueryCalls, 1)

	if err := q.Compile(t); err != nil {
		return nil, err
	}

	var jbits *vec.BitSet

	defer func() {
		atomic.AddInt64(&t.stats.QueriedTuples, int64(q.rowsMatched))
		q.Close()
		if jbits != nil {
			jbits.Close()
		}
	}()

	jbits = q.Conditions.MatchPack(t.journal, PackageHeader{}).Reverse()
	q.journalTime = time.Since(q.lap)

	if err := q.QueryIndexes(ctx, tx); err != nil {
		return nil, err
	}

	pkg := t.packPool.Get().(*Package)
	pkg.KeepFields(q.reqfields)
	pkg.UpdateAliasesFrom(q.reqfields)

	res := &Result{
		fields: q.reqfields,
		pkg:    pkg,
		table:  t,
	}

	if jbits.Count() == 0 && q.IsEmptyMatch() {
		return res, nil
	}

	_, maxPackedPk := t.packs.GlobalMinMax()

	for idx, length := jbits.Run(jbits.Size() - 1); idx >= 0; idx, length = jbits.Run(idx - length) {
		for i := idx; i > idx-length; i-- {
			pkid, err := t.journal.Uint64At(t.journal.pkindex, i)
			if err != nil {
				continue
			}

			if pkid <= maxPackedPk {
				continue
			}

			if t.tombstone.PkIndex(pkid, 0) >= 0 {
				continue
			}

			if err := res.pkg.AppendFrom(t.journal, i, 1, true); err != nil {
				res.Close()
				return nil, err
			}
			q.rowsMatched++
			jbits.Clear(i)

			if q.Limit > 0 && q.rowsMatched == q.Limit {
				break
			}
		}
	}
	q.journalTime = time.Since(q.lap)

	if !q.IsEmptyMatch() {
		q.lap = time.Now()
	packloop:
		for _, p := range q.MakePackSchedule(true) {
			if util.InterruptRequested(ctx) {
				res.Close()
				return nil, ctx.Err()
			}

			pkg, err := t.loadPack(tx, t.packs.heads[p].Key, !q.NoCache, q.reqfields)
			if err != nil {
				res.Close()
				return nil, err
			}
			q.packsScanned++

			bits := q.Conditions.MatchPack(pkg, t.packs.heads[p]).Reverse()
			for idx, length := bits.Run(bits.Size() - 1); idx >= 0; idx, length = bits.Run(idx - length) {
				for i := idx; i > idx-length; i-- {
					pkid, err := pkg.Uint64At(pkg.pkindex, i)
					if err != nil {
						continue
					}

					if t.tombstone.PkIndex(pkid, 0) >= 0 {
						continue
					}

					src := pkg
					index := i

					if j := t.journal.PkIndex(pkid, 0); j >= 0 {
						if !jbits.IsSet(j) {
							continue
						}
						jbits.Clear(j)
						src = t.journal
						index = j
					}

					if err := res.pkg.AppendFrom(src, index, 1, true); err != nil {
						bits.Close()
						res.Close()
						return nil, err
					}
					q.rowsMatched++

					if q.Limit > 0 && q.rowsMatched == q.Limit {
						bits.Close()
						break packloop
					}
				}
			}
			bits.Close()
		}
		q.scanTime = time.Since(q.lap)
		q.lap = time.Now()
	}

	return res, nil
}

func (t *Table) Count(ctx context.Context, q Query) (int64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if util.InterruptRequested(ctx) {
		return 0, ctx.Err()
	}

	tx, err := t.db.Tx(false)
	if err != nil {
		return 0, err
	}

	defer tx.Rollback()
	return t.CountTx(ctx, tx, q)
}

func (t *Table) CountTx(ctx context.Context, tx *Tx, q Query) (int64, error) {
	atomic.AddInt64(&t.stats.QueryCalls, 1)

	if err := q.Compile(t); err != nil {
		return 0, err
	}

	var jbits *vec.BitSet

	defer func() {
		atomic.AddInt64(&t.stats.QueriedTuples, int64(q.rowsMatched))
		jbits.Close()
		q.Close()
	}()

	jbits = q.Conditions.MatchPack(t.journal, PackageHeader{})
	q.journalTime = time.Since(q.lap)

	if err := q.QueryIndexes(ctx, tx); err != nil {
		return 0, err
	}

	if jbits.Count() == 0 && q.IsEmptyMatch() {
		return 0, nil
	}

	if !q.IsEmptyMatch() {
		q.lap = time.Now()
		for _, p := range q.MakePackSchedule(false) {
			if util.InterruptRequested(ctx) {
				return int64(q.rowsMatched), ctx.Err()
			}

			pkg, err := t.loadPack(tx, t.packs.heads[p].Key, !q.NoCache, q.reqfields)
			if err != nil {
				return 0, err
			}
			q.packsScanned++

			bits := q.Conditions.MatchPack(pkg, t.packs.heads[p])
			for idx, length := bits.Run(0); idx >= 0; idx, length = bits.Run(idx + length) {
				for i := idx; i < idx+length; i++ {
					pkid, err := pkg.Uint64At(pkg.pkindex, i)
					if err != nil {
						continue
					}

					if t.tombstone.PkIndex(pkid, 0) >= 0 {
						continue
					}

					if j := t.journal.PkIndex(pkid, 0); j >= 0 {
						if !jbits.IsSet(j) {
							continue
						}
						jbits.Clear(j)
					}

					q.rowsMatched++
				}
			}
			bits.Close()
		}
		q.scanTime = time.Since(q.lap)
	}

	q.rowsMatched += int(jbits.Count())

	return int64(q.rowsMatched), nil
}

func (t *Table) Stream(ctx context.Context, q Query, fn func(r Row) error) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if util.InterruptRequested(ctx) {
		return ctx.Err()
	}

	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}

	defer tx.Rollback()
	if q.Order == OrderAsc {
		return t.StreamTx(ctx, tx, q, fn)
	} else {
		return t.StreamTxDesc(ctx, tx, q, fn)
	}
}

func (t *Table) StreamTx(ctx context.Context, tx *Tx, q Query, fn func(r Row) error) error {
	atomic.AddInt64(&t.stats.StreamCalls, 1)

	if err := q.Compile(t); err != nil {
		return err
	}

	var jbits *vec.BitSet

	defer func() {
		atomic.AddInt64(&t.stats.StreamedTuples, int64(q.rowsMatched))
		if jbits != nil {
			jbits.Close()
		}
		q.Close()
	}()

	jbits = q.Conditions.MatchPack(t.journal, PackageHeader{})
	q.journalTime = time.Since(q.lap)

	if err := q.QueryIndexes(ctx, tx); err != nil {
		return err
	}

	if jbits.Count() == 0 && q.IsEmptyMatch() {
		return nil
	}

	res := Result{fields: q.reqfields}

	if !q.IsEmptyMatch() {
		q.lap = time.Now()
	packloop:
		for _, p := range q.MakePackSchedule(false) {
			if util.InterruptRequested(ctx) {
				return ctx.Err()
			}

			pkg, err := t.loadPack(tx, t.packs.heads[p].Key, !q.NoCache, q.reqfields)
			if err != nil {
				return err
			}
			q.packsScanned++

			bits := q.Conditions.MatchPack(pkg, t.packs.heads[p])
			for idx, length := bits.Run(0); idx >= 0; idx, length = bits.Run(idx + length) {
				for i := idx; i < idx+length; i++ {
					pkid, err := pkg.Uint64At(pkg.pkindex, i)
					if err != nil {
						continue
					}

					if t.tombstone.PkIndex(pkid, 0) >= 0 {
						continue
					}

					res.pkg = pkg
					index := i

					if j := t.journal.PkIndex(pkid, 0); j >= 0 {
						if !jbits.IsSet(j) {
							continue
						}
						res.pkg = t.journal
						index = j
						jbits.Clear(j)
					}

					if err := fn(Row{res: &res, n: index}); err != nil {
						bits.Close()
						return err
					}
					res.pkg = nil
					q.rowsMatched++

					if q.Limit > 0 && q.rowsMatched >= q.Limit {
						bits.Close()
						break packloop
					}
				}
			}
			bits.Close()
		}
		q.scanTime = time.Since(q.lap)
		q.lap = time.Now()
	}

	if q.Limit > 0 && q.rowsMatched >= q.Limit {
		return nil
	}

	res.pkg = t.journal
	for idx, length := jbits.Run(0); idx >= 0; idx, length = jbits.Run(idx + length) {
		for i := idx; i < idx+length; i++ {
			pkid, err := t.journal.Uint64At(t.journal.pkindex, i)
			if err != nil {
				continue
			}

			if t.tombstone.PkIndex(pkid, 0) >= 0 {
				continue
			}

			if err := fn(Row{res: &res, n: i}); err != nil {
				return err
			}
			q.rowsMatched++

			if q.Limit > 0 && q.rowsMatched >= q.Limit {
				return nil
			}
		}
	}
	q.journalTime += time.Since(q.lap)

	return nil
}

func (t *Table) StreamTxDesc(ctx context.Context, tx *Tx, q Query, fn func(r Row) error) error {
	atomic.AddInt64(&t.stats.StreamCalls, 1)

	if err := q.Compile(t); err != nil {
		return err
	}

	var jbits *vec.BitSet

	defer func() {
		atomic.AddInt64(&t.stats.StreamedTuples, int64(q.rowsMatched))
		if jbits != nil {
			jbits.Close()
		}
		q.Close()
	}()

	jbits = q.Conditions.MatchPack(t.journal, PackageHeader{}).Reverse()
	q.journalTime = time.Since(q.lap)

	if err := q.QueryIndexes(ctx, tx); err != nil {
		return err
	}

	if jbits.Count() == 0 && q.IsEmptyMatch() {
		return nil
	}

	_, maxPackedPk := t.packs.GlobalMinMax()
	res := Result{fields: q.reqfields}
	res.pkg = t.journal

	for idx, length := jbits.Run(jbits.Size() - 1); idx >= 0; idx, length = jbits.Run(idx - length) {
		for i := idx; i > idx-length; i-- {
			pkid, err := t.journal.Uint64At(t.journal.pkindex, i)
			if err != nil {
				continue
			}

			if pkid <= maxPackedPk {
				continue
			}

			if t.tombstone.PkIndex(pkid, 0) >= 0 {
				continue
			}

			if err := fn(Row{res: &res, n: i}); err != nil {
				return err
			}
			q.rowsMatched++

			if q.Limit > 0 && q.rowsMatched >= q.Limit {
				return nil
			}
		}
	}
	q.journalTime += time.Since(q.lap)

	if !q.IsEmptyMatch() {
		q.lap = time.Now()
	packloop:
		for _, p := range q.MakePackSchedule(true) {
			if util.InterruptRequested(ctx) {
				return ctx.Err()
			}

			pkg, err := t.loadPack(tx, t.packs.heads[p].Key, !q.NoCache, q.reqfields)
			if err != nil {
				return err
			}
			q.packsScanned++

			bits := q.Conditions.MatchPack(pkg, t.packs.heads[p]).Reverse()
			for idx, length := bits.Run(bits.Size() - 1); idx >= 0; idx, length = bits.Run(idx - length) {
				for i := idx; i > idx-length; i-- {
					pkid, err := pkg.Uint64At(pkg.pkindex, i)
					if err != nil {
						continue
					}

					if t.tombstone.PkIndex(pkid, 0) >= 0 {
						continue
					}

					res.pkg = pkg
					index := i

					if j := t.journal.PkIndex(pkid, 0); j >= 0 {
						if !jbits.IsSet(j) {
							continue
						}
						res.pkg = t.journal
						index = j
						jbits.Clear(j)
					}

					if err := fn(Row{res: &res, n: index}); err != nil {
						bits.Close()
						return err
					}
					res.pkg = nil
					q.rowsMatched++

					if q.Limit > 0 && q.rowsMatched >= q.Limit {
						bits.Close()
						break packloop
					}
				}
			}
			bits.Close()
		}
		q.scanTime = time.Since(q.lap)
		q.lap = time.Now()
	}

	return nil
}

func (t *Table) StreamLookup(ctx context.Context, ids []uint64, fn func(r Row) error) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if util.InterruptRequested(ctx) {
		return ctx.Err()
	}

	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	return t.StreamLookupTx(ctx, tx, ids, fn)
}

func (t *Table) StreamLookupTx(ctx context.Context, tx *Tx, ids []uint64, fn func(r Row) error) error {
	atomic.AddInt64(&t.stats.StreamCalls, 1)
	q := NewQuery(t.name+".stream-lookup", t)

	ids = vec.UniqueUint64Slice(ids)
	if len(ids) > 0 && ids[0] == 0 {
		ids = ids[1:]
	}
	maxRows := len(ids)
	var maxNonZeroId uint64
	if maxRows > 0 {
		maxNonZeroId = ids[maxRows-1]
	} else {
		return nil
	}

	defer func() {
		atomic.AddInt64(&t.stats.StreamedTuples, int64(q.rowsMatched))
		q.Close()
	}()

	if t.tombstone.Len() > 0 {
		for i, v := range ids {
			if v == 0 || t.tombstone.PkIndex(v, 0) >= 0 {
				ids[i] = 0
			} else {
				maxNonZeroId = v
			}
		}
	}

	res := Result{
		fields: t.Fields(),
		pkg:    t.journal,
	}

	pkidx := t.journal.pkindex
	col, _ := t.journal.Column(pkidx)
	pk, _ := col.([]uint64)

	if t.journal.Len() > 0 {
		var last int
		for i, v := range ids {
			if pk[len(pk)-1] < v || pk[last] > maxNonZeroId {
				break
			}
			j := t.journal.PkIndex(v, last)
			if j < 0 {
				continue
			}
			if err := fn(Row{res: &res, n: j}); err != nil {
				return err
			}
			q.rowsMatched++

			ids[i] = 0
			last = j
		}
		q.journalTime = time.Since(q.lap)
	}

	if maxRows == q.rowsMatched {
		return nil
	}

	if q.rowsMatched > 0 || t.tombstone.Len() > 0 {
		clean := make([]uint64, 0, len(ids))
		for _, v := range ids {
			if v != 0 {
				clean = append(clean, v)
				maxNonZeroId = v
			}
		}
		ids = clean
	}

	if len(ids) == 0 {
		return nil
	}

	var nextid int
	q.lap = time.Now()
	for _, nextpack := range q.MakePackLookupSchedule(ids, false) {
		if maxRows == q.rowsMatched {
			break
		}

		if util.InterruptRequested(ctx) {
			return ctx.Err()
		}

		pkg, err := t.loadPack(tx, t.packs.heads[nextpack].Key, true, q.reqfields)
		if err != nil {
			return err
		}
		res.pkg = pkg
		q.packsScanned++
		col, _ := pkg.Column(pkidx)
		pk, _ := col.([]uint64)
		last := 0
		_, max := t.packs.MinMax(nextpack)

		for _, v := range ids[nextid:] {
			if max < v || pk[last] > maxNonZeroId {
				break
			}
			j := pkg.PkIndex(v, last)

			if j < 0 {
				continue
			}

			if err := fn(Row{res: &res, n: j}); err != nil {
				return err
			}

			nextid++
			q.rowsMatched++
			last = j
		}
	}
	q.scanTime = time.Since(q.lap)
	return nil
}

func (t *Table) Compact(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if util.InterruptRequested(ctx) {
		return ctx.Err()
	}

	if t.packs.Len() <= 1 {
		return nil
	}

	var (
		maxsz                 int = 1 << uint(t.opts.PackSizeLog2)
		srcSize               int64
		nextpack              uint32
		needCompact           bool
		total, moved, written int64
	)
	for i, v := range t.packs.heads {
		key := bigEndian.Uint32(v.Key)
		needCompact = needCompact || key > nextpack
		needCompact = needCompact || (i < t.packs.Len()-1 && v.NValues < maxsz)
		nextpack++
		total += int64(v.NValues)
		srcSize += int64(v.PackSize)
	}
	if !needCompact {
		log.Infof("pack: %s table %d packs / %d rows already compact", t.name, t.packs.Len(), total)
		return nil
	}

	tx, err := t.db.Tx(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var (
		dstPack, srcPack *Package
		dstSize          int64
		dstIndex         int
		lastMaxPk        uint64
		isNewPack        bool
	)

	log.Infof("pack: compacting %s table %d packs / %d rows", t.name, t.packs.Len(), total)

	for {
		if dstPack == nil {
			dstKey := t.partkey(dstIndex)
			if bytes.Compare(dstKey, t.packs.heads[dstIndex].Key) == 0 {
				if t.packs.heads[dstIndex].NValues == maxsz {
					log.Debugf("pack: skipping full dst pack %x", dstKey)
					dstIndex++
					continue
				}
				pmin, pmax := t.packs.MinMax(dstIndex)
				if pmin < lastMaxPk {
					log.Debugf("pack: skipping out-of-order dst pack %x", dstKey)
					dstIndex++
					continue
				}

				log.Debugf("pack: loading dst pack %d:%x", dstIndex, dstKey)
				dstPack, err = t.loadPack(tx, dstKey, false, nil)
				if err != nil {
					return err
				}
				lastMaxPk = pmax
				isNewPack = false
			} else {
				log.Debugf("pack: creating new dst pack %d:%x", dstIndex, dstKey)
				dstPack = t.packPool.Get().(*Package)
				dstPack.key = dstKey
				isNewPack = true
			}
		}

		if srcPack == nil {
			minSlice, _ := t.packs.MinMaxSlices()
			var startIndex, srcIndex int = dstIndex, -1
			var lastmin uint64 = math.MaxUint64
			if isNewPack {
				startIndex--
			}
			for i := startIndex; i < len(minSlice); i++ {
				currmin := minSlice[i]
				if currmin <= lastMaxPk {
					continue
				}
				if lastmin > currmin {
					lastmin = currmin
					srcIndex = i
				}
			}

			if srcIndex < 0 {
				break
			}

			ph := t.packs.heads[srcIndex]
			log.Debugf("pack: loading src pack %d:%x", srcIndex, ph.Key)
			srcPack, err = t.loadPack(tx, ph.Key, false, nil)
			if err != nil {
				return err
			}
		}

		free := maxsz - dstPack.Len()
		cp := util.Min(free, srcPack.Len())
		moved += int64(cp)

		log.Debugf("pack: moving %d/%d rows from pack %x to %x", cp, srcPack.Len(),
			srcPack.key, dstPack.key)
		if err := dstPack.AppendFrom(srcPack, 0, cp, true); err != nil {
			return err
		}
		if err := srcPack.Delete(0, cp); err != nil {
			return err
		}
		total += int64(cp)
		lastMaxPk, err = dstPack.Uint64At(dstPack.pkindex, dstPack.Len()-1)
		if err != nil {
			return err
		}

		if dstPack.Len() == maxsz {
			log.Debugf("pack: storing full dst pack %x", dstPack.key)
			n, err := t.storePack(tx, dstPack)
			if err != nil {
				return err
			}
			dstSize += int64(n)
			dstIndex++
			written += int64(maxsz)
			dstPack = nil
		}

		if srcPack.Len() == 0 {
			log.Debugf("pack: deleting empty src pack %x", srcPack.key)
		}

		if _, err := t.storePack(tx, srcPack); err != nil {
			return err
		}

		srcPack = nil

		if tx.Pending() >= txMaxSize {
			if err := t.storePackHeaders(tx.tx); err != nil {
				return err
			}
			if err := tx.CommitAndContinue(); err != nil {
				return err
			}
			if util.InterruptRequested(ctx) {
				return ctx.Err()
			}
		}
	}

	if dstPack != nil {
		log.Debugf("pack: storing last dst pack %x", dstPack.key)
		n, err := t.storePack(tx, dstPack)
		if err != nil {
			return err
		}
		dstSize += int64(n)
		written += int64(dstPack.Len())
	}

	log.Infof("pack: compacted %d/%d rows from %s table into %d packs (%s ->> %s)",
		moved, written, t.name, t.packs.Len(), util.ByteSize(srcSize), util.ByteSize(dstSize))

	if err := t.storePackHeaders(tx.tx); err != nil {
		return err
	}

	return tx.Commit()
}

func (t Table) partkey(id int) []byte {
	var buf [4]byte
	bigEndian.PutUint32(buf[:], uint32(id))
	return buf[:]
}

func (t Table) nextPackKey() []byte {
	switch l := t.packs.Len(); l {
	case 0:
		return t.partkey(0)
	default:
		maxid := bigEndian.Uint32(t.packs.heads[l-1].Key)
		return t.partkey(int(maxid + 1))
	}
}

func (t Table) cachekey(key []byte) string {
	return t.name + "/" + hex.EncodeToString(key)
}

func (t *Table) loadPack(tx *Tx, key []byte, touch bool, fields FieldList) (*Package, error) {
	stripped := len(fields) > 0 && len(fields) < len(t.Fields())
	cachefn := t.cache.Peek
	if touch {
		cachefn = t.cache.Get
	}
	cachekey := t.cachekey(key)
	if cached, ok := cachefn(cachekey); ok {
		atomic.AddInt64(&t.stats.PackCacheHits, 1)
		return cached.(*Package), nil
	}
	if stripped {
		cachekey += "#" + fields.Key()
		if cached, ok := cachefn(cachekey); ok {
			atomic.AddInt64(&t.stats.PackCacheHits, 1)
			return cached.(*Package), nil
		}
	}

	atomic.AddInt64(&t.stats.PackCacheMisses, 1)
	var (
		err error
	)
	pkg := t.packPool.Get().(*Package)
	if stripped {
		pkg = pkg.KeepFields(fields)
	}
	pkg, err = tx.loadPack(t.key, key, pkg)
	if err != nil {
		return nil, err
	}
	atomic.AddInt64(&t.stats.PacksLoaded, 1)
	atomic.AddInt64(&t.stats.PackBytesRead, int64(pkg.packedsize))
	pkg.SetKey(key)
	pkg.tinfo = t.journal.tinfo
	pkg.pkindex = t.fields.PkIndex()
	pkg.cached = touch
	if touch {
		updated, _ := t.cache.Add(cachekey, pkg)
		if updated {
			atomic.AddInt64(&t.stats.PackCacheUpdates, 1)
		} else {
			atomic.AddInt64(&t.stats.PackCacheInserts, 1)
		}
	}
	return pkg, nil
}

func (t *Table) storePack(tx *Tx, pkg *Package) (int, error) {
	key := pkg.Key()
	cachekey := t.cachekey(key)
	if len(key) == 0 {
		log.Errorf("pack: %s table store called with empty pack key", t.name)
	}
	if pkg.Len() > 0 {
		n, err := tx.storePack(t.key, key, pkg, t.opts.FillLevel)
		if err != nil {
			return 0, err
		}
		t.packs.AddOrUpdate(pkg.Header())
		atomic.AddInt64(&t.stats.PacksStored, 1)
		atomic.AddInt64(&t.stats.PackBytesWritten, int64(n))
		if pkg.cached {
			inserted, _ := t.cache.ContainsOrAdd(cachekey, pkg)
			if inserted {
				atomic.AddInt64(&t.stats.PackCacheInserts, 1)
			} else {
				atomic.AddInt64(&t.stats.PackCacheUpdates, 1)
			}
		}
		prefix := cachekey + "#"
		for _, v := range t.cache.Keys() {
			if strings.HasPrefix(v.(string), prefix) {
				t.cache.Remove(v)
			}
		}
		return n, nil
	}
	t.packs.Remove(pkg.Header())
	if err := tx.deletePack(t.key, key); err != nil {
		return 0, err
	}
	t.cache.Remove(cachekey)
	prefix := cachekey + "#"
	for _, v := range t.cache.Keys() {
		if strings.HasPrefix(v.(string), prefix) {
			t.cache.Remove(v)
		}
	}
	return 0, nil
}

func (t *Table) splitPack(tx *Tx, pkg *Package) (int, error) {
	newpkg := t.packPool.Get().(*Package)
	newpkg.cached = false
	half := pkg.Len() / 2
	if err := newpkg.AppendFrom(pkg, half, pkg.Len()-half, true); err != nil {
		return 0, err
	}
	if err := pkg.Delete(half, pkg.Len()-half); err != nil {
		return 0, err
	}
	_, err := t.storePack(tx, pkg)
	if err != nil {
		return 0, err
	}
	newpkg.key = t.partkey(t.packs.Len())
	n, err := t.storePack(tx, newpkg)
	if err != nil {
		return 0, err
	}
	t.recyclePackage(newpkg)
	return n, nil
}

func (t *Table) makePackage() interface{} {
	atomic.AddInt64(&t.stats.PacksAlloc, 1)
	pkg := t.journal.Clone(false, 1<<uint(t.opts.PackSizeLog2))
	return pkg
}

func (t *Table) onEvictedPackage(key, val interface{}) {
	pkg := val.(*Package)
	pkg.cached = false
	atomic.AddInt64(&t.stats.PackCacheEvictions, 1)
	t.recyclePackage(pkg)
}

func (t *Table) recyclePackage(pkg *Package) {
	if pkg == nil || pkg.cached {
		return
	}
	if pkg.stripped {
		pkg.Release()
		return
	}
	if c := pkg.Cap(); c < 0 || c > 1<<uint(t.opts.PackSizeLog2) {
		pkg.Release()
		return
	}
	pkg.Clear()
	atomic.AddInt64(&t.stats.PacksRecycled, 1)
	t.packPool.Put(pkg)
}

func (t *Table) Size() TableSizeStats {
	t.mu.Lock()
	defer t.mu.Unlock()
	var sz TableSizeStats
	for _, idx := range t.indexes {
		sz.IndexSize += idx.Size().TotalSize
	}
	for _, v := range t.cache.Keys() {
		val, ok := t.cache.Peek(v)
		if !ok {
			continue
		}
		pkg := val.(*Package)
		sz.CacheSize += pkg.Size()
	}
	sz.JournalSize = t.journal.Size()
	sz.TombstoneSize = t.tombstone.Size()
	sz.TotalSize = sz.JournalSize + sz.TombstoneSize + sz.IndexSize + sz.CacheSize
	return sz
}
