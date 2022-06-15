// Copyright (c) 2018-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// TODO
// - materialized views for storing expensive query results

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
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
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
	optsKey             = []byte("_options")
	fieldsKey           = []byte("_fields")
	metaKey             = []byte("_meta")
	headerKey           = []byte("_headers")
	indexesKey          = []byte("_indexes")
	journalKey   uint32 = 0xFFFFFFFF
	tombstoneKey uint32 = 0xFFFFFFFE

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

func (o Options) PackSize() int {
	return 1 << uint(o.PackSizeLog2)
}

func (o Options) JournalSize() int {
	return 1 << uint(o.JournalSizeLog2)
}

func (o Options) Merge(o2 Options) Options {
	o.PackSizeLog2 = util.NonZero(o2.PackSizeLog2, o.PackSizeLog2)
	o.JournalSizeLog2 = util.NonZero(o2.JournalSizeLog2, o.JournalSizeLog2)
	o.FillLevel = util.NonZero(o2.FillLevel, o.FillLevel)
	o.CacheSize = o2.CacheSize
	return o
}

func (o Options) Check() error {
	if o.PackSizeLog2 < 10 || o.PackSizeLog2 > 22 {
		return fmt.Errorf("PackSizeLog2 %d out of range [10, 22]", o.PackSizeLog2)
	}
	if o.JournalSizeLog2 < 10 || o.JournalSizeLog2 > 22 {
		return fmt.Errorf("JournalSizeLog2 %d out of range [10, 22]", o.JournalSizeLog2)
	}
	if o.CacheSize < 0 || o.CacheSize > 64*1024 {
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
	name     string
	opts     Options
	fields   FieldList
	indexes  IndexList
	meta     TableMeta
	db       *DB
	cache    cache.Cache
	journal  *Journal
	packs    *PackIndex
	key      []byte
	metakey  []byte
	packPool *sync.Pool
	pkPool   *sync.Pool
	stats    TableStats
	mu       sync.RWMutex
}

func (d *DB) CreateTable(name string, fields FieldList, opts Options) (*Table, error) {
	opts = DefaultOptions.Merge(opts)
	if err := opts.Check(); err != nil {
		return nil, err
	}
	maxPackSize := opts.PackSize()
	maxJournalSize := opts.JournalSize()
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
			New: func() interface{} { return make([]uint64, 0, maxPackSize) },
		},
	}
	t.stats.TableName = name
	t.stats.JournalTuplesThreshold = int64(maxJournalSize)
	t.stats.TombstoneTuplesThreshold = int64(maxJournalSize)
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
		t.journal = NewJournal(0, maxJournalSize, t.name)
		if err := t.journal.InitFields(fields); err != nil {
			return err
		}
		jsz, tsz, err := t.journal.StoreLegacy(dbTx, t.metakey)
		if err != nil {
			return err
		}
		t.stats.JournalDiskSize = int64(jsz)
		t.stats.TombstoneDiskSize = int64(tsz)
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
		t.stats.PackCacheCapacity = int64(t.opts.CacheSize)
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
	t.stats.TableName = name
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
		if len(opts) > 0 {
			if opts[0].PackSizeLog2 > 0 && t.opts.PackSizeLog2 != opts[0].PackSizeLog2 {
				return fmt.Errorf("pack: %s pack size change not allowed", name)
			}
			t.opts = t.opts.Merge(opts[0])
		}
		maxJournalSize := t.opts.JournalSize()
		// maxPackSize := t.opts.PackSize()
		t.stats.JournalTuplesThreshold = int64(maxJournalSize)
		t.stats.TombstoneTuplesThreshold = int64(maxJournalSize)
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
		t.journal = NewJournal(t.meta.Sequence, maxJournalSize, t.name)
		t.journal.InitFields(t.fields)
		err = t.journal.LoadLegacy(dbTx, t.metakey)
		if err != nil {
			return fmt.Errorf("pack: cannot open journal for table %s: %v", name, err)
		}
		log.Debugf("pack: %s table opened journal with %d entries", name, t.journal.Len())
		return t.loadPackInfo(dbTx)
	})
	if err != nil {
		return nil, err
	}
	if len(opts) > 0 {
		t.opts.CacheSize = opts[0].CacheSize
		if opts[0].JournalSizeLog2 > 0 {
			t.opts.JournalSizeLog2 = opts[0].JournalSizeLog2
		}
	}
	if t.opts.CacheSize > 0 {
		t.cache, err = lru.New2QWithEvict(int(t.opts.CacheSize), t.onEvictedPackage)
		if err != nil {
			return nil, err
		}
		t.stats.PackCacheCapacity = int64(t.opts.CacheSize)
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

func (t *Table) loadPackInfo(dbTx store.Tx) error {
	meta := dbTx.Bucket(t.metakey)
	if meta == nil {
		return ErrNoTable
	}
	heads := make(PackInfoList, 0)
	bh := meta.Bucket(headerKey)
	if bh != nil {
		log.Debugf("pack: %s table loading package headers from bucket", t.name)
		c := bh.Cursor()
		var err error
		for ok := c.First(); ok; ok = c.Next() {
			head := PackInfo{}
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
			atomic.StoreInt64(&t.stats.PacksCount, int64(t.packs.Len()))
			atomic.StoreInt64(&t.stats.MetaSize, int64(t.packs.HeapSize()))
			atomic.StoreInt64(&t.stats.PacksSize, int64(t.packs.TableSize()))
			log.Debugf("pack: %s table loaded %d package headers", t.name, t.packs.Len())
			return nil
		}
	}
	log.Warnf("pack: %s table has corrupt or missing statistics! Re-scanning table. This may take some time...", t.name)
	c := dbTx.Bucket(t.key).Cursor()
	pkg := t.journal.DataPack().Clone(false, t.opts.PackSize())
	for ok := c.First(); ok; ok = c.Next() {
		err := pkg.UnmarshalBinary(c.Value())
		if err != nil {
			return fmt.Errorf("pack: cannot scan table pack %x: %v", c.Key(), err)
		}
		pkg.SetKey(c.Key())
		if pkg.IsJournal() || pkg.IsTomb() {
			pkg.Clear()
			continue
		}
		info := pkg.Info()
		_ = info.UpdateStats(pkg)
		heads = append(heads, info)
		atomic.AddInt64(&t.stats.MetaBytesRead, int64(len(c.Value())))
		pkg.Clear()
	}
	t.packs = NewPackIndex(heads, t.fields.PkIndex())
	atomic.StoreInt64(&t.stats.PacksCount, int64(t.packs.Len()))
	atomic.StoreInt64(&t.stats.MetaSize, int64(t.packs.HeapSize()))
	atomic.StoreInt64(&t.stats.PacksSize, int64(t.packs.TableSize()))
	log.Debugf("pack: %s table scanned %d packages", t.name, t.packs.Len())
	return nil
}

func (t *Table) storePackInfo(dbTx store.Tx) error {
	meta := dbTx.Bucket(t.metakey)
	if meta == nil {
		return ErrNoTable
	}
	hb := meta.Bucket(headerKey)
	if hb == nil {
		var err error
		hb, err = meta.CreateBucketIfNotExists(headerKey)
		if err != nil {
			return err
		}
	}
	for _, v := range t.packs.removed {
		log.Debugf("pack: %s table removing dead header %x", t.name, v)
		hb.Delete(encodePackKey(v))
	}
	t.packs.removed = t.packs.removed[:0]

	for i := range t.packs.packs {
		if !t.packs.packs[i].dirty {
			continue
		}
		buf, err := t.packs.packs[i].MarshalBinary()
		if err != nil {
			return err
		}
		if err := hb.Put(t.packs.packs[i].EncodedKey(), buf); err != nil {
			return err
		}
		t.packs.packs[i].dirty = false
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

func (t *Table) Stats() []TableStats {
	t.mu.Lock()
	defer t.mu.Unlock()

	var s TableStats = t.stats
	s.TupleCount = t.meta.Rows
	s.PacksCount = int64(t.packs.Len())
	s.PackCacheCount = int64(t.cache.Len())
	s.PackCacheCapacity = int64(t.opts.CacheSize)
	s.MetaSize = int64(t.packs.HeapSize())
	s.PacksSize = int64(t.packs.TableSize())

	s.JournalTuplesCount = int64(t.journal.data.Len())
	s.JournalTuplesCapacity = int64(t.journal.data.Cap())
	s.JournalTuplesThreshold = int64(t.journal.maxsize)
	s.JournalSize = int64(t.journal.data.HeapSize())

	s.TombstoneTuplesCount = int64(len(t.journal.tomb))
	s.TombstoneTuplesCapacity = int64(cap(t.journal.tomb))
	s.TombstoneTuplesThreshold = int64(t.journal.maxsize)
	s.TombstoneSize = s.TombstoneTuplesCount * 8

	resp := []TableStats{s}
	for _, idx := range t.indexes {
		resp = append(resp, idx.Stats())
	}
	return resp
}

func (t *Table) PurgeCache() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.cache.Purge()
	atomic.StoreInt64(&t.stats.PackCacheCount, 0)
	atomic.StoreInt64(&t.stats.PackCacheSize, 0)
	for _, idx := range t.indexes {
		idx.PurgeCache()
	}
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

	if t.journal.ShouldFlush() {
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

	if t.journal.ShouldFlush() {
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

	count, err := t.journal.InsertBatch(batch)
	if err != nil {
		return err
	}
	t.meta.Sequence = util.MaxU64(t.meta.Sequence, t.journal.MaxId())
	t.meta.Rows += int64(count)
	t.meta.dirty = true
	atomic.AddInt64(&t.stats.InsertedTuples, int64(count))
	atomic.StoreInt64(&t.stats.TupleCount, t.meta.Rows)
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

	if t.journal.ShouldFlush() {
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

	if t.journal.ShouldFlush() {
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
	atomic.AddInt64(&t.stats.InsertCalls, 1)

	count, err := t.journal.InsertPack(pkg, pos, n)
	if err != nil {
		return err
	}

	t.meta.Sequence = util.MaxU64(t.meta.Sequence, t.journal.MaxId())
	t.meta.Rows += int64(count)
	t.meta.dirty = true
	atomic.AddInt64(&t.stats.InsertedTuples, int64(count))
	atomic.StoreInt64(&t.stats.TupleCount, t.meta.Rows)
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

	if t.journal.ShouldFlush() {
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

	if t.journal.ShouldFlush() {
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

	count, err := t.journal.UpdateBatch(batch)
	if err != nil {
		return err
	}
	t.meta.Sequence = util.MaxU64(t.meta.Sequence, t.journal.MaxId())
	t.meta.dirty = true
	atomic.AddInt64(&t.stats.UpdatedTuples, int64(count))
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

	n := res.Rows()
	if err := t.DeleteIdsTx(ctx, tx, res.PkColumn()); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}

	return int64(n), nil
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

	if t.journal.ShouldFlush() {
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

	if t.journal.ShouldFlush() {
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

	count, err := t.journal.DeleteBatch(ids)
	if err != nil {
		return err
	}

	// Note: we don't check if ids actually exist, so row counter may be off
	// until journal/tombstone are flushed
	t.meta.Rows -= int64(count)
	t.meta.dirty = true
	atomic.AddInt64(&t.stats.DeletedTuples, int64(count))
	atomic.StoreInt64(&t.stats.TupleCount, t.meta.Rows)
	return nil
}

func (t *Table) Close() error {
	log.Debugf("pack: closing %s table with %d pending journal records", t.name, t.journal.Len())
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

	if jsz, tsz, err := t.journal.StoreLegacy(tx.tx, t.metakey); err != nil {
		return err
	} else {
		t.stats.JournalDiskSize = int64(jsz)
		t.stats.TombstoneDiskSize = int64(tsz)
	}

	if err := t.storePackInfo(tx.tx); err != nil {
		return err
	}

	for _, idx := range t.indexes {
		if err := idx.CloseTx(tx); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	t.journal.Close()
	delete(t.db.tables, t.name)
	return nil
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
	nTuples, nTomb := t.journal.Len(), t.journal.TombLen()
	nJournalBytes, nTombBytes, err := t.journal.StoreLegacy(tx.tx, t.metakey)
	if err != nil {
		return err
	}
	atomic.AddInt64(&t.stats.JournalTuplesFlushed, int64(nTuples))
	atomic.AddInt64(&t.stats.JournalPacksStored, 1)
	atomic.AddInt64(&t.stats.JournalBytesWritten, int64(nJournalBytes))
	atomic.AddInt64(&t.stats.TombstoneTuplesFlushed, int64(nTomb))
	atomic.AddInt64(&t.stats.TombstonePacksStored, 1)
	atomic.AddInt64(&t.stats.TombstoneBytesWritten, int64(nTombBytes))
	atomic.StoreInt64(&t.stats.JournalDiskSize, int64(nJournalBytes))
	atomic.StoreInt64(&t.stats.TombstoneDiskSize, int64(nTombBytes))
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
		start                            time.Time = time.Now().UTC()
	)

	atomic.AddInt64(&t.stats.FlushCalls, 1)
	atomic.AddInt64(&t.stats.FlushedTuples, int64(t.journal.Len()+t.journal.TombLen()))
	t.stats.LastFlushTime = start

	live := t.journal.keys
	dead := t.journal.tomb
	jpack := t.journal.data
	dbits := t.journal.deleted

	var (
		pkg                            *Package
		pkgsz                          int = t.opts.PackSize()
		jpos, tpos, nextpack, lastpack int
		jlen, tlen                     int = len(live), len(dead)
		needSort                       bool
		nextmax, packmax               uint64
		err                            error
	)

	_, globalmax := t.packs.GlobalMinMax()

	for {
		if jpos >= jlen && tpos >= tlen {
			break
		}

		for ; jpos < jlen && dbits.IsSet(live[jpos].idx); jpos++ {
		}

		for ; tpos < tlen && (dead[tpos] == 0 || dead[tpos] > globalmax); tpos++ {
		}

		var nextid uint64
		switch true {
		case jpos < jlen && tpos < tlen:
			nextid = util.MinU64(live[jpos].pk, dead[tpos])
		case jpos < jlen && tpos >= tlen:
			nextid = live[jpos].pk
		case jpos >= jlen && tpos < tlen:
			nextid = dead[tpos]
		default:
			// should not happen
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
					if err := t.storePackInfo(tx.tx); err != nil {
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
			packmax = 0
			pkg = nil
			pAdd = 0
			pDel = 0
			pUpd = 0
		}

		if pkg == nil {
			if nextpack < t.packs.Len() {
				pkg, err = t.loadWritablePack(tx, t.packs.packs[nextpack].Key)
				if err != nil && err != ErrPackNotFound {
					return err
				}
				packmax = nextmax
				lastpack = nextpack
			}
			if pkg == nil {
				lastpack = t.packs.Len()
				packmax = 0
				pkg = t.packPool.Get().(*Package)
				pkg.key = t.packs.NextKey()
				pkg.cached = false
			}
		}

		if tpos < tlen && packmax > 0 {
			var ppos int
			for ; tpos < tlen; tpos++ {
				pkid := dead[tpos]

				if pkid == 0 {
					continue
				}

				if pkid > packmax {
					break
				}

				pkcol := pkg.PkColumn()
				ppos += sort.Search(len(pkcol)-ppos, func(i int) bool { return pkcol[i+ppos] >= pkid })
				if ppos == len(pkcol) || pkcol[ppos] != pkid {
					dead[tpos] = 0
					continue
				}

				n := 1
				for ; tpos+n < tlen && ppos+n < len(pkcol) && pkcol[ppos+n] == dead[tpos+n]; n++ {
				}

				for _, idx := range t.indexes {
					if err := idx.RemoveTx(tx, pkg, ppos, n); err != nil {
						return err
					}
				}

				pkg.Delete(ppos, n)
				for i := 0; i < n; i++ {
					dead[tpos+i] = 0
				}
				dead[tpos] = 0
				nDel += n
				pDel += n
				tpos += n - 1
			}
		}

		for lastoffset := 0; jpos < jlen; jpos++ {
			key := live[jpos]

			if dbits.IsSet(key.idx) {
				continue
			}

			if best, _, _ := t.findBestPack(key.pk); best != lastpack {
				break
			}

			if offs := pkg.PkIndex(key.pk, lastoffset); offs > -1 {
				lastoffset = offs
				for _, idx := range t.indexes {
					if !idx.Field.Type.EqualPacksAt(
						pkg, idx.Field.Index, offs,
						jpack, idx.Field.Index, key.idx,
					) {
						if err := idx.RemoveTx(tx, pkg, offs, 1); err != nil {
							return err
						}
						if err := idx.AddTx(tx, jpack, key.idx, 1); err != nil {
							return err
						}
					}
				}

				if err := pkg.CopyFrom(jpack, offs, key.idx, 1); err != nil {
					return err
				}
				nUpd++
				pUpd++
			} else {
				if pkg.Len() >= pkgsz {
					bmin, bmax := t.packs.MinMax(lastpack)
					if lastpack < t.packs.Len() && key.pk > bmin && key.pk < bmax {
						log.Warnf("flush: %s table splitting full pack %x (%d/%d) with min=%d max=%d on out-of-order insert pk %d",
							t.name, pkg.Key(), lastpack, t.packs.Len(), bmin, bmax, key.pk)
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
							if err := t.storePackInfo(tx.tx); err != nil {
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
							if err := t.storePackInfo(tx.tx); err != nil {
								return err
							}
							if err := tx.CommitAndContinue(); err != nil {
								return err
							}
						}
						break
					}
				}
				if err := pkg.AppendFrom(jpack, key.idx, 1, true); err != nil {
					return err
				}
				needSort = needSort || key.pk < packmax
				packmax = util.MaxU64(packmax, key.pk)
				globalmax = util.MaxU64(globalmax, key.pk)
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

	t.stats.LastFlushDuration = time.Since(start)
	log.Debugf("flush: %s table %d packs add=%d del=%d total_size=%s in %s",
		t.name, nParts, nAdd, nDel, util.ByteSize(nBytes), t.stats.LastFlushDuration)

	for _, idx := range t.indexes {
		if err := idx.FlushTx(ctx, tx); err != nil {
			return err
		}
	}

	if tlen > nDel {
		t.meta.Rows += int64(tlen - nDel)
		t.meta.dirty = true
		atomic.StoreInt64(&t.stats.TupleCount, t.meta.Rows)
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

	if err := t.storePackInfo(tx.tx); err != nil {
		return err
	}

	t.journal.Reset()

	return t.flushJournalTx(ctx, tx)
}

func (t *Table) findBestPack(pkval uint64) (int, uint64, uint64) {
	bestpack, min, max, _ := t.packs.Best(pkval)

	if t.packs.Len() == 0 || pkval-min <= max-min {
		return bestpack, min, max
	}

	if t.packs.packs[bestpack].NValues >= t.opts.PackSize() {
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

	q := NewQuery(t.name+".lookup", t)
	defer func() {
		atomic.AddInt64(&t.stats.QueriedTuples, int64(q.rowsMatched))
		q.Close()
	}()
	atomic.AddInt64(&t.stats.QueryCalls, 1)

	ids, _ = vec.Uint64RemoveZeros(ids)
	ids = vec.UniqueUint64Slice(ids)

	if t.journal.TombLen() > 0 {
		var (
			ok   bool
			last int
		)
		for i, v := range ids {
			ok, last = t.journal.IsDeleted(v, last)
			if ok {
				ids[i] = 0
			}
			if last == t.journal.TombLen() {
				break
			}
		}
		ids, _ = vec.Uint64RemoveZeros(ids)
	}

	if len(ids) == 0 || ids[0] > t.meta.Sequence {
		return res, nil
	}

	maxRows := len(ids)
	maxNonZeroId := ids[maxRows-1]

	var (
		idx, last  int
		needUpdate bool
	)
	for i, v := range ids {
		if last == t.journal.Len() {
			break
		}

		idx, last = t.journal.PkIndex(v, last)
		if idx < 0 {
			continue
		}

		if err := res.pkg.AppendFrom(t.journal.DataPack(), idx, 1, true); err != nil {
			res.Close()
			return nil, err
		}
		q.rowsMatched++

		ids[i] = 0
		needUpdate = true
	}
	if needUpdate {
		ids, _ = vec.Uint64RemoveZeros(ids)
	}

	q.journalTime = time.Since(q.lap)

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

		pkg, err := t.loadSharedPack(tx, t.packs.packs[nextpack].Key, true, q.reqfields)
		if err != nil {
			res.Close()
			return nil, err
		}
		q.packsScanned++

		pk := pkg.PkColumn()
		_, max := t.packs.MinMax(nextpack)

		last := 0
		for _, v := range ids[nextid:] {
			if max < v || pk[last] > maxNonZeroId {
				break
			}
			j := pkg.PkIndex(v, last)
			if j < 0 {
				nextid++
				continue
			}
			if err := res.pkg.AppendFrom(pkg, j, 1, true); err != nil {
				res.Close()
				return nil, err
			}
			nextid++
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

	jbits = q.Conditions.MatchPack(t.journal.DataPack(), PackInfo{})
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

			pkg, err := t.loadSharedPack(tx, t.packs.packs[p].Key, !q.NoCache, q.reqfields)
			if err != nil {
				res.Close()
				return nil, err
			}
			q.packsScanned++

			bits := q.Conditions.MatchPack(pkg, t.packs.packs[p])
			for idx, length := bits.Run(0); idx >= 0; idx, length = bits.Run(idx + length) {
				for i := idx; i < idx+length; i++ {
					pkid, err := pkg.Uint64At(pkg.pkindex, i)
					if err != nil {
						continue
					}

					if ok, _ := t.journal.IsDeleted(pkid, 0); ok {
						continue
					}

					src := pkg
					index := i

					if j, _ := t.journal.PkIndex(pkid, 0); j >= 0 {
						if !jbits.IsSet(j) {
							continue
						}

						jbits.Clear(j)
						src = t.journal.DataPack()
						index = j
					}

					if q.Offset > 0 {
						q.Offset--
						continue
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

	idxs, _ := t.journal.SortedIndexes(jbits)
	jpack := t.journal.DataPack()
	for _, idx := range idxs {
		if q.Offset > 0 {
			q.Offset--
			continue
		}

		if err := res.pkg.AppendFrom(jpack, idx, 1, true); err != nil {
			res.Close()
			return nil, err
		}
		q.rowsMatched++

		if q.Limit > 0 && q.rowsMatched == q.Limit {
			break
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

	jbits = q.Conditions.MatchPack(t.journal.DataPack(), PackInfo{})
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

	idxs, pks := t.journal.SortedIndexesReversed(jbits)
	jpack := t.journal.DataPack()
	for i, idx := range idxs {
		if pks[i] <= maxPackedPk {
			continue
		}

		if q.Offset > 0 {
			q.Offset--
			continue
		}

		if err := res.pkg.AppendFrom(jpack, idx, 1, true); err != nil {
			res.Close()
			return nil, err
		}
		q.rowsMatched++
		jbits.Clear(idx)

		if q.Limit > 0 && q.rowsMatched == q.Limit {
			break
		}
	}
	q.journalTime = time.Since(q.lap)

	if q.Limit > 0 && q.rowsMatched >= q.Limit {
		return res, nil
	}

	if !q.IsEmptyMatch() {
		q.lap = time.Now()
	packloop:
		for _, p := range q.MakePackSchedule(true) {
			if util.InterruptRequested(ctx) {
				res.Close()
				return nil, ctx.Err()
			}

			pkg, err := t.loadSharedPack(tx, t.packs.packs[p].Key, !q.NoCache, q.reqfields)
			if err != nil {
				res.Close()
				return nil, err
			}
			q.packsScanned++

			bits := q.Conditions.MatchPack(pkg, t.packs.packs[p]).Reverse()
			for idx, length := bits.Run(bits.Size() - 1); idx >= 0; idx, length = bits.Run(idx - length) {
				for i := idx; i > idx-length; i-- {
					pkid, err := pkg.Uint64At(pkg.pkindex, i)
					if err != nil {
						continue
					}

					if ok, _ := t.journal.IsDeleted(pkid, 0); ok {
						continue
					}

					src := pkg
					index := i

					if j, _ := t.journal.PkIndex(pkid, 0); j >= 0 {
						if !jbits.IsSet(j) {
							continue
						}
						jbits.Clear(j)
						src = t.journal.DataPack()
						index = j
					}

					if q.Offset > 0 {
						q.Offset--
						continue
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

	jbits = q.Conditions.MatchPack(t.journal.DataPack(), PackInfo{})
	q.journalTime = time.Since(q.lap)

	if err := q.QueryIndexes(ctx, tx); err != nil {
		return 0, err
	}

	if jbits.Count() == 0 && q.IsEmptyMatch() {
		return 0, nil
	}

	if !q.IsEmptyMatch() {
		q.lap = time.Now()
	packloop:
		for _, p := range q.MakePackSchedule(q.Order == OrderDesc) {
			if util.InterruptRequested(ctx) {
				return int64(q.rowsMatched), ctx.Err()
			}

			pkg, err := t.loadSharedPack(tx, t.packs.packs[p].Key, !q.NoCache, q.reqfields)
			if err != nil {
				return 0, err
			}
			q.packsScanned++

			bits := q.Conditions.MatchPack(pkg, t.packs.packs[p])
			for idx, length := bits.Run(0); idx >= 0; idx, length = bits.Run(idx + length) {
				for i := idx; i < idx+length; i++ {
					pkid, err := pkg.Uint64At(pkg.pkindex, i)
					if err != nil {
						continue
					}

					if ok, _ := t.journal.IsDeleted(pkid, 0); ok {
						continue
					}

					if j, _ := t.journal.PkIndex(pkid, 0); j >= 0 {
						if !jbits.IsSet(j) {
							continue
						}
						jbits.Clear(j)
					}

					if q.Offset > 0 {
						q.Offset--
						continue
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
	}

	q.rowsMatched += util.NonZero(q.Limit, util.Max(int(jbits.Count())-q.Offset, 0))

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

	jbits = q.Conditions.MatchPack(t.journal.DataPack(), PackInfo{})
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

			pkg, err := t.loadSharedPack(tx, t.packs.packs[p].Key, !q.NoCache, q.reqfields)
			if err != nil {
				return err
			}
			q.packsScanned++

			bits := q.Conditions.MatchPack(pkg, t.packs.packs[p])
			for idx, length := bits.Run(0); idx >= 0; idx, length = bits.Run(idx + length) {
				for i := idx; i < idx+length; i++ {
					pkid, err := pkg.Uint64At(pkg.pkindex, i)
					if err != nil {
						continue
					}

					if ok, _ := t.journal.IsDeleted(pkid, 0); ok {
						continue
					}

					res.pkg = pkg
					index := i

					if j, _ := t.journal.PkIndex(pkid, 0); j >= 0 {
						if !jbits.IsSet(j) {
							continue
						}
						res.pkg = t.journal.DataPack()
						index = j
						jbits.Clear(j)
					}

					if q.Offset > 0 {
						q.Offset--
						continue
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

	res.pkg = t.journal.DataPack()
	idxs, _ := t.journal.SortedIndexes(jbits)
	for _, idx := range idxs {
		if q.Offset > 0 {
			q.Offset--
			continue
		}

		if err := fn(Row{res: &res, n: idx}); err != nil {
			return err
		}
		q.rowsMatched++

		if q.Limit > 0 && q.rowsMatched == q.Limit {
			return nil
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

	jbits = q.Conditions.MatchPack(t.journal.DataPack(), PackInfo{})
	q.journalTime = time.Since(q.lap)

	if err := q.QueryIndexes(ctx, tx); err != nil {
		return err
	}

	if jbits.Count() == 0 && q.IsEmptyMatch() {
		return nil
	}

	_, maxPackedPk := t.packs.GlobalMinMax()
	res := Result{fields: q.reqfields}
	res.pkg = t.journal.DataPack()

	idxs, pks := t.journal.SortedIndexesReversed(jbits)
	for i, idx := range idxs {
		if pks[i] <= maxPackedPk {
			continue
		}

		jbits.Clear(idx)

		if q.Offset > 0 {
			q.Offset--
			continue
		}

		if err := fn(Row{res: &res, n: idx}); err != nil {
			return err
		}
		q.rowsMatched++

		if q.Limit > 0 && q.rowsMatched >= q.Limit {
			return nil
		}
	}
	q.journalTime += time.Since(q.lap)

	if q.IsEmptyMatch() {
		return nil
	}

	q.lap = time.Now()
packloop:
	for _, p := range q.MakePackSchedule(true) {
		if util.InterruptRequested(ctx) {
			return ctx.Err()
		}

		pkg, err := t.loadSharedPack(tx, t.packs.packs[p].Key, !q.NoCache, q.reqfields)
		if err != nil {
			return err
		}
		q.packsScanned++

		bits := q.Conditions.MatchPack(pkg, t.packs.packs[p]).Reverse()
		for idx, length := bits.Run(bits.Size() - 1); idx >= 0; idx, length = bits.Run(idx - length) {
			for i := idx; i > idx-length; i-- {
				pkid, err := pkg.Uint64At(pkg.pkindex, i)
				if err != nil {
					continue
				}

				if ok, _ := t.journal.IsDeleted(pkid, 0); ok {
					continue
				}

				res.pkg = pkg
				index := i

				if j, _ := t.journal.PkIndex(pkid, 0); j >= 0 {
					if !jbits.IsSet(j) {
						continue
					}
					res.pkg = t.journal.DataPack()
					index = j
					jbits.Clear(j)
				}

				if q.Offset > 0 {
					q.Offset--
					continue
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

	ids, _ = vec.Uint64RemoveZeros(ids)
	ids = vec.UniqueUint64Slice(ids)

	if t.journal.TombLen() > 0 {
		var (
			ok   bool
			last int
		)
		for i, v := range ids {
			ok, last = t.journal.IsDeleted(v, last)
			if ok {
				ids[i] = 0
			}
			if last == t.journal.TombLen() {
				break
			}
		}
		ids, _ = vec.Uint64RemoveZeros(ids)
	}

	if len(ids) == 0 || ids[0] > t.meta.Sequence {
		return nil
	}

	maxRows := len(ids)
	maxNonZeroId := ids[maxRows-1]

	res := Result{
		fields: t.Fields(),
		pkg:    t.journal.DataPack(),
	}

	var (
		idx, last  int
		needUpdate bool
	)
	for i, v := range ids {
		if last == t.journal.Len() {
			break
		}

		idx, last = t.journal.PkIndex(v, last)
		if idx < 0 {
			continue
		}

		if err := fn(Row{res: &res, n: idx}); err != nil {
			return err
		}
		q.rowsMatched++
		ids[i] = 0
		needUpdate = true
	}
	if needUpdate {
		ids, _ = vec.Uint64RemoveZeros(ids)
	}
	q.journalTime = time.Since(q.lap)

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

		pkg, err := t.loadSharedPack(tx, t.packs.packs[nextpack].Key, true, q.reqfields)
		if err != nil {
			return err
		}
		res.pkg = pkg
		q.packsScanned++
		pk := pkg.PkColumn()
		_, max := t.packs.MinMax(nextpack)

		last := 0
		for _, v := range ids[nextid:] {
			if max < v || pk[last] > maxNonZeroId {
				break
			}
			j := pkg.PkIndex(v, last)

			if j < 0 {
				nextid++
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
	start := time.Now()

	if util.InterruptRequested(ctx) {
		return ctx.Err()
	}

	if t.packs.Len() <= 1 {
		return nil
	}

	var (
		maxsz                 int = t.opts.PackSize()
		srcSize               int64
		nextpack              uint32
		needCompact           bool
		srcPacks              int = t.packs.Len()
		total, moved, written int64
	)
	for i, v := range t.packs.packs {
		needCompact = needCompact || v.Key > nextpack
		needCompact = needCompact || (i < t.packs.Len()-1 && v.NValues < maxsz)
		nextpack++
		total += int64(v.NValues)
		srcSize += int64(v.Packsize)
	}
	if !needCompact {
		log.Debugf("pack: %s table %d packs / %d rows already compact", t.name, t.packs.Len(), total)
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

	log.Debugf("pack: compacting %s table %d packs / %d rows", t.name, t.packs.Len(), total)

	for {
		if dstIndex == t.packs.Len() {
			break
		}

		if dstPack == nil {
			dstKey := uint32(dstIndex)
			if dstKey == t.packs.packs[dstIndex].Key {
				if t.packs.packs[dstIndex].NValues == maxsz {
					dstIndex++
					continue
				}
				pmin, pmax := t.packs.MinMax(dstIndex)
				if pmin < lastMaxPk {
					dstIndex++
					continue
				}

				dstPack, err = t.loadWritablePack(tx, dstKey)
				if err != nil {
					return err
				}
				lastMaxPk = pmax
				isNewPack = false
			} else {
				dstPack = t.packPool.Get().(*Package)
				dstPack.key = dstKey
				isNewPack = true
			}
		}

		if srcPack == nil {
			minSlice, _ := t.packs.MinMaxSlices()
			var startIndex, srcIndex int = dstIndex, -1
			var lastmin uint64 = math.MaxUint64
			if isNewPack && startIndex > 0 {
				startIndex--
			}
			for i := startIndex; i < len(minSlice); i++ {
				if t.packs.packs[i].Key < dstPack.key {
					continue
				}
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

			ph := t.packs.packs[srcIndex]
			srcPack, err = t.loadWritablePack(tx, ph.Key)
			if err != nil {
				return err
			}
		}

		free := maxsz - dstPack.Len()
		cp := util.Min(free, srcPack.Len())
		moved += int64(cp)

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
			n, err := t.storePack(tx, dstPack)
			if err != nil {
				return err
			}
			dstSize += int64(n)
			dstIndex++
			written += int64(maxsz)
			dstPack = nil
		}

		if _, err := t.storePack(tx, srcPack); err != nil {
			return err
		}

		srcPack = nil

		if tx.Pending() >= txMaxSize {
			if err := t.storePackInfo(tx.tx); err != nil {
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
		n, err := t.storePack(tx, dstPack)
		if err != nil {
			return err
		}
		dstSize += int64(n)
		written += int64(dstPack.Len())
	}

	log.Debugf("pack: %s table compacted %d(+%d) rows into %d(%d) packs (%s ->> %s) in %s",
		t.name, moved, written-moved,
		t.packs.Len(), srcPacks-t.packs.Len(),
		util.ByteSize(srcSize), util.ByteSize(dstSize),
		time.Since(start),
	)
	if err := t.storePackInfo(tx.tx); err != nil {
		return err
	}

	return tx.Commit()
}

func (t *Table) partkey(id int) []byte {
	var buf [4]byte
	bigEndian.PutUint32(buf[:], uint32(id))
	return buf[:]
}

func (t *Table) cachekey(key []byte) string {
	return t.name + "/" + hex.EncodeToString(key)
}

func (t *Table) loadSharedPack(tx *Tx, id uint32, touch bool, fields FieldList) (*Package, error) {
	key := encodePackKey(id)
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
	var err error
	pkg := t.packPool.Get().(*Package)
	if stripped {
		pkg = pkg.KeepFields(fields)
	}
	pkg, err = tx.loadPack(t.key, key, pkg)
	if err != nil {
		return nil, err
	}
	atomic.AddInt64(&t.stats.PacksLoaded, 1)
	atomic.AddInt64(&t.stats.PacksBytesRead, int64(pkg.packedsize))
	pkg.tinfo = t.journal.data.tinfo
	pkg.pkindex = t.fields.PkIndex()
	pkg.cached = touch
	// head := pkg.Info()
	// log.Infof("%s: loaded read pack %d", t.Name(), head.Key)
	// head.DumpDetail(log.Logger().Writer())

	if touch {
		updated, _ := t.cache.Add(cachekey, pkg)
		if updated {
			atomic.AddInt64(&t.stats.PackCacheUpdates, 1)
		} else {
			atomic.AddInt64(&t.stats.PackCacheInserts, 1)
			atomic.AddInt64(&t.stats.PackCacheCount, 1)
			atomic.AddInt64(&t.stats.PackCacheSize, int64(pkg.HeapSize()))
		}
	}
	return pkg, nil
}

func (t *Table) loadWritablePack(tx *Tx, id uint32) (*Package, error) {
	key := encodePackKey(id)
	if cached, ok := t.cache.Get(t.cachekey(key)); ok {
		atomic.AddInt64(&t.stats.PackCacheHits, 1)
		pkg := cached.(*Package)
		clone := pkg.Clone(true, t.opts.PackSize())
		clone.key = pkg.key
		clone.cached = false
		// head := clone.Info()
		// log.Infof("%s: cloned pack %d", t.Name(), head.Key)
		// head.DumpDetail(log.Logger().Writer())
		return clone, nil
	}

	atomic.AddInt64(&t.stats.PackCacheMisses, 1)
	pkg, err := tx.loadPack(t.key, key, t.packPool.Get().(*Package))
	if err != nil {
		return nil, err
	}
	pkg.tinfo = t.journal.data.tinfo
	pkg.pkindex = t.fields.PkIndex()
	// head := pkg.Info()
	// log.Infof("%s: loaded write pack %d", t.Name(), head.Key)
	// head.DumpDetail(log.Logger().Writer())
	atomic.AddInt64(&t.stats.PacksLoaded, 1)
	atomic.AddInt64(&t.stats.PacksBytesRead, int64(pkg.packedsize))
	return pkg, nil
}

func (t *Table) storePack(tx *Tx, pkg *Package) (int, error) {
	key := pkg.Key()

	defer func() {
		cachekey := t.cachekey(key)
		t.cache.Remove(cachekey)
		cachekey += "#"
		for _, v := range t.cache.Keys() {
			if strings.HasPrefix(v.(string), cachekey) {
				t.cache.Remove(v)
			}
		}
	}()

	if pkg.Len() > 0 {
		n, err := tx.storePack(t.key, key, pkg, t.opts.FillLevel)
		if err != nil {
			return 0, err
		}
		head := pkg.Info()
		if err = head.UpdateStats(pkg); err != nil {
			return 0, err
		}

		// log.Infof("%s: store pack %d", t.Name(), head.Key)
		// head.DumpDetail(log.Logger().Writer())

		t.packs.AddOrUpdate(head)
		atomic.AddInt64(&t.stats.PacksStored, 1)
		atomic.AddInt64(&t.stats.PacksBytesWritten, int64(n))
		atomic.StoreInt64(&t.stats.PacksCount, int64(t.packs.Len()))
		atomic.StoreInt64(&t.stats.MetaSize, int64(t.packs.HeapSize()))
		atomic.StoreInt64(&t.stats.PacksSize, int64(t.packs.TableSize()))

		return n, nil

	} else {
		t.packs.Remove(pkg.key)
		if err := tx.deletePack(t.key, key); err != nil {
			return 0, err
		}

		atomic.StoreInt64(&t.stats.PacksCount, int64(t.packs.Len()))
		atomic.StoreInt64(&t.stats.MetaSize, int64(t.packs.HeapSize()))
		atomic.StoreInt64(&t.stats.PacksSize, int64(t.packs.TableSize()))

		return 0, nil
	}
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
	m, err := t.storePack(tx, pkg)
	if err != nil {
		return 0, err
	}
	newpkg.key = t.packs.NextKey()
	n, err := t.storePack(tx, newpkg)
	if err != nil {
		return 0, err
	}
	t.recyclePackage(newpkg)
	return n + m, nil
}

func (t *Table) makePackage() interface{} {
	atomic.AddInt64(&t.stats.PacksAlloc, 1)
	pkg := t.journal.data.Clone(false, t.opts.PackSize())
	return pkg
}

func (t *Table) onEvictedPackage(key, val interface{}) {
	pkg := val.(*Package)
	pkg.cached = false
	atomic.AddInt64(&t.stats.PackCacheEvictions, 1)
	atomic.AddInt64(&t.stats.PackCacheCount, -1)
	atomic.AddInt64(&t.stats.PackCacheSize, int64(-pkg.HeapSize()))
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
	if c := pkg.Cap(); c < 0 || c > t.opts.PackSize() {
		pkg.Release()
		return
	}
	pkg.Clear()
	atomic.AddInt64(&t.stats.PacksRecycled, 1)
	t.packPool.Put(pkg)
}
