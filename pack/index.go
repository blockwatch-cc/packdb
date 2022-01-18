// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"blockwatch.cc/packdb/cache"
	"blockwatch.cc/packdb/cache/lru"
	"blockwatch.cc/packdb/hash"
	"blockwatch.cc/packdb/store"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/packdb/vec"
)

type IndexType int

type IndexValueFunc func(typ FieldType, val interface{}) uint64
type IndexValueAtFunc func(typ FieldType, pkg *Package, index, pos int) uint64
type IndexZeroAtFunc func(pkg *Package, index, pos int) bool

const (
	IndexTypeHash IndexType = iota
	IndexTypeInteger
)

func (t IndexType) String() string {
	switch t {
	case IndexTypeHash:
		return "hash"
	case IndexTypeInteger:
		return "int"
	default:
		return "invalid"
	}
}

func (t IndexType) ValueFunc() IndexValueFunc {
	switch t {
	case IndexTypeHash:
		return hashValue
	case IndexTypeInteger:
		return intValue
	default:
		return nil
	}
}

func (t IndexType) ValueAtFunc() IndexValueAtFunc {
	switch t {
	case IndexTypeHash:
		return hashValueAt
	case IndexTypeInteger:
		return intValueAt
	default:
		return nil
	}
}

func (t IndexType) ZeroAtFunc() IndexZeroAtFunc {
	switch t {
	case IndexTypeHash:
		return hashZeroAt
	case IndexTypeInteger:
		return intZeroAt
	default:
		return nil
	}
}

func (t IndexType) MayHaveCollisions() bool {
	switch t {
	case IndexTypeHash:
		return true
	case IndexTypeInteger:
		return true
	default:
		return false
	}
}

type IndexEntry struct {
	Key uint64 `pack:"K,pk,snappy"`
	Id  uint64 `pack:"I,snappy"`
}

type Index struct {
	Name  string    `json:"name"`
	Type  IndexType `json:"typ"`
	Field Field     `json:"field"`
	opts  Options

	indexValue   IndexValueFunc
	indexValueAt IndexValueAtFunc
	indexZeroAt  IndexZeroAtFunc

	table     *Table
	cache     cache.Cache
	journal   *Package
	tombstone *Package
	packs     *PackIndex
	key       []byte
	metakey   []byte
	packPool  *sync.Pool
	stats     TableStats
}

type IndexList []*Index

func (l IndexList) FindField(fieldname string) *Index {
	for _, v := range l {
		if v.Field.Name == fieldname {
			return v
		}
	}
	return nil
}

func (t *Table) CreateIndex(name string, field Field, typ IndexType, opts Options) (*Index, error) {
	opts = DefaultOptions.Merge(opts)
	if err := opts.Check(); err != nil {
		return nil, err
	}
	field.Flags |= FlagIndexed
	// maxPackSize := opts.PackSize()
	maxJournalSize := opts.JournalSize()
	idx := &Index{
		Name:         name,
		Type:         typ,
		Field:        field,
		opts:         opts,
		table:        t,
		packs:        NewPackIndex(nil, 0),
		key:          []byte(t.name + "_" + name + "_index"),
		metakey:      []byte(t.name + "_" + name + "_index_meta"),
		indexValue:   typ.ValueFunc(),
		indexValueAt: typ.ValueAtFunc(),
		indexZeroAt:  typ.ZeroAtFunc(),
	}
	idx.stats.IndexName = t.name + "_" + name + "_index"
	idx.stats.JournalTuplesThreshold = int64(maxJournalSize)
	idx.stats.TombstoneTuplesThreshold = int64(maxJournalSize)
	idx.packPool = &sync.Pool{
		New: idx.makePackage,
	}
	err := t.db.db.Update(func(dbTx store.Tx) error {
		b := dbTx.Bucket(idx.key)
		if b != nil {
			return ErrIndexExists
		}
		_, err := dbTx.Root().CreateBucketIfNotExists(idx.key)
		if err != nil {
			return err
		}
		meta, err := dbTx.Root().CreateBucketIfNotExists(idx.metakey)
		if err != nil {
			return err
		}
		_, err = meta.CreateBucketIfNotExists(headerKey)
		if err != nil {
			return err
		}
		buf, err := json.Marshal(idx.opts)
		if err != nil {
			return err
		}
		err = meta.Put(optsKey, buf)
		if err != nil {
			return err
		}
		idx.journal = NewPackage()
		idx.journal.key = journalKey
		if err := idx.journal.Init(IndexEntry{}, idx.opts.JournalSize()); err != nil {
			return err
		}
		_, err = storePackTx(dbTx, idx.metakey, journalKey, idx.journal, idx.opts.FillLevel)
		if err != nil {
			return err
		}
		idx.tombstone = NewPackage()
		idx.tombstone.key = tombstoneKey
		if err := idx.tombstone.Init(IndexEntry{}, idx.opts.JournalSize()); err != nil {
			return err
		}
		_, err = storePackTx(dbTx, idx.metakey, tombstoneKey, idx.tombstone, idx.opts.FillLevel)
		if err != nil {
			return err
		}
		meta = dbTx.Bucket(t.metakey)
		t.indexes = append(t.indexes, idx)
		buf, err = json.Marshal(t.indexes)
		if err != nil {
			return err
		}
		err = meta.Put(indexesKey, buf)
		if err != nil {
			return err
		}
		for i, v := range t.fields {
			if v.Name == idx.Field.Name {
				t.fields[i].Flags |= FlagIndexed
			}
		}
		buf, err = json.Marshal(t.fields)
		if err != nil {
			return err
		}
		err = meta.Put(fieldsKey, buf)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if idx.opts.CacheSize > 0 {
		idx.cache, err = lru.New2QWithEvict(int(idx.opts.CacheSize), idx.onEvictedPackage)
		if err != nil {
			return nil, err
		}
		idx.stats.PackCacheCapacity = int64(idx.opts.CacheSize)
	} else {
		idx.cache = cache.NewNoCache()
	}

	log.Debugf("Created %s index %s_%s", typ.String(), t.name, name)
	return idx, nil
}

func (t *Table) CreateIndexIfNotExists(name string, field Field, typ IndexType, opts Options) (*Index, error) {
	idx, err := t.CreateIndex(name, field, typ, opts)
	if err != nil {
		if err != ErrIndexExists {
			return nil, err
		}
		for _, v := range t.indexes {
			if v.Name == name {
				return v, nil
			}
		}
		return nil, ErrIndexNotFound
	}
	return idx, nil
}

func (t *Table) DropIndex(name string) error {
	var (
		pos int = -1
		idx *Index
	)
	for i, v := range t.indexes {
		if v.Name == name {
			pos, idx = i, v
			break
		}
	}
	if idx == nil {
		return ErrNoIndex
	}
	idx.cache.Purge()
	t.indexes = append(t.indexes[:pos], t.indexes[pos+1:]...)
	for i, v := range t.fields {
		if v.Name == idx.Field.Name {
			t.fields[i].Flags ^= FlagIndexed
		}
	}

	return t.db.db.Update(func(dbTx store.Tx) error {
		meta := dbTx.Bucket(t.metakey)
		buf, err := json.Marshal(t.indexes)
		if err != nil {
			return err
		}
		err = meta.Put(indexesKey, buf)
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
		err = dbTx.Root().DeleteBucket([]byte(t.name + "_" + name + "_index"))
		if err != nil {
			return err
		}
		return dbTx.Root().DeleteBucket([]byte(t.name + "_" + name + "_index_meta"))
	})
}

func (t *Table) OpenIndex(idx *Index, opts ...Options) error {
	if len(opts) > 0 {
		log.Debugf("Opening %s_%s index with opts %#v", t.name, idx.Name, opts[0])
	} else {
		log.Debugf("Opening %s_%s index with default opts", t.name, idx.Name)
	}
	idx.packs = NewPackIndex(nil, 0)
	idx.table = t
	idx.key = []byte(t.name + "_" + idx.Name + "_index")
	idx.metakey = []byte(t.name + "_" + idx.Name + "_index_meta")
	idx.packPool = &sync.Pool{
		New: idx.makePackage,
	}
	idx.stats.IndexName = t.name + "_" + idx.Name + "_index"
	idx.indexValue = idx.Type.ValueFunc()
	idx.indexValueAt = idx.Type.ValueAtFunc()
	idx.indexZeroAt = idx.Type.ZeroAtFunc()

	err := t.db.db.View(func(dbTx store.Tx) error {
		b := dbTx.Bucket(idx.metakey)
		if b == nil {
			return ErrNoIndex
		}
		buf := b.Get(optsKey)
		if buf == nil {
			return fmt.Errorf("pack: missing options for index %s", idx.cachekey(nil))
		}
		err := json.Unmarshal(buf, &idx.opts)
		if err != nil {
			return err
		}
		if len(opts) > 0 {
			if opts[0].PackSizeLog2 > 0 && idx.opts.PackSizeLog2 != opts[0].PackSizeLog2 {
				return fmt.Errorf("pack: %s pack size change not allowed", idx.name())
			}
			idx.opts = idx.opts.Merge(opts[0])
		}
		// maxPackSize := idx.opts.PackSize()
		maxJournalSize := idx.opts.JournalSize()
		idx.stats.JournalTuplesThreshold = int64(maxJournalSize)
		idx.stats.TombstoneTuplesThreshold = int64(maxJournalSize)
		idx.journal, err = loadPackTx(dbTx, idx.metakey, journalKey, nil)
		if err != nil {
			return fmt.Errorf("pack: cannot open journal for index %s: %v", idx.cachekey(nil), err)
		}
		idx.journal.initType(IndexEntry{})
		idx.journal.key = journalKey
		log.Debugf("pack: loaded %s index journal with %d entries", idx.cachekey(nil), idx.journal.Len())
		idx.tombstone, err = loadPackTx(dbTx, idx.metakey, tombstoneKey, nil)
		if err != nil {
			return fmt.Errorf("pack: %s index cannot open tombstone: %v", idx.cachekey(nil), err)
		}
		idx.tombstone.initType(IndexEntry{})
		idx.tombstone.key = tombstoneKey
		log.Debugf("pack: index %s loaded tombstone with %d entries",
			idx.cachekey(nil), idx.tombstone.Len())
		return idx.loadPackHeaders(dbTx)
	})
	if err != nil {
		return err
	}
	if len(opts) > 0 {
		idx.opts.CacheSize = opts[0].CacheSize
		if opts[0].JournalSizeLog2 > 0 {
			idx.opts.JournalSizeLog2 = opts[0].JournalSizeLog2
		}
	}
	if idx.opts.CacheSize > 0 {
		idx.cache, err = lru.New2QWithEvict(int(idx.opts.CacheSize), idx.onEvictedPackage)
		if err != nil {
			return err
		}
		idx.stats.PackCacheCapacity = int64(idx.opts.CacheSize)
	} else {
		idx.cache = cache.NewNoCache()
	}

	return nil
}

func (idx *Index) Options() Options {
	return idx.opts
}

func (idx *Index) PurgeCache() {
	idx.cache.Purge()
	atomic.StoreInt64(&idx.stats.PackCacheCount, 0)
	atomic.StoreInt64(&idx.stats.PackCacheSize, 0)
}

func (idx *Index) name() string {
	return string(idx.key)
}

func (idx *Index) loadPackHeaders(dbTx store.Tx) error {
	b := dbTx.Bucket(idx.metakey)
	if b == nil {
		return ErrNoTable
	}
	heads := make(PackageHeaderList, 0)
	bh := b.Bucket(headerKey)
	if bh != nil {
		log.Debugf("pack: %s index loading package headers from bucket", idx.cachekey(nil))
		c := bh.Cursor()
		var err error
		for ok := c.First(); ok; ok = c.Next() {
			head := PackageHeader{}
			err = head.UnmarshalBinary(c.Value())
			if err != nil {
				break
			}
			heads = append(heads, head)
			atomic.AddInt64(&idx.stats.MetaBytesRead, int64(len(c.Value())))
		}
		if err != nil {
			heads = heads[:0]
			log.Errorf("pack: header decode for index %s pack %x: %v", idx.cachekey(nil), c.Key(), err)
		} else {
			idx.packs = NewPackIndex(heads, 0)
			atomic.StoreInt64(&idx.stats.PacksCount, int64(idx.packs.Len()))
			atomic.StoreInt64(&idx.stats.MetaSize, int64(idx.packs.HeapSize()))
			atomic.StoreInt64(&idx.stats.PacksSize, int64(idx.packs.TableSize()))
			log.Debugf("pack: %s index loaded %d package headers", idx.cachekey(nil), idx.packs.Len())
			return nil
		}
	}
	log.Infof("pack: scanning headers for index %s...", idx.cachekey(nil))
	c := dbTx.Bucket(idx.key).Cursor()
	pkg := idx.journal.Clone(false, idx.opts.PackSize())
	for ok := c.First(); ok; ok = c.Next() {
		ph, err := pkg.UnmarshalHeader(c.Value())
		if err != nil {
			return fmt.Errorf("pack: cannot scan index pack %s: %v", idx.cachekey(c.Key()), err)
		}
		ph.dirty = true
		ph.Key = make([]byte, len(c.Key()))
		copy(ph.Key, c.Key())
		if pkg.IsJournal() || pkg.IsTomb() {
			continue
		}
		heads = append(heads, ph)
		atomic.AddInt64(&idx.stats.MetaBytesRead, int64(len(c.Value())))
	}
	idx.packs = NewPackIndex(heads, 0)
	atomic.StoreInt64(&idx.stats.PacksCount, int64(idx.packs.Len()))
	atomic.StoreInt64(&idx.stats.MetaSize, int64(idx.packs.HeapSize()))
	atomic.StoreInt64(&idx.stats.PacksSize, int64(idx.packs.TableSize()))
	log.Debugf("pack: %s index scanned %d package headers", idx.cachekey(nil), idx.packs.Len())
	return nil
}

func (idx *Index) storePackHeaders(dbTx store.Tx) error {
	b := dbTx.Bucket(idx.metakey)
	if b == nil {
		return ErrNoTable
	}
	hb := b.Bucket(headerKey)
	for _, k := range idx.packs.deads {
		hb.Delete(k)
	}
	idx.packs.deads = idx.packs.deads[:0]
	for i := range idx.packs.heads {
		if !idx.packs.heads[i].dirty {
			continue
		}
		buf, err := idx.packs.heads[i].MarshalBinary()
		if err != nil {
			return err
		}
		if err := hb.Put(idx.packs.heads[i].Key, buf); err != nil {
			return err
		}
		idx.packs.heads[i].dirty = false
		atomic.AddInt64(&idx.stats.MetaBytesWritten, int64(len(buf)))
	}
	return nil
}

func (idx *Index) AddTx(tx *Tx, pkg *Package, srcPos, srcLen int) error {
	var pk []uint64
	if col, err := pkg.Column(pkg.pkindex); err != nil {
		return err
	} else {
		pk, _ = col.([]uint64)
	}
	atomic.AddInt64(&idx.stats.InsertCalls, 1)

	var count int64
	for i := srcPos; i < srcPos+srcLen; i++ {
		if idx.indexZeroAt(pkg, idx.Field.Index, i) {
			continue
		}

		entry := IndexEntry{
			Key: idx.indexValueAt(idx.Field.Type, pkg, idx.Field.Index, i),
			Id:  pk[i],
		}

		if err := idx.journal.Push(entry); err != nil {
			return err
		}
		count++
	}

	atomic.AddInt64(&idx.stats.InsertedTuples, count)
	return nil
}

func (idx *Index) RemoveTx(tx *Tx, pkg *Package, srcPos, srcLen int) error {
	col, _ := pkg.Column(pkg.pkindex)
	pk, _ := col.([]uint64)
	atomic.AddInt64(&idx.stats.DeleteCalls, 1)

	var count int64
	for i := srcPos; i < srcPos+srcLen; i++ {
		if idx.indexZeroAt(pkg, idx.Field.Index, i) {
			continue
		}

		entry := IndexEntry{
			Key: idx.indexValueAt(idx.Field.Type, pkg, idx.Field.Index, i),
			Id:  pk[i],
		}
		if err := idx.tombstone.Push(entry); err != nil {
			return err
		}
		count++
	}
	atomic.AddInt64(&idx.stats.DeletedTuples, count)

	return nil
}

func (idx *Index) CanMatch(cond Condition) bool {
	if idx.Field.Name != cond.Field.Name {
		return false
	}
	switch cond.Mode {
	case FilterModeEqual, FilterModeIn, FilterModeNotIn:
		return true
	default:
		return false
	}
}

func (idx *Index) LookupTx(ctx context.Context, tx *Tx, cond Condition) ([]uint64, error) {
	if !idx.CanMatch(cond) {
		return nil, fmt.Errorf("pack: condition %s incompatibe with %s index %s_%s",
			cond, idx.Type, idx.table.name, idx.Name)
	}

	keys := idx.table.pkPool.Get().([]uint64)

	switch cond.Mode {
	case FilterModeEqual:
		if !idx.Field.Type.isZero(cond.Value) {
			keys = append(keys, idx.indexValue(idx.Field.Type, cond.Value))
		}
	case FilterModeIn, FilterModeNotIn:
		slice := reflect.ValueOf(cond.Value)
		if slice.Kind() != reflect.Slice {
			return nil, fmt.Errorf("pack: %s index lookup requires slice type, got %T",
				idx.Type, cond.Value)
		}
		for i, l := 0, slice.Len(); i < l; i++ {
			v := slice.Index(i).Interface()
			if !idx.Field.Type.isZero(v) {
				keys = append(keys, idx.indexValue(idx.Field.Type, v))
			}
		}
		vec.Uint64Sorter(keys).Sort()
	}

	res, err := idx.lookupKeys(ctx, tx, keys, cond.Mode == FilterModeNotIn)
	if err != nil {
		return nil, err
	}
	if cond.Mode != FilterModeNotIn {
		idx.table.pkPool.Put(keys[:0])
	}
	return res, nil
}

func (idx *Index) lookupKeys(ctx context.Context, tx *Tx, in []uint64, neg bool) ([]uint64, error) {
	atomic.AddInt64(&idx.stats.QueryCalls, 1)
	if len(in) == 0 {
		return []uint64{}, nil
	}

	out := idx.table.pkPool.Get().([]uint64)
	var nPacks int

	for nextpack := idx.packs.Len() - 1; nextpack >= 0; nextpack-- {
		if len(in) == 0 {
			break
		}

		if util.InterruptRequested(ctx) {
			out = out[:0]
			idx.table.pkPool.Put(out)
			return nil, ctx.Err()
		}

		min, max := idx.packs.MinMax(nextpack)
		if max < in[0] || min > in[len(in)-1] {
			continue
		}

		ipkg, err := idx.loadSharedPack(tx, idx.packs.heads[nextpack].Key, true)
		if err != nil {
			return nil, err
		}
		nPacks++
		col, _ := ipkg.Column(0)
		keys, _ := col.([]uint64)
		col, _ = ipkg.Column(1)
		values, _ := col.([]uint64)

		for h, i, hl, il := 0, 0, len(keys), len(in); h < hl && i < il; {
			if max < in[i] {
				break
			}
			for h < hl && keys[h] < in[i] {
				h++
			}
			if h == hl {
				break
			}
			for i < il && keys[h] > in[i] {
				i++
			}
			if i == il {
				break
			}
			if keys[h] == in[i] {
				out = append(out, values[h])
				for ; h+1 < hl && keys[h+1] == in[i]; h++ {
					out = append(out, values[h+1])
				}
				if h+1 == hl && min == in[i] {
					break
				}
				in = append(in[:i], in[i+1:]...)
				il--
			}
		}
	}
	if neg {
		idx.table.pkPool.Put(out[:0])
		out = in
	}

	if len(out) > 1 && !neg {
		vec.Uint64Sorter(out).Sort()
	}
	atomic.AddInt64(&idx.stats.QueriedTuples, int64(len(out)))
	return out, nil
}

func (idx *Index) Reindex(ctx context.Context, flushEvery int, ch chan<- float64) error {
	tx, err := idx.table.db.Tx(true)
	if err != nil {
		return err
	}

	defer tx.Rollback()
	if err := idx.ReindexTx(ctx, tx, flushEvery, ch); err != nil {
		tx.Rollback()
		return err
	}

	tx.Commit()
	return nil
}

func (idx *Index) ReindexTx(ctx context.Context, tx *Tx, flushEvery int, ch chan<- float64) error {
	for i := idx.packs.Len() - 1; i >= 0; i-- {
		key := idx.packs.heads[i].Key
		cachekey := idx.cachekey(key)
		if err := tx.deletePack(idx.key, key); err != nil {
			return err
		}
		idx.cache.Remove(cachekey)
	}
	idx.packs = NewPackIndex(nil, 0)

	idx.journal.Clear()
	if _, err := tx.storePack(idx.metakey, journalKey, idx.journal, idx.opts.FillLevel); err != nil {
		return err
	}
	idx.tombstone.Clear()
	if _, err := tx.storePack(idx.metakey, tombstoneKey, idx.tombstone, idx.opts.FillLevel); err != nil {
		return err
	}

	if flushEvery < 128 {
		flushEvery = 128
	}

	for i, ph := range idx.table.packs.heads {
		if util.InterruptRequested(ctx) {
			return ctx.Err()
		}

		fields := idx.table.Fields().Select(idx.Field.Name).Add(idx.table.Fields().Pk())
		pkg, err := idx.table.loadSharedPack(tx, ph.Key, false, fields)
		if err != nil {
			return err
		}

		err = idx.AddTx(tx, pkg, 0, pkg.Len())
		if err != nil {
			return err
		}

		idx.table.recyclePackage(pkg)
		if i%flushEvery == 0 {
			select {
			case ch <- float64(i*100) / float64(idx.table.packs.Len()):
			default:
			}
			err = idx.FlushTx(ctx, tx)
			if err != nil {
				return err
			}
		}
	}

	select {
	case ch <- float64(99):
	default:
	}
	err := idx.FlushTx(ctx, tx)
	if err != nil {
		return err
	}
	select {
	case ch <- float64(100):
	default:
	}

	if idx.journal.IsDirty() {
		_, err := tx.storePack(idx.metakey, journalKey, idx.journal, idx.opts.FillLevel)
		if err != nil {
			return err
		}
	}
	return nil
}

func (idx *Index) CloseTx(tx *Tx) error {
	log.Debugf("pack: closing %s index %s with %d/%d records", idx.Type,
		idx.cachekey(nil), idx.journal.Len(), idx.tombstone.Len())
	_, err := tx.storePack(idx.metakey, journalKey, idx.journal, idx.opts.FillLevel)
	if err != nil {
		return err
	}
	_, err = tx.storePack(idx.metakey, tombstoneKey, idx.tombstone, idx.opts.FillLevel)
	if err != nil {
		return err
	}
	if err := idx.storePackHeaders(tx.tx); err != nil {
		return err
	}
	return nil
}

func (idx *Index) FlushTx(ctx context.Context, tx *Tx) error {
	atomic.AddInt64(&idx.stats.FlushCalls, 1)
	atomic.AddInt64(&idx.stats.FlushedTuples, int64(idx.journal.Len()+idx.tombstone.Len()))
	begin := time.Now()
	idx.stats.LastFlushTime = begin

	if err := idx.journal.PkSort(); err != nil {
		return err
	}
	if err := idx.tombstone.PkSort(); err != nil {
		return err
	}

	col, _ := idx.tombstone.Column(0)
	dead, _ := col.([]uint64)
	col, _ = idx.tombstone.Column(1)
	deadval, _ := col.([]uint64)
	col, _ = idx.journal.Column(0)
	pk, _ := col.([]uint64)
	col, _ = idx.journal.Column(1)
	pkval, _ := col.([]uint64)
	var nAdd, nDel, nParts, nBytes int

	log.Debugf("flush: %s idx %s %d journal and %d tombstone records",
		idx.Type, idx.cachekey(nil), len(pk), len(dead))

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
			for i := 0; j+i < jl && dead[d] == pk[j+i]; {
				if deadval[d] == pkval[j+i] {
					idx.journal.Delete(j+i, 1)
					jl--
					nDel++
					col, _ = idx.journal.Column(idx.journal.pkindex)
					pk, _ = col.([]uint64)
					col, _ = idx.journal.Column(1)
					pkval, _ = col.([]uint64)
				} else {
					i++
				}
			}
			idx.tombstone.Delete(d, 1)
			dl--
			col, _ = idx.tombstone.Column(idx.tombstone.pkindex)
			dead, _ = col.([]uint64)
			col, _ = idx.tombstone.Column(1)
			deadval, _ = col.([]uint64)
		}
	}
	log.Debugf("flush: %s idx %s deleted %d journal entries",
		idx.Type, idx.cachekey(nil), nDel)

	if idx.tombstone.Len() > 0 {
		var packsProcessed int
		for nextPack, lenPacks := 0, idx.packs.Len(); len(dead) > 0 && packsProcessed < 3*lenPacks; nextPack = (nextPack + 1) % lenPacks {
			packsProcessed++
			min, max := idx.packs.MinMax(nextPack)
			if max < dead[0] || min > dead[len(dead)-1] {
				continue
			}

			pkg, err := idx.loadWritablePack(tx, idx.packs.heads[nextPack].Key)
			if err != nil {
				return err
			}

			log.Debugf("flush: %s idx removing dead entries from pack %s",
				idx.Type, idx.cachekey(pkg.key))

			col, _ = pkg.Column(0)
			pk, _ = col.([]uint64)
			col, _ = pkg.Column(1)
			pkval, _ = col.([]uint64)

			before := nDel
			for i, d, il, dl := 0, 0, len(pk), len(dead); i < il && d < dl; {
				if max < dead[d] {
					break
				}
				for d < dl && dead[d] < pk[i] {
					d++
				}
				if d == dl {
					break
				}
				for i < il && dead[d] > pk[i] {
					i++
				}
				if i == il {
					break
				}
				if dead[d] == pk[i] {
					for j := 0; i+j < il && dead[d] == pk[i+j]; {
						if deadval[d] == pkval[i+j] {
							pkg.Delete(i+j, 1)
							il--
							nDel++
							col, _ = pkg.Column(0)
							pk, _ = col.([]uint64)
							col, _ = pkg.Column(1)
							pkval, _ = col.([]uint64)
						} else {
							j++
						}
					}

					if il > 0 && d+1 == dl && max == dead[d] {
						break
					}

					idx.tombstone.Delete(d, 1)
					dl--
					col, _ = idx.tombstone.Column(0)
					dead, _ = col.([]uint64)
					col, _ = idx.tombstone.Column(1)
					deadval, _ = col.([]uint64)
				}
			}
			log.Debugf("flush: %s idx removed %d dead entries from pack %s, %d are left",
				idx.Type, nDel-before, idx.cachekey(pkg.key), idx.tombstone.Len())

			n, err := idx.storePack(tx, pkg)
			idx.recyclePackage(pkg)
			if err != nil {
				return err
			}
			nParts++
			nBytes += n

			if tx.Pending() >= txMaxSize {
				if err := idx.storePackHeaders(tx.tx); err != nil {
					return err
				}
				if err := tx.CommitAndContinue(); err != nil {
					return err
				}
				if util.InterruptRequested(ctx) {
					_, err := tx.storePack(idx.metakey, tombstoneKey, idx.tombstone, idx.opts.FillLevel)
					if err != nil {
						return err
					}
					if err := tx.Commit(); err != nil {
						return err
					}
					return ctx.Err()
				}
			}
		}
		log.Debugf("flush: %s idx %s removed %d dead entries total, %d are not found",
			idx.Type, idx.cachekey(nil), nDel, idx.tombstone.Len())

		if idx.tombstone.Len() > 0 {
			idx.tombstone.Clear()
		}

		if idx.tombstone.IsDirty() {
			_, err := tx.storePack(idx.metakey, tombstoneKey, idx.tombstone, idx.opts.FillLevel)
			if err != nil {
				return err
			}
			if err := tx.CommitAndContinue(); err != nil {
				return err
			}
		}
	}

	col, _ = idx.journal.Column(idx.journal.pkindex)
	pk, _ = col.([]uint64)

	if idx.journal.Len() > 0 {
		var (
			pkg           *Package
			err           error
			lastpack      int
			nextpack      int = -1
			min, max, rng uint64
			lastkey       uint64
			needsort      bool
			packsz        int = idx.opts.PackSize()
		)

		if idx.packs.Len() == 0 {
			pkg = idx.packPool.Get().(*Package)
			pkg.key = idx.partkey(idx.packs.Len())
		}

		for i, l := 0, len(pk); i < l; i++ {
			if nextpack < 0 || (pk[i]-min > rng) {
				nextpack, min, max = idx.packs.Best(pk[i])
				rng = max - min + 1
			}

			if lastpack != nextpack && pkg != nil {
				if pkg.IsDirty() {
					if needsort {
						if err := pkg.PkSort(); err != nil {
							return err
						}
						needsort = false
					}
					n, err := idx.storePack(tx, pkg)
					if err != nil {
						return err
					}
					nParts++
					nBytes += n
				}
				pkg = nil
				lastkey = 0
				needsort = false
				lastpack = nextpack
				if tx.Pending() >= txMaxSize {
					if err := idx.storePackHeaders(tx.tx); err != nil {
						return err
					}
					if err := tx.CommitAndContinue(); err != nil {
						return err
					}
				}
			}

			if pkg == nil {
				pkg, err = idx.loadWritablePack(tx, idx.packs.heads[nextpack].Key)
				if err != nil {
					return err
				}
				lastkey, _ = pkg.Uint64At(pkg.pkindex, pkg.Len()-1)
			}

			err := pkg.AppendFrom(idx.journal, i, 1, false)
			if err != nil {
				return err
			}
			needsort = needsort || pk[i] < lastkey
			lastkey = pk[i]
			min = util.MinU64(min, pk[i])
			max = util.MaxU64(max, pk[i])
			rng = max - min + 1
			nAdd++

			if pkg.Len() == packsz {
				if needsort {
					if err := pkg.PkSort(); err != nil {
						return err
					}
					needsort = false
				}
				n, err := idx.splitPack(tx, pkg)
				if err != nil {
					return err
				}
				nParts++
				nBytes += n
				lastkey, _ = pkg.Uint64At(pkg.pkindex, pkg.Len()-1)
				needsort = false
				nextpack = -1
				if tx.Pending() >= txMaxSize {
					if err := idx.storePackHeaders(tx.tx); err != nil {
						return err
					}
					if err := tx.CommitAndContinue(); err != nil {
						return err
					}
				}
			}
		}

		if pkg != nil && pkg.IsDirty() {
			if needsort {
				if err := pkg.PkSort(); err != nil {
					return err
				}
			}
			n, err := idx.storePack(tx, pkg)
			if err != nil {
				return err
			}
			idx.recyclePackage(pkg)
			nParts++
			nBytes += n
		}
		idx.journal.Clear()
		_, err = tx.storePack(idx.metakey, journalKey, idx.journal, idx.opts.FillLevel)
		if err != nil {
			return err
		}
	}

	if err := idx.storePackHeaders(tx.tx); err != nil {
		return err
	}

	atomic.StoreInt64(&idx.stats.PacksCount, int64(idx.packs.Len()))
	atomic.StoreInt64(&idx.stats.TupleCount, int64(idx.packs.Count()))
	atomic.StoreInt64(&idx.stats.MetaSize, int64(idx.packs.HeapSize()))
	atomic.StoreInt64(&idx.stats.PacksSize, int64(idx.packs.TableSize()))

	log.Debugf("flush: %s index %s %d packs add=%d del=%d total_size=%s in %s",
		idx.Type, idx.cachekey(nil), nParts, nAdd, nDel, util.ByteSize(nBytes),
		time.Since(begin))

	return nil
}

func (idx *Index) splitPack(tx *Tx, pkg *Package) (int, error) {
	newpkg := idx.packPool.Get().(*Package)
	newpkg.cached = false
	half := pkg.Len() / 2
	if err := newpkg.AppendFrom(pkg, half, pkg.Len()-half, true); err != nil {
		return 0, err
	}
	if err := pkg.Delete(half, pkg.Len()-half); err != nil {
		return 0, err
	}
	_, err := idx.storePack(tx, pkg)
	if err != nil {
		return 0, err
	}
	newpkg.key = idx.partkey(idx.packs.Len())
	n, err := idx.storePack(tx, newpkg)
	if err != nil {
		return 0, err
	}
	idx.recyclePackage(newpkg)
	return n, nil
}

func (idx Index) cachekey(key []byte) string {
	return string(idx.key) + "/" + hex.EncodeToString(key)
}

func (idx Index) partkey(id int) []byte {
	var buf [4]byte
	bigEndian.PutUint32(buf[:], uint32(id))
	return buf[:]
}

func (idx *Index) loadSharedPack(tx *Tx, key []byte, touch bool) (*Package, error) {
	cachekey := idx.cachekey(key)
	cachefn := idx.cache.Peek
	if touch {
		cachefn = idx.cache.Get
	}
	if cached, ok := cachefn(cachekey); ok {
		atomic.AddInt64(&idx.stats.PackCacheHits, 1)
		return cached.(*Package), nil
	}
	atomic.AddInt64(&idx.stats.PackCacheMisses, 1)
	pkg, err := tx.loadPack(idx.key, key, idx.packPool.Get().(*Package))
	if err != nil {
		return nil, err
	}
	atomic.AddInt64(&idx.stats.PacksLoaded, 1)
	atomic.AddInt64(&idx.stats.PacksBytesRead, int64(pkg.packedsize))
	pkg.SetKey(key)
	pkg.tinfo = idx.journal.tinfo
	pkg.pkindex = 0
	pkg.cached = touch
	if touch {
		updated, _ := idx.cache.Add(cachekey, pkg)
		if updated {
			atomic.AddInt64(&idx.stats.PackCacheUpdates, 1)
		} else {
			atomic.AddInt64(&idx.stats.PackCacheInserts, 1)
			atomic.AddInt64(&idx.stats.PackCacheCount, 1)
			atomic.AddInt64(&idx.stats.PackCacheSize, int64(pkg.HeapSize()))
		}
	}
	return pkg, nil
}

func (idx *Index) loadWritablePack(tx *Tx, key []byte) (*Package, error) {
	if cached, ok := idx.cache.Get(idx.cachekey(key)); ok {
		atomic.AddInt64(&idx.stats.PackCacheHits, 1)
		pkg := cached.(*Package)
		clone := pkg.Clone(true, idx.opts.PackSize())
		clone.SetKey(pkg.key)
		clone.cached = false
		return clone, nil
	}

	atomic.AddInt64(&idx.stats.PackCacheMisses, 1)
	pkg, err := tx.loadPack(idx.key, key, idx.packPool.Get().(*Package))
	if err != nil {
		return nil, err
	}
	pkg.SetKey(key)
	pkg.tinfo = idx.journal.tinfo
	pkg.pkindex = 0
	atomic.AddInt64(&idx.stats.PacksLoaded, 1)
	atomic.AddInt64(&idx.stats.PacksBytesRead, int64(pkg.packedsize))
	return pkg, nil
}

func (idx *Index) storePack(tx *Tx, pkg *Package) (int, error) {
	key := pkg.key

	defer func() {
		cachekey := idx.cachekey(key)
		idx.cache.Remove(cachekey)
	}()

	if pkg.Len() > 0 {
		n, err := tx.storePack(idx.key, key, pkg, idx.opts.FillLevel)
		if err != nil {
			return 0, err
		}
		head := pkg.Header()
		if err := head.UpdateStats(pkg); err != nil {
			return 0, err
		}
		idx.packs.AddOrUpdate(head)
		atomic.AddInt64(&idx.stats.PacksStored, 1)
		atomic.AddInt64(&idx.stats.PacksBytesWritten, int64(n))
		return n, nil
	} else {
		idx.packs.RemoveKey(key)
		if err := tx.deletePack(idx.key, key); err != nil {
			return 0, err
		}
		return 0, nil
	}
}

func (idx *Index) makePackage() interface{} {
	atomic.AddInt64(&idx.stats.PacksAlloc, 1)
	return idx.journal.Clone(false, idx.opts.PackSize())
}

func (idx *Index) onEvictedPackage(key, val interface{}) {
	pkg := val.(*Package)
	pkg.cached = false
	atomic.AddInt64(&idx.stats.PackCacheEvictions, 1)
	atomic.AddInt64(&idx.stats.PackCacheCount, -1)
	atomic.AddInt64(&idx.stats.PackCacheSize, int64(-pkg.HeapSize()))
	idx.recyclePackage(pkg)
}

func (idx *Index) recyclePackage(pkg *Package) {
	if pkg == nil || pkg.cached {
		return
	}
	if c := pkg.Cap(); c < 0 || c > idx.opts.PackSize() {
		pkg.Release()
		return
	}
	pkg.Clear()
	atomic.AddInt64(&idx.stats.PacksRecycled, 1)
	idx.packPool.Put(pkg)
}

func (idx *Index) Stats() TableStats {
	var s TableStats = idx.stats

	// s.TupleCount = idx.meta.Rows
	s.PacksCount = int64(idx.packs.Len())
	s.PackCacheCount = int64(idx.cache.Len())
	s.PackCacheCapacity = int64(idx.opts.CacheSize)
	s.MetaSize = int64(idx.packs.HeapSize())
	s.PacksSize = int64(idx.packs.TableSize())

	s.JournalTuplesCount = int64(idx.journal.Len())
	s.JournalTuplesCapacity = int64(idx.journal.Cap())
	s.JournalTuplesThreshold = int64(idx.opts.JournalSize())
	s.JournalSize = int64(idx.journal.HeapSize())

	s.TombstoneTuplesCount = int64(idx.tombstone.Len())
	s.TombstoneTuplesCapacity = int64(idx.tombstone.Cap())
	s.TombstoneTuplesThreshold = int64(idx.opts.JournalSize())
	s.TombstoneSize = int64(idx.tombstone.HeapSize())

	for _, v := range idx.cache.Keys() {
		val, ok := idx.cache.Peek(v)
		if !ok {
			continue
		}
		s.PackCacheSize += int64(val.(*Package).HeapSize())
	}

	return s
}

func hashValue(typ FieldType, val interface{}) uint64 {
	h := hash.NewInlineFNV64a()
	var buf [8]byte
	switch typ {
	case FieldTypeBytes:
		h.Write(val.([]byte))
	case FieldTypeBoolean:
		if b, _ := val.(bool); b {
			h.Write([]byte{1})
		} else {
			h.Write([]byte{0})
		}
	case FieldTypeInt64:
		bigEndian.PutUint64(buf[:], uint64(val.(int64)))
		h.Write(buf[:])
	case FieldTypeUint64:
		bigEndian.PutUint64(buf[:], val.(uint64))
		h.Write(buf[:])
	case FieldTypeFloat64:
		bigEndian.PutUint64(buf[:], math.Float64bits(val.(float64)))
		h.Write(buf[:])
	case FieldTypeString:
		h.Write([]byte(val.(string)))
	case FieldTypeDatetime:
		bigEndian.PutUint64(buf[:], uint64(val.(time.Time).UnixNano()))
		h.Write(buf[:])
	default:
		panic(fmt.Errorf("hash index: unsupported value type %s", typ))
	}
	return h.Sum64()
}

func hashValueAt(typ FieldType, pkg *Package, index, pos int) uint64 {
	h := hash.NewInlineFNV64a()
	var buf [8]byte
	switch typ {
	case FieldTypeBytes:
		val, _ := pkg.BytesAt(index, pos)
		h.Write(val)
	case FieldTypeBoolean:
		if b, _ := pkg.BoolAt(index, pos); b {
			h.Write([]byte{1})
		} else {
			h.Write([]byte{0})
		}
	case FieldTypeInt64:
		val, _ := pkg.Int64At(index, pos)
		bigEndian.PutUint64(buf[:], uint64(val))
		h.Write(buf[:])
	case FieldTypeUint64:
		val, _ := pkg.Uint64At(index, pos)
		bigEndian.PutUint64(buf[:], val)
		h.Write(buf[:])
	case FieldTypeFloat64:
		val, _ := pkg.Float64At(index, pos)
		bigEndian.PutUint64(buf[:], math.Float64bits(val))
		h.Write(buf[:])
	case FieldTypeString:
		val, _ := pkg.StringAt(index, pos)
		h.Write([]byte(val))
	case FieldTypeDatetime:
		val, _ := pkg.TimeAt(index, pos)
		bigEndian.PutUint64(buf[:], uint64(val.UnixNano()))
		h.Write(buf[:])
	default:
		panic(fmt.Errorf("hash index: unsupported value type %s", typ))
	}
	return h.Sum64()
}

func intValue(typ FieldType, val interface{}) uint64 {
	switch typ {
	case FieldTypeInt64:
		return uint64(val.(int64))
	case FieldTypeUint64:
		return val.(uint64)
	case FieldTypeDatetime:
		return uint64(val.(time.Time).UnixNano())
	default:
		// FieldTypeBytes, FieldTypeBoolean, FieldTypeString, FieldTypeFloat64
		return 0
	}
}

func intValueAt(typ FieldType, pkg *Package, index, pos int) uint64 {
	switch typ {
	case FieldTypeInt64, FieldTypeDatetime:
		val, _ := pkg.Int64At(index, pos)
		return uint64(val)
	case FieldTypeUint64:
		val, _ := pkg.Uint64At(index, pos)
		return val
	default:
		// FieldTypeBytes, FieldTypeBoolean, FieldTypeString, FieldTypeFloat64
		return 0
	}
}

func hashZeroAt(pkg *Package, index, pos int) bool {
	return pkg.IsZeroAt(index, pos, false)
}

func intZeroAt(pkg *Package, index, pos int) bool {
	return pkg.IsZeroAt(index, pos, true)
}
