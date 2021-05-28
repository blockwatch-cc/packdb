// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"fmt"
	"sort"

	"blockwatch.cc/packdb/store"
	"blockwatch.cc/packdb/util"
	"blockwatch.cc/packdb/vec"
)

const sizeStep int = 1 << 12

func roundSize(sz int) int {
	return (sz + (sizeStep - 1)) & ^(sizeStep - 1)
}

type Journal struct {
	lastid   uint64
	maxid    uint64
	maxsize  int
	sortData bool
	data     *Package
	keys     journalEntryList
	tomb     []uint64
	deleted  *vec.BitSet
	wal      *Wal
	prefix   string
}

type journalEntry struct {
	pk  uint64
	idx int
}

type journalEntryList []journalEntry

func (l journalEntryList) Len() int           { return len(l) }
func (l journalEntryList) Less(i, j int) bool { return l[i].pk < l[j].pk }
func (l journalEntryList) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }

func NewJournal(maxid uint64, size int, name string) *Journal {
	return &Journal{
		maxid:   maxid,
		maxsize: size,
		data:    NewPackage(),
		keys:    make(journalEntryList, 0, roundSize(size)),
		tomb:    make([]uint64, 0, roundSize(size)),
		deleted: vec.NewBitSet(roundSize(size)).Resize(0),
		prefix:  name,
	}
}

func (j *Journal) InitFields(fields []Field) error {
	return j.data.InitFields(fields, 0)
}

func (j *Journal) InitType(typ interface{}) error {
	return j.data.initType(typ)
}

func (j *Journal) Open(path string) error {
	w, err := OpenWal(path, j.prefix)
	if err != nil {
		return fmt.Errorf("pack: opening WAL for %s journal failed: %v", j.prefix, err)
	}
	j.wal = w
	return nil
}

func (j *Journal) Close() {
	if j.wal != nil {
		j.wal.Close()
		j.wal = nil
	}
}

func (j *Journal) LoadLegacy(dbTx store.Tx, bucketName []byte) error {
	j.Reset()
	if _, err := loadPackTx(dbTx, bucketName, journalKey, j.data); err != nil {
		return err
	}
	j.sortData = false
	for i, n := range j.data.PkColumn() {
		j.keys = append(j.keys, journalEntry{n, i})
		j.sortData = j.sortData || n < j.lastid
		j.lastid = util.MaxU64(j.lastid, n)
	}
	if j.sortData {
		sort.Sort(j.keys)
	}
	tomb, err := loadPackTx(dbTx, bucketName, tombstoneKey, nil)
	if err != nil {
		return fmt.Errorf("pack: cannot open tombstone for table %s: %v", string(bucketName), err)
	}
	tomb.initType(Tombstone{})
	pk := tomb.PkColumn()
	if cap(j.tomb) < len(pk) {
		j.tomb = make([]uint64, len(pk), roundSize(len(pk)))
	}
	j.tomb = j.tomb[:len(pk)]
	copy(j.tomb, pk)
	tomb.Release()
	j.deleted.Resize(len(j.keys))
	var idx, last int
	for _, v := range j.tomb {
		idx, last = j.PkIndex(v, last)
		if idx < 0 {
			continue
		}
		if last >= len(j.keys) {
			break
		}
		j.deleted.Set(idx)
	}
	return nil
}

func (j *Journal) StoreLegacy(dbTx store.Tx, bucketName []byte) (int, error) {
	n, err := storePackTx(dbTx, bucketName, journalKey, j.data, defaultJournalFillLevel)
	if err != nil {
		return 0, err
	}
	tomb := NewPackage()
	tomb.Init(Tombstone{}, len(j.tomb))
	defer tomb.Release()
	for _, v := range j.tomb {
		ts := Tombstone{v}
		_ = tomb.Push(ts)
	}
	m, err := storePackTx(dbTx, bucketName, tombstoneKey, tomb, defaultJournalFillLevel)
	if err != nil {
		return n, err
	}
	n += m
	return n, nil
}

func (j *Journal) Len() int {
	return j.data.Len()
}

func (j *Journal) TombLen() int {
	return len(j.tomb)
}

func (j *Journal) HeapSize() int {
	return j.data.Size() + len(j.keys)*16 + len(j.tomb)*8 + 82
}

func (j *Journal) ShouldFlush() bool {
	return j.data.Len()+len(j.tomb) > j.maxsize
}

func (j *Journal) IsSorted() bool {
	return !j.sortData
}

func (j *Journal) MaxId() uint64 {
	return j.maxid
}

func (j *Journal) LastId() uint64 {
	return j.lastid
}

func (j *Journal) next() uint64 {
	j.maxid++
	return j.maxid
}

func (j *Journal) Insert(item Item) error {
	pk := item.ID()
	updateIdx := -1
	if pk == 0 {
		pk = j.next()
		item.SetID(pk)
	} else {
		updateIdx, _ = j.PkIndex(pk, 0)
	}

	j.wal.Write(WalRecordTypeInsert, pk, item)

	if updateIdx < 0 {
		if err := j.data.Push(item); err != nil {
			return err
		}
		j.undelete([]uint64{pk})
		j.mergeKeys(journalEntryList{journalEntry{pk, j.data.Len() - 1}})
		j.deleted.Resize(len(j.keys))
		j.sortData = j.sortData || pk < j.lastid
	} else {
		if err := j.data.ReplaceAt(updateIdx, item); err != nil {
			return err
		}
		j.undelete([]uint64{pk})
	}

	j.lastid = util.MaxU64(j.lastid, pk)
	j.maxid = util.MaxU64(j.maxid, pk)

	return nil
}

func (j *Journal) InsertBatch(batch []Item) (int, error) {
	if len(batch) == 0 {
		return 0, nil
	}
	if batch[0].ID() != 0 {
		SortItems(batch)
	}

	var count, last int
	newKeys := make(journalEntryList, 0, len(batch))
	newPks := make([]uint64, 0, len(batch))

	for _, item := range batch {
		pk := item.ID()
		updateIdx := -1
		if pk == 0 {
			pk = j.next()
			item.SetID(pk)
		} else {
			updateIdx, last = j.PkIndex(pk, last)
		}

		if updateIdx < 0 {
			j.wal.Write(WalRecordTypeInsert, pk, item)
			if err := j.data.Push(item); err != nil {
				return count, err
			}
			newKeys = append(newKeys, journalEntry{pk, j.data.Len() - 1})
			j.sortData = j.sortData || pk < j.lastid
			j.lastid = util.MaxU64(j.lastid, pk)
			j.maxid = util.MaxU64(j.maxid, pk)
		} else {
			j.wal.Write(WalRecordTypeUpdate, pk, item)
			if err := j.data.ReplaceAt(updateIdx, item); err != nil {
				return count, err
			}
		}
		newPks = append(newPks, pk)
		count++
	}

	j.undelete(newPks)
	j.mergeKeys(newKeys)
	j.deleted.Resize(len(j.keys))

	return count, nil
}

func (j *Journal) InsertPack(pkg *Package, pos, n int) (int, error) {
	l := pkg.Len()
	if l == 0 || n == 0 || n+pos > l {
		return 0, nil
	}

	pkcol := pkg.PkColumn()
	pks := pkcol[pos : pos+n]
	minid, maxid := vec.Uint64Slice(pks).MinMax()
	isSorted := minid == 0 && maxid == 0
	isSorted = isSorted || sort.SliceIsSorted(pks, func(i, j int) bool { return pks[i] < pks[j] })

	var count, last int
	newKeys := make(journalEntryList, 0, n)

	if minid > j.lastid {
		j.wal.WritePack(WalRecordTypeInsert, pkg, pos, n)
		jLen := j.data.Len()
		if err := j.data.AppendFrom(pkg, pos, n, true); err != nil {
			return 0, err
		}
		count += n
		for i, v := range pks {
			newKeys = append(newKeys, journalEntry{v, jLen + i})
		}
		j.lastid = maxid

	} else {
		for i, pk := range pks {
			updateIdx := -1
			if pk == 0 {
				pk = j.next()
				pkcol[pos+i] = pk
				j.sortData = true
			} else {
				updateIdx, last = j.PkIndex(pk, last)
			}

			if updateIdx < 0 {
				j.wal.WritePack(WalRecordTypeInsert, pkg, pos+i, 1)
				if err := j.data.AppendFrom(pkg, pos+i, 1, true); err != nil {
					return count, err
				}
				newKeys = append(newKeys, journalEntry{pk, j.data.Len() - 1})
			} else {
				j.wal.WritePack(WalRecordTypeUpdate, pkg, pos+i, 1)
				if err := j.data.CopyFrom(pkg, updateIdx, pos+i, 1); err != nil {
					return count, err
				}
			}
			count++
			j.lastid = util.MaxU64(j.lastid, pk)
		}
	}

	j.undelete(pks)
	if !isSorted {
		sort.Sort(newKeys)
	}
	j.mergeKeys(newKeys)
	j.deleted.Resize(len(j.keys))
	j.sortData = j.sortData || !isSorted
	j.maxid = util.MaxU64(j.maxid, j.lastid)

	return count, nil
}

func (j *Journal) Update(item Item) error {
	pk := item.ID()
	if pk == 0 {
		return fmt.Errorf("pack: missing primary key on %T item", item)
	}

	j.wal.Write(WalRecordTypeUpdate, pk, item)

	if idx, _ := j.PkIndex(pk, 0); idx < 0 {
		if err := j.data.Push(item); err != nil {
			return err
		}
		j.undelete([]uint64{pk})
		j.mergeKeys(journalEntryList{journalEntry{pk, j.data.Len() - 1}})
		j.deleted.Resize(len(j.keys))
		j.sortData = j.sortData || pk < j.lastid
		j.lastid = util.MaxU64(j.lastid, pk)
		j.maxid = util.MaxU64(j.maxid, pk)
	} else {
		if err := j.data.ReplaceAt(idx, item); err != nil {
			return err
		}
		j.undelete([]uint64{pk})
	}

	return nil
}

func (j *Journal) UpdateBatch(batch []Item) (int, error) {
	SortItems(batch)
	newPks := make([]uint64, len(batch))

	for i, item := range batch {
		pk := item.ID()
		if pk == 0 {
			return 0, fmt.Errorf("pack: missing primary key on %T item", item)
		}
		newPks[i] = pk
	}

	j.wal.WriteMulti(WalRecordTypeUpdate, newPks, batch)

	var last, idx, count int
	newPks = newPks[:0]
	newKeys := make(journalEntryList, 0, len(batch))
	for _, item := range batch {
		pk := item.ID()

		idx, last = j.PkIndex(pk, last)
		if idx < 0 {
			if err := j.data.Push(item); err != nil {
				return count, err
			}
			count++
			newKeys = append(newKeys, journalEntry{pk, j.data.Len() - 1})
			j.sortData = j.sortData || pk < j.lastid
			j.lastid = util.MaxU64(j.lastid, pk)

		} else {
			if err := j.data.ReplaceAt(idx, item); err != nil {
				return count, err
			}
			count++
		}
		newPks = append(newPks, pk)
	}

	j.undelete(newPks)
	j.mergeKeys(newKeys)
	j.deleted.Resize(len(j.keys))
	j.maxid = util.MaxU64(j.maxid, j.lastid)

	return count, nil
}

func (j *Journal) mergeKeys(newKeys journalEntryList) {
	if len(newKeys) == 0 {
		return
	}

	if cap(j.keys) < len(j.keys)+len(newKeys) {
		cp := make(journalEntryList, len(j.keys), roundSize(len(j.keys)+len(newKeys)))
		copy(cp, j.keys)
		j.keys = cp
	}

	if len(j.keys) == 0 || newKeys[0].pk > j.keys[len(j.keys)-1].pk {
		j.keys = append(j.keys, newKeys...)
		return
	}

	last := len(j.keys) - 1
	j.keys = j.keys[:len(j.keys)+len(newKeys)]

	for in1, in2, out := last, len(newKeys)-1, len(j.keys)-1; in2 >= 0; {
		for in2 >= 0 && (in1 < 0 || j.keys[in1].pk < newKeys[in2].pk) {
			j.keys[out] = newKeys[in2]
			in2--
			out--
		}
		for in1 >= 0 && (in2 < 0 || j.keys[in1].pk >= newKeys[in2].pk) {
			j.keys[out] = j.keys[in1]
			in1--
			out--
		}
	}
}

func (j *Journal) Delete(pk uint64) (int, error) {
	if pk <= 0 || pk > j.maxid {
		return 0, fmt.Errorf("pack: delete pk out-of-bounds")
	}

	j.wal.Write(WalRecordTypeDelete, pk, nil)
	idx, _ := j.PkIndex(pk, 0)
	if idx >= 0 {
		j.data.SetFieldAt(j.data.pkindex, idx, uint64(0))
		j.deleted.Set(idx)
	}

	if cap(j.tomb) < len(j.tomb)+1 {
		cp := make([]uint64, len(j.tomb), roundSize(len(j.tomb)+1))
		copy(cp, j.tomb)
		j.tomb = cp
	}

	idx = sort.Search(len(j.tomb), func(i int) bool { return j.tomb[i] >= pk })
	if idx < len(j.tomb) && j.tomb[idx] == pk {
		return 0, nil
	}

	j.tomb = j.tomb[:len(j.tomb)+1]
	copy(j.tomb[idx+1:], j.tomb[idx:])
	j.tomb[idx] = pk

	return 1, nil
}

func (j *Journal) DeleteBatch(pks []uint64) (int, error) {
	if len(pks) == 0 {
		return 0, nil
	}

	pks = vec.Uint64Slice(pks).Unique()
	for pks[0] == 0 {
		pks = pks[1:]
	}

	if pks[len(pks)-1] > j.maxid {
		return 0, fmt.Errorf("pack: delete pk out-of-bounds")
	}

	j.wal.WriteMulti(WalRecordTypeDelete, pks, nil)

	var last, idx int
	for _, pk := range pks {
		if idx, last = j.PkIndex(pk, last); idx >= 0 {
			j.data.SetFieldAt(j.data.pkindex, idx, uint64(0))
			j.deleted.Set(idx)
		}
		if last == j.data.Len() {
			break
		}
	}

	if cap(j.tomb) < len(j.tomb)+len(pks) {
		cp := make([]uint64, len(j.tomb), roundSize(len(j.tomb)+len(pks)))
		copy(cp, j.tomb)
		j.tomb = cp
	}

	if len(j.tomb) == 0 || pks[0] > j.tomb[len(j.tomb)-1] {
		j.tomb = append(j.tomb, pks...)
		return len(pks), nil
	}

	last, count, move := len(j.tomb)-1, len(pks), 0
	j.tomb = j.tomb[:len(j.tomb)+len(pks)]

	for in1, in2, out := last, len(pks)-1, len(j.tomb)-1; in2 >= 0; {
		for in2 >= 0 && in1 >= 0 && j.tomb[in1] == pks[in2] {
			move++
			count--
			in2--
		}

		for in2 >= 0 && (in1 < 0 || j.tomb[in1] < pks[in2]) {
			j.tomb[out] = pks[in2]
			in2--
			out--
		}

		for in1 >= 0 && (in2 < 0 || j.tomb[in1] > pks[in2]) {
			j.tomb[out] = j.tomb[in1]
			in1--
			out--
		}
	}

	if move > 0 {
		copy(j.tomb[:len(j.tomb)-move], j.tomb[move:])
		j.tomb = j.tomb[:len(j.tomb)-move]
	}

	return count, nil
}

func (j *Journal) undelete(pks []uint64) {
	var idx, last, lastTomb int
	for len(pks) > 0 {
		idx, last = j.PkIndex(pks[0], last)
		if idx > -1 {
			j.deleted.Clear(idx)
			j.data.SetFieldAt(j.data.pkindex, idx, pks[0])
		}
		next := sort.Search(len(j.tomb)-lastTomb, func(k int) bool { return j.tomb[lastTomb+k] >= pks[0] })
		if lastTomb+next < len(j.tomb) && j.tomb[lastTomb+next] == pks[0] {
			count := 1
			for {
				if count >= len(pks) {
					break
				}
				if lastTomb+next+count >= len(j.tomb) {
					break
				}
				if j.tomb[lastTomb+next+count] != pks[count] {
					break
				}
				count++
				idx, last = j.PkIndex(pks[count], last)
				if idx > -1 {
					j.deleted.Clear(idx)
					j.data.SetFieldAt(j.data.pkindex, idx, pks[count])
				}
			}
			pks = pks[count:]
			j.tomb = append(j.tomb[:lastTomb+next], j.tomb[lastTomb+next+count:]...)
			lastTomb += next
		} else {
			pks = pks[1:]
		}
	}
}

func (j *Journal) IsDeleted(pk uint64, last int) (bool, int) {
	if last >= len(j.tomb) {
		return false, len(j.tomb)
	}

	idx := sort.Search(len(j.tomb)-last, func(i int) bool { return j.tomb[last+i] >= pk })

	if last+idx < len(j.tomb) && j.tomb[last+idx] == pk {
		return true, last + idx
	}

	if last+idx == len(j.tomb) {
		return false, len(j.tomb)
	}

	return false, last
}

func (j *Journal) PkIndex(pk uint64, last int) (int, int) {
	if pk > j.lastid || last >= len(j.keys) {
		return -1, len(j.keys)
	}

	idx := sort.Search(len(j.keys)-last, func(i int) bool { return j.keys[last+i].pk >= pk })

	if last+idx < len(j.keys) && j.keys[last+idx].pk == pk {
		return j.keys[last+idx].idx, last + idx
	}
	if last+idx == len(j.keys) {
		return -1, len(j.keys)
	}
	return -1, last
}

func (j *Journal) checkInvariants(when string) error {
	if a, b := j.data.Len(), len(j.keys); a != b {
		return fmt.Errorf("journal %s: INVARIANT VIOLATION: data-pack-len=%d key-len=%d", when, a, b)
	}
	if a, b := j.data.Len(), j.deleted.Size(); a != b {
		return fmt.Errorf("journal %s: INVARIANT VIOLATION: data-pack-len=%d deleted-bitset-len=%d", when, a, b)
	}
	for i, v := range j.keys {
		if i == 0 {
			continue
		}
		if j.keys[i-1].pk > v.pk {
			return fmt.Errorf("journal %s: INVARIANT VIOLATION: unsorted keys", when)
		}
		if j.keys[i-1].pk == v.pk {
			return fmt.Errorf("journal %s: INVARIANT VIOLATION: duplicate key", when)
		}
	}
	for i, v := range j.tomb {
		if i == 0 {
			continue
		}
		if j.tomb[i-1] > v {
			return fmt.Errorf("journal %s: INVARIANT VIOLATION: unsorted tomb %#v", when, j.tomb)
		}
		if j.tomb[i-1] == v {
			return fmt.Errorf("journal %s: INVARIANT VIOLATION: duplicate tomb pk %#v", when, j.tomb)
		}
	}
	pks := j.data.PkColumn()
	sorted := make([]uint64, len(pks))
	copy(sorted, pks)
	sorted = vec.Uint64Slice(sorted).Sort()
	for i, v := range sorted {
		if i == 0 || v == 0 || sorted[i-1] == 0 {
			continue
		}
		if have, want := v, sorted[i-1]; have == want {
			return fmt.Errorf("journal %s: INVARIANT VIOLATION: duplicate pk %d in data pack", when, v)
		}
	}
	return nil
}

type dualSorter struct {
	pk []uint64
	id []int
}

func (s dualSorter) Len() int           { return len(s.pk) }
func (s dualSorter) Less(i, j int) bool { return s.pk[i] < s.pk[j] }
func (s dualSorter) Swap(i, j int) {
	s.pk[i], s.pk[j] = s.pk[j], s.pk[i]
	s.id[i], s.id[j] = s.id[j], s.id[i]
}

func (j *Journal) SortedIndexes(b *vec.BitSet) ([]int, []uint64) {
	ds := dualSorter{
		pk: make([]uint64, b.Count()),
		id: b.Indexes(nil),
	}
	pk := j.data.PkColumn()
	for i, n := range ds.id {
		ds.pk[i] = pk[n]
	}
	sort.Sort(ds)
	firstNonZero := sort.Search(len(ds.pk), func(k int) bool { return ds.pk[k] > 0 })
	ds.id = ds.id[firstNonZero:]
	ds.pk = ds.pk[firstNonZero:]
	return ds.id, ds.pk
}

func (j *Journal) SortedIndexesReversed(b *vec.BitSet) ([]int, []uint64) {
	id, pk := j.SortedIndexes(b)
	for i, j := 0, len(id)-1; i < j; i, j = i+1, j-1 {
		id[i], id[j] = id[j], id[i]
		pk[i], pk[j] = pk[j], pk[i]
	}
	return id, pk
}

func (j *Journal) DataPack() *Package {
	return j.data
}

func (j *Journal) Reset() {
	j.data.Clear()
	if len(j.keys) > 0 {
		j.keys[0].idx = 0
		j.keys[0].pk = 0
		for bp := 1; bp < len(j.keys); bp *= 2 {
			copy(j.keys[bp:], j.keys[:bp])
		}
		j.keys = j.keys[:0]
	}
	if len(j.tomb) > 0 {
		j.tomb[0] = 0
		for bp := 1; bp < len(j.tomb); bp *= 2 {
			copy(j.tomb[bp:], j.tomb[:bp])
		}
		j.tomb = j.tomb[:0]
	}
	j.lastid = 0
	j.sortData = false
	j.deleted.Reset()
	j.wal.Reset()
}
