// Copyright (c) 2018-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
	"strings"

	"blockwatch.cc/packdb/encoding/block"
	"blockwatch.cc/packdb/encoding/csv"
	"blockwatch.cc/packdb/util"
)

type DumpMode int

const (
	DumpModeDec DumpMode = iota
	DumpModeHex
	DumpModeCSV
)

type CSVHeader struct {
	Key   string `csv:"Pack Key"`
	Cols  int    `csv:"Columns"`
	Rows  int    `csv:"Rows"`
	MinPk uint64 `csv:"Min RowId"`
	MaxPk uint64 `csv:"Max RowId"`
	Size  int    `csv:"Pack Size"`
}

func (t *Table) DumpType(w io.Writer) error {
	return t.journal.DataPack().DumpType(w)
}

func (t *Table) DumpPackInfo(w io.Writer, mode DumpMode, sorted bool) error {
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	switch mode {
	case DumpModeDec, DumpModeHex:
		fmt.Fprintf(w, "%-3s %-10s %-7s %-7s %-21s %-21s %-10s\n",
			"#", "Key", "Fields", "Values", "Min", "Max", "Size")
	}
	var i int
	for i = 0; i < t.packs.Len(); i++ {
		switch mode {
		case DumpModeDec, DumpModeHex:
			fmt.Fprintf(w, "%-3d ", i)
		}
		if sorted {
			err = t.packs.GetSorted(i).Dump(w, mode, len(t.fields))
		} else {
			err = t.packs.Get(i).Dump(w, mode, len(t.fields))
		}
		if err != nil {
			return err
		}
	}
	switch mode {
	case DumpModeDec, DumpModeHex:
		fmt.Fprintf(w, "%-3d ", i)
		i++
	}
	info := t.journal.DataPack().Info()
	info.UpdateStats(t.journal.DataPack())
	if err := info.Dump(w, mode, len(t.fields)); err != nil {
		return err
	}
	return nil
}

func (t *Table) DumpPackInfoDetail(w io.Writer, mode DumpMode, sorted bool) error {
	switch mode {
	case DumpModeDec, DumpModeHex:
	default:
		// unsupported
		return nil
	}
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var i int
	for i = 0; i < t.packs.Len(); i++ {
		var info PackInfo
		if sorted {
			info = t.packs.GetSorted(i)
		} else {
			info = t.packs.Get(i)
		}
		info.DumpDetail(w)
	}
	return nil
}

func (t *Table) DumpJournal(w io.Writer, mode DumpMode) error {
	err := t.journal.DataPack().DumpData(w, mode, t.fields.Aliases())
	if err != nil {
		return err
	}
	w.Write([]byte("keys:"))
	for _, v := range t.journal.keys {
		w.Write([]byte(strconv.FormatUint(v.pk, 10)))
		w.Write([]byte(">"))
		w.Write([]byte(strconv.Itoa(v.idx)))
		w.Write([]byte(","))
	}
	w.Write([]byte("\n"))
	w.Write([]byte("tomb:"))
	for _, v := range t.journal.tomb {
		w.Write([]byte(strconv.FormatUint(v, 10)))
		w.Write([]byte(","))
	}
	w.Write([]byte("\n"))
	w.Write([]byte("dbits:"))
	w.Write([]byte(hex.EncodeToString(t.journal.deleted.Bytes())))
	w.Write([]byte("\n"))
	return nil
}

func (t *Table) DumpPack(w io.Writer, i int, mode DumpMode) error {
	if i >= t.packs.Len() || i < 0 {
		return ErrPackNotFound
	}
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	pkg, err := t.loadSharedPack(tx, t.packs.Get(i).Key, false, nil)
	if err != nil {
		return err
	}
	return pkg.DumpData(w, mode, t.fields.Aliases())
}

func (t *Table) WalkPacks(fn func(*Package) error) error {
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for i := 0; i < t.packs.Len(); i++ {
		pkg, err := t.loadSharedPack(tx, t.packs.Get(i).Key, false, nil)
		if err != nil {
			return err
		}
		if err := fn(pkg); err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) WalkPacksRange(start, end int, fn func(*Package) error) error {
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	if start < 0 {
		start = 0
	}
	if end < 0 {
		end = t.packs.Len() - 1
	}
	defer tx.Rollback()
	for i := start; i <= end && i < t.packs.Len(); i++ {
		pkg, err := t.loadSharedPack(tx, t.packs.Get(i).Key, false, nil)
		if err != nil {
			return err
		}
		if err := fn(pkg); err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) DumpIndexPack(w io.Writer, i, p int, mode DumpMode) error {
	if i >= len(t.indexes) || i < 0 {
		return ErrIndexNotFound
	}
	if p >= t.indexes[i].packs.Len() || p < 0 {
		return ErrPackNotFound
	}
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	pkg, err := t.indexes[i].loadSharedPack(tx, t.indexes[i].packs.Get(p).Key, false)
	if err != nil {
		return err
	}
	return pkg.DumpData(w, mode, []string{"Hash", "Pk"})
}

func (t *Table) DumpPackBlocks(w io.Writer, mode DumpMode) error {
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	switch mode {
	case DumpModeDec, DumpModeHex:
		fmt.Fprintf(w, "%-5s %-10s %-7s %-10s %-7s %-33s %-33s %-4s %-5s %-6s %-10s %-12s %-10s %-10s\n",
			"#", "Key", "Block", "Type", "Rows", "Min", "Max", "Prec", "Flags", "Comp", "Compressed", "Uncompressed", "Memory", "GoType")
	}
	lineNo := 1
	for i := 0; i < t.packs.Len(); i++ {
		pkg, err := t.loadSharedPack(tx, t.packs.Get(i).Key, false, nil)
		if err != nil {
			return err
		}
		if n, err := pkg.DumpBlocks(w, mode, lineNo); err != nil {
			return err
		} else {
			lineNo = n
		}
	}
	return nil
}

func (t *Table) DumpIndexPackInfo(w io.Writer, mode DumpMode, sorted bool) error {
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, v := range t.indexes {
		if err := v.dumpPackInfo(tx, w, mode, sorted); err != nil {
			return err
		}
	}
	return nil
}

func (idx *Index) dumpPackInfo(tx *Tx, w io.Writer, mode DumpMode, sorted bool) error {
	min, max := idx.packs.GlobalMinMax()
	fmt.Fprintf(w, "Num Packs  %d\n", idx.packs.Len())
	fmt.Fprintf(w, "Num Tuples %d\n", idx.packs.Count())
	fmt.Fprintf(w, "Global     min=%d max=%d\n", min, max)
	fmt.Fprintf(w, "Heap Size  %d\n", idx.packs.HeapSize())
	fmt.Fprintf(w, "Table Size %d\n", idx.packs.TableSize())
	smin, smax := idx.packs.MinMaxSlices()
	fmt.Fprintf(w, "Mins       %#v\n", smin)
	fmt.Fprintf(w, "Maxs       %#v\n", smax)

	switch mode {
	case DumpModeDec, DumpModeHex:
		fmt.Fprintf(w, "%-3s %-10s %-7s %-7s %-21s %-21s %-10s\n",
			"#", "Key", "Fields", "Values", "Min", "Max", "Size")
	}
	var (
		i   int
		err error
	)
	for i = 0; i < idx.packs.Len(); i++ {
		switch mode {
		case DumpModeDec, DumpModeHex:
			fmt.Fprintf(w, "%-3d ", i)
		}
		if sorted {
			err = idx.packs.GetSorted(i).Dump(w, mode, 2)
		} else {
			err = idx.packs.Get(i).Dump(w, mode, 2)
		}
		if err != nil {
			return err
		}
	}
	switch mode {
	case DumpModeDec, DumpModeHex:
		fmt.Fprintf(w, "%-3d ", i)
		i++
	}
	info := idx.journal.Info()
	info.UpdateStats(idx.journal)
	if err := info.Dump(w, mode, 2); err != nil {
		return err
	}
	switch mode {
	case DumpModeDec, DumpModeHex:
		fmt.Fprintf(w, "%-3d ", i)
		i++
	}
	info = idx.tombstone.Info()
	info.UpdateStats(idx.tombstone)
	return info.Dump(w, mode, 2)
}

func (h PackInfo) Dump(w io.Writer, mode DumpMode, nfields int) error {
	var key string
	switch true {
	case h.Key == journalKey:
		key = "journal"
	case h.Key == tombstoneKey:
		key = "tombstone"
	default:
		key = util.ToString(h.EncodedKey())
	}
	head := h.Blocks[0]
	min, max := head.MinValue.(uint64), head.MaxValue.(uint64)
	switch mode {
	case DumpModeDec:
		_, err := fmt.Fprintf(w, "%-10s %-7d %-7d %-21d %-21d %-10s\n",
			key,
			nfields,
			h.NValues,
			min,
			max,
			util.ByteSize(h.Packsize))
		return err
	case DumpModeHex:
		_, err := fmt.Fprintf(w, "%-10s %-7d %-7d %21x %21x %-10s\n",
			key,
			nfields,
			h.NValues,
			min,
			max,
			util.ByteSize(h.Packsize))
		return err
	case DumpModeCSV:
		enc, ok := w.(*csv.Encoder)
		if !ok {
			enc = csv.NewEncoder(w)
		}
		ch := CSVHeader{
			Key:   key,
			Cols:  nfields,
			Rows:  h.NValues,
			MinPk: min,
			MaxPk: max,
			Size:  h.Packsize,
		}
		return enc.EncodeRecord(ch)
	}
	return nil
}

func (i PackInfo) DumpDetail(w io.Writer) error {
	fmt.Fprintf(w, "Pack Key   %08x ------------------------------------\n", i.Key)
	fmt.Fprintf(w, "Values     %s\n", util.PrettyInt(i.NValues))
	fmt.Fprintf(w, "Pack Size  %s\n", util.ByteSize(i.Packsize))
	fmt.Fprintf(w, "Meta Size  %s\n", util.ByteSize(i.HeapSize()))
	fmt.Fprintf(w, "Blocks     %d\n", len(i.Blocks))
	fmt.Fprintf(w, "%-3s %-10s %-7s %-7s %-7s %-33s %-33s %-10s %-10s\n",
		"#", "Type", "Comp", "Prec", "Card", "Min", "Max", "Bloom", "Flags")
	for id, head := range i.Blocks {
		var blen uint
		if head.Bloom != nil {
			blen = head.Bloom.Len()
		}
		fmt.Fprintf(w, "%-3d %-10s %-7s %-7d %-7d %-33s %-33s %-10d %-10s\n",
			id,
			head.Type,
			head.Compression,
			head.Precision,
			head.Cardinality,
			util.LimitStringEllipsis(util.ToString(head.MinValue), 33),
			util.LimitStringEllipsis(util.ToString(head.MaxValue), 33),
			blen,
			head.Flags,
		)
	}
	return nil
}

func (p *Package) DumpType(w io.Writer) error {
	typname := "undefined"
	if p.tinfo != nil {
		typname = p.tinfo.name
	}
	var key string
	switch true {
	case p.key == journalKey:
		key = "journal"
	case p.key == tombstoneKey:
		key = "tombstone"
	default:
		key = util.ToString(p.key)
	}
	fmt.Fprintf(w, "Package ------------------------------------ \n")
	fmt.Fprintf(w, "Key:        %s\n", key)
	fmt.Fprintf(w, "Version:    %d\n", p.version)
	fmt.Fprintf(w, "Fields:     %d\n", p.nFields)
	fmt.Fprintf(w, "Values:     %d\n", p.nValues)
	fmt.Fprintf(w, "Pk index:   %d\n", p.pkindex)
	fmt.Fprintf(w, "Type:       %s\n", typname)
	fmt.Fprintf(w, "Size:       %s (%d) zipped, %s (%d) unzipped\n",
		util.ByteSize(p.packedsize), p.packedsize,
		util.ByteSize(p.rawsize), p.rawsize,
	)
	for i, v := range p.blocks {
		d, fi := "", ""
		if v.Dirty {
			d = "*"
		}
		if p.tinfo != nil {
			fi = p.tinfo.fields[i].String()
		}
		var sz int
		if i+1 < len(p.blocks) {
			sz = p.offsets[i+1] - p.offsets[i]
		} else {
			sz = p.rawsize - p.offsets[i]
		}
		fmt.Fprintf(w, "Block %-02d:   %s typ=%d comp=%s flags=%d prec=%d len=%d min=%s max=%s sz=%s %s %s\n",
			i,
			p.names[i],
			v.Type,
			v.Compression,
			v.Flags,
			v.Precision,
			v.Len(),
			util.ToString(v.MinValue),
			util.ToString(v.MaxValue),
			util.PrettyInt(sz),
			fi,
			d)
	}
	return nil
}

func (p *Package) DumpBlocks(w io.Writer, mode DumpMode, lineNo int) (int, error) {
	var key string
	switch true {
	case p.key == journalKey:
		key = "journal"
	case p.key == tombstoneKey:
		key = "tombstone"
	default:
		key = util.ToString(p.key)
	}
	switch mode {
	case DumpModeDec, DumpModeHex:
		for i, v := range p.blocks {
			fi := ""
			if p.tinfo != nil {
				fi = p.tinfo.fields[i].String()
			}
			var sz int
			if i+1 < len(p.blocks) {
				sz = p.offsets[i+1] - p.offsets[i]
			} else {
				sz = p.rawsize - p.offsets[i]
			}
			head := v.MakeHeader()
			_, err := fmt.Fprintf(w, "%-5d %-10s %-7d %-10s %-7d %-33s %-33s %-4d %-5d %-6s %-10s %-12s %-10s %-10s\n",
				lineNo,
				key,
				i,
				head.Type,
				v.Len(),
				util.LimitStringEllipsis(util.ToString(head.MinValue), 33),
				util.LimitStringEllipsis(util.ToString(head.MaxValue), 33),
				head.Precision,
				head.Flags,
				head.Compression,
				util.PrettyInt(sz),
				util.PrettyInt(v.MaxStoredSize()),
				util.PrettyInt(v.HeapSize()),
				fi,
			)
			lineNo++
			if err != nil {
				return lineNo, err
			}
		}
	case DumpModeCSV:
		enc, ok := w.(*csv.Encoder)
		if !ok {
			enc = csv.NewEncoder(w)
		}
		cols := make([]interface{}, 14)
		for i, v := range p.blocks {
			fi := ""
			if p.tinfo != nil {
				fi = p.tinfo.fields[i].String()
			}
			var sz int
			if i+1 < len(p.blocks) {
				sz = p.offsets[i+1] - p.offsets[i]
			} else {
				sz = p.rawsize - p.offsets[i]
			}
			head := v.MakeHeader()
			cols[0] = key
			cols[1] = i
			cols[2] = p.names[i]
			cols[3] = head.Type.String()
			cols[4] = v.Len()
			cols[5] = head.MinValue
			cols[6] = head.MaxValue
			cols[7] = head.Precision
			cols[8] = head.Flags
			cols[9] = head.Compression.String()
			cols[10] = sz
			cols[11] = v.HeapSize()
			cols[12] = v.MaxStoredSize()
			cols[13] = fi
			if !enc.HeaderWritten() {
				if err := enc.EncodeHeader([]string{
					"Pack",
					"Block",
					"Name",
					"Type",
					"Columns",
					"Min",
					"Max",
					"Precision",
					"Flags",
					"Compression",
					"Compressed",
					"Uncompressed",
					"Memory",
					"GoType",
				}, nil); err != nil {
					return lineNo, err
				}
			}
			if err := enc.EncodeRecord(cols); err != nil {
				return lineNo, err
			}
			lineNo++
		}
	}
	return lineNo, nil
}

func (p *Package) DumpData(w io.Writer, mode DumpMode, aliases []string) error {
	names := p.names
	if len(aliases) == p.nFields && len(aliases[0]) > 0 {
		names = aliases
	}

	switch mode {
	case DumpModeDec, DumpModeHex:
		sz := make([]int, p.nFields)
		row := make([]string, p.nFields)
		for j := 0; j < p.nFields; j++ {
			sz[j] = len(names[j])
		}
		for i, l := 0, util.Min(500, p.nValues); i < l; i++ {
			for j := 0; j < p.nFields; j++ {
				var str string
				if p.blocks[j].Type == block.BlockIgnore {
					str = "[strip]"
				} else {
					val, _ := p.FieldAt(j, i)
					str = util.ToString(val)
				}
				sz[j] = util.Max(sz[j], len(str))
			}
		}
		for j := 0; j < p.nFields; j++ {
			row[j] = fmt.Sprintf("%[2]*[1]s", names[j], -sz[j])
		}
		var out string
		out = "| " + strings.Join(row, " | ") + " |\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}
		for j := 0; j < p.nFields; j++ {
			row[j] = strings.Repeat("-", sz[j])
		}
		out = "|-" + strings.Join(row, "-|-") + "-|\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}
		for i := 0; i < p.nValues; i++ {
			for j := 0; j < p.nFields; j++ {
				var str string
				if p.blocks[j].Type == block.BlockIgnore {
					str = "[strip]"
				} else {
					val, _ := p.FieldAt(j, i)
					str = util.ToString(val)
				}
				row[j] = fmt.Sprintf("%[2]*[1]s", str, -sz[j])
			}
			out = "| " + strings.Join(row, " | ") + " |\n"
			if _, err := w.Write([]byte(out)); err != nil {
				return err
			}
		}

	case DumpModeCSV:
		enc, ok := w.(*csv.Encoder)
		if !ok {
			enc = csv.NewEncoder(w)
		}
		if !enc.HeaderWritten() {
			if err := enc.EncodeHeader(names, nil); err != nil {
				return err
			}
		}
		for i := 0; i < p.nValues; i++ {
			row, _ := p.RowAt(i)
			if err := enc.EncodeRecord(row); err != nil {
				return err
			}
		}
	}
	return nil
}

func (n UnboundCondition) Dump() string {
	buf := bytes.NewBuffer(nil)
	n.dump(0, buf)
	return string(buf.Bytes())
}

func (n UnboundCondition) dump(level int, w io.Writer) {
	if n.Leaf() {
		fmt.Fprintln(w, strings.Repeat("  ", level), n.String())
	}
	if len(n.Children) > 0 {
		kind := "AND"
		if n.OrKind {
			kind = "OR"
		}
		fmt.Fprintln(w, strings.Repeat("  ", level), kind)
	}
	for _, v := range n.Children {
		v.dump(level+1, w)
	}
}

func (n ConditionTreeNode) Dump() string {
	buf := bytes.NewBuffer(nil)
	n.dump(0, buf)
	return string(buf.Bytes())
}

func (n ConditionTreeNode) dump(level int, w io.Writer) {
	if n.Leaf() {
		fmt.Fprintln(w, strings.Repeat("  ", level), n.Cond.String())
	}
	if len(n.Children) > 0 {
		kind := "AND"
		if n.OrKind {
			kind = "OR"
		}
		fmt.Fprintln(w, strings.Repeat("  ", level), kind)
	}
	for _, v := range n.Children {
		v.dump(level+1, w)
	}
}

func (q Query) Dump() string {
	buf := bytes.NewBuffer(nil)
	fmt.Fprintln(buf, "Query:", q.Name, "=>")
	q.conds.dump(0, buf)
	return string(buf.Bytes())
}

func (j Join) Dump() string {
	buf := bytes.NewBuffer(nil)
	fmt.Fprintln(buf, "Join:", j.Type.String(), "=>")
	fmt.Fprintln(buf, "  Predicate:", j.Predicate.Left.Alias, j.Predicate.Mode.String(), j.Predicate.Right.Alias)
	fmt.Fprintln(buf, "  Left:", j.Left.Table.Name())
	fmt.Fprintln(buf, "  Where:")
	j.Left.Where.dump(0, buf)
	fmt.Fprintln(buf, "  Fields:", strings.Join(j.Left.Cols, ","))
	fmt.Fprintln(buf, "  AS:", strings.Join(j.Left.ColsAs, ","))
	fmt.Fprintln(buf, "  Limit:", j.Left.Limit)
	fmt.Fprintln(buf, "  Right:", j.Right.Table.Name())
	fmt.Fprintln(buf, "  Where:")
	j.Right.Where.dump(0, buf)
	fmt.Fprintln(buf, "  Fields:", strings.Join(j.Right.Cols, ","))
	fmt.Fprintln(buf, "  AS:", strings.Join(j.Right.ColsAs, ","))
	fmt.Fprintln(buf, "  Limit:", j.Right.Limit)
	return buf.String()
}

func (r Result) Dump() string {
	buf := bytes.NewBuffer(nil)
	fmt.Fprintf(buf, "Result ------------------------------------ \n")
	fmt.Fprintf(buf, "Rows:       %d\n", r.Rows())
	fmt.Fprintf(buf, "Cols:       %d\n", len(r.fields))
	fmt.Fprintf(buf, "%-2s  %-15s  %-15s  %-10s  %-4s  %s\n", "No", "Name", "Alias", "Type", "Prec", "Flags")
	for _, v := range r.fields {
		fmt.Fprintf(buf, "%02d  %-15s  %-15s  %-10s  %2d    %d\n",
			v.Index, v.Name, v.Alias, v.Type, v.Precision, v.Flags)
	}
	return buf.String()
}

func (l PackIndex) Validate() []error {
	errs := make([]error, 0)
	for i := range l.packs {
		head := l.packs[i]
		if head.NValues == 0 {
			errs = append(errs, fmt.Errorf("%03d empty pack", head.Key))
		}
		// check min <= max
		min, max := l.minpks[i], l.maxpks[i]
		if min > max {
			errs = append(errs, fmt.Errorf("%03d min %d > max %d", head.Key, min, max))
		}
		// check invariant
		// - id's don't overlap between packs
		// - same key can span many packs, so min_a == max_b
		// - for long rows of same keys min_a == max_a
		for j := range l.packs {
			if i == j {
				continue
			}
			jmin, jmax := l.minpks[j], l.maxpks[j]
			dist := jmax - jmin + 1

			// single key packs are allowed
			if min == max {
				// check the signle key is not between any other pack (exclusing)
				if jmin < min && jmax > max {
					errs = append(errs, fmt.Errorf("%03d overlaps %03d - key %d E [%d:%d]",
						head.Key, l.packs[j].Key, min, jmin, jmax))
				}
			} else {
				// check min val is not contained in any other pack unless continued
				if min != jmin && min != jmax && min-jmin < dist {
					errs = append(errs, fmt.Errorf("%03d overlaps %03d - min %d E [%d:%d]",
						head.Key, l.packs[j].Key, min, jmin, jmax))
				}

				// check max val is not contained in any other pack unless continued
				if max != jmin && max-jmin < dist {
					errs = append(errs, fmt.Errorf("%03d overlaps %03d - max %d E [%d:%d]",
						head.Key, l.packs[j].Key, max, jmin, jmax))
				}
			}
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return errs
}

func (t *Table) ValidatePackIndex(w io.Writer) error {
	if errs := t.packs.Validate(); errs != nil {
		for _, v := range errs {
			w.Write([]byte(v.Error() + "\n"))
		}
	}
	return nil
}

func (t *Table) ValidateIndexPackIndex(w io.Writer) error {
	for _, idx := range t.indexes {
		if errs := idx.packs.Validate(); errs != nil {
			for _, v := range errs {
				w.Write([]byte(fmt.Sprintf("%s: %v\n", idx.Name, v.Error())))
			}
		}
	}
	return nil
}
