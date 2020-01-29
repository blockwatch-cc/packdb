// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"math/rand"
	"testing"
	"time"

	"blockwatch.cc/packdb/encoding/block"
)

var packBenchmarkReadWriteSizes = []packBenchmarkSize{
	{"1K", 1 * 1024},
	{"16K", 16 * 1024},
	{"32K", 32 * 1024},
	{"64K", 64 * 1024},
	{"128K", 128 * 1024},
}

var readWriteTestFields = FieldList{
	Field{Index: 0, Name: "I", Alias: "row_id", Type: FieldTypeUint64, Flags: FlagPrimary},
	Field{Index: 1, Name: "T", Alias: "time", Type: FieldTypeDatetime, Flags: 0},
	Field{Index: 2, Name: "h", Alias: "height", Type: FieldTypeUint64, Flags: 0},
	Field{Index: 3, Name: "p", Alias: "tx_n", Type: FieldTypeInt64, Flags: 0},
	Field{Index: 4, Name: "H", Alias: "tx_id", Type: FieldTypeBytes, Flags: 0},
	Field{Index: 5, Name: "L", Alias: "locktime", Type: FieldTypeInt64, Flags: 0},
	Field{Index: 6, Name: "s", Alias: "size", Type: FieldTypeInt64, Flags: 0},
	Field{Index: 7, Name: "S", Alias: "vsize", Type: FieldTypeInt64, Flags: 0},
	Field{Index: 8, Name: "V", Alias: "version", Type: FieldTypeInt64, Flags: 0},
	Field{Index: 9, Name: "N", Alias: "n_in", Type: FieldTypeInt64, Flags: 0},
	Field{Index: 10, Name: "n", Alias: "n_out", Type: FieldTypeInt64, Flags: 0},
	Field{Index: 11, Name: "t", Alias: "type", Type: FieldTypeInt64, Flags: 0},
	Field{Index: 12, Name: "D", Alias: "has_data", Type: FieldTypeBoolean, Flags: 0},
	Field{Index: 13, Name: "v", Alias: "volume", Type: FieldTypeUint64, Flags: 0},
	Field{Index: 14, Name: "f", Alias: "fee", Type: FieldTypeUint64, Flags: 0},
	Field{Index: 15, Name: "d", Alias: "days", Type: FieldTypeFloat64, Flags: FlagConvert, Precision: 5},
}

func makeReadWriteTestPackage(fields FieldList, c block.Compression, sz int) *Package {
	switch c {
	case block.SnappyCompression:
		for i := range fields {
			// store hashes uncompressed
			if fields[i].Name == "H" {
				continue
			}
			fields[i].Flags &^= FlagCompressLZ4
			fields[i].Flags |= FlagCompressSnappy
		}
	case block.LZ4Compression:
		for i := range fields {
			// store hashes uncompressed
			if fields[i].Name == "H" {
				continue
			}
			fields[i].Flags &^= FlagCompressSnappy
			fields[i].Flags |= FlagCompressLZ4
		}
	case block.NoCompression:
		for i := range fields {
			// store hashes uncompressed
			if fields[i].Name == "H" {
				continue
			}
			fields[i].Flags &^= FlagCompressLZ4 | FlagCompressSnappy
		}
	}
	pkg := NewPackage()
	pkg.InitFields(fields, sz)
	now := time.Now().UTC()
	for i := 0; i < sz; i++ {
		pkg.Append()
		pkg.SetFieldAt(0, i, uint64(i+1))
		pkg.SetFieldAt(1, i, now.Add(time.Duration(i+rand.Intn(10))*time.Minute))
		pkg.SetFieldAt(2, i, uint64(i+1))                            // height
		pkg.SetFieldAt(3, i, rand.Intn(1000))                        // tx_n
		pkg.SetFieldAt(4, i, randBytes(32))                          // tx_id
		pkg.SetFieldAt(5, i, int64(i+rand.Intn(1000)))               // locktime
		pkg.SetFieldAt(6, i, rand.Intn(4096))                        // size
		pkg.SetFieldAt(7, i, rand.Intn(4096))                        // vsize
		pkg.SetFieldAt(8, i, 2)                                      // version
		pkg.SetFieldAt(9, i, rand.Intn(5))                           // n_in
		pkg.SetFieldAt(10, i, rand.Intn(100))                        // n_out
		pkg.SetFieldAt(11, i, rand.Intn(5))                          // type
		pkg.SetFieldAt(12, i, rand.Intn(1) > 0)                      // has_data
		pkg.SetFieldAt(13, i, uint64(rand.Int63n(2100000000000000))) // volume
		pkg.SetFieldAt(14, i, uint64(rand.Intn(100000000)))          // fee
		pkg.SetFieldAt(15, i, float64(rand.Intn(100000))/1000.0)     // days
	}
	return pkg
}

func BenchmarkPackWriteLZ4(B *testing.B) {
	for _, n := range packBenchmarkReadWriteSizes {
		B.Run(n.name, func(B *testing.B) {
			pkg := makeReadWriteTestPackage(readWriteTestFields, block.LZ4Compression, n.l)
			buf, err := pkg.MarshalBinary()
			if err != nil {
				B.Fatalf("write error: %v", err)
			}
			B.ResetTimer()
			B.ReportAllocs()
			B.SetBytes(int64(len(buf)))
			for b := 0; b < B.N; b++ {
				_, _ = pkg.MarshalBinary()
			}
		})
	}
}

func BenchmarkPackWriteSnappy(B *testing.B) {
	for _, n := range packBenchmarkReadWriteSizes {
		B.Run(n.name, func(B *testing.B) {
			pkg := makeReadWriteTestPackage(readWriteTestFields, block.SnappyCompression, n.l)
			buf, err := pkg.MarshalBinary()
			if err != nil {
				B.Fatalf("write error: %v", err)
			}
			B.ResetTimer()
			B.ReportAllocs()
			B.SetBytes(int64(len(buf)))
			for b := 0; b < B.N; b++ {
				_, err := pkg.MarshalBinary()
				if err != nil {
					B.Fatalf("write error: %v", err)
				}
			}
		})
	}
}

func BenchmarkPackWriteNoCompression(B *testing.B) {
	for _, n := range packBenchmarkReadWriteSizes {
		B.Run(n.name, func(B *testing.B) {
			pkg := makeReadWriteTestPackage(readWriteTestFields, block.NoCompression, n.l)
			buf, err := pkg.MarshalBinary()
			if err != nil {
				B.Fatalf("write error: %v", err)
			}
			B.ResetTimer()
			B.ReportAllocs()
			B.SetBytes(int64(len(buf)))
			for b := 0; b < B.N; b++ {
				_, err := pkg.MarshalBinary()
				if err != nil {
					B.Fatalf("write error: %v", err)
				}
			}
		})
	}
}

func BenchmarkPackReadLZ4(B *testing.B) {
	for _, n := range packBenchmarkReadWriteSizes {
		B.Run(n.name, func(B *testing.B) {
			pkg := makeReadWriteTestPackage(readWriteTestFields, block.LZ4Compression, n.l)
			buf, err := pkg.MarshalBinary()
			if err != nil {
				B.Fatalf("write error: %v", err)
			}
			B.ResetTimer()
			B.ReportAllocs()
			B.SetBytes(int64(len(buf)))
			for b := 0; b < B.N; b++ {
				pkg2 := NewPackage()
				err := pkg2.UnmarshalBinary(buf)
				if err != nil {
					B.Fatalf("read error: %v", err)
				}
			}
		})
	}
}

func BenchmarkPackReadSnappy(B *testing.B) {
	for _, n := range packBenchmarkReadWriteSizes {
		B.Run(n.name, func(B *testing.B) {
			pkg := makeReadWriteTestPackage(readWriteTestFields, block.SnappyCompression, n.l)
			buf, err := pkg.MarshalBinary()
			if err != nil {
				B.Fatalf("write error: %v", err)
			}
			B.ResetTimer()
			B.ReportAllocs()
			B.SetBytes(int64(len(buf)))
			for b := 0; b < B.N; b++ {
				pkg2 := NewPackage()
				err := pkg2.UnmarshalBinary(buf)
				if err != nil {
					B.Fatalf("read error: %v", err)
				}
			}
		})
	}
}

func BenchmarkPackReadNoCompression(B *testing.B) {
	for _, n := range packBenchmarkReadWriteSizes {
		B.Run(n.name, func(B *testing.B) {
			pkg := makeReadWriteTestPackage(readWriteTestFields, block.NoCompression, n.l)
			buf, err := pkg.MarshalBinary()
			if err != nil {
				B.Fatalf("write error: %v", err)
			}
			B.ResetTimer()
			B.ReportAllocs()
			B.SetBytes(int64(len(buf)))
			for b := 0; b < B.N; b++ {
				pkg2 := NewPackage()
				err := pkg2.UnmarshalBinary(buf)
				if err != nil {
					B.Fatalf("read error: %v", err)
				}
			}
		})
	}
}
