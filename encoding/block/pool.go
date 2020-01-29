// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
	"bytes"
	"io"
	"runtime"
	"sync"

	"blockwatch.cc/packdb/util"
	"github.com/pierrec/lz4"
)

var (
	BlockEncoderPool = &sync.Pool{
		New: func() interface{} { return make([]byte, 0, BlockSizeHint) },
	}
	snappyWriterPool = util.NewGenericPool(
		runtime.NumCPU(),
		func() interface{} { return NewSnappyWriter(nil) },
	)
	lz4WriterPool = util.NewGenericPool(
		runtime.NumCPU(),
		func() interface{} { return lz4.NewWriter(nil) },
	)
	lz4ReaderPool = util.NewGenericPool(
		runtime.NumCPU(),
		func() interface{} { return lz4.NewReader(nil) },
	)

	integerPool = &sync.Pool{
		New: func() interface{} { return make([]int64, 0, DefaultMaxPointsPerBlock) },
	}
	unsignedPool = &sync.Pool{
		New: func() interface{} { return make([]uint64, 0, DefaultMaxPointsPerBlock) },
	}
	boolPool = &sync.Pool{
		New: func() interface{} { return make([]bool, 0, DefaultMaxPointsPerBlock) },
	}
	floatPool = &sync.Pool{
		New: func() interface{} { return make([]float64, 0, DefaultMaxPointsPerBlock) },
	}
	stringPool = &sync.Pool{
		New: func() interface{} { return make([]string, 0, DefaultMaxPointsPerBlock) },
	}
	bytesPool = &sync.Pool{
		New: func() interface{} { return make([][]byte, 0, DefaultMaxPointsPerBlock) },
	}
)

func getWriter(buf *bytes.Buffer, comp Compression) io.WriteCloser {
	switch comp {
	case SnappyCompression:
		enc := snappyWriterPool.Get().(*SnappyWriter)
		enc.Reset(buf)
		return enc
	case LZ4Compression:
		enc := lz4WriterPool.Get().(*lz4.Writer)
		enc.Reset(buf)
		enc.Header.Size = uint64(buf.Cap())
		enc.Header.BlockChecksum = true
		enc.Header.NoChecksum = true
		return enc
	default:
		return NopCloser(buf)
	}
}

func putWriter(w io.Writer, comp Compression) {
	switch comp {
	case SnappyCompression:
		snappyWriterPool.Put(w.(*SnappyWriter))
	case LZ4Compression:
		lz4WriterPool.Put(w.(*lz4.Writer))
	}
}
