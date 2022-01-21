// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
	"bytes"
	"io"

	"github.com/golang/snappy"
)

type SnappyWriter struct {
	w    io.Writer
	buf  *bytes.Buffer
	data []byte
}

func NewSnappyWriter(w io.Writer) *SnappyWriter {
	return &SnappyWriter{
		w:    w,
		buf:  bytes.NewBuffer(make([]byte, 0, BlockSizeHint)),
		data: make([]byte, BlockSizeHint),
	}
}

func (s *SnappyWriter) Close() error {
	s.data = snappy.Encode(s.data[:cap(s.data)], s.buf.Bytes())
	_, err := s.w.Write(s.data)
	return err
}

func (s *SnappyWriter) Write(p []byte) (n int, err error) {
	return s.buf.Write(p)
}

func (s *SnappyWriter) Reset(w io.Writer) error {
	s.w = w
	s.data = s.data[:0]
	s.buf.Reset()
	return nil
}

type nopCloser struct {
	io.Writer
}

func (nopCloser) Close() error {
	return nil
}

func (n nopCloser) ReadFrom(r io.Reader) (int64, error) {
	return io.Copy(n.Writer, r)
}

func NopCloser(w io.Writer) io.WriteCloser {
	return nopCloser{w}
}

func pow2(x int64) int64 {
	for i := int64(1); i < 1<<62; i = i << 1 {
		if i >= x {
			return i
		}
	}
	return 1
}
