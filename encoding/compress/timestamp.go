// Copyright (c) 2018-2019 KIDTSUNAMI
// Author: alex@kidtsunami.com
//
// Original From: InfluxData, MIT
// https://github.com/influxdata/influxdb
package compress

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"blockwatch.cc/packdb/encoding/simple8b"
)

const (
	timeUncompressed           = 0
	timeCompressedPackedSimple = 1
	timeCompressedRLE          = 2
	timeUncompressedZigZag     = 3
	timeCompressedZigZagPacked = 4
	timeCompressedZigZagRLE    = 5
	timeCompressedInvalid      = 6
)

func TimeArrayEncodedSize(src []int64) int {
	return len(src)*8 + 1
}

func TimeArrayEncodeAll(src []int64, w io.Writer) (int64, int64, error) {
	var (
		maxdelta, div uint64 = 0, 1e12
		ordered       bool   = true
		l             int    = len(src)
	)

	if l == 0 {
		return 0, 0, nil
	}
	min, max := src[l-1], src[l-1]
	deltas := ReintepretInt64ToUint64Slice(src)

	if len(deltas) > 1 {
		for i := 0; i < l && div > 1; i++ {
			v := deltas[i]
			for div > 1 && v%div != 0 {
				div /= 10
			}
		}

		if div > 1 {
			deltas[l-1] /= div
			for i := l - 1; i > 1; i-- {
				if min > src[i-1] {
					min = src[i-1]
				} else if max < src[i-1] {
					max = src[i-1]
				}
				deltas[i-1] /= div
				ordered = ordered && deltas[i-1] <= deltas[i]
				deltas[i] = deltas[i] - deltas[i-1]
				if deltas[i] > maxdelta {
					maxdelta = deltas[i]
				}
			}
			if min > src[0] {
				min = src[0]
			}
			if max < src[0] {
				max = src[0]
			}
			ordered = ordered && deltas[0]/div <= deltas[1]
			deltas[1] = deltas[1] - deltas[0]/div
			if deltas[1] > maxdelta {
				maxdelta = deltas[1]
			}

		} else {
			for i := l - 1; i > 0; i-- {
				if min > src[i-1] {
					min = src[i-1]
				} else if max < src[i-1] {
					max = src[i-1]
				}
				ordered = ordered && deltas[i-1] <= deltas[i]
				deltas[i] = deltas[i] - deltas[i-1]
				if deltas[i] > maxdelta {
					maxdelta = deltas[i]
				}
			}
		}

		if !ordered {
			maxdelta = 0
			for i := 1; i < l; i++ {
				deltas[i] = ZigZagEncode(src[i])
				if deltas[i] > maxdelta {
					maxdelta = deltas[i]
				}
			}
		}

		var rle = true
		for i := 2; i < l; i++ {
			if deltas[1] != deltas[i] {
				rle = false
				break
			}
		}

		if rle {
			typ := byte(timeCompressedRLE) << 4
			if !ordered {
				typ = byte(timeCompressedZigZagRLE) << 4
			}
			if div > 1 {
				typ |= byte(math.Log10(float64(div)))
			}

			w.Write([]byte{typ})

			var b [binary.MaxVarintLen64]byte
			binary.BigEndian.PutUint64(b[:8], deltas[0])
			w.Write(b[:8])

			n := binary.PutUvarint(b[:], deltas[1])
			w.Write(b[:n])

			n = binary.PutUvarint(b[:], uint64(len(deltas)))
			w.Write(b[:n])

			return min, max, nil
		}
	}

	if !ordered {
		deltas[0] = ZigZagEncode(int64(deltas[0] / div))
	}

	if maxdelta > simple8b.MaxValue {
		typ := byte(timeUncompressed) << 4
		if !ordered {
			typ = byte(timeUncompressedZigZag) << 4
		}
		if div > 1 {
			typ |= byte(math.Log10(float64(div)))
		}

		w.Write([]byte{typ})

		for _, v := range deltas {
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], v)
			w.Write(b[:])
		}
		return min, max, nil
	}

	encoded, err := simple8b.EncodeAll(deltas[1:])
	if err != nil {
		return 0, 0, err
	}

	typ := byte(timeCompressedPackedSimple) << 4
	if !ordered {
		typ = byte(timeCompressedZigZagPacked) << 4
	}

	typ |= byte(math.Log10(float64(div)))
	w.Write([]byte{typ})

	var b [8]byte
	binary.BigEndian.PutUint64(b[:], deltas[0])
	w.Write(b[:])

	for _, v := range encoded {
		binary.BigEndian.PutUint64(b[:], v)
		w.Write(b[:])
	}
	return min, max, nil
}

var (
	timeBatchDecoderFunc = [...]func(b []byte, dst []int64) ([]int64, error){
		timeBatchDecodeAllUncompressed,
		timeBatchDecodeAllSimple,
		timeBatchDecodeAllRLE,
		timeBatchDecodeAllZigZag,
		timeBatchDecodeAllZigZagPacked,
		timeBatchDecodeAllZigZagRLE,
		timeBatchDecodeAllInvalid,
	}
)

func TimeArrayDecodeAll(b []byte, dst []int64) ([]int64, error) {
	if len(b) == 0 {
		return []int64{}, nil
	}

	encoding := b[0] >> 4
	if encoding >= timeCompressedInvalid {
		encoding = timeCompressedInvalid
	}
	return timeBatchDecoderFunc[encoding&7](b, dst)
}

func timeBatchDecodeAllUncompressed(b []byte, dst []int64) ([]int64, error) {
	mod := int64(math.Pow10(int(b[0] & 0xF)))

	b = b[1:]
	if len(b)&0x7 != 0 {
		return []int64{}, fmt.Errorf("pack: TimeArrayDecodeAll expected multiple of 8 bytes")
	}

	count := len(b) / 8
	if cap(dst) < count {
		dst = make([]int64, count)
	} else {
		dst = dst[:count]
	}

	prev := uint64(0)
	if mod > 1 {
		for i := range dst {
			prev += binary.BigEndian.Uint64(b[i*8:])
			dst[i] = int64(prev) * mod
		}
	} else {
		for i := range dst {
			prev += binary.BigEndian.Uint64(b[i*8:])
			dst[i] = int64(prev)
		}
	}

	return dst, nil
}

func timeBatchDecodeAllSimple(b []byte, dst []int64) ([]int64, error) {
	if len(b) < 9 {
		return []int64{}, fmt.Errorf("pack: TimeArrayDecodeAll not enough data to decode packed timestamps")
	}

	mod := uint64(math.Pow10(int(b[0] & 0xF)))

	count, err := simple8b.CountBytes(b[9:])
	if err != nil {
		return []int64{}, err
	}

	count += 1

	if cap(dst) < count {
		dst = make([]int64, count)
	} else {
		dst = dst[:count]
	}

	buf := ReintepretInt64ToUint64Slice(dst)

	buf[0] = binary.BigEndian.Uint64(b[1:9])
	n, err := simple8b.DecodeBytesBigEndian(buf[1:], b[9:])
	if err != nil {
		return []int64{}, err
	}
	if n != count-1 {
		return []int64{}, fmt.Errorf("pack: TimeArrayDecodeAll unexpected number of values decoded; got=%d, exp=%d", n, count-1)
	}

	last := buf[0]
	if mod > 1 {
		for i := 1; i < len(buf); i++ {
			dgap := buf[i] * mod
			buf[i] = last + dgap
			last = buf[i]
		}
	} else {
		for i := 1; i < len(buf); i++ {
			buf[i] += last
			last = buf[i]
		}
	}

	return dst, nil
}

func timeBatchDecodeAllRLE(b []byte, dst []int64) ([]int64, error) {
	if len(b) < 9 {
		return []int64{}, fmt.Errorf("pack: TimeArrayDecodeAll not enough data to decode RLE starting value")
	}

	var k, n int
	mod := int64(math.Pow10(int(b[k] & 0xF)))
	k++

	first := binary.BigEndian.Uint64(b[k:])
	k += 8
	delta, n := binary.Uvarint(b[k:])
	if n <= 0 {
		return []int64{}, fmt.Errorf("pack: TimeArrayDecodeAll invalid run length in decodeRLE")
	}
	k += n

	delta *= uint64(mod)
	count, n := binary.Uvarint(b[k:])
	if n <= 0 {
		return []int64{}, fmt.Errorf("pack: TimeArrayDecodeAll invalid repeat value in decodeRLE")
	}

	if cap(dst) < int(count) {
		dst = make([]int64, count)
	} else {
		dst = dst[:count]
	}

	acc := first
	for i := range dst {
		dst[i] = int64(acc)
		acc += delta
	}

	return dst, nil
}

func timeBatchDecodeAllZigZag(b []byte, dst []int64) ([]int64, error) {
	mod := int64(math.Pow10(int(b[0] & 0xF)))

	b = b[1:]
	if len(b)&0x7 != 0 {
		return []int64{}, fmt.Errorf("pack: TimeArrayDecodeAll expected multiple of 8 bytes")
	}

	count := len(b) / 8
	if cap(dst) < count {
		dst = make([]int64, count)
	} else {
		dst = dst[:count]
	}

	prev := int64(0)
	if mod > 1 {
		for i := range dst {
			prev += ZigZagDecode(binary.BigEndian.Uint64(b[i*8:]))
			dst[i] = prev * mod
		}
	} else {
		for i := range dst {
			prev += ZigZagDecode(binary.BigEndian.Uint64(b[i*8:]))
			dst[i] = prev
		}
	}

	return dst, nil
}

func timeBatchDecodeAllZigZagPacked(b []byte, dst []int64) ([]int64, error) {
	if len(b) < 9 {
		return []int64{}, fmt.Errorf("pack: TimeArrayDecodeAll not enough data to decode packed timestamps")
	}

	mod := int64(math.Pow10(int(b[0] & 0xF)))
	count, err := simple8b.CountBytes(b[9:])
	if err != nil {
		return []int64{}, err
	}

	count += 1

	if cap(dst) < count {
		dst = make([]int64, count)
	} else {
		dst = dst[:count]
	}

	buf := ReintepretInt64ToUint64Slice(dst)

	buf[0] = binary.BigEndian.Uint64(b[1:9])
	n, err := simple8b.DecodeBytesBigEndian(buf[1:], b[9:])
	if err != nil {
		return []int64{}, err
	}
	if n != count-1 {
		return []int64{}, fmt.Errorf("pack: TimeArrayDecodeAll unexpected number of values decoded; got=%d, exp=%d", n, count-1)
	}

	prev := int64(0)
	if mod > 1 {
		for i := 0; i < len(buf); i++ {
			prev += ZigZagDecode(buf[i])
			dst[i] = prev * mod
		}
	} else {
		for i := 0; i < len(buf); i++ {
			prev += ZigZagDecode(buf[i])
			dst[i] = prev
		}
	}

	return dst, nil
}

func timeBatchDecodeAllZigZagRLE(b []byte, dst []int64) ([]int64, error) {
	if len(b) < 9 {
		return []int64{}, fmt.Errorf("pack: TimeArrayDecodeAll not enough data to decode RLE starting value")
	}

	var k, n int
	mod := int64(math.Pow10(int(b[k] & 0xF)))
	k++

	first := binary.BigEndian.Uint64(b[k:])
	k += 8

	delta, n := binary.Uvarint(b[k:])
	if n <= 0 {
		return []int64{}, fmt.Errorf("pack: TimeArrayDecodeAll invalid run length in decodeRLE")
	}
	k += n
	delta = uint64(ZigZagDecode(delta))
	delta *= uint64(mod)
	count, n := binary.Uvarint(b[k:])
	if n <= 0 {
		return []int64{}, fmt.Errorf("pack: TimeArrayDecodeAll invalid repeat value in decodeRLE")
	}

	if cap(dst) < int(count) {
		dst = make([]int64, count)
	} else {
		dst = dst[:count]
	}

	acc := first
	for i := range dst {
		dst[i] = int64(acc)
		acc += delta
	}

	return dst, nil
}

func timeBatchDecodeAllInvalid(b []byte, _ []int64) ([]int64, error) {
	return []int64{}, fmt.Errorf("pack: unknown time encoding %v", b[0]>>4)
}
