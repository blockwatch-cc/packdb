// Copyright (c) 2018-2019 KIDTSUNAMI
// Author: alex@kidtsunami.com
//
// Original from: InfluxData, MIT
// https://github.com/influxdata/influxdb
package compress

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/bits"
	"unsafe"
)

const (
	floatUncompressed      = 0 // unused
	floatCompressedGorilla = 1
)

var (
	errFloatBatchDecodeShortBuffer = fmt.Errorf("pack: FloatArrayDecodeAll short buffer")
)

const uvnan = 0x7FF8000000000001

func FloatArrayEncodedSize(src []float64) int {
	if len(src) == 0 {
		return 19
	}
	return len(src)*9 + 1
}

func FloatArrayEncodeAll(src []float64, w io.Writer) (float64, float64, error) {
	b := make([]byte, 9)

	b = b[:1]
	b[0] = floatCompressedGorilla << 4

	var first float64
	var finished bool
	var min, max float64
	if len(src) > 0 && math.IsNaN(src[0]) {
		return 0, 0, fmt.Errorf("pack: unsupported float value: NaN")
	} else if len(src) == 0 {
		first = math.NaN()
		finished = true
	} else {
		first = src[0]
		src = src[1:]
		min = first
		max = first
	}

	b = b[:9]
	n := uint64(8 + 64)
	prev := math.Float64bits(first)
	binary.BigEndian.PutUint64(b[1:], prev)
	prevLeading, prevTrailing := ^uint64(0), uint64(0)
	var leading, trailing uint64
	var mask uint64
	var sum float64

	for i := 0; !finished; i++ {
		var x float64
		if i < len(src) {
			x = src[i]
			min = math.Min(min, x)
			max = math.Max(max, x)
			sum += x
		} else {
			x = math.NaN()
			finished = true
		}

		{
			cur := math.Float64bits(x)
			vDelta := cur ^ prev
			if vDelta == 0 {
				n++
				prev = cur
				continue
			}

			for n>>3 >= uint64(len(b)) {
				b = append(b, byte(0))
			}

			b[n>>3] |= 128 >> (n & 7)
			n++

			leading = uint64(bits.LeadingZeros64(vDelta))
			trailing = uint64(bits.TrailingZeros64(vDelta))

			leading &= 0x1F
			if leading >= 32 {
				leading = 31
			}

			if (n+2)>>3 >= uint64(len(b)) {
				b = append(b, byte(0))
			}

			if prevLeading != ^uint64(0) && leading >= prevLeading && trailing >= prevTrailing {
				n++

				l := uint64(64 - prevLeading - prevTrailing)
				for (n+l)>>3 >= uint64(len(b)) {
					b = append(b, byte(0))
				}

				v := (vDelta >> prevTrailing) << (64 - l)

				var m = n & 7
				var written uint64
				if m > 0 {
					written = 8 - m
					if l < written {
						written = l
					}
					mask = v >> 56
					b[n>>3] |= byte(mask >> m)
					n += written

					if l-written == 0 {
						prev = cur
						continue
					}
				}

				vv := v << written

				if (n>>3)+8 >= uint64(len(b)) {
					b = append(b, 0, 0, 0, 0, 0, 0, 0, 0)
				}
				binary.BigEndian.PutUint64(b[n>>3:], vv)
				n += (l - written)
			} else {
				prevLeading, prevTrailing = leading, trailing
				b[n>>3] |= 128 >> (n & 7)
				n++

				if (n+5)>>3 >= uint64(len(b)) {
					b = append(b, byte(0))
				}

				var m = n & 7
				l := uint64(5)
				v := leading << 59
				mask = v >> 56

				if m <= 3 {
					b[n>>3] |= byte(mask >> m)
					n += l
				} else {
					written := 8 - m
					b[n>>3] |= byte(mask >> m)
					n += written
					mask = v << written
					mask >>= 56
					m = n & 7
					b[n>>3] |= byte(mask >> m)
					n += (l - written)
				}

				sigbits := 64 - leading - trailing
				if (n+6)>>3 >= uint64(len(b)) {
					b = append(b, byte(0))
				}

				m = n & 7
				l = uint64(6)
				v = sigbits << 58
				mask = v >> 56
				if m <= 2 {
					b[n>>3] |= byte(mask >> m)
					n += l
				} else {
					written := 8 - m
					b[n>>3] |= byte(mask >> m)
					n += written
					mask = v << written
					mask >>= 56
					m = n & 7
					b[n>>3] |= byte(mask >> m)
					n += l - written
				}

				m = n & 7
				l = sigbits
				v = (vDelta >> trailing) << (64 - l)
				for (n+l)>>3 >= uint64(len(b)) {
					b = append(b, byte(0))
				}

				var written uint64
				if m > 0 {
					written = 8 - m
					if l < written {
						written = l
					}
					mask = v >> 56
					b[n>>3] |= byte(mask >> m)
					n += written

					if l-written == 0 {
						prev = cur
						continue
					}
				}

				vv := v << written
				if (n>>3)+8 >= uint64(len(b)) {
					b = append(b, 0, 0, 0, 0, 0, 0, 0, 0)
				}

				binary.BigEndian.PutUint64(b[n>>3:], vv)
				n += (l - written)
			}
			prev = cur
		}
	}

	if math.IsNaN(sum) {
		return 0, 0, fmt.Errorf("pack: unsupported float value: NaN")
	}

	length := n >> 3
	if n&7 > 0 {
		length++
	}

	_, err := w.Write(b[:length])
	return min, max, err
}

var bitMask [64]uint64

func init() {
	v := uint64(1)
	for i := 1; i <= 64; i++ {
		bitMask[i&0x3f] = v
		v = v<<1 | 1
	}
}

func FloatArrayDecodeAll(b []byte, buf []float64) ([]float64, error) {
	if len(b) < 9 {
		return []float64{}, nil
	}

	var (
		val         uint64
		trailingN   uint8
		meaningfulN uint8 = 64
	)

	b = b[1:]

	val = binary.BigEndian.Uint64(b)
	if val == uvnan {
		if buf == nil {
			var tmp [1]float64
			buf = tmp[:0]
		}
		return buf[:0], nil
	}

	buf = buf[:0]
	dst := *(*[]uint64)(unsafe.Pointer(&buf))
	dst = append(dst, val)

	b = b[8:]
	var (
		brCachedVal = uint64(0)
		brValidBits = uint8(0)
	)

	if len(b) >= 8 {
		brCachedVal = binary.BigEndian.Uint64(b)
		brValidBits = 64
		b = b[8:]
	} else if len(b) > 0 {
		brCachedVal = 0
		brValidBits = uint8(len(b) * 8)
		for i := range b {
			brCachedVal = (brCachedVal << 8) | uint64(b[i])
		}
		brCachedVal = bits.RotateLeft64(brCachedVal, -int(brValidBits))
		b = b[:0]
	} else {
		goto ERROR
	}

	for {
		if brValidBits > 0 {
			goto READ0
		}

		if len(b) >= 8 {
			brCachedVal = binary.BigEndian.Uint64(b)
			brValidBits = 64
			b = b[8:]
		} else if len(b) > 0 {
			brCachedVal = 0
			brValidBits = uint8(len(b) * 8)
			for i := range b {
				brCachedVal = (brCachedVal << 8) | uint64(b[i])
			}
			brCachedVal = bits.RotateLeft64(brCachedVal, -int(brValidBits))
			b = b[:0]
		} else {
			goto ERROR
		}

	READ0:
		brValidBits -= 1
		brCachedVal = bits.RotateLeft64(brCachedVal, 1)
		if brCachedVal&1 > 0 {
			if brValidBits > 0 {
				goto READ1
			}

			if len(b) >= 8 {
				brCachedVal = binary.BigEndian.Uint64(b)
				brValidBits = 64
				b = b[8:]
			} else if len(b) > 0 {
				brCachedVal = 0
				brValidBits = uint8(len(b) * 8)
				for i := range b {
					brCachedVal = (brCachedVal << 8) | uint64(b[i])
				}
				brCachedVal = bits.RotateLeft64(brCachedVal, -int(brValidBits))
				b = b[:0]
			} else {
				goto ERROR
			}

		READ1:
			brValidBits -= 1
			brCachedVal = bits.RotateLeft64(brCachedVal, 1)
			if brCachedVal&1 > 0 {
				const leadingTrailingBitCount = 11
				var lmBits uint64
				if brValidBits >= leadingTrailingBitCount {
					brValidBits -= leadingTrailingBitCount
					brCachedVal = bits.RotateLeft64(brCachedVal, leadingTrailingBitCount)
					lmBits = brCachedVal
				} else {
					bits01 := uint8(11)
					if brValidBits > 0 {
						bits01 -= brValidBits
						lmBits = bits.RotateLeft64(brCachedVal, 11)
					}

					if len(b) >= 8 {
						brCachedVal = binary.BigEndian.Uint64(b)
						brValidBits = 64
						b = b[8:]
					} else if len(b) > 0 {
						brCachedVal = 0
						brValidBits = uint8(len(b) * 8)
						for i := range b {
							brCachedVal = (brCachedVal << 8) | uint64(b[i])
						}
						brCachedVal = bits.RotateLeft64(brCachedVal, -int(brValidBits))
						b = b[:0]
					} else {
						goto ERROR
					}
					brCachedVal = bits.RotateLeft64(brCachedVal, int(bits01))
					brValidBits -= bits01
					lmBits &^= bitMask[bits01&0x3f]
					lmBits |= brCachedVal & bitMask[bits01&0x3f]
				}

				lmBits &= 0x7ff
				leadingN := uint8((lmBits >> 6) & 0x1f)
				meaningfulN = uint8(lmBits & 0x3f)
				if meaningfulN > 0 {
					trailingN = 64 - leadingN - meaningfulN
				} else {
					trailingN = 0
					meaningfulN = 64
				}
			}

			var sBits uint64
			if brValidBits >= meaningfulN {
				brValidBits -= meaningfulN
				brCachedVal = bits.RotateLeft64(brCachedVal, int(meaningfulN))
				sBits = brCachedVal
			} else {
				mBits := meaningfulN
				if brValidBits > 0 {
					mBits -= brValidBits
					sBits = bits.RotateLeft64(brCachedVal, int(meaningfulN))
				}

				if len(b) >= 8 {
					brCachedVal = binary.BigEndian.Uint64(b)
					brValidBits = 64
					b = b[8:]
				} else if len(b) > 0 {
					brCachedVal = 0
					brValidBits = uint8(len(b) * 8)
					for i := range b {
						brCachedVal = (brCachedVal << 8) | uint64(b[i])
					}
					brCachedVal = bits.RotateLeft64(brCachedVal, -int(brValidBits))
					b = b[:0]
				} else {
					goto ERROR
				}
				brCachedVal = bits.RotateLeft64(brCachedVal, int(mBits))
				brValidBits -= mBits
				sBits &^= bitMask[mBits&0x3f]
				sBits |= brCachedVal & bitMask[mBits&0x3f]
			}
			sBits &= bitMask[meaningfulN&0x3f]

			val ^= sBits << (trailingN & 0x3f)
			if val == uvnan {
				break
			}
		}

		dst = append(dst, val)
	}

	return *(*[]float64)(unsafe.Pointer(&dst)), nil

ERROR:
	return (*(*[]float64)(unsafe.Pointer(&dst)))[:0], errFloatBatchDecodeShortBuffer
}
