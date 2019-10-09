// Copyright (c) 2019 KIDTSUNAMI
// Author: alex@kidtsunami.com

package vec

func matchBoolEqualGeneric(src []bool, val bool, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v == val {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchBoolNotEqualGeneric(src []bool, val bool, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v != val {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchBoolLessThanGeneric(src []bool, val bool, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v != val {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchBoolLessThanEqualGeneric(src []bool, val bool, bits []byte) int64 {
	if val {
		for i, _ := range bits[:len(bits)-2] {
			bits[i] = 0xff
		}
		for i := 0; i < len(src)%8; i++ {
			bits[len(bits)] |= 0x1 << uint(7-i)
		}
		return int64(len(src))
	}
	var cnt int64
	for i, v := range src {
		if v == val {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchBoolGreaterThanGeneric(src []bool, val bool, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v != val {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchBoolGreaterThanEqualGeneric(src []bool, val bool, bits []byte) int64 {
	for i, _ := range bits[:len(bits)-2] {
		bits[i] = 0xff
	}
	for i := 0; i < len(src)%8; i++ {
		bits[len(bits)] |= 0x1 << uint(7-i)
	}
	return int64(len(src))
}

func matchBoolBetweenGeneric(src []bool, a, b bool, bits []byte) int64 {
	var cnt int64
	if a != b {
		for i, _ := range bits[:len(bits)-2] {
			bits[i] = 0xff
		}
		for i := 0; i < len(src)%8; i++ {
			bits[len(bits)] |= 0x1 << uint(7-i)
		}
		return int64(len(src))
	}
	if b {
		for i, v := range src {
			if v {
				bits[i>>3] |= 0x1 << uint(7-i&0x7)
				cnt++
			}
		}
	} else {
		for i, v := range src {
			if !v {
				bits[i>>3] |= 0x1 << uint(7-i&0x7)
				cnt++
			}
		}
	}
	return cnt
}
