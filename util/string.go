// Copyright (c) 2013-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package util

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf8"
)

var stringerType = reflect.TypeOf((*fmt.Stringer)(nil)).Elem()

func ToString(t interface{}) string {
	val := reflect.Indirect(reflect.ValueOf(t))
	if !val.IsValid() {
		return ""
	}
	if val.Type().Implements(stringerType) {
		return t.(fmt.Stringer).String()
	}
	if s, err := ToRawString(val.Interface()); err == nil {
		return s
	}
	return fmt.Sprintf("%v", val.Interface())
}

func IsBase64(s string) bool {
	_, err := base64.StdEncoding.DecodeString(s)
	return err == nil
}

func ToRawString(t interface{}) (string, error) {
	val := reflect.Indirect(reflect.ValueOf(t))
	if !val.IsValid() {
		return "", nil
	}
	typ := val.Type()
	switch val.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(val.Int(), 10), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return strconv.FormatUint(val.Uint(), 10), nil
	case reflect.Float32, reflect.Float64:
		return strconv.FormatFloat(val.Float(), 'g', -1, val.Type().Bits()), nil
	case reflect.String:
		return val.String(), nil
	case reflect.Bool:
		return strconv.FormatBool(val.Bool()), nil
	case reflect.Array:
		if typ.Elem().Kind() != reflect.Uint8 {
			break
		}
		// [...]byte
		var b []byte
		if val.CanAddr() {
			b = val.Slice(0, val.Len()).Bytes()
		} else {
			b = make([]byte, val.Len())
			reflect.Copy(reflect.ValueOf(b), val)
		}
		return hex.EncodeToString(b), nil
	case reflect.Slice:
		if typ.Elem().Kind() != reflect.Uint8 {
			break
		}
		// []byte
		b := val.Bytes()
		return hex.EncodeToString(b), nil
	}
	return "", fmt.Errorf("no method for converting type %s (%v) to string", typ.String(), val.Kind())
}

func JsonString(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}

type StringList []string

func (l StringList) AsInterface() []interface{} {
	il := make([]interface{}, len(l))
	for i, v := range l {
		il[i] = v
	}
	return il
}

func (l StringList) Contains(r string) bool {
	for _, v := range l {
		if v == r {
			return true
		}
	}
	return false
}

func (l *StringList) Add(r string) StringList {
	*l = append(*l, r)
	return *l
}

func (l *StringList) AddFront(r string) StringList {
	*l = append([]string{r}, (*l)...)
	return *l
}

func (l *StringList) AddUnique(r string) StringList {
	if !(*l).Contains(r) {
		l.Add(r)
	}
	return *l
}

func (l *StringList) AddUniqueFront(r string) StringList {
	if !(*l).Contains(r) {
		l.AddFront(r)
	}
	return *l
}

func (l StringList) Index(r string) int {
	for i, v := range l {
		if v == r {
			return i
		}
	}
	return -1
}

func (l StringList) String() string {
	return strings.Join(l, ",")
}

func (l StringList) MarshalText() ([]byte, error) {
	return []byte(l.String()), nil
}

func (l *StringList) UnmarshalText(data []byte) error {
	*l = strings.Split(string(data), ",")
	return nil
}

func LimitStringEllipsis(s string, l int) string {
	c := utf8.RuneCountInString(s)
	if c <= l {
		return s
	}

	c = 0
	var b bytes.Buffer
	for _, runeVal := range s {
		b.WriteRune(runeVal)
		c += 1
		if c >= l-3 {
			break
		}
	}

	return b.String() + "..."
}

type U64String uint64

func (u U64String) String() string {
	return u.Hex()
}

func (u U64String) U64() uint64 {
	return uint64(u)
}

func (u U64String) Hex() string {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], uint64(u))
	return hex.EncodeToString(tmp[:])
}

func (u U64String) Base64() string {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], uint64(u))
	return base64.StdEncoding.EncodeToString(tmp[:])
}

func DecodeU64String(s string) (U64String, error) {
	if buf, err := base64.StdEncoding.DecodeString(s); err == nil && len(buf) == 8 {
		return U64String(binary.BigEndian.Uint64(buf)), nil
	}
	if buf, err := hex.DecodeString(s); err == nil && len(buf) == 8 {
		return U64String(binary.BigEndian.Uint64(buf)), nil
	}
	return 0, fmt.Errorf("Invalid u64 hex or base64 string")
}

func (u *U64String) UnmarshalText(data []byte) error {
	uu, err := DecodeU64String(string(data))
	*u = uu
	return err
}

func (u U64String) MarshalText() ([]byte, error) {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], uint64(u))
	return []byte(hex.EncodeToString(tmp[:])), nil
}
