// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//
package pack

import (
	"encoding"
	"fmt"
	"reflect"
	"time"
)

func (t FieldType) CastType(val interface{}, f Field) (interface{}, error) {
	var ok bool
	res := val
	switch t {
	case FieldTypeBytes:
		if vv, ok2 := val.(encoding.BinaryMarshaler); ok2 {
			r, err := vv.MarshalBinary()
			if err != nil {
				return nil, err
			}
			res = r
			ok = true
		} else {
			_, ok = val.([]byte)
		}
	case FieldTypeString:
		if vv, ok2 := val.(encoding.TextMarshaler); ok2 {
			r, err := vv.MarshalText()
			if err != nil {
				return nil, err
			}
			res = r
			ok = true
		} else {
			_, ok = val.(string)
		}
	case FieldTypeDatetime:
		_, ok = val.(time.Time)
	case FieldTypeBoolean:
		_, ok = val.(bool)
	case FieldTypeInt64:
		switch v := val.(type) {
		case int:
			res, ok = int64(v), true
		case int64:
			res, ok = int64(v), true
		case int32:
			res, ok = int64(v), true
		case int16:
			res, ok = int64(v), true
		case int8:
			res, ok = int64(v), true
		default:
			// type aliases
			vv := reflect.Indirect(reflect.ValueOf(val))
			switch vv.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				res, ok = int64(vv.Int()), true
			}
		}
	case FieldTypeUint64:
		switch v := val.(type) {
		case int:
			res, ok = uint64(v), true
		case uint:
			res, ok = uint64(v), true
		case uint64:
			res, ok = uint64(v), true
		case uint32:
			res, ok = uint64(v), true
		case uint16:
			res, ok = uint64(v), true
		case uint8:
			res, ok = uint64(v), true
		default:
			// type aliases
			vv := reflect.Indirect(reflect.ValueOf(val))
			switch vv.Kind() {
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				res, ok = uint64(vv.Uint()), true
			}
		}
	case FieldTypeFloat64:
		switch v := val.(type) {
		case int:
			res, ok = float64(v), true
		case float64:
			res, ok = float64(v), true
		case float32:
			res, ok = float64(v), true
		}
	}
	if !ok {
		return res, fmt.Errorf("pack: unexpected value type %T for %s condition", val, t)
	}
	return res, nil
}

func (t FieldType) CastSliceType(val interface{}, f Field) (interface{}, error) {
	var (
		ok  bool
		err error
	)
	res := val
	switch t {
	case FieldTypeBytes:
		_, ok = val.([][]byte)
		_, ok = val.([][]byte)
		if !ok {
			vv, ok2 := val.([]encoding.BinaryMarshaler)
			if ok2 {
				slice := make([][]byte, len(vv))
				for i := range vv {
					slice[i], err = vv[i].(encoding.BinaryMarshaler).MarshalBinary()
					if err != nil {
						return nil, err
					}
				}
				res = slice
				ok = true
			}
		}
	case FieldTypeString:
		_, ok = val.([]string)
		if !ok {
			vv, ok2 := val.([]encoding.TextMarshaler)
			if ok2 {
				slice := make([]string, len(vv))
				for i := range vv {
					buf, err := vv[i].(encoding.TextMarshaler).MarshalText()
					slice[i] = string(buf)
					if err != nil {
						return nil, err
					}
				}
				res = slice
				ok = true
			}
		}
	case FieldTypeDatetime:
		_, ok = val.([]time.Time)
	case FieldTypeBoolean:
		_, ok = val.([]bool)
	case FieldTypeInt64:
		switch v := val.(type) {
		case []int:
			cp := make([]int64, len(v))
			for i := range v {
				cp[i] = int64(v[i])
			}
			res, ok = cp, true
		case []int64:
			res, ok = val, true
		case []int32:
			cp := make([]int64, len(v))
			for i := range v {
				cp[i] = int64(v[i])
			}
			res, ok = cp, true
		case []int16:
			cp := make([]int64, len(v))
			for i := range v {
				cp[i] = int64(v[i])
			}
			res, ok = cp, true
		case []int8:
			cp := make([]int64, len(v))
			for i := range v {
				cp[i] = int64(v[i])
			}
			res, ok = cp, true
		default:
			// convert enum types
			vv := reflect.Indirect(reflect.ValueOf(val))
			if vv.Kind() == reflect.Slice {
				switch vv.Type().Elem().Kind() {
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					cp := make([]int64, vv.Len())
					for i, l := 0, vv.Len(); i < l; i++ {
						cp[i] = int64(vv.Index(i).Int())
					}
					res, ok = cp, true
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					cp := make([]int64, vv.Len())
					for i, l := 0, vv.Len(); i < l; i++ {
						cp[i] = int64(vv.Index(i).Int())
					}
					res, ok = cp, true
				}
			}
		}
	case FieldTypeUint64:
		switch v := val.(type) {
		case []uint:
			cp := make([]uint64, len(v))
			for i := range v {
				cp[i] = uint64(v[i])
			}
			res, ok = cp, true
		case []uint64:
			res, ok = val, true
		case []uint32:
			cp := make([]uint64, len(v))
			for i := range v {
				cp[i] = uint64(v[i])
			}
			res, ok = cp, true
		case []uint16:
			cp := make([]uint64, len(v))
			for i := range v {
				cp[i] = uint64(v[i])
			}
			res, ok = cp, true
		case []uint8:
			cp := make([]uint64, len(v))
			for i := range v {
				cp[i] = uint64(v[i])
			}
			res, ok = cp, true
		default:
			// convert enum types
			vv := reflect.Indirect(reflect.ValueOf(val))
			if vv.Kind() == reflect.Slice {
				switch vv.Type().Elem().Kind() {
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					cp := make([]uint64, vv.Len())
					for i, l := 0, vv.Len(); i < l; i++ {
						cp[i] = uint64(vv.Index(i).Uint())
					}
					res, ok = cp, true
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					cp := make([]uint64, vv.Len())
					for i, l := 0, vv.Len(); i < l; i++ {
						cp[i] = uint64(vv.Index(i).Uint())
					}
					res, ok = cp, true
				}
			}
		}
	case FieldTypeFloat64:
		_, ok = val.([]float64)
	}
	if !ok {
		return res, fmt.Errorf("pack: unexpected value type %T for %s slice condition", val, t)
	}
	return res, nil
}

func (t FieldType) CheckType(val interface{}) error {
	var ok bool
	switch t {
	case FieldTypeBytes:
		_, ok = val.([]byte)
	case FieldTypeString:
		_, ok = val.(string)
	case FieldTypeDatetime:
		_, ok = val.(time.Time)
	case FieldTypeBoolean:
		_, ok = val.(bool)
	case FieldTypeInt64:
		_, ok = val.(int64)
	case FieldTypeUint64:
		_, ok = val.(uint64)
	case FieldTypeFloat64:
		_, ok = val.(float64)
	}
	if !ok {
		return fmt.Errorf("unexpected value type %T for %s condition", val, t)
	}
	return nil
}

func (t FieldType) CheckSliceType(val interface{}) error {
	var ok bool
	switch t {
	case FieldTypeBytes:
		_, ok = val.([][]byte)
	case FieldTypeString:
		_, ok = val.([]string)
	case FieldTypeDatetime:
		_, ok = val.([]time.Time)
	case FieldTypeBoolean:
		_, ok = val.([]bool)
	case FieldTypeInt64:
		_, ok = val.([]int64)
	case FieldTypeUint64:
		_, ok = val.([]uint64)
	case FieldTypeFloat64:
		_, ok = val.([]float64)
	}
	if !ok {
		return fmt.Errorf("unexpected value type %T for %s slice condition", val, t)
	}
	return nil
}

func (t FieldType) CopySliceType(val interface{}) (interface{}, error) {
	switch t {
	case FieldTypeBytes:
		if slice, ok := val.([][]byte); ok {
			cp := make([][]byte, len(slice))
			for i, v := range slice {
				buf := make([]byte, len(v))
				copy(buf, v)
				cp[i] = buf
			}
			return cp, nil
		}
	case FieldTypeString:
		if slice, ok := val.([]string); ok {
			cp := make([]string, len(slice))
			copy(cp, slice)
			return cp, nil
		}
	case FieldTypeDatetime:
		if slice, ok := val.([]time.Time); ok {
			cp := make([]time.Time, len(slice))
			copy(cp, slice)
			return cp, nil
		}
	case FieldTypeBoolean:
		if slice, ok := val.([]time.Time); ok {
			cp := make([]time.Time, len(slice))
			copy(cp, slice)
			return cp, nil
		}
	case FieldTypeInt64:
		if slice, ok := val.([]int64); ok {
			cp := make([]int64, len(slice))
			copy(cp, slice)
			return cp, nil
		}
	case FieldTypeUint64:
		if slice, ok := val.([]uint64); ok {
			cp := make([]uint64, len(slice))
			copy(cp, slice)
			return cp, nil
		}
	case FieldTypeFloat64:
		if slice, ok := val.([]float64); ok {
			cp := make([]float64, len(slice))
			copy(cp, slice)
			return cp, nil
		}
	default:
		return nil, fmt.Errorf("slice copy: unsupported field type %s", t)
	}
	return nil, fmt.Errorf("slice copy: mismatched value type %T for %s field", val, t)
}
