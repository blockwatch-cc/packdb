// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package util

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"
)

type Duration time.Duration

func (d Duration) Duration() time.Duration {
	return time.Duration(d)
}

func ParseDuration(d string) (Duration, error) {
	d = strings.ToLower(d)
	multiplier := time.Second
	switch true {
	case strings.HasSuffix(d, "d"):
		multiplier = 24 * time.Hour
		d = d[:len(d)-1]
	case strings.HasSuffix(d, "w"):
		multiplier = 7 * 24 * time.Hour
		d = d[:len(d)-1]
	}
	// parse integer values as seconds
	if i, err := strconv.ParseInt(d, 10, 64); err == nil {
		return Duration(time.Duration(i) * multiplier), nil
	}
	// parse as duration string (note: no whitespace allowed)
	if i, err := time.ParseDuration(d); err == nil {
		return Duration(i), nil
	}
	// parse as duration string with whitespace removed
	d = strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}
		return r
	}, d)
	if i, err := time.ParseDuration(d); err == nil {
		return Duration(i), nil
	}
	return 0, fmt.Errorf("duration: parsing '%s': invalid syntax", d)
}

func (d Duration) MarshalText() ([]byte, error) {
	return []byte(time.Duration(d).String()), nil
}

func (d *Duration) UnmarshalText(data []byte) error {
	i, err := ParseDuration(string(data))
	if err != nil {
		return err
	}
	*d = i
	return nil
}

func (d *Duration) UnmarshalJSON(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	if data[0] == '"' {
		return d.UnmarshalText(bytes.Trim(data, "\""))
	}
	if i, err := strconv.ParseInt(string(data), 10, 64); err == nil {
		*d = Duration(time.Duration(i) * time.Second)
		return nil
	}
	return fmt.Errorf("duration: parsing '%s': invalid syntax", string(data))
}
