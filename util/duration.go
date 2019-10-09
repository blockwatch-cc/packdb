// Copyright (c) 2018 KIDTSUNAMI
// Author: alex@kidtsunami.com

package util

import (
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
