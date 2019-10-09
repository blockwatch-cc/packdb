// Copyright (c) 2018 KIDTSUNAMI
// Author: alex@kidtsunami.com

package util

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type TimeFormat int

var oneDay = 24 * time.Hour

const (
	TimeFormatDefault TimeFormat = iota
	TimeFormatUnix
	TimeFormatUnixMicro
	TimeFormatUnixMilli
	TimeFormatUnixNano
	TimeFormatDate
)

var FormatMap = map[TimeFormat]string{
	TimeFormatDefault:   time.RFC3339,
	TimeFormatUnix:      "",
	TimeFormatUnixMicro: "",
	TimeFormatUnixMilli: "",
	TimeFormatUnixNano:  "",
	TimeFormatDate:      "2006-01-02",
}

func (f TimeFormat) IsUnix() bool {
	switch f {
	case TimeFormatUnix,
		TimeFormatUnixMicro,
		TimeFormatUnixMilli,
		TimeFormatUnixNano:
		return true
	}
	return false
}

type Time struct {
	tm     time.Time
	format TimeFormat
}

func NewTime(t time.Time) Time {
	return Time{tm: t}
}

func Now() Time {
	return NewTime(time.Now())
}

var TimeFormats []string = []string{
	time.RFC3339,
	"02.01.2006T15:04:05.999999999Z07:00",
	"02.01.2006T15:04:05Z07:00",
	"02.01.2006 15:04:05.999999999Z07:00",
	"02.01.2006 15:04:05Z07:00",
	"2006:01:02 15:04:05.999999999-07:00",
	"2006:01:02 15:04:05-07:00",
	"2006:01:02:15:04:05-07:00",
	"2006:01:02:15:04:05-07",
	"2006-01-02T15:04:05.999999999Z",
	"2006-01-02T15:04:05Z",
	"2006-01-02 15:04:05.999999999Z",
	"2006-01-02 15:04:05Z",
	"02.01.2006T15:04:05.999999999Z",
	"02.01.2006T15:04:05Z",
	"02.01.2006 15:04:05.999999999Z",
	"02.01.2006 15:04:05Z",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02 15:04:05",
	"02.01.2006T15:04:05.999999999",
	"02.01.2006T15:04:05",
	"02.01.2006 15:04:05.999999999",
	"02.01.2006 15:04:05",
	"2006-01-02T15:04",
	"2006-01-02 15:04",
	"02.01.2006T15:04",
	"02.01.2006 15:04",
	"2006-01-02",
	"02.01.2006",
	"2006-01",
	"01.2006",
	"15:04:05",
	"15:04",
	"2006",
}

var dateOnly = StringList{
	"2006-01-02",
	"02.01.2006",
	"2006-01",
	"01.2006",
	"2006",
}

func (f Time) Time() time.Time {
	return f.tm
}

func (t *Time) SetFormat(f TimeFormat) *Time {
	t.format = f
	return t
}

func NewTimeFrom(ts int64, res time.Duration) Time {
	return Time{tm: time.Unix(0, ts*int64(res))}
}

func ParseTime(value string) (Time, error) {
	// parse invalid zero values
	switch value {
	case "", "-":
		return Time{}, nil
	}
	// try parsing as int
	i, ierr := strconv.ParseInt(value, 10, 64)
	if ierr != nil {
		// when failed, try parsing as hex
		i, ierr = strconv.ParseInt(value, 16, 64)
	}
	switch {
	case ierr == nil && len(value) > 4:
		// 1st try parsing as unix timestamp
		// detect UNIX timestamp scale: we choose somewhat arbitrarity
		// Dec 31, 9999 23:59:59 as cut-off time here
		switch {
		case i < 253402300799:
			// timestamp is in seconds
			return Time{tm: time.Unix(i, 0).UTC(), format: TimeFormatUnix}, nil
		case i < 253402300799000:
			// timestamp is in milliseconds
			return Time{tm: time.Unix(0, i*1000000).UTC(), format: TimeFormatUnixMilli}, nil
		case i < 253402300799000000:
			// timestamp is in microseconds
			return Time{tm: time.Unix(0, i*1000).UTC(), format: TimeFormatUnixMicro}, nil
		default:
			// timestamp is in nanoseconds
			return Time{tm: time.Unix(0, i).UTC(), format: TimeFormatUnixNano}, nil
		}

	case strings.HasPrefix(value, "now"):
		now := time.Now().UTC()
		// check for truncation and modification operators
		if strings.Contains(value, "/") {
			fields := strings.Split(value, "/")
			if fields[0] != "now" {
				return Time{}, fmt.Errorf("time: parsing '%s': invalid truncation syntax, must be `now/arg`", value)
			}
			value = fields[1]
			// parse arg as duration modifier (strip optional modifier)
			switch strings.Split(value, "-")[0] {
			case "s":
				now = now.Truncate(time.Second)
			case "m":
				now = now.Truncate(time.Minute)
			case "h":
				now = now.Truncate(time.Hour)
			case "d":
				now = now.Truncate(24 * time.Hour)
			case "w":
				now = now.Truncate(7 * 24 * time.Hour)
			case "M":
				yy, mm, _ := now.Date()
				now = time.Date(yy, mm, 1, 0, 0, 0, 0, time.UTC)
			case "Q":
				yy, mm, _ := now.Date()
				now = time.Date(yy, mm-mm%3, 1, 0, 0, 0, 0, time.UTC)
			case "y":
				now = time.Date(now.Year(), 1, 1, 0, 0, 0, 0, time.UTC)
			default:
				return Time{}, fmt.Errorf("time: parsing '%s': invalid truncation argument", value)
			}
		}
		// continue handling minus operator
		if strings.Contains(value, "-") {
			fields := strings.Split(value, "-")
			d, derr := ParseDuration(fields[1])
			if derr != nil {
				return Time{}, fmt.Errorf("time: parsing '%s': %v", value, derr)
			}
			now = now.Add(-d.Duration())
		}
		return Time{tm: now}, nil
	case value == "today":
		return Time{tm: time.Now().UTC().Truncate(oneDay)}, nil
	case value == "yesterday":
		return Time{tm: time.Now().UTC().Truncate(oneDay).AddDate(0, 0, -1)}, nil
	case value == "tomorrow":
		return Time{tm: time.Now().UTC().Truncate(oneDay).Add(oneDay)}, nil

	default:
		// 3rd try the different time formats from most to least specific
		for _, f := range TimeFormats {
			if t, err := time.Parse(f, value); err == nil {
				// catch the time-only values by offsetting with today's UTC date
				if t.Year() == 0 {
					t = time.Now().UTC().Truncate(oneDay).Add(t.Sub(time.Time{}))
				}
				if dateOnly.Contains(f) {
					return Time{tm: t, format: TimeFormatDate}, nil
				}
				return Time{tm: t}, nil
			}
		}

		return Time{}, fmt.Errorf("time: parsing '%s': invalid syntax", value)
	}
}

func (f Time) String() string {
	switch f.format {
	case TimeFormatUnix:
		return strconv.FormatInt(f.Time().Unix(), 10)
	case TimeFormatUnixMilli:
		return strconv.FormatInt(f.Time().UnixNano()/1000, 10)
	case TimeFormatUnixMicro:
		return strconv.FormatInt(f.Time().UnixNano()/1000000, 10)
	case TimeFormatUnixNano:
		return strconv.FormatInt(f.Time().UnixNano(), 10)
	default:
		fs, ok := FormatMap[f.format]
		if !ok {
			fs = FormatMap[TimeFormatDefault]
		}
		return f.Time().Format(fs)
	}
}

func (t Time) IsZero() bool {
	return t.Time().IsZero()
}

func (t Time) Before(a Time) bool {
	return t.Time().Before(a.Time())
}

func (t Time) After(a Time) bool {
	return t.Time().After(a.Time())
}

func (t Time) Unix() int64 {
	return t.Time().Unix()
}

func (t Time) Truncate(d time.Duration) Time {
	return Time{
		tm:     t.Time().Truncate(d),
		format: t.format,
	}
}

func (t Time) Add(d time.Duration) Time {
	return Time{
		tm:     t.Time().Add(d),
		format: t.format,
	}
}

func (t Time) Equal(t2 Time) bool {
	return t.Time().Equal(t2.Time())
}

func UnixNonZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.Unix()
}

func UnixMilli(t time.Time) int64 {
	return t.UnixNano() / 1000000
}

func UnixMilliNonZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano() / 1000000
}

func UnixMicro(t time.Time) int64 {
	return t.UnixNano() / 1000
}

func UnixMicroNonZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano() / 1000
}

func UnixNano(t time.Time) int64 {
	return t.UnixNano()
}

func UnixNanoNonZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}
