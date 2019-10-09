// Copyright (c) 2018-2019 KIDTSUNAMI
// Author: alex@kidtsunami.com
//
package pack

import (
	logpkg "github.com/echa/log"
)

var log logpkg.Logger

func init() {
	DisableLog()
}

func DisableLog() {
	log = logpkg.Disabled
}

func UseLogger(logger logpkg.Logger) {
	log = logger
}

type logClosure func() string

func (c logClosure) String() string {
	return c()
}

func newLogClosure(c func() string) logClosure {
	return logClosure(c)
}
