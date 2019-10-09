// Copyright (c) 2018-2019 KIDTSUNAMI
// Author: alex@kidtsunami.com

package store

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
	for _, drv := range drivers {
		if drv.UseLogger != nil {
			drv.UseLogger(logger)
		}
	}
}
