// Copyright (c) 2019 KIDTSUNAMI
// Author: alex@kidtsunami.com

// +build go1.7,amd64,!gccgo,!appengine

package vec

import "golang.org/x/sys/cpu"

func init() {
	useAVX2 = cpu.X86.HasAVX2
}
