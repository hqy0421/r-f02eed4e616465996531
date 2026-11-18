// Copyright [2023] LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

//go:build !windows
// +build !windows

package goavro

import (
	"os"
	"syscall"
)

// flockExclusive acquires an advisory, non-blocking exclusive lock on the data
// file for the lifetime of the manager. A second manager on the same file fails
// immediately instead of blocking, which enforces the single-writer rule. The
// lock is released automatically when the handle is closed.
func flockExclusive(file *os.File) error {
	return syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}

// syncDir flushes a directory's metadata so that a marker rename in it is
// durable after a crash.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
