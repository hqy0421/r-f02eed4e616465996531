// Copyright [2023] LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

//go:build windows
// +build windows

package goavro

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

var (
	kernel32       = syscall.NewLazyDLL("kernel32.dll")
	procLockFileEx = kernel32.NewProc("LockFileEx")
)

const (
	lockfileExclusiveLock   = 0x00000002
	lockfileFailImmediately = 0x00000001
)

// flockExclusive acquires a non-blocking exclusive byte-range lock over the
// whole file. A second writer on the same file fails immediately, enforcing
// the single-writer rule. The lock is released when the handle is closed.
func flockExclusive(file *os.File) error {
	var ol syscall.Overlapped
	r1, _, errno := procLockFileEx.Call(
		file.Fd(),
		uintptr(lockfileExclusiveLock|lockfileFailImmediately),
		0,
		^uintptr(0),
		^uintptr(0),
		uintptr(unsafe.Pointer(&ol)),
	)
	if r1 == 0 {
		return fmt.Errorf("LockFileEx failed: %w", errno)
	}
	return nil
}

// syncDir is a no-op on Windows, where directory handles cannot be opened for
// metadata fsync and rename durability is provided by the operating system.
func syncDir(dir string) error {
	return nil
}
