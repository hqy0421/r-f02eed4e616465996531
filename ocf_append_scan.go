// Copyright [2023] LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package goavro

import (
	"bytes"
	"fmt"
	"io"
	"os"
)

// scanOCFBlocks performs a frame-level validation of every OCF block in the
// region [start, limit) of file without decoding datum values: it reads each
// block count, block size, skips the (possibly compressed) block payload and
// verifies the 16-byte sync marker. It returns the absolute offset at which
// the next block would begin, which equals limit only when the region ends on
// a clean block boundary. A short final region simply yields the offset of the
// partial block, letting callers distinguish a clean prefix from a torn tail.
func scanOCFBlocks(header *ocfHeader, file *os.File, start, limit int64) (int64, error) {
	if start > limit {
		return 0, fmt.Errorf("scan start %d is beyond limit %d", start, limit)
	}
	reader := io.NewSectionReader(file, start, limit-start)
	sync := make([]byte, ocfSyncLength)
	pos := start

	for pos < limit {
		blockCount, err := longBinaryReader(reader)
		if err != nil {
			if err == io.EOF {
				return pos, nil // partial header of an unconfirmed block
			}
			return pos, fmt.Errorf("cannot read block count at offset %d: %s", pos, err)
		}
		if blockCount <= 0 {
			return pos, fmt.Errorf("block count is not greater than 0: %d", blockCount)
		}
		if blockCount > MaxBlockCount {
			return pos, fmt.Errorf("block count exceeds MaxBlockCount: %d > %d", blockCount, MaxBlockCount)
		}

		blockSize, err := longBinaryReader(reader)
		if err != nil {
			return pos, fmt.Errorf("cannot read block size: %s", err)
		}
		if blockSize <= 0 {
			return pos, fmt.Errorf("block size is not greater than 0: %d", blockSize)
		}
		if blockSize > MaxBlockSize {
			return pos, fmt.Errorf("block size exceeds MaxBlockSize: %d > %d", blockSize, MaxBlockSize)
		}

		// A full block plus its sync marker must fit inside the region;
		// io.CopyN and io.ReadFull fail cleanly on a truncated tail.
		if _, err = io.CopyN(io.Discard, reader, blockSize); err != nil {
			return pos, fmt.Errorf("cannot skip block payload of %d bytes: %s", blockSize, err)
		}

		var n int
		if n, err = io.ReadFull(reader, sync); err != nil {
			return pos, fmt.Errorf("cannot read sync marker: read %d out of %d bytes: %s", n, ocfSyncLength, err)
		}
		if !bytes.Equal(sync, header.syncMarker[:]) {
			return pos, fmt.Errorf("sync marker mismatch: %v != %v", sync, header.syncMarker)
		}

		// Derive the absolute end offset from the section reader position
		// rather than re-encoding the varint longs ourselves.
		relative, err := reader.Seek(0, io.SeekCurrent)
		if err != nil {
			return pos, fmt.Errorf("cannot determine scan position: %s", err)
		}
		pos = start + relative
	}
	return pos, nil
}
