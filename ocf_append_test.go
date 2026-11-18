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
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"

	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

const testAppendSchema = `{"type":"long"}`

// newTestManager creates a manager on a fresh path and returns it together
// with the data and marker paths. The caller must Close the manager.
func newTestManager(t *testing.T, compression string) (*OCFAppendManager, string, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "data.avro")
	mgr, err := NewOCFAppendManager(OCFAppendConfig{
		Path:            path,
		Schema:          testAppendSchema,
		CompressionName: compression,
	})
	if err != nil {
		t.Fatalf("NewOCFAppendManager: %s", err)
	}
	return mgr, path, path + ".ocfmarker"
}

func longs(n ...int64) []interface{} {
	out := make([]interface{}, len(n))
	for i, v := range n {
		out[i] = v
	}
	return out
}

// readAllManaged drains a ManagedOCFReader into a slice of int64 values.
func readAllManaged(t *testing.T, reader *ManagedOCFReader) []int64 {
	t.Helper()
	var got []int64
	for reader.Scan() {
		datum, err := reader.Read()
		if err != nil {
			t.Fatalf("Read: %s", err)
		}
		v, ok := datum.(int64)
		if !ok {
			t.Fatalf("expected int64 datum, got %T: %v", datum, datum)
		}
		got = append(got, v)
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("reader error: %s", err)
	}
	return got
}

func TestOCFAppendCreateAndAppend(t *testing.T) {
	mgr, path, markerPath := newTestManager(t, CompressionNullLabel)
	defer func() {
		if err := mgr.Close(); err != nil {
			t.Fatalf("Close: %s", err)
		}
	}()

	if mgr.Schema() != testAppendSchema {
		t.Fatalf("schema = %q, want %q", mgr.Schema(), testAppendSchema)
	}
	if mgr.CompressionName() != CompressionNullLabel {
		t.Fatalf("compression = %q", mgr.CompressionName())
	}
	if mgr.Boundary() <= 0 {
		t.Fatalf("initial boundary should be header size, got %d", mgr.Boundary())
	}
	if _, err := os.Stat(markerPath); err != nil {
		t.Fatalf("marker not published at creation: %s", err)
	}

	reader, err := mgr.NewReader()
	if err != nil {
		t.Fatalf("NewReader: %s", err)
	}
	defer reader.Close()
	if got := readAllManaged(t, reader); len(got) != 0 {
		t.Fatalf("fresh reader saw %d items, want 0", len(got))
	}

	receipt1, err := mgr.AppendBatch("batch-1", longs(1, 2, 3), []byte(`{"offset":3}`))
	if err != nil {
		t.Fatalf("AppendBatch 1: %s", err)
	}
	if receipt1.BatchID != "batch-1" || receipt1.ItemCount != 3 || receipt1.AppendedBytes <= 0 {
		t.Fatalf("unexpected receipt: %+v", receipt1)
	}
	if receipt1.Boundary != mgr.Boundary() {
		t.Fatalf("receipt boundary %d != manager boundary %d", receipt1.Boundary, mgr.Boundary())
	}
	if string(receipt1.Checkpoint) != `{"offset":3}` {
		t.Fatalf("checkpoint not preserved: %s", receipt1.Checkpoint)
	}

	// The pre-existing reader handle must keep working and see only the newly
	// committed prefix through the same handle.
	if got := readAllManaged(t, reader); fmt.Sprint(got) != "[1 2 3]" {
		t.Fatalf("reader after batch 1 = %v", got)
	}

	receipt2, err := mgr.AppendBatch("batch-2", longs(4, 5), []byte(`{"offset":5}`))
	if err != nil {
		t.Fatalf("AppendBatch 2: %s", err)
	}
	if receipt2.Boundary <= receipt1.Boundary {
		t.Fatalf("boundary did not advance: %d -> %d", receipt1.Boundary, receipt2.Boundary)
	}
	if got := readAllManaged(t, reader); fmt.Sprint(got) != "[4 5]" {
		t.Fatalf("reader after batch 2 = %v", got)
	}

	// A brand new reader sees the whole committed prefix.
	full, err := mgr.NewReader()
	if err != nil {
		t.Fatalf("NewReader: %s", err)
	}
	defer full.Close()
	if got := readAllManaged(t, full); fmt.Sprint(got) != "[1 2 3 4 5]" {
		t.Fatalf("full read = %v, want [1 2 3 4 5]", got)
	}

	// The data file is a standard OCF readable by the legacy OCFReader.
	legacy, err := NewOCFReader(strings.NewReader(string(readFile(t, path))))
	if err != nil {
		t.Fatalf("legacy NewOCFReader: %s", err)
	}
	var legacyValues []int64
	for legacy.Scan() {
		datum, err := legacy.Read()
		if err != nil {
			t.Fatalf("legacy Read: %s", err)
		}
		legacyValues = append(legacyValues, datum.(int64))
	}
	if err = legacy.Err(); err != nil {
		t.Fatalf("legacy reader error: %s", err)
	}
	if fmt.Sprint(legacyValues) != "[1 2 3 4 5]" {
		t.Fatalf("legacy read = %v", legacyValues)
	}
}

func TestOCFAppendAllCompressions(t *testing.T) {
	for _, compression := range []string{
		CompressionNullLabel,
		CompressionDeflateLabel,
		CompressionSnappyLabel,
	} {
		t.Run(compression, func(t *testing.T) {
			mgr, _, _ := newTestManager(t, compression)
			if mgr.CompressionName() != compression {
				t.Fatalf("compression = %q, want %q", mgr.CompressionName(), compression)
			}
			if _, err := mgr.AppendBatch("b1", longs(10, 20, 30), nil); err != nil {
				t.Fatalf("AppendBatch: %s", err)
			}
			reader, err := mgr.NewReader()
			if err != nil {
				t.Fatalf("NewReader: %s", err)
			}
			defer reader.Close()
			if got := readAllManaged(t, reader); fmt.Sprint(got) != "[10 20 30]" {
				t.Fatalf("read = %v", got)
			}
			if err = mgr.Close(); err != nil {
				t.Fatalf("Close: %s", err)
			}

			// Reopen and verify the actual header compression survived.
			reopened, err := NewOCFAppendManager(OCFAppendConfig{Path: mgr.config.Path})
			if err != nil {
				t.Fatalf("reopen: %s", err)
			}
			defer reopened.Close()
			if reopened.CompressionName() != compression {
				t.Fatalf("reopened compression = %q, want %q", reopened.CompressionName(), compression)
			}
			if _, err = reopened.AppendBatch("b2", longs(40), nil); err != nil {
				t.Fatalf("append after reopen: %s", err)
			}
			reader2, err := reopened.NewReader()
			if err != nil {
				t.Fatalf("NewReader: %s", err)
			}
			defer reader2.Close()
			if got := readAllManaged(t, reader2); fmt.Sprint(got) != "[10 20 30 40]" {
				t.Fatalf("read after reopen = %v", got)
			}
		})
	}
}

func TestOCFAppendInodeStable(t *testing.T) {
	mgr, path, _ := newTestManager(t, CompressionNullLabel)
	defer mgr.Close()

	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err = mgr.AppendBatch(fmt.Sprintf("b%d", i), longs(int64(i)), nil); err != nil {
			t.Fatalf("AppendBatch: %s", err)
		}
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) {
		t.Fatal("data file inode changed: OCF must be appended in place")
	}
}

func TestOCFAppendCrashTruncatesUnconfirmedTail(t *testing.T) {
	mgr, path, markerPath := newTestManager(t, CompressionNullLabel)

	r1, err := mgr.AppendBatch("committed-1", longs(1, 2), []byte("cp1"))
	if err != nil {
		t.Fatalf("AppendBatch: %s", err)
	}
	if err = mgr.Close(); err != nil {
		t.Fatalf("Close: %s", err)
	}

	// Simulate a crash after data bytes were appended but before the marker
	// could move the boundary: splice a raw, marker-less junk tail onto the
	// file. The persisted marker still points at the committed boundary.
	junk := make([]byte, 37)
	for i := range junk {
		junk[i] = byte(i + 1)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.Write(junk); err != nil {
		t.Fatal(err)
	}
	if err = f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	if got := fileSize(t, path); got != r1.Boundary+int64(len(junk)) {
		t.Fatalf("size before reopen = %d, want %d", got, r1.Boundary+int64(len(junk)))
	}

	reopened, err := NewOCFAppendManager(OCFAppendConfig{Path: path, MarkerPath: markerPath})
	if err != nil {
		t.Fatalf("reopen with unconfirmed tail: %s", err)
	}
	defer reopened.Close()

	if reopened.Boundary() != r1.Boundary {
		t.Fatalf("boundary after recovery = %d, want %d", reopened.Boundary(), r1.Boundary)
	}
	if got := fileSize(t, path); got != r1.Boundary {
		t.Fatalf("unconfirmed tail not truncated: size = %d, want %d", got, r1.Boundary)
	}
	reader, err := reopened.NewReader()
	if err != nil {
		t.Fatalf("NewReader: %s", err)
	}
	defer reader.Close()
	if got := readAllManaged(t, reader); fmt.Sprint(got) != "[1 2]" {
		t.Fatalf("read after recovery = %v, want [1 2]", got)
	}

	// Normal appending resumes from the recovered boundary.
	r2, err := reopened.AppendBatch("committed-2", longs(3), []byte("cp2"))
	if err != nil {
		t.Fatalf("AppendBatch after recovery: %s", err)
	}
	if r2.Boundary != r1.Boundary+r2.AppendedBytes {
		t.Fatalf("boundary math wrong: %d + %d != %d", r1.Boundary, r2.AppendedBytes, r2.Boundary)
	}
	if cp, ok := reopened.LastCheckpoint(); !ok || string(cp) != "cp2" {
		t.Fatalf("last checkpoint = %q (%v)", cp, ok)
	}
}

func TestOCFAppendReplayIsIdempotent(t *testing.T) {
	mgr, path, markerPath := newTestManager(t, CompressionDeflateLabel)
	r1, err := mgr.AppendBatch("dup-batch", longs(7, 8), []byte("cp"))
	if err != nil {
		t.Fatal(err)
	}
	// Same process replay.
	r1again, err := mgr.AppendBatch("dup-batch", longs(999), []byte("different"))
	if err != nil {
		t.Fatalf("replay returned error: %s", err)
	}
	if !receiptsEqual(r1again, r1) {
		t.Fatalf("replay receipt = %+v, want original %+v", r1again, r1)
	}
	if err = mgr.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen (crash simulation) and replay the same batch id.
	reopened, err := NewOCFAppendManager(OCFAppendConfig{Path: path, MarkerPath: markerPath})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	r2, err := reopened.AppendBatch("dup-batch", longs(999), []byte("different"))
	if err != nil {
		t.Fatalf("replay after reopen: %s", err)
	}
	if r2.Boundary != r1.Boundary || r2.ItemCount != r1.ItemCount || r2.CRC32 != r1.CRC32 {
		t.Fatalf("reopened replay receipt = %+v, want %+v", r2, r1)
	}
	if string(r2.Checkpoint) != "cp" {
		t.Fatalf("replay checkpoint = %q, want cp", r2.Checkpoint)
	}
	reader, err := reopened.NewReader()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if got := readAllManaged(t, reader); fmt.Sprint(got) != "[7 8]" {
		t.Fatalf("data after replay = %v, want [7 8]", got)
	}
}

func TestOCFAppendRollbackOnEncodingFailure(t *testing.T) {
	mgr, path, _ := newTestManager(t, CompressionNullLabel)
	r1, err := mgr.AppendBatch("good", longs(1), nil)
	if err != nil {
		t.Fatal(err)
	}
	boundaryBefore := mgr.Boundary()

	// A string datum cannot be encoded by a long schema. The failure happens
	// during native append, before the durability gate and marker publish.
	_, err = mgr.AppendBatch("bad", []interface{}{int64(2), "not-a-long"}, nil)
	if err == nil {
		t.Fatal("expected encoding failure")
	}
	if mgr.Boundary() != boundaryBefore {
		t.Fatalf("boundary moved after failed batch: %d -> %d", boundaryBefore, mgr.Boundary())
	}
	if got := fileSize(t, path); got != r1.Boundary {
		t.Fatalf("file size after rollback = %d, want %d", got, r1.Boundary)
	}

	// The manager remains usable and the next good batch starts exactly at the
	// old boundary.
	r2, err := mgr.AppendBatch("good-2", longs(2, 3), nil)
	if err != nil {
		t.Fatalf("append after rollback: %s", err)
	}
	if r2.Boundary != r1.Boundary+r2.AppendedBytes {
		t.Fatalf("batch did not start at old boundary: %d -> %d", r1.Boundary, r2.Boundary)
	}
	reader, err := mgr.NewReader()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if got := readAllManaged(t, reader); fmt.Sprint(got) != "[1 2 3]" {
		t.Fatalf("data after rollback = %v, want [1 2 3]", got)
	}
	if err = mgr.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen must not find any unconfirmed tail either.
	reopened, err := NewOCFAppendManager(OCFAppendConfig{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.Boundary() != r2.Boundary {
		t.Fatalf("reopened boundary = %d, want %d", reopened.Boundary(), r2.Boundary)
	}
}

func TestOCFAppendReaderNeverSeesUncommittedTail(t *testing.T) {
	mgr, path, _ := newTestManager(t, CompressionNullLabel)
	defer mgr.Close()
	if _, err := mgr.AppendBatch("b1", longs(1), nil); err != nil {
		t.Fatal(err)
	}

	reader, err := mgr.NewReader()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if got := readAllManaged(t, reader); fmt.Sprint(got) != "[1]" {
		t.Fatalf("initial read = %v", got)
	}

	// Simulate an in-flight append: block bytes already landed in the file,
	// but the marker boundary has not advanced. The reader must stay bounded
	// to the committed prefix and report clean EOF, not parse the junk.
	junk := make([]byte, 29)
	for i := range junk {
		junk[i] = byte(i + 1)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.Write(junk); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}

	if reader.Scan() {
		t.Fatal("reader observed bytes beyond the committed boundary")
	}
	if err = reader.Err(); err != nil {
		t.Fatalf("reader error at boundary: %s", err)
	}
	if reader.Boundary() != mgr.Boundary() {
		t.Fatalf("reader boundary %d != manager boundary %d", reader.Boundary(), mgr.Boundary())
	}

	// The real append trims the stale tail at the old boundary before writing
	// its block, after which the same handle observes the committed block.
	if _, err = mgr.AppendBatch("b2", longs(2, 3), nil); err != nil {
		t.Fatalf("AppendBatch: %s", err)
	}
	if got := readAllManaged(t, reader); fmt.Sprint(got) != "[2 3]" {
		t.Fatalf("reader after commit = %v, want [2 3]", got)
	}
}

func TestOCFAppendConfigMismatchRejected(t *testing.T) {
	mgr, path, markerPath := newTestManager(t, CompressionDeflateLabel)
	if _, err := mgr.AppendBatch("b", longs(1), nil); err != nil {
		t.Fatal(err)
	}
	if err := mgr.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := NewOCFAppendManager(OCFAppendConfig{
		Path:   path,
		Schema: `{"type":"string"}`,
	}); err == nil || !strings.Contains(err.Error(), "schema") {
		t.Fatalf("schema mismatch error expected, got: %v", err)
	}
	if _, err := NewOCFAppendManager(OCFAppendConfig{
		Path:            path,
		CompressionName: CompressionSnappyLabel,
	}); err == nil || !strings.Contains(err.Error(), "compression") {
		t.Fatalf("compression mismatch error expected, got: %v", err)
	}

	// Corrupt the marker's recorded schema so it disagrees with the actual
	// header: the header wins and the marker is rejected.
	raw, err := os.ReadFile(markerPath)
	if err != nil {
		t.Fatal(err)
	}
	var marker ocfAppendMarker
	if err = json.Unmarshal(raw, &marker); err != nil {
		t.Fatal(err)
	}
	marker.Schema = `{"type":"string"}`
	marker.Checksum = 0
	reencoded, err := json.Marshal(marker)
	if err != nil {
		t.Fatal(err)
	}
	marker.Checksum = crc32.ChecksumIEEE(reencoded)
	if err = os.WriteFile(markerPath, mustMarshalMarker(t, marker), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = NewOCFAppendManager(OCFAppendConfig{Path: path, MarkerPath: markerPath}); err == nil ||
		!strings.Contains(err.Error(), "marker schema") {
		t.Fatalf("marker/header schema disagreement error expected, got: %v", err)
	}
}

func TestOCFAppendCorruptMarkerRejected(t *testing.T) {
	mgr, path, markerPath := newTestManager(t, CompressionNullLabel)
	if _, err := mgr.AppendBatch("b", longs(1), nil); err != nil {
		t.Fatal(err)
	}
	if err := mgr.Close(); err != nil {
		t.Fatal(err)
	}

	// Parseable JSON whose payload no longer matches its checksum must be
	// rejected instead of trusted.
	raw, err := os.ReadFile(markerPath)
	if err != nil {
		t.Fatal(err)
	}
	var marker ocfAppendMarker
	if err = json.Unmarshal(raw, &marker); err != nil {
		t.Fatal(err)
	}
	marker.LastBatchBytes++ // alter payload, deliberately keep the old checksum
	altered, err := json.Marshal(marker)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(markerPath, altered, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = NewOCFAppendManager(OCFAppendConfig{Path: path, MarkerPath: markerPath}); err == nil ||
		!strings.Contains(err.Error(), "checksum") {
		t.Fatalf("checksum error expected, got: %v", err)
	}

	// A marker that is not even valid JSON is rejected just as firmly.
	if err = os.WriteFile(markerPath, []byte("{this is not json"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = NewOCFAppendManager(OCFAppendConfig{Path: path, MarkerPath: markerPath}); err == nil {
		t.Fatal("expected error for unparseable marker")
	}
}

func TestOCFAppendPreCommitFailureRollsBack(t *testing.T) {
	mgr, path, _ := newTestManager(t, CompressionSnappyLabel)
	r1, err := mgr.AppendBatch("good", longs(1, 2), nil)
	if err != nil {
		t.Fatal(err)
	}
	oldBoundary := mgr.Boundary()

	// Fail the marker publication before the atomic rename: no commit may be
	// observable, and the data file must be trimmed back to the old boundary.
	mgr.hooks.beforeMarkerRename = func() error {
		return errors.New("simulated marker publish failure")
	}
	_, err = mgr.AppendBatch("doomed", longs(3), nil)
	if err == nil {
		t.Fatal("expected marker publish failure")
	}
	var commitErr *CommitError
	if errors.As(err, &commitErr) {
		t.Fatalf("pre-rename failure must not be reported as committed: %v", err)
	}
	if mgr.Boundary() != oldBoundary {
		t.Fatalf("boundary moved after pre-commit failure: %d -> %d", oldBoundary, mgr.Boundary())
	}
	if got := fileSize(t, path); got != oldBoundary {
		t.Fatalf("data file not rolled back: size = %d, want %d", got, oldBoundary)
	}
	if _, ok := mgr.LastReceipt(); !ok {
		t.Fatal("previous receipt lost after failed batch")
	}

	// Recovery agrees: reopen finds no unconfirmed tail and the boundary is
	// unchanged.
	if err = mgr.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewOCFAppendManager(OCFAppendConfig{Path: path})
	if err != nil {
		t.Fatalf("reopen: %s", err)
	}
	defer reopened.Close()
	if reopened.Boundary() != oldBoundary {
		t.Fatalf("reopened boundary = %d, want %d", reopened.Boundary(), oldBoundary)
	}

	// The failed batch id was never committed, so appending it now succeeds
	// instead of being treated as a replay.
	r2, err := reopened.AppendBatch("doomed", longs(3), nil)
	if err != nil {
		t.Fatalf("append of previously failed batch: %s", err)
	}
	if r2.Boundary == r1.Boundary {
		t.Fatal("retry of failed batch did not advance the boundary")
	}
	reader, err := reopened.NewReader()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if got := readAllManaged(t, reader); fmt.Sprint(got) != "[1 2 3]" {
		t.Fatalf("read after recovery = %v, want [1 2 3]", got)
	}
}

func TestOCFAppendSingleWriterLock(t *testing.T) {
	mgr, path, _ := newTestManager(t, CompressionNullLabel)
	defer mgr.Close()

	second, err := NewOCFAppendManager(OCFAppendConfig{Path: path})
	if err == nil {
		_ = second.Close()
		t.Fatal("second concurrent writer was allowed")
	}
}

func TestOCFAppendLegacyFileWithoutMarker(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "legacy.avro")

	// Build a standard OCF with the legacy writer.
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		t.Fatal(err)
	}
	ocfw, err := NewOCFWriter(OCFConfig{W: f, Schema: testAppendSchema})
	if err != nil {
		t.Fatal(err)
	}
	if err = ocfw.Append(longs(1, 2)); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	legacySize := fileSize(t, path)

	// First manager adoption: full intact file becomes the committed prefix
	// and a marker is published.
	mgr, err := NewOCFAppendManager(OCFAppendConfig{Path: path})
	if err != nil {
		t.Fatalf("adopt legacy OCF: %s", err)
	}
	if mgr.Boundary() != legacySize {
		t.Fatalf("adopted boundary = %d, want file size %d", mgr.Boundary(), legacySize)
	}
	if mgr.Schema() != testAppendSchema {
		t.Fatalf("adopted schema = %q", mgr.Schema())
	}
	if _, err = mgr.AppendBatch("new-batch", longs(3), nil); err != nil {
		t.Fatalf("append to adopted file: %s", err)
	}
	reader, err := mgr.NewReader()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if got := readAllManaged(t, reader); fmt.Sprint(got) != "[1 2 3]" {
		t.Fatalf("adopted read = %v", got)
	}
	if err = mgr.Close(); err != nil {
		t.Fatal(err)
	}

	// A truncated legacy file with no marker cannot be recovered silently.
	path2 := filepath.Join(dir, "legacy-trunc.avro")
	raw := readFile(t, path)
	if err = os.WriteFile(path2, raw[:len(raw)-5], 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = NewOCFAppendManager(OCFAppendConfig{Path: path2}); err == nil {
		t.Fatal("expected error adopting truncated marker-less OCF")
	}
}

func TestOCFAppendPostCommitErrorReportsTrueState(t *testing.T) {
	mgr, _, _ := newTestManager(t, CompressionNullLabel)
	defer mgr.Close()

	mgr.hooks.afterMarkerRename = func() error {
		return errors.New("simulated directory sync failure")
	}
	receipt, err := mgr.AppendBatch("b", longs(1, 2), []byte("cp"))
	var commitErr *CommitError
	if !errors.As(err, &commitErr) {
		t.Fatalf("expected CommitError, got %v", err)
	}
	if !commitErr.Committed {
		t.Fatal("Committed should be true after rename")
	}
	if commitErr.Boundary != receipt.Boundary {
		t.Fatalf("CommitError boundary = %d, receipt = %d", commitErr.Boundary, receipt.Boundary)
	}

	// The committed data must survive and be readable: no truncation allowed.
	if mgr.Boundary() != receipt.Boundary {
		t.Fatalf("boundary after CommitError = %d, want %d", mgr.Boundary(), receipt.Boundary)
	}
	reader, err := mgr.NewReader()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if got := readAllManaged(t, reader); fmt.Sprint(got) != "[1 2]" {
		t.Fatalf("read after CommitError = %v", got)
	}

	// Replay returns the same receipt rather than re-appending.
	again, err := mgr.AppendBatch("b", longs(99), nil)
	if err != nil {
		t.Fatalf("replay: %s", err)
	}
	if !receiptsEqual(again, receipt) {
		t.Fatalf("replay receipt = %+v, want %+v", again, receipt)
	}
}

func TestOCFAppendValidationErrors(t *testing.T) {
	if _, err := NewOCFAppendManager(OCFAppendConfig{}); err == nil {
		t.Fatal("expected error for empty Path")
	}
	dir := t.TempDir()
	if _, err := NewOCFAppendManager(OCFAppendConfig{Path: filepath.Join(dir, "x.avro")}); err == nil {
		t.Fatal("expected error when creating without Schema")
	}

	mgr, _, _ := newTestManager(t, CompressionNullLabel)
	if _, err := mgr.AppendBatch("", longs(1), nil); err == nil {
		t.Fatal("expected error for empty BatchID")
	}
	if _, err := mgr.AppendBatch("b", nil, nil); err == nil {
		t.Fatal("expected error for empty batch")
	}
	if err := mgr.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.AppendBatch("b", longs(1), nil); err == nil {
		t.Fatal("expected error appending to closed manager")
	}
	if _, err := mgr.NewReader(); err == nil {
		t.Fatal("expected error reading from closed manager")
	}
	// Close is idempotent.
	if err := mgr.Close(); err != nil {
		t.Fatalf("second Close: %s", err)
	}
}

func TestOCFAppendMarkerChecksumRoundTrip(t *testing.T) {
	mgr, _, markerPath := newTestManager(t, CompressionSnappyLabel)
	receipt, err := mgr.AppendBatch("b", longs(1, 2, 3), []byte(`{"k":"v"}`))
	if err != nil {
		t.Fatal(err)
	}
	if err = mgr.Close(); err != nil {
		t.Fatal(err)
	}

	marker, found, err := loadOCFAppendMarker(markerPath)
	if err != nil || !found {
		t.Fatalf("loadOCFAppendMarker: %v found=%v", err, found)
	}
	if marker.Boundary != receipt.Boundary || marker.LastBatchID != "b" ||
		marker.LastBatchItemCount != 3 || marker.LastBatchCRC32 != receipt.CRC32 {
		t.Fatalf("marker does not match receipt: %+v vs %+v", marker, receipt)
	}
	if marker.Schema != testAppendSchema || marker.Compression != CompressionSnappyLabel {
		t.Fatalf("marker header fields wrong: %+v", marker)
	}
	if string(marker.LastCheckpoint) != `{"k":"v"}` {
		t.Fatalf("checkpoint = %q", marker.LastCheckpoint)
	}
}

func TestOCFAppendConcurrentReadersAndWriter(t *testing.T) {
	mgr, _, _ := newTestManager(t, CompressionDeflateLabel)

	const batches = 20
	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Several long-lived readers keep polling the advancing boundary while
	// batches are appended concurrently.
	for r := 0; r < 3; r++ {
		reader, err := mgr.NewReader()
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func(reader *ManagedOCFReader) {
			defer wg.Done()
			defer reader.Close()
			var seen int64
			for {
				select {
				case <-stop:
					return
				default:
				}
				for reader.Scan() {
					if _, err := reader.Read(); err != nil {
						t.Errorf("Read: %s", err)
						return
					}
					seen++
				}
				if err := reader.Err(); err != nil {
					t.Errorf("reader error: %s", err)
					return
				}
			}
		}(reader)
	}

	for b := 0; b < batches; b++ {
		if _, err := mgr.AppendBatch(fmt.Sprintf("batch-%d", b), longs(int64(b), int64(b+1)), nil); err != nil {
			t.Fatalf("AppendBatch %d: %s", b, err)
		}
	}
	close(stop)
	wg.Wait()

	// A fresh reader observes every committed datum, exactly once each.
	reader, err := mgr.NewReader()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	got := readAllManaged(t, reader)
	if len(got) != batches*2 {
		t.Fatalf("total items = %d, want %d", len(got), batches*2)
	}
	if err = mgr.Close(); err != nil {
		t.Fatal(err)
	}
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	stat, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return stat.Size()
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func receiptsEqual(a, b BatchReceipt) bool {
	return a.BatchID == b.BatchID &&
		a.Boundary == b.Boundary &&
		a.ItemCount == b.ItemCount &&
		a.AppendedBytes == b.AppendedBytes &&
		a.CRC32 == b.CRC32 &&
		bytesEqual(a.Checkpoint, b.Checkpoint)
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func mustMarshalMarker(t *testing.T, marker ocfAppendMarker) []byte {
	t.Helper()
	marker.Checksum = 0
	payload, err := json.Marshal(marker)
	if err != nil {
		t.Fatal(err)
	}
	marker.Checksum = crc32.ChecksumIEEE(payload)
	out, err := json.Marshal(marker)
	if err != nil {
		t.Fatal(err)
	}
	return out
}
