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
	"io"

	"os"
	"sync"
)

// ocfAppendMarkerVersion is the on-disk format version of the persisted
// append marker.
const ocfAppendMarkerVersion = 1

// ocfAppendMarker is the durable state of an OCFAppendManager. It is written
// to a side-car file atomically (temp file plus rename) and therefore every
// observable marker always describes a fully committed state. The marker is
// the single source of truth used when reopening an OCF: any data beyond the
// recorded Boundary is an unconfirmed tail and gets truncated away.
type ocfAppendMarker struct {
	Version int `json:"version"`

	// Boundary is the number of bytes of the OCF data file that belong to
	// committed blocks. Bytes at offsets greater than or equal to Boundary
	// were never committed.
	Boundary int64 `json:"boundary"`

	// Schema is the avro.schema value read from the actual OCF header when
	// the marker was written. On reopen it must match the header again.
	Schema string `json:"schema"`

	// Compression is the avro.codec value ("null", "deflate", or
	// "snappy") read from the actual OCF header.
	Compression string `json:"compression"`

	// LastBatchID identifies the most recently committed batch. Empty means
	// no batch has been committed yet.
	LastBatchID string `json:"last_batch_id,omitempty"`

	// LastBatchItemCount is the number of datum values in the most recently
	// committed batch.
	LastBatchItemCount int `json:"last_batch_item_count,omitempty"`

	// LastBatchBytes is the number of OCF block bytes appended by the most
	// recently committed batch.
	LastBatchBytes int64 `json:"last_batch_bytes,omitempty"`

	// LastBatchCRC32 is the IEEE CRC32 checksum over the block bytes appended
	// by the most recently committed batch.
	LastBatchCRC32 uint32 `json:"last_batch_crc32,omitempty"`

	// LastCheckpoint is the source checkpoint committed together with the
	// most recently committed batch, stored verbatim (JSON base64 encodes byte
	// slices automatically).
	LastCheckpoint []byte `json:"last_checkpoint,omitempty"`

	// Checksum is the IEEE CRC32 checksum of the canonical JSON encoding of
	// all the fields above. A torn or tampered marker fails this check and is
	// rejected rather than trusted.
	Checksum uint32 `json:"checksum"`
}

// OCFAppendConfig configures an OCFAppendManager.
type OCFAppendConfig struct {
	// Path is the file system path of the OCF data file (required). The file
	// is always opened in place: it is never rewritten, replaced, renamed or
	// recreated, so its inode is stable for the lifetime of the manager.
	Path string

	// MarkerPath optionally overrides the path of the durable marker file.
	// When empty, the marker lives next to the data file at Path with a
	// ".ocfmarker" suffix.
	MarkerPath string

	// Schema is required only when Path points at a file that does not yet
	// exist or is empty. For an existing OCF the schema is taken from the
	// actual file header, and if Schema is also provided it must match.
	Schema string

	// CompressionName is used only when creating a new OCF, defaulting to
	// "null". For an existing OCF the compression is taken from the actual
	// file header, and if CompressionName is also provided it must match.
	CompressionName string

	// MetaData specifies application specific meta data written into the
	// header of a newly created OCF. It is ignored when opening an existing
	// OCF, whose header is authoritative.
	MetaData map[string][]byte

	// SyncMarker optionally pins the 16-byte sync marker of a newly created
	// OCF. When the zero value is given a random marker is generated.
	SyncMarker [16]byte
}

// BatchReceipt is the durable proof that a batch was committed. It is
// published together with the boundary and the source checkpoint only after
// the appended data has been synced and the write handle closed successfully.
type BatchReceipt struct {
	// BatchID is the caller supplied identity of the batch.
	BatchID string

	// Boundary is the new committed boundary (the file size after the batch).
	Boundary int64

	// ItemCount is the number of datum values appended by the batch.
	ItemCount int

	// AppendedBytes is the number of OCF block bytes appended by the batch.
	AppendedBytes int64

	// CRC32 is the IEEE CRC32 checksum over the appended block bytes.
	CRC32 uint32

	// Checkpoint is the source checkpoint committed together with the batch,
	// copied verbatim from AppendBatch.
	Checkpoint []byte
}

// CommitError reports a failure that happened at or after the atomic marker
// publish. When Committed is true the batch itself is durably committed: the
// data file holds the complete batch and the boundary, receipt and checkpoint
// are already published. Callers must not truncate or re-append the batch;
// they should inspect the marker via LastReceipt to learn the true state.
type CommitError struct {
	// Committed is true when the batch is durably committed despite the
	// reported error.
	Committed bool

	// BatchID is the identity of the affected batch.
	BatchID string

	// Boundary is the committed boundary when Committed is true.
	Boundary int64

	// Err is the underlying failure, for example a directory fsync failure.
	Err error
}

// Error implements the error interface.
func (e *CommitError) Error() string {
	if e.Committed {
		return fmt.Sprintf("goavro: batch %q is committed at boundary %d but post-commit step failed: %s", e.BatchID, e.Boundary, e.Err)
	}
	return fmt.Sprintf("goavro: batch %q failed before commit: %s", e.BatchID, e.Err)
}

// Unwrap exposes the underlying error to errors.Is and errors.As.
func (e *CommitError) Unwrap() error { return e.Err }

// OCFAppendManager is a single-writer, in-place append manager for an Avro
// Object Container File. Every batch is appended natively to the same file
// using OCFWriter: the file is never rewritten or replaced and keeps the same
// inode. A durable marker records the committed boundary, the last batch
// receipt and the source checkpoint as one atomic publication, which makes
// crash recovery deterministic: unconfirmed tails are truncated, confirmed
// batches are never replayed.
type OCFAppendManager struct {
	config OCFAppendConfig

	// lockFile is kept open for the whole lifetime of the manager solely to
	// hold an advisory exclusive lock enforcing the single-writer rule.
	lockFile *os.File

	mu       sync.RWMutex
	closed   bool
	marker   ocfAppendMarker
	header   *ocfHeader
	receipts map[string]BatchReceipt

	// Test only failure injection hooks.
	hooks appendHooks
}

// appendHooks holds optional failure injection points used by the tests.
type appendHooks struct {
	// beforeMarkerRename is invoked after the new marker content has been
	// fully written and synced to the temp file but before the atomic rename.
	beforeMarkerRename func() error
	// afterMarkerRename is invoked right after the rename succeeds, before
	// the marker directory is synced.
	afterMarkerRename func() error
}

// NewOCFAppendManager opens or creates the managed OCF data file and returns
// its append manager. On creation the header is built from the config and
// written once. On reopen the actual header's schema and compression are read
// from the file and verified against both the persisted marker and, when
// provided, the config; the config is never trusted blindly. Any unconfirmed
// tail beyond the persisted committed boundary is truncated before the
// manager becomes usable.
func NewOCFAppendManager(config OCFAppendConfig) (*OCFAppendManager, error) {
	if config.Path == "" {
		return nil, errors.New("cannot create OCFAppendManager when Path is empty")
	}
	markerPath := config.MarkerPath
	if markerPath == "" {
		markerPath = config.Path + ".ocfmarker"
	}

	// Open the data file read-write, creating it if needed, without ever
	// truncating it.
	file, err := os.OpenFile(config.Path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("cannot open OCF data file: %s", err)
	}

	if err = flockExclusive(file); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("cannot acquire single-writer lock on OCF data file: %s", err)
	}

	manager := &OCFAppendManager{
		config:   config,
		lockFile: file,
		receipts: make(map[string]BatchReceipt),
	}

	stat, err := file.Stat()
	if err != nil {
		_ = manager.closeLocked()
		return nil, fmt.Errorf("cannot stat OCF data file: %s", err)
	}

	if stat.Size() == 0 {
		if err = manager.createOCF(file); err != nil {
			_ = manager.closeLocked()
			return nil, err
		}
	} else {
		if err = manager.openOCF(file, markerPath); err != nil {
			_ = manager.closeLocked()
			return nil, err
		}
	}

	return manager, nil
}

// createOCF writes a brand new OCF header into an empty file and publishes the
// initial marker at boundary zero.
func (m *OCFAppendManager) createOCF(file *os.File) error {
	if m.config.Schema == "" {
		return errors.New("cannot create new OCF without Schema specified")
	}
	header, err := newOCFHeader(OCFConfig{
		Schema:          m.config.Schema,
		CompressionName: m.config.CompressionName,
		MetaData:        m.config.MetaData,
		SyncMarker:      m.config.SyncMarker,
	})
	if err != nil {
		return fmt.Errorf("cannot create OCF header: %s", err)
	}
	if err = writeOCFHeader(header, file); err != nil {
		return fmt.Errorf("cannot write OCF header: %s", err)
	}
	if err = file.Sync(); err != nil {
		return fmt.Errorf("cannot sync new OCF header: %s", err)
	}
	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("cannot stat new OCF data file: %s", err)
	}
	m.header = header
	m.marker = ocfAppendMarker{
		Version:     ocfAppendMarkerVersion,
		Boundary:    stat.Size(),
		Schema:      header.codec.Schema(),
		Compression: compressionLabel(header.compressionID),
	}
	if err = m.persistMarkerLocked(); err != nil {
		return fmt.Errorf("cannot persist initial marker: %s", err)
	}
	return nil
}

// openOCF validates an existing OCF data file against the persisted marker,
// truncating any unconfirmed tail, and reconciles the result with the config.
func (m *OCFAppendManager) openOCF(file *os.File, markerPath string) error {
	// The actual file header is always the authority for schema and
	// compression.
	headerReader := io.NewSectionReader(file, 0, 1<<63-1)
	header, err := readOCFHeader(headerReader)
	if err != nil {
		return fmt.Errorf("cannot read OCF header from data file: %s", err)
	}
	m.header = header
	// The section reader position after readOCFHeader is exactly the header
	// size, which is where OCF data blocks begin.
	headerSize, err := headerReader.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("cannot determine OCF header size: %s", err)
	}

	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("cannot stat OCF data file: %s", err)
	}
	fileSize := stat.Size()

	stored, markerFound, err := loadOCFAppendMarker(markerPath)
	if err != nil {
		return err
	}

	var boundary int64
	if markerFound {
		// The marker describes a previously committed state. Re-verify every
		// recorded fact against the actual header instead of trusting it.
		if stored.Schema != header.codec.Schema() {
			return fmt.Errorf("OCF marker schema does not match actual file header")
		}
		if stored.Compression != compressionLabel(header.compressionID) {
			return fmt.Errorf("OCF marker compression %q does not match actual file header %q", stored.Compression, compressionLabel(header.compressionID))
		}
		if stored.Boundary > fileSize {
			return fmt.Errorf("OCF marker boundary %d exceeds data file size %d", stored.Boundary, fileSize)
		}
		if stored.Boundary < headerSize {
			return fmt.Errorf("OCF marker boundary %d is inside the OCF header of size %d", stored.Boundary, headerSize)
		}
		boundary = stored.Boundary
	} else {
		// No marker: the whole file is treated as the legacy committed prefix,
		// but only if every block in it is intact.
		boundary, err = scanOCFBlocks(header, file, headerSize, fileSize)
		if err != nil {
			return fmt.Errorf("cannot verify OCF data file without marker: %s", err)
		}
		if boundary != fileSize {
			return fmt.Errorf("OCF data file ends with an incomplete block at offset %d and has no marker to recover from", boundary)
		}
	}

	// Truncate every byte that never made it into a committed marker, then
	// sync so the trim itself is durable.
	if fileSize > boundary {
		if err = file.Truncate(boundary); err != nil {
			return fmt.Errorf("cannot truncate unconfirmed OCF tail: %s", err)
		}
		if err = file.Sync(); err != nil {
			return fmt.Errorf("cannot sync OCF data file after truncation: %s", err)
		}
	}

	// Validate the committed prefix block-by-block up to the boundary.
	verified, err := scanOCFBlocks(header, file, headerSize, boundary)
	if err != nil {
		return fmt.Errorf("cannot verify committed OCF prefix: %s", err)
	}
	if verified != boundary {
		return fmt.Errorf("committed OCF prefix ends with an incomplete block at offset %d", verified)
	}

	// Reconcile the actual header with the caller supplied config. The config
	// can only agree with the file; it can never redefine the file.
	if m.config.Schema != "" && m.config.Schema != header.codec.Schema() {
		return fmt.Errorf("configured schema does not match actual OCF file header")
	}
	if m.config.CompressionName != "" && m.config.CompressionName != compressionLabel(header.compressionID) {
		return fmt.Errorf("configured compression %q does not match actual OCF file header %q", m.config.CompressionName, compressionLabel(header.compressionID))
	}

	if markerFound {
		m.marker = *stored
		if stored.LastBatchID != "" {
			m.receipts[stored.LastBatchID] = receiptFromMarker(*stored)
		}
	} else {
		m.marker = ocfAppendMarker{
			Version:     ocfAppendMarkerVersion,
			Boundary:    boundary,
			Schema:      header.codec.Schema(),
			Compression: compressionLabel(header.compressionID),
		}
		if err = m.persistMarkerLocked(); err != nil {
			return fmt.Errorf("cannot persist recovery marker: %s", err)
		}
	}
	return nil
}

// AppendBatch appends one batch of datum values to the managed OCF using the
// native OCFWriter append path, without rewriting or replacing the file. The
// boundary, batch receipt and source checkpoint are published together in one
// atomic marker update, and only after the appended bytes have been synced and
// the write handle closed successfully. Any failure before that publication
// rolls the file back to the previous boundary. A repeated BatchID returns the
// previously committed receipt without appending anything, so replaying a
// committed batch after a crash never duplicates data.
func (m *OCFAppendManager) AppendBatch(batchID string, data []interface{}, checkpoint []byte) (BatchReceipt, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return BatchReceipt{}, errors.New("cannot append batch to closed OCFAppendManager")
	}
	if batchID == "" {
		return BatchReceipt{}, errors.New("cannot append batch with empty BatchID")
	}
	if len(data) == 0 {
		return BatchReceipt{}, errors.New("cannot append empty batch")
	}
	// Idempotent replay of an already committed batch: return the stored
	// receipt and do not touch the data file.
	if receipt, ok := m.receipts[batchID]; ok {
		return receipt, nil
	}

	oldBoundary := m.marker.Boundary

	// Reopen a fresh write handle positioned exactly at the committed
	// boundary. Opening per batch is what lets us close (and therefore drop
	// all kernel buffers) before publishing the marker.
	file, err := os.OpenFile(m.config.Path, os.O_RDWR, 0644)
	if err != nil {
		return BatchReceipt{}, fmt.Errorf("cannot open OCF data file for append: %s", err)
	}

	rollback := func(cause error) (BatchReceipt, error) {
		_ = file.Close()
		// Best effort restore of the old boundary. A failure here is
		// reported; the persisted marker still protects the data because the
		// next reopen truncates the tail deterministically.
		if rbErr := m.restoreBoundary(oldBoundary); rbErr != nil {
			return BatchReceipt{}, fmt.Errorf("batch %q failed: %s; rollback also failed: %s", batchID, cause, rbErr)
		}
		return BatchReceipt{}, fmt.Errorf("batch %q failed before commit and was rolled back: %s", batchID, cause)
	}

	if _, err = file.Seek(oldBoundary, io.SeekStart); err != nil {
		return rollback(fmt.Errorf("cannot seek to committed boundary: %s", err))
	}
	// Defensive: make sure no stale bytes sit beyond the boundary even though
	// recovery should already have trimmed them.
	if stat, statErr := file.Stat(); statErr != nil {
		return rollback(fmt.Errorf("cannot stat OCF data file: %s", statErr))
	} else if stat.Size() > oldBoundary {
		if truncErr := file.Truncate(oldBoundary); truncErr != nil {
			return rollback(fmt.Errorf("cannot trim stale tail before append: %s", truncErr))
		}
	}

	counter := &countingWriter{w: file}
	// Reuse the native OCFWriter append machinery against a writer anchored at
	// the boundary, so no full-file rescan is needed.
	ocfw := &OCFWriter{header: m.header, iow: counter}
	if err = ocfw.Append(data); err != nil {
		return rollback(fmt.Errorf("native OCF append failed: %s", err))
	}
	appended := counter.n
	if appended <= 0 {
		return rollback(errors.New("OCF append wrote no bytes"))
	}

	// Durability gate: nothing may be published until the data is on stable
	// storage and the write handle is closed.
	if err = file.Sync(); err != nil {
		return rollback(fmt.Errorf("cannot sync appended batch: %s", err))
	}
	if err = file.Close(); err != nil {
		// The close failed; do not publish anything. The marker keeps the old
		// boundary, so reopen will treat these bytes as an unconfirmed tail.
		return BatchReceipt{}, fmt.Errorf("cannot close OCF write handle after batch %q: %s", batchID, err)
	}

	newBoundary := oldBoundary + appended
	receipt := BatchReceipt{
		BatchID:       batchID,
		Boundary:      newBoundary,
		ItemCount:     len(data),
		AppendedBytes: appended,
		CRC32:         counter.crc,
		Checkpoint:    checkpointCopy(checkpoint),
	}

	// Single atomic publication of boundary, batch receipt and checkpoint.
	next := m.marker
	next.Boundary = newBoundary
	next.LastBatchID = receipt.BatchID
	next.LastBatchItemCount = receipt.ItemCount
	next.LastBatchBytes = receipt.AppendedBytes
	next.LastBatchCRC32 = receipt.CRC32
	next.LastCheckpoint = receipt.Checkpoint
	next.Checksum = 0
	if err = m.persistMarkerWith(next, m.hooks); err != nil {
		var commitErr *CommitError
		if errors.As(err, &commitErr) {
			// Marker is already published; accept the commit and report the
			// true state instead of truncating confirmed data.
			m.marker = next
			m.receipts[batchID] = receipt
			commitErr.Boundary = newBoundary
			return receipt, commitErr
		}
		// Publication failed before the rename: roll the data file back to
		// the old boundary.
		if rbErr := m.restoreBoundary(oldBoundary); rbErr != nil {
			return BatchReceipt{}, fmt.Errorf("marker publish failed: %s; rollback also failed: %s", err, rbErr)
		}
		return BatchReceipt{}, fmt.Errorf("marker publish failed before commit: %s", err)
	}

	m.marker = next
	m.receipts[batchID] = receipt
	return receipt, nil
}

// restoreBoundary truncates and syncs the data file back to boundary using a
// short-lived handle, leaving the manager's lock handle untouched.
func (m *OCFAppendManager) restoreBoundary(boundary int64) error {
	file, err := os.OpenFile(m.config.Path, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	if err = file.Truncate(boundary); err != nil {
		return err
	}
	return file.Sync()
}

// NewReader returns a managed OCFReader over the data file. The reader only
// ever observes the committed prefix: it is bounded by the current committed
// boundary and cannot read into an unconfirmed tail. The same handle keeps
// working across later AppendBatch calls, and after a commit Scan picks up the
// newly published blocks.
func (m *OCFAppendManager) NewReader() (*ManagedOCFReader, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return nil, errors.New("cannot create reader on closed OCFAppendManager")
	}
	file, err := os.Open(m.config.Path)
	if err != nil {
		return nil, fmt.Errorf("cannot open OCF data file for reading: %s", err)
	}
	limiter := &boundedFileReader{manager: m, file: file}
	ocfr, err := NewOCFReader(limiter)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	return &ManagedOCFReader{ocfr: ocfr, file: file, limiter: limiter}, nil
}

// Boundary returns the current committed boundary, i.e. the size in bytes of
// the committed prefix of the data file.
func (m *OCFAppendManager) Boundary() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.marker.Boundary
}

// Schema returns the Avro schema read from the actual OCF file header.
func (m *OCFAppendManager) Schema() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.header.codec.Schema()
}

// CompressionName returns the compression algorithm read from the actual OCF
// file header.
func (m *OCFAppendManager) CompressionName() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return compressionLabel(m.header.compressionID)
}

// Codec returns the codec read from the actual OCF file header.
func (m *OCFAppendManager) Codec() *Codec {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.header.codec
}

// LastReceipt returns the receipt of the most recently committed batch and
// true, or the zero value and false when no batch has been committed yet.
func (m *OCFAppendManager) LastReceipt() (BatchReceipt, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.marker.LastBatchID == "" {
		return BatchReceipt{}, false
	}
	return receiptFromMarker(m.marker), true
}

// LastCheckpoint returns the checkpoint of the most recently committed batch
// and true, or nil and false when no batch has been committed yet.
func (m *OCFAppendManager) LastCheckpoint() ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.marker.LastBatchID == "" || len(m.marker.LastCheckpoint) == 0 {
		return nil, false
	}
	out := make([]byte, len(m.marker.LastCheckpoint))
	copy(out, m.marker.LastCheckpoint)
	return out, true
}

// Close releases the single-writer lock and its backing handle. Already
// returned ManagedOCFReader handles keep working on their own file handles.
func (m *OCFAppendManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	return m.closeLocked()
}

// closeLocked closes the lock handle without touching the closed flag or
// taking the mutex, for use during constructor failure paths.
func (m *OCFAppendManager) closeLocked() error {
	if m.lockFile == nil {
		return nil
	}
	err := m.lockFile.Close()
	m.lockFile = nil
	return err
}

// persistMarkerLocked checkpoints the in-memory marker to disk atomically.
func (m *OCFAppendManager) persistMarkerLocked() error {
	return m.persistMarkerWith(m.marker, m.hooks)
}

// persistMarkerWith atomically writes marker to the configured marker path:
// fully encode and fsync a temp file in the same directory, rename it over the
// previous marker, then fsync the directory so the rename itself is durable.
func (m *OCFAppendManager) persistMarkerWith(marker ocfAppendMarker, hooks appendHooks) error {
	marker.Version = ocfAppendMarkerVersion
	marker.Checksum = 0
	payload, err := json.Marshal(marker)
	if err != nil {
		return fmt.Errorf("cannot encode marker: %s", err)
	}
	marker.Checksum = crc32.ChecksumIEEE(payload)

	dir, name := splitMarkerPath(m.markerPath())
	tmp, err := os.CreateTemp(dir, name+".tmp-*")
	if err != nil {
		return fmt.Errorf("cannot create marker temp file: %s", err)
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }

	encoded, err := json.Marshal(marker)
	if err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("cannot encode marker: %s", err)
	}
	if _, err = tmp.Write(encoded); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("cannot write marker temp file: %s", err)
	}
	if err = tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("cannot sync marker temp file: %s", err)
	}
	if err = tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("cannot close marker temp file: %s", err)
	}

	if hooks.beforeMarkerRename != nil {
		if err = hooks.beforeMarkerRename(); err != nil {
			cleanup()
			return err
		}
	}

	if err = os.Rename(tmpName, m.markerPath()); err != nil {
		cleanup()
		// Rename did not happen: the new marker is not observable, so this is
		// a pre-commit failure and the caller must roll the data file back.
		return fmt.Errorf("cannot publish marker: %s", err)
	}

	if hooks.afterMarkerRename != nil {
		if err = hooks.afterMarkerRename(); err != nil {
			// The rename already succeeded, so the commit is observable even
			// though a later step failed. Report the true committed state.
			return &CommitError{Committed: true, BatchID: marker.LastBatchID, Err: err}
		}
	}

	if err = syncDir(dir); err != nil {
		// Same reasoning: the rename succeeded, the batch is committed. Do
		// not let the caller truncate confirmed data.
		return &CommitError{Committed: true, BatchID: marker.LastBatchID, Err: fmt.Errorf("cannot sync marker directory: %s", err)}
	}
	return nil
}

func (m *OCFAppendManager) markerPath() string {
	if m.config.MarkerPath != "" {
		return m.config.MarkerPath
	}
	return m.config.Path + ".ocfmarker"
}

// ManagedOCFReader is an OCFReader bounded to the committed prefix of its
// manager's data file. It never reads uncommitted bytes and transparently
// sees blocks appended by later committed batches.
type ManagedOCFReader struct {
	ocfr    *OCFReader
	file    *os.File
	limiter *boundedFileReader
}

// Scan reports whether at least one more committed datum is available. When
// the committed prefix is exhausted it returns false with no error; after the
// manager commits another batch, calling Scan again reads the new blocks
// through the same handle.
func (r *ManagedOCFReader) Scan() bool {
	for {
		if r.ocfr.Scan() {
			return true
		}
		if r.ocfr.Err() != nil {
			return false
		}
		// Clean EOF at the current boundary. Re-check whether the boundary
		// advanced while we were waiting.
		if !r.limiter.boundaryAdvanced() {
			return false
		}
		// New committed bytes exist: clear the EOF condition and retry.
		r.ocfr.rerr = nil
	}
}

// Read consumes one datum value after a successful Scan.
func (r *ManagedOCFReader) Read() (interface{}, error) { return r.ocfr.Read() }

// Err returns the first non-EOF error encountered while reading.
func (r *ManagedOCFReader) Err() error { return r.ocfr.Err() }

// Codec returns the codec found within the OCF file header.
func (r *ManagedOCFReader) Codec() *Codec { return r.ocfr.Codec() }

// CompressionName returns the compression algorithm found within the OCF file
// header.
func (r *ManagedOCFReader) CompressionName() string { return r.ocfr.CompressionName() }

// MetaData returns the file metadata map found within the OCF file header.
func (r *ManagedOCFReader) MetaData() map[string][]byte { return r.ocfr.MetaData() }

// RemainingBlockItems returns the number of items remaining in the block
// being processed.
func (r *ManagedOCFReader) RemainingBlockItems() int64 { return r.ocfr.RemainingBlockItems() }

// SkipThisBlockAndReset mirrors OCFReader.SkipThisBlockAndReset.
func (r *ManagedOCFReader) SkipThisBlockAndReset() { r.ocfr.SkipThisBlockAndReset() }

// Boundary returns the committed boundary as observed the last time this
// reader noticed an advancement.
func (r *ManagedOCFReader) Boundary() int64 { return r.limiter.lastBoundary() }

// Close releases the reader's private file handle.
func (r *ManagedOCFReader) Close() error {
	if r.file == nil {
		return nil
	}
	err := r.file.Close()
	r.file = nil
	return err
}

// boundedFileReader adapts an *os.File to an io.Reader that can never cross
// the manager's current committed boundary. Every Read re-evaluates the
// boundary under the manager's read lock, so a long-lived reader
// automatically gains access to newly committed blocks without reopening the
// file and can never observe an uncommitted partial block.
type boundedFileReader struct {
	manager *OCFAppendManager
	file    *os.File
	pos     int64
	seen    int64
}

// Read implements io.Reader within the committed prefix.
func (r *boundedFileReader) Read(p []byte) (int, error) {
	r.manager.mu.RLock()
	boundary := r.manager.marker.Boundary
	r.manager.mu.RUnlock()

	if r.pos >= boundary {
		r.seen = boundary
		return 0, io.EOF
	}
	remaining := boundary - r.pos
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}
	n, err := r.file.Read(p)
	r.pos += int64(n)
	if err == io.EOF && r.pos < boundary {
		// The file shrank underneath us even though the marker promised more
		// committed bytes: real corruption rather than a clean end of data.
		return n, io.ErrUnexpectedEOF
	}
	return n, err
}

// boundaryAdvanced reports whether the committed boundary moved past the
// point this reader has reached.
func (r *boundedFileReader) boundaryAdvanced() bool {
	r.manager.mu.RLock()
	boundary := r.manager.marker.Boundary
	r.manager.mu.RUnlock()
	if boundary > r.seen {
		r.seen = boundary
		return true
	}
	return false
}

// lastBoundary returns the most recently observed committed boundary.
func (r *boundedFileReader) lastBoundary() int64 {
	r.manager.mu.RLock()
	defer r.manager.mu.RUnlock()
	return r.manager.marker.Boundary
}

// countingWriter counts bytes written through it and computes their IEEE
// CRC32 checksum, giving every batch receipt a size and checksum provenance.
type countingWriter struct {
	w   io.Writer
	n   int64
	crc uint32
}

// Write implements io.Writer while tallying bytes and folding them into the
// running checksum.
func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	if n > 0 {
		c.n += int64(n)
		c.crc = crc32.Update(c.crc, crc32.IEEETable, p[:n])
	}
	return n, err
}

// loadOCFAppendMarker reads, parses and verifies the marker file. A missing
// marker is reported as markerFound=false with no error.
func loadOCFAppendMarker(path string) (*ocfAppendMarker, bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("cannot read OCF marker: %s", err)
	}
	var marker ocfAppendMarker
	if err = json.Unmarshal(data, &marker); err != nil {
		return nil, false, fmt.Errorf("cannot parse OCF marker: %s", err)
	}
	if marker.Version != ocfAppendMarkerVersion {
		return nil, false, fmt.Errorf("unsupported OCF marker version: %d", marker.Version)
	}
	storedChecksum := marker.Checksum
	marker.Checksum = 0
	payload, err := json.Marshal(marker)
	if err != nil {
		return nil, false, fmt.Errorf("cannot re-encode OCF marker: %s", err)
	}
	if crc32.ChecksumIEEE(payload) != storedChecksum {
		return nil, false, errors.New("OCF marker checksum mismatch: refusing to trust corrupted marker")
	}
	if marker.Boundary < 0 {
		return nil, false, errors.New("OCF marker carries negative boundary")
	}
	return &marker, true, nil
}

// receiptFromMarker rebuilds a BatchReceipt from the persisted marker state.
func receiptFromMarker(marker ocfAppendMarker) BatchReceipt {
	receipt := BatchReceipt{
		BatchID:       marker.LastBatchID,
		Boundary:      marker.Boundary,
		ItemCount:     marker.LastBatchItemCount,
		AppendedBytes: marker.LastBatchBytes,
		CRC32:         marker.LastBatchCRC32,
	}
	if len(marker.LastCheckpoint) > 0 {
		receipt.Checkpoint = make([]byte, len(marker.LastCheckpoint))
		copy(receipt.Checkpoint, marker.LastCheckpoint)
	}
	return receipt
}

// checkpointCopy returns an independent copy of checkpoint so later caller
// mutations cannot alter the committed receipt.
func checkpointCopy(checkpoint []byte) []byte {
	if checkpoint == nil {
		return nil
	}
	out := make([]byte, len(checkpoint))
	copy(out, checkpoint)
	return out
}

// compressionLabel maps the internal compression identifier to its OCF label.
func compressionLabel(id compressionID) string {
	switch id {
	case compressionNull:
		return CompressionNullLabel
	case compressionDeflate:
		return CompressionDeflateLabel
	case compressionSnappy:
		return CompressionSnappyLabel
	default:
		return "unknown"
	}
}

// splitMarkerPath splits the marker path into directory and base name for
// temp-file creation and directory fsync purposes.
func splitMarkerPath(path string) (dir, name string) {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' || path[i] == os.PathSeparator {
			return path[:i], path[i+1:]
		}
	}
	return ".", path
}
