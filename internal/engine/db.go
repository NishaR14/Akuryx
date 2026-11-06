package engine

import (
	"Akuryx/internal/index"
	"Akuryx/internal/storage"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrDBClosed    = errors.New("database is closed")
)

type DB struct {
	mu          sync.RWMutex
	wal         *storage.WAL
	index       *index.Index
	path        string
	maxBytes    int64
	nextID      uint64
	closed      bool
	readLeases  sync.Map
	leaseWindow time.Duration
}

func Open(path string, maxBytes int64) (*DB, error) {
	if path == "" {
		return nil, errors.New("database path cannot be empty")
	}

	db := &DB{
		index:       index.New(),
		path:        path,
		maxBytes:    maxBytes,
		leaseWindow: 50 * time.Millisecond,
	}

	wal, transactions, err := db.recoverCompaction()
	if err != nil {
		return nil, err
	}

	if wal == nil {
		wal, transactions, err = storage.OpenWAL(path, maxBytes)
		if err != nil {
			return nil, err
		}
	}

	db.wal = wal
	db.rebuildIndex(transactions)

	return db, nil
}

// Put stores a value.
func (db *DB) Put(key string, value []byte) error {
	return db.Batch([]storage.Operation{
		{
			Type:  storage.OpPut,
			Key:   key,
			Value: value,
		},
	})
}

func (db *DB) goroutineID() uint64 {
	buf := make([]byte, 64)
	for {
		n := runtime.Stack(buf, false)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, len(buf)*2)
	}
	stack := string(buf)
	parts := strings.Fields(stack)
	if len(parts) < 2 {
		return 0
	}
	id, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0
	}
	return id
}

func (db *DB) acquireReadLease() {
	id := db.goroutineID()
	if id == 0 {
		return
	}
	expiry := time.Now().Add(db.leaseWindow)
	db.readLeases.Store(id, expiry)
}

func (db *DB) waitForReadLeases() {
	for {
		active := false
		now := time.Now()
		db.readLeases.Range(func(key, value any) bool {
			expiry, ok := value.(time.Time)
			if !ok || !expiry.After(now) {
				db.readLeases.Delete(key)
				return true
			}
			active = true
			return true
		})
		if !active {
			return
		}
		time.Sleep(1 * time.Millisecond)
	}
}

// Get retrieves a value.
func (db *DB) Get(key string) ([]byte, error) {
	db.acquireReadLease()
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil, ErrDBClosed
	}

	entry, exists := db.index.Get(key)
	if !exists {
		return nil, ErrKeyNotFound
	}

	tx, err := db.wal.ReadAt(entry.Offset)
	if err != nil {
		return nil, err
	}

	if entry.OperationPos < 0 ||
		entry.OperationPos >= len(tx.Operations) {
		return nil, fmt.Errorf("invalid operation position")
	}

	op := tx.Operations[entry.OperationPos]
	if op.Type != storage.OpPut {
		return nil, fmt.Errorf("indexed operation is not a put")
	}

	return append([]byte(nil), op.Value...), nil
}

// Delete removes a key.
func (db *DB) Delete(key string) error {
	return db.Batch([]storage.Operation{
		{
			Type: storage.OpDelete,
			Key:  key,
		},
	})
}

// Batch applies multiple operations as one transaction.
func (db *DB) Batch(operations []storage.Operation) error {
	db.waitForReadLeases()
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrDBClosed
	}

	if len(operations) == 0 {
		return errors.New("batch cannot be empty")
	}

	tx := storage.Transaction{
		ID:         db.nextID + 1,
		Operations: operations,
	}

	// Capture the starting offset before appending.
	// Append is serialized by the database mutex.
	offset, err := db.wal.Size()
	if err != nil {
		return err
	}

	// Persist and synchronize before updating the index.
	if err := db.wal.Append(tx); err != nil {
		return err
	}

	for position, op := range operations {
		switch op.Type {
		case storage.OpPut:
			db.index.Set(op.Key, index.Entry{
				Offset:       offset,
				OperationPos: position,
			})
		case storage.OpDelete:
			db.index.Delete(op.Key)
		}
	}

	db.nextID = tx.ID
	return nil
}

// rebuildIndex reconstructs the in-memory index from recovered transactions.
// rebuildIndex reconstructs the in-memory index from recovered transactions.
func (db *DB) rebuildIndex(transactions []storage.RecoveredTransaction) {

	db.index = index.New()
	db.nextID = 0

	for _, recovered := range transactions {
		tx := recovered.Transaction

		for position, op := range tx.Operations {
			switch op.Type {
			case storage.OpPut:
				db.index.Set(op.Key, index.Entry{
					Offset:       recovered.Offset,
					OperationPos: position,
				})

			case storage.OpDelete:
				db.index.Delete(op.Key)
			}
		}

		if tx.ID > db.nextID {
			db.nextID = tx.ID
		}
	}
}

// liveOperations returns the latest value of every existing key.
// liveOperations returns the latest value of every existing key.
func (db *DB) liveOperations() ([]storage.Operation, error) {
	entries := db.index.Entries()

	operations := make([]storage.Operation, 0, len(entries))

	for key, entry := range entries {
		tx, err := db.wal.ReadAt(entry.Offset)
		if err != nil {
			return nil, err
		}

		if entry.OperationPos < 0 || entry.OperationPos >= len(tx.Operations) {
			return nil, storage.ErrInvalidOffset
		}

		op := tx.Operations[entry.OperationPos]

		if op.Type != storage.OpPut || op.Key != key {
			return nil, storage.ErrDatabaseCorrupt
		}

		operations = append(operations, storage.Operation{
			Type:  storage.OpPut,
			Key:   key,
			Value: append([]byte(nil), op.Value...),
		})
	}

	return operations, nil
}

// buildCompactionTransaction creates one transaction containing all live values.
// buildCompactionTransactions splits live operations into valid transactions.
func (db *DB) buildCompactionTransactions() ([]storage.Transaction, error) {
	operations, err := db.liveOperations()
	if err != nil {
		return nil, err
	}

	var transactions []storage.Transaction

	for start := 0; start < len(operations); start += storage.MaxBatchOperations {
		end := start + storage.MaxBatchOperations

		if end > len(operations) {
			end = len(operations)
		}

		tx := storage.Transaction{
			ID:         db.nextID + uint64(len(transactions)) + 1,
			Operations: operations[start:end],
		}

		transactions = append(transactions, tx)
	}

	return transactions, nil
}

// compactionPath returns the temporary database file path.
func (db *DB) compactionPath() string {
	return filepath.Join(
		filepath.Dir(db.path),
		filepath.Base(db.path)+".compact",
	)
}

func (db *DB) compactionMarkerPath() string {
	return db.path + ".compact.marker"
}

// writeCompactionMarker writes and synchronizes the compaction recovery marker.
// writeCompactionMarker writes metadata for the compacted database.
func (db *DB) writeCompactionMarker(compactedSize int64) error {
	if compactedSize < storage.FileHeaderSize {
		return errors.New("invalid compacted file size")
	}

	// Calculate checksum of the compacted database.
	tempFile, err := os.Open(db.compactionPath())
	if err != nil {
		return err
	}

	hash := crc32.NewIEEE()

	written, err := io.Copy(hash, tempFile)
	closeErr := tempFile.Close()

	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}

	if written != compactedSize {
		return errors.New("compacted file size changed")
	}

	markerPath := db.compactionMarkerPath()

	file, err := os.OpenFile(
		markerPath,
		os.O_CREATE|os.O_TRUNC|os.O_WRONLY,
		0600,
	)
	if err != nil {
		return err
	}

	marker := make([]byte, 24)

	copy(marker[:8], "AKCMP003")
	binary.LittleEndian.PutUint64(marker[8:16], uint64(compactedSize))
	binary.LittleEndian.PutUint32(marker[16:20], hash.Sum32())
	binary.LittleEndian.PutUint32(
		marker[20:24],
		crc32.ChecksumIEEE(marker[:20]),
	)

	if err := writeAll(file, marker); err != nil {
		_ = file.Close()
		return err
	}

	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}

	return file.Close()
}

// backupPath returns the path used to preserve the original database.
func (db *DB) backupPath() string {
	return db.path + ".backup"
}

// damagedPath returns the path used to preserve a corrupted database.
func (db *DB) damagedPath() string {
	return db.path + ".damaged"
}

// recoverCompaction restores a valid backup if the main database is invalid.

// recoverCompaction repairs the main WAL's torn tail or restores a valid
// backup after an interrupted compaction.
func (db *DB) recoverCompaction() (*storage.WAL, []storage.RecoveredTransaction, error) {
	mainExists, err := fileExists(db.path)
	if err != nil {
		return nil, nil, err
	}

	backupExists, err := fileExists(db.backupPath())
	if err != nil {
		return nil, nil, err
	}

	var mainErr error

	if mainExists {
		wal, transactions, openErr := storage.OpenWAL(db.path, db.maxBytes)
		if openErr == nil {
			if err := removeIfExists(db.backupPath()); err != nil {
				wal.Close()
				return nil, nil, fmt.Errorf("remove stale backup: %w", err)
			}
			if err := removeIfExists(db.compactionPath()); err != nil {
				wal.Close()
				return nil, nil, fmt.Errorf("remove stale compacted file: %w", err)
			}
			if err := removeIfExists(db.compactionMarkerPath()); err != nil {
				wal.Close()
				return nil, nil, fmt.Errorf("remove stale compaction marker: %w", err)
			}

			return wal, transactions, nil
		}

		mainErr = openErr
	}

	if !backupExists {
		if mainExists {
			return nil, nil, fmt.Errorf(
				"main database is invalid and no backup exists: %w",
				mainErr,
			)
		}
		return nil, nil, nil
	}

	backupWAL, _, err := storage.OpenWAL(db.backupPath(), db.maxBytes)
	if err != nil {
		return nil, nil, fmt.Errorf("backup database is invalid: %w", err)
	}

	if err := backupWAL.Close(); err != nil {
		return nil, nil, fmt.Errorf("close backup database: %w", err)
	}

	if mainExists {
		damagedExists, err := fileExists(db.damagedPath())
		if err != nil {
			return nil, nil, err
		}

		if damagedExists {
			return nil, nil, fmt.Errorf(
				"cannot preserve damaged database: %s already exists",
				db.damagedPath(),
			)
		}

		if err := os.Rename(db.path, db.damagedPath()); err != nil {
			return nil, nil, fmt.Errorf("preserve damaged database: %w", err)
		}
	}

	if err := os.Rename(db.backupPath(), db.path); err != nil {
		return nil, nil, fmt.Errorf("restore database backup: %w", err)
	}

	if err := removeIfExists(db.compactionPath()); err != nil {
		return nil, nil, fmt.Errorf("remove abandoned compacted file: %w", err)
	}

	if err := removeIfExists(db.compactionMarkerPath()); err != nil {
		return nil, nil, fmt.Errorf("remove abandoned compaction marker: %w", err)
	}

	// Reopen the restored database and return its recovered transactions.
	wal, transactions, err := storage.OpenWAL(db.path, db.maxBytes)
	if err != nil {
		return nil, nil, fmt.Errorf("open restored database: %w", err)
	}

	return wal, transactions, nil
}

// fileExists reports whether a file exists at the given path.
func fileExists(path string) (bool, error) {
	_, err := os.Stat(path)

	if err == nil {
		return true, nil
	}

	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}

	return false, err
}

// checkWAL opens a database file and verifies that its WAL can be recovered.

// inspectWAL opens a WAL, recovers its transactions, and closes it.
// inspectWAL opens an existing WAL, recovers its transactions, and closes it.
func inspectWAL(path string) ([]storage.RecoveredTransaction, error) {
	exists, err := fileExists(path)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, os.ErrNotExist
	}

	wal, err := storage.OpenWALReadOnly(path)
	if err != nil {
		return nil, err
	}
	defer wal.Close()

	return wal.Inspect()
}

// removeIfExists removes a file, treating a missing file as success.
func removeIfExists(path string) error {
	err := os.Remove(path)

	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	return nil
}

// writeAll writes every byte or returns an error.
func writeAll(file *os.File, data []byte) error {
	for len(data) > 0 {
		n, err := file.Write(data)
		if err != nil {
			return err
		}

		if n == 0 {
			return errors.New("short write")
		}

		data = data[n:]
	}

	return nil
}

// validateCompactionMarker checks the marker signature and checksum.
// validateCompactionMarker checks marker metadata and compacted file integrity.
func (db *DB) validateCompactionMarker() (int64, error) {
	data, err := os.ReadFile(db.compactionMarkerPath())
	if err != nil {
		return 0, err
	}

	if len(data) != 24 {
		return 0, errors.New("invalid compaction marker length")
	}

	if string(data[:8]) != "AKCMP003" {
		return 0, errors.New("invalid compaction marker signature")
	}

	expectedMarkerCRC := binary.LittleEndian.Uint32(data[20:24])
	actualMarkerCRC := crc32.ChecksumIEEE(data[:20])

	if expectedMarkerCRC != actualMarkerCRC {
		return 0, errors.New("invalid compaction marker checksum")
	}

	size := binary.LittleEndian.Uint64(data[8:16])

	if size < storage.FileHeaderSize || size > uint64(^uint64(0)>>1) {
		return 0, errors.New("invalid compacted file size")
	}

	// Check the compacted file's actual size and checksum.
	tempFile, err := os.Open(db.compactionPath())
	if err != nil {
		return 0, err
	}

	hash := crc32.NewIEEE()
	written, copyErr := io.Copy(hash, tempFile)
	closeErr := tempFile.Close()

	if copyErr != nil {
		return 0, copyErr
	}
	if closeErr != nil {
		return 0, closeErr
	}

	if uint64(written) != size {
		return 0, errors.New("compacted file size does not match marker")
	}

	expectedFileCRC := binary.LittleEndian.Uint32(data[16:20])

	if hash.Sum32() != expectedFileCRC {
		return 0, errors.New("compacted file checksum does not match marker")
	}

	return int64(size), nil
}
func (db *DB) validateCompactionFile() error {
	// First verify the marker and the file checksum.
	if _, err := db.validateCompactionMarker(); err != nil {
		return err
	}

	// Then verify that the compacted file is a valid database.
	if _, err := inspectWAL(db.compactionPath()); err != nil {
		return fmt.Errorf("compacted database is invalid: %w", err)
	}

	return nil
}

// writeCompactionFile writes compacted transactions into a new temporary WAL.
func (db *DB) writeCompactionFile(
	transactions []storage.Transaction,
) (*storage.WAL, []storage.RecoveredTransaction, error) {
	tempPath := db.compactionPath()

	// Remove an old temporary file if one exists.
	// Do not overwrite a temporary file left by an earlier compaction.
	// Recovery must inspect it before it can be removed.
	exists, err := fileExists(tempPath)
	if err != nil {
		return nil, nil, err
	}
	if exists {
		return nil, nil, fmt.Errorf(
			"temporary compaction file already exists: %s; recover it before starting another compaction",
			tempPath,
		)
	}

	// Create the temporary database.
	tempWAL, _, err := storage.OpenWAL(tempPath, db.maxBytes)
	if err != nil {
		return nil, nil, err
	}

	// Write every compacted transaction.
	for _, tx := range transactions {
		if err := tempWAL.Append(tx); err != nil {
			_ = tempWAL.Close()
			return nil, nil, err
		}
	}

	// Return the temporary WAL and its recovered transactions.
	recovered, err := tempWAL.Recover()
	if err != nil {
		_ = tempWAL.Close()
		return nil, nil, err
	}

	return tempWAL, recovered, nil
}

// reopenWAL opens the database file again after replacement.
func (db *DB) reopenWAL() error {
	wal, transactions, err := storage.OpenWAL(db.path, db.maxBytes)
	if err != nil {
		return err
	}

	db.wal = wal
	db.rebuildIndex(transactions)

	return nil
}

// Compact creates and verifies a compacted temporary database file.
// The original database remains unchanged.
// Compact creates and verifies a compacted temporary database file.
// The original database remains unchanged.
func (db *DB) Compact() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrDBClosed
	}

	// The currently open main database is valid.
	// Remove leftovers from any earlier interrupted preparation.
	for _, path := range []string{
		db.backupPath(),
		db.compactionPath(),
		db.compactionMarkerPath(),
	} {
		if err := removeIfExists(path); err != nil {
			return fmt.Errorf("remove stale compaction file %s: %w", path, err)
		}
	}

	transactions, err := db.buildCompactionTransactions()
	if err != nil {
		return err
	}

	tempWAL, recovered, err := db.writeCompactionFile(transactions)
	if err != nil {
		return err
	}
	defer tempWAL.Close()

	// Verify the recovered transactions.
	if len(recovered) != len(transactions) {
		return fmt.Errorf(
			"compaction verification failed: expected %d transactions, got %d",
			len(transactions),
			len(recovered),
		)
	}

	for txIndex, tx := range transactions {
		actualTx := recovered[txIndex].Transaction

		if len(tx.Operations) != len(actualTx.Operations) {
			return errors.New("compaction verification failed: operation count mismatch")
		}

		for opIndex, expected := range tx.Operations {
			actual := actualTx.Operations[opIndex]

			if actual.Type != expected.Type ||
				actual.Key != expected.Key ||
				!bytes.Equal(actual.Value, expected.Value) {
				return fmt.Errorf(
					"compaction verification failed at transaction %d operation %d",
					txIndex,
					opIndex,
				)
			}
		}
	}

	compactedSize, err := tempWAL.Size()
	if err != nil {
		return fmt.Errorf("get compacted file size: %w", err)
	}

	if err := tempWAL.Close(); err != nil {
		return fmt.Errorf("close compacted database: %w", err)
	}

	if err := db.writeCompactionMarker(compactedSize); err != nil {
		return fmt.Errorf("write compaction marker: %w", err)
	}

	if err := db.validateCompactionFile(); err != nil {
		return fmt.Errorf("validate compacted database: %w", err)
	}

	// Close the original WAL before renaming its file.
	if err := db.wal.Close(); err != nil {
		return fmt.Errorf("close original database: %w", err)
	}

	// Preserve the original database.
	if err := os.Rename(db.path, db.backupPath()); err != nil {
		if reopenErr := db.reopenWAL(); reopenErr != nil {
			db.closed = true
			return fmt.Errorf(
				"backup rename failed: %v; reopening original failed: %w",
				err,
				reopenErr,
			)
		}
		return fmt.Errorf("rename original database to backup: %w", err)
	}

	// Promote the verified compacted database.
	if err := os.Rename(db.compactionPath(), db.path); err != nil {
		restoreErr := os.Rename(db.backupPath(), db.path)
		if restoreErr != nil {
			db.closed = true
			return fmt.Errorf(
				"promote compacted database failed: %v; restore backup failed: %w",
				err,
				restoreErr,
			)
		}

		if reopenErr := db.reopenWAL(); reopenErr != nil {
			db.closed = true
			return fmt.Errorf(
				"promote failed: %v; reopening restored database failed: %w",
				err,
				reopenErr,
			)
		}

		return fmt.Errorf("promote compacted database: %w", err)
	}

	// Open the new main database and rebuild its index.
	if err := db.reopenWAL(); err != nil {
		db.closed = true
		return fmt.Errorf(
			"compacted database promoted but could not be reopened; backup preserved: %w",
			err,
		)
	}

	// The new main database is open. Remove the old copy.
	if err := removeIfExists(db.backupPath()); err != nil {
		return fmt.Errorf("remove old database backup: %w", err)
	}

	if err := removeIfExists(db.compactionMarkerPath()); err != nil {
		return fmt.Errorf("remove compaction marker: %w", err)
	}

	return nil
}

// Close closes the database.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil
	}

	db.closed = true
	return db.wal.Close()
}

// syncDirectory synchronizes directory changes to disk.
func syncDirectory(path string) error {
	dir := filepath.Dir(path)

	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()

	return f.Sync()
}

// Item represents one live key-value pair.
type Item struct {
	Key   string
	Value []byte
}

// All returns a snapshot of every live key-value pair.
func (db *DB) All() ([]Item, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrDBClosed
	}

	entries := db.index.Entries()
	items := make([]Item, 0, len(entries))

	for key, entry := range entries {
		tx, err := db.wal.ReadAt(entry.Offset)
		if err != nil {
			return nil, err
		}

		if entry.OperationPos < 0 ||
			entry.OperationPos >= len(tx.Operations) {
			return nil, storage.ErrInvalidOffset
		}

		op := tx.Operations[entry.OperationPos]
		if op.Type != storage.OpPut || op.Key != key {
			return nil, storage.ErrDatabaseCorrupt
		}

		items = append(items, Item{
			Key:   key,
			Value: append([]byte(nil), op.Value...),
		})
	}

	return items, nil
}

// Count returns the number of live keys.
func (db *DB) Count() (int, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return 0, errors.New("database is closed")
	}

	return db.index.Len(), nil
}
