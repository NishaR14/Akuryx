package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sync"
)

var (
	ErrDatabaseCorrupt = errors.New("database file is corrupted")
	ErrFileLimit       = errors.New("maximum database file size reached")
	ErrWALClosed       = errors.New("database file is closed")
	ErrInvalidOffset   = errors.New("invalid transaction offset")
	ErrWALFailed       = errors.New("database requires recovery after a storage error")
)

// RecoveredTransaction contains a transaction and its starting file offset.
type RecoveredTransaction struct {
	Offset      int64
	Transaction Transaction
}

type WAL struct {
	mu       sync.Mutex
	file     *os.File
	path     string
	maxBytes int64
	size     int64
	failed   bool
}

// OpenWAL opens or creates the database and recovers its transactions.
// maxBytes <= 0 means no configured size limit.
func OpenWAL(path string, maxBytes int64) (*WAL, []RecoveredTransaction, error) {
	if path == "" {
		return nil, nil, errors.New("database path cannot be empty")
	}

	if maxBytes > 0 && maxBytes < FileHeaderSize {
		return nil, nil, errors.New("maximum file size is smaller than the header")
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, nil, fmt.Errorf("open database: %w", err)
	}

	w := &WAL{
		file:     f,
		path:     path,
		maxBytes: maxBytes,
	}

	if err := w.initializeHeader(); err != nil {
		_ = f.Close()
		return nil, nil, err
	}

	transactions, err := w.Recover()
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}

	return w, transactions, nil
}

// OpenWALReadOnly opens an existing WAL without creating or modifying it.
func OpenWALReadOnly(path string) (*WAL, error) {
	if path == "" {
		return nil, errors.New("database path cannot be empty")
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open database read-only: %w", err)
	}

	w := &WAL{
		file: f,
		path: path,
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	if info.Size() < FileHeaderSize {
		_ = f.Close()
		return nil, fmt.Errorf("%w: incomplete file header", ErrDatabaseCorrupt)
	}

	header := make([]byte, FileHeaderSize)
	if _, err := f.ReadAt(header, 0); err != nil {
		_ = f.Close()
		return nil, err
	}

	if string(header[:8]) != FileMagic {
		_ = f.Close()
		return nil, fmt.Errorf("%w: invalid file magic", ErrDatabaseCorrupt)
	}

	version := binary.LittleEndian.Uint32(header[8:12])
	if version != FormatVersion {
		_ = f.Close()
		return nil, fmt.Errorf("%w: unsupported version %d",
			ErrDatabaseCorrupt, version)
	}

	expected := binary.LittleEndian.Uint32(header[12:16])
	actual := crc32.ChecksumIEEE(header[:12])
	if expected != actual {
		_ = f.Close()
		return nil, fmt.Errorf("%w: invalid file header checksum",
			ErrDatabaseCorrupt)
	}

	w.size = info.Size()
	return w, nil
}

// initializeHeader creates or validates the database file header.
// initializeHeader creates a header for a new empty database
// or validates the header of an existing database.
func (w *WAL) initializeHeader() error {
	info, err := w.file.Stat()
	if err != nil {
		return err
	}

	// Only a genuinely empty file is treated as a new database.
	if info.Size() == 0 {
		return w.writeNewHeader()
	}

	// An existing file with an incomplete header is damaged.
	if info.Size() < FileHeaderSize {
		return fmt.Errorf(
			"%w: incomplete database header: got %d bytes, need %d",
			ErrDatabaseCorrupt,
			info.Size(),
			FileHeaderSize,
		)
	}
	header := make([]byte, FileHeaderSize)

	if _, err := w.file.ReadAt(header, 0); err != nil {
		return err
	}

	if string(header[:8]) != FileMagic {
		return fmt.Errorf("%w: invalid file magic", ErrDatabaseCorrupt)
	}

	version := binary.LittleEndian.Uint32(header[8:12])
	if version != FormatVersion {
		return fmt.Errorf("%w: unsupported version %d",
			ErrDatabaseCorrupt, version)
	}

	expected := binary.LittleEndian.Uint32(header[12:16])
	actual := crc32.ChecksumIEEE(header[:12])

	if expected != actual {
		return fmt.Errorf("%w: invalid file header checksum",
			ErrDatabaseCorrupt)
	}

	w.size = info.Size()
	return nil
}

// writeNewHeader writes and synchronizes the database header.
func (w *WAL) writeNewHeader() error {
	header := make([]byte, FileHeaderSize)

	copy(header[:8], FileMagic)
	binary.LittleEndian.PutUint32(header[8:12], FormatVersion)

	checksum := crc32.ChecksumIEEE(header[:12])
	binary.LittleEndian.PutUint32(header[12:16], checksum)

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	if err := writeFull(w.file, header); err != nil {
		return err
	}

	if err := w.file.Sync(); err != nil {
		return err
	}

	w.size = FileHeaderSize
	return nil
}

// writeFull writes all bytes or returns an error.
func writeFull(f *os.File, data []byte) error {
	for len(data) > 0 {
		n, err := f.Write(data)
		if err != nil {
			return err
		}

		if n == 0 {
			return io.ErrShortWrite
		}

		data = data[n:]
	}

	return nil
}

// Append writes one complete transaction and synchronizes it.
// A successful return means File.Sync() completed successfully.
func (w *WAL) Append(tx Transaction) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return ErrWALClosed
	}

	if w.failed {
		return ErrWALFailed
	}

	frame, err := EncodeTransaction(tx)
	if err != nil {
		return err
	}

	if int64(len(frame)) > math.MaxInt64-w.size {
		return ErrFileLimit
	}

	newSize := w.size + int64(len(frame))

	if w.maxBytes > 0 && newSize > w.maxBytes {
		return ErrFileLimit
	}

	start := w.size

	if _, err := w.file.Seek(start, io.SeekStart); err != nil {
		return err
	}

	if err := writeFull(w.file, frame); err != nil {
		return w.rollbackAppend(start, err)
	}

	if err := w.file.Sync(); err != nil {
		return w.rollbackAppend(start, err)
	}

	w.size = newSize
	return nil
}

// rollbackAppend removes a failed, unacknowledged tail.
func (w *WAL) rollbackAppend(start int64, cause error) error {
	if err := w.file.Truncate(start); err != nil {
		w.failed = true
		return fmt.Errorf("append failed: %v; rollback failed: %w",
			cause, err)
	}

	if err := w.file.Sync(); err != nil {
		w.failed = true
		return fmt.Errorf("append failed: %v; rollback sync failed: %w",
			cause, err)
	}

	if _, err := w.file.Seek(start, io.SeekStart); err != nil {
		w.failed = true
		return fmt.Errorf("append failed: %v; seek rollback failed: %w",
			cause, err)
	}

	w.size = start
	return cause
}

// Recover scans the file and rebuilds the transaction history.
// An incomplete or invalid final frame is discarded.
// Corruption before the final frame is reported.
func (w *WAL) recover(repairTail bool) ([]RecoveredTransaction, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return nil, ErrWALClosed
	}

	info, err := w.file.Stat()
	if err != nil {
		return nil, err
	}

	fileSize := info.Size()
	offset := int64(FileHeaderSize)

	var transactions []RecoveredTransaction

	for offset < fileSize {
		remaining := fileSize - offset

		// Incomplete frame header at the end.
		if remaining < FrameHeaderSize {
			if !repairTail {
				return nil, fmt.Errorf(
					"%w: incomplete frame header at offset %d",
					ErrDatabaseCorrupt, offset,
				)
			}

			if err := w.truncateTail(offset); err != nil {
				return nil, err
			}
			fileSize = offset
			break
		}

		frameHeader := make([]byte, FrameHeaderSize)

		if _, err := w.file.ReadAt(frameHeader, offset); err != nil {
			return nil, err
		}

		if string(frameHeader[:4]) != FrameMagic {
			return nil, fmt.Errorf(
				"%w: invalid frame magic at offset %d",
				ErrDatabaseCorrupt, offset,
			)
		}

		payloadLength := binary.LittleEndian.Uint32(frameHeader[4:8])

		// Incomplete frame payload at the end.
		if uint64(payloadLength) > uint64(remaining-FrameHeaderSize) {
			if !repairTail {
				return nil, fmt.Errorf(
					"%w: incomplete frame payload at offset %d",
					ErrDatabaseCorrupt, offset,
				)
			}

			if err := w.truncateTail(offset); err != nil {
				return nil, err
			}
			fileSize = offset
			break
		}

		// Guard against unreasonable allocations.
		if uint64(payloadLength) > uint64(math.MaxInt) {
			return nil, fmt.Errorf(
				"%w: frame too large",
				ErrDatabaseCorrupt,
			)
		}

		frameLength := int64(FrameHeaderSize) + int64(payloadLength)
		frameEnd := offset + frameLength

		frame := make([]byte, int(frameLength))

		if _, err := w.file.ReadAt(frame, offset); err != nil {
			return nil, err
		}

		tx, decodeErr := DecodeTransaction(frame)

		if decodeErr != nil {
			// A bad final frame can be an interrupted append.
			if frameEnd == fileSize {
				if !repairTail {
					return nil, fmt.Errorf(
						"%w: invalid final frame at offset %d: %v",
						ErrDatabaseCorrupt, offset, decodeErr,
					)
				}

				if err := w.truncateTail(offset); err != nil {
					return nil, err
				}
				fileSize = offset
				break
			}

			return nil, fmt.Errorf(
				"%w at offset %d: %v",
				ErrDatabaseCorrupt, offset, decodeErr,
			)
		}

		transactions = append(transactions, RecoveredTransaction{
			Offset:      offset,
			Transaction: tx,
		})

		offset = frameEnd
	}

	w.size = fileSize
	return transactions, nil
}

// Recover repairs an incomplete final record when necessary.
func (w *WAL) Recover() ([]RecoveredTransaction, error) {
	return w.recover(true)
}

// Inspect reads transactions without modifying the WAL.
func (w *WAL) Inspect() ([]RecoveredTransaction, error) {
	return w.recover(false)
}

// ReadAt reads and decodes one transaction at the given file offset.
func (w *WAL) ReadAt(offset int64) (Transaction, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	var tx Transaction

	if w.file == nil {
		return tx, ErrWALClosed
	}

	if w.failed {
		return tx, ErrWALFailed
	}

	if offset < FileHeaderSize || offset >= w.size {
		return tx, ErrInvalidOffset
	}

	header := make([]byte, FrameHeaderSize)

	if _, err := w.file.ReadAt(header, offset); err != nil {
		return tx, err
	}

	if string(header[:4]) != FrameMagic {
		return tx, ErrDatabaseCorrupt
	}

	payloadLength := binary.LittleEndian.Uint32(header[4:8])

	if uint64(payloadLength) > uint64(math.MaxInt) {
		return tx, ErrDatabaseCorrupt
	}

	frameLength := int64(FrameHeaderSize) + int64(payloadLength)

	if frameLength > w.size-offset {
		return tx, ErrDatabaseCorrupt
	}

	frame := make([]byte, int(frameLength))

	if _, err := w.file.ReadAt(frame, offset); err != nil {
		return tx, err
	}

	return DecodeTransaction(frame)
}

// truncateTail removes an incomplete or invalid final frame.
func (w *WAL) truncateTail(offset int64) error {
	if err := w.file.Truncate(offset); err != nil {
		return err
	}

	if err := w.file.Sync(); err != nil {
		return err
	}

	w.size = offset
	return nil
}

// Size returns the current database file size.
func (w *WAL) Size() (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return 0, ErrWALClosed
	}

	return w.size, nil
}

// Close closes the underlying database file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return nil
	}

	err := w.file.Close()
	w.file = nil
	return err
}
