package engine

import (
	"Akuryx/internal/storage"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"time"

	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestPutAndGet(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	value := []byte("hello akuryx")

	if err := db.Put("greeting", value); err != nil {
		t.Fatal(err)
	}

	got, err := db.Get("greeting")
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(got, value) {
		t.Fatalf("expected %q, got %q", value, got)
	}
}

func TestDelete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put("fruit", []byte("apple")); err != nil {
		t.Fatal(err)
	}

	if err := db.Delete("fruit"); err != nil {
		t.Fatal(err)
	}

	_, err = db.Get("fruit")
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestBatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Batch([]storage.Operation{
		{Type: storage.OpPut, Key: "one", Value: []byte("first")},
		{Type: storage.OpPut, Key: "two", Value: []byte("second")},
	})

	if err != nil {
		t.Fatal(err)
	}

	for key, expected := range map[string]string{
		"one": "first",
		"two": "second",
	} {
		got, err := db.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != expected {
			t.Fatalf("%s: expected %q, got %q", key, expected, got)
		}
	}
}

func TestPersistenceAfterReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Put("name", []byte("Akuryx")); err != nil {
		t.Fatal(err)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	got, err := db.Get("name")
	if err != nil {
		t.Fatal(err)
	}

	if string(got) != "Akuryx" {
		t.Fatalf("expected Akuryx, got %s", got)
	}
}

func TestMixedBatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put("old", []byte("value")); err != nil {
		t.Fatal(err)
	}

	err = db.Batch([]storage.Operation{
		{Type: storage.OpPut, Key: "new", Value: []byte("hello")},
		{Type: storage.OpDelete, Key: "old"},
	})

	if err != nil {
		t.Fatal(err)
	}

	got, err := db.Get("new")
	if err != nil || string(got) != "hello" {
		t.Fatalf("unexpected new value: %q, error: %v", got, err)
	}

	_, err = db.Get("old")
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected old key to be deleted, got: %v", err)
	}
}

func TestFileSizeLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "limited.db")

	db, err := Open(path, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Put("key", []byte("this value is deliberately too large for the database limit"))

	if !errors.Is(err, storage.ErrFileLimit) {
		t.Fatalf("expected ErrFileLimit, got %v", err)
	}

	_, err = db.Get("key")
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected key to remain absent, got %v", err)
	}
}

func TestExistingDataSurvivesFileLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "limited.db")

	db, err := Open(path, 100)
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Put("a", []byte("b")); err != nil {
		t.Fatal(err)
	}

	err = db.Put("another-key", []byte("a much larger value"))
	if !errors.Is(err, storage.ErrFileLimit) {
		t.Fatalf("expected ErrFileLimit, got %v", err)
	}

	got, err := db.Get("a")
	if err != nil {
		t.Fatalf("existing data became unreadable: %v", err)
	}

	if string(got) != "b" {
		t.Fatalf("expected b, got %s", got)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(path, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	got, err = db.Get("a")
	if err != nil || string(got) != "b" {
		t.Fatalf("data did not survive reopening: value=%q error=%v", got, err)
	}
}

func TestCompactionFileContents(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put("name", []byte("Harsh")); err != nil {
		t.Fatal(err)
	}

	if err := db.Put("age", []byte("21")); err != nil {
		t.Fatal(err)
	}

	if err := db.Put("name", []byte("Akuryx")); err != nil {
		t.Fatal(err)
	}

	if err := db.Delete("age"); err != nil {
		t.Fatal(err)
	}

	transactions, err := db.buildCompactionTransactions()
	if err != nil {
		t.Fatal(err)
	}

	if len(transactions) != 1 {
		t.Fatalf("expected 1 transaction, got %d", len(transactions))
	}

	ops := transactions[0].Operations
	if len(ops) != 1 {
		t.Fatalf("expected 1 live operation, got %d", len(ops))
	}

	if ops[0].Key != "name" || string(ops[0].Value) != "Akuryx" {
		t.Fatalf("unexpected compacted operation: %+v", ops[0])
	}
}

func TestWriteCompactionFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put("name", []byte("Harsh")); err != nil {
		t.Fatal(err)
	}

	if err := db.Put("name", []byte("Akuryx")); err != nil {
		t.Fatal(err)
	}

	transactions, err := db.buildCompactionTransactions()
	if err != nil {
		t.Fatal(err)
	}

	tempWAL, recovered, err := db.writeCompactionFile(transactions)
	if err != nil {
		t.Fatal(err)
	}
	defer tempWAL.Close()

	if len(recovered) != 1 {
		t.Fatalf("expected 1 recovered transaction, got %d", len(recovered))
	}

	if len(recovered[0].Transaction.Operations) != 1 {
		t.Fatal("expected exactly one operation in compacted file")
	}

	op := recovered[0].Transaction.Operations[0]

	if op.Key != "name" || string(op.Value) != "Akuryx" {
		t.Fatalf("unexpected compacted record: %+v", op)
	}
}

func TestWriteCompactionFilePreservesExistingTempFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create a temporary compaction file with recognizable contents.
	tempPath := db.compactionPath()
	originalContents := []byte("previous compaction data")

	if err := os.WriteFile(tempPath, originalContents, 0600); err != nil {
		t.Fatal(err)
	}

	// A new compaction must refuse to overwrite the existing file.
	transactions := []storage.Transaction{
		{
			ID: 1,
			Operations: []storage.Operation{
				{
					Type:  storage.OpPut,
					Key:   "name",
					Value: []byte("Akuryx"),
				},
			},
		},
	}

	_, _, err = db.writeCompactionFile(transactions)
	if err == nil {
		t.Fatal("expected compaction to reject an existing temporary file")
	}

	// Verify that the original temporary file remains unchanged.
	got, err := os.ReadFile(tempPath)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(got, originalContents) {
		t.Fatalf("temporary file was overwritten: got %q", got)
	}
}
func TestCompactionReducesFileSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 100; i++ {
		if err := db.Put("name", []byte("old-value")); err != nil {
			t.Fatal(err)
		}
	}

	operations, err := db.liveOperations()
	if err != nil {
		t.Fatal(err)
	}

	if len(operations) != 1 {
		t.Fatalf("expected 1 live operation, got %d", len(operations))
	}

	transactions, err := db.buildCompactionTransactions()
	if err != nil {
		t.Fatal(err)
	}

	tempWAL, _, err := db.writeCompactionFile(transactions)
	if err != nil {
		t.Fatal(err)
	}
	defer tempWAL.Close()

	originalSize, err := db.wal.Size()
	if err != nil {
		t.Fatal(err)
	}

	compactedSize, err := tempWAL.Size()
	if err != nil {
		t.Fatal(err)
	}

	if compactedSize >= originalSize {
		t.Fatalf(
			"expected compacted file to be smaller: original=%d, compacted=%d",
			originalSize,
			compactedSize,
		)
	}
}

func TestCompactionRespectsFileSizeLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "limited.db")

	db, err := Open(path, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put("a", []byte("b")); err != nil {
		t.Fatal(err)
	}

	transactions, err := db.buildCompactionTransactions()
	if err != nil {
		t.Fatal(err)
	}

	tempWAL, _, err := db.writeCompactionFile(transactions)
	if err != nil {
		t.Fatal(err)
	}
	defer tempWAL.Close()

	size, err := tempWAL.Size()
	if err != nil {
		t.Fatal(err)
	}

	if size > 100 {
		t.Fatalf("compacted file exceeds limit: size=%d, limit=100", size)
	}
}

func TestCompactPreservesOriginalDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put("name", []byte("Harsh")); err != nil {
		t.Fatal(err)
	}

	if err := db.Put("name", []byte("Akuryx")); err != nil {
		t.Fatal(err)
	}

	if err := db.Put("city", []byte("Mangaluru")); err != nil {
		t.Fatal(err)
	}

	if err := db.Delete("city"); err != nil {
		t.Fatal(err)
	}

	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}

	got, err := db.Get("name")
	if err != nil {
		t.Fatal(err)
	}

	if string(got) != "Akuryx" {
		t.Fatalf("expected Akuryx, got %s", got)
	}

	_, err = db.Get("city")
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected city to be deleted, got %v", err)
	}
}

func TestCompactionFileSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Put("name", []byte("Harsh")); err != nil {
		t.Fatal(err)
	}

	if err := db.Put("name", []byte("Akuryx")); err != nil {
		t.Fatal(err)
	}

	transactions, err := db.buildCompactionTransactions()
	if err != nil {
		t.Fatal(err)
	}

	tempWAL, _, err := db.writeCompactionFile(transactions)
	if err != nil {
		t.Fatal(err)
	}

	if err := tempWAL.Close(); err != nil {
		t.Fatal(err)
	}

	tempWAL, recovered, err := storage.OpenWAL(db.compactionPath(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer tempWAL.Close()

	if len(recovered) != 1 {
		t.Fatalf("expected 1 transaction after reopen, got %d", len(recovered))
	}

	op := recovered[0].Transaction.Operations[0]
	if op.Key != "name" || string(op.Value) != "Akuryx" {
		t.Fatalf("unexpected recovered value: %+v", op)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}
func TestValidateCompactionFileRejectsCorruptedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put("name", []byte("Akuryx")); err != nil {
		t.Fatal(err)
	}

	createCompactionMarkerForTest(t, db)

	// Corrupt the compacted database after its marker was written.
	file, err := os.OpenFile(db.compactionPath(), os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := file.WriteAt([]byte("X"), 0); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}

	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	if err := db.validateCompactionFile(); err == nil {
		t.Fatal("expected corrupted compacted file to be rejected")
	}
}

func TestReopenWALRebuildsIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put("name", []byte("Akuryx")); err != nil {
		t.Fatal(err)
	}

	// Remove the in-memory index entry to simulate a stale index.
	db.index.Delete("name")

	if _, ok := db.index.Get("name"); ok {
		t.Fatal("expected index entry to be absent")
	}

	// Close the existing WAL before reopening it.
	if err := db.wal.Close(); err != nil {
		t.Fatal(err)
	}

	if err := db.reopenWAL(); err != nil {
		t.Fatal(err)
	}

	if _, ok := db.index.Get("name"); !ok {
		t.Fatal("expected reopening WAL to rebuild the index")
	}
}
func TestBackupPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	expected := path + ".backup"

	if got := db.backupPath(); got != expected {
		t.Fatalf("expected backup path %q, got %q", expected, got)
	}
}
func TestFileExists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test-file")

	// A file that does not exist should return false.
	exists, err := fileExists(path)
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("expected file not to exist")
	}

	// Create the file.
	if err := os.WriteFile(path, []byte("test"), 0600); err != nil {
		t.Fatal(err)
	}

	// An existing file should return true.
	exists, err = fileExists(path)
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("expected file to exist")
	}
}
func TestRemoveIfExists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test-file")

	// Removing a file that does not exist should not return an error.
	if err := removeIfExists(path); err != nil {
		t.Fatalf("unexpected error for missing file: %v", err)
	}

	// Create the file.
	if err := os.WriteFile(path, []byte("test"), 0600); err != nil {
		t.Fatal(err)
	}

	// Remove the existing file.
	if err := removeIfExists(path); err != nil {
		t.Fatal(err)
	}

	// Confirm that the file is gone.
	exists, err := fileExists(path)
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("expected file to be removed")
	}
}
func TestInspectWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Put("name", []byte("Akuryx")); err != nil {
		t.Fatal(err)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	transactions, err := inspectWAL(path)
	if err != nil {
		t.Fatal(err)
	}

	if len(transactions) != 1 {
		t.Fatalf("expected 1 transaction, got %d", len(transactions))
	}

	op := transactions[0].Transaction.Operations[0]

	if op.Key != "name" || string(op.Value) != "Akuryx" {
		t.Fatalf("unexpected recovered operation: %+v", op)
	}
}

func TestInspectWALMultipleTransactions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Put("name", []byte("Akuryx")); err != nil {
		t.Fatal(err)
	}

	if err := db.Put("city", []byte("Mangaluru")); err != nil {
		t.Fatal(err)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	transactions, err := inspectWAL(path)
	if err != nil {
		t.Fatal(err)
	}

	if len(transactions) != 2 {
		t.Fatalf("expected 2 transactions, got %d", len(transactions))
	}

	if transactions[0].Transaction.Operations[0].Key != "name" {
		t.Fatal("first transaction has an unexpected key")
	}

	if transactions[1].Transaction.Operations[0].Key != "city" {
		t.Fatal("second transaction has an unexpected key")
	}
}
func TestInspectWALMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.db")

	transactions, err := inspectWAL(path)

	if err == nil {
		t.Fatal("expected an error for a missing database file")
	}
	// Inspection must not create the missing file.
	exists, statErr := fileExists(path)
	if statErr != nil {
		t.Fatal(statErr)
	}
	if exists {
		t.Fatal("inspectWAL unexpectedly created the missing file")
	}
	if transactions != nil {
		t.Fatalf("expected no transactions, got %d", len(transactions))
	}
}
func TestBackupCanBeInspected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Put("name", []byte("Akuryx")); err != nil {
		t.Fatal(err)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	backupPath := path + ".backup"

	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(backupPath, original, 0600); err != nil {
		t.Fatal(err)
	}

	transactions, err := inspectWAL(backupPath)
	if err != nil {
		t.Fatal(err)
	}

	if len(transactions) != 1 {
		t.Fatalf("expected 1 transaction in backup, got %d", len(transactions))
	}

	op := transactions[0].Transaction.Operations[0]

	if op.Key != "name" || string(op.Value) != "Akuryx" {
		t.Fatalf("unexpected backup contents: %+v", op)
	}
}
func TestRecoverMissingMainFromBackup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	// Create a database with data.
	db, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Put("name", []byte("Akuryx")); err != nil {
		t.Fatal(err)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Copy the database to its backup path.
	backupPath := path + ".backup"

	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(backupPath, original, 0600); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash state where the main database is missing.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}

	// Reopening should restore the backup.
	recoveredDB, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer recoveredDB.Close()

	value, err := recoveredDB.Get("name")
	if err != nil {
		t.Fatal(err)
	}

	if string(value) != "Akuryx" {
		t.Fatalf("expected Akuryx, got %s", value)
	}
}
func TestRecoverRejectsInvalidBackup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	backupPath := path + ".backup"

	if err := os.WriteFile(backupPath, []byte("not a valid database"), 0600); err != nil {
		t.Fatal(err)
	}

	// Remove the main database to trigger backup recovery.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}

	_, err = Open(path, 0)
	if err == nil {
		t.Fatal("expected invalid backup to be rejected")
	}

	// The invalid backup must not be deleted.
	exists, err := fileExists(backupPath)
	if err != nil {
		t.Fatal(err)
	}

	if !exists {
		t.Fatal("invalid backup should not have been deleted")
	}
}
func TestRecoverCorruptedMainFromBackup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	// Create a valid database.
	db, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Put("name", []byte("Akuryx")); err != nil {
		t.Fatal(err)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Preserve a valid backup.
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	backupPath := path + ".backup"
	if err := os.WriteFile(backupPath, original, 0600); err != nil {
		t.Fatal(err)
	}

	// Corrupt the main database.
	if err := os.WriteFile(path, []byte("corrupted database"), 0600); err != nil {
		t.Fatal(err)
	}

	// Opening should recover from the valid backup.
	recoveredDB, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer recoveredDB.Close()

	value, err := recoveredDB.Get("name")
	if err != nil {
		t.Fatal(err)
	}

	if string(value) != "Akuryx" {
		t.Fatalf("expected Akuryx, got %s", value)
	}

	// Confirm that the corrupted original was preserved.
	damagedPath := path + ".damaged"

	exists, err := fileExists(damagedPath)
	if err != nil {
		t.Fatal(err)
	}

	if !exists {
		t.Fatal("expected corrupted original database to be preserved")
	}
}
func TestRecoverPreservesExistingDamagedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Put("name", []byte("Akuryx")); err != nil {
		t.Fatal(err)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(path+".backup", original, 0600); err != nil {
		t.Fatal(err)
	}

	// Create a corrupted main database.
	if err := os.WriteFile(path, []byte("corrupted"), 0600); err != nil {
		t.Fatal(err)
	}

	// Create an existing damaged file that must not be overwritten.
	damagedPath := path + ".damaged"
	previousDamaged := []byte("previous damaged database")

	if err := os.WriteFile(damagedPath, previousDamaged, 0600); err != nil {
		t.Fatal(err)
	}

	_, err = Open(path, 0)
	if err == nil {
		t.Fatal("expected recovery to reject an existing damaged-file destination")
	}

	got, err := os.ReadFile(damagedPath)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(got, previousDamaged) {
		t.Fatal("existing damaged file was overwritten")
	}
}
func TestRecoverKeepsValidMainDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Put("name", []byte("Original")); err != nil {
		t.Fatal(err)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Create a different valid backup.
	backupPath := path + ".backup"

	backupDB, err := Open(backupPath, 0)
	if err != nil {
		t.Fatal(err)
	}

	if err := backupDB.Put("name", []byte("Backup")); err != nil {
		t.Fatal(err)
	}

	if err := backupDB.Close(); err != nil {
		t.Fatal(err)
	}

	// Opening should preserve the valid main database.
	recoveredDB, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer recoveredDB.Close()

	value, err := recoveredDB.Get("name")
	if err != nil {
		t.Fatal(err)
	}

	if string(value) != "Original" {
		t.Fatalf("expected Original, got %s", value)
	}
}
func TestRecoverWithNoDatabaseOrBackup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "new.db")

	db, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put("hello", []byte("world")); err != nil {
		t.Fatal(err)
	}

	value, err := db.Get("hello")
	if err != nil {
		t.Fatal(err)
	}

	if string(value) != "world" {
		t.Fatalf("expected world, got %s", value)
	}
}

// createCompactionMarkerForTest creates a compacted file and its marker.
func createCompactionMarkerForTest(t *testing.T, db *DB) int64 {
	t.Helper()

	transactions, err := db.buildCompactionTransactions()
	if err != nil {
		t.Fatal(err)
	}

	tempWAL, _, err := db.writeCompactionFile(transactions)
	if err != nil {
		t.Fatal(err)
	}

	size, err := tempWAL.Size()
	if err != nil {
		_ = tempWAL.Close()
		t.Fatal(err)
	}

	if err := tempWAL.Close(); err != nil {
		t.Fatal(err)
	}

	if err := db.writeCompactionMarker(size); err != nil {
		t.Fatal(err)
	}

	return size
}

func TestWriteCompactionMarker(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put("name", []byte("Akuryx")); err != nil {
		t.Fatal(err)
	}

	createCompactionMarkerForTest(t, db)

	data, err := os.ReadFile(db.compactionMarkerPath())
	if err != nil {
		t.Fatal(err)
	}

	if len(data) != 24 {
		t.Fatalf("expected 24 marker bytes, got %d", len(data))
	}

	if string(data[:8]) != "AKCMP003" {
		t.Fatalf("unexpected marker signature: %q", data[:8])
	}

	// Verify the marker's own checksum.
	expectedMarkerCRC := binary.LittleEndian.Uint32(data[20:24])
	actualMarkerCRC := crc32.ChecksumIEEE(data[:20])

	if expectedMarkerCRC != actualMarkerCRC {
		t.Fatal("marker checksum is incorrect")
	}

	// Verify the compacted file's size matches the marker.
	expectedSize := binary.LittleEndian.Uint64(data[8:16])

	info, err := os.Stat(db.compactionPath())
	if err != nil {
		t.Fatal(err)
	}

	if uint64(info.Size()) != expectedSize {
		t.Fatalf(
			"marker size mismatch: expected %d, got %d",
			expectedSize,
			info.Size(),
		)
	}
}

func TestValidateCompactionMarker(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put("name", []byte("Akuryx")); err != nil {
		t.Fatal(err)
	}

	expectedSize := createCompactionMarkerForTest(t, db)

	size, err := db.validateCompactionMarker()
	if err != nil {
		t.Fatalf("valid marker rejected: %v", err)
	}

	if size != expectedSize {
		t.Fatalf("expected size %d, got %d", expectedSize, size)
	}
}
func TestValidateCompactionMarkerMissingCompactionFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put("name", []byte("Akuryx")); err != nil {
		t.Fatal(err)
	}

	// Create a valid compacted file and its marker.
	createCompactionMarkerForTest(t, db)

	// Remove only the compacted file, leaving the marker behind.
	if err := os.Remove(db.compactionPath()); err != nil {
		t.Fatal(err)
	}

	// Validation must reject a marker without its compacted file.
	_, err = db.validateCompactionMarker()
	if err == nil {
		t.Fatal("expected validation to fail when compacted file is missing")
	}
}
func TestValidateCorruptedCompactionMarkerChecksum(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put("name", []byte("Akuryx")); err != nil {
		t.Fatal(err)
	}

	createCompactionMarkerForTest(t, db)

	markerPath := db.compactionMarkerPath()

	data, err := os.ReadFile(markerPath)
	if err != nil {
		t.Fatal(err)
	}

	// Change the stored checksum without updating the marker.
	data[20] ^= 0xFF

	if err := os.WriteFile(markerPath, data, 0600); err != nil {
		t.Fatal(err)
	}

	if _, err := db.validateCompactionMarker(); err == nil {
		t.Fatal("expected corrupted marker checksum to be rejected")
	}
}
func TestValidateCorruptedCompactionFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put("name", []byte("Akuryx")); err != nil {
		t.Fatal(err)
	}

	createCompactionMarkerForTest(t, db)

	compactPath := db.compactionPath()

	file, err := os.OpenFile(compactPath, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}

	// Change one byte in the compacted database.
	if _, err := file.WriteAt([]byte("X"), 0); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}

	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := db.validateCompactionMarker(); err == nil {
		t.Fatal("expected corrupted compacted file to be rejected")
	}
}
func TestCompactActuallyReducesDatabaseSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		if err := db.Put("name", []byte("Akuryx")); err != nil {
			t.Fatal(err)
		}
	}

	before, err := db.wal.Size()
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Compact(); err != nil {
		t.Fatalf("compaction failed: %v", err)
	}

	after, err := db.wal.Size()
	if err != nil {
		t.Fatal(err)
	}

	if after >= before {
		t.Fatalf("database did not shrink: before=%d after=%d", before, after)
	}

	value, err := db.Get("name")
	if err != nil {
		t.Fatal(err)
	}
	if string(value) != "Akuryx" {
		t.Fatalf("expected Akuryx, got %s", value)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Verify persistence after a complete restart.
	db, err = Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	value, err = db.Get("name")
	if err != nil || string(value) != "Akuryx" {
		t.Fatalf("reopened database returned %q, error: %v", value, err)
	}
}
func TestRecoverAfterCompactionPromotion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	// Create the new main database.
	db, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Put("name", []byte("NewValue")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Create an older backup with a different value.
	backup, err := Open(path+".backup", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := backup.Put("name", []byte("OldValue")); err != nil {
		t.Fatal(err)
	}
	if err := backup.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate restarting after the new main was promoted.
	db, err = Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	value, err := db.Get("name")
	if err != nil {
		t.Fatal(err)
	}
	if string(value) != "NewValue" {
		t.Fatalf("expected NewValue, got %s", value)
	}

	if exists, err := fileExists(path + ".backup"); err != nil {
		t.Fatal(err)
	} else if exists {
		t.Fatal("stale backup was not removed")
	}
}

func TestCrashRecoveryAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "crash-test.db")

	// Process 1: write data and close.
	db, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Put("before-crash", []byte("must-survive")); err != nil {
		t.Fatal(err)
	}
	if err := db.Put("latest", []byte("version-2")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Process 2: simulate restart by opening the same database.
	db, err = Open(path, 0)
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	defer db.Close()

	value, err := db.Get("before-crash")
	if err != nil {
		t.Fatal(err)
	}
	if string(value) != "must-survive" {
		t.Fatalf("unexpected recovered value: %s", value)
	}

	value, err = db.Get("latest")
	if err != nil {
		t.Fatal(err)
	}
	if string(value) != "version-2" {
		t.Fatalf("unexpected latest value: %s", value)
	}
}

func TestOpenRepairsOneByteTruncatedFinalFrame(t *testing.T) {
	path := filepath.Join(t.TempDir(), "truncated.db")

	db, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Put("first", []byte("preserved")); err != nil {
		t.Fatal(err)
	}

	if err := db.Put("second", []byte("possibly-torn")); err != nil {
		t.Fatal(err)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	// Remove the final byte to simulate a lost tail.
	if err := os.Truncate(path, info.Size()-1); err != nil {
		t.Fatal(err)
	}

	db, err = Open(path, 0)
	if err != nil {
		t.Fatalf("open should repair truncated tail: %v", err)
	}
	defer db.Close()

	value, err := db.Get("first")
	if err != nil {
		t.Fatalf("first key should survive: %v", err)
	}

	if string(value) != "preserved" {
		t.Fatalf("unexpected value: %s", value)
	}
}

func TestRecoverValidMainBeforeRemovingBackup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "recovery.db")

	db, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Put("winner", []byte("main")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Create a different, valid backup.
	backup, err := Open(path+".backup", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := backup.Put("winner", []byte("backup")); err != nil {
		t.Fatal(err)
	}
	if err := backup.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	value, err := db.Get("winner")
	if err != nil {
		t.Fatal(err)
	}

	if string(value) != "main" {
		t.Fatalf("expected main database to win, got %q", value)
	}

	exists, err := fileExists(path + ".backup")
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("stale backup should have been removed")
	}
}

func TestConcurrentWriters(t *testing.T) {
	path := filepath.Join(t.TempDir(), "concurrent.db")

	db, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const (
		writers       = 10
		operations    = 50
		expectedTotal = writers * operations
	)

	var wg sync.WaitGroup
	errCh := make(chan error, writers)

	for w := 0; w < writers; w++ {
		wg.Add(1)

		go func(writerID int) {
			defer wg.Done()

			for i := 0; i < operations; i++ {
				key := fmt.Sprintf("writer-%d-key-%d", writerID, i)
				value := []byte(fmt.Sprintf("value-%d-%d", writerID, i))

				if err := db.Put(key, value); err != nil {
					errCh <- fmt.Errorf("writer %d: %w", writerID, err)
					return
				}
			}
		}(w)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}

	count, err := db.Count()
	if err != nil {
		t.Fatal(err)
	}

	if count != expectedTotal {
		t.Fatalf("expected %d keys, got %d", expectedTotal, count)
	}

	for w := 0; w < writers; w++ {
		for i := 0; i < operations; i++ {
			key := fmt.Sprintf("writer-%d-key-%d", w, i)
			expected := fmt.Sprintf("value-%d-%d", w, i)

			got, err := db.Get(key)
			if err != nil {
				t.Fatalf("could not read %s: %v", key, err)
			}

			if string(got) != expected {
				t.Fatalf("%s: expected %q, got %q", key, expected, got)
			}
		}
	}
}

func TestConcurrentReadersAndWriters(t *testing.T) {
	path := filepath.Join(t.TempDir(), "concurrent-read-write.db")

	db, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const (
		writers    = 4
		readers    = 4
		operations = 50
	)

	var wg sync.WaitGroup
	errCh := make(chan error, writers+readers)

	// Start writers.
	for w := 0; w < writers; w++ {
		wg.Add(1)

		go func(writerID int) {
			defer wg.Done()

			for i := 0; i < operations; i++ {
				key := fmt.Sprintf("writer-%d-key-%d", writerID, i)
				value := []byte(fmt.Sprintf("value-%d-%d", writerID, i))

				if err := db.Put(key, value); err != nil {
					errCh <- fmt.Errorf("writer %d: %w", writerID, err)
					return
				}
			}
		}(w)
	}

	// Start readers while writers are running.
	for r := 0; r < readers; r++ {
		wg.Add(1)

		go func(readerID int) {
			defer wg.Done()

			for i := 0; i < operations*2; i++ {
				key := fmt.Sprintf("writer-%d-key-%d",
					i%writers, i%operations)

				_, err := db.Get(key)

				// A key may not have been written yet.
				if err != nil && !errors.Is(err, ErrKeyNotFound) {
					errCh <- fmt.Errorf("reader %d: %w", readerID, err)
					return
				}
			}
		}(r)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}

	count, err := db.Count()
	if err != nil {
		t.Fatal(err)
	}

	expected := writers * operations
	if count != expected {
		t.Fatalf("expected %d keys, got %d", expected, count)
	}
}

func TestBatchVisibilityIsAtomic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "atomic-batch.db")

	db, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	keys := []string{"account1", "account2", "account3"}

	initial := make([]storage.Operation, 0, len(keys))
	for _, key := range keys {
		initial = append(initial, storage.Operation{
			Type:  storage.OpPut,
			Key:   key,
			Value: []byte("OLD"),
		})
	}

	if err := db.Batch(initial); err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	errCh := make(chan error, 2)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start

		for i := 0; i < 100; i++ {
			value := "NEW"
			if i%2 == 0 {
				value = "OLD"
			}

			operations := make([]storage.Operation, 0, len(keys))
			for _, key := range keys {
				operations = append(operations, storage.Operation{
					Type:  storage.OpPut,
					Key:   key,
					Value: []byte(value),
				})
			}

			if err := db.Batch(operations); err != nil {
				errCh <- fmt.Errorf("batch write: %w", err)
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start

		for i := 0; i < 300; i++ {
			for _, key := range keys {
				value, err := db.Get(key)
				if err != nil {
					errCh <- fmt.Errorf("get %s: %w", key, err)
					return
				}

				if string(value) != "OLD" && string(value) != "NEW" {
					errCh <- fmt.Errorf(
						"invalid value for %s: %q",
						key, value,
					)
					return
				}
			}
		}
	}()

	close(start)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}
}

func TestMillionKeysOpenBenchmark(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping million-key benchmark in short mode")
	}

	path := filepath.Join(t.TempDir(), "million.db")

	db, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}

	const totalKeys = 1_000_000

	t.Logf("Writing %d keys...", totalKeys)

	startWrite := time.Now()

	const batchSize = 10_000

	for start := 0; start < totalKeys; start += batchSize {
		end := start + batchSize
		if end > totalKeys {
			end = totalKeys
		}

		operations := make([]storage.Operation, 0, end-start)

		for i := start; i < end; i++ {
			operations = append(operations, storage.Operation{
				Type:  storage.OpPut,
				Key:   fmt.Sprintf("key-%07d", i),
				Value: []byte("benchmark-value"),
			})
		}

		if err := db.Batch(operations); err != nil {
			t.Fatalf("batch failed at keys %d-%d: %v", start, end-1, err)
		}

		if end%100_000 == 0 {
			t.Logf("Created %d / %d keys", end, totalKeys)
		}
	}

	writeDuration := time.Since(startWrite)
	t.Logf("Writing took: %v", writeDuration)

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	startOpen := time.Now()

	db, err = Open(path, 0)
	openDuration := time.Since(startOpen)

	if err != nil {
		t.Fatalf("reopening database failed: %v", err)
	}
	defer db.Close()

	t.Logf("Opening database with 1 million keys took: %v", openDuration)
	t.Logf("Required time: less than 2 seconds")

	if openDuration >= 2*time.Second {
		t.Errorf("FAIL: opening took %v; target is under 2 seconds", openDuration)
	}
}

func TestInterruptedBatchRecoveryIsAllOrNothing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "interrupted-batch.db")

	db, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}

	// This acknowledged write must survive.
	if err := db.Put("safe", []byte("preserved")); err != nil {
		t.Fatal(err)
	}

	// Write one batch containing three operations.
	batch := []storage.Operation{
		{Type: storage.OpPut, Key: "batch1", Value: []byte("one")},
		{Type: storage.OpPut, Key: "batch2", Value: []byte("two")},
		{Type: storage.OpPut, Key: "batch3", Value: []byte("three")},
	}

	if err := db.Batch(batch); err != nil {
		t.Fatal(err)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate an interrupted final batch append.
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := os.Truncate(path, info.Size()-1); err != nil {
		t.Fatal(err)
	}

	// Reopen and recover.
	db, err = Open(path, 0)
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	defer db.Close()

	// The earlier acknowledged write must remain.
	value, err := db.Get("safe")
	if err != nil {
		t.Fatalf("acknowledged write was lost: %v", err)
	}
	if string(value) != "preserved" {
		t.Fatalf("unexpected safe value: %q", value)
	}

	// No operation from the interrupted batch may be visible.
	for _, key := range []string{"batch1", "batch2", "batch3"} {
		_, err := db.Get(key)
		if !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("interrupted batch was partially recovered: key=%s err=%v", key, err)
		}
	}
}
