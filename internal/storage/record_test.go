package storage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestTransactionEncodeDecode(t *testing.T) {

	original := Transaction{
		ID: 1,
		Operations: []Operation{
			{
				Type:  OpPut,
				Key:   "101",
				Value: []byte("Sajan"),
			},
			{
				Type:  OpPut,
				Key:   "102",
				Value: []byte("Rakshith"),
			},
			{
				Type: OpDelete,
				Key:  "103",
			},
		},
	}

	// Encode transaction.
	encoded, err := EncodeTransaction(original)

	if err != nil {
		t.Fatalf("Encoding failed: %v", err)
	}

	// Decode transaction.
	decoded, err := DecodeTransaction(encoded)

	if err != nil {
		t.Fatalf("Decoding failed: %v", err)
	}

	// Verify transaction ID.
	if decoded.ID != original.ID {
		t.Fatal("Transaction ID mismatch")
	}

	// Verify operation count.
	if len(decoded.Operations) != len(original.Operations) {
		t.Fatal("Operation count mismatch")
	}

	// Verify every operation.
	for i, op := range original.Operations {

		got := decoded.Operations[i]

		if got.Type != op.Type {
			t.Fatalf("Operation type mismatch at %d", i)
		}

		if got.Key != op.Key {
			t.Fatalf("Key mismatch at %d", i)
		}

		if !bytes.Equal(got.Value, op.Value) {
			t.Fatalf("Value mismatch at %d", i)
		}
	}
}

func TestChecksumValidation(t *testing.T) {

	tx := Transaction{
		ID: 2,
		Operations: []Operation{
			{
				Type:  OpPut,
				Key:   "name",
				Value: []byte("Akuryx"),
			},
		},
	}

	encoded, err := EncodeTransaction(tx)

	if err != nil {
		t.Fatal(err)
	}

	// Change one payload byte.
	encoded[len(encoded)-1] ^= 0xFF

	_, err = DecodeTransaction(encoded)

	if err == nil {
		t.Fatal("Expected checksum validation to fail")
	}
}

func TestIncompleteFrame(t *testing.T) {

	tx := Transaction{
		ID: 3,
		Operations: []Operation{
			{
				Type:  OpPut,
				Key:   "key",
				Value: []byte("value"),
			},
		},
	}

	encoded, err := EncodeTransaction(tx)

	if err != nil {
		t.Fatal(err)
	}

	// Remove the final bytes.
	encoded = encoded[:len(encoded)-2]

	_, err = DecodeTransaction(encoded)

	if err == nil {
		t.Fatal("Expected incomplete frame error")
	}
}

func TestEmptyTransaction(t *testing.T) {

	tx := Transaction{
		ID:         4,
		Operations: nil,
	}

	_, err := EncodeTransaction(tx)

	if err == nil {
		t.Fatal("Expected empty transaction to fail")
	}
}
func TestOpenWALRejectsIncompleteHeader(t *testing.T) {
	path := t.TempDir() + "/damaged.db"

	// Create a file containing an incomplete database header.
	if err := os.WriteFile(path, []byte("AKURYXDB"), 0600); err != nil {
		t.Fatal(err)
	}

	// Opening an existing file with an incomplete header must fail.
	wal, _, err := OpenWAL(path, 0)
	if err == nil {
		if wal != nil {
			_ = wal.Close()
		}
		t.Fatal("expected an incomplete database header to be rejected")
	}

	if !errors.Is(err, ErrDatabaseCorrupt) {
		t.Fatalf("expected ErrDatabaseCorrupt, got: %v", err)
	}

	// Verify the damaged file was not silently replaced.
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(got, []byte("AKURYXDB")) {
		t.Fatalf("damaged file was modified: got %q", got)
	}
}

func TestOpenWALReadOnlyRejectsIncompleteHeader(t *testing.T) {
	path := t.TempDir() + "/damaged.db"

	// Create a file containing an incomplete database header.
	if err := os.WriteFile(path, []byte("AKURYXDB"), 0600); err != nil {
		t.Fatal(err)
	}

	// Read-only opening must reject the incomplete header.
	wal, err := OpenWALReadOnly(path)
	if err == nil {
		if wal != nil {
			_ = wal.Close()
		}
		t.Fatal("expected an incomplete database header to be rejected")
	}

	if !errors.Is(err, ErrDatabaseCorrupt) {
		t.Fatalf("expected ErrDatabaseCorrupt, got: %v", err)
	}

	// Verify the damaged file was not modified.
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(got, []byte("AKURYXDB")) {
		t.Fatalf("damaged file was modified: got %q", got)
	}
}

func TestOpenWALReadOnlyDoesNotCreateFile(t *testing.T) {
	path := t.TempDir() + "/missing.db"

	wal, err := OpenWALReadOnly(path)
	if err == nil {
		if wal != nil {
			_ = wal.Close()
		}
		t.Fatal("expected an error for a missing database file")
	}

	if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("expected file to remain missing, got stat error: %v", statErr)
	}
}

func TestRecoverRejectsMiddleOfFileCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "middle-corruption.db")

	wal, _, err := OpenWAL(path, 0)
	if err != nil {
		t.Fatal(err)
	}

	makeOperation := func(key, value string) Transaction {
		return Transaction{
			Operations: []Operation{
				{
					Type:  OpPut,
					Key:   key,
					Value: []byte(value),
				},
			},
		}
	}

	if err := wal.Append(makeOperation("first", "one")); err != nil {
		t.Fatal(err)
	}

	if err := wal.Append(makeOperation("second", "two")); err != nil {
		t.Fatal(err)
	}

	if err := wal.Append(makeOperation("third", "three")); err != nil {
		t.Fatal(err)
	}

	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}

	// Corrupt the first transaction's payload.
	if _, err := file.WriteAt([]byte{0xFF}, FileHeaderSize+FrameHeaderSize+12); err != nil {
		file.Close()
		t.Fatal(err)
	}

	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	_, _, err = OpenWAL(path, 0)
	if err == nil {
		t.Fatal("expected middle-of-file corruption to be rejected")
	}

	if !errors.Is(err, ErrDatabaseCorrupt) {
		t.Fatalf("expected ErrDatabaseCorrupt, got %v", err)
	}
}
