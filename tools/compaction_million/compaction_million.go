
package main

import (
	"fmt"
	"log"
	"os"

	"Akuryx/internal/engine"
	"Akuryx/internal/storage"
)

const (
	dbPath    = "akuryx-compaction-test.db"
	total     = 1_000_000
	batchSize = 10_000
)

func fileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		log.Fatalf("cannot read database file size: %v", err)
	}
	return info.Size()
}

func main() {
	// Start with a clean, separate benchmark database.
	for _, suffix := range []string{
		"", ".backup", ".compact", ".compact.marker", ".damaged",
	} {
		if err := os.Remove(dbPath + suffix); err != nil && !os.IsNotExist(err) {
			log.Fatalf("cannot remove %s: %v", dbPath+suffix, err)
		}
	}

	db, err := engine.Open(dbPath, 0)
	if err != nil {
		log.Fatalf("open failed: %v", err)
	}

	fmt.Println("PHASE 1: Inserting one million keys")

	for start := 0; start < total; start += batchSize {
		ops := make([]storage.Operation, 0, batchSize)

		for i := start; i < start+batchSize; i++ {
			ops = append(ops, storage.Operation{
				Type:  storage.OpPut,
				Key:   fmt.Sprintf("compact-%07d", i),
				Value: []byte("benchmark-data"),
			})
		}

		if err := db.Batch(ops); err != nil {
			log.Fatalf("insert batch at %d failed: %v", start, err)
		}

		if (start+batchSize)%100_000 == 0 {
			fmt.Printf("Inserted %d / %d\n", start+batchSize, total)
		}
	}

	count, err := db.Count()
	if err != nil {
		log.Fatalf("count after inserts failed: %v", err)
	}
	if count != total {
		log.Fatalf("expected %d keys after inserts, found %d", total, count)
	}

	fmt.Println("\nPHASE 2: Deleting one million keys")

	for start := 0; start < total; start += batchSize {
		ops := make([]storage.Operation, 0, batchSize)

		for i := start; i < start+batchSize; i++ {
			ops = append(ops, storage.Operation{
				Type: storage.OpDelete,
				Key:  fmt.Sprintf("compact-%07d", i),
			})
		}

		if err := db.Batch(ops); err != nil {
			log.Fatalf("delete batch at %d failed: %v", start, err)
		}

		if (start+batchSize)%100_000 == 0 {
			fmt.Printf("Deleted %d / %d\n", start+batchSize, total)
		}
	}

	count, err = db.Count()
	if err != nil {
		log.Fatalf("count after deletes failed: %v", err)
	}
	if count != 0 {
		log.Fatalf("expected zero keys after deletes, found %d", count)
	}

	before := fileSize(dbPath)
	fmt.Printf("\nFile size before compaction: %d bytes\n", before)

	fmt.Println("\nPHASE 3: Compacting database")

	if err := db.Compact(); err != nil {
		log.Fatalf("compaction failed: %v", err)
	}

	after := fileSize(dbPath)
	fmt.Printf("File size after compaction:  %d bytes\n", after)

	if err := db.Close(); err != nil {
		log.Fatalf("close failed: %v", err)
	}

	fmt.Println("\nPHASE 4: Reopening and verifying")

	db, err = engine.Open(dbPath, 0)
	if err != nil {
		log.Fatalf("reopen failed: %v", err)
	}
	defer db.Close()

	count, err = db.Count()
	if err != nil {
		log.Fatalf("count after reopen failed: %v", err)
	}
	if count != 0 {
		log.Fatalf("expected zero keys after reopen, found %d", count)
	}

	if after >= before {
		log.Fatalf("file did not shrink: before=%d after=%d", before, after)
	}

	fmt.Println("\nCOMPACTION TEST PASSED")
	fmt.Printf("Inserted: %d keys\n", total)
	fmt.Printf("Deleted:  %d keys\n", total)
	fmt.Printf("Keys remaining after reopen: %d\n", count)
	fmt.Printf("Space reclaimed: %d bytes\n", before-after)
}
