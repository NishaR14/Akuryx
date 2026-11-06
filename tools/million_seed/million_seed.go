package main

import (
	"fmt"
	"log"

	"Akuryx/internal/engine"
	"Akuryx/internal/storage"
)

func main() {
    db, err := engine.Open("akuryx-crash-million.db", 0)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    const total = 1_000_000
    const batchSize = 10_000

    for start := 0; start < total; start += batchSize {
        ops := make([]storage.Operation, 0, batchSize)

        for i := start; i < start+batchSize; i++ {
            ops = append(ops, storage.Operation{
                Type:  storage.OpPut,
                Key:   fmt.Sprintf("base-%07d", i),
                Value: []byte("baseline-data"),
            })
        }

        if err := db.Batch(ops); err != nil {
            log.Fatalf("batch failed at %d: %v", start, err)
        }

        if (start+batchSize)%100000 == 0 {
            fmt.Printf("Created %d / %d keys\n", start+batchSize, total)
        }
    }
}
