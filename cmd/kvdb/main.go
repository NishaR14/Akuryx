package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"Akuryx/internal/engine"
	"Akuryx/internal/storage"
)

type request struct {
	ID    string `json:"id"`
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value"`
	Batch []struct {
		Type  string `json:"type"`
		Key   string `json:"key"`
		Value string `json:"value"`
	} `json:"batch"`
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		usage()
		return nil
	}

	if args[0] == "open" {
		if len(args) != 2 && len(args) != 4 {
			return errors.New("usage: kvdb open <path> [--max-bytes N]")
		}

		path := args[1]
		maxBytes := int64(0)
		if len(args) == 4 {
			if args[2] != "--max-bytes" {
				return errors.New("expected --max-bytes N")
			}
			var err error
			maxBytes, err = parseLimit(args[3])
			if err != nil {
				return err
			}
		}

		start := time.Now()
		db, err := engine.Open(path, maxBytes)
		if err != nil {
			return err
		}
		defer db.Close()

		count, err := db.Count()
		if err != nil {
			return err
		}
		fmt.Printf("OK: database opened\nKeys: %d\nRecovery time: %s\n",
			count, time.Since(start))
		return nil
	}

	if len(args) < 2 {
		usage()
		return nil
	}

	path := args[0]
	args = args[1:]

	maxBytes := int64(0)
	// Accept --max-bytes before or after the command arguments.
	for i := 0; i < len(args); {
		if args[i] != "--max-bytes" {
			i++
			continue
		}
		if i+1 >= len(args) {
			return errors.New("--max-bytes requires a number")
		}
		if maxBytes != 0 {
			return errors.New("--max-bytes specified more than once")
		}
		var err error
		maxBytes, err = parseLimit(args[i+1])
		if err != nil {
			return err
		}
		args = append(args[:i], args[i+2:]...)
	}

	if len(args) == 0 {
		return errors.New("missing command")
	}

	command := args[0]
	commandArgs := args[1:]

	if command == "serve" {
		if len(commandArgs) != 0 {
			return errors.New("usage: kvdb <path> serve [--max-bytes N]")
		}
		return serve(path, maxBytes)
	}

	db, err := engine.Open(path, maxBytes)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	switch command {
	case "put":
		if len(commandArgs) != 2 {
			return errors.New("usage: kvdb <path> put <key> <value>")
		}
		if err := db.Put(commandArgs[0], []byte(commandArgs[1])); err != nil {
			return err
		}
		fmt.Println("OK: value stored")

	case "get":
		if len(commandArgs) != 1 {
			return errors.New("usage: kvdb <path> get <key>")
		}
		value, err := db.Get(commandArgs[0])
		if err != nil {
			return err
		}
		fmt.Println(string(value))

	case "del", "delete":
		if len(commandArgs) != 1 {
			return errors.New("usage: kvdb <path> del <key>")
		}
		if err := db.Delete(commandArgs[0]); err != nil {
			return err
		}
		fmt.Println("OK: key deleted")

	case "batch":
		if len(commandArgs) != 1 {
			return errors.New("usage: kvdb <path> batch <file>")
		}
		operations, err := readBatchFile(commandArgs[0])
		if err != nil {
			return err
		}
		if err := db.Batch(operations); err != nil {
			return err
		}
		fmt.Printf("OK: %d operations committed atomically\n", len(operations))

	case "dump":
		if len(commandArgs) != 0 {
			return errors.New("usage: kvdb <path> dump")
		}
		items, err := db.All()
		if err != nil {
			return err
		}
		sort.Slice(items, func(i, j int) bool {
			return items[i].Key < items[j].Key
		})
		for _, item := range items {
			fmt.Printf("%s\t%s\n", item.Key, item.Value)
		}
		fmt.Printf("Total keys: %d\n", len(items))

	case "check":
		if len(commandArgs) != 0 {
			return errors.New("usage: kvdb <path> check")
		}
		count, err := db.Count()
		if err != nil {
			return err
		}
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		fmt.Printf("OK: database opened and index rebuilt\nKeys: %d\nFile size: %d bytes\n",
			count, info.Size())

	case "compact":
		if len(commandArgs) != 0 {
			return errors.New("usage: kvdb <path> compact")
		}
		before, err := os.Stat(path)
		if err != nil {
			return err
		}
		if err := db.Compact(); err != nil {
			return err
		}
		after, err := os.Stat(path)
		if err != nil {
			return err
		}
		fmt.Printf("Compaction complete\nBefore: %d bytes\nAfter:  %d bytes\n",
			before.Size(), after.Size())

	case "batch-demo":
		if len(commandArgs) != 0 {
			return errors.New("usage: kvdb <path> batch-demo")
		}
		operations := []storage.Operation{
			{Type: storage.OpPut, Key: "user:1", Value: []byte("Alice")},
			{Type: storage.OpPut, Key: "user:2", Value: []byte("Bob")},
			{Type: storage.OpPut, Key: "user:3", Value: []byte("Charlie")},
		}
		if err := db.Batch(operations); err != nil {
			return err
		}
		fmt.Println("OK: all 3 operations committed in one batch")

	default:
		usage()
		return fmt.Errorf("unknown command: %s", command)
	}

	return nil
}

func parseLimit(value string) (int64, error) {
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil || n <= 0 {
		return 0, errors.New("--max-bytes must be a positive integer")
	}
	return n, nil
}

func readBatchFile(path string) ([]storage.Operation, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var operations []storage.Operation
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 4096), 2*1024*1024)

	for line := 1; scanner.Scan(); line++ {
		raw := strings.TrimSpace(scanner.Text())
		if raw == "" || strings.HasPrefix(raw, "#") {
			continue
		}

		parts := strings.SplitN(raw, " ", 3)
		switch parts[0] {
		case "put":
			if len(parts) != 3 || parts[1] == "" {
				return nil, fmt.Errorf("batch line %d: expected put <key> <value>", line)
			}
			operations = append(operations, storage.Operation{
				Type: storage.OpPut, Key: parts[1], Value: []byte(parts[2]),
			})
		case "del", "delete":
			if len(parts) != 2 || parts[1] == "" {
				return nil, fmt.Errorf("batch line %d: expected del <key>", line)
			}
			operations = append(operations, storage.Operation{
				Type: storage.OpDelete, Key: parts[1],
			})
		default:
			return nil, fmt.Errorf("batch line %d: unknown operation %q", line, parts[0])
		}

		if len(operations) > storage.MaxBatchOperations {
			return nil, fmt.Errorf("batch exceeds %d operations", storage.MaxBatchOperations)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(operations) == 0 {
		return nil, errors.New("batch file contains no operations")
	}
	return operations, nil
}

func serve(path string, maxBytes int64) error {
	db, err := engine.Open(path, maxBytes)
	if err != nil {
		return err
	}
	defer db.Close()

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 4096), 16*1024*1024)

	writer := bufio.NewWriter(os.Stdout)
	defer writer.Flush()

	for scanner.Scan() {
		var req request
		if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
			fmt.Fprintln(writer, "ERR unknown invalid_json")
			writer.Flush()
			continue
		}
		if req.ID == "" {
			fmt.Fprintln(writer, "ERR unknown missing_id")
			writer.Flush()
			continue
		}

		var opErr error
		switch req.Op {
		case "put":
			opErr = db.Put(req.Key, []byte(req.Value))
		case "del", "delete":
			opErr = db.Delete(req.Key)
		case "get":
			value, err := db.Get(req.Key)
			if err == nil {
				encoded, _ := json.Marshal(string(value))
				fmt.Fprintf(writer, "VALUE %s %s\n", req.ID, encoded)
				writer.Flush()
				continue
			}
			opErr = err
		case "batch":
			operations := make([]storage.Operation, 0, len(req.Batch))
			for _, item := range req.Batch {
				switch item.Type {
				case "put":
					operations = append(operations, storage.Operation{
						Type: storage.OpPut, Key: item.Key, Value: []byte(item.Value),
					})
				case "del", "delete":
					operations = append(operations, storage.Operation{
						Type: storage.OpDelete, Key: item.Key,
					})
				default:
					opErr = fmt.Errorf("unknown batch operation: %s", item.Type)
				}
				if opErr != nil {
					break
				}
			}
			if opErr == nil {
				opErr = db.Batch(operations)
			}
		default:
			opErr = fmt.Errorf("unknown operation: %s", req.Op)
		}

		if opErr != nil {
			message := strings.ReplaceAll(opErr.Error(), "\n", " ")
			fmt.Fprintf(writer, "ERR %s %s\n", req.ID, message)
		} else {
			fmt.Fprintf(writer, "ACK %s\n", req.ID)
		}
		writer.Flush()
	}
	return scanner.Err()
}

func usage() {
	fmt.Println(`Akuryx - crash-safe key-value database

Commands:
  kvdb open <path> [--max-bytes N]
  kvdb <path> [--max-bytes N] put <key> <value>
  kvdb <path> [--max-bytes N] get <key>
  kvdb <path> [--max-bytes N] del <key>
  kvdb <path> batch <file>
  kvdb <path> dump
  kvdb <path> check
  kvdb <path> compact
  kvdb <path> serve [--max-bytes N]`)
}
