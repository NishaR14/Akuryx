package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"Akuryx/internal/engine"
	"Akuryx/internal/storage"
)

type BatchItem struct {
	Type  string `json:"type"`
	Key   string `json:"key"`
	Value string `json:"value"`
}
type Request struct {
	ID    string      `json:"id"`
	Op    string      `json:"op"`
	Key   string      `json:"key,omitempty"`
	Value string      `json:"value,omitempty"`
	Batch []BatchItem `json:"batch,omitempty"`
}
type TestWrite struct{ ID, Key, Value string }
type Results struct {
	mu   sync.Mutex
	acks map[string]bool
}

func (r *Results) addAck(id string)        { r.mu.Lock(); r.acks[id] = true; r.mu.Unlock() }
func (r *Results) wasAcked(id string) bool { r.mu.Lock(); defer r.mu.Unlock(); return r.acks[id] }

func main() {
	iterations := flag.Int("iterations", 500, "number of crash/restart cycles")
	seed := flag.Int64("seed", time.Now().UnixNano(), "random seed")
	binaryFlag := flag.String("binary", "", "path to an existing kvdb executable")
	mode := flag.String("mode", "crash", "crash, recovery-crash, large-values, batches, compaction, disk-full")
	flag.Parse()
	valid := map[string]bool{"crash": true, "recovery-crash": true, "large-values": true, "batches": true, "compaction": true, "disk-full": true}
	if !valid[*mode] {
		fail("invalid mode", fmt.Errorf("mode must be crash, recovery-crash, large-values, batches, compaction, or disk-full"))
	}
	if *iterations < 1 {
		fail("invalid iterations", errors.New("iterations must be at least 1"))
	}
	rng := rand.New(rand.NewSource(*seed))
	workDir, err := os.MkdirTemp("", "akuryx-torture-")
	if err != nil {
		fail("create temporary directory", err)
	}
	defer os.RemoveAll(workDir)
	dbPath := filepath.Join(workDir, "torture.db")
	binaryPath := *binaryFlag
	if binaryPath == "" {
		binaryPath = filepath.Join(workDir, "kvdb.exe")
		fmt.Println("Building Akuryx CLI...")
		build := exec.Command("go", "build", "-o", binaryPath, "./cmd/kvdb")
		build.Stdout = os.Stdout
		build.Stderr = os.Stderr
		if err := build.Run(); err != nil {
			fail("build Akuryx", err)
		}
	}
	fmt.Printf("Akuryx torture test\nMode: %s\nIterations: %d\nRandom seed: %d\nDatabase: %s\n\n", *mode, *iterations, *seed, dbPath)
	var runErr error
	switch *mode {
	case "crash":
		runErr = runCrash(binaryPath, dbPath, *iterations, rng)
	case "recovery-crash":
		runErr = runRecoveryCrash(binaryPath, dbPath, *iterations)
	case "large-values":
		runErr = runLargeValues(binaryPath, dbPath)
	case "batches":
		runErr = runBatches(binaryPath, dbPath)
	case "compaction":
		runErr = runCompaction(dbPath)
	case "disk-full":
		runErr = runDiskFull(binaryPath, dbPath)
	}
	if runErr != nil {
		fail(*mode+" test", runErr)
	}
	fmt.Printf("\nTORTURE TEST PASSED\nMode: %s\nSeed: %d\n", *mode, *seed)
	if *mode == "crash" || *mode == "recovery-crash" {
		fmt.Printf("Cycles: %d\n", *iterations)
	}
}

func startServer(binaryPath, dbPath string, args ...string) (*exec.Cmd, io.WriteCloser, <-chan string, error) {
	allArgs := append([]string{dbPath}, args...)
	allArgs = append(allArgs, "serve")
	cmd := exec.Command(binaryPath, allArgs...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, nil, nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, nil, err
	}
	cmd.Stderr = os.Stderr
	if err = cmd.Start(); err != nil {
		return nil, nil, nil, err
	}
	lines := make(chan string, 2048)
	go func() {
		defer close(lines)
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 4096), 16*1024*1024)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
	}()
	return cmd, stdin, lines, nil
}
func sendRequest(stdin io.Writer, mu *sync.Mutex, r Request) error {
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	mu.Lock()
	defer mu.Unlock()
	_, err = stdin.Write(data)
	return err
}
func readAcks(lines <-chan string, results *Results, done chan<- struct{}) {
	defer close(done)
	for line := range lines {
		if strings.HasPrefix(line, "ACK ") {
			results.addAck(strings.TrimSpace(strings.TrimPrefix(line, "ACK ")))
		}
	}
}

func runCrash(binaryPath, dbPath string, iterations int, rng *rand.Rand) error {
	for cycle := 1; cycle <= iterations; cycle++ {
		fmt.Printf("[%d/%d] crash cycle\n", cycle, iterations)

		child, stdin, lines, err := startServer(binaryPath, dbPath)
		if err != nil {
			return err
		}

		results := &Results{acks: make(map[string]bool)}
		readerDone := make(chan struct{})
		go readAcks(lines, results, readerDone)

		var sendMu sync.Mutex
		var writeMu sync.Mutex
		var writers sync.WaitGroup
		var writes []TestWrite
		started := make(chan struct{}, 2)

		for writer := 1; writer <= 2; writer++ {
			writers.Add(1)
			go func(writerID int) {
				defer writers.Done()
				started <- struct{}{}

				for n := 1; n <= 10; n++ {
					id := fmt.Sprintf("c%d-w%d-op%d", cycle, writerID, n)
					w := TestWrite{
						ID:    id,
						Key:   "key-" + id,
						Value: "value-" + id,
					}

					err := sendRequest(stdin, &sendMu, Request{
						ID: id, Op: "put", Key: w.Key, Value: w.Value,
					})
					if err != nil {
						return
					}

					writeMu.Lock()
					writes = append(writes, w)
					writeMu.Unlock()
				}
			}(writer)
		}

		// Ensure both client writers have started before the crash delay.
		<-started
		<-started

		time.Sleep(time.Duration(rng.Intn(40)+1) * time.Millisecond)

		_ = child.Process.Kill()
		_ = stdin.Close()
		_ = child.Wait()

		writers.Wait()
		<-readerDone

		if err := verifyDatabase(binaryPath, dbPath, writes, results); err != nil {
			return fmt.Errorf("cycle %d: %w", cycle, err)
		}

		fmt.Printf(
			"  attempted=%d ACKed=%d; recovery verified\n",
			len(writes),
			countAcks(writes, results),
		)
	}

	return nil
}

func countAcks(writes []TestWrite, r *Results) int {
	n := 0
	for _, w := range writes {
		if r.wasAcked(w.ID) {
			n++
		}
	}
	return n
}

func runRecoveryCrash(binaryPath, dbPath string, iterations int) error {
	for i := 1; i <= iterations; i++ {
		id := fmt.Sprintf("recovery-%06d", i)
		key := "recovery-key-" + id
		value := "durable-" + id
		child, stdin, lines, err := startServer(binaryPath, dbPath)
		if err != nil {
			return err
		}
		if err = sendRequest(stdin, &sync.Mutex{}, Request{ID: id, Op: "put", Key: key, Value: value}); err != nil {
			_ = child.Process.Kill()
			return err
		}
		acked := false
		for line := range lines {
			if line == "ACK "+id {
				acked = true
				break
			}
			if strings.HasPrefix(line, "ERR ") {
				_ = child.Process.Kill()
				_ = child.Wait()
				return errors.New(line)
			}
		}
		if !acked {
			_ = child.Process.Kill()
			_ = child.Wait()
			return fmt.Errorf("cycle %d: write did not ACK", i)
		}
		_ = child.Process.Kill()
		_ = stdin.Close()
		_ = child.Wait()
		out, err := exec.Command(binaryPath, dbPath, "get", key).CombinedOutput()
		if err != nil {
			return fmt.Errorf("cycle %d: ACKed value missing: %w: %s", i, err, out)
		}
		if strings.TrimSpace(string(out)) != value {
			return fmt.Errorf("cycle %d: expected %q, got %q", i, value, strings.TrimSpace(string(out)))
		}
		fmt.Printf("[%d/%d] ACK survived forced kill\n", i, iterations)
	}
	return nil
}

func verifyDatabase(binaryPath, dbPath string, writes []TestWrite, results *Results) error {
	check, err := exec.Command(binaryPath, dbPath, "check").CombinedOutput()
	if err != nil {
		return fmt.Errorf("check failed: %w\n%s", err, check)
	}
	dump, err := exec.Command(binaryPath, dbPath, "dump").Output()
	if err != nil {
		return fmt.Errorf("dump failed: %w", err)
	}
	found := make(map[string]string)
	scanner := bufio.NewScanner(strings.NewReader(string(dump)))
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), "\t", 2)
		if len(parts) == 2 {
			found[parts[0]] = parts[1]
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	for _, w := range writes {
		actual, exists := found[w.Key]
		if results.wasAcked(w.ID) {
			if !exists {
				return fmt.Errorf("ACKed key %q is missing", w.Key)
			}
			if actual != w.Value {
				return fmt.Errorf("wrong value for %q: expected %q, found %q", w.Key, w.Value, actual)
			}
		} else if exists && actual != w.Value {
			return fmt.Errorf("unACKed key %q has torn/wrong value: %q", w.Key, actual)
		}
	}
	return nil
}

func runLargeValues(binaryPath, dbPath string) error {
	fmt.Println("Testing 1 MiB value...")
	child, stdin, lines, err := startServer(binaryPath, dbPath)
	if err != nil {
		return err
	}
	value := strings.Repeat("A", 1024*1024)
	if err = sendRequest(stdin, &sync.Mutex{}, Request{ID: "large-value-001", Op: "put", Key: "large-value", Value: value}); err != nil {
		_ = child.Process.Kill()
		return err
	}
	acked := false
	for line := range lines {
		if line == "ACK large-value-001" {
			acked = true
			break
		}
		if strings.HasPrefix(line, "ERR ") {
			_ = child.Process.Kill()
			_ = child.Wait()
			return errors.New(line)
		}
	}
	_ = stdin.Close()
	_ = child.Wait()
	if !acked {
		return errors.New("large-value write did not ACK")
	}
	out, err := exec.Command(binaryPath, dbPath, "get", "large-value").Output()
	if err != nil {
		return err
	}
	if strings.TrimSuffix(string(out), "\n") != value {
		return fmt.Errorf("large value mismatch: got %d bytes", len(out))
	}
	fmt.Printf("Verified %d-byte value after restart.\n", len(value))
	return nil
}
func runBatches(binaryPath, dbPath string) error {
	fmt.Println("Testing 10,000-operation atomic batch...")
	child, stdin, lines, err := startServer(binaryPath, dbPath)
	if err != nil {
		return err
	}
	r := Request{ID: "large-batch-001", Op: "batch", Batch: make([]BatchItem, 0, 10000)}
	for i := 0; i < 10000; i++ {
		r.Batch = append(r.Batch, BatchItem{Type: "put", Key: fmt.Sprintf("batch-key-%04d", i), Value: fmt.Sprintf("batch-value-%04d", i)})
	}
	if err = sendRequest(stdin, &sync.Mutex{}, r); err != nil {
		_ = child.Process.Kill()
		return err
	}
	acked := false
	for line := range lines {
		if line == "ACK large-batch-001" {
			acked = true
			break
		}
		if strings.HasPrefix(line, "ERR ") {
			_ = child.Process.Kill()
			_ = child.Wait()
			return errors.New(line)
		}
	}
	_ = stdin.Close()
	_ = child.Wait()
	if !acked {
		return errors.New("batch did not ACK")
	}
	if out, err := exec.Command(binaryPath, dbPath, "check").CombinedOutput(); err != nil {
		return fmt.Errorf("check failed: %w: %s", err, out)
	}
	out, err := exec.Command(binaryPath, dbPath, "dump").Output()
	if err != nil {
		return err
	}
	entries := make(map[string]string)
	s := bufio.NewScanner(strings.NewReader(string(out)))
	for s.Scan() {
		p := strings.SplitN(s.Text(), "\t", 2)
		if len(p) == 2 {
			entries[p[0]] = p[1]
		}
	}
	if err := s.Err(); err != nil {
		return err
	}
	for i := 0; i < 10000; i++ {
		k := fmt.Sprintf("batch-key-%04d", i)
		v := fmt.Sprintf("batch-value-%04d", i)
		if entries[k] != v {
			return fmt.Errorf("missing/wrong batch entry %s", k)
		}
	}
	fmt.Println("Verified all 10,000 batch operations after restart.")
	return nil
}

func runCompaction(dbPath string) error {
	fmt.Println("Compaction mode: one million puts, one million deletes")
	db, err := engine.Open(dbPath, 0)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			_ = db.Close()
		}
	}()
	const total = 1_000_000
	const batchSize = 10_000
	for start := 0; start < total; start += batchSize {
		ops := make([]storage.Operation, 0, batchSize)
		for i := start; i < start+batchSize; i++ {
			ops = append(ops, storage.Operation{Type: storage.OpPut, Key: fmt.Sprintf("compact-%07d", i), Value: []byte("torture-data")})
		}
		if err := db.Batch(ops); err != nil {
			return fmt.Errorf("put batch %d: %w", start, err)
		}
		if (start+batchSize)%100000 == 0 {
			fmt.Printf("  inserted %d/%d\n", start+batchSize, total)
		}
	}
	for start := 0; start < total; start += batchSize {
		ops := make([]storage.Operation, 0, batchSize)
		for i := start; i < start+batchSize; i++ {
			ops = append(ops, storage.Operation{Type: storage.OpDelete, Key: fmt.Sprintf("compact-%07d", i)})
		}
		if err := db.Batch(ops); err != nil {
			return fmt.Errorf("delete batch %d: %w", start, err)
		}
		if (start+batchSize)%100000 == 0 {
			fmt.Printf("  deleted %d/%d\n", start+batchSize, total)
		}
	}
	count, err := db.Count()
	if err != nil {
		return err
	}
	if count != 0 {
		return fmt.Errorf("expected zero keys before compaction, got %d", count)
	}
	before, err := os.Stat(dbPath)
	if err != nil {
		return err
	}
	if err = db.Compact(); err != nil {
		return err
	}
	after, err := os.Stat(dbPath)
	if err != nil {
		return err
	}
	if err = db.Close(); err != nil {
		return err
	}
	closed = true
	db, err = engine.Open(dbPath, 0)
	if err != nil {
		return fmt.Errorf("reopen after compaction: %w", err)
	}
	defer db.Close()
	count, err = db.Count()
	if err != nil {
		return err
	}
	if count != 0 {
		return fmt.Errorf("expected zero keys after reopen, got %d", count)
	}
	if after.Size() >= before.Size() {
		return fmt.Errorf("file did not shrink: before=%d after=%d", before.Size(), after.Size())
	}
	fmt.Printf("Compaction passed: %d -> %d bytes; reclaimed %d bytes; keys=%d\n", before.Size(), after.Size(), before.Size()-after.Size(), count)
	return nil
}

func runDiskFull(binaryPath, dbPath string) error {
	fmt.Println("Disk-full mode: verify cap rejection preserves prior data")
	// First write establishes a real file size; then cap the next invocation
	// at one byte above that size, guaranteeing the next frame cannot fit.
	const key = "disk-full-safe"
	const value = "must-remain-readable"
	put := exec.Command(binaryPath, dbPath, "put", key, value)
	if out, err := put.CombinedOutput(); err != nil {
		return fmt.Errorf("initial write failed: %w: %s", err, out)
	}
	info, err := os.Stat(dbPath)
	if err != nil {
		return err
	}
	capBytes := info.Size() + 1
	cmd := exec.Command(binaryPath, dbPath, "--max-bytes", strconv.FormatInt(capBytes, 10), "put", "disk-full-rejected", strings.Repeat("X", 1024))
	out, err := cmd.CombinedOutput()
	if err == nil {
		return fmt.Errorf("write unexpectedly succeeded with cap %d", capBytes)
	}
	errorText := strings.ToLower(string(out))
	expectedError := strings.Contains(errorText, "limit") ||
		strings.Contains(errorText, "full") ||
		strings.Contains(errorText, "maximum database file size reached")

	if !expectedError {
		return fmt.Errorf("write failed for unexpected reason: %v: %s", err, out)
	}
	get := exec.Command(binaryPath, dbPath, "get", key)
	got, err := get.CombinedOutput()
	if err != nil {
		return fmt.Errorf("previous value became unreadable: %w: %s", err, got)
	}
	if strings.TrimSpace(string(got)) != value {
		return fmt.Errorf("previous value changed: %q", strings.TrimSpace(string(got)))
	}
	check := exec.Command(binaryPath, dbPath, "check")
	checked, err := check.CombinedOutput()
	if err != nil {
		return fmt.Errorf("database check failed after cap rejection: %w: %s", err, checked)
	}
	fmt.Printf("Cap=%d bytes; oversized write rejected; previous value readable; %s", capBytes, strings.TrimSpace(string(checked)))
	return nil
}

func fail(message string, err error) {
	fmt.Fprintln(os.Stderr, "FAILED:", message)
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
