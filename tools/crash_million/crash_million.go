package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

const (
	totalWrites = 100
	dbPath      = ".\\akuryx-crash-million.db"
	binary      = ".\\kvdb.exe"
)

func main() {
	fmt.Println("Starting Akuryx crash-recovery test...")

	cmd := exec.Command(binary, dbPath, "serve")
	stdin, err := cmd.StdinPipe()
	must(err)

	stdout, err := cmd.StdoutPipe()
	must(err)
	cmd.Stderr = os.Stderr

	must(cmd.Start())

	responses := make(chan string)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			responses <- scanner.Text()
		}
		close(responses)
	}()

	acks := make(map[string]string)

	for i := 0; i < totalWrites; i++ {
		id := fmt.Sprintf("crash-%03d", i)
		key := fmt.Sprintf("crash-key-%03d", i)
		value := fmt.Sprintf("crash-value-%03d", i)

		request := map[string]string{
			"id": id, "op": "put", "key": key, "value": value,
		}

		data, err := json.Marshal(request)
		must(err)

		if _, err := fmt.Fprintln(stdin, string(data)); err != nil {
			break
		}

		select {
		case response, ok := <-responses:
			if !ok {
				break
			}
			if response == "ACK "+id {
				acks[key] = value
			} else {
				fmt.Println("Unexpected response:", response)
			}
		case <-time.After(10 * time.Second):
			fmt.Println("ACK timeout:", id)
			break
		}
	}

	fmt.Printf("Acknowledged writes: %d\n", len(acks))
	fmt.Println("Force-killing Akuryx...")

	_ = stdin.Close()
	_ = cmd.Process.Kill()
	_ = cmd.Wait()

	fmt.Println("Process terminated.")
	fmt.Println("Reopening database and measuring recovery...")

	start := time.Now()
	check := exec.Command(binary, "open", dbPath)
	output, err := check.CombinedOutput()
	recoveryTime := time.Since(start)

	fmt.Print(string(output))
	must(err)

	fmt.Printf("Measured recovery time: %v\n", recoveryTime)

	if recoveryTime >= 2*time.Second {
		fmt.Println("WARNING: recovery exceeded 2 seconds")
	} else {
		fmt.Println("Recovery completed under 2 seconds.")
	}

	// Verify all acknowledged writes in one serve session.
	fmt.Println("Verifying acknowledged writes in one session...")

	verify := exec.Command(binary, dbPath, "serve")
	verifyIn, err := verify.StdinPipe()
	must(err)

	verifyOut, err := verify.StdoutPipe()
	must(err)
	verify.Stderr = os.Stderr
	must(verify.Start())

	scanner := bufio.NewScanner(verifyOut)
	scanner.Buffer(make([]byte, 4096), 16*1024*1024)

	for key, expected := range acks {
		id := "verify-" + key
		req := map[string]string{"id": id, "op": "get", "key": key}
		data, err := json.Marshal(req)
		must(err)

		_, err = fmt.Fprintln(verifyIn, string(data))
		must(err)

		if !scanner.Scan() {
			panic("verification server stopped unexpectedly")
		}

		line := scanner.Text()
		prefix := "VALUE " + id + " "

		if !strings.HasPrefix(line, prefix) {
			panic("unexpected verification response: " + line)
		}

		var actual string
		must(json.Unmarshal([]byte(strings.TrimPrefix(line, prefix)), &actual))

		if actual != expected {
			panic(fmt.Sprintf("wrong value for %s", key))
		}
	}

	_ = verifyIn.Close()
	must(verify.Wait())

	fmt.Printf("Verified %d acknowledged writes.\n", len(acks))

	if len(acks) != totalWrites {
		panic("not all writes received acknowledgements")
	}

	fmt.Println("CRASH RECOVERY TEST PASSED")
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
