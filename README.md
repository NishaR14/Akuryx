\# Akuryx



\*\*A crash-safe, embedded key-value database written in Go.\*\*



Akuryx is a single-process key-value database built from scratch using Go's standard library. It uses a write-ahead log (WAL) for durable writes, an in-memory index for fast lookups, transaction framing and checksums for recovery, and compaction to reclaim unused storage.



\## Features



\- Persistent key-value storage.

\- Write-ahead logging with checksums.

\- Recovery after interrupted writes and process crashes.

\- Atomic batch operations.

\- Concurrent readers and writers within one database process.

\- In-memory key index rebuilt during database opening.

\- Configurable maximum database file size.

\- Compaction to remove deleted and obsolete records.

\- JSON-lines server protocol with acknowledgements.

\- Database integrity checking and key dumping.



\## Requirements



\- Go 1.26 or a compatible Go version.

\- Windows, Linux, or macOS.

\- No external database or storage dependencies.



\## Build



From the project root:



```powershell

go build -o .\\kvdb.exe .\\cmd\\kvdb

```



Run the test suite:



```powershell

go test ./... -count=1

```



Run tests with the race detector:



```powershell

go test -race ./... -count=1

```



Run static analysis:



```powershell

go vet ./...

```



\## Command-line usage



General syntax:



```text

kvdb <path> \[--max-bytes N] <command> \[arguments]

```



\### Open a database



```powershell

.\\kvdb.exe .\\data.db open

```



Open with a maximum database file size:



```powershell

.\\kvdb.exe .\\data.db --max-bytes 10485760 open

```



The maximum size is expressed in bytes.



\### Put a value



```powershell

.\\kvdb.exe .\\data.db put user:1 Alice

```



\### Get a value



```powershell

.\\kvdb.exe .\\data.db get user:1

```



\### Delete a key



```powershell

.\\kvdb.exe .\\data.db del user:1

```



\### Execute a batch



Create a text file containing one operation per line:



```text

put user:1 Alice

put user:2 Bob

put user:3 Charlie

del user:2

```



Save it as `batch.txt`, then run:



```powershell

.\\kvdb.exe .\\data.db batch .\\batch.txt

```



Supported batch operations:



\- `put <key> <value>`

\- `del <key>`

\- `delete <key>`



The batch is committed atomically.



\### Dump database contents



```powershell

.\\kvdb.exe .\\data.db dump

```



\### Check database integrity



```powershell

.\\kvdb.exe .\\data.db check

```



\### Compact the database



```powershell

.\\kvdb.exe .\\data.db compact

```



Compaction rewrites live records and reclaims space occupied by deleted or superseded records.



\### Start the JSON-lines server



```powershell

.\\kvdb.exe .\\data.db serve

```



The server accepts one JSON request per input line and returns a response per request.



Example put request:



```json

{"id":"request-1","op":"put","key":"user:1","value":"Alice"}

```



Successful operations return an acknowledgement:



```text

ACK request-1

```



The acknowledgement is emitted after the database operation succeeds durably.



Example get request:



```json

{"id":"request-2","op":"get","key":"user:1"}

```



Example batch request:



```json

{"id":"request-3","op":"batch","batch":\[{"type":"put","key":"user:2","value":"Bob"},{"type":"put","key":"user:3","value":"Charlie"}]}

```



Supported server operations include `put`, `get`, `del`, `delete`, and `batch`.



\## Database design



Akuryx uses a write-ahead log as its persistent source of truth and an in-memory index for key lookup.



\### Write path



1\. Validate the requested operation.

2\. Construct a transaction.

3\. Encode the transaction into a WAL frame.

4\. Write the frame to the database file.

5\. Synchronize the file to make the write durable.

6\. Update the in-memory index.

7\. Return success to the caller.



A successful write is acknowledged only after the WAL synchronization succeeds.



\### Read path



1\. Look up the key in the in-memory index.

2\. Locate the corresponding record in the WAL.

3\. Read and validate the record.

4\. Return the stored value.



\### Recovery



When a database is opened:



1\. Validate the database header.

2\. Scan WAL frames.

3\. Validate transaction framing and checksums.

4\. Reconstruct the in-memory index.

5\. Repair an incomplete final WAL frame where safe.

6\. Reject detected corruption in the middle of the database file.



The WAL is used to reconstruct the index after a restart.



\### Atomic batches



A batch is encoded as one transaction frame. The database applies the transaction to the index only after the WAL append succeeds. Readers are synchronized with the update so they cannot observe an index partway through a batch update.



\### Compaction



Compaction creates a replacement database containing live records. It uses temporary and backup files and a marker to support recovery if the process is interrupted during replacement.



\### File-size limit



A configurable maximum database file size can be supplied when opening the database. Writes that exceed the configured limit are rejected. Existing committed records remain available.



\## Data format



The database begins with a fixed header:



| Field | Size |

|---|---:|

| File magic | 8 bytes |

| Format version | 4 bytes |

| Reserved/header data | 4 bytes |



Transactions are stored in frames containing:



| Field | Size |

|---|---:|

| Frame magic | 4 bytes |

| Payload length | 4 bytes |

| Checksum | 4 bytes |

| Transaction payload | Variable |



Transaction payloads contain a transaction ID, operation count, and encoded operations. Integer fields use little-endian encoding.



Each operation contains an operation type, key length, value length, key bytes, and value bytes.



\## Limits



\- Key size: 1–1024 bytes.

\- Value size: 0–1 MiB.

\- Maximum batch size: 10,000 operations.

\- Intended workload: up to 1 million keys.

\- Database size: configurable through `--max-bytes`.



\## Testing



Run all tests:



```powershell

go test ./... -count=1

```



Run race tests:



```powershell

go test -race ./... -count=1

```



Run the torture test:



```powershell

go build -o .\\torture.exe .\\torture

.\\torture.exe --mode crash --iterations 500 --seed 12345

```



Available torture modes:



```text

crash

recovery-crash

large-values

batches

compaction

disk-full

```



Examples:



```powershell

.\\torture.exe --mode recovery-crash --iterations 10 --seed 12345

.\\torture.exe --mode large-values

.\\torture.exe --mode batches

.\\torture.exe --mode compaction

.\\torture.exe --mode disk-full

```



The crash mode uses randomized delays and forced process termination. It records server acknowledgements independently and checks recovered database contents against the acknowledgement records.



\## Project structure



```text

Akuryx/

├── cmd/

│   └── kvdb/             # Command-line application and server

├── internal/

│   ├── engine/           # Database API, index updates, recovery, compaction

│   ├── index/            # In-memory key index

│   └── storage/          # WAL, transaction encoding, file format

├── torture/

│   └── torture.go        # Crash and recovery test harness

├── tests/                # Additional test resources

├── diagrams/             # Architecture diagrams

├── tools/                # Benchmark and stress-test helpers

├── ARCHITECTURE.md       # Architecture and crash-safety documentation

├── BREAKING.md           # Failure analysis

├── PROMPTS.md            # Development prompt log

└── README.md

```



\## Design goals



Akuryx focuses on:



\- Durability of acknowledged operations.

\- Recovery from interrupted writes.

\- Atomic transaction batches.

\- Consistent concurrent access.

\- Detection of database corruption.

\- Safe storage reclamation through compaction.

\- Reproducible crash and recovery testing.



Akuryx is a single-process embedded database, not a distributed database or a multi-process database server.

