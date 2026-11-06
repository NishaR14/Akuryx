\# Akuryx — System Architecture



\## 1. Architecture Diagram



The following diagram shows the main components and data flow of Akuryx.



```mermaid

flowchart TD

&#x20;   A\["USER / APPLICATION"]

&#x20;   B\["CLI INTERFACE<br/>put · get · del · batch · serve"]

&#x20;   C\["DATABASE ENGINE<br/>Put · Get · Delete · Batch"]



&#x20;   D\["DATABASE MUTEX<br/>Coordinates operations"]

&#x20;   E\["COMPACTION MANAGER<br/>Rewrite live records"]



&#x20;   F\["WRITE-AHEAD LOG (WAL)<br/>Encode · Append · Checksum · Sync"]

&#x20;   G\[("TEMP / BACKUP FILES<br/>.compact · .backup · marker")]



&#x20;   H\[("DATABASE FILE<br/>Persistent transactions")]

&#x20;   I\["RECOVERY ENGINE<br/>Scan · Validate · Repair tail"]

&#x20;   J\["REBUILD INDEX<br/>Restore latest key locations"]

&#x20;   K\[("IN-MEMORY INDEX<br/>Key → WAL location")]



&#x20;   A --> B

&#x20;   B --> C

&#x20;   C --> D

&#x20;   C --> E

&#x20;   D --> F

&#x20;   E --> G

&#x20;   F --> H

&#x20;   G --> H

&#x20;   H --> I

&#x20;   I --> J

&#x20;   J --> K

&#x20;   D --> K



&#x20;   classDef component fill:#e5f2ff,stroke:#6b9ac4,color:#123b60,stroke-width:1px

&#x20;   classDef storage fill:#e5f2ff,stroke:#6b9ac4,color:#123b60,stroke-width:1px



&#x20;   class A,B,C,D,E,F,I,J component

&#x20;   class G,H,K storage

```



\## 2. Component Responsibilities



| Component | Responsibility |

|---|---|

| User / Application | Sends database requests. |

| CLI Interface | Provides put, get, delete, batch, and serve commands. |

| Database Engine | Implements database operations and coordinates storage and indexing. |

| Database Mutex | Coordinates database operations and protects shared state. |

| Write-Ahead Log (WAL) | Encodes and appends transactions, checksums frames, and synchronizes writes. |

| Compaction Manager | Rewrites live records and manages temporary and backup files. |

| Database File | Stores persistent transactions. |

| Recovery Engine | Scans and validates WAL frames and repairs an incomplete final frame where safe. |

| Rebuild Index | Reconstructs the latest key locations from recovered transactions. |

| In-Memory Index | Maps keys to their corresponding WAL locations. |



\## 3. Main Data Flows



\### Write flow



1\. The application sends a write request through the CLI or server.

2\. The database engine validates and processes the operation.

3\. The database coordinates access using its mutex.

4\. The operation is encoded into a WAL transaction frame.

5\. The frame is appended to the database file.

6\. The WAL is synchronized before the operation is acknowledged.

7\. The in-memory index is updated.



\### Read flow



1\. The application requests a key.

2\. The database engine looks up the key in the in-memory index.

3\. The corresponding WAL location is read.

4\. The stored value is returned to the application.



\### Recovery flow



1\. The database file is opened.

2\. The recovery engine validates the file header.

3\. WAL frames are scanned and validated.

4\. An incomplete final frame is repaired where safe.

5\. Recovered transactions are replayed.

6\. The in-memory index is reconstructed.



\### Compaction flow



1\. The compaction manager identifies live records.

2\. A temporary compacted database is written.

3\. The replacement is verified.

4\. Backup and marker files support interrupted-compaction recovery.

5\. The compacted database replaces the previous database.



\## 4. Persistence and Recovery



The WAL is the persistent source of truth. The in-memory index is reconstructed from recovered transactions when the database opens.



A successful write is acknowledged only after its WAL append and synchronization succeed.



An incomplete final transaction is not applied as a partial transaction. Corruption detected in the middle of the file is treated as database corruption rather than silently discarded.



\## 5. Important Files



| File | Purpose |

|---|---|

| `internal/engine/db.go` | Database API, recovery, index updates and compaction |

| `internal/storage/wal.go` | WAL append, synchronization and recovery |

| `internal/storage/record.go` | Transaction encoding and decoding |

| `internal/storage/format.go` | File and frame format constants |

| `internal/index/index.go` | In-memory key index |

| `cmd/kvdb/main.go` | CLI and JSON-lines server |



\---



\*This diagram describes the high-level Akuryx architecture. Detailed on-disk layout, exact operation ordering, lock behavior, and crash-scenario proofs are documented in the following sections of this architecture document.\*



\## 6. Exact On-Disk Format



Akuryx stores transactions in a single database file. The file begins with a 16-byte header, followed by transaction frames.



\### 6.1 Database header



| Field | Size | Description |

|---|---:|---|

| File magic | 8 bytes | `AKURYXDB` |

| Format version | 4 bytes | Version `1`, little-endian |

| Header checksum | 4 bytes | Header integrity field |



Total header size: \*\*16 bytes\*\*.



\### 6.2 Transaction frame header



Every transaction begins with a 12-byte frame header.



| Field | Size | Description |

|---|---:|---|

| Frame magic | 4 bytes | `AKTX` |

| Payload length | 4 bytes | Transaction payload size |

| Checksum | 4 bytes | CRC32 checksum of the transaction payload |



Total frame header size: \*\*12 bytes\*\*.



\### 6.3 Transaction payload



| Field | Size |

|---|---:|

| Transaction ID | 8 bytes |

| Operation count | 4 bytes |

| Encoded operations | Variable |



The transaction header is \*\*12 bytes\*\*. Integer fields are little-endian.



\### 6.4 Operation encoding



Each operation contains:



| Field | Size |

|---|---:|

| Operation type | 1 byte |

| Key length | 4 bytes |

| Value length | 4 bytes |

| Key | Variable |

| Value | Variable |



Operation types:



| Value | Meaning |

|---:|---|

| 1 | Put |

| 2 | Delete |



Delete operations have no value payload.



\### 6.5 Format limits



\- Maximum key length: 1,024 bytes.

\- Maximum value length: 1 MiB.

\- Maximum operations per batch: 10,000.

\- File header: 16 bytes.

\- Frame header: 12 bytes.

\- Transaction header: 12 bytes.



\## 7. Write Ordering and Durability



The write path follows this order:



1\. Validate the key, value, and operation.

2\. Acquire the database write lock.

3\. Construct the transaction with its transaction ID.

4\. Encode the transaction and calculate its checksum.

5\. Check whether the resulting append would exceed the configured file-size limit.

6\. Seek to the end of the WAL.

7\. Write the complete encoded frame.

8\. Call `Sync()` on the WAL file.

9\. Update the in-memory index.

10\. Release the database lock and return success.



The WAL append and synchronization happen before the index update and before a successful response is returned.



If an append or synchronization fails, Akuryx attempts to truncate the file back to the original append offset and synchronize the rollback. If rollback itself fails, the WAL is marked failed to prevent further unsafe writes.



\## 8. Recovery Algorithm



During database opening:



1\. Open the database file.

2\. Validate the file header.

3\. Scan transaction frames sequentially from the end of the file header.

4\. Validate each frame magic, payload length, and checksum.

5\. Decode the transaction payload.

6\. Rebuild the index by replaying valid transactions in order.

7\. If the final frame is incomplete, truncate the incomplete tail and synchronize the repair.

8\. If corruption is detected in the middle of the file, return a database-corruption error instead of silently skipping data.



A transaction is applied as a whole. An incomplete final transaction is not replayed as a partial batch.



The index is derived state: it can be rebuilt from the persistent transaction log.



\## 9. Locking and Concurrency



The database uses synchronization to coordinate reads, writes, and index updates.



\- A database mutex protects database state and transaction application.

\- Reads use the database synchronization before accessing the index and reading the corresponding WAL record.

\- Batch writes append one transaction and update the index while holding the database lock.

\- The index has its own read/write mutex.

\- The WAL has its own mutex to serialize file operations.



The server currently processes incoming JSON-lines requests sequentially. Concurrent database access is tested directly through multiple goroutines using the database API.



The database is intended for a single process. Cross-process file locking is not provided.



\## 10. File-Size Limit and Failure Behavior



When a maximum file size is configured, the WAL checks the current file size and the complete encoded frame size before appending.



If the append would exceed the configured maximum:



\- The write is rejected.

\- The rejected transaction is not applied to the index.

\- Previously committed records remain readable.

\- The database can still be checked and reopened.



Compaction is responsible for reclaiming space occupied by deleted and superseded records.



\## 11. Compaction and Interrupted Replacement



Compaction writes a replacement database containing live records.



The implementation uses:



\- `.compact` — temporary compacted database.

\- `.compact.marker` — marker containing compacted-file verification information.

\- `.backup` — backup of the previous database.

\- `.damaged` — preserved damaged main database when recovery needs to restore a backup.



The replacement is verified before promotion. Recovery checks the main database and backup files to select a valid database after an interrupted replacement.



\## 12. Crash-Safety Guarantees



The implementation is designed to provide these guarantees:



1\. A successfully acknowledged transaction has been synchronized to the WAL.

2\. An incomplete final transaction is not partially applied during recovery.

3\. A valid transaction is replayed in log order.

4\. Corruption in the middle of the WAL is reported rather than silently ignored.

5\. The in-memory index can be reconstructed from the WAL.

6\. Batch operations are applied as one transaction.

7\. Compaction retains a backup to support interrupted replacement recovery.



These guarantees describe process-crash recovery under the filesystem's successful write and synchronization semantics. They do not claim cross-process locking or universal power-loss durability for directory rename metadata.



\## 13. Scenario-to-Mechanism Proof Table



This table maps the 32 crash scenarios from the hackathon specification to the Akuryx implementation. Verification labels distinguish tests actually executed from scenarios whose exact crash point has not yet been tested.



\### Group A — Crash During a Single Write



| Scenario | Disk state at kill | Mechanism | Code location | Verified by |

|---|---|---|---|---|

| A1 | No bytes of the new transaction frame have reached the file. | Previous committed frames remain authoritative; no new operation is replayed. | `internal/storage/wal.go` — `Append`, `Recover` | Not verified at this exact crash point |

| A2 | Only part of the final frame has reached the file. | Recovery detects the incomplete final frame and truncates the tail. | `internal/storage/wal.go` — `recover`, `truncateTail` | Manual one-byte truncated-tail test |

| A3 | Complete frame may exist, but its `Sync()` has not returned. | Recovery validates the frame checksum. A valid complete frame may be replayed; an incomplete or invalid final frame is discarded. | `internal/storage/wal.go` — `Append`, `recover` | Not verified at this exact crash point |

| A4 | Complete frame has been synchronized, but the caller has not received success. | The complete synchronized transaction is replayed during recovery. | `internal/storage/wal.go` — `Append`, `Recover`; `internal/engine/db.go` — `Open` | Not verified at the exact instruction boundary |

| A5 | Put has returned successfully; its transaction frame is synchronized. | Acknowledged transaction is recovered and its value restored in the index. | `internal/storage/wal.go` — `Append`; `internal/engine/db.go` — `rebuildIndex` | `recovery-crash`; 10 cycles, seed 12345 |

| A6 | Delete may be incomplete or fully synchronized. | Recovery replays the complete delete or ignores an incomplete final transaction. | `internal/storage/wal.go` — `recover`; `internal/engine/db.go` — `rebuildIndex` | Not verified at the exact delete crash point |

| A7 | A large value spans the transaction payload across multiple disk blocks. | Frame length and checksum validation prevent an incomplete final value from being returned as valid data. | `internal/storage/record.go` — `EncodeTransaction`, `DecodeTransaction`; `internal/storage/wal.go` — `recover` | `large-values` mode passed; exact mid-write kill not verified |



\### Group B — Crash During a Batch



| Scenario | Disk state at kill | Mechanism | Code location | Verified by |

|---|---|---|---|---|

| B1 | Only part of the batch frame has been written. | Incomplete final transaction frame is truncated; none of its operations are replayed. | `internal/storage/wal.go` — `recover`, `truncateTail` | Interrupted-batch tail-truncation test |

| B2 | Batch payload may be written, but the complete checksummed frame is absent. | Recovery rejects or truncates an incomplete final frame. Akuryx does not use a separate commit-marker record. | `internal/storage/wal.go` — `Append`, `recover` | Not verified at this exact crash point |

| B3 | Complete frame may exist before `Sync()` returns. | A complete valid frame may be replayed; an incomplete or invalid final frame is discarded. The batch is never applied partially. | `internal/storage/wal.go` — `Append`, `recover` | Not verified at this exact crash point |

| B4 | Complete batch frame has been synchronized and the batch returned success. | Recovery replays all operations from the complete transaction. | `internal/storage/wal.go` — `Append`, `Recover`; `internal/engine/db.go` — `Batch`, `rebuildIndex` | 10,000-operation batch test passed; exact post-return kill covered by general ACK recovery testing |

| B5 | One transaction contains a put of key X followed by a delete of key X. | Operations are replayed in transaction order. The final state of X is absent. | `internal/engine/db.go` — `Batch`, `rebuildIndex` | Not verified with this exact same-key batch |

| B6 | A 10,000-operation batch frame is incomplete at the time of the kill. | The incomplete final transaction is discarded as a whole. | `internal/storage/wal.go` — `recover`; `internal/storage/record.go` — `DecodeTransaction` | 10,000-operation batch passed; killed-halfway scenario not verified |



\### Group C — Crash During Recovery and Maintenance



| Scenario | Disk state at kill | Mechanism | Code location | Verification |

|---|---|---|---|---|

| C1 | Recovery is replaying WAL transactions. | Recovery rebuilds the index from the persistent WAL on the next open. | `internal/engine/db.go` — `Open`, `rebuildIndex` | Recovery/reopen tests passed; exact mid-replay kill not verified |

| C2 | Recovery is truncating or repairing an incomplete tail. | Tail truncation is synchronized; the next open scans the repaired file. | `internal/storage/wal.go` — `truncateTail`, `recover` | Truncated-tail test passed; kill during repair not verified |

| C3 | Compaction is writing the temporary replacement; old main file remains. | The original database remains authoritative until replacement promotion. | `internal/engine/db.go` — `Compact`, `writeCompactionFile` | Compaction test passed; exact pre-replacement kill not verified |

| C4 | Main and backup files may be involved in replacement renames. | Backup recovery restores a valid database when the main file is unavailable or invalid. | `internal/engine/db.go` — `Compact`, `recoverCompaction` | Backup and promotion recovery tests passed |

| C5 | Replacement has been promoted; temporary or backup files may remain. | Open validates the main database and cleans stale auxiliary files. | `internal/engine/db.go` — `recoverCompaction` | Valid-main and compaction-recovery tests passed |

| C6 | Database process is interrupted during close. | Reopening validates and recovers the persistent WAL. | `internal/engine/db.go` — `Close`, `Open` | Persistence-after-reopen test passed; exact close interruption not verified |



\### Group D — Concurrency



| Scenario | Disk state at kill | Mechanism | Code location | Verification |

|---|---|---|---|---|

| D1 | Two writers have appended transactions for different keys; some may be acknowledged. | WAL mutex serializes appends; synchronized transactions are replayed in order. | `internal/storage/wal.go` — `Append`; `internal/engine/db.go` — `Batch` | `TestConcurrentWriters`; 500-cycle crash torture test |

| D2 | Two writers update the same key. | Serialized transactions determine the final value; each complete value is stored in its own transaction. | `internal/storage/wal.go` — `Append`; `internal/engine/db.go` — `Batch`, `rebuildIndex` | Concurrent writer test passed; same-key forced-crash test not verified |

| D3 | A long batch and individual puts are executing concurrently. | Each batch is one WAL transaction; the database lock coordinates transaction application. | `internal/engine/db.go` — `Batch`; `internal/storage/wal.go` — `Append` | Concurrent writer tests passed; exact mixed batch/put crash test not verified |

| D4 | A reader calls Get while a batch updates the same keys. | Database synchronization prevents a Get from observing an index partway through batch application. | `internal/engine/db.go` — `Get`, `Batch` | `TestBatchVisibilityIsAtomic`; `TestConcurrentReadersAndWriters` |



\### Group E — Disk Full



| Scenario | Disk state at kill | Mechanism | Code location | Verification |

|---|---|---|---|---|

| E1 | A single put would exceed the configured file-size limit. | WAL checks the complete frame size before append and returns `ErrFileLimit`. | `internal/storage/wal.go` — `Append` | Single-write disk-cap test passed |

| E2 | A batch would exceed the file-size limit. | The complete transaction is checked before append; rejected batches are not applied to the index. | `internal/storage/wal.go` — `Append`; `internal/engine/db.go` — `Batch` | Batch disk-cap test passed |

| E3 | The cap has been reached and the process is killed. | Previously synchronized transactions remain recoverable; rejected writes are not acknowledged. | `internal/storage/wal.go` — `Append`, `Recover` | Disk-full and recovery-crash modes passed separately; combined scenario not verified |

| E4 | Compaction cannot complete within the configured storage limit. | Compaction failure handling and backup recovery protect the previous database. | `internal/engine/db.go` — `Compact`, `recoverCompaction` | Compaction with a configured cap tested; exact cap-exhaustion during compaction not verified |

| E5 | Deletes have created obsolete records and compaction reclaims space. | Compaction rewrites live records into a smaller database, allowing future writes when sufficient space is available. | `internal/engine/db.go` — `Compact`, `liveOperations` | Million-key deletion and compaction passed; subsequent write-after-compaction not separately verified |



\### Group F — Repeated Abuse



| Scenario | Disk state at kill | Mechanism | Code location | Verification |

|---|---|---|---|---|

| F1 | Repeated random process kills and restarts occur during

