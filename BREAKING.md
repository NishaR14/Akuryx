\# BREAKING.md — How I Would Break Akuryx



\## 1. Crash Points Table



| Crash point / function | Possible disk state | What `Open()` does |

|---|---|---|

| `WAL.Append`: before frame write | Previous complete WAL | Opens and recovers previous transactions |

| `WAL.Append`: during frame write | Incomplete final frame | Repairs incomplete tail |

| `WAL.Append`: after write, before `Sync()` | Frame may or may not be durable | Recovers valid complete frame or repairs incomplete tail |

| `WAL.Append`: after successful `Sync()` | Complete durable frame | Replays transaction |

| `Batch`: after WAL append, before index update | Durable transaction, old in-memory index lost on process exit | Rebuilds index from WAL |

| `recover`: during tail repair | Tail may be partly or fully truncated | Attempts recovery and repair again |

| `Compact`: while writing `.compact` | Original database remains authoritative | Opens original database and cleans stale temporary files |

| `Compact`: after main renamed to `.backup` | Backup and compact file may exist | Attempts backup recovery if main is unavailable |

| `Compact`: after compact file promoted to main | New main and backup may both exist | Opens valid main and removes stale backup |

| `Compact`: during backup cleanup | Valid main may coexist with backup | Opens valid main and cleans stale files |



These rows describe the intended recovery paths. They are not proof that every individual instruction has been tested with a forced process kill.



\## 2. Ordering Argument



For a normal write, Akuryx encodes a transaction frame, appends it to the WAL, and calls `Sync()` before returning success. The in-memory index is updated only after the append succeeds. Therefore, an acknowledged write has passed the WAL sync step.



For a batch, all operations are encoded into one transaction frame. The WAL append and sync happen before the index is updated. The database mutex prevents readers from observing the index while the batch update is in progress.



During compaction, Akuryx writes a replacement file, verifies it, writes and syncs a compaction marker, closes the original WAL, renames the original to a backup, and promotes the compact file. If promotion fails, it attempts to restore the backup.



Akuryx uses file `Sync()` calls and filesystem rename operations. \*\*It does not successfully sync the containing directory on Windows.\*\* Therefore, this implementation does not claim directory-metadata durability across every power-loss scenario.



\## 3. Known Weaknesses



1\. Directory metadata is not successfully synced on Windows. A sudden power loss around a rename may have different guarantees from a process crash.

2\. The database is designed for a single process. It does not provide a cross-process file lock for multiple independent database processes opening the same file.

3\. Recovery scans the WAL to rebuild the in-memory index. Very large logs may increase startup time; the one-million-key benchmark passed on the tested machine, but that does not guarantee the same result on every machine.

4\. The torture tests exercise important crash and recovery paths, but they do not force a kill at every individual filesystem instruction.



\## 4. What We Tried That Failed



\- The first key/value validation implementation did not enforce the 1,024-byte maximum key size. Validation was added to transaction encoding and decoding.

\- An initial batch-visibility test compared several independent `Get()` calls as if they were one atomic snapshot. That test was corrected because a batch may commit between separate calls.

\- The disk-full torture test initially failed to recognize the actual file-limit error text. Its error matcher was corrected and the test passed.

\- On Windows, attempting to sync the containing directory returned an access-denied error. The implementation does not claim that this directory sync succeeded.



\## 5. If We Had 24 More Hours



1\. Add deterministic crash-injection hooks at WAL write, file sync, compaction marker, and rename boundaries; test every boundary with forced process termination.

2\. Improve and independently validate Windows crash durability around compaction renames and directory metadata.

3\. Add a comprehensive multi-process safety strategy or explicitly enforce exclusive process ownership of each database file.

