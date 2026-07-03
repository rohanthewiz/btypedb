# Session: btypedb-group-commit-and-compaction-powerloss

- **Session ID**: `834d4d16-eb42-4a5f-843c-db7f95789db2`
- **Date**: 2026-07-03
- **Continues**: [2026-0703-0026-btypedb-v0-release.md](2026-0703-0026-btypedb-v0-release.md) (post-v0.1.0 open threads)
- **Result**: all four open threads closed in commit `768e18f` — "v0.2.0 prep: reverse iteration, group commit, compaction power-loss harness" (11 files, +938/−80). 50 tests green under `-race`. **Not pushed, not tagged.**

## What was done

### 1. Reverse iteration + expired-aware Len (`db.go`, `tx.go`, `state.go`)
- `Descend(from)` (starts at last key <= from, descending — btype `Descend` pivot is inclusive, verified by test) and `Backward()` on both `DB` and `Tx`; expired keys skipped; Tx variants lock-free over the snapshot with own-write visibility.
- `LiveLen()` on `DB`/`Tx`: `data.Len()` minus the expired prefix of the deadline-ordered `exp` tree (`dbState.liveLen`), so cost is O(expired-unswept). Plain `Len` still counts unswept expired keys.
- Tests in `iter_test.go` (`TestBackwardDescend`, `TestLiveLen`), incl. between-key pivots and early-break lock release.

### 2. Group commit under SyncAlways (`groupcommit.go` + write-path restructure)
- **Shape**: log append + in-memory apply (+ state swap for tx commits) stay under `writerMu`/`mu`; the pre-ack fsync moves *outside* the locks. Each `db.file.Write` bumps `db.appendSeq` (in new `writeLog`, used by both `appendToLog` and `Tx.Commit`); the committer then calls `waitDurable(seq)` with no locks held.
- **`groupSync`** (own mutex + cond; lock order: `mu` → `gsync.mu`, never reversed): committers wait until the durable watermark `synced` passes their seq; whoever finds no fsync in flight becomes leader, captures `(file, appendSeq)` under `mu.RLock`, fsyncs once, advances `synced` to the captured cover, broadcasts. One fsync acks every append that preceded it.
- **Compaction race handled**: a leader's fsync can fail because Compact swapped and closed the handle mid-sync. Detected by re-checking `db.file != f`; not a durability failure (the pre-rename tmp sync covered those appends) so the error is discarded. Genuine sync failures set sticky `db.writeErr` + `gsync.err` (DB goes read-only).
- **Watermark advancement outside the leader path**: `markDurable()` (callers hold `mu`) after Close's final sync, `DB.Sync`, and Compact's swap — releases in-flight waiters.
- **Semantics change (documented on `SyncAlways` + README)**: a committed write is readable slightly before its fsync completes; the writer is never acked until data is on disk. Sequential workloads unaffected — `TestPowerLossDurability` (ack-boundary exact-state) passes unchanged. On a failed group sync the write *is* applied in memory and may or may not be durable; DB read-only after.
- Tests: `TestGroupCommit` (8 writers × 25 ops against a slow-sync counting file: asserts fsyncs < ops and full replay after reopen) and `TestGroupCommitWithCompaction` (4 writers vs 5 concurrent manual Compacts: no write may fail, all replay).

### 3. Compaction power-loss fault injection (`fs.go`, `powerfailfs_test.go`)
- **Seam widened** from single-logfile injection to a filesystem interface `fsys` (`OpenFile`/`Create`/`Rename`/`Remove`/`SyncDir`), `realFS` in production, `db.fs` field, options gain `fs` (old `openFile` seam removed; `withLogfile` now wraps a `logfileFS`, new `withFS` injects whole filesystems).
- **Real bug found & fixed while building it**: `Open` never dir-synced after *creating* the log file, so a brand-new database could vanish wholesale on power loss (inode synced, directory entry not). `Open` now calls `fs.SyncDir(path)` best-effort after `OpenFile`.
- **`powerFS` double**: extends the `powerFile` model to directory metadata — file contents durable only after the file's `Sync`; create/rename/remove durable only after `SyncDir`. Records an `fsCut` at every operation boundary (every file Write/Sync/Truncate via a `pfsFile` wrapper, every dir op) in two flavors: *conservative* (no un-synced dir op persisted) and *eager* (all issued dir ops persisted), bracketing real crash outcomes.
- **`TestPowerLossCompaction`**: churn workload (overwrites, deletes, TTL key, multi-op tx) under SyncAlways, then `Compact()`; every recorded cut in both modes must materialize (to real files), open cleanly, discard any leftover `.compact`, recover the *exact* pre-compaction state, and accept writes. Then 3 post-compaction acked writes must survive metadata-conservative cuts — proving tmp is fsynced before the rename and the directory after it.
- **Mutation-tested the harness**: deleting the pre-rename `tmp.Sync` fails the compaction cuts; deleting the post-rename `SyncDir` fails the post-compaction conservative cut. Both caught at the expected spot.

### 4. btype release watch
- `go list -m -versions`: v0.3.0 is still the latest upstream; pin + `TestBtypePinned` stand unchanged. Nothing to re-verify.

### README
- Descend/Backward/LiveLen examples; group-commit paragraph under Sync policies (incl. the visibility-before-durability caveat); compaction power-loss harness section under Crash safety testing; roadmap gains a checked v0.2.0-prep line.

## Gotchas / notes
- `git checkout -- compact.go` during mutation-testing clobbered uncommitted changes once — reapplied; later mutations used scratchpad backup/restore instead.
- powerFS metadata model is all-or-nothing (conservative vs eager), not per-op subsets; file-content tearing on the compaction tmp file itself is not simulated (main log torn cuts covered by `TestPowerLossDurability`).

## State at end of session
- Commit `768e18f` on main, **local only** — push and `v0.2.0` tag pending user go-ahead.
- 50 tests green under `-race` (~26s; exhaustive prefix scan skips with `-short`).
- New files: fs.go, groupcommit.go, groupcommit_test.go, powerfailfs_test.go.
- Deps unchanged: btype v0.3.0 (pinned), serr v1.3.0. Go 1.26.

## Open threads (post-v0.2.0 ideas)
- Push + tag `v0.2.0` (remember module-proxy immutability: fixes go in v0.2.1, never a re-tag).
- Torn-write injection on the compaction temp file's content (whole-sync granularity today).
- Expired-aware `Len` done; consider descending secondary-index pivots (`DescendIndexFrom`).
- Keep watching btype releases; re-verify COW internals before any upgrade (see pin_test.go).
