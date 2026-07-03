# Session: btypedb-v0-release

- **Session ID**: `702f091a-986a-46da-b3f7-ae4d62701dde`
- **Date**: 2026-07-03
- **Continues**: [2026-0703-0006-phase-1-4-impl.md](2026-0703-0006-phase-1-4-impl.md) (same session, later segment)
- **Result**: v0.1.0 punch list implemented and **v0.1.0 tagged and pushed** — btype pin guard, power-loss fault-injection harness, `Keys()`/`Values()` iterators. 45 tests green under `-race`.

## What was done

### 1. btype dependency pin (`go.mod` + `pin_test.go`)
- go.mod comment marks the v0.3.0 pin as deliberate (survives `go mod tidy` — verified).
- `TestBtypePinned` fails if go.mod drifts from `github.com/tidwall/btype v0.3.0`.
- Rationale (stricter than normal pre-v1 caution): lock-free snapshot reads depend on btype internals verified only by source inspection — **atomic COW refcounts** and **no in-place mutation of shared nodes** — neither documented as contract. Upgrade procedure (in test comment + README): re-inspect those properties, run race hammer + crash + power-loss suites, then bump go.mod **and** `pinnedBtype` in pin_test.go.

### 2. Power-loss fault-injection harness (`powerfail_test.go`)
Closes the gap SIGKILL testing can't cover: **fsync ordering** (kills don't lose the OS page cache; power loss does).

- **Seam** (production change, minimal): `db.file` is now internal interface `logfile` (Read/Write/ReadAt/Close/Seek/Sync/Truncate/Stat) — `*os.File` satisfies it directly, zero wrapping. Test-only `openFile func(path) (logfile, error)` hook in `options`; in-package tests inject via unexported `withLogfile(f)` Option. `compact.go` `writeSnapshot` now takes `io.Writer`.
- **`powerFile` double**: models the page cache. Writes append to `pending`; `Sync` promotes `pending` → `durable`; Read/ReadAt/Stat see both (process view). Simulated power cut = keep only `durable` (± torn prefix of pending, ± garbage). Append-only write enforcement; Truncate/Seek supported for the Open path.
- **`TestPowerLossDurability`**: mixed workload (direct sets with overwrites, 3-op Update batches, deletes incl. no-op deletes) under `SyncAlways` against a powerFile. After every acked op: snapshot durable bytes + expected map. Then for each cut point, materialize to a real file and reopen:
  - clean ack-boundary cut → recovered state must equal expected **exactly** (catches ack-before-fsync and apply-before-log bugs);
  - torn cut (half of the next op's byte delta) → must land exactly on pre-op state (single records tear away; multi-op batches discard whole via batch framing);
  - garbage tail → must not confuse replay (CRC framing);
  - post-recovery probe write must succeed.
- **`TestPowerLossEveryPrefix`**: builds a real log (14 rounds: 4-key tx groups `g:NN:j` + singles + deletes + TTL sets, SyncNever), then opens **every byte-length prefix** 0..len, some with random garbage tails (seeded rand). Asserts open never fails and every tx group recovers all-or-nothing with uniform values. Skipped under `-short`. Trimmed from 25→14 rounds to keep the `-race` suite ~15s (was 23s+).

### 3. `Keys()` / `Values()` iterators
On both `DB` and `Tx`: ascending key order, expired-skipping, values in key order. Tx variants lock-free over the snapshot and see own uncommitted writes. `iter_test.go` covers TTL exclusion, tx own-writes, and lock release on early `break`.

### 4. README
New sections: **Crash safety testing** (honest scoping — SIGKILL layer = replay + compaction swap window; power-loss layer = fsync ordering + torn writes + prefix consistency) and **Dependency pin** (upgrade procedure). Roadmap: v0.1.0-prep line checked.

## Commits / release
- `34618ce` — "v0.1.0 prep: btype pin guard, power-loss harness, Keys/Values iterators" (8 files, +589/−3), pushed to main.
- **Tag `v0.1.0`** (annotated, release summary in message) pushed. Module is fetchable: `go get github.com/rohanthewiz/btypedb@v0.1.0`.
- ⚠️ Module-proxy immutability: once v0.1.0 is fetched through proxy.golang.org it's cached forever — fixes go in `v0.1.1`, never a re-tag.

## State at end of session
- 45 tests, all green under `-race`; suite ~15s (exhaustive prefix scan skips with `-short`).
- Files: db.go, state.go, tx.go, wal.go, compact.go, ttl.go, index.go, codec.go + tests (db, tx, compact, crash, ttl, index, rangedel, powerfail, iter, pin).
- Deps: btype v0.3.0 (pinned), serr v1.3.0. Go 1.26.

## Open threads (post-v0.1.0 ideas)
- `Descend`/`Backward` on the primary tree; expired-aware `Len` variant
- Group-commit across concurrent writers (fsync coalescing)
- Extend fault injection to the compaction temp-file/rename path (currently real files; power-loss coverage of compaction is via the SIGKILL suite + stale-temp discard on Open)
- Watch btype releases; re-verify COW internals before any upgrade (see pin_test.go)
