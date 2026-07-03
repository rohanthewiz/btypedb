# Session: bytdb-milestones-3-4-txns-and-alter

- **Session ID**: `0f674b70-d2d3-4c31-9801-ef2b07303533`
- **Date**: 2026-07-03
- **Continues**: [2026-0703-1022-btypedb-v0.3-skill-and-bytdb-relational-layer.md](2026-0703-1022-btypedb-v0.3-skill-and-bytdb-relational-layer.md) (same live session, later turns)
- **Repo**: `~/projs/go/bytdb` (github.com/rohanthewiz/bytdb)
- **Result**: milestones 3 and 4 complete and pushed — `c16f747` (row Update + engine transactions), `138d7b1` (column-ID-tagged sparse row values, AddColumn/DropColumn). All tests green under `-race`. **Next: milestone 5 SQL frontend — user wants a scoping conversation about go-mysql-server vs hand-rolled subset first.**

## Milestone 3 (`c16f747`): row Update + engine transactions

### Update
- `Engine.Update(table, pkVals, set map[string]any)` → (updated bool, err). Sets columns by name; **PK changes move the row** (index entries embed the pk in both forms, so all entries move too); unique collisions/NULL-pk/unknown-column/bad-type rejected.
- **Check-before-write discipline** (`updateRow` in dml.go): phase 1 computes new row/key + all index entry moves and runs every uniqueness check; phase 2 writes. A failed update stages *nothing* — so inside a WriteTxn a caller can catch e.g. a unique violation and still commit sound state (`TestWriteTxnHandledErrorStaysClean`). `insertRow` got the same treatment.
- Unique-check subtlety: on a changed unique entry key, any occupant of the new key must be another row (own entry sits at the unchanged old key) → `enforced && nk != ok && tx.Contains(nk)`.

### Transactions
- `Engine.WriteTxn(fn)` / `Engine.ReadTxn(fn)` over btypedb `Update`/`View` (names `Update`/`View` were taken by DML). Commit on nil, rollback on error/panic; serializable free via single-writer.
- `Txn` snapshots the **catalog** at begin: `maps.Clone(e.tables)` is consistent because DDL clones descriptors, never mutates in place (m2 pattern). Data snapshot from btypedb tx. Own writes visible; scans lock-free.
- DDL cannot run inside a Txn (would deadlock behind the writer lock) — documented, as is "don't call one-shot Engine writes inside WriteTxn".
- Refactor: DML cores `insertRow/updateRow/deleteRow` take a btypedb tx; scans (`scanRows`, `scanIndexRows`) take a `kvView` interface (Get/Contains/Ascend) satisfied by both DB and Tx — Engine one-shots and Txn methods are thin wrappers over identical paths. `rowFromIndexEntry` also takes kvView. scanIndexRows doc: never pass the bare DB (inner Get inside DB-level Ascend = recursive-RLock deadlock risk).

## Milestone 4 (`138d7b1`): column-ID row values + ALTER

- **Row value format change** (pre-release break, no migration): positional tuple → sparse `(colID, value)` pairs, NULLs omitted. `Column` gains engine-assigned `ID uint32` (never reused; `TableDesc.NextColID` tracks). Decode: all-NULL row, pk from key, pairs matched by ID, **unknown IDs skipped**.
- `AddColumn`: metadata-only; old rows read NULL; new arity required on insert; index-on-added-column works (NULL rows land in the pk-suffixed NULL group).
- `DropColumn`: metadata-only; rejects pk + indexed columns (drop index first); **renumbers ordinal refs** (`PKCols`, `IndexDesc.Cols` entries above the dropped ordinal shift down — keys/entries encode values not ordinals, so disk is untouched). `writeDesc` helper: descriptor write + in-callback catalog publish.
- **Key safety property tested**: drop `email` then re-add `email` → fresh ID → stale data reads NULL, never resurfaces (`TestDropColumnAndIDNeverReused`).
- `TestDropColumnOrdinalShift`: drop a column *before* pk+indexed columns; verify insert/get/index-scan/update through a reopen (renumbered descriptor reloads).
- API ripple: `Column{ID}` broke unkeyed literals → converted tests/README to keyed (`{Name: ..., Type: ...}`); one perl-regex miss on `{"a", "jsonb"}` fixed by hand.

## Gotchas / notes
- gopls "BrokenImport/undefined" diagnostics for bytdb files are workspace noise (module not in go.work of the btypedb session root); `go build` is the arbiter.
- Row value `[]byte{}` (all-NULL non-key cols) is valid and distinct from nil handling in tests.
- The engine's row-DML and txn-closure naming: `Update` = row update, `WriteTxn`/`ReadTxn` = transactions.

## State at end of session
- bytdb main = `138d7b1`, pushed, in sync. Tests: engine + tuple packages green under `-race` (~2.7s). Deps unchanged: btypedb v0.3.0, serr v1.3.0, Go 1.26.
- btypedb untouched this stretch (still `b1c4612` / v0.3.0).

## Open threads
- **Milestone 5 scoping conversation (user-requested, pending)**: SQL frontend — go-mysql-server storage interface (full MySQL dialect for free; questions: dependency weight, interface fit over Engine/Txn, how its expected transaction/iterator model maps onto single-writer bytdb) vs hand-rolled SQL subset (small, owns the dialect, months of parser work). Decide, then implement.
- Roadmap after m5: DESC key columns, NOT NULL/CHECK constraints, savepoints, covering indexes, planner with filter pushdown.
- btypedb nice-to-haves for bytdb: savepoints/nested txs, backup/checkpoint API.
