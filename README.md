# btypedb

An embedded, pure-Go, memory-resident key-value database with disk
durability, built on the copy-on-write B-tree collections of
[tidwall/btype](https://github.com/tidwall/btype).

**Status: phase 1 scaffold — experimental.** The dataset lives entirely in
memory (a `btype.Map`); durability comes from an append-only write-ahead
log replayed on open. Think BuntDB-with-generics, not bbolt: your data
must fit in RAM.

## Usage

```go
import "github.com/rohanthewiz/btypedb"

type User struct {
    Name string `json:"name"`
    Age  int    `json:"age"`
}

db, err := btypedb.Open("users.db", btypedb.StringCodec, btypedb.JSONCodec[User]())
if err != nil { /* ... */ }
defer db.Close()

err = db.Set("ada", User{Name: "Ada", Age: 36})
u, ok := db.Get("ada")

// Keys are always sorted — range scans come free from the B-tree.
for k, u := range db.Ascend("m") { // every key >= "m", ascending
    _ = k
    _ = u
}
for k, u := range db.Descend("m") { /* every key <= "m", descending */ }
for k, u := range db.Backward() { /* all pairs, descending */ }
for k := range db.Keys() { _ = k }   // ascending keys
for u := range db.Values() { _ = u } // values in key order

n := db.Len()      // stored keys (expired-but-unswept included)
n = db.LiveLen()   // unexpired keys only
```

Codecs only define the on-disk encoding; in-memory ordering is the key
type's natural `cmp.Ordered` ordering. Built-ins: `StringCodec`,
`BytesCodec`, `Int64Codec`, `Uint64Codec`, `JSONCodec[T]()`.

### Transactions

Transactions run against O(1) copy-on-write snapshots of the B-tree:

```go
// Read-only: a frozen, lock-free view — later commits are invisible to it.
err = db.View(func(tx *btypedb.Tx[string, User]) error {
    u, ok := tx.Get("ada")
    for k, v := range tx.All() { /* consistent snapshot */ }
    return nil
})

// Writable: stages changes privately, then commits them atomically —
// one batched log append + fsync, one root-pointer swap.
err = db.Update(func(tx *btypedb.Tx[string, User]) error {
    if err := tx.Set("grace", User{Name: "Grace", Age: 45}); err != nil {
        return err
    }
    _, err := tx.Delete("ada")
    return err // non-nil → rollback
})

// Or manage explicitly:
tx, err := db.Begin(true) // true = writable
// ... tx.Set / tx.Delete / tx.Get ...
err = tx.Commit()         // or tx.Rollback()
```

Writable transactions serialize with each other (single writer) and hold
their writes invisibly until Commit. Readers never block and never take
locks while iterating. Multi-op commits are framed as an atomic batch in
the log, so crash recovery applies a transaction all-or-nothing.

### TTL

```go
err = db.SetTTL("session:42", sess, 30*time.Minute) // expires in 30m
d, ok := db.TTL("session:42")                       // remaining time
```

Expired keys become invisible to reads immediately and are physically
removed by a background sweeper (default every 500ms; tune or disable
with `WithSweepInterval`). Deadlines are stored absolutely in the log,
so they survive restarts; keys that expire while the database is closed
are gone on reopen. A plain `Set` clears any TTL.

### Secondary indexes

An index is an extra sort order over the same pairs, defined by a
comparator and maintained atomically with every commit:

```go
err = db.CreateIndex("by-age", func(ak string, av User, bk string, bv User) int {
    return cmp.Compare(av.Age, bv.Age)
})
for k, u := range db.AscendIndex("by-age") { /* youngest first */ }
for k, u := range db.DescendIndex("by-age") { /* oldest first */ }
for k, u := range db.AscendIndexFrom("by-age", "", User{Age: 40}) { /* 40+ */ }
for k, u := range db.DescendIndexFrom("by-age", "", User{Age: 40}) { /* ≤40, oldest of those first */ }
```

Transactions see indexes transactionally: a write tx queries its own
uncommitted updates via `tx.AscendIndex`, and rollback discards index
changes with everything else. Comparators can't be persisted, so
re-register indexes after each `Open` (the build scans existing data).

### Range deletes

```go
n, err := db.DeleteRange("user:", "user;") // [min, max), returns count
```

Runs as one atomic transaction — a single batched log append that
replays all-or-nothing. Also available as `tx.DeleteRange`.

### Sync policies

```go
btypedb.Open(path, kc, vc, btypedb.WithSyncPolicy(btypedb.SyncEverySecond))
```

- `SyncAlways` (default) — fsync before acking every write; durable to
  the last acknowledged op
- `SyncEverySecond` — background fsync; a crash loses ≤1s of writes
- `SyncNever` — the OS decides

Under `SyncAlways`, concurrent committers share fsyncs (**group
commit**): each write is acknowledged only once an fsync covers it, but
one fsync releases every writer queued behind it, so N concurrent
writers cost far fewer than N fsyncs. The one visible consequence: a
committed write becomes readable slightly before its fsync completes,
so a concurrent reader can briefly observe a write that a power cut in
that window would lose. The writer itself is never acknowledged until
the data is on disk.

### Compaction

The log grows with every write; compaction rewrites it as a minimal
snapshot of live data, dropping overwritten and deleted records. It runs
in the background by default once the log is ≥32 MB and has doubled since
the last compaction:

```go
btypedb.Open(path, kc, vc, btypedb.WithAutoCompact(8<<20, 50)) // ≥8MB, +50% growth
btypedb.Open(path, kc, vc, btypedb.WithAutoCompactDisabled())  // manual only
err = db.Compact()                                             // on demand
```

Writers pause only twice, briefly: to take the O(1) snapshot, and to
splice in ops committed during streaming before an atomic rename swaps
the compacted file in. A crash at any point leaves either the old or the
new complete log — a leftover `.compact` temp file is discarded on open.

## Log format

Single append-only file of framed records:

```
op(1) | klen(4) | vlen(4) | key | val | crc32(4)
```

Ops are `set(1)`, `delete(2)`, `batch(3)` — a batch header (val =
uint64 count) marks the next N records as one atomic transaction — and
`setttl(4)`, whose value bytes are prefixed with the absolute expiry
deadline (8 bytes, unix nanos).

On open the log is replayed into the B-tree. A torn or CRC-failing record
(crash mid-append) marks the end of valid data; the tail is truncated and
the database continues from the last good record. A batch is applied
all-or-nothing: a tear anywhere inside it discards the whole group.

## Roadmap

- [x] **Phase 1**: Open/Close, Get/Set/Delete, WAL append + replay, torn-tail recovery, sync policies, ordered iteration
- [x] **Phase 2**: transactions via btype's O(1) COW snapshots (`Begin/Commit/Rollback`, `View`/`Update`), lock-free read snapshots, atomic batch commits, batched fsync
- [x] **Phase 3**: background + manual compaction (snapshot rewrite with atomic swap), SIGKILL crash-test suite
- [x] **Phase 4**: secondary indexes, atomic range-delete, TTL with background sweeper (deadline-ordered `btype.Table`)
- [x] **v0.1.0 prep**: btype version pin with guard test, power-loss fault-injection harness, `Keys()`/`Values()` iterators
- [x] **v0.2.0 prep**: `Descend`/`Backward` reverse iteration, expired-aware `LiveLen`, group commit (fsync coalescing across concurrent writers), power-loss fault injection for the compaction temp-file/rename path
- [x] **v0.3.0 prep**: `DescendIndexFrom` descending index pivots; compaction power-loss harness hardened with torn temp-file content and writes racing the compaction into the spliced tail

## Crash safety testing

Two layers beyond ordinary unit tests:

- **SIGKILL suite** (`crash_test.go`): the test binary re-execs itself as
  a write-hammering child and kills it at varied points, verifying
  recovery and transaction atomicity. Kills don't lose the OS page
  cache, so this exercises replay and the compaction swap window — not
  fsync ordering.
- **Power-loss harness** (`powerfail_test.go`): a fault-injecting log
  file models durable-vs-unsynced bytes with `Sync` as the promotion
  point. The durability test asserts that with `SyncAlways` every
  acknowledged op survives a cut at every ack boundary *exactly* (this
  catches ack-before-fsync and apply-before-log ordering bugs), plus
  torn mid-record cuts and garbage tails. The consistency test opens
  every byte-length prefix of a real log and checks transactions are
  applied all-or-nothing.
- **Compaction power-loss harness** (`powerfailfs_test.go`): a
  fault-injecting *filesystem* extends the model to directory metadata —
  file contents are durable only after the file's fsync, and
  create/rename/remove only after the directory's. Power is cut at
  every operation boundary of a compaction under both a
  metadata-conservative and a metadata-eager assumption, plus torn
  variants where the compaction temp file keeps only a torn prefix of
  its unsynced content; every cut must recover exactly the state acked
  at that point. Writes race the compaction itself, landing in the log
  tail that gets spliced into the compacted file — so a tail that rides
  unsynced through the rename is caught — and writes acknowledged after
  the compaction must survive even if no later metadata persisted
  (verifying the temp file is fsynced before the rename, and the
  directory after it).

## Dependency pin

`tidwall/btype` is pre-v1 with an unstable API, and this package's
lock-free snapshot reads rely on btype internals verified by source
inspection (atomic COW refcounts; shared nodes are copied, never mutated
in place) that aren't part of its documented contract. The version is
deliberately pinned; `TestBtypePinned` fails on drift. To upgrade:
re-inspect those properties, run the full suite (race hammer + crash +
power-loss tests), then bump the pin in `go.mod` and `pin_test.go`.
