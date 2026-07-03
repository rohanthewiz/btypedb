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

### Sync policies

```go
btypedb.Open(path, kc, vc, btypedb.WithSyncPolicy(btypedb.SyncEverySecond))
```

- `SyncAlways` (default) — fsync every write; durable to the last op
- `SyncEverySecond` — background fsync; a crash loses ≤1s of writes
- `SyncNever` — the OS decides

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

Ops are `set(1)`, `delete(2)`, and `batch(3)` — a batch header (val =
uint64 count) marks the next N records as one atomic transaction.

On open the log is replayed into the B-tree. A torn or CRC-failing record
(crash mid-append) marks the end of valid data; the tail is truncated and
the database continues from the last good record. A batch is applied
all-or-nothing: a tear anywhere inside it discards the whole group.

## Roadmap

- [x] **Phase 1**: Open/Close, Get/Set/Delete, WAL append + replay, torn-tail recovery, sync policies, ordered iteration
- [x] **Phase 2**: transactions via btype's O(1) COW snapshots (`Begin/Commit/Rollback`, `View`/`Update`), lock-free read snapshots, atomic batch commits, batched fsync
- [x] **Phase 3**: background + manual compaction (snapshot rewrite with atomic swap), SIGKILL crash-test suite
- [ ] **Phase 4**: secondary indexes, range-delete, TTL via `btype.Prique`
