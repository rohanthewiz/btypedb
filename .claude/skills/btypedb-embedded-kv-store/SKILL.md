---
name: btypedb-embedded-kv-store
description: "btypedb is an embedded, typed, ordered key-value store for Go — WAL durability with crash recovery, ACID transactions over O(1) COW snapshots, TTL, secondary indexes, and background compaction, all in a single file"
---

# btypedb - Embedded Typed Key-Value Store for Go

btypedb is a single-file, embedded key-value database generic over key and value types (`DB[K cmp.Ordered, V any]`). Keys are kept in sorted order in an in-memory B-tree; every write is appended to a write-ahead log and replayed on open. No server, no CGO, one dependency tier (tidwall/btype + serr).

## Key Philosophy

- Typed end-to-end: `Get`/`Set` take and return your K and V, no `[]byte` casts
- The full dataset lives in memory (B-tree); the log on disk is for durability, not paging — right for datasets that fit in RAM
- Transactions snapshot the entire DB in O(1) via copy-on-write; readers never block writers
- Crash safety is a contract: with `SyncAlways` (default) every acknowledged write survives power loss, verified by fault-injection harnesses

## Import

```go
import "github.com/rohanthewiz/btypedb"
```

## Opening and Closing

```go
db, err := btypedb.Open("app.db", btypedb.StringCodec, btypedb.JSONCodec[User]())
if err != nil { /* handle */ }
defer db.Close() // syncs and closes; idempotent
```

Open replays the log into memory. A torn or corrupt tail record (crash mid-append) is truncated automatically; recovery is not an error.

### Codecs

Codecs define only the on-disk encoding — in-memory ordering always comes from the key type's natural `cmp.Ordered` order.

```go
btypedb.StringCodec      // strings as raw bytes
btypedb.BytesCodec       // []byte pass-through (decode clones)
btypedb.Int64Codec       // 8 fixed bytes, little-endian
btypedb.Uint64Codec      // 8 fixed bytes, little-endian
btypedb.JSONCodec[T]()   // any type via encoding/json — the usual choice for struct values
```

### Options

```go
btypedb.WithSyncPolicy(btypedb.SyncEverySecond) // default SyncAlways
btypedb.WithAutoCompact(8<<20, 50)              // compact at ≥8MB and +50% growth (default 32MB, +100%)
btypedb.WithAutoCompactDisabled()               // manual Compact() only
btypedb.WithSweepInterval(time.Minute)          // expired-key sweeper cadence; 0 disables
```

## Basic Operations

```go
err = db.Set("alice", User{Age: 36})     // insert or overwrite (clears any TTL)
u, ok := db.Get("alice")                 // zero value + false if absent/expired
existed, err := db.Delete("alice")       // reports whether the key was visible
ok = db.Contains("alice")
n := db.Len()                            // includes expired-but-unswept keys
n = db.LiveLen()                         // unexpired only (costs O(expired-unswept))
```

## TTL

```go
err = db.SetTTL("session:9", sess, 30*time.Minute)
remaining, ok := db.TTL("session:9") // false if absent, expired, or no TTL
```

Expired keys read as absent immediately; a background sweeper removes them physically. TTLs are absolute deadlines persisted in the log — keys that expire while the DB is closed are gone on reopen. A plain `Set` clears any TTL.

## Iteration (Go 1.23 range-over-func)

```go
for k, v := range db.All() {}           // ascending key order
for k, v := range db.Backward() {}      // descending
for k, v := range db.Ascend("m") {}     // from first key >= "m"
for k, v := range db.Descend("m") {}    // from last key <= "m", downward
for k := range db.Keys() {}
for v := range db.Values() {}
```

All iterators skip expired keys. **DB-level iteration holds a read lock for the whole loop** — keep loop bodies short and never write to the DB from inside one (deadlock). For long scans, iterate inside `View` instead: a transaction's iterators run lock-free over its private snapshot.

## Transactions

O(1) copy-on-write snapshot of the entire database. Read transactions are cheap and never block anything; one writable transaction runs at a time.

```go
// Managed (preferred): Commit on nil return, Rollback on error or panic.
err = db.Update(func(tx *btypedb.Tx[string, User]) error {
    u, _ := tx.Get("alice")
    u.Age++
    return tx.Set("alice", u) // visible to tx now, to others after Commit
})

err = db.View(func(tx *btypedb.Tx[string, User]) error {
    for k, v := range tx.All() {} // consistent point-in-time scan, lock-free
    return nil
})

// Manual
tx, err := db.Begin(true) // writable; false = read-only
defer tx.Rollback()       // safe no-op after Commit
// ... tx.Set / tx.Delete / tx.Get ...
err = tx.Commit() // one atomic batch append; replays all-or-nothing after a crash
```

`Tx` mirrors the whole DB API: `Get/Set/SetTTL/TTL/Delete/DeleteRange/Contains/Len/LiveLen`, all iterators, and index queries. Rules:

- Every `Begin` must end in `Commit` or `Rollback`, or the snapshot leaks
- **Never call `db.Set` (or any direct DB write) inside `Update`** — direct writes block while a writable tx is open → self-deadlock; use the `tx` methods
- Writes through a read-only tx return `ErrTxNotWritable`; a finished tx returns `ErrTxClosed`

## Secondary Indexes

Extra sort orders over the same pairs, maintained atomically with every commit. Ties fall back to primary-key order.

```go
err = db.CreateIndex("by-age", func(ak string, av User, bk string, bv User) int {
    return cmp.Compare(av.Age, bv.Age)
})
for k, u := range db.AscendIndex("by-age") {}                       // youngest first
for k, u := range db.DescendIndex("by-age") {}                      // oldest first
for k, u := range db.AscendIndexFrom("by-age", "", User{Age: 40}) {}  // first entry >= pivot
for k, u := range db.DescendIndexFrom("by-age", "", User{Age: 40}) {} // last entry <= pivot, downward
db.Indexes()            // sorted names
db.DropIndex("by-age")
```

- **Indexes are not persisted** (comparators can't be) — re-register after every `Open`; the build scans existing data
- Unknown index names iterate nothing — check `Indexes()` if unsure
- Pivot entries compare through your comparator, so a pivot can set only the fields the comparator reads
- Transactions query their own uncommitted index state via `tx.AscendIndex` etc.

## Range Deletes

```go
n, err := db.DeleteRange("user:", "user;") // half-open [min, max), returns count
```

One atomic transaction (single batched log append). Also on `Tx`.

## Sync Policies and Durability

- `SyncAlways` (default) — fsync before acking every write; durable to the last acknowledged op. Concurrent writers share fsyncs (group commit), so N goroutines cost far fewer than N fsyncs. One caveat: a committed write is readable by others slightly before its fsync lands; the writer itself is never acked until data is on disk.
- `SyncEverySecond` — background fsync; a crash loses ≤1s of acked writes
- `SyncNever` — the OS decides

`db.Sync()` forces an fsync on demand. After a genuine sync/append failure the DB goes read-only and returns the sticky error on writes.

## Compaction

The log grows with every write; compaction rewrites it as a minimal snapshot of live data. Automatic by default (≥32MB and doubled since last compaction); `db.Compact()` runs one on demand. Writers pause only briefly; a crash at any point leaves either the old or the new complete log (atomic rename; leftover `.compact` temp files are discarded on open).

## Common Patterns

### Struct store keyed by string

```go
type User struct {
    Name string `json:"name"`
    Age  int    `json:"age"`
}
db, err := btypedb.Open("users.db", btypedb.StringCodec, btypedb.JSONCodec[User]())
```

### Key-prefix scan (string keys)

```go
for k, v := range db.Ascend("user:") {
    if !strings.HasPrefix(k, "user:") { break }
    // ...
}
```

### Read-modify-write, atomically

```go
err = db.Update(func(tx *btypedb.Tx[string, int64]) error {
    n, _ := tx.Get("counter")
    return tx.Set("counter", n+1)
})
```

## Gotchas

- One process per database file; `DB` itself is safe for concurrent use by many goroutines
- Whole dataset must fit in memory
- `Len` counts expired-unswept keys; use `LiveLen` when TTLs are in play
- Errors are serr-wrapped — log with `logger.LogErr(err)` to get full context (see the serr/logger skills)
- Sentinels: `ErrClosed`, `ErrTxClosed`, `ErrTxNotWritable` (check with `errors.Is`)
