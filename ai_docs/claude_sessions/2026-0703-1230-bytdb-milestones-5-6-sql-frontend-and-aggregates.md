# Session: bytdb-milestones-5-6-sql-frontend-and-aggregates

- **Session ID**: `123070ed-1461-43bc-a7b6-b301963f11ba`
- **Date**: 2026-07-03
- **Continues**: [2026-0703-1133-bytdb-milestones-3-4-txns-and-alter.md](2026-0703-1133-bytdb-milestones-3-4-txns-and-alter.md) (loaded as context; new session)
- **Repo**: `~/projs/go/bytdb` (github.com/rohanthewiz/bytdb)
- **Result**: milestone 5 scoping conversation held and decided (hand-rolled SQL, Postgres-flavored), then milestones 5 and 6 built, tested, and pushed — `0d53f26` (SQL frontend), `18b6233` (aggregates/GROUP BY/HAVING). All tests green under `-race`; verified end-to-end as an external consumer module.

## Milestone 5 scoping decision

- **Hand-rolled SQL engine, Postgres-flavored dialect, embedded API first.** go-mysql-server rejected: heavy dep tree (dolthub vitess fork) against the zero-dep ethos, handle-based txn model mismatched with callback-scoped WriteTxn on a single-writer engine, MySQL type system lossy over ColTypes — and user doesn't care about MySQL.
- **Decisive input from user: future adapter target is Postgres, not MySQL.** No GMS-equivalent exists for Postgres in Go, so the engine must be hand-built either way; the pgwire layer is thin and separable (future `bytdb-pgwire` module with own go.mod using `jeroenrinzema/psql-wire` or `jackc/pgproto3`). ColTypes map 1:1 onto PG OIDs (bool/int8/float8/text/bytea).
- Corrected a prior-session miscalibration: "months of parser work" is for full SQL; the scoped subset was ~a day's work in practice.
- Postgres conventions baked in from day one: `'string'`/`"ident"` quoting with doubled-quote escapes, unquoted idents fold to lowercase, `--` and `/* */` comments, PG type-name aliases, `$1` placeholders lexed but rejected ("not supported yet").

## Milestone 5 (`0d53f26`): sql package

`github.com/rohanthewiz/bytdb/sql`, zero new deps. `bsql.New(engine)` → `Exec(query) (*Result, error)`; `Result{Cols, Types, Rows, RowsAffected}` (deliberately the shape pgwire needs later).

- **Files**: `lexer.go` → `parser.go` (recursive descent over `ast.go`) → `plan.go` → `exec.go`; API+package doc in `sql.go`.
- **Statements**: CREATE/DROP TABLE (inline or table-level PRIMARY KEY), ALTER ADD/DROP COLUMN, CREATE [UNIQUE]/DROP INDEX (`DROP INDEX name` resolves across tables, errors if ambiguous), INSERT (multi-row, optional column list → missing cols NULL), single-table SELECT/UPDATE/DELETE with WHERE/ORDER BY/LIMIT/OFFSET.
- **WHERE model**: flat `[]Pred` (AND-ed) — `col op literal` (either operand order; literal-first flipped at parse) or `IS [NOT] NULL`. `= NULL` rejected with "use IS NULL" hint. OR/expressions deferred; AST will need an expr tree then.
- **Planner** (`planScan`) is the roadmap's filter pushdown: full-PK equality → point `Get`; equality prefix (+ ≤1 range col) of PK or an index → `ScanRange`/`ScanIndex` with pushed inclusive `from` + **executor-side stop conditions** for upper bounds/equality-region ends. **No engine API changes needed** — upper bounds that the []any-bounds API can't express (prefix-end) become stop checks on decoded row values. All predicates always re-checked residually, so pushdown is purely an optimization; correctness never depends on it.
- **NULL-ordering subtlety** (tuple tagNull=0x01 sorts first): range stops must *continue* on NULL (scan may enter at an index's NULL group — dedicated test `TestSQLIndexUpperBoundSkipsNullGroup`); equality-prefix stops safely stop on NULL.
- **Atomicity**: multi-row INSERT/UPDATE/DELETE each in one WriteTxn; UPDATE/DELETE materialize matching PKs before mutating (Halloween-proof). SELECT in ReadTxn.
- **Engine addition**: `Txn.Table(name)` (4 lines, txn.go) so statements plan against their txn's catalog snapshot instead of racing DDL.
- Postgres details: NULLS LAST asc / FIRST desc in ORDER BY; comparisons vs NULL or mismatched kinds are false (no type errors — documented divergence); LIMIT without ORDER BY stops the scan early.

## Milestone 6 (`18b6233`): aggregates

- COUNT(*)/COUNT(col)/SUM/AVG/MIN/MAX, GROUP BY, HAVING, ORDER BY over grouped cols and aggregates. `Select.Cols` became `Items []SelectItem{Col, Agg, Star}`; `OrderItem` embeds SelectItem; `Having []AggPred`.
- **Hash aggregation keyed by `tuple.Encode(groupVals)`** — one stone, three birds: NULL-safe map key, NULLs group together, and byte-sorting the keys emits groups in ascending group-column order for free (the default output order).
- Distinct aggregate calls across select/HAVING/ORDER BY dedupe into shared accumulators (`aggRef{group|acc}` indirection in `agg.go`).
- SQL semantics: aggregates ignore NULL inputs; ungrouped aggregate query returns exactly one row even over zero rows (group created upfront); zero rows + GROUP BY → zero groups; plain cols must appear in GROUP BY; SUM/AVG require numeric (SUM int→int64, float→float64; AVG always float64); MIN/MAX any comparable type; aggregates in WHERE → error pointing at HAVING.
- Ident sharing an agg name stays a column unless `(` follows (one-token lookahead `peekOp`).
- `checkPred` (agg.go) now the single comparison evaluator; exec.go `matches` delegates to it.

## Verification pattern (worth repeating)

Per the verify skill: don't re-run tests as "verification" — drove the library surface as a real external consumer: scratchpad module with `go mod edit -replace` to the local repo, exercising happy path, error probes (syntax, `= NULL`, `$1`, arity, two-statements), rollback atomicity, and **persistence across engine reopen**. Finding worth carrying: serr's `%v` prints only the message; the structured fields (`want`/`got`/`pos`) need serr's field rendering — the future pgwire/REPL layer must fold fields into client-facing messages.

## State at end of session

- bytdb main = `18b6233`, pushed, in sync. sql package ~1,500 lines + ~1,000 test lines. Tests green under `-race` (~2s sql pkg). Deps unchanged: btypedb v0.3.0, serr v1.3.0, Go 1.26.
- btypedb untouched (still `b1c4612` / v0.3.0 + this session doc).
- gopls diagnostics for bytdb files remain workspace noise (module not in session go.work); `go build` is the arbiter.

## Open threads / next

- **Deferred SQL, roughly in order**: OR + expression trees in WHERE (will replace flat `[]Pred`), joins, prepared statements (`$1` lexes already), `bytdb-pgwire` module (autocommit-only first; single-writer + long-held wire txns needs thought; `pg_catalog` emulation is the ORM long tail).
- Engine roadmap: DESC key columns (byte inversion), CHECK/NOT NULL constraints, savepoints, EXPLAIN.
- btypedb nice-to-haves for bytdb: savepoints/nested txs, backup/checkpoint API; handle-based txns if pgwire ever wants interactive transactions.
