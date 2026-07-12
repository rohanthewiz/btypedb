module github.com/rohanthewiz/btypedb

go 1.26.1

require (
	github.com/rohanthewiz/serr v1.4.0
	// Deliberately pinned: btype is pre-v1 with an unstable API, and our
	// concurrency model relies on verified internals (atomic COW
	// refcounts). Upgrade consciously — see pin_test.go.
	github.com/tidwall/btype v0.3.0
)
