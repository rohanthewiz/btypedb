package btypedb

import (
	"os"
	"strings"
	"testing"
)

// pinnedBtype is the btype version this package has been verified
// against. The pin matters beyond ordinary API stability: the
// transaction and snapshot design relies on btype internals confirmed
// by source inspection — reference counts are updated atomically, and
// copy-on-write mutation never modifies shared nodes in place, which is
// what makes lock-free snapshot reads sound. Neither property is part
// of btype's documented contract.
const pinnedBtype = "github.com/tidwall/btype v0.3.0"

// TestBtypePinned fails when go.mod drifts from the verified btype
// version. To upgrade: re-inspect btype's COW refcounting (atomic rc
// updates, no in-place mutation of shared nodes), run the full suite
// including the race hammer and power-loss harness, then update
// pinnedBtype above.
func TestBtypePinned(t *testing.T) {
	mod, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(mod), pinnedBtype) {
		t.Fatalf("go.mod no longer pins %q — btype is pre-v1 and this package depends on "+
			"verified COW internals; re-verify before bumping (see comment on pinnedBtype)", pinnedBtype)
	}
}
