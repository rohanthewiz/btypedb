package btypedb

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

const crashChildEnv = "BTYPEDB_CRASH_PATH"

// TestCrashRecovery repeatedly SIGKILLs a child process that is
// hammering the database with transactions, direct writes, deletes, and
// aggressive auto-compaction, then verifies after every kill that the
// database opens and every transaction was applied all-or-nothing.
func TestCrashRecovery(t *testing.T) {
	if path := os.Getenv(crashChildEnv); path != "" {
		crashChild(path) // never returns
	}
	if testing.Short() {
		t.Skip("crash test skipped in -short mode")
	}
	if runtime.GOOS == "windows" {
		t.Skip("crash test relies on POSIX kill and rename semantics")
	}

	path := filepath.Join(t.TempDir(), "crash.db")
	for round := range 6 {
		cmd := exec.Command(os.Args[0], "-test.run=^TestCrashRecovery$")
		cmd.Env = append(os.Environ(), crashChildEnv+"="+path)
		var out strings.Builder
		cmd.Stdout, cmd.Stderr = &out, &out
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		// Vary the kill point so deaths land during commits, direct
		// writes, and compaction phases alike.
		time.Sleep(time.Duration(60+round*45) * time.Millisecond)
		cmd.Process.Kill()
		cmd.Wait()

		verifyCrashDB(t, path, round, out.String())
	}
}

// crashChild writes forever until killed: batch transactions of 8 keys,
// direct sets, periodic deletes, with tiny compaction thresholds so
// kills frequently land mid-compaction.
func crashChild(path string) {
	db, err := Open(path, StringCodec, StringCodec,
		WithSyncPolicy(SyncNever), WithAutoCompact(8<<10, 20))
	if err != nil {
		fmt.Fprintf(os.Stderr, "child open: %v\n", err)
		os.Exit(2)
	}
	for round := 0; ; round++ {
		group := round % 300 // wrap so overwrites create dead records
		val := fmt.Sprintf("%d", round)
		err := db.Update(func(tx *Tx[string, string]) error {
			for j := range 8 {
				if err := tx.Set(fmt.Sprintf("b:%04d:%d", group, j), val); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "child update: %v\n", err)
			os.Exit(2)
		}
		if err := db.Set(fmt.Sprintf("d:%04d", round%200), val); err != nil {
			fmt.Fprintf(os.Stderr, "child set: %v\n", err)
			os.Exit(2)
		}
		if round%10 == 9 {
			if _, err := db.Delete(fmt.Sprintf("d:%04d", (round-5)%200)); err != nil {
				fmt.Fprintf(os.Stderr, "child delete: %v\n", err)
				os.Exit(2)
			}
		}
	}
}

// verifyCrashDB opens the database after a kill and checks the
// transactional invariant: every batch group "b:NNNN:*" present must
// have all 8 members carrying the same value — a partial group means a
// transaction was torn in half.
func verifyCrashDB(t *testing.T, path string, round int, childOut string) {
	t.Helper()
	db, err := Open(path, StringCodec, StringCodec)
	if err != nil {
		t.Fatalf("round %d: reopen after kill: %v\nchild output:\n%s", round, err, childOut)
	}
	defer db.Close()

	type group struct {
		members int
		values  map[string]bool
	}
	groups := map[string]*group{}
	total := 0
	for k, v := range db.All() {
		total++
		if !strings.HasPrefix(k, "b:") {
			continue
		}
		id := k[:strings.LastIndex(k, ":")]
		g := groups[id]
		if g == nil {
			g = &group{values: map[string]bool{}}
			groups[id] = g
		}
		g.members++
		g.values[v] = true
	}
	for id, g := range groups {
		if g.members != 8 {
			t.Fatalf("round %d: group %s has %d/8 members: transaction torn\nchild output:\n%s",
				round, id, g.members, childOut)
		}
		if len(g.values) != 1 {
			t.Fatalf("round %d: group %s has mixed values %v: transaction interleaved\nchild output:\n%s",
				round, id, g.values, childOut)
		}
	}
	if round > 0 && total == 0 {
		t.Fatalf("round %d: database empty after prior rounds wrote data\nchild output:\n%s",
			round, childOut)
	}

	// The recovered DB must accept and persist new writes.
	probe := fmt.Sprintf("probe:%d", round)
	if err := db.Set(probe, "ok"); err != nil {
		t.Fatalf("round %d: write after recovery: %v", round, err)
	}
	if v, ok := db.Get(probe); !ok || v != "ok" {
		t.Fatalf("round %d: probe readback = %q, %v", round, v, ok)
	}
}
