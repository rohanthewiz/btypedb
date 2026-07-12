package btypedb

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestBackupBasic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	bak := filepath.Join(dir, "test.backup")

	db := openStr(t, path, WithSweepInterval(0))
	defer db.Close()
	for i := range 10 {
		if err := db.Set(key2(i), val2(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Backup(bak); err != nil {
		t.Fatal(err)
	}
	// Writes after the backup must not appear in it.
	if err := db.Set("post", "backup"); err != nil {
		t.Fatal(err)
	}

	raw, err := os.ReadFile(bak)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(raw, []byte(logMagic)) {
		t.Fatal("backup is not a current-format database file")
	}
	if _, err := os.Stat(bak + backupSuffix); !os.IsNotExist(err) {
		t.Fatal("backup temp file left behind")
	}

	db2 := openStr(t, bak, WithSweepInterval(0))
	defer db2.Close()
	checkRows(t, db2, 10)
	if _, ok := db2.Get("post"); ok {
		t.Fatal("backup contains a write made after it was taken")
	}
}

func TestBackupDuringConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	bak := filepath.Join(dir, "test.backup")

	db := openStr(t, path, WithSyncPolicy(SyncNever), WithSweepInterval(0))
	defer db.Close()

	// Seed rows that must all be in the backup.
	for i := range 50 {
		if err := db.Set(key2(i), val2(i)); err != nil {
			t.Fatal(err)
		}
	}

	// Hammer multi-op transactions while the backup streams; each group
	// must appear in the backup all-or-nothing.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for r := 0; ; r++ {
			select {
			case <-stop:
				return
			default:
			}
			err := db.Update(func(tx *Tx[string, string]) error {
				for j := range 4 {
					if err := tx.Set(fmt.Sprintf("g:%04d:%d", r, j), fmt.Sprintf("%d", r)); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				t.Error(err)
				return
			}
		}
	}()

	if err := db.Backup(bak); err != nil {
		t.Fatal(err)
	}
	close(stop)
	wg.Wait()

	db2 := openStr(t, bak, WithSweepInterval(0))
	defer db2.Close()
	for i := range 50 {
		if v, ok := db2.Get(key2(i)); !ok || v != val2(i) {
			t.Fatalf("seed row %d missing from backup: %q,%v", i, v, ok)
		}
	}
	// Whatever transaction groups made it in must be whole and uniform.
	groups := map[string]int{}
	for k := range db2.All() {
		if len(k) > 2 && k[:2] == "g:" {
			groups[k[:6]]++
		}
	}
	for id, n := range groups {
		if n != 4 {
			t.Fatalf("group %s has %d/4 members in the backup: torn transaction", id, n)
		}
	}
}
