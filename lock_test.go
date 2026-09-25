package btypedb

import (
	"bufio"
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"

	"github.com/rohanthewiz/serr"
)

// TestLockSameProcess: a second DB on an open path is refused with
// ErrLocked (naming this process as holder), and Close frees the path —
// including after a compaction has renamed a fresh file over it, the
// case that rules out locking the database file itself.
func TestLockSameProcess(t *testing.T) {
	if !lockingSupported() {
		t.Skip("no file locking on " + runtime.GOOS)
	}
	path := filepath.Join(t.TempDir(), "test.db")
	db := openStr(t, path)
	if err := db.Set("k", "v"); err != nil {
		t.Fatal(err)
	}

	_, err := Open(path, StringCodec, StringCodec)
	if !errors.Is(err, ErrLocked) {
		t.Fatalf("second Open: want ErrLocked, got %v", err)
	}
	if got := fieldOf(err, "holder_pid"); got != strconv.Itoa(os.Getpid()) {
		t.Errorf("holder_pid = %q, want %d", got, os.Getpid())
	}

	// Compaction swaps the database file's inode; the sidecar lock must
	// still exclude.
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path, StringCodec, StringCodec); !errors.Is(err, ErrLocked) {
		t.Fatalf("Open after compaction: want ErrLocked, got %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	db2 := openStr(t, path)
	defer db2.Close()
	if v, ok := db2.Get("k"); !ok || v != "v" {
		t.Fatalf("reopened Get = %q, %v; want v, true", v, ok)
	}
}

// TestLockSymlink: a symlink to an open database resolves to the same
// sidecar, so it cannot sneak a second DB onto the file.
func TestLockSymlink(t *testing.T) {
	if !lockingSupported() {
		t.Skip("no file locking on " + runtime.GOOS)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db := openStr(t, path)
	defer db.Close()

	link := filepath.Join(dir, "link.db")
	if err := os.Symlink(path, link); err != nil {
		t.Skip("cannot create symlink: " + err.Error())
	}
	if _, err := Open(link, StringCodec, StringCodec); !errors.Is(err, ErrLocked) {
		t.Fatalf("Open via symlink: want ErrLocked, got %v", err)
	}
}

// TestLockReleasedOnFailedOpen: an Open that fails after taking the
// lock (here, a key supplied for a plaintext file) gives it back, so the
// retry without the key is not refused by the failed attempt's ghost.
func TestLockReleasedOnFailedOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	if err := openStr(t, path).Close(); err != nil {
		t.Fatal(err)
	}

	key := bytes.Repeat([]byte{7}, 32)
	if _, err := Open(path, StringCodec, StringCodec, WithEncryptionKey(key)); !errors.Is(err, ErrNotEncrypted) {
		t.Fatalf("keyed Open of plaintext db: want ErrNotEncrypted, got %v", err)
	}
	openStr(t, path).Close()
}

// TestLockBackupDestination: Backup refuses a destination that is a
// live database — another DB's, or its own path — and leaves it
// untouched; once the destination's DB closes, the same Backup works.
func TestLockBackupDestination(t *testing.T) {
	if !lockingSupported() {
		t.Skip("no file locking on " + runtime.GOOS)
	}
	dir := t.TempDir()
	src := openStr(t, filepath.Join(dir, "src.db"))
	defer src.Close()
	if err := src.Set("from", "src"); err != nil {
		t.Fatal(err)
	}

	destPath := filepath.Join(dir, "dest.db")
	dest := openStr(t, destPath)
	if err := dest.Set("from", "dest"); err != nil {
		t.Fatal(err)
	}

	if err := src.Backup(destPath); !errors.Is(err, ErrLocked) {
		t.Fatalf("Backup onto a live db: want ErrLocked, got %v", err)
	}
	if err := src.Backup(filepath.Join(dir, "src.db")); !errors.Is(err, ErrLocked) {
		t.Fatalf("Backup onto its own path: want ErrLocked, got %v", err)
	}
	if _, err := os.Stat(destPath + backupSuffix); !os.IsNotExist(err) {
		t.Fatalf("refused Backup left a temp file behind (stat err %v)", err)
	}

	// The refused backup must not have swapped dest's file: its own
	// write, made after the attempt, has to survive a reopen.
	if err := dest.Set("after", "attempt"); err != nil {
		t.Fatal(err)
	}
	if err := dest.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openStr(t, destPath)
	if v, _ := reopened.Get("from"); v != "dest" {
		t.Fatalf("dest after refused Backup: from = %q, want dest", v)
	}
	if _, ok := reopened.Get("after"); !ok {
		t.Fatal("dest lost a write made after the refused Backup")
	}
	reopened.Close()

	// With dest closed, the path is free and the backup goes through.
	if err := src.Backup(destPath); err != nil {
		t.Fatalf("Backup onto a closed db: %v", err)
	}
	restored := openStr(t, destPath)
	defer restored.Close()
	if v, _ := restored.Get("from"); v != "src" {
		t.Fatalf("backup copy: from = %q, want src", v)
	}
}

// TestAcquireLock: the exported lock and Open's are one lock, in both
// directions, and closing it frees the path.
func TestAcquireLock(t *testing.T) {
	if !lockingSupported() {
		t.Skip("no file locking on " + runtime.GOOS)
	}
	path := filepath.Join(t.TempDir(), "test.db")

	db := openStr(t, path)
	if _, err := AcquireLock(path); !errors.Is(err, ErrLocked) {
		t.Fatalf("AcquireLock on an open db: want ErrLocked, got %v", err)
	}
	db.Close()

	l, err := AcquireLock(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path, StringCodec, StringCodec); !errors.Is(err, ErrLocked) {
		t.Fatalf("Open while AcquireLock holds it: want ErrLocked, got %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	openStr(t, path).Close()
}

// lockHelperEnv switches TestLockHelperProcess from a no-op into the
// child side of the cross-process tests; its value is the database path.
const lockHelperEnv = "BTYPEDB_LOCK_HELPER_DB"

// TestLockHelperProcess is not a test on its own: the cross-process tests
// re-run this binary with lockHelperEnv set, and then it opens the
// database, reports the outcome on stdout, and holds the DB open until
// stdin closes (or it is killed).
func TestLockHelperProcess(t *testing.T) {
	path := os.Getenv(lockHelperEnv)
	if path == "" {
		t.Skip("helper for the cross-process lock tests")
	}
	db, err := Open(path, StringCodec, StringCodec)
	switch {
	case errors.Is(err, ErrLocked):
		os.Stdout.WriteString("locked " + fieldOf(err, "holder_pid") + "\n")
		os.Exit(0)
	case err != nil:
		os.Stdout.WriteString("error " + err.Error() + "\n")
		os.Exit(1)
	}
	os.Stdout.WriteString("opened\n")
	bufio.NewReader(os.Stdin).ReadString('\n') // hold until the parent lets go
	db.Close()
	os.Exit(0)
}

// startHelper launches the helper child on path and returns its first
// output line, the running command, and a func that releases it.
func startHelper(t *testing.T, path string) (string, *exec.Cmd, func()) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestLockHelperProcess$")
	cmd.Env = append(os.Environ(), lockHelperEnv+"="+path)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	line, _ := bufio.NewReader(stdout).ReadString('\n')
	return line, cmd, func() { stdin.Close(); cmd.Wait() }
}

// TestLockCrossProcess: a second process opening a live database is
// refused and told who holds it.
func TestLockCrossProcess(t *testing.T) {
	if !lockingSupported() {
		t.Skip("no file locking on " + runtime.GOOS)
	}
	path := filepath.Join(t.TempDir(), "test.db")
	db := openStr(t, path)
	defer db.Close()

	line, _, done := startHelper(t, path)
	done()
	if want := "locked " + strconv.Itoa(os.Getpid()) + "\n"; line != want {
		t.Fatalf("child: got %q, want %q", line, want)
	}
}

// TestLockReleasedOnKill: the lock is the OS's, not the sidecar's
// contents, so a holder killed without Close leaves nothing that blocks
// the next Open — no stale-lock cleanup is ever needed.
func TestLockReleasedOnKill(t *testing.T) {
	if !lockingSupported() {
		t.Skip("no file locking on " + runtime.GOOS)
	}
	path := filepath.Join(t.TempDir(), "test.db")

	line, cmd, done := startHelper(t, path)
	if line != "opened\n" {
		done()
		t.Fatalf("child: got %q, want opened", line)
	}
	if _, err := Open(path, StringCodec, StringCodec); !errors.Is(err, ErrLocked) {
		done()
		t.Fatalf("Open while child holds it: want ErrLocked, got %v", err)
	}

	cmd.Process.Kill()
	done()

	openStr(t, path).Close()
}

func lockingSupported() bool {
	switch runtime.GOOS {
	case "darwin", "dragonfly", "freebsd", "linux", "netbsd", "openbsd", "windows":
		return true
	}
	return false
}

// fieldOf returns the value of a structured field on a serr error.
func fieldOf(err error, key string) string {
	flds := serr.SErrFromErr(err).UserFields()
	for i := 0; i+1 < len(flds); i += 2 {
		if flds[i] == key {
			return flds[i+1]
		}
	}
	return ""
}
