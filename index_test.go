package btypedb

import (
	"cmp"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

type person struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func openPeople(t *testing.T, path string, opts ...Option) *DB[string, person] {
	t.Helper()
	db, err := Open(path, StringCodec, JSONCodec[person](), opts...)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func byAge(ak string, av person, bk string, bv person) int {
	return cmp.Compare(av.Age, bv.Age)
}

func indexKeys[K cmp.Ordered, V any](seq func(func(K, V) bool)) []K {
	var keys []K
	for k := range seq {
		keys = append(keys, k)
	}
	return keys
}

func TestCreateIndexAndIterate(t *testing.T) {
	db := openPeople(t, filepath.Join(t.TempDir(), "test.db"))
	defer db.Close()

	people := map[string]person{
		"ada":    {Name: "Ada", Age: 36},
		"grace":  {Name: "Grace", Age: 45},
		"alan":   {Name: "Alan", Age: 29},
		"edsger": {Name: "Edsger", Age: 51},
	}
	for k, p := range people {
		if err := db.Set(k, p); err != nil {
			t.Fatal(err)
		}
	}

	// Index created after data exists must backfill.
	if err := db.CreateIndex("by-age", byAge); err != nil {
		t.Fatal(err)
	}

	got := indexKeys(db.AscendIndex("by-age"))
	want := []string{"alan", "ada", "grace", "edsger"} // by ascending age
	if !slices.Equal(got, want) {
		t.Fatalf("AscendIndex order = %v; want %v", got, want)
	}
	gotDesc := indexKeys(db.DescendIndex("by-age"))
	slices.Reverse(want)
	if !slices.Equal(gotDesc, want) {
		t.Fatalf("DescendIndex order = %v; want %v", gotDesc, want)
	}

	// Pivot: everyone at least 40 (pivot compares only by age here).
	from := indexKeys(db.AscendIndexFrom("by-age", "", person{Age: 40}))
	if !slices.Equal(from, []string{"grace", "edsger"}) {
		t.Fatalf("AscendIndexFrom(40) = %v; want [grace edsger]", from)
	}

	// Descending pivot: everyone at most 40, oldest of those first.
	downFrom := indexKeys(db.DescendIndexFrom("by-age", "", person{Age: 40}))
	if !slices.Equal(downFrom, []string{"ada", "alan"}) {
		t.Fatalf("DescendIndexFrom(40) = %v; want [ada alan]", downFrom)
	}
	// An exact-match pivot is included.
	at := indexKeys(db.DescendIndexFrom("by-age", "grace", person{Age: 45}))
	if !slices.Equal(at, []string{"grace", "ada", "alan"}) {
		t.Fatalf("DescendIndexFrom(grace,45) = %v; want [grace ada alan]", at)
	}

	if names := db.Indexes(); !slices.Equal(names, []string{"by-age"}) {
		t.Fatalf("Indexes() = %v", names)
	}
}

func TestIndexMaintainedByWrites(t *testing.T) {
	db := openPeople(t, filepath.Join(t.TempDir(), "test.db"))
	defer db.Close()

	if err := db.CreateIndex("by-age", byAge); err != nil {
		t.Fatal(err)
	}
	if err := db.Set("a", person{Name: "A", Age: 30}); err != nil {
		t.Fatal(err)
	}
	if err := db.Set("b", person{Name: "B", Age: 20}); err != nil {
		t.Fatal(err)
	}
	if got := indexKeys(db.AscendIndex("by-age")); !slices.Equal(got, []string{"b", "a"}) {
		t.Fatalf("after inserts: %v; want [b a]", got)
	}

	// Overwrite moves the entry, never duplicates it.
	if err := db.Set("a", person{Name: "A", Age: 10}); err != nil {
		t.Fatal(err)
	}
	if got := indexKeys(db.AscendIndex("by-age")); !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("after overwrite: %v; want [a b]", got)
	}

	if _, err := db.Delete("b"); err != nil {
		t.Fatal(err)
	}
	if got := indexKeys(db.AscendIndex("by-age")); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("after delete: %v; want [a]", got)
	}
}

func TestIndexTransactional(t *testing.T) {
	db := openPeople(t, filepath.Join(t.TempDir(), "test.db"))
	defer db.Close()

	if err := db.CreateIndex("by-age", byAge); err != nil {
		t.Fatal(err)
	}
	if err := db.Set("x", person{Name: "X", Age: 50}); err != nil {
		t.Fatal(err)
	}

	// A read snapshot's index is frozen.
	rtx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer rtx.Rollback()

	// A write tx sees its own index updates before commit; the DB does not.
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Set("y", person{Name: "Y", Age: 25}); err != nil {
		t.Fatal(err)
	}
	if got := indexKeys(tx.AscendIndex("by-age")); !slices.Equal(got, []string{"y", "x"}) {
		t.Fatalf("tx index = %v; want own write visible", got)
	}
	if got := indexKeys(tx.DescendIndexFrom("by-age", "", person{Age: 40})); !slices.Equal(got, []string{"y"}) {
		t.Fatalf("tx DescendIndexFrom(40) = %v; want [y]", got)
	}
	if got := indexKeys(db.DescendIndexFrom("by-age", "", person{Age: 40})); got != nil {
		t.Fatalf("db DescendIndexFrom(40) sees uncommitted write: %v", got)
	}
	if got := indexKeys(db.AscendIndex("by-age")); !slices.Equal(got, []string{"x"}) {
		t.Fatalf("db index sees uncommitted write: %v", got)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if got := indexKeys(db.AscendIndex("by-age")); !slices.Equal(got, []string{"y", "x"}) {
		t.Fatalf("db index after commit = %v; want [y x]", got)
	}
	if got := indexKeys(rtx.AscendIndex("by-age")); !slices.Equal(got, []string{"x"}) {
		t.Fatalf("read snapshot index changed after commit: %v", got)
	}

	// Rollback discards index changes with everything else.
	tx2, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx2.Set("z", person{Name: "Z", Age: 1}); err != nil {
		t.Fatal(err)
	}
	tx2.Rollback()
	if got := indexKeys(db.AscendIndex("by-age")); !slices.Equal(got, []string{"y", "x"}) {
		t.Fatalf("index after rollback = %v; want [y x]", got)
	}
}

func TestIndexSkipsExpired(t *testing.T) {
	db := openPeople(t, filepath.Join(t.TempDir(), "test.db"), WithSweepInterval(0))
	defer db.Close()

	if err := db.CreateIndex("by-age", byAge); err != nil {
		t.Fatal(err)
	}
	if err := db.SetTTL("temp", person{Name: "T", Age: 1}, 15*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := db.Set("perm", person{Name: "P", Age: 2}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if got := indexKeys(db.AscendIndex("by-age")); !slices.Equal(got, []string{"perm"}) {
		t.Fatalf("index shows expired key: %v", got)
	}
}

func TestDropAndUnknownIndex(t *testing.T) {
	db := openPeople(t, filepath.Join(t.TempDir(), "test.db"))
	defer db.Close()

	if err := db.CreateIndex("by-age", byAge); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateIndex("by-age", byAge); err == nil {
		t.Fatal("duplicate CreateIndex succeeded")
	}
	if err := db.DropIndex("by-age"); err != nil {
		t.Fatal(err)
	}
	if err := db.DropIndex("by-age"); err == nil {
		t.Fatal("double DropIndex succeeded")
	}
	if got := indexKeys(db.AscendIndex("by-age")); got != nil {
		t.Fatalf("dropped index still iterates: %v", got)
	}
	if got := indexKeys(db.AscendIndex("never-existed")); got != nil {
		t.Fatalf("unknown index yields entries: %v", got)
	}
}
