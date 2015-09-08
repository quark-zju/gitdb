package gitdb

import (
	"bytes"
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
)

var dbDir string = filepath.Join(os.TempDir(), "gitdb-test", "db")

func createDb(name string) *sql.DB {
	os.MkdirAll(dbDir, 0755)
	dp := filepath.Join(dbDir, fmt.Sprintf("%s.sqlite3", filepath.Base(name)))
	os.RemoveAll(dp)
	db, err := sql.Open("sqlite3", dp)
	if err != nil {
		panic(err)
	}
	if _, err := CreateTable(db); err != nil {
		panic(err)
	}
	return db
}

func updateRepo(name string, n int) {
	createRandomRepo(name, 15, false, false)
}

func TestImportExport(t *testing.T) {
	if !checkGit() {
		return
	}

	db := createDb("importExport")
	defer db.Close()

	n := 100
	if os.Getenv("N") != "" {
		n, _ = strconv.Atoi(os.Getenv("N"))
		if n < 30 {
			n = 30
		}
	}
	dir1 := createRandomRepo("b1", n, true, true)
	ref1, oids1, e := Import(db, dir1, "HEAD")
	if e != nil {
		t.Fatal("Import error", e)
	}
	if len(oids1) == 0 {
		t.Fatal("Import unexpected: imported nothing")
	}

	// Test Re-import
	for _, ref := range []string{"HEAD", ref1} {
		if r, oids, e := Import(db, dir1, ref); r != ref1 || e != nil || len(oids) != 0 {
			t.Fatal("Import unexpected: wrong ref or wrong oids or error", e, oids)
		}
	}

	// Test multiple repo
	dir2 := createRandomRepo("b2", n, true, true)
	ref2, oids, e := Import(db, dir2, "HEAD")
	if e != nil {
		t.Fatal("Import error", e)
	}
	if len(oids) == 0 {
		t.Fatal("Import unexpected: imported nothing")
	}

	// Test sync (fs -> db)
	updateRepo("b1", 15)
	ref1u, oids1u, e := Import(db, dir1, "HEAD")
	if e != nil {
		t.Fatal("Import error", e)
	}
	if ref1u == ref1 {
		t.Fatal("Import unexpected: wrong ref")
	}
	if len(oids1u) == 0 {
		t.Fatal("Import unexpected: imported nothing")
	}

	// Test GC
	tx, e := db.Begin()
	if e != nil {
		t.Fatal("Failed to start transaction", e)
	}
	oids, e = GC(tx, []string{ref1u, ref2})
	if e != nil {
		t.Fatal("GC error", e)
	}
	if len(oids) > 0 {
		t.Fatal("GC unexpected: reachable objects are deleted", oids)
	}

	oids, e = GC(tx, []string{ref1, ref2})
	if e != nil {
		t.Fatal("GC error", e)
	}
	if len(oids) != len(oids1u) {
		t.Fatal("GC unexpected: unreachable objects ", oids1u, " are not deleted", oids)
	}
	e = tx.Commit()
	if e != nil {
		t.Fatal("Failed to commit transaction")
	}

	// Re-import missing objects
	ref1u, oids, e = Import(db, dir1, "HEAD")
	if e != nil {
		t.Fatal("Import error", e)
	}
	if len(oids) != len(oids1u) {
		t.Fatal("Import unexpected: imported objects not expected", oids, oids1u)
	}

	// Test sync (db -> fs)
	// Export to an up-to-date repo
	for _, ref := range []string{ref1u, ref1} {
		oids, e = Export(db, dir1, ref, "")
		if e != nil {
			t.Fatal("Export error:", e)
		}
		if len(oids) != 0 {
			t.Fatal("Export unexpected: should write nothing (actually wrote", oids, ")")
		}
	}

	oids, e = Export(db, dir2, ref1, "")
	if e != nil {
		t.Fatal("Export error", e)
	}
	if len(oids) == 0 {
		t.Fatal("Export unexpected: should write something (actually wrote", oids, ")")
	}

	// Export to an empty repo
	dir3 := createRandomRepo("c", 0, false, true)
	oids, e = Export(db, dir3, ref1, "")
	if e != nil {
		t.Fatal("Export error", e)
	}
	if len(oids) != len(oids1) {
		t.Fatal("Export unexpected: expected wrote ", oids1, " actually wrote", oids, ")")
	}
	oids, e = Export(db, dir3, ref1u, "")
	if e != nil {
		t.Fatal("Export error", e)
	}
	if len(oids) != len(oids1u) {
		t.Fatal("Export unexpected: expected wrote ", oids1u, " actually wrote", oids, ")")
	}

	if err := exec.Command("git", "fsck", "--full", "--strict", ref1u).Run(); err != nil {
		t.Fatal("Export unexpected: failed git fsck check", err)
	}
}

func TestRead(t *testing.T) {
	db := createDb("read")
	defer db.Close()

	dir := createRandomRepo("r", 50, true, true)
	oid, _, e := Import(db, dir, "HEAD")
	if e != nil {
		t.Fatal("Import error", e)
	}

	modes, oids, paths, e := ReadTree(db, oid)
	if e != nil {
		t.Fatal("ReadTree error", e)
	}
	if len(paths) != len(oids) || len(oids) != len(modes) {
		t.Fatal("ReadTree returns inconsistent numbers of paths, oids, modes: ", len(paths), len(oids), len(modes))
	}
	for _, m := range modes {
		if m != 0100644 {
			t.Fatal("Mode ", m, " is unexpected")
		}
	}

	blobs, e := ReadBlobs(db, oids)
	if e != nil {
		t.Fatal("ReadBlobs error", e)
	}
	if len(blobs) != len(oids) {
		t.Fatal("ReadBlobs returns ", len(blobs), " items, does not equal to oids (", len(oids), ")")
	}

	for i, path := range paths {
		b, e := ioutil.ReadFile(filepath.Join(dir, path))
		if e != nil {
			t.Fatal("ReadFile", path, "error", e)
		}
		if bytes.Compare(b, blobs[i]) != 0 {
			t.Error("File content mismatched ", path)

		}
	}
}
