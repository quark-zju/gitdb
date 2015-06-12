package gitdb

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
)

func createDb() *sql.DB {
	dp := filepath.Join(tmpDir, "d.sqlite3")
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

func createRepo(name string, n int, reuse bool) string {
	dir := filepath.Join(tmpDir, name)
	if isDir(dir) && reuse && os.Getenv("REUSE") != "" {
		fmt.Println("Reusing ", dir)
		return dir
	}
	os.RemoveAll(dir)
	createRandomRepo(dir, n)
	return dir
}

func updateRepo(name string) {
	dir := filepath.Join(tmpDir, name)
	createRandomRepo(dir, 15)
}

func TestDb(t *testing.T) {
	if !checkGit() {
		return
	}

	os.MkdirAll(tmpDir, 0755)
	db := createDb()
	defer db.Close()

	n := 100
	if os.Getenv("N") != "" {
		n, _ = strconv.Atoi(os.Getenv("N"))
		if n < 30 {
			n = 30
		}
		fmt.Println("User set N =", n)
	}
	dir1 := createRepo("b1", n, true)
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
	dir2 := createRepo("b2", n, true)
	ref2, oids, e := Import(db, dir2, "HEAD")
	if e != nil {
		t.Fatal("Import error", e)
	}
	if len(oids) == 0 {
		t.Fatal("Import unexpected: imported nothing")
	}

	// Test sync (fs -> db)
	updateRepo("b1")
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

	// Test gc
	oids, e = GC(db, []string{ref1u, ref2})
	if e != nil {
		t.Fatal("GC error", e)
	}
	if len(oids) > 0 {
		t.Fatal("GC unexpected: reachable objects are deleted", oids)
	}

	oids, e = GC(db, []string{ref1, ref2})
	if e != nil {
		t.Fatal("GC error", e)
	}
	if len(oids) != len(oids1u) {
		t.Fatal("GC unexpected: unreachable objects ", oids1u, " are not deleted", oids)
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
			t.Fatal("Export error", e)
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
	dir3 := createRepo("c", 0, false)
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
