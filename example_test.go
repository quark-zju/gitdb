package gitdb_test

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/quark-zju/gitdb"
	"os"
)

var db *sql.DB

// Works like `git ls-tree -r `
func ExampleReadTree() {
	modes, oids, paths, err := gitdb.ReadTree(db, "9864be5e4fac9b4108b3412b60ed55e3c7095559")
	if err != nil {
		panic(err)
	}
	for i, mode := range modes {
		fmt.Printf("%o blob %s\t%s\n", mode, oids[i], paths[i])
	}
	// Sample Output:
	// 100644 blob e69de29bb2d1d6434b8b29ae775ad8c2e48c5391	a
	// 100644 blob 6a69f92020f5df77af6e8813ff1232493383b708	b/c
}

// Works like `git cat-file --batch='--'`
func ExampleReadBlobs() {
	var oids []gitdb.Oid
	for {
		var s string
		if n, _ := fmt.Scan(&s); n != 1 {
			break
		}
		oids = append(oids, gitdb.Oid(s))
	}

	blobs, err := gitdb.ReadBlobs(db, oids)
	if err != nil {
		panic(err)
	}

	for _, b := range blobs {
		fmt.Printf("--\n%s\n", string(b))
	}
	// Sample Output:
	// --
	// foo
	// --
	// bar
}

// An example CLI tool to test Import, Export and GC.
func Example_exampleCliTool() {
	// To test the it, build, rename to `gitdbc` and try:
	//  mkdir -p /tmp/repo/
	//  pushd /tmp/repo && git clone https://gitlab.com/quark/gitdb && popd
	//  gitdbc import /tmp/repo/gitdb
	//
	//  mkdir -p /tmp/repo/gitdb2
	//  pushd /tmp/repo/gitdb2 && git init && popd
	//  OID=`git --git-dir /tmp/repo/gitdb rev-parse HEAD`
	//  gitdbc export /tmp/repo/gitdb2 $OID
	//
	//  gitdbc gc $OID
	usage := func() {
		fmt.Printf("%s i[mport] dir [ref=master]    # import objects from filesystem\n"+
			"%s e[xport] dir oid           # export objects to existing git repo. update master.\n"+
			"%s g[c] oid1 [oid [oid] ...]  # give some reachable oids, delete others\n",
			os.Args[0], os.Args[0], os.Args[0])
		os.Exit(1)
	}

	if len(os.Args) < 3 {
		usage()
	}

	var err error
	db, err = sql.Open("sqlite3", "/tmp/gitdb.sqlite3")
	if err != nil {
		panic(err)
	}
	_, err = gitdb.CreateTable(db)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	switch os.Args[1][0] {
	case 'i': // Import
		ref := "master"
		if len(os.Args) >= 4 {
			ref = os.Args[3]
		}
		dir := os.Args[2]
		oids, rOid, err := gitdb.Import(db, dir, ref)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Imported from %s: %d objects; %s is '%s'.\n", dir, len(oids), ref, rOid)
	case 'e': // Export
		if len(os.Args) < 4 {
			usage()
		}
		dir := os.Args[2]
		oid := gitdb.Oid(os.Args[3])
		oids, err := gitdb.Export(db, dir, oid, "refs/heads/master")
		if err != nil {
			panic(err)
		}
		fmt.Printf("Exported to %s: %d objects; master set to '%s'.\n", dir, len(oids), oid)
	case 'g': // GC
		var oids []gitdb.Oid
		for _, o := range os.Args[2:len(os.Args)] {
			oids = append(oids, gitdb.Oid(o))
		}
		tx, err := db.Begin()
		if err != nil {
			panic(err)
		}
		delOids, err := gitdb.GC(tx, oids)
		if err != nil {
			panic(err)
		}
		if err := tx.Commit(); err != nil {
			panic(err)
		}
		fmt.Printf("GC completed. %d objects deleted.\n", len(delOids))
	default:
		usage()
	}
	db.Close()
}
