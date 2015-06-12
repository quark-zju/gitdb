gitdb
=====

A golang library for syncing git objects between database and filesystem.

gitdb tries to be simple, easy to use and performs reasonable well. It requires external git binary.


Core API
--------

To create git objects table required by gitdb:

    db, _ := sql.Open(...)
    gitdb.CreateTable(db)

To import git repo located at "/foo/bar" to database:

    gitdb.Import(db, "/foo/bar", "HEAD")

To export git objects to filesystem and update HEAD:

    oid := "d18eb8215851573416b558cdf224c49580731249"
    gitdb.Export(db, "/foo/bar", oid, "HEAD")


Full Example
------------

Here is a sample client supporting import, output and gc:

    package main

    import (
        "database/sql"
        "fmt"
        _ "github.com/mattn/go-sqlite3"
        "gitlab.com/quark/gitdb"
        "os"
    )

    var db *sql.DB

    func doImport(dir string, ref string) {
        rOid, oids, err := gitdb.Import(db, dir, ref)
        if err != nil {
            panic(err)
        }
        fmt.Printf("Imported from %s: %d objects; %s is '%s'.\n", dir, len(oids), ref, rOid)
    }

    func doExport(dir string, oid string) {
        oids, err := gitdb.Export(db, dir, oid, "refs/heads/master")
        if err != nil {
            panic(err)
        }
        fmt.Printf("Exported to %s: %d objects; master set to '%s'.\n", dir, len(oids), oid)
    }

    func doGC(oids []string) {
        oids, err := gitdb.GC(db, oids)
        if err != nil {
            panic(err)
        }
        fmt.Printf("GC completed. %d objects deleted.\n", len(oids))
    }

    func usage() {
        fmt.Printf("%s i[mport] dir [ref=master]    # import objects from filesystem\n"+
            "%s e[xport] dir oid           # export objects to existing git repo. update master.\n"+
            "%s g[c] oid1 [oid [oid] ...]  # give some reachable oids, delete others\n",
        os.Args[0], os.Args[0], os.Args[0])
        os.Exit(1)
    }

    func main() {
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
        case 'i':
            ref := "master"
            if len(os.Args) >= 4 {
                ref = os.Args[3]
            }
            doImport(os.Args[2], ref)
        case 'e':
            if len(os.Args) < 4 {
                usage()
            }
            doExport(os.Args[2], os.Args[3])
        case 'g':
            doGC(os.Args[2:len(os.Args)])
        default:
            usage()
        }
        db.Close()
    }

Build the script to `gitdbc` and try:

    mkdir -p /tmp/repo/
    pushd /tmp/repo && git clone https://gitlab.com/quark/gitdb && popd
    gitdbc import /tmp/repo/gitdb

    mkdir -p /tmp/repo/gitdb2
    pushd /tmp/repo/gitdb2 && git init && popd
    OID=`git --git-dir /tmp/repo/gitdb rev-parse HEAD`
    gitdbc export /tmp/repo/gitdb2 $OID

    gitdbc gc $OID


FAQ
---

**Q: Why sync git objects to database?**

A: Easier deployment. Usually applications on running multiple nodes can connect to a centric database but do not have a centric filesystem.


**Q: Does gitdb scale?**

A: Sadly git does not scale and neither does gitdb.
   Repos with thousands of commits probably won't perform well.
   If you need to delete unused objects (gitable.GC), do not store too many repos because GC will scan the whole table.
   Things can be better using recursive SQL query. However some databases (namely MySQL 5.6) do not support it. For now, gitdb chooses to be compatible with MySQL 5.6.
   Database latency is extremely important to gitdb performance. Keep database as near as possible to the application.


**Q: Will Import and Export ignore existing objects?**

A: Yes. Import and Export will first decide which objects already exist and skip importing or exporting them.
   This means even for a relatively large repo, if it is synced frequently, the performance is still probably acceptable.


**Q: Can I use gitdb as a general purpose git library?**

A: No. The libary is designed to be simple and do not have unnecessary features. It even executes external git binary for relatively complex tasks.


**Q: Why not use a native git library, instead of calling external git?**

A: Because a git library is not simple.  A decent go git library will probably cause the codebase 4x larger.
   libgit2 is good but I tried not to introduce non-go dependencies and git is widely installed while libgit2 is not.
   Do not worry. Although gitdb executes external git, it will batch whatever batchable so performance is probably okay.
   Typically, one Import or Export call will run git only 1 to 2 times regardless of repo size.


**Q: Can I modify the gitobjects table on my own?**

A: Please do it only when you understand what you are doing. Deleting or altering rows in gitobjects may break gitdb in strange ways.
