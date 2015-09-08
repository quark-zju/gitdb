gitdb [![Doc](https://godoc.org/github.com/quark-zju/gitdb?status.svg)](https://godoc.org/github.com/quark-zju/gitdb)
=====

A lightweight golang package to sync git objects between database and filesystem.

gitdb depends on standard Go packages and git binary, has a reasonable performance.


Features
--------

* Sync git objects between filesystem and database, incrementally.
* Read tree and blobs from git database directly.


Core API by examples
--------------------

To create the database table, which is required by gitdb:

    db, err := sql.Open(...)
    gitdb.CreateTable(db)

To import git repo located at "/foo/bar" to database:

    gitdb.Import(db, "/foo/bar", "HEAD")

To export git objects to filesystem and update its HEAD:

    oid := "d18eb8215851573416b558cdf224c49580731249"
    gitdb.Export(db, "/foo/bar", oid, "HEAD")

To read file paths and contents of a tree (and all subtrees) from database:

    // oid can be either a commit or a tree
    oid := "d18eb8215851573416b558cdf224c49580731249"
    modes, oids, paths, err := gitdb.ReadTree(db, oid)
    contents, err = gitdb.ReadBlobs(db, oids)
    for i, p := range paths {
        fmt.Println(path, oids[i], contents[i])
    }


FAQ
---

**Q: Why sync git objects to database?**

A: Easier deployment. Usually applications on running multiple nodes can connect to a centric database but do not have a centric filesystem.


**Q: Does gitdb scale?**

A: Sadly git does not scale and neither does gitdb.
   Repos with thousands of commits probably won't perform well.
   If you need to delete unused objects (gitable.GC), do not store too many repos because GC will scan the whole table.
   Things can be better using recursive SQL query. However some databases (namely MySQL 5.6) do not support it. For now, gitdb chooses to not use complex SQLs and be compatible with MySQL 5.6.
   Database latency is extremely important to gitdb performance. Keep database as near as possible to the application.


**Q: Will Import and Export ignore existing objects?**

A: Yes. Import and Export will skip importing or exporting existing objects.
   This means even for a relatively large repo, when syncs frequently, the performance is still acceptable.


**Q: Can I use gitdb as a general purpose git library?**

A: No. The package is designed to be simple. It even runs external git binary for some complex tasks.
   For unsupported tasks such as adding a commit, you can use export, do modifications using other git library or even git binary, then import.


**Q: Why not use a native git library, instead of executing external git?**

A: Because a git library is not simple. A decent go git library will probably cause the codebase much larger.
   libgit2 is good but not widely installed. And I tried not to introduce non-go dependencies.


**Q: Can I modify the gitobjects table on my own?**

A: Please do it only when you understand what you are doing. Deleting or altering rows in gitobjects may break gitdb in several ways.
