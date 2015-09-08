package gitdb

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
)

const table = "gitobjects"
const batchRows = 500

// dbOrTx is compatible with sql.DB and sql.Tx
type dbOrTx interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
}

// rowScanFunc matches the signature of (*sql.Rows).Scan
type rowScanFunc func(...interface{}) error

// CreateTable creates the required git objects table on demand.
func CreateTable(db *sql.DB) (sql.Result, error) {
	return db.Exec("CREATE TABLE IF NOT EXISTS " + table + " (" +
		"oid CHAR(40) PRIMARY KEY NOT NULL," +
		"type CHAR(6) NOT NULL," +
		// Note: zcontent (zlib compressed content of a git object)
		// has the information of all other fields:
		// - oid: sha1(zcontent)
		// - type: header in uncompressed zcontent
		// - referred: parsing uncompressed content
		// These fields seem to be unnecessary but they exist for
		// performance reason.
		//
		// MEDIUMBLOB is MySQL specific, which is 16MB. BLOB in Sqlite
		// does not have a length limit.
		"zcontent MEDIUMBLOB NOT NULL," +
		"referred TEXT)")
}

// ReadTree reads trees and sub-trees recursively from database.
// Returns modes, oids, full paths for non-tree objects.
// It is like `git ls-tree -r` but works directly in database.
//
// dt is either *sql.DB or *sql.Tx.
// oid is the git object ID of a git tree or commit.
func ReadTree(dt dbOrTx, oid Oid) (modes []int32, oids []Oid, paths []string, err error) {
	tx, txByUs, err := getOrCreateTx(dt)
	if err != nil {
		return nil, nil, nil, err
	}
	if txByUs {
		defer tx.Rollback()
	}

	prefixes := map[Oid]string{oid: ""}
	for nextOids := []Oid{oid}; len(nextOids) > 0; {
		objs, err := readObjects(tx, nextOids)
		nextOids = []Oid{}
		if err != nil {
			return nil, nil, nil, err
		}
		for _, o := range objs {
			prefix := prefixes[o.Oid]
			switch o.Type {
			case "commit":
				// extract tree oid from commit object automatically
				treeOid := Oid(o.Body[5:45])
				nextOids = append(nextOids, treeOid)
			case "tree":
				for _, ti := range parseTree(o.Body) {
					path := filepath.Join(prefix, ti.Name)
					if ti.IsTree() {
						prefixes[ti.Oid] = path
						nextOids = append(nextOids, ti.Oid)
					} else {
						paths = append(paths, path)
						oids = append(oids, ti.Oid)
						modes = append(modes, ti.Mode)
					}
				}
			}
		}
	}
	return modes, oids, paths, nil
}

// ReadBlobs reads blob contents from database.
// It is like `git cat-file --batch` but only returns contents, without type or
// size information.
//
// dt is either *sql.DB or *sql.Tx.
// oids are the git object IDs of the blobs to be read.
//
// It is often used after ReadTree.
//
// Note: ReadBlobs does not check git object type. It can be used to read raw
// contents of other git objects.
func ReadBlobs(dt dbOrTx, oids []Oid) ([][]byte, error) {
	tx, txByUs, err := getOrCreateTx(dt)
	if err != nil {
		return nil, err
	}
	if txByUs {
		defer tx.Rollback()
	}

	objs, err := readObjects(tx, oids)
	if err != nil {
		return nil, err
	}

	result := make([][]byte, 0, len(oids))
	for _, obj := range objs {
		result = append(result, obj.Body)
	}
	return result, nil
}

// readObjects reads git objects from database and return gitObjs.
// For duplicated oids, returns two pointers to a same gitObj.
// Missing objects or mismatched SHA1 will cause errors.
func readObjects(dt dbOrTx, oids []Oid) ([]*gitObj, error) {
	tx, txByUs, err := getOrCreateTx(dt)
	if err != nil {
		return nil, err
	}
	if txByUs {
		defer tx.Rollback()
	}

	m := make(map[Oid]*gitObj, len(oids))
	err = queryByOids(tx, "oid, zcontent", oids, func(scan rowScanFunc) error {
		var s string
		var zcontent []byte
		if err := scan(&s, &zcontent); err != nil {
			return err
		}
		oid := Oid(s)
		o, err := newGitObjFromZcontent(zcontent)
		if err != nil {
			return fmt.Errorf("cannot read object %s: %s", oid, err)
		}
		if o.Oid != oid {
			return fmt.Errorf("sha1 mismatch: oid = %s, sha1(content) = %s", oid, o.Oid)
		}
		m[oid] = o
		return nil
	})
	if err != nil {
		return nil, err
	}

	result := make([]*gitObj, 0, len(oids))
	for _, oid := range oids {
		obj, ok := m[oid]
		if !ok {
			return nil, fmt.Errorf("object not found: %s", oid)
		}
		result = append(result, obj)
	}
	return result, nil
}

// Import syncs git objects from filesystem to database.
// It is like `git push` running from the filesystem.
//
// dt is either *sql.DB or *sql.Tx.
// path is the path of the git repository. It can be the `.git` directory,
// or its parent.
// ref is the reference string. It can be "HEAD", a tag name, a branch name,
// a commit hash or its prefix.
//
// Returns oids, refOid, err.
// oids are imported object IDs. If nothing is imported (the database is
// up-to-date), oids will be an empty array.
// refOid is the parsed git object ID (40-char hex string) of the given ref.
func Import(dt dbOrTx, path string, ref string) (oids []Oid, refOid Oid, err error) {
	// List object IDs to check or import
	repo := newRepo(path)
	oids, err = repo.listOids(ref)
	if err != nil || len(oids) == 0 {
		return nil, "", err
	}
	refOid = oids[0]

	tx, txByUs, err := getOrCreateTx(dt)
	if err != nil {
		return nil, refOid, err
	}
	if txByUs {
		// For transaction created by us, remember to commit or rollback it.
		// tx.Rollback will do nothing after tx.Commit().
		defer tx.Rollback()
	}

	// Remove oids that exist in database
	oids, err = unseenOids(tx, oids)
	if err != nil {
		return nil, refOid, err
	}

	// Read new objects
	objs, err := repo.readObjects(oids)
	if err != nil {
		return nil, refOid, err
	}

	// Write new objects
	stmt, err := tx.Prepare("INSERT INTO " + table + " (oid, zcontent, type, referred) VALUES (?, ?, ?, ?)")
	if err != nil {
		return nil, refOid, err
	}
	defer stmt.Close()

	for _, obj := range objs {
		_, err = stmt.Exec(string(obj.Oid), obj.zcontent(), obj.Type, joinOids(obj.referredOids(), ","))
		if err != nil {
			return nil, refOid, err
		}
	}

	if txByUs {
		if err = tx.Commit(); err != nil {
			return nil, refOid, err
		}
	}

	return oids, refOid, nil
}

// Export syncs git objects from database to filesystem.
// It is like `git pull` running from the filesystem.
//
// dt is either *sql.DB or *sql.Tx.
// path is the path of the git repository. It can be the `.git` directory,
// or its parent.
// oid is the git object ID in database.
// ref is the reference string which will be written to the filesystem.
// It is usually "HEAD". It could also be "refs/tags/foo", or "refs/heads/bar".
// If ref is an empty string, a generated tag name will be used to make the
// newly written objects not orphaned.
//
// Returns oids and error.
// oids is a list of git object IDs exported. If nothing is exported (the
// git repository in the filesystem is up-to-date), oids will be an empty
// array.
func Export(dt dbOrTx, path string, oid Oid, ref string) ([]Oid, error) {
	if len(ref) == 0 {
		ref = "refs/tags/gitdb/" + string(oid)
	}

	// Quick up-to-date test
	repo := newRepo(path)
	if repo.hasOid(oid) {
		return nil, repo.writeRef(ref, oid)
	}

	tx, txByUs, err := getOrCreateTx(dt)
	if err != nil {
		return nil, err
	}
	if txByUs {
		defer tx.Rollback()
	}

	// Scan oids that the repo already have
	repoOids, err := repo.listOids("--all")
	if err != nil {
		return nil, err
	}

	// BFS the database to select what we need to export
	// Note: If an object exists in the repo, we won't check its parent.
	// This requires writting objects in a certain order. See below.
	newOids, err := bfsOids(tx, []Oid{oid}, repoOids)
	if err != nil {
		return nil, err
	}

	// Read contents of selected oids
	zmap := make(map[Oid][]byte, len(newOids))
	err = queryByOids(tx, "oid, zcontent", newOids, func(scan rowScanFunc) error {
		var s string
		var zcontent []byte
		if err := scan(&s, &zcontent); err != nil {
			return err
		}
		oid := Oid(s)
		zmap[oid] = zcontent
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Write git objects to filesystem
	// Dependent objects (with higher level of the BFS tree) are written first.
	for i := len(newOids) - 1; i >= 0; i-- {
		o := newOids[i]
		z, ok := zmap[o]
		if !ok {
			return nil, errDbMissingObject(o)
		}
		if err := repo.writeRawObject(o, z); err != nil {
			return nil, err
		}
	}

	if txByUs {
		if err = tx.Commit(); err != nil {
			return nil, err
		}
	}

	// Write ref so they are no longer orphaned
	return newOids, repo.writeRef(ref, oid)
}

// GC removes all objects from database except for oids and their parents
// and ancestors.
//
// Returns deleted git object IDs.
func GC(tx *sql.Tx, oids []Oid) ([]Oid, error) {
	// Scan reachable objects
	oids, err := bfsOids(tx, oids, nil)
	if err != nil {
		return nil, err
	}
	reachable := toSet(oids)

	// Find out deletable objects
	deletable := make([]Oid, 0)
	rows, err := tx.Query("SELECT oid FROM " + table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var s string
		if err = rows.Scan(&s); err != nil {
			return nil, err
		}
		oid := Oid(s)
		if reachable[oid] == false {
			deletable = append(deletable, oid)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Delete objects in batch
	for i := 0; i < len(deletable); i += batchRows {
		j := min(i+batchRows, len(deletable))
		args := toInterfaces(deletable[i:j])
		_, err := tx.Exec("DELETE FROM "+table+" WHERE oid IN (?"+strings.Repeat(",?", len(args)-1)+")", args...)
		if err != nil {
			return nil, err
		}
	}

	return deletable, nil
}

// bfsOids returns all referred oids by reading referred oids recursively.
// It is like `git rev-list $oids` but works directly in database.
// If an oid matches one in skipOids, the object and its parents will be
// skipped.
//
// bfsOids is useful in two cases:
// 1. Given a repo's HEAD oid, get all oids of that repo for Export
// 2. Given all repo HEADs, get all reachable oids for GC
//
// Returns oids and error. oids is in BFS order.
//
// Note: bfsOids is slow. Use cache whenever possible.
func bfsOids(tx *sql.Tx, initOids []Oid, skipOids []Oid) ([]Oid, error) {
	visited := toSet(append(initOids, skipOids...))
	result := initOids
	for currOids := initOids; len(currOids) > 0; {
		nextOids := make([]Oid, 0)
		err := queryByOids(tx, "referred", currOids, func(scan rowScanFunc) error {
			var referred string
			if err := scan(&referred); err != nil {
				return err
			}
			for _, v := range strings.Split(referred, ",") {
				if o := Oid(v); len(v) > 0 && visited[o] == false {
					nextOids = append(nextOids, o)
					result = append(result, o)
					visited[o] = true
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		currOids = nextOids
	}
	return result, nil
}

// unseenOids removes oids already stored in the database.
func unseenOids(tx *sql.Tx, oids []Oid) ([]Oid, error) {
	exists := make([]Oid, 0)
	err := queryByOids(tx, "oid", oids, func(scan rowScanFunc) error {
		var s string
		if err := scan(&s); err != nil {
			return err
		}
		oid := Oid(s)
		exists = append(exists, oid)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return minus(oids, exists), nil
}

// queryByOids fetches db rows by oids.
// It handles large oids array by spltting it into smaller queries.
func queryByOids(tx *sql.Tx, columns string, oids []Oid, rowHandler func(rowScanFunc) error) error {
	if (len(oids)) == 0 {
		return nil
	}
	for i := 0; i < len(oids); i += batchRows {
		j := min(i+batchRows, len(oids))
		args := toInterfaces(oids[i:j])
		rows, err := tx.Query("SELECT "+columns+" FROM "+table+" WHERE oid IN (?"+strings.Repeat(",?", len(args)-1)+")", args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			if err := rowHandler(rows.Scan); err != nil {
				return err
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
	}
	return nil
}

// getOrCreateTx creates a new tx and set txByUs to true if dt is sql.DB,
// otherwise, getOrCreateTx returns tx as is and txByUs is false.
func getOrCreateTx(dt dbOrTx) (tx *sql.Tx, txByUs bool, err error) {
	db, isDb := dt.(*sql.DB)
	if isDb {
		tx, err := db.Begin()
		return tx, true, err
	}
	tx, isTx := dt.(*sql.Tx)
	if isTx {
		return tx, false, nil
	}
	panic("dt should be either sql.DB or sql.Tx")
}

// minus returns []Oid with elements in a but not b.
func minus(a []Oid, b []Oid) []Oid {
	m := toSet(b)
	r := make([]Oid, 0)
	for _, v := range a {
		if m[v] == false {
			r = append(r, v)
		}
	}
	return r
}

// toSet converts []Oid to map[Oid]bool.
func toSet(a []Oid) map[Oid]bool {
	m := make(map[Oid]bool, len(a))
	for _, v := range a {
		m[v] = true
	}
	return m
}

// toInterfaces converts []Oid to []interface{}. It is useful in db.Query
// and db.Exec.
func toInterfaces(a []Oid) []interface{} {
	result := make([]interface{}, 0, len(a))
	for _, v := range a {
		result = append(result, string(v))
	}
	return result
}

func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

type errDbMissingObject string

func (e errDbMissingObject) Error() string {
	return fmt.Sprintf("git object %s required but not found in database", string(e))
}
