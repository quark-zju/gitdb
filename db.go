package gitdb

import (
	"database/sql"
	"fmt"
	"strings"
)

const table = "gitobjects"
const batchRows = 500

// TODO support both DB and Tx
type dbOrTx interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
}

// TODO
type rowScanFunc func(...interface{}) error

// TODO
func CreateTable(db *sql.DB) (sql.Result, error) {
	return db.Exec("CREATE TABLE IF NOT EXISTS " + table + " (" +
		"oid CHAR(40) PRIMARY KEY NOT NULL," +
		"type CHAR(6) NOT NULL," +
		// Actually, zcontent contains all other fields:
		// - oid: sha1(zcontent).
		// - type: header in inflated zcontent.
		// - referred: parsing inflated zcontent.
		// Other fields exist for performance reason.
		"zcontent MEDIUMBLOB NOT NULL," +
		"referred TEXT)")
}

// TODO
func Import(dt dbOrTx, path string, ref string) (refOid string, oids []string, err error) {
	// 1: list ids
	// 2: what ids are we missing
	// 3: import that ids
	repo := newRepo(path)
	oids, err = repo.listOids(ref)
	if err != nil || len(oids) == 0 {
		return "", nil, err
	}
	refOid = oids[0]

	tx, txByUs, err := getTx(dt)
	if err != nil {
		return refOid, nil, err
	}
	if txByUs {
		defer tx.Rollback()
	}

	oids, err = unseenOids(tx, oids)
	if err != nil {
		return refOid, nil, err
	}

	objs, err := repo.readObjects(oids)
	if err != nil {
		return refOid, nil, err
	}

	stmt, err := tx.Prepare("INSERT INTO " + table + " (oid, zcontent, type, referred) VALUES (?, ?, ?, ?)")
	if err != nil {
		return refOid, nil, err
	}
	defer stmt.Close()

	for _, obj := range objs {
		_, err = stmt.Exec(obj.Oid, obj.zlibContent(), obj.Type, strings.Join(obj.referredOids(), ","))
		if err != nil {
			return refOid, nil, err
		}
	}

	if txByUs {
		if err = tx.Commit(); err != nil {
			return refOid, nil, err
		}
	}

	return refOid, oids, nil
}

// TODO
func Export(dt dbOrTx, path string, oid string, ref string) ([]string, error) {
	// 1: quick test: is repo up-to-date ?
	// 2: compare oids, decide which oids to be written
	// 3: write oids
	// TODO
	if len(ref) == 0 {
		ref = "refs/tags/gitdb/" + oid
	}

	repo := newRepo(path)
	if repo.hasOid(oid) {
		return nil, repo.writeRef(ref, oid)
	}

	tx, txByUs, err := getTx(dt)
	if err != nil {
		return nil, err
	}
	if txByUs {
		defer tx.Rollback()
	}

	// Check oids on the fly ?
	repoOids, err := repo.listOids("--all")
	if err != nil {
		return nil, err
	}

	newOids, err := bfsOids(tx, []string{oid}, repoOids)
	if err != nil {
		return nil, err
	}

	// read content
	zmap := make(map[string][]byte, len(newOids))
	err = queryByOids(tx, "oid, zcontent", newOids, func(scan rowScanFunc) error {
		var o string
		var z []byte
		if err := scan(&o, &z); err != nil {
			return err
		}
		zmap[o] = z
		return nil
	})
	if err != nil {
		return nil, err
	}

	// write in reversed order, make sure required objects are written first
	for i := len(newOids) - 1; i >= 0; i-- {
		o := newOids[i]
		z, ok := zmap[o]
		if !ok {
			return nil, errMissingObject(o)
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

	// Write ref so that `git rev-list --all` will list these oids
	return newOids, repo.writeRef(ref, oid)
}

// Gc TODO
func GC(dt dbOrTx, oids []string) ([]string, error) {
	tx, txByUs, err := getTx(dt)
	if err != nil {
		return nil, err
	}
	if txByUs {
		defer tx.Rollback()
	}

	oids, err = bfsOids(tx, oids, nil)
	if err != nil {
		return nil, err
	}

	reachable := toSet(oids)
	deletable := make([]string, 0)

	rows, err := tx.Query("SELECT oid FROM " + table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var oid string
		if err = rows.Scan(&oid); err != nil {
			return nil, err
		}
		if reachable[oid] == false {
			deletable = append(deletable, oid)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for i := 0; i < len(deletable); i += batchRows {
		j := min(i+batchRows, len(deletable))
		args := toInterfaces(deletable[i:j])
		_, err := tx.Exec("DELETE FROM "+table+" WHERE oid IN (?"+strings.Repeat(",?", len(args)-1)+")", args...)
		if err != nil {
			return nil, err
		}
	}

	if txByUs {
		if err = tx.Commit(); err != nil {
			return nil, err
		}
	}
	return deletable, nil
}

// bfsOids returns all referred oids by reading referred oids recursively.
// It is like `git rev-list $oids` but works in database directly.
// If traversal meets an oid in skipOids, the oid will be ignored.
// traversal is useful in in two cases:
// 1. Given a repo's HEAD oid, get all oids of that repo for Export
// 2. Given all repo HEADs, get all reachable oids for GC
// Note: traversal is slow. Use cache whenever possible.
// result is ordered. initOids first followed by referred oids.
func bfsOids(tx *sql.Tx, initOids []string, skipOids []string) ([]string, error) {
	visited := toSet(append(initOids, skipOids...))
	result := initOids
	for currOids := initOids; len(currOids) > 0; {
		nextOids := make([]string, 0)
		err := queryByOids(tx, "referred", currOids, func(scan rowScanFunc) error {
			var referred string
			if err := scan(&referred); err != nil {
				return err
			}
			for _, v := range strings.Split(referred, ",") {
				if len(v) > 0 && visited[v] == false {
					nextOids = append(nextOids, v)
					result = append(result, v)
					visited[v] = true
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

// unseenOids removes oids which are already stored in database.
func unseenOids(tx *sql.Tx, oids []string) ([]string, error) {
	exists := make([]string, 0)
	err := queryByOids(tx, "oid", oids, func(scan rowScanFunc) error {
		var oid string
		if err := scan(&oid); err != nil {
			return err
		}
		exists = append(exists, oid)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return minus(oids, exists), nil
}

// queryByOids fetch db rows by oids.
func queryByOids(tx *sql.Tx, columns string, oids []string, rowHandler func(rowScanFunc) error) error {
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
		if err := rows.Err(); err != nil {
			return err
		}
		for rows.Next() {
			if err := rowHandler(rows.Scan); err != nil {
				return err
			}
		}
	}
	return nil
}

// getTx TODO
func getTx(dt dbOrTx) (tx *sql.Tx, txByUs bool, err error) {
	db, isDb := dt.(*sql.DB)
	if isDb {
		tx, err := db.Begin()
		return tx, true, err
	}
	tx, isTx := dt.(*sql.Tx)
	if isTx {
		return tx, false, nil
	}
	return nil, false, fmt.Errorf("neither sql.DB nor sql.Tx")
}

// minus returns []string with elements in a but not b.
func minus(a []string, b []string) []string {
	m := toSet(b)
	r := make([]string, 0)
	for _, v := range a {
		if m[v] == false {
			r = append(r, v)
		}
	}
	return r
}

// toMap converts []string to "set" (map[string]bool, values are true).
func toSet(a []string) map[string]bool {
	m := make(map[string]bool, len(a))
	for _, v := range a {
		m[v] = true
	}
	return m
}

// toInterfaces converts []string to []interface{}. It is useful in
// db.Query and db.Exec.
func toInterfaces(a []string) []interface{} {
	result := make([]interface{}, 0, len(a))
	for _, v := range a {
		result = append(result, v)
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

// TODO
type errMissingObject string

func (e errMissingObject) Error() string {
	return fmt.Sprintf("git object %s required but not found", string(e))
}
