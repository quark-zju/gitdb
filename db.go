package gitdb

import (
	"database/sql"
	"strings"
)

const table = "gitobjects"

func CreateTable(db *sql.DB) (sql.Result, error) {
	return db.Exec("CREATE TABLE IF NOT EXISTS " + table + " (" +
		"oid CHAR(40) PRIMARY KEY NOT NULL," +
		"type CHAR(6) NOT NULL," +
		// Actually, zcontent contains all other fields:
		// - oid: sha1(zcontent).
		// - type: header in inflated zcontent.
		// - referred: parsing inflated zcontent.
		// Other fields exist for performance reason.
		"zcontent BLOB NOT NULL," +
		"referred TEXT)")
}

// toMap converts []string to "set" (map[string]bool, values are true).
func toSet(a []string) map[string]bool {
	m := make(map[string]bool, len(a))
	for _, v := range a {
		m[v] = true
	}
	return m
}

// walk returns all referred oids by reading db referred oids recursively.
// Note: walk is slow. Use cache whenever possible.
// It is useful in in two cases:
// 1. Given a repo's HEAD commit oid, get all oids of that repo for export
// 2. Given all HEADs, get all alive oids for gc
func walk(tx *sql.Tx, oids []string, skipOids []string) ([]string, error) {
	walked := toSet(oids)
	for _, v := range skipOids {
		walked[v] = true
	}
	queryOids := oids // oids for current query
	result := oids    // result. not using keys of m because it's unordered
	for len(queryOids) > 0 {
		rows, err := queryByOids(tx, "referred", queryOids)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		queryOids = make([]string, 0)
		for rows.Next() {
			var referred string
			if err = rows.Scan(&referred); err != nil {
				return nil, err
			}
			for _, v := range strings.Split(referred, ",") {
				if len(v) > 0 && walked[v] == false {
					queryOids = append(queryOids, v)
					result = append(result, v)
					walked[v] = true
				}
			}
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// Gc TODO
func GC(db *sql.DB, oids []string) ([]string, error) {
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	oids, err = walk(tx, oids, nil)
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

	if len(deletable) > 0 {
		args := toInterfaces(deletable)
		_, err := tx.Exec("DELETE FROM "+table+" WHERE oid IN (?"+strings.Repeat(",?", len(deletable)-1)+")", args...)
		if err != nil {
			return nil, err
		}
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}
	return deletable, nil
}

// TODO
func Export(db *sql.DB, path string, oid string, ref string) ([]string, error) {
	// 1: quick test: is repo up-to-date ?
	// 2: compare oids, decide which oids to be written
	// 3: write oids
	// TODO
	repo := newRepo(path)

	if repo.hasOid(oid) {
		return nil, nil
	}

	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Check oids on the fly ?
	repoOids, err := repo.listOids("--all")
	if err != nil {
		return nil, err
	}

	newOids, err := walk(tx, []string{oid}, repoOids)
	if err != nil {
		return nil, err
	}

	// read content
	rows, err := queryByOids(tx, "oid, zcontent", newOids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	zmap := make(map[string][]byte, len(newOids))
	for rows.Next() {
		var o string
		var z []byte
		if err = rows.Scan(&o, &z); err != nil {
			return nil, err
		}
		zmap[o] = z
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for i := len(newOids) - 1; i >= 0; i-- {
		o := newOids[i]
		z, ok := zmap[o]
		if !ok {
			return nil, sql.ErrNoRows
		}
		if err := repo.writeRawObject(o, z); err != nil {
			return nil, err
		}
	}

	if err = tx.Commit(); err != nil {
		return nil, err
	}

	// Write tag ref so that `git rev-list --all` will list these oids
	if len(ref) == 0 {
		ref = "refs/tags/gitdb/" + oid
	}
	if err := repo.writeRef(ref, oid); err != nil {
		return newOids, err
	}

	return newOids, nil
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

// toInterfaces converts []string to []interface{}. It is useful in
// db.Query and db.Exec.
func toInterfaces(a []string) []interface{} {
	result := make([]interface{}, 0, len(a))
	for _, v := range a {
		result = append(result, v)
	}
	return result
}

// queryByOids fetch db rows by oids.
func queryByOids(tx *sql.Tx, columns string, oids []string) (*sql.Rows, error) {
	if (len(oids)) == 0 {
		return tx.Query("SELECT " + columns + " FROM " + table + " WHERE 0=1")
	}
	args := toInterfaces(oids)
	return tx.Query("SELECT "+columns+" FROM "+table+" WHERE oid IN (?"+strings.Repeat(",?", len(args)-1)+")", args...)
}

// unseenOids removes oids which are already stored in database.
func unseenOids(tx *sql.Tx, oids []string) (unseen []string, err error) {
	rows, err := queryByOids(tx, "oid", oids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	exists := make([]string, 0)
	for rows.Next() {
		var oid string
		if err = rows.Scan(&oid); err != nil {
			return nil, err
		}
		exists = append(exists, oid)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}

	return minus(oids, exists), nil
}

func Import(db *sql.DB, path string, ref string) (refOid string, oids []string, err error) {
	// 1: list ids
	// 2: what ids are we missing
	// 3: import that ids
	repo := newRepo(path)
	oids, err = repo.listOids(ref)
	if err != nil || len(oids) == 0 {
		return "", nil, err
	}
	refOid = oids[0]

	tx, err := db.Begin()
	if err != nil {
		return refOid, nil, err
	}
	defer tx.Rollback()

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

	if err = tx.Commit(); err != nil {
		return refOid, nil, err
	}

	return refOid, oids, nil
}
