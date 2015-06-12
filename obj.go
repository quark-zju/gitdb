package gitdb

import (
	"bytes"
	"compress/zlib"
	"encoding/hex"
	"fmt"
	"regexp"
)

// gitObject is a lightweight description of a git object.
type gitObject struct {
	Oid  string
	Type string
	Body []byte
}

var oidRegex *regexp.Regexp = regexp.MustCompile("^[0-9a-f]{40}$")

// isOid returns whether a string is valid git object id (ie. sha1 hex).
func isOid(oid string) bool {
	return oidRegex.MatchString(oid)
}

// referredOids returns oids the git object depends on.
// For blob object, returns empty array.
// For commit object, returns tree oid, followed by parent oids.
// For tree object, returns tree and blob oids referred directly.
// For other (unsupported) objects, returns empty array.
func (o *gitObject) referredOids() []string {
	var oids []string
	switch o.Type {
	case "tree":
		// mode + " " + name + "\0" + binOid (20 bytes)
		for i := 0; i < len(o.Body)-20; i++ {
			if o.Body[i] == 0 {
				oids = append(oids, hex.EncodeToString(o.Body[i+1:i+21]))
				i += 20
			}
		}
	case "commit":
		// first line: "tree " + oid + "\n"
		// followed by 0 or more: "parent " + oid + "\n"
		for i := len("tree "); i < len(o.Body); i += len("parent \n") + 40 {
			oid := string(o.Body[i : i+40])
			if isOid(oid) {
				oids = append(oids, oid)
			} else {
				break
			}
		}
	case "blob":
		// blob does not refer to other objects
	}
	return oids
}

// zlibContent returns zlib compressed git object header + body.
func (o *gitObject) zlibContent() []byte {
	var b bytes.Buffer
	w := zlib.NewWriter(&b)
	w.Write([]byte(fmt.Sprintf("%s %d\x00", o.Type, len(o.Body))))
	w.Write(o.Body)
	w.Close()
	return b.Bytes()
}
