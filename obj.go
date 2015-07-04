package gitdb

import (
	"bytes"
	"compress/zlib"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"regexp"
)

// gitObj is a lightweight description of a git object.
type gitObj struct {
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
func (o *gitObj) referredOids() []string {
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

// zcontent returns zlib compressed git object header + body.
func (o *gitObj) zcontent() []byte {
	var b bytes.Buffer
	w := zlib.NewWriter(&b)
	w.Write([]byte(fmt.Sprintf("%s %d\x00", o.Type, len(o.Body))))
	w.Write(o.Body)
	w.Close()
	return b.Bytes()
}

type errInvalidZcontent string

func (e errInvalidZcontent) Error() string {
	return "illformed zcontent: " + string(e)
}

// TODO doc
func newGitObjFromZcontent(zcontent []byte) (*gitObj, error) {
	r, err := zlib.NewReader(bytes.NewReader(zcontent))
	if err != nil {
		return nil, err
	}
	defer r.Close()

	var out bytes.Buffer
	io.Copy(&out, r)
	b := out.Bytes()
	i := bytes.IndexByte(b, '\x00')
	if i <= 0 || i >= len(b) {
		return nil, errInvalidZcontent("no header delimiter")
	}

	o := gitObj{Oid: fmt.Sprintf("%040x", sha1.Sum(b)), Body: b[i+1:]}
	var size int
	if _, err := fmt.Sscanf(string(b[0:i]), "%s %d", &o.Type, &size); err != nil {
		return nil, errInvalidZcontent("confusing header + " + string(b[0:i]))
	}
	if size != len(o.Body) {
		return nil, errInvalidZcontent(fmt.Sprintf("body size mismatch %d vs %d", size, len(o.Body)))
	}
	return &o, nil
}
