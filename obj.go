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

// Oid is a 40-char sha1sum in hex form, used as the ID of a git object.
type Oid string

// gitObj is a lightweight representation of a git object.
type gitObj struct {
	Oid  Oid
	Type string
	Body []byte
}

var oidRegex *regexp.Regexp = regexp.MustCompile("^[0-9a-f]{40}$")

// IsValid tests whether o is valid by checking whether it is 40-char sha1sum.
func (o Oid) IsValid() bool {
	return oidRegex.MatchString(string(o))
}

// referredOids returns oids the git object depends on.
// For blob object, returns empty array.
// For commit object, returns tree oid, followed by parent oids.
// For tree object, returns tree and blob oids referred directly.
// For other (unsupported) objects, returns empty array.
func (o *gitObj) referredOids() []Oid {
	var oids []Oid
	switch o.Type {
	case "tree":
		// mode + " " + name + "\0" + binOid (20 bytes)
		for i := 0; i < len(o.Body)-20; i++ {
			if o.Body[i] == 0 {
				oids = append(oids, Oid(hex.EncodeToString(o.Body[i+1:i+21])))
				i += 20
			}
		}
	case "commit":
		// first line: "tree " + oid + "\n"
		// followed by 0 or more: "parent " + oid + "\n"
		for i := len("tree "); i < len(o.Body); i += len("parent \n") + 40 {
			oid := Oid(o.Body[i : i+40])
			if oid.IsValid() {
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

// newGitObjFromZcontent constructs a new gitObj using zcontent.
// zcontent has the same format as the file of a unpacked git object.
func newGitObjFromZcontent(zcontent []byte) (*gitObj, error) {
	// Uncompress
	r, err := zlib.NewReader(bytes.NewReader(zcontent))
	if err != nil {
		return nil, err
	}
	defer r.Close()

	var out bytes.Buffer
	io.Copy(&out, r)
	b := out.Bytes()

	// Find header delimiter
	i := bytes.IndexByte(b, '\x00')
	if i <= 0 || i >= len(b) {
		return nil, errInvalidZcontent("no header delimiter")
	}

	// Calculate SHA1 and parse header to get oid and type, size
	o := gitObj{Oid: Oid(fmt.Sprintf("%040x", sha1.Sum(b))), Body: b[i+1:]}
	var size int
	if n, err := fmt.Sscanf(string(b[0:i]), "%s %d", &o.Type, &size); err != nil || n < 2 {
		return nil, errInvalidZcontent("illegal header + " + string(b[0:i]))
	}
	if size != len(o.Body) {
		return nil, errInvalidZcontent(fmt.Sprintf("body size mismatch: claimed %d, actual %d", size, len(o.Body)))
	}

	return &o, nil
}

// joinOids is like strings.Join but works with []Oid
func joinOids(oids []Oid, sep string) string {
	if len(oids) == 0 {
		return ""
	} else if len(oids) == 1 {
		return string(oids[0])
	} else {
		n := len(oids)*(40+len(sep)) - len(sep)
		b := make([]byte, n)
		bp := copy(b, oids[0][0:40])
		for _, oid := range oids[1:] {
			bp += copy(b[bp:], sep)
			bp += copy(b[bp:], oid[0:40])
		}
		return string(b)
	}
}
