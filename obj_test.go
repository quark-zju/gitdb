package gitdb

import (
	"bytes"
	"strings"
	"testing"
)

func TestIsOid(t *testing.T) {
	cases := []struct {
		Oid      string
		Expected bool
	}{
		{"d318a662507e9592830be3a3cbbb2f670b6ce7a5", true},
		{"d318a662507e9592830be3a3cbbb2f670b6ce7A5", false},
		{"cd318a662507e9592830be3a3cbbb2f670b6ce7a5", false},
		{"d318a662507e9592830be3a3cbbb2f670b6ce7a5 ", false},
		{"d318a66207e9592830be3a3cbbb2f670b6ce7a5 ", false},
		{"", false},
	}

	for _, c := range cases {
		if isOid(c.Oid) != c.Expected {
			t.Errorf("IsOid(%v) != %v", c.Oid, c.Expected)
		}
	}
}

func TestReferredOids(t *testing.T) {
	// blob
	obj := gitObj{Type: "blob"}
	referredOids := obj.referredOids()
	if len(referredOids) != 0 {
		t.Errorf("ReferredOids for blob object is incorrect")
	}

	// tree
	obj = gitObj{
		Type: "tree",
		Body: []byte("100644 a\x00\x01\x00\x02\x00\x03\x00\x04\x00\x05\x00\x06\x00\x07\x00\x08\x00\x09\x00\x00\x00" +
			"100644 b\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf1"),
	}
	referredOids = obj.referredOids()
	if strings.Join(referredOids, ",") != "0100020003000400050006000700080009000000,00000000000000000000000000000000000000f1" {
		t.Errorf("ReferredOids for tree object is incorrect")
	}

	// commit
	oids := []string{
		"d318a662507e9592830be3a3cbbb2f670b6ce7a5",
		"7b9fe328531202c2f5c2906b21b3a2677a799c40",
		"0702d34643a8b644846748a00c425ef76a4634d3",
	}
	obj = gitObj{
		Type: "commit",
		Body: []byte("" +
			"tree " + oids[0] + "\n" +
			"parent " + oids[1] + "\n" +
			"parent " + oids[2] + "\n" +
			"author Foo <a@example.com> 1433758557 +0800\n" +
			"committer Foo Wu <a@example.com> 1433758557 +0800\n" +
			"\n" +
			"Merge branch 'bbb' into aaa\n"),
	}

	referredOids = obj.referredOids()
	if strings.Join(oids, ",") != strings.Join(referredOids, ",") {
		t.Errorf("ReferredOids for commit object is incorrect")
	}

	// other
	obj = gitObj{Type: "unknown"}
	referredOids = obj.referredOids()
	if len(referredOids) != 0 {
		t.Errorf("ReferredOids for unknown object is incorrect")
	}
}

func TestZcontent(t *testing.T) {
	obj := gitObj{
		Oid:  "496d6428b9cf92981dc9495211e6e1120fb6f2ba",
		Type: "tree",
		Body: []byte{0x31, 0x30, 0x30, 0x36, 0x34, 0x34, 0x20, 0x61, 0x00, 0xe6, 0x9d, 0xe2,
			0x9b, 0xb2, 0xd1, 0xd6, 0x43, 0x4b, 0x8b, 0x29, 0xae, 0x77, 0x5a, 0xd8,
			0xc2, 0xe4, 0x8c, 0x53, 0x91},
	}
	zcontent := obj.zcontent()
	obj2, err := newGitObjFromZcontent(zcontent)
	if err != nil || obj2 == nil {
		t.Errorf("newGitObjFromZcontent fails to decode: %s", err)
	}
	if obj2.Oid != obj.Oid || obj2.Type != obj.Type || bytes.Compare(obj2.Body, obj.Body) != 0 {
		t.Errorf("Git object differs after encoding to zcontent and decoding: %v %v", obj, obj2)
	}
}
