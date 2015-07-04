package gitdb

import (
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
