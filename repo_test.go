package gitdb

import (
	"bytes"
	"compress/zlib"
	"crypto/sha1"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"
)

var gitVer float64 = -1
var tmpDir string = filepath.Join(os.TempDir(), "gitdb-test")

func checkGit() bool {
	if gitVer < 0 {
		cmd := exec.Command("git", "--version")
		var out bytes.Buffer
		cmd.Stdout = &out
		err := cmd.Run()
		if err != nil {
			gitVer = 0
		} else {
			verStr := regexp.MustCompile(`[0-9]+(?:\.[0-9]+)?`).FindString(out.String())
			gitVer, _ = strconv.ParseFloat(verStr, 64)
			if gitVer < 1.6 {
				fmt.Printf("git %v >= 1.6 not found. skip tests\n", gitVer)
			}
		}
	}
	return gitVer >= 1.6
}

func dummyBytes(n int, prefix string) []byte {
	v := make([]byte, 0, n+len(prefix))
	v = append(v, []byte(prefix)...)
	for i := 0; i < n; i++ {
		if i%10 == 0 {
			v = append(v, '\n')
		} else {
			v = append(v, byte('a')+byte(i%10))
		}
	}
	return v
}

var tick int = 0

func uniqueString() string {
	tick += 1
	return strconv.Itoa(tick) + "\n"
}

func createRandomFile(dir string, prefix string, size int) {
	name := prefix + string([]rune{'a' + rune(rand.Intn(26))})
	d := dir
	for rand.Intn(2) > 0 {
		if len(d)-len(dir) > 2 {
			break
		}
		d = filepath.Join(d, string([]rune{'0' + rune(rand.Intn(10))}))
	}
	os.MkdirAll(d, 0755)
	ioutil.WriteFile(filepath.Join(d, name), dummyBytes(rand.Intn(size), uniqueString()), 0644)
}

func createRandomRepo(dir string, steps int) {
	os.MkdirAll(dir, 0755)
	os.Chdir(dir)
	exec.Command("git", "init", dir).Run()
	exec.Command("git", "checkout", "-b", "master").Run()
	exec.Command("git", "config", "--local", "user.name", "Alice").Run()
	exec.Command("git", "config", "--local", "user.email", "a@example.com").Run()

	for i := 0; i < steps; i++ {
		fmt.Printf("Write repo %s: %d / %d\r", dir, i, steps)
		v := i % 15
		if i > 30 {
			v = rand.Intn(15)
		}
		if i == steps-1 {
			v = 14 // commit
		}
		switch v {
		case 4, 14: // commit
			exec.Command("git", "add", "--all", ".").Run()
			exec.Command("git", "commit", "--allow-empty", "-m", "meh").Run()
		case 7: // merge
			exec.Command("git", "checkout", "-b", "dev").Run()
			createRandomFile(dir, "d-", 8000)
			exec.Command("git", "add", "--all", ".").Run()
			exec.Command("git", "commit", "--allow-empty", "-m", "poi", "--author", "Bob <b@example.com>").Run()
			exec.Command("git", "checkout", "master").Run()
			createRandomFile(dir, "m-", 8000)
			exec.Command("git", "add", "--all", ".").Run()
			exec.Command("git", "commit", "--allow-empty", "-m", "huh").Run()
			exec.Command("git", "merge", "--no-ff", "--no-edit", "dev").Run()
			exec.Command("git", "branch", "-D", "dev").Run()
		default:
			// write file
			createRandomFile(dir, "", 128000)
		}
	}
	fmt.Printf("Write repo %s: DONE     \n", dir)
}

func verifyGitObject(obj *gitObject) bool {
	z := obj.zlibContent()
	// decompress
	r, _ := zlib.NewReader(bytes.NewBuffer(z))
	defer r.Close()
	b := make([]byte, 500000)
	n, _ := io.ReadFull(r, b)
	b = b[0:n]
	// verify sha1
	h := sha1.New()
	h.Write(b)
	s := fmt.Sprintf("%x", h.Sum(nil))
	return s == obj.Oid
}

func TestRepo(t *testing.T) {
	if !checkGit() {
		return
	}

	dir := filepath.Join(tmpDir, "a")
	n := 30
	os.RemoveAll(dir)
	createRandomRepo(dir, n)

	r := newRepo(dir)

	// Test ListOids
	oids, e := r.listOids("HEAD")
	if e != nil {
		t.Fatal("Failed to listOids: ", e)
	}
	for _, v := range oids {
		if !isOid(v) {
			t.Error("Not oid: ", v)
		}
	}
	if len(oids) < n {
		t.Fatal("len(oids) = ", len(oids), " < ", n)
	}

	// Test ReadObjects
	objs, e := r.readObjects(oids)
	if e != nil {
		t.Fatal("Failed to readObjects: ", e)
	}
	for _, o := range objs {
		if verifyGitObject(o) == false {
			t.Error("Git object checksum mismatch: ", o.Oid)
		}
	}
}
