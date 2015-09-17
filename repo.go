package gitdb

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// repo describes a git repository stored in local filesystem.
type repo struct {
	dir string
}

// newRepo returns a new Repo that mapped to a git repo in local filesystem.
func newRepo(dir string) *repo {
	altDir := filepath.Join(dir, ".git")
	if fi, err := os.Stat(altDir); err == nil && fi.IsDir() {
		return &repo{dir: altDir}
	} else {
		return &repo{dir: dir}
	}
}

// listOids lists the git object IDs in hex form.
// ref is a git commit, for example, "HEAD", "master", "17ae1d07" etc.
func (r *repo) listOids(ref string) (oids []Oid, err error) {
	cmd := exec.Command("git", "--git-dir", r.dir, "rev-list", "--objects", ref)
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err = cmd.Start(); err != nil {
		return nil, err
	}

	reader := bufio.NewReader(out)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		oid := Oid(strings.Split(line, " ")[0][0:40])
		if oid.IsValid() {
			oids = append(oids, oid)
		}
	}

	cmd.Wait()
	return oids, nil
}

// readObjects reads git objects in batch and returns an array of GitObject.
func (r *repo) readObjects(oids []Oid) (objs []*gitObj, err error) {
	cmd := exec.Command("git", "--git-dir", r.dir, "cat-file", "--batch")
	cmd.Stdin = strings.NewReader(joinOids(oids, "\n"))
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}

	err = cmd.Start()
	if err != nil {
		return nil, err
	}

	objs = make([]*gitObj, 0, len(oids))
	for {
		// header: sha1 + " " + type + " " + size + "\n"
		var obj gitObj
		var size int
		n, err := fmt.Fscanf(out, "%s %s %d\n", &obj.Oid, &obj.Type, &size)
		if err != nil || n < 3 {
			break
		}

		// body: bytes + "\n"
		obj.Body = make([]byte, size)
		n, err = io.ReadFull(out, obj.Body)
		fmt.Fscanf(out, "\n")
		if err != nil || n < size {
			break
		}
		objs = append(objs, &obj)
	}

	cmd.Process.Kill()
	cmd.Wait()

	if len(objs) != len(oids) {
		return objs, fmt.Errorf("git cat-file only returns %d objects, but %d required", len(objs), len(oids))
	}

	return objs, nil
}

// hasOid checks whether an object exists or not.
// It runs an external git process so do not call frequently.
func (r *repo) hasOid(oid Oid) bool {
	cmd := exec.Command("git", "--git-dir", r.dir, "cat-file", "-e", string(oid))
	err := cmd.Run()
	return err == nil
}

func (r *repo) writeRawObject(oid Oid, zlibContent []byte) error {
	dir := filepath.Join(r.dir, "objects", string(oid)[0:2])
	path := filepath.Join(dir, string(oid)[2:40])
	if fi, err := os.Stat(path); err == nil && fi.Mode().IsRegular() {
		return nil
	}

	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return err
	}

	tmpPath := fmt.Sprintf("%s.%d", path, rand.Int())
	err = ioutil.WriteFile(tmpPath, zlibContent, 0444)
	if err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

func (r *repo) writeRef(ref string, oid Oid) error {
	if filepath.IsAbs(ref) || strings.Contains(ref, "..") {
		return errUnsafeRefName(ref)
	}
	ref = filepath.Clean(ref)
	ok := ref == "HEAD" || strings.HasPrefix(ref, "refs/tags/") || strings.HasPrefix(ref, "refs/heads/")
	if !ok {
		return errUnsafeRefName(ref)
	}
	path := filepath.Join(r.dir, ref)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return ioutil.WriteFile(path, []byte(oid), 0644)
}

type errUnsafeRefName string

func (e errUnsafeRefName) Error() string {
	return "unsafe ref name: " + string(e)
}
