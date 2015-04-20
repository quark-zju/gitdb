package gitdb

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// repo describes a git repository stored in local filesystem.
type repo struct {
	dir string
}

// errUnexpectedOutput means while reading output of a git command, we cannot
// get expected output. Possibly because ill-formed output format caused by
// wrong git version, the git process exited abnormally, etc.
var errUnexpectedOutput = errors.New("unexpected git output")

// newRepo returns a new Repo that mapped to a git repo in local filesystem.
func newRepo(dir string) *repo {
	altDir := filepath.Join(dir, ".git")
	if isDir(altDir) {
		return &repo{dir: altDir}
	} else {
		return &repo{dir: dir}
	}
}

// listOids lists the git object IDs in hex form.
// ref is a git commit, for example, "HEAD", "master", "17ae1d07" etc.
func (r *repo) listOids(ref string) ([]string, error) {
	oids := make([]string, 0)
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
		oid := strings.Split(line, " ")[0][0:40]
		if isOid(oid) {
			oids = append(oids, oid)
		}
	}

	cmd.Wait()
	return oids, nil
}

// readObjects reads git objects in batch and returns an array of GitObject.
func (r *repo) readObjects(oids []string) (objs []*gitObject, err error) {
	cmd := exec.Command("git", "--git-dir", r.dir, "cat-file", "--batch")
	cmd.Stdin = strings.NewReader(strings.Join(oids, "\n"))
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}

	err = cmd.Start()
	if err != nil {
		return nil, err
	}

	objs = make([]*gitObject, 0, len(oids))
	for {
		// header: sha1 + " " + type + " " + size + "\n"
		var obj gitObject
		var size int
		var lf rune
		n, err := fmt.Fscanf(out, "%s %s %d%c", &obj.Oid, &obj.Type, &size, &lf)
		if err != nil || n < 4 || lf != '\n' {
			break
		}

		// body: bytes + "\n"
		obj.Body = make([]byte, size)
		n, err = io.ReadFull(out, obj.Body)
		fmt.Fscanf(out, "%c", &lf)
		if err != nil || n < size || lf != '\n' {
			break
		}
		objs = append(objs, &obj)
	}

	err = cmd.Wait()
	if err != nil {
		return objs, err
	}

	if len(objs) != len(oids) {
		return objs, errUnexpectedOutput
	}

	return objs, nil
}

// hasOid checks whether an object exists or not.
// It runs an external git process so do not use frequently.
func (r *repo) hasOid(oid string) bool {
	cmd := exec.Command("git", "--git-dir", r.dir, "cat-file", "-e", oid)
	err := cmd.Run()
	return err == nil
}

func (r *repo) writeRawObject(oid string, zlibContent []byte) error {
	dir := filepath.Join(r.dir, "objects", oid[0:2])
	path := filepath.Join(dir, oid[2:40])
	if isFile(path) {
		return nil
	}

	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(path, zlibContent, 0444)
	if err != nil {
		return err
	}
	return nil
}

var unsafeRefName = errors.New("unsafe ref name")

func (r *repo) writeRef(ref string, oid string) error {
	if filepath.IsAbs(ref) || strings.Contains(ref, "..") {
		return unsafeRefName
	}
	ref = filepath.Clean(ref)
	ok := ref == "HEAD" || strings.HasPrefix(ref, "refs/tags/") || strings.HasPrefix(ref, "refs/heads/")
	if !ok {
		return unsafeRefName
	}
	path := filepath.Join(r.dir, ref)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return ioutil.WriteFile(path, []byte(oid), 0644)
}

func isDir(path string) bool {
	fi, err := os.Stat(path)
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeDir) != 0
}

func isFile(path string) bool {
	fi, err := os.Stat(path)
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeDir) == 0
}
