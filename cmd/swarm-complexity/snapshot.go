package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
	"path"
	"strconv"
	"strings"
)

type sourceFile struct {
	path, object string
	data         []byte
}

func git(ctx context.Context, repo string, args ...string) ([]byte, error) {
	c := exec.CommandContext(ctx, "git", args...)
	c.Dir = repo
	var stderr bytes.Buffer
	c.Stderr = &stderr
	b, err := c.Output()
	if err != nil {
		return nil, fmt.Errorf("git %s: %w: %s", args[0], err, stderr.String())
	}
	return b, nil
}

func objectID(s string) bool {
	if (len(s) != 40 && len(s) != 64) || strings.Trim(s, "0") == "" {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func revision(ctx context.Context, repo, ref string) (string, error) {
	if ref == "" {
		return "", fmt.Errorf("Git revision is required")
	}
	b, err := git(ctx, repo, "rev-parse", "--verify", "--end-of-options", ref+"^{commit}")
	if err != nil {
		return "", err
	}
	sha := strings.TrimSpace(string(b))
	if !objectID(sha) {
		return "", fmt.Errorf("invalid resolved revision %q", sha)
	}
	return sha, nil
}

func inventory(ctx context.Context, repo, sha string) ([]sourceFile, []fileRecord, error) {
	b, err := git(ctx, repo, "ls-tree", "-r", "-z", "--full-tree", sha)
	if err != nil {
		return nil, nil, err
	}
	var sources []sourceFile
	files := []fileRecord{}
	for _, entry := range bytes.Split(bytes.TrimSuffix(b, []byte{0}), []byte{0}) {
		f, err := treeFile(string(entry))
		if err != nil {
			return nil, nil, err
		}
		if f.path == "" {
			continue
		}
		if strings.HasSuffix(f.path, "_test.go") {
			files = append(files, fileRecord{f.path, "test"})
		} else {
			sources = append(sources, f)
		}
	}
	if len(sources) == 0 {
		return nil, nil, fmt.Errorf("empty authored Go candidate inventory at %s", sha)
	}
	if err := readBlobs(ctx, repo, sources); err != nil {
		return nil, nil, err
	}
	return sources, files, nil
}

func treeFile(entry string) (sourceFile, error) {
	meta, name, ok := strings.Cut(entry, "\t")
	if !ok {
		return sourceFile{}, fmt.Errorf("malformed Git tree entry")
	}
	if !strings.HasSuffix(name, ".go") {
		return sourceFile{}, nil
	}
	fields := strings.Fields(meta)
	if len(fields) != 3 || fields[1] != "blob" || !objectID(fields[2]) {
		return sourceFile{}, fmt.Errorf("unsupported tracked Go object %q", name)
	}
	if fields[0] != "100644" && fields[0] != "100755" {
		return sourceFile{}, fmt.Errorf("tracked Go source must be a regular file: %q", name)
	}
	if path.Clean(name) != name || path.IsAbs(name) || strings.HasPrefix(name, "../") {
		return sourceFile{}, fmt.Errorf("invalid tracked path %q", name)
	}
	return sourceFile{path: name, object: fields[2]}, nil
}

func readBlobs(ctx context.Context, repo string, files []sourceFile) error {
	var requests strings.Builder
	for _, f := range files {
		fmt.Fprintln(&requests, f.object)
	}
	c := exec.CommandContext(ctx, "git", "cat-file", "--batch")
	c.Dir, c.Stdin = repo, strings.NewReader(requests.String())
	var stderr bytes.Buffer
	c.Stderr = &stderr
	b, err := c.Output()
	if err != nil {
		return fmt.Errorf("read Git blobs: %w: %s", err, stderr.String())
	}
	r := bufio.NewReader(bytes.NewReader(b))
	for i := range files {
		data, err := readBlob(r, files[i].object)
		if err != nil {
			return fmt.Errorf("%s: %w", files[i].path, err)
		}
		files[i].data = data
	}
	if _, err := r.ReadByte(); err != io.EOF {
		return fmt.Errorf("unexpected trailing Git blob data")
	}
	return nil
}

func readBlob(r *bufio.Reader, object string) ([]byte, error) {
	header, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	f := strings.Fields(header)
	if len(f) != 3 || f[0] != object || f[1] != "blob" {
		return nil, fmt.Errorf("invalid Git blob header %q", header)
	}
	size, err := strconv.Atoi(f[2])
	if err != nil || size < 0 {
		return nil, fmt.Errorf("invalid Git blob size")
	}
	b := make([]byte, size)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, err
	}
	if end, err := r.ReadByte(); err != nil || end != '\n' {
		return nil, fmt.Errorf("truncated Git blob")
	}
	return b, nil
}
