package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/division-sh/swarm/scripts/rewrite-stages-2566/formatproof"
)

type accountingProofInput struct {
	Path   string `json:"path"`
	Mode   string `json:"mode"`
	Object string `json:"object"`
	SHA256 string `json:"sha256"`
}

type accountingProofManifest struct {
	Version int                    `json:"version"`
	Head    string                 `json:"head"`
	Tree    string                 `json:"tree"`
	Inputs  []accountingProofInput `json:"inputs"`
}

type accountingProofSnapshot struct {
	Root         string
	Origin       string
	ConfigDigest string
	Manifest     accountingProofManifest
}

// Tree objects, not archive attributes or a clean index, define proof inputs.
// The complete tracked tree covers runtime indirection and source read as data.
func accountingTreeInputs(ctx context.Context, repo, head string, consume func(accountingProofInput, []byte) error) (accountingProofManifest, error) {
	manifest := accountingProofManifest{Version: 1, Head: head}
	if !hexIdentity(head, 40) {
		return manifest, fmt.Errorf("invalid proof source commit")
	}
	tree, err := accountingGit(ctx, repo, "rev-parse", head+"^{tree}")
	if err != nil {
		return manifest, err
	}
	manifest.Tree = strings.TrimSpace(string(tree))
	entries, err := accountingGit(ctx, repo, "ls-tree", "-r", "-z", "--full-tree", head)
	if err != nil {
		return manifest, err
	}
	command := accountingGitCommand(ctx, repo, "cat-file", "--batch")
	var requests bytes.Buffer
	for _, entry := range bytes.Split(entries, []byte{0}) {
		if len(entry) == 0 {
			continue
		}
		metadata, name, ok := bytes.Cut(entry, []byte{'\t'})
		fields := strings.Fields(string(metadata))
		path := string(name)
		if !ok || len(fields) != 3 || (fields[0] != "100644" && fields[0] != "100755") || fields[1] != "blob" || !hexIdentity(fields[2], 40) || !filepath.IsLocal(path) || filepath.ToSlash(filepath.Clean(path)) != path || strings.Contains(path, "\n") || path == ".git" || strings.HasPrefix(path, ".git/") || path == "test-results" || strings.HasPrefix(path, "test-results/") {
			return manifest, fmt.Errorf("unsupported proof tree entry %q", entry)
		}
		manifest.Inputs = append(manifest.Inputs, accountingProofInput{Path: path, Mode: fields[0], Object: fields[2]})
		fmt.Fprintln(&requests, fields[2])
	}
	command.Stdin = &requests
	output, err := command.StdoutPipe()
	if err != nil {
		return manifest, err
	}
	if err = command.Start(); err != nil {
		return manifest, err
	}
	reader := bufio.NewReader(output)
	var readErr error
	for i := range manifest.Inputs {
		input := &manifest.Inputs[i]
		var body []byte
		body, readErr = readAccountingBlob(reader, input.Object)
		if readErr != nil {
			break
		}
		input.SHA256 = formatproof.Digest(body)
		if consume != nil {
			if readErr = consume(*input, body); readErr != nil {
				break
			}
		}
	}
	// A reader refusal must not leave cat-file blocked on its output pipe.
	closeErr := output.Close()
	if err = errors.Join(readErr, closeErr, command.Wait(), ctx.Err()); err != nil {
		return manifest, err
	}
	sort.Slice(manifest.Inputs, func(i, j int) bool { return manifest.Inputs[i].Path < manifest.Inputs[j].Path })
	return manifest, nil
}

func readAccountingBlob(reader *bufio.Reader, expected string) ([]byte, error) {
	header, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	fields := strings.Fields(header)
	if len(fields) != 3 || fields[0] != expected || fields[1] != "blob" {
		return nil, fmt.Errorf("proof input is not its exact Git blob")
	}
	size, err := strconv.ParseInt(fields[2], 10, 64)
	if err != nil || size < 0 || size > 64<<20 {
		return nil, fmt.Errorf("unsupported proof blob size")
	}
	body := make([]byte, int(size))
	if _, err := io.ReadFull(reader, body); err != nil {
		return nil, err
	}
	separator, err := reader.ReadByte()
	if err != nil || separator != '\n' {
		return nil, fmt.Errorf("incomplete proof blob framing: %v", err)
	}
	object := append([]byte(fmt.Sprintf("blob %d\x00", len(body))), body...)
	if fmt.Sprintf("%x", sha1.Sum(object)) != expected {
		return nil, fmt.Errorf("proof blob object identity changed")
	}
	return body, nil
}

func freezeAccountingProof(ctx context.Context, repo, head, origin string, policy accountingPolicyObservation) (snapshot accountingProofSnapshot, result error) {
	directory, err := os.MkdirTemp("", "swarm-accounting-proof-")
	if err != nil {
		return snapshot, err
	}
	snapshot.Root = directory
	snapshot.Origin = origin
	defer func() {
		if result != nil {
			result = errors.Join(result, snapshot.Close())
		}
	}()
	if _, err = accountingGit(ctx, directory, "init", "-q", "--template="); err != nil {
		return snapshot, err
	}
	if snapshot.Manifest, err = accountingTreeInputs(ctx, repo, head, func(input accountingProofInput, body []byte) error {
		path := filepath.Join(directory, input.Path)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			return err
		}
		mode := os.FileMode(0444)
		if input.Mode == "100755" {
			mode = 0555
		}
		file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
		if err != nil {
			return err
		}
		_, writeErr := file.Write(body)
		return errors.Join(writeErr, file.Close())
	}); err != nil {
		return snapshot, err
	}
	roots := []string{head, origin, policy.Commit, policy.Policy.ExecutorSource}
	history, err := gitAccountingBlob(ctx, repo, head, ".github/test-proof-history-inputs.json")
	if err != nil {
		return snapshot, err
	}
	var historical map[string][]string
	if err = json.Unmarshal(history, &historical); err != nil || len(historical) == 0 {
		return snapshot, fmt.Errorf("missing canonical proof history inputs: %v", err)
	}
	for _, commits := range historical {
		roots = append(roots, commits...)
	}
	if err = copyAccountingObjects(ctx, repo, directory, roots); err != nil {
		return snapshot, err
	}
	for _, args := range [][]string{
		{"update-ref", "--no-deref", "HEAD", head},
		{"update-ref", "refs/remotes/origin/master", origin},
		{"remote", "add", "origin", "https://github.com/" + accountingRepository + ".git"},
		{"read-tree", head},
	} {
		if _, err = accountingGit(ctx, directory, args...); err != nil {
			return snapshot, err
		}
	}
	config, err := os.ReadFile(filepath.Join(directory, ".git/config"))
	if err != nil {
		return snapshot, err
	}
	snapshot.ConfigDigest = formatproof.Digest(config)
	if err = os.Chmod(filepath.Join(directory, ".git/config"), 0444); err != nil {
		return snapshot, err
	}
	if err = os.MkdirAll(filepath.Join(directory, "test-results"), 0700); err != nil {
		return snapshot, err
	}
	// Tools may write outputs/private copies, never the proof's source tree.
	err = filepath.WalkDir(directory, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == filepath.Join(directory, ".git") || path == filepath.Join(directory, "test-results") {
			return filepath.SkipDir
		}
		if entry.IsDir() {
			return os.Chmod(path, 0555)
		}
		return nil
	})
	return snapshot, errors.Join(err, snapshot.Validate(ctx))
}

// Retain ancestry as commits and complete trees for the declared read cuts.
// No shared object database, replacement refs, or lazy remote reads survive.
func copyAccountingObjects(ctx context.Context, source, target string, roots []string) error {
	for _, root := range roots {
		if !hexIdentity(root, 40) {
			return fmt.Errorf("unbound proof history object")
		}
	}
	commits, err := accountingGit(ctx, source, append([]string{"rev-list"}, roots...)...)
	if err != nil {
		return fmt.Errorf("proof ancestry unavailable: %w", err)
	}
	objects, err := accountingGit(ctx, source, append([]string{"rev-list", "--objects", "--no-walk", "--no-object-names"}, roots...)...)
	if err != nil {
		return fmt.Errorf("proof history tree unavailable: %w", err)
	}
	identities := map[string]bool{}
	for _, object := range strings.Fields(string(commits) + string(objects)) {
		if !hexIdentity(object, 40) {
			return fmt.Errorf("invalid proof history object")
		}
		identities[object] = true
	}
	var names []string
	for name := range identities {
		names = append(names, name)
	}
	sort.Strings(names)
	pack, err := os.CreateTemp(filepath.Join(target, ".git"), "accounting-pack-")
	if err != nil {
		return err
	}
	defer os.Remove(pack.Name())
	producer := accountingGitCommand(ctx, source, "pack-objects", "--stdout", "--no-reuse-delta")
	producer.Stdin = strings.NewReader(strings.Join(names, "\n") + "\n")
	producer.Stdout = pack
	if err = errors.Join(producer.Run(), pack.Close()); err != nil {
		return err
	}
	input, err := os.Open(pack.Name())
	if err != nil {
		return err
	}
	consumer := accountingGitCommand(ctx, target, "index-pack", "--stdin")
	consumer.Stdin = input
	consumer.Stdout = io.Discard
	if err := errors.Join(consumer.Run(), input.Close()); err != nil {
		return err
	}
	_, err = accountingGit(ctx, target, append([]string{"rev-list"}, roots...)...)
	return err
}

func (snapshot accountingProofSnapshot) Validate(ctx context.Context) error {
	expected, err := accountingTreeInputs(ctx, snapshot.Root, snapshot.Manifest.Head, nil)
	if err != nil || !reflect.DeepEqual(expected, snapshot.Manifest) {
		return fmt.Errorf("proof manifest differs from Git objects: %v", err)
	}
	allowed := map[string]accountingProofInput{}
	directories := map[string]bool{".": true}
	for _, input := range snapshot.Manifest.Inputs {
		allowed[input.Path] = input
		for dir := filepath.Dir(input.Path); dir != "."; dir = filepath.Dir(dir) {
			directories[filepath.ToSlash(dir)] = true
		}
	}
	err = filepath.WalkDir(snapshot.Root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		name, err := filepath.Rel(snapshot.Root, path)
		if err != nil {
			return err
		}
		name = filepath.ToSlash(name)
		if name == ".git" || name == "test-results" {
			return filepath.SkipDir
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if !directories[name] || info.Mode().Perm()&0222 != 0 {
				return fmt.Errorf("unexpected or writable proof directory %s", name)
			}
			return nil
		}
		input, found := allowed[name]
		if !found || !info.Mode().IsRegular() || info.Mode().Perm()&0222 != 0 || (info.Mode().Perm()&0111 != 0) != (input.Mode == "100755") {
			return fmt.Errorf("unexpected, writable or replaced proof input %s", name)
		}
		body, err := os.ReadFile(path)
		if err != nil || formatproof.Digest(body) != input.SHA256 {
			return fmt.Errorf("proof input changed %s: %v", name, err)
		}
		delete(allowed, name)
		return nil
	})
	if err != nil || len(allowed) != 0 {
		return fmt.Errorf("incomplete or changed proof snapshot (%d missing): %w", len(allowed), err)
	}
	head, err := accountingGit(ctx, snapshot.Root, "rev-parse", "HEAD")
	if err != nil || strings.TrimSpace(string(head)) != snapshot.Manifest.Head {
		return fmt.Errorf("proof HEAD changed: %v", err)
	}
	if snapshot.Origin != "" {
		origin, err := accountingGit(ctx, snapshot.Root, "rev-parse", "origin/master^{commit}")
		if err != nil || strings.TrimSpace(string(origin)) != snapshot.Origin {
			return fmt.Errorf("proof comparison ref changed: %v", err)
		}
	}
	if snapshot.ConfigDigest != "" {
		config, err := os.ReadFile(filepath.Join(snapshot.Root, ".git/config"))
		if err != nil || formatproof.Digest(config) != snapshot.ConfigDigest {
			return fmt.Errorf("proof Git configuration changed: %v", err)
		}
	}
	for _, forbidden := range []string{"objects/info/alternates", "info/grafts", "info/attributes", "info/exclude", "shallow", "refs/replace", "config.worktree"} {
		if _, err := os.Lstat(filepath.Join(snapshot.Root, ".git", forbidden)); !os.IsNotExist(err) {
			return fmt.Errorf("undeclared proof Git input %s: %v", forbidden, err)
		}
	}
	return nil
}

func accountingManifestDigest(manifest accountingProofManifest) string {
	body, _ := json.Marshal(manifest)
	return formatproof.Digest(body)
}

func verifyFrozenAccountingSource(ctx context.Context, root string) error {
	head, err := accountingSource(ctx, root, "", true)
	if err != nil {
		return err
	}
	manifest, err := accountingTreeInputs(ctx, root, head, nil)
	if err != nil {
		return err
	}
	if accountingManifestDigest(manifest) != accountingProofDigest {
		return fmt.Errorf("frozen proof source differs from supervisor manifest")
	}
	return (accountingProofSnapshot{Root: root, Origin: accountingProofOrigin, ConfigDigest: accountingProofConfig, Manifest: manifest}).Validate(ctx)
}

func checkAccountingExtractedTree(directory string, manifest accountingProofManifest, extra string) error {
	remaining := map[string]accountingProofInput{}
	for _, input := range manifest.Inputs {
		remaining[input.Path] = input
	}
	err := filepath.WalkDir(directory, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		name, err := filepath.Rel(directory, path)
		if err != nil || name == "." {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		name = filepath.ToSlash(name)
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() {
			return fmt.Errorf("nonregular executor input %s: %v", name, err)
		}
		if name == extra && extra != "" {
			return nil
		}
		input, ok := remaining[name]
		if !ok || (info.Mode().Perm()&0111 != 0) != (input.Mode == "100755") {
			return fmt.Errorf("unapproved executor input %s", name)
		}
		body, err := os.ReadFile(path)
		if err != nil || formatproof.Digest(body) != input.SHA256 {
			return fmt.Errorf("executor input differs from Git object %s: %v", name, err)
		}
		delete(remaining, name)
		return nil
	})
	if err != nil {
		return err
	}
	if len(remaining) != 0 {
		return fmt.Errorf("executor archive omitted %d Git inputs", len(remaining))
	}
	return nil
}

func accountingSource(ctx context.Context, root, expected string, clean bool) (string, error) {
	body, err := accountingGit(ctx, root, "rev-parse", "HEAD")
	if err != nil {
		return "", err
	}
	head := strings.TrimSpace(string(body))
	if !hexIdentity(head, 40) || (expected != "" && head != expected) {
		return "", fmt.Errorf("accounting source changed")
	}
	if clean {
		status, err := accountingGit(ctx, root, "status", "--porcelain=v1", "--untracked-files=all", "--ignore-submodules=none")
		if err != nil || len(status) != 0 {
			return "", fmt.Errorf("accounting requires clean source: %s: %v", status, err)
		}
	}
	return head, nil
}

func absoluteAccountingInputs(root string, args []string) ([]string, error) {
	result := append([]string{}, args...)
	for i := 1; i < len(result); i++ {
		flag, value, inline := strings.Cut(result[i], "=")
		if flag != "--plan" && flag != "--receipt" && flag != "-plan" && flag != "-receipt" {
			continue
		}
		if !inline {
			i++
			if i == len(result) {
				return nil, fmt.Errorf("missing accounting input path")
			}
			value = result[i]
		}
		if value == "" {
			return nil, fmt.Errorf("empty accounting input path")
		}
		if !filepath.IsAbs(value) {
			value = filepath.Join(root, value)
		}
		if inline {
			result[i] = flag + "=" + value
		} else {
			result[i] = value
		}
	}
	return result, nil
}

func newAccountingEvidence(root string) (string, error) {
	directory := filepath.Join(root, "test-results")
	if err := os.MkdirAll(directory, 0700); err != nil {
		return "", err
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("accounting evidence requires an owned regular directory: %v", err)
	}
	return os.MkdirTemp(directory, "accounting-")
}

func saveAccountingManifest(path string, manifest accountingProofManifest) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	return errors.Join(writeAccountingJSON(file, manifest), file.Close())
}

func retainAccountingPreflight(root, relative, evidence string) (string, error) {
	if !filepath.IsLocal(relative) || filepath.Base(relative) != "receipt.json" {
		return "", fmt.Errorf("nonlocal canonical preflight evidence")
	}
	source := filepath.Dir(filepath.Join(root, relative))
	target := filepath.Join(evidence, "preflight", filepath.Base(source))
	err := filepath.WalkDir(source, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		destination := filepath.Join(target, rel)
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.IsDir() {
			return os.MkdirAll(destination, 0700)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("nonregular preflight evidence %s", rel)
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		out, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		_, writeErr := out.Write(body)
		return errors.Join(writeErr, out.Close())
	})
	if err != nil {
		return "", err
	}
	return filepath.Join(target, "receipt.json"), nil
}

func validateBoundAccountingPreflight(path, head, base string) error {
	if !filepath.IsAbs(path) || filepath.Base(path) != "receipt.json" {
		return fmt.Errorf("accounting preflight requires its retained absolute path")
	}
	var receipt preflightReceipt
	if err := readAccountingJSON(path, &receipt); err != nil {
		return err
	}
	if receipt.Plan.HeadSHA != head || receipt.Plan.BaseSHA != base {
		return fmt.Errorf("accounting preflight head/base binding changed")
	}
	return validatePreflightStatusReceipt(filepath.Dir(path), receipt)
}

func (snapshot accountingProofSnapshot) Close() error {
	if snapshot.Root == "" {
		return nil
	}
	permissionErr := filepath.WalkDir(snapshot.Root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return os.Chmod(path, 0700)
		}
		return nil
	})
	return errors.Join(permissionErr, os.RemoveAll(snapshot.Root))
}
