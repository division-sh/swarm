package main

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/testplanning"
	"github.com/division-sh/swarm/scripts/rewrite-stages-2566/formatproof"
)

const accountingPolicyPath = ".github/accounting-policy.json"
const accountingRepository = "division-sh/swarm"

var accountingVariants = []string{"linux/amd64/default", "linux/amd64/race", "linux/amd64/issue2413", "darwin/arm64/default", "darwin/arm64/race", "darwin/arm64/issue2413"}

type accountingInput struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

type accountingPolicy struct {
	Version          int               `json:"version"`
	Category         string            `json:"category"`
	Status           string            `json:"status"`
	Rule             string            `json:"rule"`
	ExecutorSource   string            `json:"executor_source"`
	DependencyDigest string            `json:"dependency_digest"`
	Inputs           []accountingInput `json:"inputs"`
	Variants         []string          `json:"variants"`
}

type accountingPolicyObservation struct {
	Commit string           `json:"commit"`
	Blob   string           `json:"blob"`
	Digest string           `json:"digest"`
	Policy accountingPolicy `json:"policy"`
}

func hexIdentity(value string, n int) bool {
	return len(value) == n && strings.Trim(value, "0123456789abcdef") == ""
}

func (p accountingPolicy) validate() error {
	if p.Version != 1 || p.Category != "finite-go-format" || p.Rule != formatproof.Rule || !reflect.DeepEqual(p.Variants, accountingVariants) {
		return fmt.Errorf("unknown accounting policy, rule or context")
	}
	if p.Status != "active" {
		return fmt.Errorf("accounting category is %q, not active", p.Status)
	}
	if !hexIdentity(p.ExecutorSource, 40) || !hexIdentity(p.DependencyDigest, 64) || len(p.Inputs) == 0 {
		return fmt.Errorf("accounting executor has no reviewed dependency closure")
	}
	last := ""
	for _, input := range p.Inputs {
		if !filepath.IsLocal(input.Path) || filepath.ToSlash(filepath.Clean(input.Path)) != input.Path || input.Path <= last || !hexIdentity(input.SHA256, 64) {
			return fmt.Errorf("invalid, duplicate or unordered accounting dependency")
		}
		last = input.Path
	}
	raw, _ := json.Marshal(p.Inputs)
	if formatproof.Digest(raw) != p.DependencyDigest {
		return fmt.Errorf("accounting dependency manifest digest changed")
	}
	return nil
}

type accountingRemote func(context.Context, string) ([]byte, error)

func accountingGitHub(ctx context.Context, path string) ([]byte, error) {
	return exec.CommandContext(ctx, "gh", "api", "--hostname", "github.com", "repos/"+accountingRepository+"/"+path).Output()
}

// The local comparison ref is deliberately not the approval/revocation owner.
func observeAccountingPolicy(ctx context.Context, remote accountingRemote) (accountingPolicyObservation, error) {
	var result accountingPolicyObservation
	raw, err := remote(ctx, "git/ref/heads/master")
	if err != nil {
		return result, fmt.Errorf("fresh accounting policy unavailable: %w", err)
	}
	var ref struct {
		Ref    string `json:"ref"`
		Object struct {
			SHA  string `json:"sha"`
			Type string `json:"type"`
		} `json:"object"`
	}
	if err = json.Unmarshal(raw, &ref); err != nil || ref.Ref != "refs/heads/master" || ref.Object.Type != "commit" || !hexIdentity(ref.Object.SHA, 40) {
		return result, fmt.Errorf("invalid protected-master accounting identity")
	}
	raw, err = remote(ctx, "contents/"+accountingPolicyPath+"?ref="+ref.Object.SHA)
	if err != nil {
		return result, fmt.Errorf("accounting policy at protected ref unavailable: %w", err)
	}
	var blob struct{ Type, Path, SHA, Encoding, Content string }
	if err = json.Unmarshal(raw, &blob); err != nil || blob.Type != "file" || blob.Path != accountingPolicyPath || blob.Encoding != "base64" || !hexIdentity(blob.SHA, 40) {
		return result, fmt.Errorf("invalid accounting policy blob")
	}
	body, err := base64.StdEncoding.DecodeString(strings.ReplaceAll(blob.Content, "\n", ""))
	if err != nil {
		return result, err
	}
	object := append([]byte(fmt.Sprintf("blob %d\x00", len(body))), body...)
	if fmt.Sprintf("%x", sha1.Sum(object)) != blob.SHA {
		return result, fmt.Errorf("accounting policy blob identity mismatch")
	}
	if err = formatproof.Decode(body, &result.Policy); err != nil {
		return result, err
	}
	if err = result.Policy.validate(); err != nil {
		return result, err
	}
	result.Commit, result.Blob, result.Digest = ref.Object.SHA, blob.SHA, formatproof.Digest(body)
	return result, nil
}

func requireAccountingPolicy(ctx context.Context, remote accountingRemote, expected accountingPolicyObservation) error {
	current, err := observeAccountingPolicy(ctx, remote)
	if err != nil {
		return err
	}
	// Unrelated master advances are fine; rule/status changes are not.
	if current.Digest != expected.Digest || current.Blob != expected.Blob || !reflect.DeepEqual(current.Policy, expected.Policy) {
		return fmt.Errorf("accounting approval/revocation changed during operation")
	}
	return nil
}

func accountingGit(ctx context.Context, root string, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, "git", append([]string{"-C", root, "--no-optional-locks", "--literal-pathspecs", "--no-replace-objects"}, args...)...)
	return command.Output()
}

func fetchAccountingExecutor(ctx context.Context, root string, p accountingPolicy, remote accountingRemote) error {
	if err := p.validate(); err != nil {
		return err
	}
	// The fixed repository, not a candidate-controlled origin, supplies the object.
	if _, err := accountingGit(ctx, root, "cat-file", "-e", p.ExecutorSource+"^{commit}"); err != nil {
		if _, err = accountingGit(ctx, root, "fetch", "--no-tags", "--depth=1", "https://github.com/"+accountingRepository+".git", p.ExecutorSource); err != nil {
			return fmt.Errorf("approved accounting executor unavailable: %w", err)
		}
	}
	raw, err := remote(ctx, "commits/"+p.ExecutorSource)
	if err != nil {
		return err
	}
	var commit struct {
		SHA string `json:"sha"`
	}
	if err = json.Unmarshal(raw, &commit); err != nil || commit.SHA != p.ExecutorSource {
		return fmt.Errorf("approved executor is not the authenticated repository object")
	}
	return nil
}

// Archive only immutable Git data. A fresh private directory cannot inherit a
// candidate worktree's imports, hooks, go.work, init code or cached policy.
func archiveAccountingExecutor(ctx context.Context, root, source string) (string, error) {
	if !hexIdentity(source, 40) {
		return "", fmt.Errorf("invalid executor source")
	}
	body, err := accountingGit(ctx, root, "archive", "--format=tar", source)
	if err != nil {
		return "", err
	}
	directory, err := os.MkdirTemp("", "swarm-accounting-executor-")
	if err != nil {
		return "", err
	}
	failed := true
	defer func() {
		if failed {
			_ = os.RemoveAll(directory)
		}
	}()
	archive := tar.NewReader(bytes.NewReader(body))
	headerSeen := false
	for {
		header, err := archive.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
		if header.Typeflag == tar.TypeXGlobalHeader {
			if headerSeen || header.Name != "pax_global_header" || len(header.PAXRecords) != 1 || header.PAXRecords["comment"] != source {
				return "", fmt.Errorf("executor archive lacks its exact Git commit header")
			}
			headerSeen = true
			continue
		}
		if !headerSeen {
			return "", fmt.Errorf("executor archive precedes its Git commit header")
		}
		if err = writeAccountingArchiveEntry(directory, archive, header); err != nil {
			return "", err
		}
	}
	if !headerSeen {
		return "", fmt.Errorf("executor archive has no Git commit identity")
	}
	failed = false
	return directory, nil
}

func writeAccountingArchiveEntry(directory string, archive *tar.Reader, header *tar.Header) error {
	name := strings.TrimSuffix(header.Name, "/")
	if !filepath.IsLocal(name) || filepath.ToSlash(filepath.Clean(name)) != name {
		return fmt.Errorf("nonlocal executor archive entry")
	}
	path := filepath.Join(directory, name)
	switch header.Typeflag {
	case tar.TypeDir:
		return os.MkdirAll(path, 0700)
	case tar.TypeReg:
		if header.Size < 0 || header.Size > 64<<20 {
			return fmt.Errorf("unsupported executor archive size")
		}
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			return err
		}
		mode := os.FileMode(0600)
		if header.Mode&0100 != 0 {
			mode |= 0100
		}
		output, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
		if err != nil {
			return err
		}
		_, copyErr := io.CopyN(output, archive, header.Size)
		return errors.Join(copyErr, output.Close())
	default:
		return fmt.Errorf("unsupported executor archive member %s", name)
	}
}

func accountingBuildEnv(goos, goarch string) []string {
	var result []string
	for _, entry := range os.Environ() {
		key := strings.SplitN(entry, "=", 2)[0]
		if strings.HasPrefix(key, "GO") || strings.HasPrefix(key, "CGO") || strings.HasPrefix(key, "SWARM_UPDATE") || strings.HasPrefix(key, "SWARM_REFRESH") || strings.HasPrefix(key, "SWARM_DEBT_CACHE") || key == "CC" || key == "CXX" {
			continue
		}
		result = append(result, entry)
	}
	return append(result, "GOENV=off", "GOTOOLCHAIN=go1.26.8", "GOWORK=off", "GOFLAGS=", "GOOS="+goos, "GOARCH="+goarch, "CGO_ENABLED=1")
}

func accountingClosure(ctx context.Context, root string) ([]accountingInput, error) {
	if (runtime.GOOS != "linux" || runtime.GOARCH != "amd64") && (runtime.GOOS != "darwin" || runtime.GOARCH != "arm64") {
		return nil, fmt.Errorf("unsupported accounting host")
	}
	verify := execAccountingGo(ctx, root, "mod", "verify")
	if _, err := verify.Output(); err != nil {
		return nil, fmt.Errorf("approved external module bytes are not verified: %w", err)
	}
	paths := map[string]bool{"go.mod": true, "go.sum": true}
	packages := map[string]bool{"./cmd/swarm-test": true, "./scripts/rewrite-stages-2566/...": true, "./internal/testplanning": true, "./internal/testtiming": true, "./internal/store": true, "./cmd/swarm-openrpc-gen": true, "./cmd/swarm-complexity": true}
	structural, err := testplanning.StaticAuthorityGuardRoots(root)
	if err != nil {
		return nil, err
	}
	for _, identity := range structural {
		packages["./"+strings.TrimPrefix(identity.Package, "github.com/division-sh/swarm/")] = true
	}
	var selected []string
	for pkg := range packages {
		selected = append(selected, pkg)
	}
	sort.Strings(selected)
	for _, variant := range accountingVariants {
		names, err := accountingVariantInputs(ctx, root, variant, selected)
		if err != nil {
			return nil, err
		}
		for _, name := range names {
			paths[name] = true
		}
	}
	for _, path := range []string{".github/test-proof-plan.yaml", ".github/test-proof-history-inputs.json", ".github/scripts/prepare-proof-history.sh", testplanning.GeneratedWeightModelPath, ".github/flake-ledger.json", "internal/apiv1/testdata/public_surface_backend_matrix.yaml", formatproof.Ledger, "scripts/rewrite-stages-2566/intent.json", "scripts/rewrite-stages-2566/entries.json"} {
		paths[path] = true
	}
	paths["scripts/rewrite-stages-2566/formatproof/testdata/original.go.txt"] = true
	var inputs []accountingInput
	for path := range paths {
		info, err := os.Lstat(filepath.Join(root, path))
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("nonregular accounting dependency %s", path)
		}
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			return nil, err
		}
		inputs = append(inputs, accountingInput{path, formatproof.Digest(raw)})
	}
	sort.Slice(inputs, func(i, j int) bool { return inputs[i].Path < inputs[j].Path })
	return inputs, nil
}

func accountingVariantInputs(ctx context.Context, root, variant string, selected []string) ([]string, error) {
	parts := strings.Split(variant, "/")
	args := []string{"list", "-mod=readonly", "-deps", "-test", "-json"}
	if parts[2] == "race" {
		args = append(args, "-race")
	}
	if parts[2] == "issue2413" {
		args = append(args, "-tags=issue2413")
	}
	args = append(args, selected...)
	command := exec.CommandContext(ctx, "go", args...)
	command.Dir = root
	command.Env = accountingBuildEnv(parts[0], parts[1])
	raw, err := command.Output()
	if err != nil {
		return nil, fmt.Errorf("derive accounting closure %s: %w", variant, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	var paths []string
	for {
		var pkg struct {
			Dir                                                                                                       string
			Standard                                                                                                  bool
			GoFiles, CgoFiles, TestGoFiles, XTestGoFiles, EmbedFiles, TestEmbedFiles, XTestEmbedFiles, IgnoredGoFiles []string
			CFiles, CXXFiles, MFiles, HFiles, FFiles, SFiles, SwigFiles, SwigCXXFiles, SysoFiles                      []string
			Module                                                                                                    *struct{ Replace *struct{ Version string } }
		}
		if err = decoder.Decode(&pkg); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		if pkg.Module != nil && pkg.Module.Replace != nil && pkg.Module.Replace.Version == "" {
			return nil, fmt.Errorf("local module replacements are outside the approved executor closure")
		}
		var files []string
		for _, names := range [][]string{pkg.GoFiles, pkg.CgoFiles, pkg.TestGoFiles, pkg.XTestGoFiles, pkg.EmbedFiles, pkg.TestEmbedFiles, pkg.XTestEmbedFiles, pkg.IgnoredGoFiles, pkg.CFiles, pkg.CXXFiles, pkg.MFiles, pkg.HFiles, pkg.FFiles, pkg.SFiles, pkg.SwigFiles, pkg.SwigCXXFiles, pkg.SysoFiles} {
			files = append(files, names...)
		}
		names, err := accountingLocalInputs(root, pkg.Dir, pkg.Standard, files)
		if err != nil {
			return nil, err
		}
		paths = append(paths, names...)
	}
	return paths, nil
}

func accountingLocalInputs(root, directory string, standard bool, files []string) ([]string, error) {
	if standard {
		return nil, nil
	}
	rel, err := filepath.Rel(root, directory)
	if err != nil || !filepath.IsLocal(rel) {
		return nil, nil
	}
	var paths []string
	for _, name := range files {
		if filepath.IsAbs(name) {
			continue
		}
		path := filepath.ToSlash(filepath.Join(rel, name))
		if !filepath.IsLocal(path) {
			return nil, fmt.Errorf("nonlocal accounting dependency")
		}
		paths = append(paths, path)
	}
	return paths, nil
}
