package store_test

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"sync"
	"testing"

	"golang.org/x/tools/go/packages"
)

var sharedDebtHeadCensus debtHeadCensusMemo

type debtHeadCensusMemo struct {
	sync.Mutex
	root, inputs string
	findings     []authorityFinding
}

func debtLoadHeadCensus(t *testing.T, root string) []authorityFinding {
	t.Helper()
	return sharedDebtHeadCensus.load(t, root, debtLoadPersistenceAuthorityFindings)
}

func (memo *debtHeadCensusMemo) load(t *testing.T, root string, collect func(*testing.T, string) []authorityFinding) []authorityFinding {
	t.Helper()
	memo.Lock()
	defer memo.Unlock()
	inputs, ok := debtHeadInputDigest(t, root)
	if ok && memo.root == root && memo.inputs == inputs {
		return slices.Clone(memo.findings)
	}
	findings := collect(t, root)
	after, unchanged := debtHeadInputDigest(t, root)
	if ok && unchanged && inputs == after {
		memo.root, memo.inputs, memo.findings = root, inputs, slices.Clone(findings)
	} else {
		memo.root, memo.inputs, memo.findings = "", "", nil
	}
	return findings
}

func debtHeadInputDigest(t *testing.T, root string) (string, bool) {
	t.Helper()
	sources := debtOwnedAuthoritySources(t, root)
	_, patterns := debtAuthorityPackagePatterns(t, root, sources)
	env := debtCensusEnvironment()
	pkgs, err := packages.Load(&packages.Config{
		Dir: root, Env: env, BuildFlags: []string{"-trimpath"}, Tests: true,
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedImports |
			packages.NeedDeps | packages.NeedModule | packages.NeedEmbedFiles,
	}, patterns...)
	if err != nil {
		return "", false
	}
	// Metadata enumeration does not type-check. Hash every owned Go file,
	// including ignored/inactive source, and the complete dependency/build inputs.
	files := map[string]bool{filepath.Join(root, "go.mod"): true}
	if _, err := os.Stat(filepath.Join(root, "go.sum")); err == nil {
		files[filepath.Join(root, "go.sum")] = true
	}
	for path := range sources {
		files[path] = true
	}
	visited := map[string]bool{}
	valid := true
	var visit func(*packages.Package)
	visit = func(pkg *packages.Package) {
		if pkg == nil || visited[pkg.ID] {
			return
		}
		visited[pkg.ID] = true
		if len(pkg.Errors) != 0 {
			valid = false
		}
		for _, list := range [][]string{pkg.GoFiles, pkg.OtherFiles, pkg.IgnoredFiles, pkg.EmbedFiles} {
			for _, path := range list {
				files[path] = true
			}
		}
		if pkg.Module != nil {
			if pkg.Module.GoMod != "" {
				files[pkg.Module.GoMod] = true
			}
			if pkg.Module.Replace != nil && pkg.Module.Replace.GoMod != "" {
				files[pkg.Module.Replace.GoMod] = true
			}
		}
		for _, imported := range pkg.Imports {
			visit(imported)
		}
	}
	for _, pkg := range pkgs {
		visit(pkg)
	}
	if !valid {
		return "", false // Preserve the original collector's fail-closed diagnostic.
	}
	// GOGCCFLAGS contains a per-command temporary path, not a source-setting
	// change. Bind the effective settings, not that generated compiler argument.
	command := exec.Command("go", "env", "-json", "GOVERSION", "GOROOT", "GOTOOLCHAIN", "GOOS", "GOARCH", "GOAMD64", "GOEXPERIMENT", "CGO_ENABLED", "CC", "CXX", "CGO_CFLAGS", "CGO_CPPFLAGS", "CGO_CXXFLAGS", "CGO_FFLAGS", "CGO_LDFLAGS", "GOFLAGS", "GOWORK", "GOMOD", "GOTOOLDIR")
	command.Dir, command.Env = root, env
	context, err := command.Output()
	if err != nil {
		return "", false
	}
	paths := make([]string, 0, len(files))
	for path := range files {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	hash := sha256.New()
	fmt.Fprintf(hash, "%s\x00%s\x00", root, runtime.Version())
	for _, value := range env {
		fmt.Fprintf(hash, "%s\x00", value)
	}
	hash.Write(context)
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return "", false
		}
		fmt.Fprintf(hash, "%s\x00%d\x00", path, len(data))
		hash.Write(data)
	}
	return hex.EncodeToString(hash.Sum(nil)), true
}
