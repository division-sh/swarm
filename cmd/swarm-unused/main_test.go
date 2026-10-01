package main

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
)

func writeFixtureFile(t *testing.T, root, name, content string) {
	t.Helper()
	path := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
}

func fixtureRepo(t *testing.T, additions map[string]string) string {
	t.Helper()
	root := t.TempDir()
	files := map[string]string{
		"go.mod":     "module example.com/fixture\n\ngo 1.25.0\n",
		".gitignore": "test-results/\n",
		"fixture.go": `package fixture
type service interface { value() int }
type implementation struct{}
func (implementation) value() int { return 1 }
func Exported() int { var s service = implementation{}; return s.value() }
func taggedOnly() int { return 2 }
func raceOnly() int { return 3 }
`,
		"tag.go":  "//go:build issue2413\n\npackage fixture\nfunc Tagged() int { return taggedOnly() }\n",
		"race.go": "//go:build race\n\npackage fixture\nfunc Raced() int { return raceOnly() }\n",
		"fixture_test.go": `package fixture
import "testing"
func TestExported(t *testing.T) { if Exported() != 1 { t.Fatal("interface method") } }
`,
		// This deliberately cannot load, proving the named parked boundary is explicit.
		"parked.go": "//go:build issue2438\n\npackage fixture\nvar Broken = undefinedParkedSymbol\n",
	}
	for name, content := range additions {
		files[name] = content
	}
	for name, content := range files {
		writeFixtureFile(t, root, name, content)
	}
	for _, args := range [][]string{
		{"init", "--quiet"}, {"add", "."},
		{"-c", "user.name=Guard Fixture", "-c", "user.email=fixture@example.com", "commit", "--quiet", "--no-gpg-sign", "-m", "fixture"},
	} {
		if _, err := gitOutput(root, args...); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func TestCLIRejectsInvalidModes(t *testing.T) {
	for _, args := range [][]string{
		{"-collect"}, {"-merge"}, {"-collect", "-merge"}, {"-out", "x"}, {"-in", "x"},
		{"-collect", "-out", "x", "-in", "y"}, {"-merge", "-in", "x", "-out", "y"}, {"./..."}, {"-tests=false"},
	} {
		if err := run(args, io.Discard, io.Discard); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
}

func TestPinnedInvocationPolicy(t *testing.T) {
	want := configuration{
		Tool: "honnef.co/go/tools/cmd/staticcheck@v0.8.1", Toolchain: "go1.26.8", Checks: "U1000",
		Matrix: "default:\nrace: -race\nissue2413: -tags=issue2413\n", Packages: "./...", TargetGo: "module",
		Tests: true, ShowIgnored: true, GOWORK: "off", GOENV: "off", U1000Directives: "reject",
	}
	if pinnedConfig() != want {
		t.Fatal("guard policy changed: pins, tests, supported variants and no exclusions are required")
	}
	g := guard{root: t.TempDir()}
	c := g.staticcheck("-matrix", "-f=binary", "./...")
	if !slices.Equal(c.Args, []string{"go", "run", want.Tool, "-checks=U1000", "-tests=true", "-go=module", "-show-ignored", "-matrix", "-f=binary", "./..."}) {
		t.Fatalf("analysis invocation differs from evidence policy: %v", c.Args)
	}
	for _, pair := range [][2]string{{"GOTOOLCHAIN", "auto"}, {"GOFLAGS", "-tags=issue2438"}, {"GOENV", "/tmp/unapproved-goenv"}, {"GOWORK", "/tmp/unapproved-workspace"}, {"GOROOT", "/tmp/unapproved-toolchain"}} {
		t.Setenv(pair[0], pair[1])
	}
	env := map[string]string{}
	for _, value := range analysisEnv() {
		key, value, _ := strings.Cut(value, "=")
		env[key] = value
	}
	if env["GOTOOLCHAIN"] != want.Toolchain || env["GOFLAGS"] != "" || env["GOENV"] != "off" || env["GOWORK"] != "off" || env["GOROOT"] != "" {
		t.Fatalf("inherited environment diluted pinned policy: %v", env)
	}
}

func TestCollectionRejectsConcurrentSourceMutation(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("guard requires native Linux or Darwin")
	}
	root := fixtureRepo(t, nil)
	dir := filepath.Join(root, "test-results", "unused", runtime.GOOS)
	// The binary is opened only after cleanHead, so its creation marks the
	// collection window without adding test hooks to the actual guard.
	mutated := make(chan error, 1)
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				mutated <- nil
				return
			case <-ticker.C:
				if _, err := os.Stat(filepath.Join(dir, resultFile)); err != nil {
					continue
				}
				path := filepath.Join(root, "mutation.pending")
				if err := os.WriteFile(path, []byte("package fixture\nfunc Mutation() {}\n"), 0644); err != nil {
					mutated <- err
					return
				}
				mutated <- os.Rename(path, filepath.Join(root, "mutation.go"))
				return
			}
		}
	}()
	var stdout, stderr bytes.Buffer
	g := guard{root: root, stdout: &stdout, stderr: &stderr}
	err := g.collect(dir)
	close(stop)
	if mutationErr := <-mutated; mutationErr != nil {
		t.Fatal(mutationErr)
	}
	if err == nil || !strings.Contains(err.Error(), "source changed during collection") {
		t.Fatalf("concurrent mutation not rejected: %v %s", err, &stderr)
	}
	if _, err := os.Stat(filepath.Join(dir, metaFile)); !os.IsNotExist(err) {
		t.Fatal("source mutation left success evidence")
	}
}

func TestPinnedGuardRejectsU1000Suppression(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("guard requires native Linux or Darwin")
	}
	for _, directive := range []string{"ignore", "file-ignore"} {
		t.Run(directive, func(t *testing.T) {
			root := fixtureRepo(t, map[string]string{
				"suppressed.go": "package fixture\n//lint:" + directive + " U1000 suppression resistance fixture\nfunc unusedSuppressed() {}\n",
			})
			t.Chdir(root)
			var stdout, stderr bytes.Buffer
			g := guard{root: root, stdout: &stdout, stderr: &stderr}
			// Show the upstream limitation with the real pin, not a mocked diagnostic.
			c := g.staticcheck("-matrix", "-f=json", "./...")
			c.Stdin = strings.NewReader(buildMatrix)
			c.Stderr = &stderr
			b, err := c.Output()
			if err != nil || len(b) != 0 {
				t.Fatalf("expected upstream suppression to hide unused even with -show-ignored: %v %s %s", err, b, &stderr)
			}
			for _, args := range [][]string{nil, {"-collect", "-out", filepath.Join(root, "test-results", "unused", runtime.GOOS)}} {
				if err := run(args, &stdout, &stderr); err == nil || !strings.Contains(err.Error(), "U1000 suppression forbidden at suppressed.go:2:") {
					t.Fatalf("actual guard accepted %s suppression: %v", directive, err)
				}
			}
		})
	}
}

func TestPinnedBinaryUnionAcrossCheckoutRoots(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("guard requires native Linux or Darwin")
	}
	root := fixtureRepo(t, nil)
	other := filepath.Join(t.TempDir(), "other-checkout")
	if _, err := gitOutput(root, "clone", "--quiet", root, other); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	g := guard{root: root, stdout: &stdout, stderr: &stderr}
	otherGuard := guard{root: other, stdout: &stdout, stderr: &stderr}
	matrixPath, defaultPath := filepath.Join(t.TempDir(), "matrix.bin"), filepath.Join(t.TempDir(), "default.bin")
	if _, err := g.analyze(matrixPath); err != nil {
		t.Fatalf("matrix: %v %s", err, &stderr)
	}
	f, err := os.Create(defaultPath)
	if err != nil {
		t.Fatal(err)
	}
	// Both executions are native on this host. The deliberately default-only
	// second binary is a path-identity probe, NOT a required platform receipt.
	c := otherGuard.staticcheck("-f=binary", "./...")
	c.Stdout, c.Stderr = f, &stderr
	runErr, closeErr := c.Run(), f.Close()
	if runErr != nil || closeErr != nil {
		t.Fatalf("default binary: %v %v %s", runErr, closeErr, &stderr)
	}
	if err := g.validateDiagnostics(defaultPath); err != nil {
		t.Fatal(err)
	}
	if err := g.decide([]string{defaultPath}); err == nil || !strings.Contains(stdout.String(), "taggedOnly") || !strings.Contains(stdout.String(), "raceOnly") {
		t.Fatalf("default-only probe must have U1000: %v %s", err, &stdout)
	}
	stdout.Reset()
	if err := g.decide([]string{matrixPath, defaultPath}); err != nil || stdout.Len() != 0 {
		t.Fatalf("upstream binary paths did not union across checkout roots: %v %s %s", err, &stdout, &stderr)
	}
}

func TestPinnedGuardRealFixtures(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("guard requires native Linux or Darwin")
	}
	// Parent settings must not float the analysis toolchain or introduce extra tags.
	t.Setenv("GOTOOLCHAIN", "auto")
	t.Setenv("GOFLAGS", "-tags=issue2438")
	t.Setenv("GOWORK", "off")
	for _, tc := range []struct {
		name, file, source, finding string
	}{
		{name: "tag-race-interface-export-positive"},
		{name: "unused-production", file: "dead.go", source: "package fixture\nfunc unusedProduction() {}\n", finding: "unusedProduction"},
		{name: "unused-test", file: "dead_test.go", source: "package fixture\nfunc unusedTestDeclaration() {}\n", finding: "unusedTestDeclaration"},
		{name: "tagged-load-failure", file: "broken.go", source: "//go:build issue2413\n\npackage fixture\nvar Broken = undefinedSymbol\n", finding: "undefinedSymbol"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			additions := map[string]string{}
			if tc.file != "" {
				additions[tc.file] = tc.source
			}
			root := fixtureRepo(t, additions)
			t.Chdir(root)
			var stdout, stderr bytes.Buffer
			err := run(nil, &stdout, &stderr)
			if tc.finding == "" {
				if err != nil {
					t.Fatalf("positive guard: %v\n%s\n%s", err, &stdout, &stderr)
				}
			} else if err == nil || !strings.Contains(stdout.String()+stderr.String()+err.Error(), tc.finding) {
				t.Fatalf("negative guard should identify %s: %v\n%s\n%s", tc.finding, err, &stdout, &stderr)
			}
			if !strings.Contains(stderr.String(), "NOT Linux/Darwin union") {
				t.Fatal("local command did not disclose coverage limit")
			}
			stdout.Reset()
			stderr.Reset()
			dir := filepath.Join(root, "test-results", "unused", runtime.GOOS)
			err = run([]string{"-collect", "-out", dir}, &stdout, &stderr)
			if tc.name == "tagged-load-failure" {
				if err == nil || !strings.Contains(err.Error()+stderr.String(), "undefinedSymbol") {
					t.Fatalf("collector accepted load failure: %v %s", err, &stderr)
				}
				if _, err := os.Stat(filepath.Join(dir, metaFile)); !os.IsNotExist(err) {
					t.Fatal("collector wrote success for a load error")
				}
				return
			}
			if err != nil {
				t.Fatalf("collection must allow platform U1000: %v\n%s", err, &stderr)
			}
			head, err := cleanHead(root, filepath.Join(dir, resultFile), filepath.Join(dir, metaFile))
			if err != nil {
				t.Fatal(err)
			}
			path, err := readEvidence(dir, head, runtime.GOOS)
			if err != nil {
				t.Fatal(err)
			}
			g := guard{root: root, stdout: &stdout, stderr: &stderr}
			stdout.Reset()
			err = g.decide([]string{path})
			if (err == nil) != (tc.finding == "") {
				t.Fatalf("actual upstream decision: %v %s", err, &stdout)
			}
			if tc.finding != "" && (!strings.Contains(stdout.String(), tc.finding) || !strings.Contains(stdout.String(), tc.file+":2:") || !strings.Contains(stdout.String(), "U1000")) {
				t.Fatalf("missing precise unused diagnostic: %s", &stdout)
			}
			if tc.finding != "" {
				if _, err := gitOutput(root, "rm", "--", tc.file); err != nil {
					t.Fatal(err)
				}
				if _, err := gitOutput(root, "-c", "user.name=Guard Fixture", "-c", "user.email=fixture@example.com", "commit", "--quiet", "--no-gpg-sign", "-m", "remove unused injection"); err != nil {
					t.Fatal(err)
				}
				newHead, err := gitOutput(root, "rev-parse", "HEAD")
				if err != nil {
					t.Fatal(err)
				}
				if _, err := readEvidence(dir, newHead, runtime.GOOS); err == nil || !strings.Contains(err.Error(), "stale evidence") {
					t.Fatalf("accepted previous-head negative evidence: %v", err)
				}
				stdout.Reset()
				stderr.Reset()
				if err := run(nil, &stdout, &stderr); err != nil || stdout.Len() != 0 {
					t.Fatalf("actual guard must pass after removing %s: %v %s %s", tc.finding, err, &stdout, &stderr)
				}
			}
			if tc.finding == "" {
				// Default alone is insufficient; both variant-only callers matter.
				c := g.staticcheck("-f=json", "./...")
				c.Stderr = &stderr
				b, err := c.Output()
				if err == nil || !bytes.Contains(b, []byte("taggedOnly")) || !bytes.Contains(b, []byte("raceOnly")) {
					t.Fatalf("fixture did not prove matrix union: %v %s", err, b)
				}
				if err := g.merge(filepath.Dir(dir)); err == nil {
					t.Fatal("merge accepted missing second platform")
				}
				// Even a hash-valid nonempty corrupt binary must fail upstream decoding.
				writeFixtureFile(t, dir, resultFile, "not a staticcheck binary")
				b, err = os.ReadFile(filepath.Join(dir, metaFile))
				if err != nil {
					t.Fatal(err)
				}
				var e evidence
				if err := json.Unmarshal(b, &e); err != nil {
					t.Fatal(err)
				}
				e.ResultSHA256, e.ResultBytes, err = hashResult(path)
				if err != nil {
					t.Fatal(err)
				}
				writeMetadata(t, dir, e)
				if err := g.validateDiagnostics(path); err == nil {
					t.Fatal("accepted hash-valid corrupt upstream binary")
				}
			}
		})
	}
}
