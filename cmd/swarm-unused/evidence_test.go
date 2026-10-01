package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func validEvidence(platform string) evidence {
	return evidence{
		Schema: 1, State: "complete", SourceSHA: strings.Repeat("a", 40), Config: pinnedConfig(),
		Native:       native{GOOS: platform, GOHOSTOS: platform, GOARCH: "arm64", GOHOSTARCH: "arm64", CGO_ENABLED: "1", GOVERSION: toolchain},
		ResultSHA256: strings.Repeat("b", 64), ResultBytes: 100,
	}
}

func TestEvidenceRejectsPolicyDilution(t *testing.T) {
	mutations := map[string]func(*evidence){
		"schema":        func(e *evidence) { e.Schema++ },
		"missing":       func(e *evidence) { e.State = "" },
		"failed":        func(e *evidence) { e.State = "failed" },
		"skipped":       func(e *evidence) { e.State = "skipped" },
		"stale":         func(e *evidence) { e.SourceSHA = strings.Repeat("c", 40) },
		"tool":          func(e *evidence) { e.Config.Tool = "staticcheck@latest" },
		"toolchain":     func(e *evidence) { e.Config.Toolchain = "auto" },
		"checks":        func(e *evidence) { e.Config.Checks = "all" },
		"tests":         func(e *evidence) { e.Config.Tests = false },
		"matrix":        func(e *evidence) { e.Config.Matrix = "default:\n" },
		"extra-tag":     func(e *evidence) { e.Config.Matrix += "issue2438: -tags=issue2438\n" },
		"packages":      func(e *evidence) { e.Config.Packages = "./cmd/..." },
		"target":        func(e *evidence) { e.Config.TargetGo = "1.26" },
		"flags":         func(e *evidence) { e.Config.GOFLAGS = "-tags=issue2438" },
		"workspace":     func(e *evidence) { e.Config.GOWORK = "auto" },
		"goenv":         func(e *evidence) { e.Config.GOENV = "default" },
		"suppression":   func(e *evidence) { e.Config.ShowIgnored = false },
		"directives":    func(e *evidence) { e.Config.U1000Directives = "allow" },
		"wrong-os":      func(e *evidence) { e.Native.GOOS = "darwin" },
		"cross-os":      func(e *evidence) { e.Native.GOHOSTOS = "darwin" },
		"cross-arch":    func(e *evidence) { e.Native.GOHOSTARCH = "amd64" },
		"missing-arch":  func(e *evidence) { e.Native.GOARCH = "" },
		"cgo-disabled":  func(e *evidence) { e.Native.CGO_ENABLED = "0" },
		"execution-go":  func(e *evidence) { e.Native.GOVERSION = "go1.26.9" },
		"experiment":    func(e *evidence) { e.Native.GOEXPERIMENT = "greenteagc" },
		"empty":         func(e *evidence) { e.ResultBytes = 0 },
		"negative-size": func(e *evidence) { e.ResultBytes = -1 },
		"no-hash":       func(e *evidence) { e.ResultSHA256 = "" },
		"bad-hash":      func(e *evidence) { e.ResultSHA256 = strings.Repeat("z", 64) },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			e := validEvidence("linux")
			head := e.SourceSHA
			mutate(&e)
			if err := validateEvidence(e, head, "linux"); err == nil {
				t.Fatal("accepted invalid evidence")
			}
		})
	}
	for _, platform := range []string{"linux", "darwin"} {
		e := validEvidence(platform)
		if err := validateEvidence(e, e.SourceSHA, platform); err != nil {
			t.Fatal(err)
		}
	}
}

func writeMetadata(t *testing.T, dir string, e evidence) {
	t.Helper()
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	writeFixtureFile(t, dir, metaFile, string(b))
}

func TestReadEvidenceFailsClosed(t *testing.T) {
	for _, failure := range []string{"missing-metadata", "bad-json", "unknown-field", "trailing", "missing-result", "empty-result", "hash", "size"} {
		t.Run(failure, func(t *testing.T) {
			dir := t.TempDir()
			e := validEvidence("linux")
			writeFixtureFile(t, dir, resultFile, "binary fixture")
			var err error
			e.ResultSHA256, e.ResultBytes, err = hashResult(filepath.Join(dir, resultFile))
			if err != nil {
				t.Fatal(err)
			}
			writeMetadata(t, dir, e)
			switch failure {
			case "missing-metadata":
				err = os.Remove(filepath.Join(dir, metaFile))
			case "bad-json":
				writeFixtureFile(t, dir, metaFile, "{")
			case "unknown-field":
				b, _ := json.Marshal(e)
				writeFixtureFile(t, dir, metaFile, strings.TrimSuffix(string(b), "}")+`,"extra":true}`)
			case "trailing":
				b, _ := json.Marshal(e)
				writeFixtureFile(t, dir, metaFile, string(b)+" {}")
			case "missing-result":
				err = os.Remove(filepath.Join(dir, resultFile))
			case "empty-result":
				writeFixtureFile(t, dir, resultFile, "")
			case "hash":
				writeFixtureFile(t, dir, resultFile, "changed binary")
			case "size":
				e.ResultBytes++
				writeMetadata(t, dir, e)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := readEvidence(dir, e.SourceSHA, "linux"); err == nil {
				t.Fatal("accepted incomplete/corrupt evidence")
			}
		})
	}
}

func TestExactSourceRejectionAndSuccessInvalidation(t *testing.T) {
	root := fixtureRepo(t, nil)
	head, err := cleanHead(root)
	if err != nil || head == "" {
		t.Fatalf("clean source: %s %v", head, err)
	}
	writeFixtureFile(t, root, "extra.go", "package fixture\n")
	if _, err := cleanHead(root); err == nil {
		t.Fatal("accepted untracked source")
	}
	if err := os.Remove(filepath.Join(root, "extra.go")); err != nil {
		t.Fatal(err)
	}
	writeFixtureFile(t, root, "fixture.go", "package fixture\n")
	if _, err := cleanHead(root); err == nil {
		t.Fatal("accepted tracked source change")
	}
	dir := filepath.Join(root, "test-results", "unused", "linux")
	writeMetadata(t, dir, validEvidence("linux"))
	g := guard{root: root}
	if err := g.collect(dir); err == nil {
		t.Fatal("collected dirty source")
	}
	if _, err := os.Stat(filepath.Join(dir, metaFile)); !os.IsNotExist(err) {
		t.Fatalf("left previous success metadata: %v", err)
	}
}

func TestExactSourceRejectsAllUntrackedAndIgnoredInputs(t *testing.T) {
	root := fixtureRepo(t, map[string]string{".gitignore": "ignored/\ntest-results/\n"})
	for _, name := range []string{"extra.c", "extra.h", "extra.s", "embedded.txt", "ignored/extra.go", "ignored/extra.c", "ignored/staticcheck.conf", "test-results/unused/linux/extra.go"} {
		t.Run(name, func(t *testing.T) {
			writeFixtureFile(t, root, name, "untracked input")
			if _, err := cleanHead(root); err == nil || !strings.Contains(err.Error(), "untracked file") {
				t.Fatalf("accepted untracked/ignored analysis input %s: %v", name, err)
			}
			if err := os.Remove(filepath.Join(root, name)); err != nil {
				t.Fatal(err)
			}
		})
	}
	dir := filepath.Join(root, "test-results", "unused", "linux")
	result, metadata := filepath.Join(dir, resultFile), filepath.Join(dir, metaFile)
	writeFixtureFile(t, dir, resultFile, "result")
	writeMetadata(t, dir, validEvidence("linux"))
	if _, err := cleanHead(root); err == nil {
		t.Fatal("ignored output requires explicit evidence file exceptions")
	}
	if _, err := cleanHead(root, result, metadata); err != nil {
		t.Fatalf("exact result filenames should be permitted: %v", err)
	}
	writeFixtureFile(t, dir, "embedded.txt", "untracked input")
	if _, err := cleanHead(root, result, metadata); err == nil {
		t.Fatal("entire output directory became an input bypass")
	}
}
