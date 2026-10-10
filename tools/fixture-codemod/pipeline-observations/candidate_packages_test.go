package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func candidateModuleForTest(t *testing.T) (string, func(string, string) pendingFile) {
	t.Helper()
	t.Setenv("GOWORK", "off")
	t.Setenv("GOFLAGS", "")
	root := t.TempDir()
	put := func(path, source string) pendingFile {
		t.Helper()
		path = filepath.Join(root, path)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(source), 0600); err != nil {
			t.Fatal(err)
		}
		return pendingFile{path: path, data: []byte(source)}
	}
	put("go.mod", "module example.test/preflight\n\ngo 1.25.0\n")
	// Keep the old load patterns valid so missing candidate coverage cannot
	// masquerade as a refusal caused by a nonexistent package.
	for _, path := range []string{"internal/runtime", "internal/runtime/pipeline", "internal/runtime/tools", "internal/apiv1"} {
		put(path+"/source.go", "package "+filepath.Base(path)+"\n")
	}
	return root, put
}

func requireCandidateRefusalBeforeWrite(t *testing.T, root string, files []pendingFile) {
	t.Helper()
	original := make(map[string][]byte)
	for _, file := range files {
		data, err := os.ReadFile(file.path)
		if err != nil {
			t.Fatal(err)
		}
		original[file.path] = data
	}
	if err := applyPendingFiles(root, files, true); err == nil || !strings.Contains(err.Error(), "candidate type checking failed") {
		t.Errorf("invalid or unrepresented candidate passed preflight: %v", err)
	}
	for path, before := range original {
		after, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(before, after) {
			t.Errorf("failed preflight changed candidate %s: %v", path, err)
		}
	}
}

func TestCandidatePreflightRejectsOmittedInternalAndExternalTestsBeforeWrite(t *testing.T) {
	for _, path := range []string{"internal/serveapp", "internal/runtime/bus", "internal/store/internal/runtimepersistence"} {
		for _, external := range []bool{false, true} {
			name := "internal"
			if external {
				name = "external"
			}
			t.Run(path+"/"+name, func(t *testing.T) {
				root, put := candidateModuleForTest(t)
				packageName := filepath.Base(path)
				put(path+"/source.go", "package "+packageName+"\n")
				if external {
					packageName += "_test"
				}
				file := put(path+"/candidate_test.go", "package "+packageName+"\nvar candidateValue int = 1\n")
				file.data = []byte("package " + packageName + "\nvar candidateValue int = missingCandidateSymbol\n")
				requireCandidateRefusalBeforeWrite(t, root, []pendingFile{file})
			})
		}
	}
}

func TestCandidatePreflightRejectsMixedBatchBeforeAnyWrite(t *testing.T) {
	root, put := candidateModuleForTest(t)
	valid := put("internal/runtime/candidate_test.go", "package runtime\nvar candidateValue int = 1\n")
	valid.data = []byte("package runtime\nvar candidateValue int = 2\n")
	invalid := put("internal/serveapp/candidate_test.go", "package serveapp\nvar candidateValue int = 1\n")
	invalid.data = []byte("package serveapp\nvar candidateValue int = \"invalid\"\n")
	requireCandidateRefusalBeforeWrite(t, root, []pendingFile{valid, invalid})
}

func TestCandidatePreflightRequiresBuildExcludedFileCoverage(t *testing.T) {
	root, put := candidateModuleForTest(t)
	file := put("internal/runtime/excluded_test.go", "//go:build codemod_preflight_excluded\n\npackage runtime\nvar candidateValue int = 1\n")
	file.data = bytes.Replace(file.data, []byte("= 1"), []byte("= missingCandidateSymbol"), 1)
	requireCandidateRefusalBeforeWrite(t, root, []pendingFile{file})
}

func TestCandidatePreflightChecksValidCrossPackageOverlayTogether(t *testing.T) {
	root, put := candidateModuleForTest(t)
	put("internal/runtime/bus/source.go", "package bus\n")
	provider := put("internal/serveapp/value.go", "package serveapp\nvar Value string\n")
	consumer := put("internal/runtime/bus/value_test.go", "package bus_test\nimport \"example.test/preflight/internal/serveapp\"\nvar Value string = serveapp.Value\n")
	provider.data = []byte("package serveapp\nvar Value int\n")
	consumer.data = []byte("package bus_test\nimport \"example.test/preflight/internal/serveapp\"\nvar Value int = serveapp.Value\n")
	files := []pendingFile{consumer, provider}
	// The consumer replacement requires the provider replacement in the same overlay.
	requireCandidateRefusalBeforeWrite(t, root, []pendingFile{consumer})
	if err := applyPendingFiles(root, files, false); err != nil {
		t.Fatalf("valid combined dry-run refused: %v", err)
	}
	for _, file := range files {
		onDisk, err := os.ReadFile(file.path)
		if err != nil || bytes.Equal(onDisk, file.data) {
			t.Fatalf("dry-run wrote a candidate: %s/%v", file.path, err)
		}
	}
	if err := applyPendingFiles(root, files, true); err != nil {
		t.Fatalf("valid simultaneous replacements refused: %v", err)
	}
	for _, file := range files {
		onDisk, err := os.ReadFile(file.path)
		if err != nil || !bytes.Equal(onDisk, file.data) {
			t.Fatalf("valid candidate not written exactly: %s/%v", file.path, err)
		}
	}
	if err := applyPendingFiles(root, nil, true); err != nil {
		t.Fatalf("no-op candidate changed behavior: %v", err)
	}
}

func TestCandidatePackageInputsAreStableAndRejectUnmatchedFiles(t *testing.T) {
	root := t.TempDir()
	first := pendingFile{path: filepath.Join(root, "internal", "serveapp", "a.go"), data: []byte("a")}
	second := pendingFile{path: filepath.Join(root, "internal", "serveapp", "b_test.go"), data: []byte("b")}
	third := pendingFile{path: filepath.Join(root, "internal", "runtime", "bus", "c_test.go"), data: []byte("c")}
	for _, files := range [][]pendingFile{{first, second, third}, {third, second, first}} {
		overlay, patterns, err := candidateTypeCheckInputs(root, files)
		if err != nil || strings.Join(patterns, ",") != "./internal/runtime/bus,./internal/serveapp" || len(overlay) != 3 || !bytes.Equal(overlay[second.path], second.data) {
			t.Fatalf("unstable, duplicate or incomplete candidate scope: %v/%v", patterns, err)
		}
	}
	for _, files := range [][]pendingFile{nil, {first, first}, {{path: filepath.Join(root, "..", "foreign.go")}}, {{path: filepath.Join(root, "not-go.txt")}}} {
		if _, _, err := candidateTypeCheckInputs(root, files); err == nil {
			t.Fatal("empty, duplicate, foreign or non-Go input accepted")
		}
	}
}
