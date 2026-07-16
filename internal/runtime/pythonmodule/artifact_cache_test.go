package pythonmodule

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const artifactCacheHelperEnv = "SWARM_TEST_PYTHON_ARTIFACT_CACHE_HELPER"

var testArtifactCacheRoot string

func TestMain(m *testing.M) {
	if os.Getenv(artifactCacheHelperEnv) != "" {
		os.Exit(m.Run())
	}
	root, err := os.MkdirTemp("", "swarm-pythonmodule-tests-")
	if err != nil {
		fmt.Fprintf(os.Stderr, "create pythonmodule test root: %v\n", err)
		os.Exit(1)
	}
	if err := os.Setenv("TMPDIR", root); err != nil {
		fmt.Fprintf(os.Stderr, "isolate pythonmodule TMPDIR: %v\n", err)
		_ = os.RemoveAll(root)
		os.Exit(1)
	}
	testArtifactCacheRoot = filepath.Join(root, "cache")
	artifactCacheBaseDir = func() (string, error) { return testArtifactCacheRoot, nil }

	code := m.Run()
	if code == 0 {
		if err := verifyPackageArtifactLifecycle(root); err != nil {
			fmt.Fprintf(os.Stderr, "pythonmodule artifact lifecycle proof: %v\n", err)
			code = 1
		}
	}
	if err := os.RemoveAll(root); err != nil && code == 0 {
		fmt.Fprintf(os.Stderr, "remove pythonmodule test root: %v\n", err)
		code = 1
	}
	os.Exit(code)
}

func verifyPackageArtifactLifecycle(tempRoot string) error {
	legacy, err := filepath.Glob(filepath.Join(tempRoot, "swarm-cpython-wasi-*"))
	if err != nil {
		return err
	}
	if len(legacy) != 0 {
		return fmt.Errorf("created legacy anonymous artifact directories: %v", legacy)
	}
	scratch, err := filepath.Glob(filepath.Join(tempRoot, "swarm-python-module-*"))
	if err != nil {
		return err
	}
	if len(scratch) != 0 {
		return fmt.Errorf("retained invocation scratch directories: %v", scratch)
	}
	if artifactDir == "" {
		return nil
	}
	raw, err := artifactFS.ReadFile(artifactZipPath)
	if err != nil {
		return err
	}
	manifest, digestHex, err := manifestFromArchive(raw, InterpreterDigest)
	if err != nil {
		return err
	}
	expected := filepath.Join(testArtifactCacheRoot, "sha256", digestHex)
	if artifactDir != expected {
		return fmt.Errorf("materialized artifact = %s, want %s", artifactDir, expected)
	}
	if err := validateMaterializedArtifact(artifactDir, manifest); err != nil {
		return err
	}
	entries, err := os.ReadDir(filepath.Dir(expected))
	if err != nil {
		return err
	}
	if len(entries) != 1 || entries[0].Name() != digestHex || !entries[0].IsDir() {
		return fmt.Errorf("cache entries = %v, want exactly %s", entryNames(entries), digestHex)
	}
	return nil
}

func TestDefaultArtifactCacheBaseDirIsProcessStable(t *testing.T) {
	before, beforeErr := defaultArtifactCacheBaseDir()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	after, afterErr := defaultArtifactCacheBaseDir()
	if before != after || !reflect.DeepEqual(beforeErr, afterErr) {
		t.Fatalf("default cache root followed mutable process environment: before=(%q, %v) after=(%q, %v)", before, beforeErr, after, afterErr)
	}
}

func TestArtifactCacheConcurrentProcessesConvergeOnEmbeddedArtifact(t *testing.T) {
	if os.Getenv(artifactCacheHelperEnv) != "" {
		cacheRoot := os.Getenv("SWARM_TEST_PYTHON_ARTIFACT_CACHE_ROOT")
		dir, err := materializeEmbeddedArtifact(cacheRoot)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintln(os.Stdout, dir)
		return
	}

	const processCount = 2
	commands := make([]*exec.Cmd, 0, processCount)
	outputs := make([]bytes.Buffer, processCount)
	for index := 0; index < processCount; index++ {
		command := exec.Command(os.Args[0], "-test.run=^TestArtifactCacheConcurrentProcessesConvergeOnEmbeddedArtifact$", "-test.count=1")
		command.Env = append(os.Environ(),
			artifactCacheHelperEnv+"=1",
			"SWARM_TEST_PYTHON_ARTIFACT_CACHE_ROOT="+testArtifactCacheRoot,
		)
		command.Stdout = &outputs[index]
		command.Stderr = &outputs[index]
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
		commands = append(commands, command)
	}
	for index, command := range commands {
		if err := command.Wait(); err != nil {
			t.Fatalf("helper %d: %v\n%s", index, err, outputs[index].String())
		}
	}
	digestHex := strings.TrimPrefix(InterpreterDigest, "sha256:")
	finalDir := filepath.Join(testArtifactCacheRoot, "sha256", digestHex)
	if err := validateMaterializedArtifact(finalDir, embeddedArtifactManifest); err != nil {
		t.Fatalf("converged artifact: %v", err)
	}
	entries, err := os.ReadDir(filepath.Dir(finalDir))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != digestHex {
		t.Fatalf("concurrent publication entries = %v, want one final tree", entryNames(entries))
	}
}

func TestMaterializedArtifactDirPublishesAndReusesEmbeddedArtifact(t *testing.T) {
	first, err := materializedArtifactDir()
	if err != nil {
		t.Fatalf("first materialization: %v", err)
	}
	info, err := os.Stat(first)
	if err != nil {
		t.Fatal(err)
	}
	second, err := materializedArtifactDir()
	if err != nil {
		t.Fatalf("cache hit: %v", err)
	}
	secondInfo, err := os.Stat(second)
	if err != nil {
		t.Fatal(err)
	}
	if first != second || !os.SameFile(info, secondInfo) {
		t.Fatalf("cache hit changed artifact identity: first=%s second=%s", first, second)
	}
	if !strings.HasSuffix(first, strings.TrimPrefix(InterpreterDigest, "sha256:")) {
		t.Fatalf("artifact path %s is not keyed by full digest", first)
	}
}

func TestEmbeddedArtifactManifestMatchesArchive(t *testing.T) {
	raw, err := artifactFS.ReadFile(artifactZipPath)
	if err != nil {
		t.Fatal(err)
	}
	derived, _, err := manifestFromArchive(raw, InterpreterDigest)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(embeddedArtifactManifest, derived) {
		t.Fatal("generated CPython-WASI artifact manifest does not match pinned archive")
	}
}

func TestArtifactCacheReplacesInvalidPublishedTrees(t *testing.T) {
	raw := syntheticArtifactArchive(t)
	digest := digestBytes(raw)
	tests := []struct {
		name   string
		mutate func(*testing.T, string)
	}{
		{
			name: "altered content",
			mutate: func(t *testing.T, root string) {
				path := filepath.Join(root, pythonWasmPath)
				if err := os.Chmod(path, 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte("poisoned"), 0o600); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "missing file",
			mutate: func(t *testing.T, root string) {
				if err := os.Remove(filepath.Join(root, "lib", "stdlib.py")); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "extra file",
			mutate: func(t *testing.T, root string) {
				if err := os.WriteFile(filepath.Join(root, "extra.py"), []byte("poisoned"), 0o600); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "extra directory",
			mutate: func(t *testing.T, root string) {
				if err := os.Mkdir(filepath.Join(root, "extra"), 0o700); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "symlink",
			mutate: func(t *testing.T, root string) {
				path := filepath.Join(root, pythonWasmPath)
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(filepath.Join("lib", "stdlib.py"), path); err != nil {
					t.Skipf("symlink unavailable: %v", err)
				}
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cacheRoot := t.TempDir()
			first, err := materializeArtifact(cacheRoot, raw, digest)
			if err != nil {
				t.Fatal(err)
			}
			tc.mutate(t, first)
			second, err := materializeArtifact(cacheRoot, raw, digest)
			if err != nil {
				t.Fatalf("replace invalid cache: %v", err)
			}
			manifest, _, err := manifestFromArchive(raw, digest)
			if err != nil {
				t.Fatal(err)
			}
			if err := validateMaterializedArtifact(second, manifest); err != nil {
				t.Fatalf("replacement validation: %v", err)
			}
		})
	}
}

func TestArtifactCacheIgnoresInterruptedStagingAndReplacesPartialFinal(t *testing.T) {
	raw := syntheticArtifactArchive(t)
	digest := digestBytes(raw)
	digestHex := strings.TrimPrefix(digest, "sha256:")
	cacheRoot := t.TempDir()
	parent := filepath.Join(cacheRoot, "sha256")
	if err := os.MkdirAll(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	interrupted := filepath.Join(parent, "."+digestHex+".staging-interrupted")
	if err := os.Mkdir(interrupted, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(interrupted, pythonWasmPath), []byte("partial staging"), 0o600); err != nil {
		t.Fatal(err)
	}
	quarantine := filepath.Join(parent, "."+digestHex+".invalid-interrupted")
	if err := os.Mkdir(quarantine, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(quarantine, pythonWasmPath), []byte("invalid quarantine"), 0o600); err != nil {
		t.Fatal(err)
	}
	partialFinal := filepath.Join(parent, digestHex)
	if err := os.Mkdir(partialFinal, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(partialFinal, pythonWasmPath), []byte("partial final"), 0o600); err != nil {
		t.Fatal(err)
	}

	materialized, err := materializeArtifact(cacheRoot, raw, digest)
	if err != nil {
		t.Fatal(err)
	}
	if materialized == interrupted {
		t.Fatal("interrupted staging directory was published")
	}
	manifest, _, err := manifestFromArchive(raw, digest)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateMaterializedArtifact(materialized, manifest); err != nil {
		t.Fatalf("materialized artifact: %v", err)
	}
	for _, path := range []string{interrupted, quarantine} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("superseded work tree %s still exists: %v", path, err)
		}
	}
}

func TestArtifactCacheRejectsDigestMismatchAndUnsafeArchive(t *testing.T) {
	raw := syntheticArtifactArchive(t)
	if _, err := materializeArtifact(t.TempDir(), raw, "sha256:"+strings.Repeat("0", sha256.Size*2)); err == nil || !strings.Contains(err.Error(), "does not match declared") {
		t.Fatalf("digest mismatch error = %v", err)
	}

	for _, name := range []string{"../escape", "lib/../escape", "lib/./escape"} {
		unsafe := archiveBytes(t, []testArchiveEntry{
			{name: name, body: []byte("escape")},
			{name: pythonWasmPath, body: []byte("wasm")},
		})
		if _, err := materializeArtifact(t.TempDir(), unsafe, digestBytes(unsafe)); err == nil || !strings.Contains(err.Error(), "unsafe path") {
			t.Fatalf("unsafe path %q error = %v", name, err)
		}
	}

	symlink := archiveBytes(t, []testArchiveEntry{
		{name: pythonWasmPath, body: []byte("lib/stdlib.py"), mode: os.ModeSymlink | 0o777},
		{name: "lib/stdlib.py", body: []byte("stdlib")},
	})
	if _, err := materializeArtifact(t.TempDir(), symlink, digestBytes(symlink)); err == nil || !strings.Contains(err.Error(), "unsupported entry") {
		t.Fatalf("archive symlink error = %v", err)
	}
}

type testArchiveEntry struct {
	name string
	body []byte
	mode os.FileMode
}

func syntheticArtifactArchive(t *testing.T) []byte {
	t.Helper()
	return archiveBytes(t, []testArchiveEntry{
		{name: pythonWasmPath, body: []byte("synthetic wasm")},
		{name: "lib/stdlib.py", body: []byte("value = 1\n")},
		{name: "LICENSE", body: []byte("test license\n")},
	})
}

func archiveBytes(t *testing.T, entries []testArchiveEntry) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	for _, entry := range entries {
		header := &zip.FileHeader{Name: entry.name, Method: zip.Deflate}
		if entry.mode != 0 {
			header.SetMode(entry.mode)
		} else {
			header.SetMode(0o644)
		}
		file, err := writer.CreateHeader(header)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Write(entry.body); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

func digestBytes(raw []byte) string {
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func entryNames(entries []os.DirEntry) []string {
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}
