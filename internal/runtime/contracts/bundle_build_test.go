package contracts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestBuildBundleMaterializationWritesReproducibleContractsRoot(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := writeBundleBuildContractsDir(t)
	platform := DefaultPlatformSpecFile(repo)

	outA := filepath.Join(t.TempDir(), "build-a")
	reportA, err := BuildBundleMaterialization(context.Background(), BundleBuildRequest{
		RepoRoot:         repo,
		ContractsRoot:    root,
		PlatformSpecPath: platform,
		OutputRoot:       outA,
	})
	if err != nil {
		t.Fatalf("BuildBundleMaterialization A: %v", err)
	}
	if !IsBundleHash(reportA.BundleHash) {
		t.Fatalf("BundleHash = %q, want canonical hash", reportA.BundleHash)
	}
	if got, want := filepath.Base(reportA.OutputPath), reportA.BundleHash; got != want {
		t.Fatalf("output dir basename = %q, want %q", got, want)
	}
	for _, rel := range []string{
		"package.yaml",
		"flows/render/schema.yaml",
		"flows/render/policy.yaml",
		"modules/structured_renderer.wasm",
		"src/structured_renderer.rs",
		"build-manifest.json",
	} {
		if _, err := os.Stat(filepath.Join(reportA.OutputPath, filepath.FromSlash(rel))); err != nil {
			t.Fatalf("materialized %s missing: %v", rel, err)
		}
	}
	if strings.Contains(readBundleBuildFile(t, reportA.OutputPath, "build-manifest.json"), root) {
		t.Fatalf("build manifest leaked source contracts root")
	}

	var manifest BundleBuildManifest
	if err := json.Unmarshal([]byte(readBundleBuildFile(t, reportA.OutputPath, "build-manifest.json")), &manifest); err != nil {
		t.Fatalf("decode build manifest: %v", err)
	}
	if manifest.BundleHash != reportA.BundleHash || manifest.APIVersion != bundleBuildManifestAPIVersion {
		t.Fatalf("manifest identity = %#v, want hash=%s api=%s", manifest, reportA.BundleHash, bundleBuildManifestAPIVersion)
	}
	if got, want := len(manifest.Modules), 1; got != want {
		t.Fatalf("manifest modules = %d, want %d", got, want)
	}
	module := manifest.Modules[0]
	if module.ID != "structured_renderer" || module.Kind != "wasm" || module.Path != "modules/structured_renderer.wasm" || module.SourcePath != "src/structured_renderer.rs" || module.SourceHash == "" {
		t.Fatalf("manifest module = %#v", module)
	}
	if len(reportA.Steps) != 1 || reportA.Steps[0].Name != bundleBuildStepWasmModules || reportA.Steps[0].Status != "passed" {
		t.Fatalf("steps = %#v", reportA.Steps)
	}

	materialized, err := LoadWorkflowContractBundleWithOverrides(repo, reportA.OutputPath, platform)
	if err != nil {
		t.Fatalf("Load materialized bundle: %v", err)
	}
	materializedHash, err := BundleHash(materialized)
	if err != nil {
		t.Fatalf("BundleHash materialized: %v", err)
	}
	if materializedHash != reportA.BundleHash {
		t.Fatalf("materialized hash = %s, want %s", materializedHash, reportA.BundleHash)
	}

	outB := filepath.Join(t.TempDir(), "build-b")
	reportB, err := BuildBundleMaterialization(context.Background(), BundleBuildRequest{
		RepoRoot:         repo,
		ContractsRoot:    root,
		PlatformSpecPath: platform,
		OutputRoot:       outB,
	})
	if err != nil {
		t.Fatalf("BuildBundleMaterialization B: %v", err)
	}
	if reportB.BundleHash != reportA.BundleHash {
		t.Fatalf("rebuild hash = %s, want %s", reportB.BundleHash, reportA.BundleHash)
	}
	if gotA, gotB := materializedHashedFileBytes(t, reportA.OutputPath), materializedHashedFileBytes(t, reportB.OutputPath); !reflect.DeepEqual(gotA, gotB) {
		t.Fatalf("materialized hashed files drifted:\nA=%#v\nB=%#v", gotA, gotB)
	}
}

func TestBuildBundleMaterializationFailsClosedForSourceHashDrift(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := writeBundleBuildContractsDir(t)
	platform := DefaultPlatformSpecFile(repo)
	writeBundleHashText(t, filepath.Join(root, "flows", "render", "policy.yaml"), bundleBuildPolicyYAML(t, root, "sha256:0000000000000000000000000000000000000000000000000000000000000000"))

	_, err := BuildBundleMaterialization(context.Background(), BundleBuildRequest{
		RepoRoot:         repo,
		ContractsRoot:    root,
		PlatformSpecPath: platform,
		OutputRoot:       filepath.Join(t.TempDir(), "build"),
	})
	if err == nil || !strings.Contains(err.Error(), "stale source_hash") {
		t.Fatalf("BuildBundleMaterialization error = %v, want stale source_hash", err)
	}
}

func TestBuildBundleMaterializationRejectsSourceProofReservedArtifactCollision(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := writeBundleBuildContractsDir(t)
	sourceBytes := []byte("declared source proof that must not be overwritten\n")
	writeBundleHashBytes(t, filepath.Join(root, bundleBuildManifestPath), sourceBytes)
	sum := sha256.Sum256(sourceBytes)
	policy := bundleBuildPolicyYAML(t, root, "sha256:"+hex.EncodeToString(sum[:]))
	policy = strings.Replace(policy, "source_path: src/structured_renderer.rs", "source_path: "+bundleBuildManifestPath, 1)
	writeBundleHashText(t, filepath.Join(root, "flows", "render", "policy.yaml"), policy)

	_, err := BuildBundleMaterialization(context.Background(), BundleBuildRequest{
		RepoRoot:         repo,
		ContractsRoot:    root,
		PlatformSpecPath: DefaultPlatformSpecFile(repo),
		OutputRoot:       filepath.Join(t.TempDir(), "build"),
	})
	if err == nil || !strings.Contains(err.Error(), "collides with generated bundle build artifact") {
		t.Fatalf("BuildBundleMaterialization error = %v, want reserved artifact collision", err)
	}
}

func TestBuildBundleMaterializationFailsClosedBeforeRuntimeForModuleDrift(t *testing.T) {
	repo := repoRootForContractsTest(t)
	platform := DefaultPlatformSpecFile(repo)
	for _, tc := range []struct {
		name   string
		mutate func(string)
		want   string
	}{
		{
			name: "missing module bytes",
			mutate: func(root string) {
				if err := os.Remove(filepath.Join(root, "modules", "structured_renderer.wasm")); err != nil {
					t.Fatal(err)
				}
			},
			want: "no such file",
		},
		{
			name: "schema drift",
			mutate: func(root string) {
				policy := bundleBuildPolicyYAML(t, root, bundleBuildSourceHash(t, root))
				policy = strings.Replace(policy, `line_count:
          type: integer`, `line_count:
          type: number`, 1)
				writeBundleHashText(t, filepath.Join(root, "flows", "render", "policy.yaml"), policy)
			},
			want: "float/number",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := writeBundleBuildContractsDir(t)
			tc.mutate(root)
			_, err := BuildBundleMaterialization(context.Background(), BundleBuildRequest{
				RepoRoot:         repo,
				ContractsRoot:    root,
				PlatformSpecPath: platform,
				OutputRoot:       filepath.Join(t.TempDir(), "build"),
			})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("BuildBundleMaterialization error = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestBuildBundleMaterializationRejectsOutputInsideHashedRecursiveInput(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := writeBundleBuildContractsDir(t)
	outputRoot := filepath.Join(root, "flows", "render", "data", "build-output")
	if err := os.MkdirAll(filepath.Dir(outputRoot), 0o755); err != nil {
		t.Fatal(err)
	}

	_, err := BuildBundleMaterialization(context.Background(), BundleBuildRequest{
		RepoRoot:         repo,
		ContractsRoot:    root,
		PlatformSpecPath: DefaultPlatformSpecFile(repo),
		OutputRoot:       outputRoot,
	})
	if err == nil || !strings.Contains(err.Error(), "hashed recursive input") {
		t.Fatalf("BuildBundleMaterialization error = %v, want hashed recursive input rejection", err)
	}
}

func TestBundleBuildStepRegistryHasOnlyWasmSliceA(t *testing.T) {
	names := BundleBuildStepNames(nil)
	if !reflect.DeepEqual(names, []string{bundleBuildStepWasmModules}) {
		t.Fatalf("BundleBuildStepNames(nil) = %#v, want wasm-only slice", names)
	}
	for _, name := range names {
		if strings.Contains(strings.ToLower(name), "python") || strings.Contains(strings.ToLower(name), "snapshot") {
			t.Fatalf("slice A build step %q must not register python/snapshot support", name)
		}
	}
}

func writeBundleBuildContractsDir(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeBundleHashText(t, filepath.Join(root, "package.yaml"), `name: bundle-build-module
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: render
    flow: render
`)
	writeBundleHashText(t, filepath.Join(root, "flows", "render", "schema.yaml"), `name: render
mode: static
states: [ready]
initial_state: ready
terminal_states: [ready]
`)
	raw, err := os.ReadFile(filepath.Join("..", "computemodule", "testdata", "structured_renderer.wasm"))
	if err != nil {
		t.Fatal(err)
	}
	writeBundleHashBytes(t, filepath.Join(root, "modules", "structured_renderer.wasm"), raw)
	writeBundleHashText(t, filepath.Join(root, "src", "structured_renderer.rs"), "fn compute() {}\n")
	writeBundleHashText(t, filepath.Join(root, "flows", "render", "policy.yaml"), bundleBuildPolicyYAML(t, root, bundleBuildSourceHash(t, root)))
	return root
}

func bundleBuildPolicyYAML(t *testing.T, root, sourceHash string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, "modules", "structured_renderer.wasm"))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	return `modules:
  structured_renderer:
    path: modules/structured_renderer.wasm
    abi: core-json-v1
    entry: compute
    digest: sha256:` + hex.EncodeToString(sum[:]) + `
    source_path: src/structured_renderer.rs
    source_hash: ` + sourceHash + `
    limits:
      gas: 5000000
      memory_pages: 17
      output_bytes: 1024
    input_schema:
      type: object
      additionalProperties: false
      required: [component, owner, language, files]
      properties:
        component:
          type: string
        owner:
          type: string
        language:
          type: string
        files:
          type: array
          items:
            type: string
    output_schema:
      type: object
      additionalProperties: false
      required: [content, format, line_count]
      properties:
        content:
          type: string
        format:
          type: string
        line_count:
          type: integer
`
}

func bundleBuildSourceHash(t *testing.T, root string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, "src", "structured_renderer.rs"))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func readBundleBuildFile(t *testing.T, root, rel string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
	if err != nil {
		t.Fatalf("read %s: %v", rel, err)
	}
	return string(raw)
}

func materializedHashedFileBytes(t *testing.T, root string) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d == nil || d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if rel == "build-manifest.json" {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		out[rel] = string(raw)
		return nil
	})
	if err != nil {
		t.Fatalf("walk materialized root: %v", err)
	}
	return out
}
