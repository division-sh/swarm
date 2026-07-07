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

	"github.com/division-sh/swarm/internal/runtime/pythonmodule"
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
	if len(reportA.Steps) != 2 ||
		reportA.Steps[0].Name != bundleBuildStepWasmModules ||
		reportA.Steps[0].Status != "passed" ||
		reportA.Steps[1].Name != bundleBuildStepPythonModules ||
		reportA.Steps[1].Status != "passed" {
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
	sourcePath := "BUILD-MANIFEST.JSON"
	writeBundleHashBytes(t, filepath.Join(root, sourcePath), sourceBytes)
	sum := sha256.Sum256(sourceBytes)
	policy := bundleBuildPolicyYAML(t, root, "sha256:"+hex.EncodeToString(sum[:]))
	policy = strings.Replace(policy, "source_path: src/structured_renderer.rs", "source_path: "+sourcePath, 1)
	writeBundleHashText(t, filepath.Join(root, "flows", "render", "policy.yaml"), policy)

	_, err := BuildBundleMaterialization(context.Background(), BundleBuildRequest{
		RepoRoot:         repo,
		ContractsRoot:    root,
		PlatformSpecPath: DefaultPlatformSpecFile(repo),
		OutputRoot:       filepath.Join(t.TempDir(), "build"),
	})
	if err == nil || !strings.Contains(err.Error(), "reserved generated bundle build artifact path") {
		t.Fatalf("BuildBundleMaterialization error = %v, want reserved artifact collision", err)
	}
}

func TestBuildBundleMaterializationRejectsModuleByteReservedArtifactCollision(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := writeBundleBuildContractsDir(t)
	modulePath := "Build-Manifest.Json"
	raw, err := os.ReadFile(filepath.Join(root, "modules", "structured_renderer.wasm"))
	if err != nil {
		t.Fatal(err)
	}
	writeBundleHashBytes(t, filepath.Join(root, modulePath), raw)
	policy := bundleBuildPolicyYAML(t, root, bundleBuildSourceHash(t, root))
	policy = strings.Replace(policy, "path: modules/structured_renderer.wasm", "path: "+modulePath, 1)
	writeBundleHashText(t, filepath.Join(root, "flows", "render", "policy.yaml"), policy)

	_, err = BuildBundleMaterialization(context.Background(), BundleBuildRequest{
		RepoRoot:         repo,
		ContractsRoot:    root,
		PlatformSpecPath: DefaultPlatformSpecFile(repo),
		OutputRoot:       filepath.Join(t.TempDir(), "build"),
	})
	if err == nil || !strings.Contains(err.Error(), "reserved generated bundle build artifact path") {
		t.Fatalf("BuildBundleMaterialization error = %v, want reserved artifact collision", err)
	}
}

func TestBundleBuildReservedArtifactPathRejectsCaseVariants(t *testing.T) {
	for _, tc := range []struct {
		label string
		path  string
	}{
		{label: "bundle build input", path: "BUILD-MANIFEST.JSON"},
		{label: "policy module path", path: "Build-Manifest.Json"},
		{label: "policy module source_path", path: "./build-manifest.json"},
	} {
		t.Run(tc.label, func(t *testing.T) {
			err := validateBundleBuildReservedArtifactPath(tc.label, tc.path)
			if err == nil || !strings.Contains(err.Error(), "reserved generated bundle build artifact path") {
				t.Fatalf("validateBundleBuildReservedArtifactPath(%q, %q) = %v, want reserved artifact collision", tc.label, tc.path, err)
			}
		})
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

func TestBundleBuildStepRegistryHasWasmAndPythonSliceB(t *testing.T) {
	names := BundleBuildStepNames(nil)
	want := []string{bundleBuildStepWasmModules, bundleBuildStepPythonModules}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("BundleBuildStepNames(nil) = %#v, want %#v", names, want)
	}
}

func TestBuildBundleMaterializationMaterializesPythonModuleIdentity(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := writePythonBundleBuildContractsDir(t, pythonRendererSource())
	platform := DefaultPlatformSpecFile(repo)

	report, err := BuildBundleMaterialization(context.Background(), BundleBuildRequest{
		RepoRoot:         repo,
		ContractsRoot:    root,
		PlatformSpecPath: platform,
		OutputRoot:       filepath.Join(t.TempDir(), "build"),
	})
	if err != nil {
		t.Fatalf("BuildBundleMaterialization: %v", err)
	}
	if len(report.Steps) != 2 || report.Steps[0].Name != bundleBuildStepWasmModules || report.Steps[1].Name != bundleBuildStepPythonModules {
		t.Fatalf("steps = %#v", report.Steps)
	}
	var manifest BundleBuildManifest
	if err := json.Unmarshal([]byte(readBundleBuildFile(t, report.OutputPath, "build-manifest.json")), &manifest); err != nil {
		t.Fatalf("decode build manifest: %v", err)
	}
	if got, want := len(manifest.Modules), 1; got != want {
		t.Fatalf("manifest modules = %d, want %d", got, want)
	}
	module := manifest.Modules[0]
	if module.ID != "python_renderer" ||
		module.Kind != pythonmodule.Kind ||
		module.Path != "modules/python_renderer.py" ||
		module.SourcePath != "modules/python_renderer.py" ||
		module.SourceHash != module.Digest ||
		module.Interpreter != pythonmodule.Interpreter ||
		module.InterpreterDigest != pythonmodule.InterpreterDigest ||
		module.SnapshotDigest == "" ||
		module.HarnessABI != pythonmodule.HarnessABI {
		t.Fatalf("manifest module = %#v", module)
	}
	if _, err := os.Stat(filepath.Join(report.OutputPath, "modules", "python_renderer.py")); err != nil {
		t.Fatalf("materialized python source missing: %v", err)
	}
}

func TestBuildBundleMaterializationFailsClosedForPythonSyntax(t *testing.T) {
	repo := repoRootForContractsTest(t)
	for _, tc := range []struct {
		name   string
		source []byte
		want   string
	}{
		{name: "syntax", source: []byte("def handle(input):\n    return {\n"), want: "compute_module_compile"},
		{name: "missing_handle", source: []byte("def other(input):\n    return {}\n"), want: "must define callable handle(input)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := writePythonBundleBuildContractsDir(t, tc.source)
			_, err := BuildBundleMaterialization(context.Background(), BundleBuildRequest{
				RepoRoot:         repo,
				ContractsRoot:    root,
				PlatformSpecPath: DefaultPlatformSpecFile(repo),
				OutputRoot:       filepath.Join(t.TempDir(), "build"),
			})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("BuildBundleMaterialization error = %v, want %q", err, tc.want)
			}
		})
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

func writePythonBundleBuildContractsDir(t *testing.T, source []byte) string {
	t.Helper()
	root := t.TempDir()
	writeBundleHashText(t, filepath.Join(root, "package.yaml"), `name: python-bundle-build-module
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
	writeBundleHashBytes(t, filepath.Join(root, "modules", "python_renderer.py"), source)
	writeBundleHashText(t, filepath.Join(root, "flows", "render", "policy.yaml"), pythonBundleBuildPolicyYAML(source))
	return root
}

func pythonBundleBuildPolicyYAML(source []byte) string {
	sum := sha256.Sum256(source)
	return `modules:
  python_renderer:
    path: modules/python_renderer.py
    kind: python
    abi: python-json-v1
    entry: handle
    digest: sha256:` + hex.EncodeToString(sum[:]) + `
    limits:
      gas: 2500000000
      memory_pages: 8192
      output_bytes: 4096
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

func pythonRendererSource() []byte {
	return []byte(`def handle(input):
    lines = [
        "component: " + input["component"],
        "owner: " + input["owner"],
        "language: " + input["language"],
    ]
    for name in input["files"]:
        if name.endswith(".yaml"):
            lines.append("- deploy/" + name)
        elif name.endswith(".go"):
            lines.append("- src/" + name)
        else:
            lines.append("- " + name)
    return {"content": "\n".join(lines), "format": "yaml", "line_count": len(lines)}
`)
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
