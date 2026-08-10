package contracts

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestBuildBundleRegistrationDirectoryUploadPackagesTextAndData(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := t.TempDir()
	writeBundleRegistrationUploadFixture(t, root)
	writeBundleHashText(t, filepath.Join(root, ".DS_Store"), "ignored")
	writeBundleHashText(t, filepath.Join(root, "prompts", ".#ignored.md"), "ignored")

	upload, err := BuildBundleRegistrationDirectoryUpload(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatalf("BuildBundleRegistrationDirectoryUpload: %v", err)
	}

	var envelope bundleRegistrationEnvelopeUploadV1
	if err := yaml.Unmarshal([]byte(upload.ContentYAML), &envelope); err != nil {
		t.Fatalf("unmarshal content_yaml: %v\n%s", err, upload.ContentYAML)
	}
	if envelope.APIVersion != bundleRegistrationEnvelopeAPIVersion {
		t.Fatalf("api_version = %q, want %q", envelope.APIVersion, bundleRegistrationEnvelopeAPIVersion)
	}
	var paths []string
	for _, file := range envelope.Files {
		paths = append(paths, file.Path)
		if strings.Contains(file.Text, "ignored") {
			t.Fatalf("ignored content leaked through %s", file.Path)
		}
	}
	wantPaths := []string{
		"agents.yaml",
		"flows/alpha/flows/gamma/schema.yaml",
		"flows/alpha/package.yaml",
		"flows/alpha/schema.yaml",
		"package.yaml",
		"packages/foo/flows/beta/schema.yaml",
		"packages/foo/package.yaml",
		"prompts/declared.md",
	}
	if !reflect.DeepEqual(paths, wantPaths) {
		t.Fatalf("files = %#v, want %#v\n%s", paths, wantPaths, upload.ContentYAML)
	}
	if upload.DataBlob == nil {
		t.Fatal("DataBlob = nil, want one data entry")
	}
	if upload.DataBlob.APIVersion != bundleRegistrationDataAPIVersion {
		t.Fatalf("data api_version = %q, want %q", upload.DataBlob.APIVersion, bundleRegistrationDataAPIVersion)
	}
	if got, want := len(upload.DataBlob.Entries), 4; got != want {
		t.Fatalf("data entries = %d, want %d", got, want)
	}
	wantData := []BundleRegisterDataEntryV1{
		{Path: "flows/alpha/data/empty.bin", DataBase64: ""},
		{Path: "flows/alpha/data/payload.bin", DataBase64: base64.StdEncoding.EncodeToString([]byte{0x01, 0x02, 0x03})},
		{Path: "flows/alpha/flows/gamma/data/nested.bin", DataBase64: base64.StdEncoding.EncodeToString([]byte{0x09})},
		{Path: "packages/foo/flows/beta/data/child.bin", DataBase64: base64.StdEncoding.EncodeToString([]byte{0x04, 0x05})},
	}
	if !reflect.DeepEqual(upload.DataBlob.Entries, wantData) {
		t.Fatalf("data entries = %#v, want %#v", upload.DataBlob.Entries, wantData)
	}
}

func TestDataDirectoryIntentSurvivesLoadHashRegistrationAndReconstruction(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := t.TempDir()
	platform := DefaultPlatformSpecFile(repo)
	writeBundleHashText(t, filepath.Join(root, "package.yaml"), `name: data-intent
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: review
    flow: review
`)
	writeBundleHashText(t, filepath.Join(root, "flows", "review", "schema.yaml"), `name: review
mode: static
states: [ready]
initial_state: ready
terminal_states: [ready]
`)
	writeBundleHashText(t, filepath.Join(root, "flows", "review", "agents.yaml"), `worker:
  id: worker
  role: worker
  intent: data/worker.md
  model: regular
  subscriptions: [work.requested]
`)
	const intentContent = "Perform the review work exactly.\n"
	writeBundleHashText(t, filepath.Join(root, "flows", "review", "data", "worker.md"), intentContent)

	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, platform)
	if err != nil {
		t.Fatalf("load bundle: %v", err)
	}
	bundleHash, err := BundleHash(bundle)
	if err != nil {
		t.Fatalf("BundleHash: %v", err)
	}
	projection, err := BuildBundleCatalogProjectionWithOptions(bundle, BundleCatalogProjectionOptions{Source: "test"})
	if err != nil {
		t.Fatalf("BuildBundleCatalogProjection: %v", err)
	}
	upload, err := BuildBundleRegistrationDirectoryUpload(repo, root, platform)
	if err != nil {
		t.Fatalf("BuildBundleRegistrationDirectoryUpload: %v", err)
	}
	if !strings.Contains(upload.ContentYAML, "flows/review/data/worker.md") || !strings.Contains(upload.ContentYAML, "Perform the review work exactly.") {
		t.Fatalf("registration upload omitted exact data-directory intent:\n%s", upload.ContentYAML)
	}

	source, err := LoadBundleCatalogRuntimeSource(repo, BundleCatalogRuntimeLoadRequest{
		BundleHash:              bundleHash,
		ContentYAML:             projection.ContentYAML,
		DataBlob:                projection.DataBlob,
		RunningPlatformSpecPath: platform,
	})
	if err != nil {
		t.Fatalf("LoadBundleCatalogRuntimeSource: %v", err)
	}
	defer source.Cleanup()
	reloadedHash, err := BundleHash(source.Bundle)
	if err != nil {
		t.Fatalf("reloaded BundleHash: %v", err)
	}
	if reloadedHash != bundleHash {
		t.Fatalf("reloaded hash = %s, want %s", reloadedHash, bundleHash)
	}
	workerRaw, err := os.ReadFile(filepath.Join(source.ContractsRoot, "flows", "review", "data", "worker.md"))
	if err != nil {
		t.Fatalf("read reconstructed intent: %v", err)
	}
	if string(workerRaw) != intentContent {
		t.Fatalf("reconstructed intent = %q, want exact %q", workerRaw, intentContent)
	}
}

func TestBuildBundleRegistrationDirectoryUploadCarriesDeclaredMockModules(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := writeMockedAgentContractsDir(t)

	upload, err := BuildBundleRegistrationDirectoryUpload(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatalf("BuildBundleRegistrationDirectoryUpload: %v", err)
	}
	if upload.DataBlob == nil {
		t.Fatal("DataBlob = nil, want declared mock module entry")
	}
	var paths []string
	for _, entry := range upload.DataBlob.Entries {
		paths = append(paths, entry.Path)
	}
	if !reflect.DeepEqual(paths, []string{"mocks/assistant.py"}) {
		t.Fatalf("data entries = %#v, want mocks/assistant.py only", paths)
	}
	want := base64.StdEncoding.EncodeToString([]byte(mockAssistantSource))
	if got := upload.DataBlob.Entries[0].DataBase64; got != want {
		t.Fatalf("mock data_base64 = %q, want exact declared module bytes", got)
	}
	var envelope bundleRegistrationEnvelopeUploadV1
	if err := yaml.Unmarshal([]byte(upload.ContentYAML), &envelope); err != nil {
		t.Fatalf("unmarshal content_yaml: %v\n%s", err, upload.ContentYAML)
	}
	for _, file := range envelope.Files {
		if file.Path == "mocks/assistant.py" {
			t.Fatal("mock module must be carried as a data blob entry, not a text envelope file")
		}
	}
}

func TestBuildBundleRegistrationDirectoryUploadCarriesPolicyModules(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := writePolicyModuleContractsDir(t)

	upload, err := BuildBundleRegistrationDirectoryUpload(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatalf("BuildBundleRegistrationDirectoryUpload: %v", err)
	}
	if upload.DataBlob == nil {
		t.Fatal("DataBlob = nil, want policy module entries")
	}
	wasmRaw, err := os.ReadFile(filepath.Join(root, "modules", "structured_renderer.wasm"))
	if err != nil {
		t.Fatal(err)
	}
	pythonRaw := []byte(pythonRendererSource())
	want := []BundleRegisterDataEntryV1{
		{Path: "modules/python_renderer.py", DataBase64: base64.StdEncoding.EncodeToString(pythonRaw)},
		{Path: "modules/structured_renderer.wasm", DataBase64: base64.StdEncoding.EncodeToString(wasmRaw)},
	}
	if !reflect.DeepEqual(upload.DataBlob.Entries, want) {
		t.Fatalf("data entries = %#v, want %#v", upload.DataBlob.Entries, want)
	}
}

func TestPolicyModuleRegistrationRuntimeSourceReloadRoundTrip(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := writePolicyModuleContractsDir(t)
	platform := DefaultPlatformSpecFile(repo)

	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, platform)
	if err != nil {
		t.Fatalf("load policy module bundle: %v", err)
	}
	bundleHash, err := BundleHash(bundle)
	if err != nil {
		t.Fatalf("BundleHash: %v", err)
	}
	projection, err := BuildBundleCatalogProjectionWithOptions(bundle, BundleCatalogProjectionOptions{Source: "test"})
	if err != nil {
		t.Fatalf("BuildBundleCatalogProjection: %v", err)
	}
	source, err := LoadBundleCatalogRuntimeSource(repo, BundleCatalogRuntimeLoadRequest{
		BundleHash:              bundleHash,
		ContentYAML:             projection.ContentYAML,
		DataBlob:                projection.DataBlob,
		RunningPlatformSpecPath: platform,
	})
	if err != nil {
		t.Fatalf("LoadBundleCatalogRuntimeSource: %v", err)
	}
	defer source.Cleanup()
	reloadedHash, err := BundleHash(source.Bundle)
	if err != nil {
		t.Fatalf("reloaded bundle hash: %v", err)
	}
	if reloadedHash != bundleHash {
		t.Fatalf("reloaded hash = %s, want %s", reloadedHash, bundleHash)
	}
	for _, rel := range []string{"modules/structured_renderer.wasm", "modules/python_renderer.py"} {
		info, err := os.Stat(filepath.Join(source.ContractsRoot, filepath.FromSlash(rel)))
		if err != nil {
			t.Fatalf("reconstructed root missing %s: %v", rel, err)
		}
		if !info.Mode().IsRegular() {
			t.Fatalf("reconstructed %s is not a regular file", rel)
		}
	}

	mutated := append([]byte(nil), wasmPolicyModuleBytes(t)...)
	mutated[len(mutated)-1] ^= 0xff
	writeBundleHashBytes(t, filepath.Join(root, "modules", "structured_renderer.wasm"), mutated)
	writeBundleHashText(t, filepath.Join(root, "flows", "render", "policy.yaml"), combinedPolicyModuleYAML(t, root, mutated, []byte(pythonRendererSource())))
	drifted, err := LoadWorkflowContractBundleWithOverrides(repo, root, platform)
	if err != nil {
		t.Fatalf("load drifted policy module bundle: %v", err)
	}
	driftedHash, err := BundleHash(drifted)
	if err != nil {
		t.Fatalf("drifted bundle hash: %v", err)
	}
	if driftedHash == bundleHash {
		t.Fatalf("policy module byte change produced identical bundle hash %s; content replaced under unchanged identity", bundleHash)
	}
}

func TestBuildBundleRegistrationDirectoryUploadFailsClosed(t *testing.T) {
	repo := repoRootForContractsTest(t)
	for _, tc := range []struct {
		name       string
		mutate     func(t *testing.T, root string)
		wantErrSub string
	}{
		{
			name: "symlink",
			mutate: func(t *testing.T, root string) {
				target := filepath.Join(root, "prompts", "root.md")
				link := filepath.Join(root, "prompts", "declared.md")
				if err := os.Remove(link); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, link); err != nil {
					t.Skipf("symlink unsupported: %v", err)
				}
			},
			wantErrSub: "symlink",
		},
		{
			name: "ascii case collision",
			mutate: func(t *testing.T, root string) {
				writeBundleHashText(t, filepath.Join(root, "prompts", "Declared.md"), "collision\n")
				writeBundleHashText(t, filepath.Join(root, "agents.yaml"), "root:\n  id: root\n  role: root\n  intent: prompts/declared.md\n  model: regular\n  subscriptions: [root.requested]\ncollider:\n  id: collider\n  role: collision\n  intent: prompts/Declared.md\n  model: regular\n  subscriptions: [collision.requested]\n")
				lower, errLower := os.Stat(filepath.Join(root, "prompts", "declared.md"))
				upper, errUpper := os.Stat(filepath.Join(root, "prompts", "Declared.md"))
				if errLower == nil && errUpper == nil && os.SameFile(lower, upper) {
					t.Skip("case-insensitive filesystem cannot represent ASCII case collision fixture")
				}
			},
			wantErrSub: "case-colliding",
		},
		{
			name: "invalid prompt utf8",
			mutate: func(t *testing.T, root string) {
				writeBundleHashBytes(t, filepath.Join(root, "prompts", "declared.md"), []byte{0xff})
			},
			wantErrSub: "valid UTF-8",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writeBundleRegistrationUploadFixture(t, root)
			tc.mutate(t, root)

			_, err := BuildBundleRegistrationDirectoryUpload(repo, root, DefaultPlatformSpecFile(repo))
			if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
				t.Fatalf("error = %v, want substring %q", err, tc.wantErrSub)
			}
		})
	}
}

func writeBundleRegistrationUploadFixture(t *testing.T, root string) {
	t.Helper()
	writeBundleHashText(t, filepath.Join(root, "package.yaml"), `
name: upload-fixture
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
packages:
  - path: packages/foo
flows:
  - id: alpha
    flow: alpha
`)
	writeBundleHashText(t, filepath.Join(root, "flows", "alpha", "schema.yaml"), `
initial_state: start
states:
  - start
  - done
`)
	writeBundleHashText(t, filepath.Join(root, "prompts", "root.md"), "root prompt\n")
	writeBundleHashText(t, filepath.Join(root, "agents.yaml"), "root:\n  id: root\n  role: root\n  intent: prompts/declared.md\n  model: regular\n  subscriptions: [root.requested]\n")
	writeBundleHashText(t, filepath.Join(root, "prompts", "declared.md"), "declared intent\n")
	writeBundleHashText(t, filepath.Join(root, "flows", "alpha", "package.yaml"), `
name: nested-flow-package
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: gamma
    flow: gamma
`)
	writeBundleHashText(t, filepath.Join(root, "flows", "alpha", "flows", "gamma", "schema.yaml"), `
initial_state: start
states:
  - start
  - done
`)
	writeBundleHashText(t, filepath.Join(root, "packages", "foo", "package.yaml"), `
name: child-package
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: beta
    flow: beta
`)
	writeBundleHashText(t, filepath.Join(root, "packages", "foo", "flows", "beta", "schema.yaml"), `
initial_state: start
states:
  - start
  - done
`)
	writeBundleHashBytes(t, filepath.Join(root, "flows", "alpha", "data", "empty.bin"), nil)
	writeBundleHashBytes(t, filepath.Join(root, "flows", "alpha", "data", "payload.bin"), []byte{0x01, 0x02, 0x03})
	writeBundleHashBytes(t, filepath.Join(root, "flows", "alpha", "flows", "gamma", "data", "nested.bin"), []byte{0x09})
	writeBundleHashBytes(t, filepath.Join(root, "packages", "foo", "flows", "beta", "data", "child.bin"), []byte{0x04, 0x05})
}

func wasmPolicyModuleBytes(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "computemodule", "testdata", "structured_renderer.wasm"))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func writePolicyModuleContractsDir(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeBundleHashText(t, filepath.Join(root, "package.yaml"), `name: policy-module-bundle
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
	writeBundleHashBytes(t, filepath.Join(root, "modules", "structured_renderer.wasm"), wasmPolicyModuleBytes(t))
	writeBundleHashBytes(t, filepath.Join(root, "modules", "python_renderer.py"), []byte(pythonRendererSource()))
	writeBundleHashText(t, filepath.Join(root, "flows", "render", "policy.yaml"), combinedPolicyModuleYAML(t, root, wasmPolicyModuleBytes(t), []byte(pythonRendererSource())))
	return root
}

func combinedPolicyModuleYAML(t *testing.T, root string, wasmBytes, pythonBytes []byte) string {
	t.Helper()
	wasmSum := sha256.Sum256(wasmBytes)
	pythonSum := sha256.Sum256(pythonBytes)
	srcRaw, err := os.ReadFile(filepath.Join(root, "src", "structured_renderer.rs"))
	if err != nil {
		srcRaw = []byte("fn compute() {}\n")
	}
	srcSum := sha256.Sum256(srcRaw)
	return `modules:
  structured_renderer:
    path: modules/structured_renderer.wasm
    abi: core-json-v1
    entry: compute
    digest: sha256:` + hex.EncodeToString(wasmSum[:]) + `
    source_path: src/structured_renderer.rs
    source_hash: sha256:` + hex.EncodeToString(srcSum[:]) + `
    limits:
      gas: 5000000
      memory_pages: 17
      output_bytes: 1024
    input_schema:
      type: object
      additionalProperties: false
      required: [component, owner, language, files]
      properties:
        component: {type: string}
        owner: {type: string}
        language: {type: string}
        files: {type: array, items: {type: string}}
    output_schema:
      type: object
      additionalProperties: false
      required: [content, format, line_count]
      properties:
        content: {type: string}
        format: {type: string}
        line_count: {type: integer}
  python_renderer:
    path: modules/python_renderer.py
    kind: python
    abi: python-json-v1
    entry: handle
    digest: sha256:` + hex.EncodeToString(pythonSum[:]) + `
    limits:
      gas: 2500000000
      memory_pages: 8192
      output_bytes: 4096
    input_schema:
      type: object
      additionalProperties: false
      required: [component, owner, language, files]
      properties:
        component: {type: string}
        owner: {type: string}
        language: {type: string}
        files: {type: array, items: {type: string}}
    output_schema:
      type: object
      additionalProperties: false
      required: [content, format, line_count]
      properties:
        content: {type: string}
        format: {type: string}
        line_count: {type: integer}
`
}
