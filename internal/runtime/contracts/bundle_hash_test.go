package contracts

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

func TestBundleHashV1GoldenCorpus(t *testing.T) {
	root := t.TempDir()
	platform := DefaultPlatformSpecFile(t.TempDir())
	writeBundleHashText(t, platform, `
api_specification:
  zeta: 1.0
  alpha: true
`)
	writeBundleHashText(t, filepath.Join(root, "package.yaml"), `
version: "1.0.0"
name: golden-bundle
flows:
  - flow: alpha
    id: alpha
`)
	writeBundleHashText(t, filepath.Join(root, "schema.yaml"), `
description: root schema
fields:
  topic:
    type: string
`)
	writeBundleHashText(t, filepath.Join(root, "prompts", "guide.md"), "\xef\xbb\xbfhello\r\nwith spaces  ")
	writeBundleHashText(t, filepath.Join(root, "flows", "alpha", "schema.yaml"), `
initial_state: start
states:
  - start
  - done
ref_example:
  $ref: ./types.yaml
`)
	writeBundleHashText(t, filepath.Join(root, "flows", "alpha", "prompts", "alpha.md"), "alpha prompt\rwithout final newline")
	writeBundleHashBytes(t, filepath.Join(root, "flows", "alpha", "data", "payload.bin"), []byte{0x00, 0xff, 'a', '\r', '\n'})

	bundle := bundleHashTestBundle(root, platform)
	got, err := BundleHash(bundle)
	if err != nil {
		t.Fatalf("BundleHash: %v", err)
	}
	const want = "bundle-v1:sha256:c556830bbbad9624a56718bdd101d7ceb00fde71cb79f5c533a6b5181721204b"
	if got != want {
		t.Fatalf("BundleHash = %q, want %q", got, want)
	}
	if !regexp.MustCompile(`^bundle-v1:sha256:[a-f0-9]{64}$`).MatchString(got) {
		t.Fatalf("BundleHash = %q, want v1 bundle hash shape", got)
	}
	legacy, err := BundleFingerprint(bundle)
	if err != nil {
		t.Fatalf("BundleFingerprint: %v", err)
	}
	if legacy == got || strings.HasPrefix(legacy, bundleHashV1Prefix) {
		t.Fatalf("legacy fingerprint must not equal canonical bundle_hash: legacy=%q canonical=%q", legacy, got)
	}
}

func TestBundleHashV1EquivalentYAMLAndPromptLineEndings(t *testing.T) {
	rootA, platformA := writeEquivalentBundleHashFixture(t, "\r\n", "name: equivalent\r\nversion: \"1.0.0\"\r\nflows: []\r\n")
	rootB, platformB := writeEquivalentBundleHashFixture(t, "\n", "flows: []\nversion: \"1.0.0\"\nname: equivalent\n")

	hashA, err := BundleHash(bundleHashTestBundle(rootA, platformA))
	if err != nil {
		t.Fatalf("BundleHash A: %v", err)
	}
	hashB, err := BundleHash(bundleHashTestBundle(rootB, platformB))
	if err != nil {
		t.Fatalf("BundleHash B: %v", err)
	}
	if hashA != hashB {
		t.Fatalf("equivalent bundle hashes drifted:\nA=%s\nB=%s", hashA, hashB)
	}
}

func TestBundleHashV1AcceptsCurrentPlatformSpec(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := t.TempDir()
	writeBundleHashText(t, filepath.Join(root, "package.yaml"), "name: current-platform\nversion: \"1.0.0\"\nflows: []\n")

	got, err := BundleHash(bundleHashTestBundle(root, DefaultPlatformSpecFile(repo)))
	if err != nil {
		t.Fatalf("BundleHash with current platform spec: %v", err)
	}
	if !regexp.MustCompile(`^bundle-v1:sha256:[a-f0-9]{64}$`).MatchString(got) {
		t.Fatalf("BundleHash = %q, want v1 bundle hash shape", got)
	}
}

func TestBundleHashV1IncludesPolicyModuleBytes(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := t.TempDir()
	writeBundleHashText(t, filepath.Join(root, "package.yaml"), "name: module-bytes\nversion: \"1.0.0\"\nflows: []\n")
	modulePath := filepath.Join(root, "modules", "structured_renderer.wasm")
	raw, err := os.ReadFile(filepath.Join("..", "computemodule", "testdata", "structured_renderer.wasm"))
	if err != nil {
		t.Fatal(err)
	}
	writeBundleHashBytes(t, modulePath, raw)
	sum := sha256.Sum256(raw)
	module := PolicyModule{
		Path:   "modules/structured_renderer.wasm",
		ABI:    "core-json-v1",
		Entry:  "compute",
		Digest: "sha256:" + hex.EncodeToString(sum[:]),
		InputSchema: map[string]any{
			"type":       "object",
			"properties": map[string]any{"component": map[string]any{"type": "string"}},
		},
		OutputSchema: map[string]any{
			"type":       "object",
			"properties": map[string]any{"content": map[string]any{"type": "string"}},
		},
		Limits: PolicyModuleLimits{Gas: 1, MemoryPages: 17, OutputBytes: 64},
	}
	flow := FlowContractView{
		Paths: FlowContractPaths{ID: "render", Flow: "render"},
		Policy: PolicyDocument{Modules: map[string]PolicyModule{
			"structured_renderer": module,
		}},
	}
	bundle := bundleHashTestBundle(root, DefaultPlatformSpecFile(repo))
	bundle.FlowTree = FlowTree{
		Root: &flow,
		ByID: map[string]*FlowContractView{
			"render": &flow,
		},
	}
	before, err := BundleHash(bundle)
	if err != nil {
		t.Fatalf("BundleHash before: %v", err)
	}
	raw[len(raw)-1] ^= 0xff
	writeBundleHashBytes(t, modulePath, raw)
	after, err := BundleHash(bundle)
	if err != nil {
		t.Fatalf("BundleHash after: %v", err)
	}
	if before == after {
		t.Fatalf("BundleHash did not change after module byte change: %s", before)
	}
}

func TestBundleCatalogProjectionUsesCanonicalInputsWithoutHostPaths(t *testing.T) {
	root, platform := writeEquivalentBundleHashFixture(t, "\n", `name: projection
version: "1.0.0"
keywords:
  - dedup-index
  - catalog
license: MIT
repository: https://github.com/division-sh/swarm
extra:
  colony.division.sh/display_name: Projection Fixture
flows:
  - id: alpha
    flow: alpha
`)
	writeBundleHashText(t, filepath.Join(root, "flows", "alpha", "agents.yaml"), `
agents:
  reviewer:
    role: review
`)
	writeBundleHashBytes(t, filepath.Join(root, "flows", "alpha", "data", "payload.bin"), []byte{0x01, 0x02, 0x03})
	bundle := bundleHashTestBundle(root, platform)
	bundle.Package = ProjectPackageDocument{
		Name:            "projection",
		Version:         "1.0.0",
		PlatformVersion: ">=0.7.0 <0.8.0",
		Keywords:        []string{"dedup-index", "catalog"},
		License:         "MIT",
		Repository:      "https://github.com/division-sh/swarm",
		Extra: map[string]string{
			"colony.division.sh/display_name": "Projection Fixture",
		},
	}
	bundle.Semantics.Name = "projection"
	bundle.Semantics.Version = "1.0.0"
	bundle.Agents = map[string]AgentRegistryEntry{
		"reviewer": {
			Role:             "review",
			Type:             "managed",
			Model:            "cheap",
			PromptRef:        "flows/alpha/prompts/reviewer.md",
			Subscriptions:    []string{"scan.requested"},
			Tools:            []string{"web_search"},
			ConversationMode: "session",
		},
	}

	projection, err := BuildBundleCatalogProjection(bundle)
	if err != nil {
		t.Fatalf("BuildBundleCatalogProjection: %v", err)
	}
	if !regexp.MustCompile(`^bundle-v1:sha256:[a-f0-9]{64}$`).MatchString(projection.BundleHash) {
		t.Fatalf("BundleHash = %q, want v1 shape", projection.BundleHash)
	}
	if strings.Contains(projection.ContentYAML, root) || strings.Contains(projection.ContentYAML, platform) {
		t.Fatalf("ContentYAML leaked host path:\n%s", projection.ContentYAML)
	}
	if !strings.Contains(projection.ContentYAML, `label: "bundle/package.yaml"`) {
		t.Fatalf("ContentYAML missing package label:\n%s", projection.ContentYAML)
	}
	canonicalPackage, err := canonicalBundleHashContent(filepath.Join(root, "package.yaml"), bundleHashYAML)
	if err != nil {
		t.Fatalf("canonical package: %v", err)
	}
	if !strings.Contains(projection.ContentYAML, base64.StdEncoding.EncodeToString(canonicalPackage)) {
		t.Fatalf("ContentYAML missing base64 package bytes:\n%s", projection.ContentYAML)
	}
	var data struct {
		Entries []struct {
			Label         string `json:"label"`
			ContentBase64 string `json:"content_base64"`
		} `json:"entries"`
	}
	if err := json.Unmarshal(projection.DataBlob, &data); err != nil {
		t.Fatalf("decode DataBlob: %v", err)
	}
	if len(data.Entries) != 1 || data.Entries[0].Label != "bundle/flows/alpha/data/payload.bin" || data.Entries[0].ContentBase64 != base64.StdEncoding.EncodeToString([]byte{0x01, 0x02, 0x03}) {
		t.Fatalf("data blob = %#v", data)
	}
	agents, ok := projection.ParsedJSON["agents"].(map[string]any)
	if !ok {
		t.Fatalf("agents projection = %#v", projection.ParsedJSON["agents"])
	}
	reviewer, ok := agents["reviewer"].(map[string]any)
	if !ok {
		t.Fatalf("reviewer projection = %#v", agents["reviewer"])
	}
	if _, hasRuntimeState := reviewer["runtime_state"]; hasRuntimeState {
		t.Fatalf("agents projection contains runtime state: %#v", reviewer)
	}
	pkg, ok := projection.ParsedJSON["package"].(map[string]any)
	if !ok {
		t.Fatalf("package projection = %#v", projection.ParsedJSON["package"])
	}
	if got, want := pkg["license"], "MIT"; got != want {
		t.Fatalf("package license = %#v, want %q", got, want)
	}
	if got, want := pkg["repository"], "https://github.com/division-sh/swarm"; got != want {
		t.Fatalf("package repository = %#v, want %q", got, want)
	}
	if got, want := pkg["keywords"], []string{"dedup-index", "catalog"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("package keywords = %#v, want %#v", got, want)
	}
	if got, want := pkg["extra"], map[string]string{"colony.division.sh/display_name": "Projection Fixture"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("package extra = %#v, want %#v", got, want)
	}
	if projection.Metadata["projection_version"] != bundleCatalogProjectionVersion || projection.Metadata["source"] != "swarm serve --contracts" {
		t.Fatalf("metadata = %#v", projection.Metadata)
	}
	if got, want := projection.Metadata["package_license"], "MIT"; got != want {
		t.Fatalf("metadata package_license = %#v, want %q", got, want)
	}
	if got, want := projection.Metadata["package_extra"], map[string]string{"colony.division.sh/display_name": "Projection Fixture"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("metadata package_extra = %#v, want %#v", got, want)
	}
}

func TestBundleCatalogProjectionStableForEquivalentCanonicalContent(t *testing.T) {
	rootA, platformA := writeEquivalentBundleHashFixture(t, "\r\n", "name: equivalent\r\nversion: \"1.0.0\"\r\nflows: []\r\n")
	rootB, platformB := writeEquivalentBundleHashFixture(t, "\n", "flows: []\nversion: \"1.0.0\"\nname: equivalent\n")

	projectionA, err := BuildBundleCatalogProjection(bundleHashTestBundle(rootA, platformA))
	if err != nil {
		t.Fatalf("BuildBundleCatalogProjection A: %v", err)
	}
	projectionB, err := BuildBundleCatalogProjection(bundleHashTestBundle(rootB, platformB))
	if err != nil {
		t.Fatalf("BuildBundleCatalogProjection B: %v", err)
	}
	if projectionA.BundleHash != projectionB.BundleHash {
		t.Fatalf("equivalent hashes drifted: A=%s B=%s", projectionA.BundleHash, projectionB.BundleHash)
	}
	if projectionA.ContentYAML != projectionB.ContentYAML {
		t.Fatalf("equivalent content_yaml projections drifted:\nA=%s\nB=%s", projectionA.ContentYAML, projectionB.ContentYAML)
	}
}

func TestBundleCatalogRuntimeLoaderReconstructsConfigAndData(t *testing.T) {
	repo := repoRootForContractsTest(t)
	contractsRoot := filepath.Join(repo, "tests", "tier12-runtime-tools", "test-flow-data-access")
	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, contractsRoot, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	projection, err := BuildBundleCatalogProjection(bundle)
	if err != nil {
		t.Fatalf("BuildBundleCatalogProjection: %v", err)
	}

	loaded, err := LoadBundleCatalogRuntimeSource(repo, BundleCatalogRuntimeLoadRequest{
		BundleHash:  projection.BundleHash,
		ContentYAML: projection.ContentYAML,
		DataBlob:    projection.DataBlob,
	})
	if err != nil {
		t.Fatalf("LoadBundleCatalogRuntimeSource: %v", err)
	}
	defer loaded.Cleanup()

	if loaded.BundleHash != projection.BundleHash {
		t.Fatalf("loaded hash = %q, want %q", loaded.BundleHash, projection.BundleHash)
	}
	gotHash, err := BundleHash(loaded.Bundle)
	if err != nil {
		t.Fatalf("BundleHash(loaded): %v", err)
	}
	if gotHash != projection.BundleHash {
		t.Fatalf("loaded BundleHash = %q, want %q", gotHash, projection.BundleHash)
	}
	if strings.Contains(loaded.ContractsRoot, contractsRoot) || strings.Contains(loaded.PlatformSpecPath, contractsRoot) {
		t.Fatalf("loaded runtime source leaked original contracts path: root=%s platform=%s", loaded.ContractsRoot, loaded.PlatformSpecPath)
	}
	dataPath := filepath.Join(loaded.ContractsRoot, "flows", "support", "data", "exclusions.yaml")
	data, err := os.ReadFile(dataPath)
	if err != nil {
		t.Fatalf("read reconstructed data file: %v", err)
	}
	if !bytes.Contains(data, []byte("unmanaged-host-file-reads")) {
		t.Fatalf("reconstructed data file = %q", string(data))
	}
	if _, err := os.Stat(filepath.Join(loaded.ContractsRoot, "expected.yaml")); !os.IsNotExist(err) {
		t.Fatalf("non-canonical fixture file was materialized, err=%v", err)
	}
	for _, check := range []struct {
		path string
		mode os.FileMode
	}{
		{path: filepath.Dir(loaded.ContractsRoot), mode: 0o755},
		{path: loaded.ContractsRoot, mode: 0o755},
		{path: dataPath, mode: 0o644},
	} {
		info, err := os.Stat(check.path)
		if err != nil {
			t.Fatalf("stat reconstructed source path %s: %v", check.path, err)
		}
		if got := info.Mode().Perm(); got != check.mode {
			t.Fatalf("mode for %s = %o, want %o", check.path, got, check.mode)
		}
	}
}

func TestBundleCatalogRuntimeLoaderFailsClosedForMissingDataOrHashMismatch(t *testing.T) {
	repo := repoRootForContractsTest(t)
	contractsRoot := filepath.Join(repo, "tests", "tier12-runtime-tools", "test-flow-data-access")
	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, contractsRoot, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	projection, err := BuildBundleCatalogProjection(bundle)
	if err != nil {
		t.Fatalf("BuildBundleCatalogProjection: %v", err)
	}

	_, err = LoadBundleCatalogRuntimeSource(repo, BundleCatalogRuntimeLoadRequest{
		BundleHash:  projection.BundleHash,
		ContentYAML: projection.ContentYAML,
	})
	if err == nil || !strings.Contains(err.Error(), "missing canonical input") {
		t.Fatalf("missing data_blob error = %v, want missing canonical input", err)
	}

	_, err = LoadBundleCatalogRuntimeSource(repo, BundleCatalogRuntimeLoadRequest{
		BundleHash:  "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ContentYAML: projection.ContentYAML,
		DataBlob:    projection.DataBlob,
	})
	if err == nil || !strings.Contains(err.Error(), "hash mismatch") {
		t.Fatalf("hash mismatch error = %v, want hash mismatch", err)
	}
}

func TestBundleCatalogRuntimeLoaderRejectsUnknownPackageManifestField(t *testing.T) {
	repo := repoRootForContractsTest(t)
	contractsRoot := filepath.Join(repo, "tests", "tier12-runtime-tools", "test-flow-data-access")
	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, contractsRoot, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	projection, err := BuildBundleCatalogProjection(bundle)
	if err != nil {
		t.Fatalf("BuildBundleCatalogProjection: %v", err)
	}

	contentYAML := rewriteBundleCatalogPackageYAML(t, projection.ContentYAML, `"name":"test-flow-data-access"`, `"homepage":"https://division.sh","name":"test-flow-data-access"`)
	_, err = LoadBundleCatalogRuntimeSource(repo, BundleCatalogRuntimeLoadRequest{
		BundleHash:  projection.BundleHash,
		ContentYAML: contentYAML,
		DataBlob:    projection.DataBlob,
	})
	diagnostic, ok := AsLoaderDiagnostic(err)
	if !ok || diagnostic.Code != "contract_loader.undefined_field" || !strings.Contains(diagnostic.Problem, "homepage") {
		t.Fatalf("LoadBundleCatalogRuntimeSource error = %v, want homepage loader diagnostic", err)
	}
}

func TestBundleHashV1PromptPreservesTrailingWhitespace(t *testing.T) {
	rootA, platformA := writeEquivalentBundleHashFixture(t, "\n", "name: prompt-space\nversion: \"1.0.0\"\nflows: []\n")
	rootB, platformB := writeEquivalentBundleHashFixture(t, "\n", "name: prompt-space\nversion: \"1.0.0\"\nflows: []\n")
	writeBundleHashText(t, filepath.Join(rootA, "prompts", "guide.md"), "same line\n")
	writeBundleHashText(t, filepath.Join(rootB, "prompts", "guide.md"), "same line  \n")

	hashA, err := BundleHash(bundleHashTestBundle(rootA, platformA))
	if err != nil {
		t.Fatalf("BundleHash A: %v", err)
	}
	hashB, err := BundleHash(bundleHashTestBundle(rootB, platformB))
	if err != nil {
		t.Fatalf("BundleHash B: %v", err)
	}
	if hashA == hashB {
		t.Fatalf("prompt trailing spaces were not preserved: %s", hashA)
	}
}

func TestBundleHashV1RawDataAndIgnoredFiles(t *testing.T) {
	root, platform := writeEquivalentBundleHashFixture(t, "\n", "name: raw-data\nversion: \"1.0.0\"\nflows:\n  - id: alpha\n    flow: alpha\n")
	writeBundleHashText(t, filepath.Join(root, "flows", "alpha", "schema.yaml"), "name: alpha\n")
	writeBundleHashBytes(t, filepath.Join(root, "flows", "alpha", "data", "payload.bin"), []byte{0xff, 0x00, 0xfe})

	before, err := BundleHash(bundleHashTestBundle(root, platform))
	if err != nil {
		t.Fatalf("BundleHash before ignored files: %v", err)
	}
	writeBundleHashText(t, filepath.Join(root, "prompts", ".DS_Store"), "ignored prompt junk")
	writeBundleHashText(t, filepath.Join(root, "prompts", "__pycache__", "ignored.pyc"), "ignored dir junk")
	writeBundleHashBytes(t, filepath.Join(root, "flows", "alpha", "data", "payload.tmp"), []byte("ignored data junk"))
	after, err := BundleHash(bundleHashTestBundle(root, platform))
	if err != nil {
		t.Fatalf("BundleHash after ignored files: %v", err)
	}
	if before != after {
		t.Fatalf("ignored files changed bundle hash: before=%s after=%s", before, after)
	}

	writeBundleHashBytes(t, filepath.Join(root, "prompts", "invalid.md"), []byte{0xff})
	if _, err := BundleHash(bundleHashTestBundle(root, platform)); err == nil || !strings.Contains(err.Error(), "valid UTF-8") {
		t.Fatalf("BundleHash invalid prompt error = %v, want UTF-8 failure", err)
	}
}

func TestBundleHashV1RejectsSymlinksWhenSupported(t *testing.T) {
	root, platform := writeEquivalentBundleHashFixture(t, "\n", "name: symlink\nversion: \"1.0.0\"\nflows: []\n")
	target := filepath.Join(root, "prompts", "target.md")
	link := filepath.Join(root, "prompts", "link.md")
	writeBundleHashText(t, target, "target\n")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink creation unsupported: %v", err)
	}
	if _, err := BundleHash(bundleHashTestBundle(root, platform)); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("BundleHash symlink error = %v, want symlink rejection", err)
	}
}

func TestBundleCatalogProjectionConsumesCanonicalBundleHashOwner(t *testing.T) {
	root, platform := writeEquivalentBundleHashFixture(t, "\n", "name: projected\nversion: \"1.0.0\"\nflows: []\n")
	writeBundleHashText(t, filepath.Join(root, "agents.yaml"), `
researcher:
  id: researcher
  role: research
  model: regular
  mode: task
  subscriptions:
    - scan.requested
`)
	bundle, err := LoadWorkflowContractBundleWithOverrides(filepath.Dir(root), root, platform)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	wantHash, err := BundleHash(bundle)
	if err != nil {
		t.Fatalf("BundleHash: %v", err)
	}
	projection, err := BuildBundleCatalogProjectionWithOptions(bundle, BundleCatalogProjectionOptions{
		Source:             "bundle.register",
		PlatformSpecSHA256: strings.Repeat("a", 64),
	})
	if err != nil {
		t.Fatalf("BuildBundleCatalogProjectionWithOptions: %v", err)
	}
	if projection.BundleHash != wantHash {
		t.Fatalf("projection bundle hash = %q, want %q", projection.BundleHash, wantHash)
	}
	if projection.Metadata["source"] != "bundle.register" || projection.Metadata["platform_spec_sha256"] != strings.Repeat("a", 64) {
		t.Fatalf("projection metadata = %#v", projection.Metadata)
	}
	agents := projection.ParsedJSON["agents"].(map[string]any)
	researcher := agents["researcher"].(map[string]any)
	if researcher["model"] != "regular" || researcher["mode"] != "task" {
		t.Fatalf("projected researcher = %#v", researcher)
	}
	if _, ok := researcher["status"]; ok {
		t.Fatalf("projection leaked runtime status: %#v", researcher)
	}
	if !strings.Contains(projection.ContentYAML, "bundle/agents.yaml") || !strings.Contains(projection.ContentYAML, "platform/platform-spec.yaml") {
		t.Fatalf("projection content_yaml missing canonical inputs:\n%s", projection.ContentYAML)
	}
}

func TestBundleHashV1YAMLProfile(t *testing.T) {
	equivalentA, err := canonicalBundleHashYAML([]byte(`
value: &num !!int 1
copy: *num
text: !!str true
ref:
  $ref: ./schema.yaml
`))
	if err != nil {
		t.Fatalf("canonical yaml A: %v", err)
	}
	equivalentB, err := canonicalBundleHashYAML([]byte(`
copy: 1.0
ref: {$ref: ./schema.yaml}
text: "true"
value: 1e0
`))
	if err != nil {
		t.Fatalf("canonical yaml B: %v", err)
	}
	if !bytes.Equal(equivalentA, equivalentB) {
		t.Fatalf("equivalent YAML canonicalization drifted:\nA=%s\nB=%s", equivalentA, equivalentB)
	}
	refLiteral, err := canonicalBundleHashYAML([]byte(`ref: {$ref: ./schema.yaml}`))
	if err != nil {
		t.Fatalf("canonical ref literal: %v", err)
	}
	refInlined, err := canonicalBundleHashYAML([]byte(`ref: {type: object}`))
	if err != nil {
		t.Fatalf("canonical ref inline: %v", err)
	}
	if bytes.Equal(refLiteral, refInlined) {
		t.Fatal("$ref literal and inlined content canonicalized identically")
	}
	numbers, err := canonicalBundleHashYAML([]byte("big: 1.23e6\nsmall: 1e-6\nsmaller: 1e-7\n"))
	if err != nil {
		t.Fatalf("number canonicalization: %v", err)
	}
	if !bytes.Equal(numbers, []byte(`{"big":1230000,"small":0.000001,"smaller":1e-7}`)) {
		t.Fatalf("number canonicalization = %s, want JCS fixed/exponent thresholds", numbers)
	}
	if got := formatBundleHashJSONNumber(1e21); got != "1e+21" {
		t.Fatalf("positive exponent canonicalization = %s, want JCS positive exponent sign", got)
	}
	explicitString, err := canonicalBundleHashYAML([]byte(`text: !!str "true"`))
	if err != nil {
		t.Fatalf("explicit string tag on quoted scalar: %v", err)
	}
	if !bytes.Equal(explicitString, []byte(`{"text":"true"}`)) {
		t.Fatalf("explicit string canonicalization = %s, want quoted string", explicitString)
	}

	cases := []struct {
		name     string
		yaml     string
		contains string
	}{
		{name: "duplicate key", yaml: "a: 1\na: 2\n", contains: "duplicate"},
		{name: "unsupported tag", yaml: "a: !custom value\n", contains: "unsupported"},
		{name: "negative zero", yaml: "a: -0\n", contains: "negative zero"},
		{name: "non finite number", yaml: "a: .nan\n", contains: "non-finite"},
		{name: "non string key", yaml: "true: value\n", contains: "not a string"},
		{name: "multi document", yaml: "a: 1\n---\nb: 2\n", contains: "multiple documents"},
		{name: "explicit bool quoted", yaml: `a: !!bool "true"` + "\n", contains: "widens quoted"},
		{name: "explicit int quoted", yaml: `a: !!int "1"` + "\n", contains: "widens quoted"},
		{name: "explicit null quoted", yaml: `a: !!null "null"` + "\n", contains: "widens quoted"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := canonicalBundleHashYAML([]byte(tc.yaml))
			if err == nil || !strings.Contains(err.Error(), tc.contains) {
				t.Fatalf("canonicalBundleHashYAML error = %v, want containing %q", err, tc.contains)
			}
		})
	}
}

func TestBundleHashV1LabelValidation(t *testing.T) {
	builder := &bundleHashEntryBuilder{
		seenPaths:    map[string]struct{}{},
		labels:       map[string]string{},
		foldedLabels: map[string]string{},
	}
	if err := builder.addEntry("/tmp/A", "bundle/A.md", bundleHashPrompt); err != nil {
		t.Fatalf("add entry A: %v", err)
	}
	if err := builder.addEntry("/tmp/B", "bundle/A.md", bundleHashPrompt); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("duplicate label error = %v, want duplicate", err)
	}
	builder = &bundleHashEntryBuilder{seenPaths: map[string]struct{}{}, labels: map[string]string{}, foldedLabels: map[string]string{}}
	if err := builder.addEntry("/tmp/A", "bundle/A.md", bundleHashPrompt); err != nil {
		t.Fatalf("add entry A: %v", err)
	}
	if err := builder.addEntry("/tmp/a", "bundle/a.md", bundleHashPrompt); err == nil || !strings.Contains(err.Error(), "case-colliding") {
		t.Fatalf("case collision error = %v, want case collision", err)
	}
	if err := validateBundleHashLabel("bundle/cafe\u0301.md"); err == nil || !strings.Contains(err.Error(), "NFC") {
		t.Fatalf("NFC label error = %v, want NFC rejection", err)
	}
}

func writeEquivalentBundleHashFixture(t *testing.T, lineEnding, packageYAML string) (string, string) {
	t.Helper()
	root := t.TempDir()
	platform := DefaultPlatformSpecFile(t.TempDir())
	writeBundleHashText(t, platform, strings.ReplaceAll("platform:\n  name: swarm\n  version: 0.7.0\napi_specification:\n  alpha: true\n  number: 1\n", "\n", lineEnding))
	if !strings.Contains(packageYAML, "platform_version:") {
		packageYAML = strings.Replace(packageYAML, "version: \"1.0.0\"", "version: \"1.0.0\"\nplatform_version: \">=0.7.0 <0.8.0\"", 1)
	}
	writeBundleHashText(t, filepath.Join(root, "package.yaml"), packageYAML)
	writeBundleHashText(t, filepath.Join(root, "schema.yaml"), strings.ReplaceAll("name: equivalent\n", "\n", lineEnding))
	writeBundleHashText(t, filepath.Join(root, "prompts", "guide.md"), strings.ReplaceAll("hello\nworld", "\n", lineEnding))
	return root, platform
}

func bundleHashTestBundle(root, platform string) *WorkflowContractBundle {
	return &WorkflowContractBundle{
		Paths: ResolveWorkflowContractPathsWithOverrides(filepath.Dir(root), root, platform),
	}
}

func writeBundleHashText(t *testing.T, path, contents string) {
	t.Helper()
	writeBundleHashBytes(t, path, []byte(contents))
}

func writeBundleHashBytes(t *testing.T, path string, contents []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, contents, 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
