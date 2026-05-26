package contracts

import (
	"bytes"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestBundleHashV1GoldenCorpus(t *testing.T) {
	root := t.TempDir()
	platform := filepath.Join(t.TempDir(), "platform-spec.yaml")
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
	platform := filepath.Join(t.TempDir(), "platform-spec.yaml")
	writeBundleHashText(t, platform, strings.ReplaceAll("api_specification:\n  alpha: true\n  number: 1\n", "\n", lineEnding))
	writeBundleHashText(t, filepath.Join(root, "package.yaml"), packageYAML)
	writeBundleHashText(t, filepath.Join(root, "schema.yaml"), strings.ReplaceAll("name: equivalent\nfields:\n  topic: string\n", "\n", lineEnding))
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
