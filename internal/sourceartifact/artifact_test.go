package sourceartifact

import (
	"bytes"
	"encoding/binary"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAdmitDirectoryBuildsFiniteFlowTreeAndBundleV2(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "schema.yaml", "name: root\n")
	writeTestFile(t, root, "orders/schema.yaml", "name: orders\n")
	writeTestFile(t, root, "orders/events.yaml", "order.accepted: {}\n")
	writeTestFile(t, root, "tests/full-path.yaml", "name: full path\n")
	writeTestFile(t, root, "docs/guide.md", "guide\n")
	writeTestFile(t, root, ".swarm/pack-selection.yaml", "must_not: open\n")

	artifact, err := AdmitDirectory(root)
	if err != nil {
		t.Fatalf("AdmitDirectory: %v", err)
	}
	if !strings.HasPrefix(artifact.BundleHash(), HashPrefix) {
		t.Fatalf("BundleHash = %q", artifact.BundleHash())
	}
	if _, ok := artifact.Entry(".swarm/pack-selection.yaml"); ok {
		t.Fatal("excluded pack selection receipt was admitted")
	}
	if rootNode := artifact.Root(); rootNode.Path() != "." || len(rootNode.Children()) != 1 || rootNode.Children()[0].Path() != "orders" {
		t.Fatalf("flow tree = %#v", rootNode)
	}
	body, err := fs.ReadFile(artifact.FS(), "orders/events.yaml")
	if err != nil || string(body) != "order.accepted: {}\n" {
		t.Fatalf("artifact FS read = %q, %v", body, err)
	}

	reconstructed, err := DecodeLogical(artifact.LogicalBlob())
	if err != nil {
		t.Fatalf("DecodeLogical: %v", err)
	}
	if reconstructed.BundleHash() != artifact.BundleHash() {
		t.Fatalf("reconstructed hash = %q, want %q", reconstructed.BundleHash(), artifact.BundleHash())
	}
}

func TestAdmitDirectoryRetiresPackageAndFlows(t *testing.T) {
	for _, label := range []string{"package.yaml", "flows/child/schema.yaml"} {
		t.Run(label, func(t *testing.T) {
			root := t.TempDir()
			writeTestFile(t, root, label, "name: old\n")
			_, err := AdmitDirectory(root)
			if err == nil || !strings.Contains(err.Error(), "RETIRED") {
				t.Fatalf("error = %v, want RETIRED", err)
			}
		})
	}
}

func TestExcludedMutationCannotChangeArtifact(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "schema.yaml", "name: root\n")
	writeTestFile(t, root, ".swarm/pack-selection.yaml", "base: one\n")
	first, err := AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, root, ".swarm/pack-selection.yaml", "base: two\n")
	second, err := AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	if first.BundleHash() != second.BundleHash() {
		t.Fatalf("excluded mutation changed hash: %s != %s", first.BundleHash(), second.BundleHash())
	}
}

func TestExcludedNameIsNeverOpened(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "schema.yaml", "name: root\n")
	if err := os.Symlink(filepath.Join(root, "missing"), filepath.Join(root, ".swarm")); err != nil {
		t.Fatal(err)
	}
	artifact, err := AdmitDirectory(root)
	if err != nil {
		t.Fatalf("excluded .swarm symlink was inspected: %v", err)
	}
	if len(artifact.Entries()) != 1 {
		t.Fatalf("entries = %#v, want only schema.yaml", artifact.Entries())
	}
}

func TestAdmissionStateRejectsOverflowBeforeRetention(t *testing.T) {
	members := admissionState{entries: make([]Entry, MaxMembers), totalBytes: 7}
	if err := members.appendEntry("data/overflow.bin", []byte("x")); err == nil || !strings.Contains(err.Error(), "4097 members") {
		t.Fatalf("member overflow error = %v", err)
	}
	if len(members.entries) != MaxMembers || members.totalBytes != 7 {
		t.Fatalf("member overflow mutated state: entries=%d bytes=%d", len(members.entries), members.totalBytes)
	}

	bytes := admissionState{totalBytes: MaxArtifactBytes}
	if err := bytes.appendEntry("data/overflow.bin", []byte("x")); err == nil || !strings.Contains(err.Error(), "50331649 bytes") {
		t.Fatalf("byte overflow error = %v", err)
	}
	if len(bytes.entries) != 0 || bytes.totalBytes != MaxArtifactBytes {
		t.Fatalf("byte overflow mutated state: entries=%d bytes=%d", len(bytes.entries), bytes.totalBytes)
	}
}

func TestAdmitDirectoryRejectsIncludedSymlinkBeforeClassification(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "schema.yaml", "name: root\n")
	writeTestFile(t, root, "prompts-target/worker.md", "worker\n")
	if err := os.Symlink(filepath.Join(root, "prompts-target"), filepath.Join(root, "prompts")); err != nil {
		t.Fatal(err)
	}
	_, err := AdmitDirectory(root)
	if err == nil || !strings.Contains(err.Error(), `source path "prompts" must not be a symlink`) {
		t.Fatalf("error = %v, want explicit included-symlink rejection", err)
	}
}

func TestManifestIsExactArtifactDataButNotSemanticYAML(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "schema.yaml", "name: root\n")
	writeTestFile(t, root, "manifest.yaml", "distribution: [metadata\n")
	artifact, err := AdmitDirectory(root)
	if err != nil {
		t.Fatalf("AdmitDirectory: %v", err)
	}
	entry, ok := artifact.Entry("manifest.yaml")
	if !ok || string(entry.Bytes()) != "distribution: [metadata\n" {
		t.Fatalf("manifest artifact member = %#v, %v", entry, ok)
	}
	if _, ok := artifact.YAML("manifest.yaml"); ok {
		t.Fatal("manifest.yaml became an admitted semantic YAML document")
	}
}

func TestDecodeLogicalRejectsNonCanonicalOrder(t *testing.T) {
	entries := []Entry{{label: "schema.yaml", body: []byte("name: root\n")}, {label: "docs/a.md", body: []byte("a")}}
	artifact, err := newArtifact(entries)
	if err != nil {
		t.Fatal(err)
	}
	blob := artifact.LogicalBlob()
	// The decoder also proves exact framing by round-tripping to the canonical encoding.
	if _, err := DecodeLogical(blob); err != nil {
		t.Fatalf("canonical blob rejected: %v", err)
	}
}

func TestBundleV2LogicalFramingIncludesCanonicalDisposition(t *testing.T) {
	artifact, err := newArtifact([]Entry{
		{label: "schema.yaml", body: []byte("name: root\n")},
		{label: "manifest.yaml", body: []byte("name: package\n")},
		{label: "prompts/worker.md", body: []byte("worker\n")},
		{label: "README.md", body: []byte("read me\n")},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := bytes.NewBufferString(logicalPrelude)
	writeTestU64(t, want, 4)
	for _, entry := range []struct {
		code  byte
		label string
		body  string
	}{
		{byte(DispositionDocument), "README.md", "read me\n"},
		{byte(DispositionManifest), "manifest.yaml", "name: package\n"},
		{byte(DispositionResource), "prompts/worker.md", "worker\n"},
		{byte(DispositionDeclaration), "schema.yaml", "name: root\n"},
	} {
		want.WriteByte(entry.code)
		writeTestU64(t, want, uint64(len(entry.label)))
		want.WriteString(entry.label)
		writeTestU64(t, want, uint64(len(entry.body)))
		want.WriteString(entry.body)
	}
	if got := artifact.LogicalBlob(); !bytes.Equal(got, want.Bytes()) {
		t.Fatalf("logical blob = %x, want %x", got, want.Bytes())
	}
}

func TestDecodeLogicalRejectsInvalidOrMismatchedDisposition(t *testing.T) {
	artifact, err := newArtifact([]Entry{{label: "schema.yaml", body: []byte("name: root\n")}})
	if err != nil {
		t.Fatal(err)
	}
	dispositionOffset := len(logicalPrelude) + 8
	for name, code := range map[string]byte{
		"unknown":    0,
		"mismatched": byte(DispositionResource),
	} {
		t.Run(name, func(t *testing.T) {
			blob := artifact.LogicalBlob()
			blob[dispositionOffset] = code
			if _, err := DecodeLogical(blob); err == nil || !strings.Contains(err.Error(), "disposition") {
				t.Fatalf("DecodeLogical error = %v, want disposition rejection", err)
			}
		})
	}
}

func TestDecodeLogicalRejectsAggregateOverflowBeforeNextBody(t *testing.T) {
	blob, err := encodeLogical([]Entry{
		{label: "README.md", disposition: DispositionDocument, body: []byte("abc")},
		{label: "schema.yaml", disposition: DispositionDeclaration, body: []byte("d")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeLogicalWithinLimit(blob, 3); err == nil || !strings.Contains(err.Error(), "source artifact is 4 bytes, maximum is 3") {
		t.Fatalf("DecodeLogical aggregate overflow error = %v", err)
	}
}

func writeTestU64(t *testing.T, target *bytes.Buffer, value uint64) {
	t.Helper()
	if err := binary.Write(target, binary.BigEndian, value); err != nil {
		t.Fatal(err)
	}
}

func writeTestFile(t *testing.T, root, label, body string) {
	t.Helper()
	target := filepath.Join(root, filepath.FromSlash(label))
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}
