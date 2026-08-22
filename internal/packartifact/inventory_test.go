package packartifact

import (
	"bytes"
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"

	"gopkg.in/yaml.v3"
)

const testPlatformVersion = "0.7.0"

func TestLoadPlatformPackInventoryRejectsHostileMembership(t *testing.T) {
	valid := testInventoryFS(t, "provider.demo", TypeTrigger, "provider-triggers/demo", "platform", []byte("provider: demo\n"))
	tests := []struct {
		name   string
		mutate func(fstest.MapFS)
	}{
		{
			name: "missing inventory manifest",
			mutate: func(fsys fstest.MapFS) {
				delete(fsys, InventoryManifestFileName)
			},
		},
		{
			name: "malformed inventory manifest",
			mutate: func(fsys fstest.MapFS) {
				fsys[InventoryManifestFileName].Data = []byte("version: 1\npacks: [\n")
			},
		},
		{
			name: "inventory manifest has trailing document",
			mutate: func(fsys fstest.MapFS) {
				fsys[InventoryManifestFileName].Data = append(fsys[InventoryManifestFileName].Data, []byte("---\nversion: 1\n")...)
			},
		},
		{
			name: "missing listed body",
			mutate: func(fsys fstest.MapFS) {
				delete(fsys, "provider-triggers/demo/trigger.yaml")
			},
		},
		{
			name: "unlisted file",
			mutate: func(fsys fstest.MapFS) {
				fsys["provider-triggers/demo/notes.yaml"] = &fstest.MapFile{Data: []byte("hidden: authority\n")}
			},
		},
		{
			name: "duplicate identity",
			mutate: func(fsys fstest.MapFS) {
				fsys[InventoryManifestFileName] = &fstest.MapFile{Data: []byte("version: 1\npacks:\n  - id: provider.demo\n    type: trigger\n    path: provider-triggers/demo\n  - id: provider.demo\n    type: trigger\n    path: provider-triggers/demo\n")}
			},
		},
		{
			name: "duplicate path",
			mutate: func(fsys fstest.MapFS) {
				fsys[InventoryManifestFileName].Data = []byte("version: 1\npacks:\n  - id: provider.demo\n    type: trigger\n    path: provider-triggers/demo\n  - id: provider.other\n    type: trigger\n    path: provider-triggers/demo\n")
			},
		},
		{
			name: "traversal",
			mutate: func(fsys fstest.MapFS) {
				fsys[InventoryManifestFileName] = &fstest.MapFile{Data: []byte("version: 1\npacks:\n  - id: provider.demo\n    type: trigger\n    path: ../demo\n")}
			},
		},
		{
			name: "declared type contradiction",
			mutate: func(fsys fstest.MapFS) {
				fsys[InventoryManifestFileName] = &fstest.MapFile{Data: []byte("version: 1\npacks:\n  - id: provider.demo\n    type: connector\n    path: provider-triggers/demo\n")}
			},
		},
		{
			name: "missing envelope",
			mutate: func(fsys fstest.MapFS) {
				delete(fsys, "provider-triggers/demo/pack.yaml")
			},
		},
		{
			name: "malformed envelope",
			mutate: func(fsys fstest.MapFS) {
				fsys["provider-triggers/demo/pack.yaml"].Data = []byte("id: [\n")
			},
		},
		{
			name: "envelope has trailing document",
			mutate: func(fsys fstest.MapFS) {
				file := fsys["provider-triggers/demo/pack.yaml"]
				file.Data = append(file.Data, []byte("---\nid: provider.second\n")...)
			},
		},
		{
			name: "wrong provenance",
			mutate: func(fsys fstest.MapFS) {
				file := fsys["provider-triggers/demo/pack.yaml"]
				file.Data = []byte(strings.Replace(string(file.Data), "source: platform", "source: external", 1))
			},
		},
		{
			name: "incompatible platform",
			mutate: func(fsys fstest.MapFS) {
				file := fsys["provider-triggers/demo/pack.yaml"]
				file.Data = []byte(strings.Replace(string(file.Data), ">=0.7.0 <0.8.0", ">=99.0.0 <100.0.0", 1))
			},
		},
		{
			name: "body symlink",
			mutate: func(fsys fstest.MapFS) {
				fsys["provider-triggers/demo/trigger.yaml"].Mode = fs.ModeSymlink
			},
		},
		{
			name: "body non regular",
			mutate: func(fsys fstest.MapFS) {
				fsys["provider-triggers/demo/trigger.yaml"].Mode = fs.ModeDevice
			},
		},
		{
			name: "body hash mismatch",
			mutate: func(fsys fstest.MapFS) {
				fsys["provider-triggers/demo/trigger.yaml"] = &fstest.MapFile{Data: []byte("provider: changed\n")}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			candidate := cloneMapFS(valid)
			tc.mutate(candidate)
			if _, err := LoadPlatformPackInventoryFS(candidate, InventoryManifestFileName, testPlatformVersion, SelectionEmbedded); err == nil {
				t.Fatal("LoadPlatformPackInventoryFS succeeded for hostile inventory")
			}
		})
	}
}

func TestEffectivePackInventoryProjectShadowUsesCurrentBodyIdentity(t *testing.T) {
	baseFS := testInventoryFS(t, "provider.demo", TypeTrigger, "provider-triggers/demo", "platform", []byte("provider: demo\nrevision: embedded\n"))
	base, err := LoadPlatformPackInventoryFS(baseFS, InventoryManifestFileName, testPlatformVersion, SelectionEmbedded)
	if err != nil {
		t.Fatalf("LoadPlatformPackInventoryFS: %v", err)
	}
	baseEntry, ok := base.Lookup("provider.demo")
	if !ok {
		t.Fatal("embedded provider.demo missing")
	}

	projectBody := []byte("provider: demo\nrevision: project\n")
	projectEnvelope := baseEntry.Envelope()
	projectEnvelope.Provenance.Source = ProvenanceProject
	projectEnvelope.ManifestHash = ManifestHashDerived
	projectEnvelopeBody, err := yaml.Marshal(projectEnvelope)
	if err != nil {
		t.Fatalf("marshal project envelope: %v", err)
	}
	project := ProjectPackSource{
		Path:         "provider.demo",
		EnvelopeBody: projectEnvelopeBody,
		ManifestBody: projectBody,
		Origin: ImportOrigin{
			Source: ProvenanceEmbedded, ID: baseEntry.ID(), Version: baseEntry.Version(), ManifestHash: baseEntry.ManifestHash(),
			EnvelopeHash: importedEnvelopeHash(projectEnvelopeBody),
		},
	}

	effective, err := NewEffectivePackInventory(base, []ProjectPackSource{project})
	if err != nil {
		t.Fatalf("NewEffectivePackInventory: %v", err)
	}
	entry, ok := effective.Lookup("provider.demo")
	if !ok {
		t.Fatal("effective provider.demo missing")
	}
	if entry.Source() != ProvenanceProject {
		t.Fatalf("source = %q, want project", entry.Source())
	}
	if !entry.ShadowsBase() {
		t.Fatal("project entry does not report shadowed base")
	}
	if entry.ManifestHash() == baseEntry.ManifestHash() {
		t.Fatalf("project current manifest hash retained embedded digest %q", entry.ManifestHash())
	}
	if !bytes.Equal(entry.ManifestBody(), projectBody) {
		t.Fatalf("manifest body = %q, want %q", entry.ManifestBody(), projectBody)
	}
	if got := entry.Origin(); got != project.Origin {
		t.Fatalf("origin = %#v, want %#v", got, project.Origin)
	}
}

func TestProjectPackModifiedTracksEnvelopeAndBodyEdits(t *testing.T) {
	base, err := LoadPlatformPackInventoryFS(
		testInventoryFS(t, "provider.demo", TypeTrigger, "provider-triggers/demo", ProvenancePlatform, []byte("provider: demo\n")),
		InventoryManifestFileName,
		testPlatformVersion,
		SelectionEmbedded,
	)
	if err != nil {
		t.Fatal(err)
	}
	baseEntry, _ := base.Lookup("provider.demo")
	baselineEnvelope := baseEntry.Envelope()
	baselineEnvelope.Provenance.Source = ProvenanceProject
	baselineEnvelope.ManifestHash = ManifestHashDerived
	baselineEnvelopeBody, err := yaml.Marshal(baselineEnvelope)
	if err != nil {
		t.Fatal(err)
	}
	origin := ImportOrigin{
		Source: ProvenanceEmbedded, ID: baseEntry.ID(), Version: baseEntry.Version(), ManifestHash: baseEntry.ManifestHash(),
		EnvelopeHash: importedEnvelopeHash(baselineEnvelopeBody),
	}
	tests := []struct {
		name     string
		mutate   func(*Envelope, *[]byte)
		modified bool
	}{
		{name: "unchanged", mutate: func(*Envelope, *[]byte) {}, modified: false},
		{name: "version", mutate: func(envelope *Envelope, _ *[]byte) { envelope.Version = "0.1.1" }, modified: true},
		{name: "tests", mutate: func(envelope *Envelope, _ *[]byte) {
			envelope.Tests = append(append([]string(nil), envelope.Tests...), "providertriggers/edited")
		}, modified: true},
		{name: "body", mutate: func(_ *Envelope, body *[]byte) { *body = []byte("provider: demo\nrevision: edited\n") }, modified: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			envelope := baselineEnvelope
			body := baseEntry.ManifestBody()
			tc.mutate(&envelope, &body)
			envelopeBody, err := yaml.Marshal(envelope)
			if err != nil {
				t.Fatal(err)
			}
			effective, err := NewEffectivePackInventory(base, []ProjectPackSource{{
				Path: "provider.demo", EnvelopeBody: envelopeBody, ManifestBody: body, Origin: origin,
			}})
			if err != nil {
				t.Fatal(err)
			}
			entry, ok := effective.Lookup("provider.demo")
			if !ok || entry.Modified() != tc.modified {
				t.Fatalf("project edit = %#v present=%t modified=%t, want %t", entry, ok, entry.Modified(), tc.modified)
			}
		})
	}
}

func TestEffectivePackInventoryDigestBindsSelectedBase(t *testing.T) {
	firstFS := testInventoryFS(t, "provider.demo", TypeTrigger, "provider-triggers/demo", "platform", []byte("provider: demo\nrevision: one\n"))
	secondFS := testInventoryFS(t, "provider.demo", TypeTrigger, "provider-triggers/demo", "platform", []byte("provider: demo\nrevision: two\n"))
	first, err := LoadPlatformPackInventoryFS(firstFS, InventoryManifestFileName, testPlatformVersion, SelectionEmbedded)
	if err != nil {
		t.Fatalf("load first base: %v", err)
	}
	second, err := LoadPlatformPackInventoryFS(secondFS, InventoryManifestFileName, testPlatformVersion, SelectionEmbedded)
	if err != nil {
		t.Fatalf("load second base: %v", err)
	}
	firstEffective, err := NewEffectivePackInventory(first, nil)
	if err != nil {
		t.Fatalf("first effective inventory: %v", err)
	}
	secondEffective, err := NewEffectivePackInventory(second, nil)
	if err != nil {
		t.Fatalf("second effective inventory: %v", err)
	}
	if firstEffective.BaseDigest() == secondEffective.BaseDigest() {
		t.Fatalf("base digest did not change: %q", firstEffective.BaseDigest())
	}
	if firstEffective.Digest() == secondEffective.Digest() {
		t.Fatalf("effective digest did not bind selected base: %q", firstEffective.Digest())
	}
}

func TestEmbeddedPlatformPackInventoryOwnsEveryBodyKind(t *testing.T) {
	inventory, err := LoadEmbeddedPlatformPackInventory(testPlatformVersion)
	if err != nil {
		t.Fatalf("LoadEmbeddedPlatformPackInventory: %v", err)
	}
	want := map[string]string{
		"provider.telegram":              TypeTrigger,
		"provider.telegram.connector":    TypeConnector,
		"provider.telegram.hitl_channel": TypeChannel,
	}
	for id, packType := range want {
		entry, ok := inventory.Lookup(id)
		if !ok {
			t.Fatalf("embedded inventory missing %q", id)
		}
		if entry.Type() != packType {
			t.Fatalf("embedded %q type = %q, want %q", id, entry.Type(), packType)
		}
		if entry.Source() != ProvenanceEmbedded {
			t.Fatalf("embedded %q source = %q", id, entry.Source())
		}
	}
	if inventory.Digest() == "" {
		t.Fatal("embedded inventory digest is empty")
	}
}

func testInventoryFS(t *testing.T, id, packType, packPath, provenance string, manifestBody []byte) fstest.MapFS {
	t.Helper()
	envelope := Envelope{
		ID: id, Version: "0.1.0", PlatformVersion: ">=0.7.0 <0.8.0", Type: packType,
		Provenance:   Provenance{Source: provenance},
		Capabilities: testCapabilities(packType), Requires: Requires{}, Tests: []string{"inventory/test"},
	}
	stamped, envelopeBody, err := StampEnvelope(envelope, manifestBody)
	if err != nil {
		t.Fatalf("StampEnvelope: %v", err)
	}
	_ = stamped
	manifestFile := ManifestFileNameForType(packType)
	inventoryBody := []byte("version: 1\npacks:\n  - id: " + id + "\n    type: " + packType + "\n    path: " + packPath + "\n")
	return fstest.MapFS{
		InventoryManifestFileName:         &fstest.MapFile{Data: inventoryBody, Mode: fs.FileMode(0o644)},
		packPath + "/" + EnvelopeFileName: &fstest.MapFile{Data: envelopeBody, Mode: fs.FileMode(0o644)},
		packPath + "/" + manifestFile:     &fstest.MapFile{Data: manifestBody, Mode: fs.FileMode(0o644)},
	}
}

func testCapabilities(packType string) Capabilities {
	switch packType {
	case TypeTrigger:
		return Capabilities{Can: CanCapabilities{
			ReceiveHTTPSRoute: "/webhooks/{alias}/demo", EmitEvents: []string{"inbound.demo"}, PersistDedupeMarkers: true,
		}, Cannot: []string{"emit_undeclared_events"}}
	case TypeConnector:
		return Capabilities{Can: CanCapabilities{
			CallProviderActions: []string{"demo.send"}, LowerThroughActivity: true, JournalActivityAttempts: true,
		}, Cannot: []string{"bypass_activity_attempts"}}
	case TypeChannel:
		return Capabilities{Cannot: []string{"bypass_activity_attempts"}}
	default:
		return Capabilities{}
	}
}

func cloneMapFS(source fstest.MapFS) fstest.MapFS {
	out := make(fstest.MapFS, len(source))
	for name, file := range source {
		copied := *file
		copied.Data = append([]byte(nil), file.Data...)
		out[name] = &copied
	}
	return out
}
