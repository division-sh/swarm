package packartifact

import (
	"bytes"
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/division-sh/swarm/internal/packmodel"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

func s1ProviderMembers() map[string][]byte {
	return map[string][]byte{
		"manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\ncapabilities: {can: {emit_events: [inbound.team], persist_dedupe_markers: true}, cannot: [execute code]}\n"),
		"trigger.yaml":  []byte("event_name: {literal: inbound.team}\n"),
	}
}

func s1LoadProviderMembers(directory string, members map[string][]byte) (AdmittedProvider, error) {
	files := fstest.MapFS{}
	for name, body := range members {
		files[directory+"/"+name] = &fstest.MapFile{Data: body}
	}
	return LoadProviderFS(files, directory, testPlatformVersion)
}

func TestS1ProviderArtifactOwnsExactCapturedMembers(t *testing.T) {
	members := s1ProviderMembers()
	wantBody := append([]byte(nil), members["trigger.yaml"]...)
	provider, err := s1LoadProviderMembers("team-bot", members)
	if err != nil {
		t.Fatal(err)
	}
	if !provider.Valid() || provider.Key() != "team-bot" || provider.Metadata().Version != "1.0.0" || provider.Directory() != "team-bot" || provider.DeclaringFlow() != "" {
		t.Fatalf("wrong provider identity: %+v", provider)
	}
	if provider.source.Root() != nil {
		t.Fatal("standalone provider fabricated a flow")
	}
	if _, present := provider.source.RootAdmission(); present {
		t.Fatal("standalone provider fabricated runtime root authority")
	}
	body, present := provider.Body(packmodel.BodyTrigger)
	if !present || !body.Valid() || body.ProviderKey() != "team-bot" || body.Role() != packmodel.BodyTrigger || body.Label() != "trigger.yaml" || body.Metadata().Version != "1.0.0" {
		t.Fatalf("body lost its admitted parent: %+v", body)
	}
	wantHash := body.Hash()
	for _, member := range members {
		for i := range member {
			member[i] = 'x'
		}
	}
	readback := body.Bytes()
	readback[0] = 'x'
	metadata := body.Metadata()
	metadata.Version = "9.9.9"
	if !bytes.Equal(body.Bytes(), wantBody) || body.Hash() != wantHash || body.Metadata().Version != "1.0.0" || body.ProviderKey() != "team-bot" {
		t.Fatal("mutable caller changed captured provider bytes or identity")
	}
	if value := body.SourceValue(); value.Location().File != "trigger.yaml" {
		t.Fatalf("source evidence detached from admitted body: %s", value.Location())
	}
	if _, present := provider.Body(packmodel.BodyConnector); present {
		t.Fatal("missing role was fabricated")
	}
	if !bytes.Contains(provider.ManifestBody(), []byte("version: 1.0.0\n")) {
		t.Fatal("manifest source bytes were reconstructed from display metadata")
	}
	retained, err := sourceartifact.DecodeLogical(provider.source.LogicalBlob())
	if err != nil {
		t.Fatal(err)
	}
	retainedBody, present := retained.Entry("trigger.yaml")
	if !present || !bytes.Equal(retainedBody.Bytes(), wantBody) {
		t.Fatal("logical reconstruction changed exact provider bytes")
	}
	if reopened, err := ProviderFromArtifact(retained, "."); err == nil || reopened.Valid() {
		t.Fatal("content-only reconstruction invented the captured directory identity")
	}
}

func TestS1ProviderArtifactRefusesPathAliasesAndForeignSelection(t *testing.T) {
	for _, directory := range []string{"", "../team-bot", "/team-bot", "./team-bot", "team-bot/.", "TEAM-BOT"} {
		t.Run(directory, func(t *testing.T) {
			if _, err := s1LoadProviderMembers(directory, s1ProviderMembers()); err == nil {
				t.Fatalf("noncanonical or mismatched directory %q admitted", directory)
			}
		})
	}
	standalone := fstest.MapFS{
		"providers/team-bot/manifest.yaml": {Data: s1ProviderMembers()["manifest.yaml"]},
		"providers/team-bot/trigger.yaml":  {Data: s1ProviderMembers()["trigger.yaml"]},
		"providers/link":                   {Mode: fs.ModeSymlink, Data: []byte("team-bot")},
		"providers/file":                   {Data: []byte("not a directory")},
	}
	for _, directory := range []string{"providers/team_bot", "providers/TEAM-BOT", "providers/link", "providers/file"} {
		if provider, err := LoadProviderFS(standalone, directory, testPlatformVersion); err == nil || provider.Valid() {
			t.Fatalf("unobserved or non-directory selection %q admitted", directory)
		}
	}
	if provider, err := LoadProviderFS(nil, "team-bot", testPlatformVersion); err == nil || provider.Valid() {
		t.Fatal("missing reader produced a captured provider")
	}
	files := map[string][]byte{
		"manifest.yaml":       []byte("version: 1.0.0\nplatform_version: '*'\n"),
		"child/manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\n"),
		"schema.yaml":         []byte("description: root\n"),
	}
	for label, body := range s1ProviderMembers() {
		files["child/team-bot/"+label] = body
	}
	source, err := sourceartifact.AdmitMembers(files)
	if err != nil {
		t.Fatal(err)
	}
	provider, err := ProviderFromArtifact(source, "child/team-bot")
	if err != nil || provider.DeclaringFlow() != "child" {
		t.Fatalf("declaring scope changed: %+v, %v", provider, err)
	}
	for _, directory := range []string{"team-bot", "child/team_bot", "child/./team-bot", "foreign/team-bot"} {
		if _, err := ProviderFromArtifact(source, directory); err == nil {
			t.Fatalf("foreign/normalized provider %q selected", directory)
		}
	}
	if _, err := ProviderFromArtifact(nil, "team-bot"); err == nil {
		t.Fatal("nil source produced provider authority")
	}
	var body ProviderBody
	if body.Valid() || body.Hash() != "" || body.SourceValue().Location().File != "" || body.Bytes() != nil {
		t.Fatal("zero body produced admitted evidence")
	}
	validBody, _ := provider.Body(packmodel.BodyTrigger)
	foreignLabel := validBody
	foreignLabel.label = "foreign/trigger.yaml"
	missingRole := validBody
	missingRole.role = packmodel.BodyConnector
	for _, malformed := range []ProviderBody{foreignLabel, missingRole} {
		if malformed.Valid() || malformed.Hash() != "" || malformed.Bytes() != nil || malformed.ProviderKey() != "" {
			t.Fatal("contradictory body acquired admitted evidence")
		}
	}
}

func TestS1ProviderArtifactRefusesMalformedCapturedMembers(t *testing.T) {
	for _, aggregate := range []bool{false, true} {
		files := fstest.MapFS{}
		for name, body := range s1ProviderMembers() {
			files["team-bot/"+name] = &fstest.MapFile{Data: body}
		}
		if aggregate {
			body := make([]byte, sourceartifact.MaxMemberBytes)
			for _, name := range []string{"a.dat", "b.dat", "c.dat"} {
				files["team-bot/"+name] = &fstest.MapFile{Data: body}
			}
		} else {
			files["team-bot/large.dat"] = &fstest.MapFile{Data: make([]byte, sourceartifact.MaxMemberBytes+1)}
		}
		if provider, err := LoadProviderFS(files, "team-bot", testPlatformVersion); err == nil || provider.Valid() || !strings.Contains(err.Error(), "byte bound") {
			t.Fatalf("oversized provider (aggregate=%t) produced captured evidence: %v", aggregate, err)
		}
	}
	for _, bad := range []string{"provider: team-bot\n", "[]\n", "null\n"} {
		t.Run(bad, func(t *testing.T) {
			members := s1ProviderMembers()
			members["trigger.yaml"] = []byte(bad)
			if _, err := s1LoadProviderMembers("team-bot", members); err == nil || !strings.Contains(err.Error(), "trigger.yaml:") {
				t.Fatalf("captured member bypassed canonical admission: %v", err)
			}
		})
	}
	for _, label := range []string{"manifest.yaml", "trigger.yaml"} {
		files := fstest.MapFS{}
		for name, body := range s1ProviderMembers() {
			files["team-bot/"+name] = &fstest.MapFile{Data: body}
		}
		files["team-bot/"+label].Mode = fs.ModeSymlink
		if provider, err := LoadProviderFS(files, "team-bot", testPlatformVersion); err == nil || provider.Valid() || !strings.Contains(err.Error(), "regular file") {
			t.Fatalf("non-regular %s produced captured evidence: %v", label, err)
		}
	}
}
