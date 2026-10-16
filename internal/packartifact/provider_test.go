package packartifact

import (
	"bytes"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packmodel"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

func s1ProviderMembers() map[string][]byte {
	return map[string][]byte{
		"manifest.yaml": []byte("name: team-bot\nversion: 1.0.0\nplatform_version: '*'\ncapabilities: {can: {emit_events: [inbound.team], persist_dedupe_markers: true}, cannot: [execute code]}\n"),
		"trigger.yaml":  []byte("event_name: {literal: inbound.team}\n"),
	}
}

func TestS1ProviderArtifactOwnsExactCapturedMembers(t *testing.T) {
	members := s1ProviderMembers()
	wantBody := append([]byte(nil), members["trigger.yaml"]...)
	provider, err := AdmitProviderMembers("team-bot", members)
	if err != nil {
		t.Fatal(err)
	}
	if !provider.Valid() || provider.Metadata().Name != "team-bot" || provider.Directory() != "team-bot" || provider.DeclaringFlow() != "." {
		t.Fatalf("wrong provider identity: %+v", provider)
	}
	body, present := provider.Body(packmodel.BodyTrigger)
	if !present || !body.Valid() || body.Role() != packmodel.BodyTrigger || body.Label() != "team-bot/trigger.yaml" || body.Metadata().Name != "team-bot" {
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
	metadata.Name = "foreign"
	if !bytes.Equal(body.Bytes(), wantBody) || body.Hash() != wantHash || body.Metadata().Name != "team-bot" {
		t.Fatal("mutable caller changed captured provider bytes or identity")
	}
	if value := body.SourceValue(); value.Location().File != "team-bot/trigger.yaml" {
		t.Fatalf("source evidence detached from admitted body: %s", value.Location())
	}
	if _, present := provider.Body(packmodel.BodyConnector); present {
		t.Fatal("missing role was fabricated")
	}
	if !bytes.Contains(provider.ManifestBody(), []byte("name: team-bot\n")) {
		t.Fatal("manifest source bytes were reconstructed from display metadata")
	}
	retained, err := sourceartifact.DecodeLogical(provider.source.LogicalBlob())
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := ProviderFromArtifact(retained, "team-bot")
	if err != nil {
		t.Fatal(err)
	}
	reopenedBody, present := reopened.Body(packmodel.BodyTrigger)
	if !present || reopenedBody.Hash() != wantHash || !bytes.Equal(reopenedBody.Bytes(), wantBody) || reopenedBody.Metadata().Name != "team-bot" {
		t.Fatal("retained reconstruction changed provider ownership or exact bytes")
	}
}

func TestS1ProviderArtifactRefusesPathAliasesAndForeignSelection(t *testing.T) {
	for _, directory := range []string{"", "team_bot", "../team-bot", "/team-bot", "./team-bot", "team-bot/.", "TEAM-BOT"} {
		t.Run(directory, func(t *testing.T) {
			if _, err := AdmitProviderMembers(directory, s1ProviderMembers()); err == nil {
				t.Fatalf("noncanonical or mismatched directory %q admitted", directory)
			}
		})
	}
	files := map[string][]byte{"schema.yaml": []byte("description: root\n")}
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
		if malformed.Valid() || malformed.Hash() != "" || malformed.Bytes() != nil {
			t.Fatal("contradictory body acquired admitted evidence")
		}
	}
}

func TestS1ProviderArtifactRefusesMalformedCapturedMembers(t *testing.T) {
	for _, bad := range []string{"provider: team-bot\n", "[]\n", "null\n"} {
		t.Run(bad, func(t *testing.T) {
			members := s1ProviderMembers()
			members["trigger.yaml"] = []byte(bad)
			if _, err := AdmitProviderMembers("team-bot", members); err == nil || !strings.Contains(err.Error(), "trigger.yaml:") {
				t.Fatalf("captured member bypassed canonical admission: %v", err)
			}
		})
	}
}
