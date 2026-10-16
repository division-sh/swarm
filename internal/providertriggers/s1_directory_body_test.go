package providertriggers

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/packmodel"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

func s1TriggerDirectoryMembers(rawName, normalizedName string) map[string][]byte {
	body := `payload_object_required: true
secret: {required: true}
signature: {type: token_equality, header: X-Signature}
delivery_id: {header: X-Delivery, required: true}
event_type: {literal: Issue.Opened-V2}
event_name: {` + rawName + `}
`
	if normalizedName != "" {
		body += `normalized_events:
  - event: ` + normalizedName + `
    fields:
      text: {from: message.text, schema: {type: string, minLength: 1}}
`
	}
	return map[string][]byte{
		"manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\ncapabilities: {can: {emit_events: [raw, text_message]}, cannot: [execute code]}\n"),
		"trigger.yaml":  []byte(body),
	}
}

func s1AdmittedTriggerBody(t testing.TB, key, rawName, normalizedName string) packartifact.ProviderBody {
	t.Helper()
	provider, err := packartifact.AdmitProviderMembers(key, s1TriggerDirectoryMembers(rawName, normalizedName))
	if err != nil {
		t.Fatal(err)
	}
	body, present := provider.Body(packmodel.BodyTrigger)
	if !present {
		t.Fatal("admitted provider lost trigger role")
	}
	return body
}

func TestS1TriggerDirectoryBodyQualifiesOnlyDeclaredEventNames(t *testing.T) {
	var firstHash string
	for _, key := range []string{"team-bot", "team_bot", "9-team"} {
		for _, rawName := range []string{"literal: raw", "template: '{event_type}'", "template: 'raw.{event_type}'"} {
			t.Run(key+"/"+rawName, func(t *testing.T) {
				normalizedName, wantEvents, wantCatalogEntries := "text_message", 2, 1
				if rawName == "template: '{event_type}'" {
					normalizedName, wantEvents, wantCatalogEntries = "", 1, 0
				} else if rawName == "literal: raw" {
					wantCatalogEntries = 2
				}
				body := s1AdmittedTriggerBody(t, key, rawName, normalizedName)
				manifest, err := AdmitBody(body)
				if err != nil {
					t.Fatalf("directory-owned short BODY refused: %v", err)
				}
				if manifest.Provider() != key || !bytes.Equal(manifest.SourceBytes(), body.Bytes()) {
					t.Fatal("BODY admission changed identity or original source bytes")
				}
				if rawName == "literal: raw" {
					if firstHash == "" {
						firstHash = body.Hash()
					} else if body.Hash() != firstHash {
						t.Fatal("directory identity contaminated body hash")
					}
				}
				catalog := manifest.EventCatalogEntries()
				if len(catalog) != wantCatalogEntries {
					t.Fatalf("typed catalog=%+v", catalog)
				}
				if _, present := catalog["inbound."+key+".text_message"]; present != (normalizedName != "") {
					t.Fatalf("typed catalog lost directory namespace: %+v", catalog)
				}
				request := Request{
					Provider: key, Target: Target{WebhookSecret: "fixture-secret"},
					Headers: http.Header{"X-Signature": {"fixture-secret"}, "X-Delivery": {"delivery-1"}},
					Payload: map[string]any{"message": map[string]any{"text": "inbound.telegram.text_message must stay payload text"}},
				}
				delivery, err := manifest.Accept(request)
				if err != nil || len(delivery.Events) != wantEvents {
					t.Fatalf("admitted BODY delivery=%+v err=%v", delivery, err)
				}
				wantRaw := "inbound." + key + ".raw"
				if strings.HasPrefix(rawName, "template:") {
					wantRaw = "inbound." + key + ".issue_opened_v2"
					if strings.Contains(rawName, "raw.") {
						wantRaw = "inbound." + key + ".raw.issue_opened_v2"
					}
				}
				if string(delivery.Events[0].Name) != wantRaw || delivery.Events[0].Kind != OutputKindRaw || !delivery.Events[0].Authorization.Empty() {
					t.Fatalf("raw output changed namespace or gained authority: %+v", delivery.Events[0])
				}
				if normalizedName != "" {
					normalized := delivery.Events[1]
					if string(normalized.Name) != "inbound."+key+".text_message" || normalized.Kind != OutputKindNormalized || normalized.Payload["text"] != request.Payload.(map[string]any)["message"].(map[string]any)["text"] {
						t.Fatalf("normalized names or payload changed: %+v", normalized)
					}
					if !normalized.Authorization.Empty() {
						t.Fatal("BODY presence minted catalog/publication authority")
					}
				}
				request.Headers.Set("X-Signature", "wrong")
				if _, err := manifest.Accept(request); err == nil {
					t.Fatal("short-name lowering bypassed signature admission")
				}
			})
		}
	}
}

func TestS1TriggerDirectoryBodyPreservesRawCollisionRefusal(t *testing.T) {
	for _, raw := range []string{"literal: text_message", "template: '{event_type}'"} {
		body := s1AdmittedTriggerBody(t, "team-bot", raw, "text_message")
		if _, err := AdmitBody(body); err == nil || !strings.Contains(err.Error(), "collides with the raw event-name policy") {
			t.Fatalf("qualified BODY weakened the raw collision guard: %v", err)
		}
	}
}

func TestS1TriggerDirectoryBodyRefusesQualifiedAndNoncanonicalNames(t *testing.T) {
	for _, raw := range []string{"'inbound.team-bot.raw'", "'inbound.foreign.raw'", "' raw'", "'raw '", "'foo .bar'", "'raw/foreign'", "'raw.*'"} {
		t.Run("raw/"+raw, func(t *testing.T) {
			body := s1AdmittedTriggerBody(t, "team-bot", "literal: "+raw, "text_message")
			if _, err := AdmitBody(body); err == nil || !strings.Contains(err.Error(), "team-bot/trigger.yaml:") || !strings.Contains(err.Error(), "short") {
				t.Fatalf("invalid raw name gained policy or lost teaching location: %v", err)
			}
		})
	}
	for _, name := range []string{"'inbound.team-bot.text_message'", "'inbound.foreign.text_message'", "' text_message'", "'text_message '", "'foo .bar'", "'text/message'", "'text.*'"} {
		t.Run("normalized/"+name, func(t *testing.T) {
			body := s1AdmittedTriggerBody(t, "team-bot", "literal: raw", name)
			if _, err := AdmitBody(body); err == nil || !strings.Contains(err.Error(), "team-bot/trigger.yaml:") || !strings.Contains(err.Error(), "short") {
				t.Fatalf("invalid normalized name gained policy or lost teaching location: %v", err)
			}
		})
	}
	for _, template := range []string{"inbound.team-bot.{event_type}", "inbound.foreign.{event_type}", "raw.{other}", "raw.{event_type}.suffix", "raw.{event_type} "} {
		t.Run("template/"+template, func(t *testing.T) {
			body := s1AdmittedTriggerBody(t, "team-bot", "template: '"+template+"'", "text_message")
			if _, err := AdmitBody(body); err == nil || !strings.Contains(err.Error(), "short") || !strings.Contains(err.Error(), "{event_type}") {
				t.Fatalf("invalid template gained policy or lost valid forms: %v", err)
			}
		})
	}
}

func TestS1TriggerDirectoryBodyRetainedAndScopedSourceUsesFrozenKey(t *testing.T) {
	members := map[string][]byte{"schema.yaml": []byte("description: root\n"), "child/schema.yaml": []byte("description: child\n")}
	for _, directory := range []string{"team-bot", "child/team_bot"} {
		for label, body := range s1TriggerDirectoryMembers("literal: raw", "text_message") {
			members[directory+"/"+label] = body
		}
	}
	source, err := sourceartifact.AdmitMembers(members)
	if err != nil {
		t.Fatal(err)
	}
	retained, err := sourceartifact.DecodeLogical(source.LogicalBlob())
	if err != nil {
		t.Fatal(err)
	}
	for _, artifact := range []*sourceartifact.AdmittedSourceArtifact{source, retained} {
		for _, directory := range []string{"team-bot", "child/team_bot"} {
			provider, err := packartifact.ProviderFromArtifact(artifact, directory)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := provider.Body(packmodel.BodyTrigger)
			manifest, err := AdmitBody(body)
			if err != nil || manifest.Provider() != provider.Key() || !bytes.Equal(manifest.SourceBytes(), body.Bytes()) {
				t.Fatalf("retained BODY lost frozen directory evidence: %s err=%v", directory, err)
			}
			schema, present := manifest.EventCatalogEntries()["inbound."+provider.Key()+".text_message"]
			encoded, err := json.Marshal(schema)
			if !present || err != nil || !bytes.Contains(encoded, []byte("text")) {
				t.Fatalf("retained typed catalog lost schema: %s err=%v", encoded, err)
			}
		}
	}
	if _, err := AdmitBody(packartifact.ProviderBody{}); err == nil {
		t.Fatal("zero BODY admitted trigger policy")
	}
}

func TestS1TriggerDirectoryBodyCopyRenameRetainsExactBytesAndPolicy(t *testing.T) {
	parent := t.TempDir()
	members := s1TriggerDirectoryMembers("literal: raw", "text_message")
	var captured []*sourceartifact.AdmittedSourceArtifact
	for _, key := range []string{"team-bot", "team_bot"} {
		root := filepath.Join(parent, key)
		if err := os.Mkdir(root, 0o700); err != nil {
			t.Fatal(err)
		}
		for label, body := range members {
			if err := os.WriteFile(filepath.Join(root, label), body, 0o600); err != nil {
				t.Fatal(err)
			}
		}
		artifact, err := sourceartifact.AdmitDirectory(root)
		if err != nil {
			t.Fatal(err)
		}
		captured = append(captured, artifact)
	}
	if captured[0].BundleHash() != captured[1].BundleHash() || !bytes.Equal(captured[0].LogicalBlob(), captured[1].LogicalBlob()) {
		t.Fatal("copy changed content identity instead of only directory identity")
	}
	if err := os.RemoveAll(parent); err != nil {
		t.Fatal(err)
	}
	var firstHash string
	for _, artifact := range captured {
		provider, err := packartifact.ProviderFromArtifact(artifact, ".")
		if err != nil {
			t.Fatal(err)
		}
		body, _ := provider.Body(packmodel.BodyTrigger)
		manifest, err := AdmitBody(body)
		if err != nil {
			t.Fatal(err)
		}
		if firstHash != "" && body.Hash() != firstHash {
			t.Fatal("namespace projection changed exact BODY hash")
		}
		firstHash = body.Hash()
		catalog, err := NewCatalogSnapshot(CatalogEntry{
			Manifest: manifest, Identity: PackIdentity{ID: provider.Key(), Version: provider.Metadata().Version, ManifestHash: body.Hash(), Provenance: "project"},
			Source: "directory-body-test", SourcePath: body.Label(),
		})
		if err != nil {
			t.Fatal(err)
		}
		plan, err := catalog.CompileAdmission(CompileAdmissionRequest{Alias: "shop", Provider: provider.Key(), SigningSecret: "fixture-signing-role"})
		if err != nil {
			t.Fatal(err)
		}
		request := Request{
			Provider: provider.Key(), Target: Target{WebhookSecret: "fixture-secret"},
			Headers: http.Header{"X-Signature": {"fixture-secret"}, "X-Delivery": {"delivery-1"}},
			Payload: map[string]any{"message": map[string]any{"text": "hello"}},
		}
		delivery, err := plan.Accept(request)
		if err != nil || len(delivery.Events) != 2 {
			t.Fatalf("captured copy cannot execute policy after source removal: %+v err=%v", delivery, err)
		}
		authority := delivery.Events[1].Authorization
		if !authority.Valid() || authority.Provider() != provider.Key() || authority.Event() != "inbound."+provider.Key()+".text_message" || authority.ManifestHash() != body.Hash() || authority.PackID() != provider.Key() {
			t.Fatalf("catalog seal disagrees with captured BODY: %+v", authority)
		}
		if err := catalog.VerifyProviderOutputAuthorization(authority); err != nil {
			t.Fatal(err)
		}
		request.Provider = "foreign"
		if _, err := plan.Accept(request); err == nil {
			t.Fatal("renamed BODY accepted foreign provider authority")
		}
		if !bytes.Equal(body.Bytes(), members["trigger.yaml"]) || !bytes.Equal(manifest.SourceBytes(), members["trigger.yaml"]) {
			t.Fatal("copy admission rewrote source bytes")
		}
	}
}

func TestS1TriggerDirectoryBodyHonorsLexicalAliasesAndRoleBoundary(t *testing.T) {
	for _, name := range []string{"&event text_message", "&event 'inbound.team-bot.text_message'"} {
		body := s1AdmittedTriggerBody(t, "team-bot", "literal: raw", name)
		manifest, err := AdmitBody(body)
		if strings.Contains(name, "inbound.") {
			if err == nil || !strings.Contains(err.Error(), `normalized_events`) || !strings.Contains(err.Error(), "short") {
				t.Fatalf("anchored qualified name escaped refusal: %v", err)
			}
		} else if err != nil || manifest.Provider() != "team-bot" {
			t.Fatalf("canonical anchored name refused: %v", err)
		}
	}
	members := s1TriggerDirectoryMembers("literal: raw", "text_message")
	members["trigger.yaml"] = bytes.Replace(members["trigger.yaml"], []byte("literal: raw"), []byte("literal: &raw raw"), 1)
	members["trigger.yaml"] = bytes.Replace(members["trigger.yaml"], []byte("event: text_message"), []byte("event: *raw"), 1)
	provider, err := packartifact.AdmitProviderMembers("team-bot", members)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := provider.Body(packmodel.BodyTrigger)
	if _, err := AdmitBody(body); err == nil || !strings.Contains(err.Error(), "collides") {
		t.Fatalf("alias hid a compiled-name collision: %v", err)
	}
	members["connector.yaml"] = []byte("{}\n")
	provider, err = packartifact.AdmitProviderMembers("team-bot", members)
	if err != nil {
		t.Fatal(err)
	}
	wrongRole, _ := provider.Body(packmodel.BodyConnector)
	if _, err := AdmitBody(wrongRole); err == nil {
		t.Fatal("connector role became a trigger policy")
	}
}
