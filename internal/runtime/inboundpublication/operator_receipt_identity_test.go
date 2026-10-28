package inboundpublication

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/packmodel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/division-sh/swarm/internal/yamlsource"
	"github.com/google/uuid"
)

func TestHTTPOperatorConstructorRequiresAuthenticatedDeliveryIdentity(t *testing.T) {
	snapshot, err := yamlsource.LoadFile(filepath.Join("..", "..", "..", "platform-spec.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	spec, err := contracts.AdmitPlatformSpecValue(snapshot.Document("platform-spec.yaml").Root())
	if err != nil {
		t.Fatal(err)
	}
	entry, found := packfixture.EmbeddedInventory(t).Lookup("telegram")
	if !found {
		t.Fatal("operator fixture requires its captured Telegram provider")
	}
	members := map[string][]byte{
		"manifest.yaml":                  []byte("version: 1.0.0\nplatform_version: '*'\n"),
		"schema.yaml":                    []byte("stages: []\n"),
		"ingress/manifest.yaml":          []byte("version: 1.0.0\nplatform_version: '*'\n"),
		"ingress/schema.yaml":            []byte("stages: []\npins:\n  inputs: [inbound.telegram.text_message]\n"),
		"ingress/telegram/manifest.yaml": entry.Provider().ManifestBody(),
	}
	for _, role := range []packmodel.BodyRole{packmodel.BodyTrigger, packmodel.BodyConnector, packmodel.BodyChannel} {
		body, present := entry.Provider().Body(role)
		if !present {
			t.Fatalf("operator fixture requires its captured %s BODY", role)
		}
		members["ingress/telegram/"+role.FileName()] = body.Bytes()
	}
	artifact, err := sourceartifact.AdmitMembers(members)
	if err != nil {
		t.Fatal(err)
	}
	rooted, _ := sourceartifactfixture.Rooted(t, "operator-receipt", artifact)
	captured, err := packartifact.ProviderFromRootedSource(rooted, "ingress/telegram")
	if err != nil {
		t.Fatal(err)
	}
	provider, err := packadmission.AdmitProviderDirectory(captured, spec)
	if err != nil {
		t.Fatal(err)
	}
	channel, found := provider.ChannelPlan()
	if !found {
		t.Fatal("operator fixture requires its admitted channel plan")
	}
	catalog, found := provider.TriggerCatalog()
	if !found {
		t.Fatal("operator fixture requires its matching trigger catalog")
	}
	trigger, err := catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{
		Alias: "telegram", Provider: "telegram", SigningSecret: "fixture_signing"})
	if err != nil {
		t.Fatal(err)
	}
	payload := map[string]any{"update_id": "delivery-1", "message": map[string]any{
		"message_id": 92, "text": "Open inbox", "from": map[string]any{"id": 1001},
		"chat": map[string]any{"id": 42, "type": "private"}, "reply_to_message": map[string]any{"message_id": 91}}}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	signed, err := trigger.AdmitRequest(providertriggers.Request{Provider: "telegram", Method: http.MethodPost,
		Body: body, Payload: payload, Target: providertriggers.Target{WebhookSecret: "fixture-signing"},
		Headers: http.Header{"X-Telegram-Bot-Api-Secret-Token": {"fixture-signing"}}})
	if err != nil {
		t.Fatal(err)
	}
	fact := sourceartifactfixture.FactFor(rooted.Artifact())
	ctx := correlation.WithSourceArtifactFact(context.Background(), fact)
	original := evidenceProofRequest(t)
	original.Provider, original.TargetAlias, original.ProviderEventID = "telegram", "telegram", signed.ProviderEventID()
	original.PublicationID, original.MarkerEventID, err = DeterministicIDs(original.Identity())
	if err != nil {
		t.Fatal(err)
	}
	delivery, admission, err := trigger.ProjectPublication(signed, fact.BundleHash(), original.FlowPath)
	if err != nil {
		t.Fatal(err)
	}
	standalone, err := packadmission.AdmitProviderDirectory(entry.Provider(), spec)
	if err != nil {
		t.Fatal(err)
	}
	standalonePlan, found := standalone.ChannelPlan()
	if !found {
		t.Fatal("operator fixture requires standalone metadata control")
	}
	for _, output := range delivery.Events {
		if output.Kind != providertriggers.OutputKindNormalized {
			continue
		}
		for _, flow := range []string{"", ".", " ingress", "ingress/", "other"} {
			request := original
			request.FlowPath = flow
			if projection, err := ProjectOperatorOutput(output, request, []packs.SatisfactionPlan{channel}, nil, nil); err != nil || projection != nil {
				t.Fatalf("foreign holder %q acquired operator projection: %+v error=%v", flow, projection, err)
			}
		}
		if projection, err := ProjectOperatorOutput(output, original, []packs.SatisfactionPlan{standalonePlan}, nil, nil); err != nil || projection != nil {
			t.Fatalf("standalone plan became a captured-flow fallback: %+v error=%v", projection, err)
		}
		if _, err := ProjectOperatorOutput(output, original, []packs.SatisfactionPlan{channel, channel}, nil, nil); err == nil || !strings.Contains(err.Error(), "ambiguously satisfies multiple") {
			t.Fatalf("duplicate plans within the exact holder lost ambiguity refusal: %v", err)
		}
	}
	for _, changed := range []bool{false, true} {
		request := original
		if changed {
			request.ProviderEventID = uuid.NewString()
			request.PublicationID, request.MarkerEventID, err = DeterministicIDs(request.Identity())
			if err != nil {
				t.Fatal(err)
			}
		}
		var projection *OperatorProjection
		for _, output := range delivery.Events {
			projection, err = ProjectOperatorOutput(output, request, []packs.SatisfactionPlan{channel}, nil, projection)
			if err != nil {
				t.Fatal(err)
			}
		}
		evidence, err := ProjectEvidence(request, nil, nil, executionposture.Live)
		if err != nil {
			t.Fatal(err)
		}
		_, err = NewOperatorCommit(ctx, admission, request, evidence, projection)
		if (err != nil) != changed {
			t.Fatal("HTTP operator admission changed receipt identity or required native capture evidence", changed, err)
		}
	}
}
