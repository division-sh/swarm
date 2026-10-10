package inboundpublication

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
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
	registry, err := packs.NewInterfaceRegistry(spec)
	if err != nil {
		t.Fatal(err)
	}
	plans, err := packs.CompileChannelInventory(registry, packfixture.ChannelPacks(t),
		packfixture.TriggerCatalog(t).PackDescriptors(), packfixture.ConnectorRegistry(t).PackDescriptors())
	if err != nil {
		t.Fatal(err)
	}
	channel := packfixture.ChannelPlanByID(t, plans, "provider.telegram.hitl_channel")
	trigger, err := packfixture.TriggerCatalog(t).CompileAdmission(providertriggers.CompileAdmissionRequest{
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
	fact := sourceartifactfixture.Fact()
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
