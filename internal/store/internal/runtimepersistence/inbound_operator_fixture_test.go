package runtimepersistence

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
	inbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func inboundOperatorProofPlan(t *testing.T) packs.SatisfactionPlan {
	t.Helper()
	snapshot, err := yamlsource.LoadFile(filepath.Join("..", "..", "..", "..", "platform-spec.yaml"))
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
	return packfixture.ChannelPlanByID(t, plans, "provider.telegram.hitl_channel")
}

// Persistence tests supply only external Telegram bytes. Compiled trigger and
// channel owners issue the projection; this helper cannot seal arbitrary facts.
func inboundOperatorProofCommand(t *testing.T, ctx context.Context, request inbound.Request, payload map[string]any) inbound.CommitCommand {
	t.Helper()
	channel := inboundOperatorProofPlan(t)
	trigger, err := packfixture.TriggerCatalog(t).CompileAdmission(providertriggers.CompileAdmissionRequest{Alias: "telegram", Provider: "telegram", SigningSecret: "operator_fixture_signing"})
	if err != nil {
		t.Fatal(err)
	}
	payload["update_id"] = request.ProviderEventID
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	signed, err := trigger.AdmitRequest(providertriggers.Request{Provider: "telegram", Method: http.MethodPost, Body: body, Payload: payload,
		Target:  providertriggers.Target{WebhookSecret: "operator-fixture-signing"},
		Headers: http.Header{"X-Telegram-Bot-Api-Secret-Token": {"operator-fixture-signing"}}})
	if err != nil {
		t.Fatal(err)
	}
	request.Provider, request.TargetAlias, request.ProviderEventID = "telegram", "telegram", signed.ProviderEventID()
	request.PublicationID, request.MarkerEventID, err = inbound.DeterministicIDs(request.Identity())
	if err != nil {
		t.Fatal(err)
	}
	source, found := correlation.SourceArtifactFactFromContext(ctx)
	if !found {
		t.Fatal("operator persistence fixture has no admitted source")
	}
	delivery, admission, err := trigger.ProjectPublication(signed, source.BundleHash(), request.FlowPath)
	if err != nil {
		t.Fatal(err)
	}
	var projection *inbound.OperatorProjection
	for _, output := range delivery.Events {
		projection, err = inbound.ProjectOperatorOutput(output, request, []packs.SatisfactionPlan{channel}, nil, projection)
		if err != nil {
			t.Fatal(err)
		}
	}
	command, err := inbound.NewOperatorCommit(ctx, admission, request, inboundPublicationZeroOutputEvidence(t, request), projection)
	if err != nil {
		t.Fatal(err)
	}
	return command
}
