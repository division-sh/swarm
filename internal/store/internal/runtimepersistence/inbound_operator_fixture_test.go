package runtimepersistence

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	inbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func inboundOperatorProofPlan(t *testing.T, rooted sourceartifact.RootedSource, flowPath string) (packs.SatisfactionPlan, *providertriggers.CatalogSnapshot) {
	t.Helper()
	snapshot, err := yamlsource.LoadFile(filepath.Join("..", "..", "..", "..", "platform-spec.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	spec, err := contracts.AdmitPlatformSpecValue(snapshot.Document("platform-spec.yaml").Root())
	if err != nil {
		t.Fatal(err)
	}
	providers, err := packadmission.AdmitProviderSource(rooted, spec)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := providers.Scope(flowPath)
	if err != nil {
		t.Fatal(err)
	}
	definition, found := scope.Provider("telegram")
	if !found || definition.Provider().DeclaringFlow() != flowPath {
		t.Fatal("operator fixture requires its exact captured Telegram holder")
	}
	catalog, err := scope.TriggerCatalog()
	if err != nil {
		t.Fatal(err)
	}
	trigger, hasTrigger := catalog.PackDescriptorForProvider("telegram")
	connector, hasConnector := definition.ConnectorDescriptor()
	if !hasTrigger || !hasConnector {
		t.Fatal("operator fixture requires its captured trigger and connector BODYs")
	}
	interfaces, err := packs.NewInterfaceRegistry(spec)
	if err != nil {
		t.Fatal(err)
	}
	channel, err := packs.CompileProviderChannel(interfaces, definition.Provider(), trigger, connector)
	if err != nil {
		t.Fatal(err)
	}
	return channel, catalog
}

// Persistence tests supply only external Telegram bytes. Compiled trigger and
// channel owners issue the projection; this helper cannot seal arbitrary facts.
func inboundOperatorProofCommand(t *testing.T, ctx context.Context, rooted sourceartifact.RootedSource, request inbound.Request, payload map[string]any) inbound.CommitCommand {
	t.Helper()
	channel, catalog := inboundOperatorProofPlan(t, rooted, request.FlowPath)
	trigger, err := catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{Alias: "telegram", Provider: "telegram", SigningSecret: "operator_fixture_signing"})
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
