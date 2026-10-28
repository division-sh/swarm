package serveapp

import (
	"context"
	"errors"

	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestRuntimeProcessInboundHandlerTeachesUnknownStandingAlias(t *testing.T) {
	manager, err := runtimepkg.NewRuntimeContextManager(nil)
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	rec := httptest.NewRecorder()
	runtimeProcessInboundHandler{contexts: manager}.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/webhooks/chat/telegram", strings.NewReader(`{"ok":true}`)))
	if rec.Code != http.StatusNotFound || !strings.Contains(rec.Body.String(), `no ingress target "chat" is declared`) {
		t.Fatalf("unknown alias status/body = %d %q, want teaching 404", rec.Code, rec.Body.String())
	}
}

func TestStandingIngressAliasGrammarMatchesProcessWebhookRouter(t *testing.T) {
	for _, alias := range []string{"chat", "chat.v2", "chat_v2", "chat-v2", "9chat"} {
		if err := runtimecontracts.ValidateIngressAlias(alias); err != nil {
			t.Fatalf("ValidateIngressAlias(%q): %v", alias, err)
		}
		gotAlias, provider, ok := parseProcessWebhookPath("/webhooks/" + alias + "/telegram")
		if !ok || gotAlias != alias || provider != "telegram" {
			t.Fatalf("parseProcessWebhookPath(%q) = %q/%q/%v", alias, gotAlias, provider, ok)
		}
	}
	for _, alias := range []string{"chat/support", "chat support", "chat%2Fsupport", "-chat", ".chat", "chat?x"} {
		if err := runtimecontracts.ValidateIngressAlias(alias); err == nil {
			t.Fatalf("ValidateIngressAlias(%q) error = nil", alias)
		}
	}
	if _, _, ok := parseProcessWebhookPath("/webhooks/chat/support/telegram"); ok {
		t.Fatal("parseProcessWebhookPath accepted a multi-segment alias")
	}
	for _, alias := range []string{" chat", "chat "} {
		if parsed, _, _ := parseProcessWebhookPath("/webhooks/" + alias + "/telegram"); parsed != alias {
			t.Fatalf("parser normalized unadmitted alias %q into %q", alias, parsed)
		}
	}
}

func TestProcessIngressIndexRequiresSelectedScope(t *testing.T) {
	root := writeStandingTelegramServeFixture(t, "http://127.0.0.1:1")
	_, bundle, err := cliapp.NewSwarmWorkflowModule(repoRootForTest(), root)
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	const runID = "41000000-0000-0000-0000-000000000001"
	store := newProcessIngressEventStore(t, source, runID)
	lookup := store.observation.Selection()
	observed, found, err := store.LookupFlowInstance(t.Context(), lookup)
	if err != nil || !found || observed.Owner() != store.observation.Owner() {
		t.Fatalf("exact index lookup = %+v found=%t err=%v", observed.Owner(), found, err)
	}
	for _, owners := range [][]runtimeflowidentity.RunScopedFlowInstance{nil, {observed.Owner()}} {
		scope, err := runtimepipeline.NewFlowInstanceLookupScope(source, observed.SourceFact(), runID, nil, owners)
		if err != nil {
			t.Fatal(err)
		}
		selected, err := store.ListFlowInstances(t.Context(), scope)
		if err != nil || len(selected) != len(owners) {
			t.Fatalf("index ignored its exact scope: %+v err=%v", selected, err)
		}
	}
	const foreignRun = "42000000-0000-0000-0000-000000000001"
	foreignOwner, err := runtimeflowidentity.StandingForGeneration(source, ".", foreignRun)
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := runtimepipeline.NewExactFlowInstanceLookup(source, observed.SourceFact(), runtimeflowidentity.RunScopedFlowInstance{RunID: foreignRun, Route: foreignOwner.Route()})
	if err != nil {
		t.Fatal(err)
	}
	if observed, found, err := store.LookupFlowInstance(t.Context(), foreign); err != nil || found || observed.Valid() {
		t.Fatalf("foreign selected run borrowed native root: valid=%t found=%t err=%v", observed.Valid(), found, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := store.LookupFlowInstance(ctx, lookup); !errors.Is(err, context.Canceled) {
		t.Fatalf("index lookup lost cancellation: %v", err)
	}
}

func TestProcessIngressConstructionObservationRequiresExactOwner(t *testing.T) {
	root := writeStandingTelegramServeFixture(t, "http://127.0.0.1:1")
	_, bundle, err := cliapp.NewSwarmWorkflowModule(repoRootForTest(), root)
	if err != nil {
		t.Fatal(err)
	}
	const runID = "41000000-0000-0000-0000-000000000001"
	store := newProcessIngressEventStore(t, semanticview.Wrap(bundle), runID)
	identity := store.observation.Identity()
	owner, err := runtimeflowidentity.NewRunScopedFlowInstance(runID, identity.Route())
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		owner  runtimeflowidentity.RunScopedFlowInstance
		entity string
		valid  bool
	}{
		{name: "exact", owner: owner, entity: identity.EntityID, valid: true},
		{name: "foreign run", owner: runtimeflowidentity.RunScopedFlowInstance{RunID: "42000000-0000-0000-0000-000000000001", Route: owner.Route}, entity: identity.EntityID},
		{name: "foreign route", owner: runtimeflowidentity.RunScopedFlowInstance{RunID: runID}, entity: identity.EntityID},
		{name: "foreign entity", owner: owner, entity: "42000000-0000-0000-0000-000000000002"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proof, err := store.LoadFlowConstructionPublication(t.Context(), tc.owner, tc.entity)
			if tc.valid {
				if err != nil || proof.Identity != identity {
					t.Fatalf("exact fixture observation = %+v, %v", proof, err)
				}
			} else if err == nil || proof.Identity != (runtimeflowidentity.Instance{}) {
				t.Fatalf("foreign fixture owner accepted: %+v, %v", proof, err)
			}
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := store.LoadFlowConstructionPublication(ctx, owner, identity.EntityID); !errors.Is(err, context.Canceled) {
		t.Fatalf("fixture observer lost cancellation: %v", err)
	}
	if _, err := (&processIngressEventStore{}).LoadFlowConstructionPublication(t.Context(), runtimeflowidentity.RunScopedFlowInstance{}, ""); err == nil {
		t.Fatal("empty fixture observation authorized an empty owner")
	}
}

// These transport/controller fixtures use real provider declarations without
// conversation work. Full source-loaded delivery is covered by the HTTP matrix.
func processIngressTransportSource(t *testing.T, bundle *runtimecontracts.WorkflowContractBundle, catalog *providertriggers.CatalogSnapshot) semanticview.Source {
	t.Helper()
	source, err := runtimepkg.SourceWithProviderTriggerEvents(semanticview.Wrap(bundle), catalog)
	if err != nil {
		t.Fatal(err)
	}
	return source
}
