package runtime_test

import (
	"context"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packmodel"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func seedCapturedProviderTarget(t *testing.T, ctx context.Context, persistence runtimepkg.InboundPersistence, source semanticview.Source, runID, entityID, flowInstance, slug, provider, secret, agentID string) (context.Context, runtimepkg.InboundTarget) {
	t.Helper()
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("provider fixture requires its captured bundle")
	}
	rooted, err := bundle.RootedSource()
	if err != nil {
		t.Fatal(err)
	}
	ctx = runtimecorrelation.WithRootAdmission(runtimecorrelation.WithSourceArtifactFact(ctx, sourceartifactfixture.FactFor(rooted.Artifact())), rooted.Admission())
	var target runtimepkg.InboundTarget
	switch selected := persistence.(type) {
	case *store.PostgresStore:
		if _, err := selected.EnsureRootedSource(ctx, rooted); err != nil {
			t.Fatal(err)
		}
		target = seedPostgresInboundGatewayRuntime(t, ctx, selected, runID, entityID, flowInstance, slug, provider, secret, agentID, source)
	case *store.SQLiteRuntimeStore:
		if _, err := selected.EnsureRootedSource(ctx, rooted); err != nil {
			t.Fatal(err)
		}
		target = seedSQLiteInboundGatewayRuntime(t, ctx, selected, runID, entityID, flowInstance, slug, provider, secret, agentID, source)
	default:
		t.Fatalf("unsupported provider fixture store %T", persistence)
	}
	catalog := capturedProviderCatalog(t, source)
	target.AdmissionPlan, err = catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{Alias: target.Alias, Provider: provider, SigningSecret: secret})
	if err != nil {
		t.Fatal(err)
	}
	target.Provider, target.SigningSecret = provider, secret
	return boundedInboundTargetContext(t, ctx, target), target
}

func capturedProviderCatalog(t *testing.T, source semanticview.Source) *providertriggers.CatalogSnapshot {
	t.Helper()
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("provider catalog requires its captured source")
	}
	rooted, err := bundle.RootedSource()
	if err != nil {
		t.Fatal(err)
	}
	providers, err := packadmission.AdmitProviderSource(rooted, bundle.Platform)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := providers.Scope(boundedProviderFlowID)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := scope.TriggerCatalog()
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func newCapturedProviderGateway(t *testing.T, bus *runtimebus.EventBus, source semanticview.Source, stores ...runtimepkg.InboundPersistence) *runtimepkg.InboundGateway {
	t.Helper()
	gateway := newTestInboundGateway(t, bus, nil, nil, stores...)
	bus.SetProviderOutputAuthorizationVerifier(capturedProviderCatalog(t, source))
	return gateway
}

func capturedTelegramOperatorSource(t *testing.T) semanticview.Source {
	t.Helper()
	return boundedStandingConnectorSource(t, "telegram", "https://api.telegram.org", "[inbound.telegram.raw, inbound.telegram.text_message, inbound.telegram.callback_action]", "")
}

// These five runtime fixtures author the same bounded ingress flow, with each
// provider's HTTP endpoint changed in its BODY before source capture.
func boundedStandingConnectorSource(t *testing.T, provider, baseURL, inputs, nodes string) semanticview.Source {
	t.Helper()
	const manifest = "version: 1.0.0\nplatform_version: '*'\n"
	files := map[string]string{
		"manifest.yaml":                          manifest,
		"schema.yaml":                            "stages: []\n",
		boundedProviderFlowID + "/manifest.yaml": manifest,
		boundedProviderFlowID + "/schema.yaml":   "stages:\n  active: {}\npins:\n  inputs: " + inputs + "\n",
		boundedProviderFlowID + "/entities.yaml": "bounded_entity: {}\n",
	}
	if nodes != "" {
		files[boundedProviderFlowID+"/nodes.yaml"] = nodes
	}
	providers := []string{provider}
	if provider != "telegram" && provider != "github" {
		providers = append(providers, "telegram")
	}
	origins := map[string]string{
		"telegram": "https://api.telegram.org", "github": "https://api.github.com",
		"notion": "https://api.notion.com", "microsoft_graph": "https://graph.microsoft.com",
		"slack": "https://slack.com",
	}
	for _, key := range providers {
		entry, ok := packfixture.EmbeddedInventory(t).Lookup(key)
		if !ok {
			t.Fatalf("missing embedded provider %q", key)
		}
		directory := boundedProviderFlowID + "/" + key + "/"
		files[directory+"manifest.yaml"] = string(entry.ManifestBody())
		for _, role := range []packmodel.BodyRole{packmodel.BodyTrigger, packmodel.BodyConnector, packmodel.BodyChannel} {
			body, present := entry.Provider().Body(role)
			if !present {
				continue
			}
			bytes := string(body.Bytes())
			if key == provider && role == packmodel.BodyConnector {
				bytes = strings.ReplaceAll(bytes, "url: "+origins[key], "url: "+strings.TrimRight(baseURL, "/"))
			}
			files[directory+role.FileName()] = bytes
		}
	}
	bundle := loadRuntimeTempBundle(t, files)
	base := semanticview.Wrap(bundle)
	projection, err := runtimepkg.AdmitEffectiveSourceProjection(runtimepkg.EffectiveSourceProjectionRequest{
		Source: base, SourceArtifactFact: runtimeTestSourceArtifactFact(t, base),
	})
	if err != nil {
		t.Fatalf("admit captured connector source: %v", err)
	}
	return projection.Source()
}

func telegramConnectorSupportedSurfaceSource(t *testing.T, baseURL, flowInstance string) semanticview.Source {
	t.Helper()
	return boundedStandingConnectorSource(t, "telegram", baseURL, "[inbound.telegram.text_message]", `telegram-responder:
  execution_type: system_node
  subscribes_to: [inbound.telegram.text_message]
  event_handlers:
    inbound.telegram.text_message:
      guard: {check: _entity.id != ""}
      activity:
        id: telegram_send_message
        tool: telegram.send_message
        input:
          chat_id: payload.conversation_reference
          text: payload.text
`)
}

func slackManagedConnectorSource(t *testing.T, baseURL, flowInstance string) semanticview.Source {
	t.Helper()
	return boundedStandingConnectorSource(t, "slack", baseURL, "[inbound.telegram.text_message]", `slack-responder:
  execution_type: system_node
  subscribes_to: [inbound.telegram.text_message]
  event_handlers:
    inbound.telegram.text_message:
      guard: {check: _entity.id != ""}
      activity:
        id: slack_post_message
        tool: slack.post_message
        input:
          channel: "C123"
          text: payload.text
`)
}

func notionManagedConnectorSource(t *testing.T, baseURL, flowInstance string) semanticview.Source {
	t.Helper()
	return boundedStandingConnectorSource(t, "notion", baseURL, "[inbound.telegram.text_message]", `notion-responder:
  execution_type: system_node
  subscribes_to: [inbound.telegram.text_message]
  event_handlers:
    inbound.telegram.text_message:
      guard: {check: _entity.id != ""}
      activity:
        id: notion_append_block_children
        tool: notion.append_block_children
        input:
          block_id: "block-123"
          children:
            - object: "block"
              type: "paragraph"
              paragraph:
                rich_text:
                  - type: "text"
                    text: {content: "hello from swarm"}
`)
}

func microsoftGraphConnectorSource(t *testing.T, baseURL, flowInstance string) semanticview.Source {
	t.Helper()
	return boundedStandingConnectorSource(t, "microsoft_graph", baseURL, "[inbound.telegram.text_message]", `microsoft-graph-responder:
  execution_type: system_node
  subscribes_to: [inbound.telegram.text_message]
  event_handlers:
    inbound.telegram.text_message:
      guard: {check: _entity.id != ""}
      activity:
        id: microsoft_graph_send_mail
        tool: microsoft_graph.send_mail
        input:
          user_id: "user@example.com"
          to_recipients: [{emailAddress: {address: "recipient@example.com"}}]
          subject: payload.text
          content: payload.text
`)
}
