package pinrouting_test

import (
	"strings"
	"testing"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestProviderConnectUsesCanonicalRenamedIdentitySource(t *testing.T) {
	source, err := targetFreeProviderSource(t, canonicalrouting.CopyProviderRollbackRenamedSource(t, true))
	if err != nil || source == nil {
		t.Fatalf("project captured provider fixture: %v", err)
	}
	graph := pinrouting.CompileConnectGraph(source)
	plans, issues := graph.Plans(), graph.Issues()
	if len(issues) != 0 || len(plans) != 1 || plans[0].InstanceKey() == nil {
		t.Fatalf("plans/issues = %#v/%#v, want one target-free instance plan", plans, issues)
	}
	plan := plans[0]
	if got, want := plan.InstanceKey().Readback().SourcePath, "payload.external_chat_id"; got != want {
		t.Fatalf("typed source = %q, want %q", got, want)
	}
	material, failure := pinrouting.InstanceKeyMaterialForConnectRoutePlan(plan,
		pinrouting.AdmitConnectRouteMatchValues(map[string]string{
			"payload.external_chat_id": "chat-authoritative",
			"payload.chat_id":          "chat-conflicting",
		}))
	if !failure.Empty() || len(material.Keys) != 1 || material.Keys[0].Field.Path() != "chat_id" || material.Keys[0].Value != "chat-authoritative" {
		t.Fatalf("compiled key material/failure = %#v/%q, want chat_id from authoritative renamed source", material, failure)
	}
	if projected := pinrouting.MaterializeConnectRoutePlan(plan); projected.Failure != pinrouting.ConnectFailureReceiverResolutionMissing || !projected.Target.Empty() || len(projected.TargetSet) != 0 {
		t.Fatalf("static projection elected a runtime receiver: %#v", projected)
	}
}

func TestProviderConnectRejectsAuthoritativeSourceTypeMismatch(t *testing.T) {
	source, err := targetFreeProviderSource(t, canonicalrouting.CopyProviderRollbackInvalidSourceType(t))
	if source != nil || err == nil {
		t.Fatalf("projection = %v/%v, want no source and a type blocker", source, err)
	}
	if message := err.Error(); !strings.Contains(message, "compiled connection input binding failed:") || !strings.Contains(message, "key_types_incompatible: resolution source payload.chat_id type integer is incompatible with receiver entity.chat_id type text") {
		t.Fatalf("projection error = %q, want exact source/receiver type incompatibility", message)
	}
}

func TestTemplateInputReceiverPlanAcceptsSyntheticProjectionWithDistinctSchemaEvidence(t *testing.T) {
	for _, tc := range targetFreeSyntheticProjectionCases() {
		t.Run(tc.name, func(t *testing.T) {
			source, err := targetFreeProviderSource(t, canonicalrouting.CopyProviderRollbackSyntheticProjection(t, tc.mint))
			if err != nil || source == nil {
				t.Fatalf("project captured provider fixture: %v", err)
			}
			association := semanticview.BuildAuthoredEventEndpointCensus(source).ResolveDeclaredInputEndpoint("consumer", "inbound.telegram.text_message")
			endpoint, ok := association.Endpoint()
			if !ok {
				t.Fatalf("resolve target-free input endpoint: %v", association.Err())
			}
			plan, issue := pinrouting.LowerPublicInputRoutePlan(source, endpoint)
			if !issue.Failure.Empty() {
				t.Fatalf("public input issue = %#v, want accepted synthetic projection", issue)
			}
			assertTargetFreeSchemaRoles(t, plan)
		})
	}
}

func TestProviderConnectAcceptsSyntheticProjectionWithDistinctSchemaEvidence(t *testing.T) {
	for _, tc := range targetFreeSyntheticProjectionCases() {
		t.Run(tc.name, func(t *testing.T) {
			source, err := targetFreeProviderSource(t, canonicalrouting.CopyProviderRollbackSyntheticProjection(t, tc.mint))
			if err != nil || source == nil {
				t.Fatalf("project captured provider fixture: %v", err)
			}
			graph := pinrouting.CompileConnectGraph(source)
			plans, issues := graph.Plans(), graph.Issues()
			if len(issues) != 0 || len(plans) != 1 {
				t.Fatalf("provider plans/issues = %#v/%#v, want one accepted synthetic projection", plans, issues)
			}
			assertTargetFreeSchemaRoles(t, plans[0])
		})
	}
}

func TestProviderConnectRejectsSyntheticProjectionCollision(t *testing.T) {
	for _, tc := range targetFreeSyntheticProjectionCases() {
		t.Run(tc.name, func(t *testing.T) {
			source, err := targetFreeProviderSource(t, canonicalrouting.CopyProviderRollbackSyntheticCollision(t, tc.mint))
			if source != nil || err == nil {
				t.Fatalf("projection = %v/%v, want no source and a projection collision", source, err)
			}
			projection := "generated.uuid"
			if tc.mint == canonicalrouting.CreateMintEventID {
				projection = "event.id"
			}
			want := "connection projection conflicts with producer: producer event inbound.telegram.text_message field chat_id conflicts with connection projection " + projection
			if message := err.Error(); !strings.Contains(message, "compiled connection input binding failed:") || !strings.Contains(message, want) {
				t.Fatalf("projection error = %q, want %q", message, want)
			}
		})
	}
}

func targetFreeSyntheticProjectionCases() []struct {
	name string
	mint canonicalrouting.CreateMint
} {
	return []struct {
		name string
		mint canonicalrouting.CreateMint
	}{
		{name: "generated_uuid", mint: canonicalrouting.CreateMintUUID},
		{name: "event_id", mint: canonicalrouting.CreateMintEventID},
	}
}

func assertTargetFreeSchemaRoles(t testing.TB, plan pinrouting.ConnectRoutePlan) {
	t.Helper()
	readback := plan.Readback()
	if readback.ProducerEvent == nil || readback.ReceiverEvent == nil {
		t.Fatalf("target-free schema evidence = producer:%#v receiver:%#v", readback.ProducerEvent, readback.ReceiverEvent)
	}
	producer, receiver := readback.ProducerEvent.AcceptanceSchemaDigest, readback.ReceiverEvent.AcceptanceSchemaDigest
	if producer == "" || receiver == "" || producer == receiver {
		t.Fatalf("target-free schema digests = producer:%q receiver:%q, want distinct immutable roles", producer, receiver)
	}
}

func targetFreeProviderSource(t testing.TB, root string) (semanticview.Source, error) {
	t.Helper()
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatalf("load captured provider fixture: %v", err)
	}
	return runtimepkg.SourceWithProviderTriggerEvents(semanticview.Wrap(bundle), nil)
}
