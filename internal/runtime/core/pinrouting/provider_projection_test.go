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
	source := targetFreeProviderSource(t, canonicalrouting.CopyProviderRollbackRenamedSource(t, true))
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
	source := targetFreeProviderSource(t, canonicalrouting.CopyProviderRollbackInvalidSourceType(t))
	graph := pinrouting.CompileConnectGraph(source)
	plans, issues := graph.Plans(), graph.Issues()
	if len(plans) != 0 || len(issues) != 1 || !strings.Contains(issues[0].Detail, "key_types_incompatible") {
		t.Fatalf("plans/issues = %#v/%#v, want target-free source type blocker", plans, issues)
	}
}

func TestTemplateInputReceiverPlanAcceptsSyntheticProjectionWithDistinctSchemaEvidence(t *testing.T) {
	for _, tc := range targetFreeSyntheticProjectionCases() {
		t.Run(tc.name, func(t *testing.T) {
			source := targetFreeProviderSource(t, canonicalrouting.CopyProviderRollbackSyntheticProjection(t, tc.mint))
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
			source := targetFreeProviderSource(t, canonicalrouting.CopyProviderRollbackSyntheticProjection(t, tc.mint))
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
			source := targetFreeProviderSource(t, canonicalrouting.CopyProviderRollbackSyntheticCollision(t, tc.mint))
			graph := pinrouting.CompileConnectGraph(source)
			plans, issues := graph.Plans(), graph.Issues()
			if len(plans) != 0 || len(issues) != 1 || issues[0].Failure != pinrouting.ConnectFailureResolutionProjectionCollision || !strings.Contains(issues[0].Detail, "field chat_id conflicts") {
				t.Fatalf("provider plans/issues = %#v/%#v, want synthetic projection collision", plans, issues)
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

func targetFreeProviderSource(t testing.TB, root string) semanticview.Source {
	t.Helper()
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatalf("load captured provider fixture: %v", err)
	}
	source, err := runtimepkg.SourceWithProviderTriggerEvents(semanticview.Wrap(bundle), nil)
	if err != nil {
		t.Fatalf("project captured provider fixture: %v", err)
	}
	return source
}
