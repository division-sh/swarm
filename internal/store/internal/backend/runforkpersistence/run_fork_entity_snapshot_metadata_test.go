package runforkpersistence

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func TestRunForkSnapshotMetadataRejectsBlankHistoricalEntityContract(t *testing.T) {
	entityID := "entity-1"
	snapshot := &runForkRevisionSnapshot{EntityMetadata: []runForkRevisionEntityMetadata{{
		EntityID: entityID, FlowInstance: "review/one", EntityType: "  ",
	}}}

	_, message, ok := loadRunForkMaterializedEntitySnapshotMetadata(snapshot, runfork.RunForkEntityState{EntityID: entityID})
	if ok {
		t.Fatal("blank historical entity contract was admitted")
	}
	if !strings.Contains(message, "cannot prove source-at-revision flow_instance/entity_type metadata") {
		t.Fatalf("blank historical entity contract error = %q", message)
	}
}

func TestRunForkEntityIdentityProjectsCanonicalRootToForkRun(t *testing.T) {
	const sourceRunID = "11111111-1111-4111-8111-111111111111"
	const forkRunID = "22222222-2222-4222-8222-222222222222"

	identity, err := projectRunForkEntityIdentity(sourceRunID, forkRunID, sourceRunID, sourceRunID)
	if err != nil {
		t.Fatal(err)
	}
	if identity.EntityID != forkRunID || identity.FlowInstance != forkRunID {
		t.Fatalf("projected root identity = %#v, want fork run identity", identity)
	}

	nested, err := projectRunForkEntityIdentity(sourceRunID, forkRunID, "33333333-3333-4333-8333-333333333333", "review/one")
	if err != nil {
		t.Fatal(err)
	}
	if nested.EntityID != "33333333-3333-4333-8333-333333333333" || nested.FlowInstance != "review/one" {
		t.Fatalf("projected nested identity = %#v, want unchanged", nested)
	}
}

func TestRunForkEntityIdentityRejectsContradictoryRootFacts(t *testing.T) {
	const sourceRunID = "11111111-1111-4111-8111-111111111111"
	const forkRunID = "22222222-2222-4222-8222-222222222222"

	for name, facts := range map[string][2]string{
		"root entity with non-root route": {sourceRunID, "review/one"},
		"non-root entity with root route": {"33333333-3333-4333-8333-333333333333", sourceRunID},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := projectRunForkEntityIdentity(sourceRunID, forkRunID, facts[0], facts[1]); err == nil {
				t.Fatal("contradictory root identity was admitted")
			}
		})
	}
}

func TestRunForkSelectedContractSourceEventProjectsRootWithoutWorkflowState(t *testing.T) {
	const sourceRunID = "11111111-1111-4111-8111-111111111111"
	const forkRunID = "22222222-2222-4222-8222-222222222222"
	source, err := events.NewRootRoutingSource(sourceRunID)
	if err != nil {
		t.Fatal(err)
	}

	projected, err := projectRunForkSelectedContractSourceEventWorkflowState(sourceRunID, forkRunID, nil, runfork.RunForkSelectedContractSourceEvent{
		EntityID: sourceRunID, FlowInstance: sourceRunID, RoutingSource: source,
	})
	if err != nil {
		t.Fatal(err)
	}
	if projected.EntityID != forkRunID || projected.FlowInstance != forkRunID || projected.RoutingSource.Route().EntityID != forkRunID {
		t.Fatalf("projected source event = %#v, want fork-local root identity", projected)
	}
}
