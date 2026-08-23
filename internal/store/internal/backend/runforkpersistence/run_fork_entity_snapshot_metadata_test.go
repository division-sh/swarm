package runforkpersistence

import (
	"strings"
	"testing"

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
