package apiv1

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func loadMailboxWritePersistedEvent(t *testing.T, selected any, eventID string) events.Event {
	t.Helper()
	return storetest.LoadCanonicalEventRecord(t, context.Background(), selected, eventID)
}

func sourceArtifactFactForTestBundle(t *testing.T, bundle *runtimecontracts.WorkflowContractBundle) runtimecorrelation.SourceArtifactFact {
	t.Helper()
	if bundle == nil || bundle.SourceArtifact == nil {
		t.Fatal("test bundle has no admitted source artifact")
	}
	return mustAPITestSourceArtifactFact(bundle.SourceArtifact.BundleHash())
}
