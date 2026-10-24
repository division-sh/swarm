package runtimepersistence

import (
	"context"

	"encoding/json"

	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/durabledata"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"

	"github.com/division-sh/swarm/internal/runtime/pipeline"

	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/google/uuid"
)

func newBranchPointDeploymentFixture(t *testing.T, backend eventRecordContractBackend, input []byte) (forkContentionFixture, receiverConfigActivationFixture) {
	t.Helper()
	opened := backend.open(t)
	construction := newReceiverConfigActivationFixtureForStore(t, opened.store.(agentFixtureFlowStore), false, map[string]string{
		"schema.yaml": "name: branch-revision\nstages:\n  pending: {}\n  later: {final: true}\noutputs:\n  - records.ready\n",
		"events.yaml": "records.ready:\n  body: text\n",
	}, nil, ownStoreTestAgentManager, nil)
	// Preparation must consume the construction process, not acquire a rival.
	capability, err := agentfixture.ProcessCapability(t, construction.ctx, opened.store.(agentfixture.Store))
	if err != nil {
		t.Fatal(err)
	}
	value, _ := storeTestWorkFixtures.Load(t)
	work := value.(*storeTestWorkFixture)
	work.capabilitiesMu.Lock()
	if work.capabilities == nil {
		work.capabilities = make(map[any]startupownership.ProcessCapability)
	}
	work.capabilities[opened.store] = capability
	work.capabilitiesMu.Unlock()
	data := opened.store.(interface {
		EnsureSourceArtifactWithData(context.Context, *sourceartifact.AdmittedSourceArtifact, durabledata.Catalog) (sourceartifact.EnsureResult, error)
		ExecuteDataSourceOperation(context.Context, durabledata.SourceCommand) (durabledata.SourceOperationResult, error)
	})
	catalog, err := contracts.BuildDurableDataCatalog(construction.bundle)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := data.EnsureSourceArtifactWithData(construction.ctx, construction.bundle.SourceArtifact, catalog); err != nil {
		t.Fatal(err)
	}
	ref, err := durabledata.ParseDeclarationRef(".", "records.ready")
	if err != nil {
		t.Fatal(err)
	}
	imported, err := data.ExecuteDataSourceOperation(construction.ctx, durabledata.SourceCommand{
		Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: catalog.BundleHash,
		Declaration: ref, ExpectedHead: durabledata.AbsentHead(), InputFormat: "jsonl", Input: input,
	})
	if err != nil {
		t.Fatal(err)
	}
	runID, at := uuid.NewString(), time.Now().UTC().Truncate(time.Microsecond)
	ctx := correlation.WithRunID(construction.ctx, runID)
	source := semanticview.Wrap(construction.bundle)
	plan, err := construction.manager.PrepareFlowInstanceActivation(ctx, pipeline.FlowInstanceActivationRequest{
		ContractBundle: source, OccurredAt: at, Instance: flowidentity.Stored(source, ".", runID, runID, "", ""),
	})
	if err != nil {
		t.Fatal(err)
	}
	command := runtimebus.DeploymentRunCreationCommand{
		RunCreation: durabledata.RunCreationCommand{RunID: runID, Actor: "operator", BundleHash: catalog.BundleHash,
			Data: durabledata.RunCreationDataEnvelope{Pins: []durabledata.ExplicitPin{{Declaration: ref, VersionID: imported.Candidate.VersionID}}}},
		Idempotency: apiidempotency.Request{Method: "run.start", Actor: apiidempotency.BearerActor("operator"), Now: at, TTL: time.Hour},
		Root:        runtimebus.FlowInstanceActivationCommand{Plan: plan},
	}
	result, err := opened.store.(runtimebus.DeploymentRunCreationCommitOwner).CommitDeploymentRunCreation(ctx, command)
	if err != nil || !result.Acknowledged {
		t.Fatalf("create real deployment fixture: %+v err=%v", result, err)
	}
	record, err := plan.PersistenceRecord()
	if err != nil {
		t.Fatal(err)
	}
	f := forkContentionFixture{snapshotOwnershipFixture: snapshotOwnershipFixture{store: opened.store.(snapshotOwnershipStore), db: opened.db, ctx: ctx, runID: runID, entityID: record.State.EntityID, state: record.State}, source: source}
	f.state.Transition = pipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion
	f.state.ExpectedState, f.state.ExpectedRevision = "pending", 1
	f.state.Accumulator = json.RawMessage(`{"source_advanced":{"count":1}}`)
	f.write = func(ctx context.Context, store snapshotOwnershipStore) error {
		_, err := store.CommitWorkflowEngineMutation(ctx, pipeline.WorkflowEngineMutationCommand{State: f.state})
		return err
	}
	return f, construction
}
