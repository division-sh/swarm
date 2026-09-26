package runforkexecution

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

// Component tests bind a real retained execution before constructing a runtime;
// fabricated effect evidence alone can no longer pass generation admission.
func selectedContractTestRuntimeAuthority(t testing.TB, ctx context.Context, db *sql.DB, selected *store.PostgresStore, loaded LoadedSelectedContractSource, artifact *sourceartifact.AdmittedSourceArtifact, runID string, agents selectedContractAgentRuntimePlan) (effects.Authority, *PreparedSelectedFork, runfork.RunForkSelectedContractExecutionAdmission) {
	t.Helper()
	source := loaded.SourceArtifactFact
	sourceRun, eventID, bindingID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	runlifecyclefixture.RequirePostgres(t, ctx, db, runlifecyclefixture.Fixture{
		Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: sourceRun, Source: source, Artifact: artifact,
	})
	now := time.Now().UTC()
	storetest.InsertExistingRunRootEventRecord(t, ctx, db, authoractivityfixture.DialectPostgres, eventID, sourceRun, "selected.proof",
		eventtest.Producer(events.EventProducerExternal, "selected-proof"), []byte(`{}`), events.EventEnvelope{Scope: events.EventScopeGlobal}, now)
	revision := captureSelectedExecutionSourceRevision(t, db, sourceRun)
	forkOrigin, err := runtimerunlifecycle.ForkMaterializationRunOrigin(sourceRun, runtimerunlifecycle.ForkOriginPointEvent, revision, eventID)
	if err != nil {
		t.Fatal(err)
	}
	storetest.RequireRun(t, ctx, selected, storetest.RunFixture{
		RunID: runID, State: runtimerunlifecycle.StatePaused, Origin: forkOrigin,
		Artifact: artifact, StartedAt: now,
	})
	if _, err := db.ExecContext(ctx, `INSERT INTO run_fork_selected_contract_bindings
		(binding_id,fork_run_id,source_run_id,fork_point_kind,fork_revision,fork_event_id,mode,created_at)
		VALUES ($1,$2,$3,'event',$4,$5,'selected_contracts',$6)`, bindingID, runID, sourceRun, revision, eventID, now); err != nil {
		t.Fatal(err)
	}
	plan, err := selected.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: sourceRun, At: eventID})
	if err != nil {
		t.Fatal(err)
	}
	operation := selectedContractOperationForTest(t, ctx)
	owner := selectedContractExecutionOwnerForTest(t, selected)
	prepared, err := prepareSelectedFork(ctx, operation, owner.ports, loaded, plan, runfork.RunForkContractFrontierAdmission{}, runfork.RunForkSelectedContractRecipientPlanning{}, agents)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := prepared.bind(ctx, runID, loaded, agents)
	if err != nil {
		t.Fatal(err)
	}
	admission := runfork.RunForkSelectedContractExecutionAdmission{
		Owner: runfork.RunForkSelectedContractExecutionAdmissionOwner, FutureExecutionOwner: runfork.RunForkSelectedContractExecutionOwner,
		NonMutating: true, ForkRunID: runID, SourceRunID: sourceRun, ForkPoint: plan.ForkPoint, ForkEventID: eventID,
		ContractSelection: runfork.RunForkContractSelection{Mode: "selected_contracts"}, ContractBindingOwner: runfork.RunForkSelectedContractBindingOwner,
		AdmissionUse:               runfork.RunForkSelectedContractExecutionAdmissionUseDurableBinding,
		DeferredWorkAdmissionOwner: runfork.RunForkSelectedContractDeferredWorkAdmissionOwner,
	}
	issued, err := selected.IssueRunForkSelectedContractRuntimeExecution(ctx, runfork.SelectedContractRuntimeExecutionIssueRequest{
		DeclarationPlan: agents.Declarations, Preparation: binding, Admission: admission,
		ContainerPlanFingerprint: "sha256:" + strings.Repeat("1", 64), ActorCensusFingerprint: "sha256:" + strings.Repeat("2", 64),
		EffectiveConfigFingerprint: "sha256:" + strings.Repeat("3", 64), ExecutionMode: effects.ExecutionModeLive,
	})
	if err != nil {
		t.Fatal(err)
	}
	authority, err := selected.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "component-runtime-test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return authority, prepared, admission
}
