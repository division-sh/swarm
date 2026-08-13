package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/providerconnectors"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

type scenarioProfileReaderStub struct {
	profile scenarioexecution.Profile
}

func (r scenarioProfileReaderStub) LoadScenarioExecutionProfile(context.Context, string) (scenarioexecution.Profile, bool, error) {
	return r.profile, true, nil
}

type scenarioProfileMapReader map[string]scenarioexecution.Profile

func (r scenarioProfileMapReader) LoadScenarioExecutionProfile(_ context.Context, runID string) (scenarioexecution.Profile, bool, error) {
	profile, ok := r[runID]
	return profile, ok, nil
}

func TestScenarioExecutionProfileResolvesExactPlanAfterCoordinatorRestart(t *testing.T) {
	source, tool := scenarioExecutionProfileTestSource(t)
	fact, err := runtimecorrelation.NewPersistedBundleSourceFact("bundle-v1:sha256:" + strings.Repeat("1", 64))
	if err != nil {
		t.Fatal(err)
	}
	identity, err := scenarioexecution.NewEffectiveSourceIdentity(fact, "sha256:"+strings.Repeat("2", 64))
	if err != nil {
		t.Fatal(err)
	}
	outputDigest, err := tool.OutputSchema().CanonicalHash()
	if err != nil {
		t.Fatal(err)
	}
	profile, err := scenarioexecution.NewProfile(identity, "reply", []scenarioexecution.ConnectorResponse{{
		ToolID: "provider.send", OutputSchemaDigest: outputDigest, Response: json.RawMessage(`{"ok":true}`),
	}})
	if err != nil {
		t.Fatal(err)
	}
	base, err := providerconnectors.NewMockResponsePlan(map[string]any{"provider.send": map[string]any{"ok": false}})
	if err != nil {
		t.Fatal(err)
	}
	reader := scenarioProfileReaderStub{profile: profile}
	newCoordinator := func(runtimeIdentity scenarioexecution.EffectiveSourceIdentity, posture executionposture.Posture) *PipelineCoordinator {
		return &PipelineCoordinator{
			scenarioProfiles: reader, effectiveSource: runtimeIdentity,
			executionPosture: posture, mockConnectorResponses: base,
		}
	}
	for _, lifecycle := range []string{"initial", "restarted"} {
		plan, err := newCoordinator(identity, executionposture.MockOnly).mockResponsePlanForRun(context.Background(), "run-1", source)
		if err != nil {
			t.Fatalf("%s coordinator resolve profile: %v", lifecycle, err)
		}
		admitted, err := plan.Admit("provider.send", tool)
		if err != nil {
			t.Fatalf("%s coordinator admit response: %v", lifecycle, err)
		}
		value, err := admitted.Materialize()
		if err != nil {
			t.Fatal(err)
		}
		if value.(map[string]any)["ok"] != true {
			t.Fatalf("%s coordinator response = %#v, want exact profile overlay", lifecycle, value)
		}
	}
	drifted, _ := scenarioexecution.NewEffectiveSourceIdentity(fact, "sha256:"+strings.Repeat("3", 64))
	if _, err := newCoordinator(drifted, executionposture.MockOnly).mockResponsePlanForRun(context.Background(), "run-1", source); err == nil || !strings.Contains(err.Error(), "effective source mismatch") {
		t.Fatalf("drifted runtime error = %v", err)
	}
	if _, err := newCoordinator(identity, executionposture.Live).mockResponsePlanForRun(context.Background(), "run-1", source); err == nil || !strings.Contains(err.Error(), "runtime posture") {
		t.Fatalf("live runtime error = %v", err)
	}
}

func TestScenarioExecutionProfilesRemainIsolatedAcrossConcurrentRuns(t *testing.T) {
	source, tool := scenarioExecutionProfileTestSource(t)
	fact, err := runtimecorrelation.NewPersistedBundleSourceFact("bundle-v1:sha256:" + strings.Repeat("4", 64))
	if err != nil {
		t.Fatal(err)
	}
	identity, err := scenarioexecution.NewEffectiveSourceIdentity(fact, "sha256:"+strings.Repeat("5", 64))
	if err != nil {
		t.Fatal(err)
	}
	outputDigest, err := tool.OutputSchema().CanonicalHash()
	if err != nil {
		t.Fatal(err)
	}
	newProfile := func(id string, response bool) scenarioexecution.Profile {
		profile, profileErr := scenarioexecution.NewProfile(identity, id, []scenarioexecution.ConnectorResponse{{
			ToolID: "provider.send", OutputSchemaDigest: outputDigest,
			Response: json.RawMessage(fmt.Sprintf(`{"ok":%t}`, response)),
		}})
		if profileErr != nil {
			t.Fatal(profileErr)
		}
		return profile
	}
	base, err := providerconnectors.NewMockResponsePlan(map[string]any{"provider.send": map[string]any{"ok": false}})
	if err != nil {
		t.Fatal(err)
	}
	coordinator := &PipelineCoordinator{
		scenarioProfiles: scenarioProfileMapReader{
			"run-true":  newProfile("true-profile", true),
			"run-false": newProfile("false-profile", false),
		},
		effectiveSource: identity, executionPosture: executionposture.MockOnly, mockConnectorResponses: base,
	}
	type expectation struct {
		runID string
		want  bool
	}
	expectations := []expectation{{runID: "run-true", want: true}, {runID: "run-false", want: false}, {runID: "run-unprofiled", want: false}}
	errs := make(chan error, len(expectations))
	for _, expected := range expectations {
		expected := expected
		go func() {
			plan, planErr := coordinator.mockResponsePlanForRun(context.Background(), expected.runID, source)
			if planErr != nil {
				errs <- fmt.Errorf("%s resolve: %w", expected.runID, planErr)
				return
			}
			admitted, admitErr := plan.Admit("provider.send", tool)
			if admitErr != nil {
				errs <- fmt.Errorf("%s admit: %w", expected.runID, admitErr)
				return
			}
			value, materializeErr := admitted.Materialize()
			if materializeErr != nil {
				errs <- fmt.Errorf("%s materialize: %w", expected.runID, materializeErr)
				return
			}
			if got := value.(map[string]any)["ok"]; got != expected.want {
				errs <- fmt.Errorf("%s response = %#v, want %t", expected.runID, got, expected.want)
				return
			}
			errs <- nil
		}()
	}
	for range expectations {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
}

func TestScenarioExecutionProfileMismatchFencesTerminalReplay(t *testing.T) {
	ctx := testAuthorActivityContext(t, context.Background())
	source, tool := scenarioExecutionProfileTestSource(t)
	fact, err := runtimecorrelation.NewPersistedBundleSourceFact("bundle-v1:sha256:" + strings.Repeat("6", 64))
	if err != nil {
		t.Fatal(err)
	}
	identity, err := scenarioexecution.NewEffectiveSourceIdentity(fact, "sha256:"+strings.Repeat("7", 64))
	if err != nil {
		t.Fatal(err)
	}
	outputDigest, err := tool.OutputSchema().CanonicalHash()
	if err != nil {
		t.Fatal(err)
	}
	profile, err := scenarioexecution.NewProfile(identity, "reply", []scenarioexecution.ConnectorResponse{{
		ToolID: "provider.send", OutputSchemaDigest: outputDigest, Response: json.RawMessage(`{"ok":true}`),
	}})
	if err != nil {
		t.Fatal(err)
	}
	base, err := providerconnectors.NewMockResponsePlan(map[string]any{"provider.send": map[string]any{"ok": false}})
	if err != nil {
		t.Fatal(err)
	}

	runID := uuid.NewString()
	db, journal := newSQLiteActivityJournalStore(t, ctx)
	seedActivityRun(t, db, true, runID)
	intent := testNonIdempotentActivityIntent(runID, uuid.NewString(), uuid.NewString())
	intent.Tool = "provider.send"
	intent.ActivityID = "provider_send"
	intent.ExecutionMode = executionmode.Mock
	intent.Input = mustActivityInput(map[string]any{})
	reader := scenarioProfileReaderStub{profile: profile}

	firstBus := &recordingPipelineBus{}
	first := newDurablePipelineCoordinatorForTest(firstBus, db, PipelineCoordinatorOptions{
		Module: staticSemanticWorkflowModule{source: source}, Persistence: workflowPersistenceForTest(journal),
		PipelineObligations: unavailablePipelineTestObligationOwner{}, MockConnectorResponses: base,
		ScenarioExecutionProfiles: reader, EffectiveSourceIdentity: identity, ExecutionPosture: executionposture.MockOnly,
	})
	if err := (pipelineActivityDispatcher{coordinator: first}).executeActivityIntent(ctx, intent); err != nil {
		t.Fatalf("execute initial profiled activity: %v", err)
	}
	if len(firstBus.publishes) != 1 {
		t.Fatalf("initial publications = %#v", firstBus.publishes)
	}

	drifted, err := scenarioexecution.NewEffectiveSourceIdentity(fact, "sha256:"+strings.Repeat("8", 64))
	if err != nil {
		t.Fatal(err)
	}
	restartBus := &recordingPipelineBus{}
	restarted := newDurablePipelineCoordinatorForTest(restartBus, db, PipelineCoordinatorOptions{
		Module: staticSemanticWorkflowModule{source: source}, Persistence: workflowPersistenceForTest(journal),
		PipelineObligations: unavailablePipelineTestObligationOwner{}, MockConnectorResponses: base,
		ScenarioExecutionProfiles: reader, EffectiveSourceIdentity: drifted, ExecutionPosture: executionposture.MockOnly,
	})
	if err := (pipelineActivityDispatcher{coordinator: restarted}).executeActivityIntent(ctx, intent); err != nil {
		t.Fatalf("execute drifted profiled activity: %v", err)
	}
	if len(restartBus.publishes) != 1 {
		t.Fatalf("drifted publications = %#v", restartBus.publishes)
	}
	if restartBus.publishes[0].ID() == firstBus.publishes[0].ID() {
		t.Fatalf("drifted runtime replayed predecessor result %s", firstBus.publishes[0].ID())
	}
	failure := requireActivityEventFailure(t, restartBus.publishes[0])
	if failure.Detail.Code != "scenario_execution_profile_not_admitted" {
		t.Fatalf("drifted failure = %#v", failure)
	}
}

func scenarioExecutionProfileTestSource(t *testing.T) (semanticview.Source, runtimecontracts.ToolSchemaEntry) {
	t.Helper()
	empty := runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)
	boolean := runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaBoolean)
	output, err := runtimecontracts.NewToolInputSchema(runtimecontracts.ToolSchemaObject,
		runtimecontracts.ToolSchemaProperties(map[string]runtimecontracts.ToolInputSchema{"ok": boolean}),
		runtimecontracts.ToolSchemaRequired("ok"),
	)
	if err != nil {
		t.Fatal(err)
	}
	tool, err := runtimecontracts.NewToolSchemaEntry(
		runtimecontracts.WithToolCategory("provider_connector"),
		runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),
		runtimecontracts.WithToolEffect(runtimecontracts.ActivityEffectClassNonIdempotentWrite),
		runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: "POST", URL: "https://example.invalid/send"}),
		runtimecontracts.WithToolSchemas(empty, output),
	)
	if err != nil {
		t.Fatal(err)
	}
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Tools: map[string]runtimecontracts.ToolSchemaEntry{"provider.send": tool}}), tool
}
