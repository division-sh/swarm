package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/providerconnectors"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/scenarioderivation"
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

type scenarioProfileReaderFunc func(context.Context, string) (scenarioexecution.Profile, bool, error)

func (r scenarioProfileReaderFunc) LoadScenarioExecutionProfile(ctx context.Context, runID string) (scenarioexecution.Profile, bool, error) {
	return r(ctx, runID)
}

func scenarioResponsePlanForRunForTest(ctx context.Context, coordinator *PipelineCoordinator, runID string, source semanticview.Source) (*providerconnectors.MockResponsePlan, error) {
	profile, found, err := coordinator.scenarioExecutionProfileForRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	return coordinator.mockResponsePlanForProfile(source, profile, found)
}

func TestAuthoredScenarioWitnessResponsePlanNeverRescansMaterializedData(t *testing.T) {
	output, err := runtimecontracts.NewToolInputSchema(runtimecontracts.ToolSchemaObject,
		runtimecontracts.ToolSchemaProperties(map[string]runtimecontracts.ToolInputSchema{
			"marker": runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaString),
		}), runtimecontracts.ToolSchemaRequired("marker"),
	)
	if err != nil {
		t.Fatal(err)
	}
	tool, err := runtimecontracts.NewToolSchemaEntry(
		runtimecontracts.WithToolCategory("provider_connector"), runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),

		runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: "POST", URL: "https://example.invalid/send"}),
		runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject), output),
	)
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Tools: map[string]runtimecontracts.ToolSchemaEntry{"provider.send": tool}})
	declaration, found, err := scenarioderivation.ParseDeclaration([]byte(`
name: response-stage
vars: {marker: "${'${1 + 1}'}"}
derive: {flow: '.', input: request, payload: {generate: true}}
connector_responses: {provider.send: "${{'marker': vars.marker}}"}
`), "tests/response.yaml")
	if err != nil || !found {
		t.Fatalf("authored witness admission: %v %v", found, err)
	}
	fact, err := runtimecorrelation.NewSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	identity, err := scenarioexecution.NewEffectiveSourceIdentity(fact, "sha256:"+strings.Repeat("b", 64))
	if err != nil {
		t.Fatal(err)
	}
	digest, err := output.CanonicalHash()
	if err != nil {
		t.Fatal(err)
	}
	profile, err := scenarioexecution.NewProfile(identity, declaration.Name, []scenarioexecution.ConnectorResponse{{
		ToolID: "provider.send", OutputSchemaDigest: digest, Response: declaration.ConnectorResponses["provider.send"],
	}})
	if err != nil {
		t.Fatal(err)
	}
	coordinator := &PipelineCoordinator{scenarioProfiles: scenarioProfileReaderStub{profile: profile}, effectiveSource: identity, executionPosture: executionposture.MockOnly}
	plan, err := scenarioResponsePlanForRunForTest(context.Background(), coordinator, "run-response", source)
	if err != nil {
		t.Fatal(err)
	}
	response, err := plan.Admit("provider.send", tool)
	if err != nil {
		t.Fatal(err)
	}
	value, err := response.Materialize()
	if err != nil || value.(map[string]any)["marker"] != "${1 + 1}" {
		t.Fatalf("response plan rescanned authored result: %#v %v", value, err)
	}
}

func TestScenarioExecutionProfileResolvesExactPlanAfterCoordinatorRestart(t *testing.T) {
	source, tool := scenarioExecutionProfileTestSource(t)
	fact, err := runtimecorrelation.NewSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("1", 64))
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
		plan, err := scenarioResponsePlanForRunForTest(context.Background(), newCoordinator(identity, executionposture.MockOnly), "run-1", source)
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
	if _, err := scenarioResponsePlanForRunForTest(context.Background(), newCoordinator(drifted, executionposture.MockOnly), "run-1", source); err == nil || !strings.Contains(err.Error(), "effective source mismatch") {
		t.Fatalf("drifted runtime error = %v", err)
	}
	if _, err := scenarioResponsePlanForRunForTest(context.Background(), newCoordinator(identity, executionposture.Live), "run-1", source); err == nil || !strings.Contains(err.Error(), "runtime posture") {
		t.Fatalf("live runtime error = %v", err)
	}
}

func TestScenarioExecutionProfilesRemainIsolatedAcrossConcurrentRuns(t *testing.T) {
	source, tool := scenarioExecutionProfileTestSource(t)
	fact, err := runtimecorrelation.NewSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("4", 64))
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
			plan, planErr := scenarioResponsePlanForRunForTest(context.Background(), coordinator, expected.runID, source)
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

func VerifyScenarioExecutionProfileMismatchFencesTerminalReplayForTest(t *testing.T, open func(*testing.T) WorkflowActivityNativeFixtureForTest) {
	fixture := open(t)
	ctx := fixture.Context
	source, tool := scenarioExecutionProfileTestSource(t)
	fact, found := runtimecorrelation.SourceArtifactFactFromContext(ctx)
	if !found {
		t.Fatal("native profile replay has no admitted source fact")
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
	ctx = runtimecorrelation.WithRunID(ctx, runID)
	if err := fixture.RequireRun(ctx, runID); err != nil {
		t.Fatal(err)
	}
	intent := testNonIdempotentActivityIntent(runID, uuid.NewString(), uuid.NewString())
	intent.Tool = "provider.send"
	intent.ActivityID = "provider_send"
	intent.ExecutionMode = executionmode.Mock
	intent.Input = mustActivityInput(map[string]any{})
	profileReads := 0
	reader := scenarioProfileReaderFunc(func(readCtx context.Context, readRunID string) (scenarioexecution.Profile, bool, error) {
		if readCtx.Err() != nil || readRunID != runID {
			t.Fatalf("profile admission lost its caller or run: run=%q err=%v", readRunID, readCtx.Err())
		}
		profileReads++
		return profile, true, nil
	})

	firstBus := &recordingPipelineBus{}
	first := fixture.NewCoordinator(firstBus, PipelineCoordinatorOptions{
		Module: staticSemanticWorkflowModule{source: source}, MockConnectorResponses: base,
		ScenarioExecutionProfiles: reader, EffectiveSourceIdentity: identity, ExecutionPosture: executionposture.MockOnly,
	})
	if err := (pipelineActivityDispatcher{coordinator: first}).executeActivityIntent(ctx, intent); err != nil {
		t.Fatalf("execute initial profiled activity: %v", err)
	}
	if len(firstBus.publishes) != 1 {
		t.Fatalf("initial publications = %#v", firstBus.publishes)
	}
	if profileReads != 1 {
		t.Fatalf("fresh execution profile reads = %d, want 1", profileReads)
	}
	original, found, err := fixture.Persistence.store.LoadActivityAttempt(ctx, activityRequestEventID(intent))
	if err != nil || !found || original.Status != ActivityAttemptStatusSucceeded {
		t.Fatalf("original profiled terminal attempt: %+v found=%t err=%v", original, found, err)
	}
	before, err := fixture.ReadJournal(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if err := (pipelineActivityDispatcher{coordinator: first}).executeActivityIntent(ctx, intent); err != nil {
		t.Fatalf("reuse admitted profiled result: %v", err)
	}
	if profileReads != 2 || len(firstBus.publishes) != 2 || firstBus.publishes[1].ID() != firstBus.publishes[0].ID() {
		t.Fatalf("admitted replay: profile reads=%d publications=%#v", profileReads, firstBus.publishes)
	}

	drifted, err := scenarioexecution.NewEffectiveSourceIdentity(fact, "sha256:"+strings.Repeat("8", 64))
	if err != nil {
		t.Fatal(err)
	}
	restartBus := &recordingPipelineBus{}
	restarted := fixture.NewCoordinator(restartBus, PipelineCoordinatorOptions{
		Module: staticSemanticWorkflowModule{source: source}, MockConnectorResponses: base,
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
	if profileReads != 3 {
		t.Fatalf("rejected replay profile reads = %d, want 3 total", profileReads)
	}
	if after, err := fixture.ReadJournal(ctx, runID); err != nil || after != before {
		t.Fatalf("profile replay or refusal changed native accounting: before=%+v after=%+v err=%v", before, after, err)
	}
	if retained, found, err := fixture.Persistence.store.LoadActivityAttempt(ctx, original.RequestEventID); err != nil || !found || !reflect.DeepEqual(retained, original) {
		t.Fatalf("profile replay or refusal changed terminal evidence: before=%+v after=%+v found=%t err=%v", original, retained, found, err)
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

		runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: "POST", URL: "https://example.invalid/send"}),
		runtimecontracts.WithToolSchemas(empty, output),
	)
	if err != nil {
		t.Fatal(err)
	}
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Tools: map[string]runtimecontracts.ToolSchemaEntry{"provider.send": tool}}), tool
}
