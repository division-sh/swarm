package serveapp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/google/uuid"
)

func testRunlessPlatformLivePublication(t *testing.T, backend string) {
	t.Helper()
	isolateCLIAPIConfigEnv(t)
	root := t.TempDir()
	for _, flow := range []string{".", "other"} {
		if err := os.MkdirAll(filepath.Join(root, flow), 0o700); err != nil {
			t.Fatal(err)
		}
		for file, contents := range map[string]string{
			"manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
			"schema.yaml":   "stages: []\n",
		} {
			writeStandingCandidateFile(t, filepath.Join(root, flow, file), contents)
		}
	}
	writeStandingCandidateFile(t, filepath.Join(root, "schema.yaml"), "stages: []\npins: {outputs: [observed]}\n")
	writeStandingCandidateFile(t, filepath.Join(root, "events.yaml"), "observed:\n")
	writeStandingCandidateFile(t, filepath.Join(root, "nodes.yaml"), "boot-observer:\n  platform.boot:\n    emit: {event: observed}\n")
	writeStandingCandidateFile(t, filepath.Join(root, "other", "agents.yaml"), "observer:\n  role: observer\n  intent: {inline: Observe the boot occurrence.}\n  model: regular\n  subscriptions: [platform.boot]\n  emit_events: []\n")
	opts, _ := issue2564LiveServeHarness(t, backend, root, "")
	var persistence serveRuntimePersistence
	captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) { persistence = p })
	process := startServeRuntimeTestProcess(t, *opts)
	t.Cleanup(func() {
		if code := process.stop(); code != 0 {
			t.Errorf("platform publication serve shutdown=%d\n%s", code, process.outputString())
		}
	})
	process.waitForReadyLine()
	fixture := servedBoundaryFixture{
		Endpoint:   "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc",
		BundleHash: servedEventPublishFixtureBundleHash(t, root), Runtime: servedTestProcessRuntime(t, process),
	}
	runs := map[string]string{}
	for _, flow := range []string{".", "other"} {
		runs[flow] = requireServedLiveLandingRunID(t, fixture.Endpoint, fixture.BundleHash, flow)
	}
	bootLine := regexp.MustCompile(`platform_boot_event_published[^\n]*\(([0-9a-f-]{36})\)`).FindStringSubmatch(process.outputString())
	if len(bootLine) != 2 {
		t.Fatal("served boot did not expose its exact publication identity", process.outputString())
	}
	bootID := bootLine[1]
	ctx := servedRuntimeProofAuthorActivityContext(t, fixture.Runtime, fixture.BundleHash)
	reader := persistence.deps.EventBusDurable.PreparedEvents
	prepared, found, err := reader.LoadPreparedPublishEvent(ctx, bootID)
	if err != nil || !found || prepared.Event.Event().RunID() != "" || len(prepared.DeliveryRoutes) != 2 {
		for flow := range runs {
			subscribers, lookupErr := fixture.Runtime.Bus.RouteTable().PubsubDeclarationDefinitions(flow, []string{"platform.boot"})
			t.Logf("flow=%s authored subscribers=%+v err=%v", flow, subscribers, lookupErr)
		}
		t.Fatalf("native platform publication: found=%t run=%q routes=%+v err=%v", found, prepared.Event.Event().RunID(), prepared.DeliveryRoutes, err)
	}
	for _, route := range prepared.DeliveryRoutes {
		if runID := runs[route.Target.Route().FlowID]; runID == "" || route.Context.RunID != runID {
			t.Fatal("platform delivery escaped its subscribing flow's live run", route, runs)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		var boot operatorread.OperatorEventFull
		requireServedJSONRPCResult(t, fixture.Endpoint, "event.get", map[string]any{"event_id": bootID}, &boot)
		complete := len(boot.Deliveries) == 2
		for _, delivery := range boot.Deliveries {
			if delivery.Status == "dead_lettered" || delivery.Failure != nil {
				t.Fatal("live platform receiver failed", delivery)
			}
			complete = complete && delivery.Status == "delivered"
		}
		if complete {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("platform delivery did not settle in both live runs", boot)
		}
		time.Sleep(10 * time.Millisecond)
	}
	observed := false
	for flow, runID := range runs {
		var page operatorread.OperatorEventListResult
		requireServedJSONRPCResult(t, fixture.Endpoint, "event.list", map[string]any{"filter": map[string]any{"run_id": runID}, "limit": 100}, &page)
		for _, event := range page.Events {
			if event.SourceEventID == bootID && event.EventName == "observed" {
				if flow != "." || event.RunID != runID {
					t.Fatal("node output escaped its subscribing flow's live run", event)
				}
				observed = true
			}
		}
	}
	if !observed {
		t.Fatal("authored node output lost its receiver-owned run")
	}
	journal := prepared.Event.Event()
	if err := fixture.Runtime.Bus.Publish(ctx, journal); err != nil {
		t.Fatal("exact platform replay failed", err)
	}
	// Hold the existing prepared native command across a real public pause.
	// Its old selection must roll back the entire new occurrence, not strand it.
	fresh := eventtest.RuntimeControlWithRoutingSourceAndMode(uuid.NewString(), journal.Type(), "runtime", "", journal.Payload(), 0, "", "", journal.Envelope(), journal.RoutingSource(), time.Now().UTC(), journal.ExecutionMode())
	plans, err := fixture.Runtime.Bus.PrepareEnginePublications(ctx, []engine.EmitIntent{{Event: fresh}})
	if err != nil || len(plans) != 1 {
		t.Fatalf("prepare exact live platform occurrence: plans=%d err=%v", len(plans), err)
	}
	defer func() {
		if err := fixture.Runtime.Bus.ReleaseEnginePublications(context.WithoutCancel(ctx), plans); err != nil {
			t.Error(err)
		}
	}()
	plan, ok := plans[0].(runtimebus.EnginePublicationPlan)
	if !ok {
		t.Fatal("platform preparation lost its native publication command")
	}
	requireServedJSONRPCResult(t, fixture.Endpoint, "run.pause", map[string]any{"run_id": runs["other"]}, &map[string]any{})
	owner, ok := fixture.Runtime.Bus.Store().(runtimebus.CommitPublicationOwner)
	if !ok {
		t.Fatal("served publication lost its original native commit owner")
	}
	unavailable := eventtest.RuntimeControlWithRoutingSourceAndMode(uuid.NewString(), journal.Type(), "runtime", "", journal.Payload(), 0, "", "", journal.Envelope(), journal.RoutingSource(), time.Now().UTC(), journal.ExecutionMode())
	if err := fixture.Runtime.Bus.Publish(ctx, unavailable); err != nil {
		t.Fatal("platform publication to paused landing run failed", err)
	}
	paused, found, err := reader.LoadPreparedPublishEvent(ctx, unavailable.ID())
	if err != nil || !found {
		t.Fatal("platform publication to paused landing run was not retained", found, err)
	}
	pausedRoute := false
	for _, route := range paused.DeliveryRoutes {
		if route.Target.Route().FlowID == "other" && route.Context.RunID == runs["other"] {
			pausedRoute = true
		}
	}
	if !pausedRoute {
		t.Fatal("paused platform subscriber did not retain its exact landing run", paused.DeliveryRoutes)
	}
	var pausedEvent operatorread.OperatorEventFull
	requireServedJSONRPCResult(t, fixture.Endpoint, "event.get", map[string]any{"event_id": unavailable.ID()}, &pausedEvent)
	for _, delivery := range pausedEvent.Deliveries {
		if delivery.Target.FlowID == "other" && (delivery.Status == "delivered" || delivery.Terminal) {
			t.Fatal("paused platform delivery executed before resume", delivery)
		}
	}
	requireServedJSONRPCResult(t, fixture.Endpoint, "run.stop", map[string]any{"run_id": runs["other"]}, &map[string]any{})
	if _, err := owner.CommitPublication(ctx, plan.PublicationCommand()); !errors.Is(err, runtimebus.ErrAPIEventLiveRunChanged) {
		t.Fatal("stopped landing selection was not refused atomically", err)
	}
	if _, found, err := reader.LoadPreparedPublishEvent(ctx, fresh.ID()); err != nil || found {
		t.Fatal("refused platform occurrence retained partial publication", found, err)
	}
	untargeted := journal.Envelope()
	untargeted.Target = events.RouteIdentity{}
	untargeted.TargetSet = nil
	missing := eventtest.RuntimeControlWithRoutingSourceAndMode(uuid.NewString(), journal.Type(), "runtime", "", journal.Payload(), 0, "", "", untargeted, journal.RoutingSource(), time.Now().UTC(), journal.ExecutionMode())
	if err := fixture.Runtime.Bus.Publish(ctx, missing); err != nil {
		t.Fatal("no-run platform publication did not record its outcome", err)
	}
	remaining, found, err := reader.LoadPreparedPublishEvent(ctx, missing.ID())
	if err != nil || !found || len(remaining.DeliveryRoutes) != 1 || remaining.DeliveryRoutes[0].Context.RunID != runs["."] {
		t.Fatal("no-run outcome lost the other flow's live delivery", found, remaining.DeliveryRoutes, err)
	}
	db, _, _ := selectedRuntimeStoreForTest(t, persistence)
	var failure string
	if err := db.QueryRowContext(ctx, `SELECT CAST(failure AS TEXT) FROM dead_letters WHERE original_event_id=$1`, missing.ID()).Scan(&failure); err != nil || !strings.Contains(failure, "target_unreachable_terminated") {
		t.Fatal("no-run subscriber lost its typed dead letter", failure, err)
	}
}
