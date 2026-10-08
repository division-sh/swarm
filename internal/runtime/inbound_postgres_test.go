package runtime_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeagentidentitytest "github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	runtimeingress "github.com/division-sh/swarm/internal/runtime/ingress"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	eventtestsql "github.com/division-sh/swarm/internal/store/testsql"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/flowactivationfixture"
)

func boundedInboundTestSource(t *testing.T) semanticview.Source {
	t.Helper()
	bundle := loadRuntimeTempBundle(t, map[string]string{
		"schema.yaml":                   "name: bounded-standing-connector\n",
		"bounded_inbound/schema.yaml":   "name: bounded_inbound\nstages:\n  active: {}\n",
		"bounded_inbound/entities.yaml": "bounded_entity: {}\n",
	})
	return semanticview.Wrap(bundle)
}

func newBoundedInboundTestEventBus(t *testing.T, selected runtimebus.EventStore, opts runtimebus.EventBusOptions, differentEvents ...string) (*runtimebus.EventBus, error) {
	t.Helper()
	opts.ContractBundle = boundedInboundTestSource(t)
	return newScopedTestEventBus(t, selected, opts, differentEvents...)
}

func seedBoundedInboundFlow(t *testing.T, ctx context.Context, selected interface {
	CommitFlowInstanceActivation(context.Context, runtimebus.FlowInstanceActivationCommand) (runtimepipeline.CommittedFlowInstanceActivation, error)
}, runID, entityID, path, slug string) {
	t.Helper()
	now := time.Now().UTC()
	ctx = runtimeeffects.WithExecutionMode(runtimecorrelation.WithRunID(ctx, runID), runtimeeffects.ExecutionModeLive)
	source := boundedInboundTestSource(t)
	child, err := runtimeflowidentity.KeylessChild(source,
		runtimeflowidentity.Stored(source, ".", runID, runID, runID, ""), boundedProviderFlowID)
	if err != nil || child.InstancePath != path {
		t.Fatalf("prepare bounded component parent: child=%+v path=%s err=%v", child, path, err)
	}
	// The bounded gateway control uses a prepared component aggregate, not
	// public standing construction or process attachment qualification. Its
	// parent coordinate still comes from the canonical keyless identity owner.
	// Constructor shape is fixture-local; header version belongs to the selected artifact.
	fact, found := runtimecorrelation.SourceArtifactFactFromContext(ctx)
	if !found {
		t.Fatal("bounded component header requires its admitted source fact")
	}
	execution, err := runtimecontracts.SourceExecutionIdentity(fact.BundleHash())
	if err != nil {
		t.Fatal(err)
	}
	command, err := flowactivationfixture.Command(ctx, runtimepipeline.WorkflowInstance{
		InstanceID: path, StorageRef: path, EntityID: entityID, EntityType: "bounded_entity",
		ParentFlowID: child.ParentRoute.FlowID, ParentFlowInstance: child.ParentRoute.FlowInstance, ParentEntityID: child.ParentEntityID,
		WorkflowName: boundedProviderFlowID, WorkflowVersion: execution.WorkflowVersion,
		Slug: slug, Name: "Customer A", CurrentState: "active", StageDefined: true,
		CreatedAt: now, EnteredStageAt: now, Fields: map[string]any{},
	}, runtimepipeline.WorkflowLifecycleMutationPlan{}, now)
	if err != nil {
		t.Fatal(err)
	}
	result, err := selected.CommitFlowInstanceActivation(ctx, command)
	if err != nil || !result.Acknowledged || !result.Created {
		t.Fatalf("commit inbound fixture aggregate: acknowledged=%t created=%t err=%v", result.Acknowledged, result.Created, err)
	}
}

func TestInboundGateway_GitHubPausedRuntimePersistsAndReleasesSubscribedDispatch(t *testing.T) {
	runID, entityID := boundedInboundTestCoordinates()
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	const (
		flowInstance      = boundedProviderFlowID
		entitySlug        = "customer-a"
		provider          = "github"
		webhookSecret     = "github-secret"
		providerEventID   = "delivery-123"
		agentID           = "github-webhook-subscriber"
		providerEventName = "inbound.github.raw.push"
	)
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	target := seedPostgresInboundGatewayRuntime(t, ctx, pg, runID, entityID, flowInstance, entitySlug, provider, webhookSecret, agentID)

	bus, err := newBoundedInboundTestEventBus(t, pg, runtimebus.EventBusOptions{}, providerEventName)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	installInboundStandingRecoveryOwner(t, bus, runID, provider)
	controller := runtimeingress.NewController(pg, bus, runtimeingress.Options{ExecutionPosture: executionposture.Live})
	t.Cleanup(runtimebus.ResumeRuntimeIngress)
	bus.SetRuntimeIngressDispatchGate(controller)

	eventType := events.EventType(providerEventName)
	ch := subscribeInboundGatewayAgent(t, bus, runID, agentID, flowInstance, eventType)

	if _, err := controller.Pause(context.Background(), runtimeingress.TransitionRequest{
		Reason:       "test_pause",
		ControlledBy: "test",
	}); err != nil {
		t.Fatalf("Pause: %v", err)
	}

	g := newTestInboundGateway(t, bus, nil, nil, pg)
	g.SetRuntimeIngress(controller)

	body := []byte(`{"zen":"Keep it logically awesome."}`)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/github", strings.NewReader(string(body)))
	req.Header.Set("X-Hub-Signature-256", githubWebhookSignature(webhookSecret, body))
	req.Header.Set("X-GitHub-Delivery", providerEventID)
	req.Header.Set("X-GitHub-Event", "push")
	rec := httptest.NewRecorder()
	handleBoundedProviderDelivery(t, g, bus, target, rec, req, provider, webhookSecret)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
	}
	if got := countInboundMarkers(t, ctx, pg, target, provider, providerEventID); got != 1 {
		t.Fatalf("inbound marker rows = %d, want 1", got)
	}
	eventID := loadInboundProviderEventID(t, ctx, pg, target, provider, providerEventName, providerEventID)
	if got := countInboundProviderEvents(t, ctx, pg, runID, providerEventName, providerEventID); got != 1 {
		t.Fatalf("provider event rows = %d, want 1", got)
	}
	requireNoInboundBusEvent(t, ch, "paused GitHub webhook before resume")
	if got := countInboundAgentDeliveries(t, ctx, pg, eventID, agentID); got != 1 {
		t.Fatalf("agent delivery rows while paused = %d, want 1", got)
	}
	if got := loadInboundAgentDeliveryStatus(t, ctx, pg, eventID, agentID); got != "pending" {
		t.Fatalf("agent delivery status while paused = %q, want pending", got)
	}
	if got := countInboundPipelineReceipts(t, ctx, pg, runID, eventID); got != 0 {
		t.Fatalf("pipeline receipts while paused = %d, want 0", got)
	}
	if got := countInboundNonPlatformReceipts(t, ctx, pg, runID, eventID); got != 0 {
		t.Fatalf("agent receipts while paused = %d, want 0", got)
	}

	resumed, err := controller.Resume(context.Background(), runtimeingress.TransitionRequest{
		Reason:       "test_resume",
		ControlledBy: "test",
	})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if resumed.ReleasedCount != 1 {
		t.Fatalf("released count = %d, want 1", resumed.ReleasedCount)
	}
	got := requireInboundBusEvent(t, ch, "paused GitHub webhook release after resume")
	if got.ID() != eventID {
		t.Fatalf("delivered event = %s, want %s", got.ID(), eventID)
	}
	if got.Type() != eventType {
		t.Fatalf("delivered event type = %s, want %s", got.Type(), eventType)
	}
	requireNoInboundBusEvent(t, ch, "paused GitHub webhook releases exactly once")
	if got := countInboundPipelineReceipts(t, ctx, pg, runID, eventID); got != 1 {
		t.Fatalf("pipeline receipts after resume = %d, want 1", got)
	}
	if got := countInboundMarkers(t, ctx, pg, target, provider, providerEventID); got != 1 {
		t.Fatalf("inbound marker rows after resume = %d, want 1", got)
	}
	if got := countInboundProviderEvents(t, ctx, pg, runID, providerEventName, providerEventID); got != 1 {
		t.Fatalf("provider event rows after resume = %d, want 1", got)
	}
	unsubscribeAndWaitForInboundBusQuiescence(t, bus, runID, agentID, flowInstance)
}

func TestInboundGateway_SlackPausedRuntimePersistsAndReleasesSubscribedDispatch(t *testing.T) {
	runID, entityID := boundedInboundTestCoordinates()
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	const (
		flowInstance      = boundedProviderFlowID
		entitySlug        = "customer-a"
		provider          = "slack"
		webhookSecret     = "slack-secret"
		providerEventID   = "Ev123ABC456"
		agentID           = "slack-webhook-subscriber"
		providerEventName = "inbound.slack.message"
	)
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	target := seedPostgresInboundGatewayRuntime(t, ctx, pg, runID, entityID, flowInstance, entitySlug, provider, webhookSecret, agentID)

	bus, err := newBoundedInboundTestEventBus(t, pg, runtimebus.EventBusOptions{}, providerEventName)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	installInboundStandingRecoveryOwner(t, bus, runID, provider)
	controller := runtimeingress.NewController(pg, bus, runtimeingress.Options{ExecutionPosture: executionposture.Live})
	t.Cleanup(runtimebus.ResumeRuntimeIngress)
	bus.SetRuntimeIngressDispatchGate(controller)

	eventType := events.EventType(providerEventName)
	ch := subscribeInboundGatewayAgent(t, bus, runID, agentID, flowInstance, eventType)

	if _, err := controller.Pause(context.Background(), runtimeingress.TransitionRequest{
		Reason:       "test_pause",
		ControlledBy: "test",
	}); err != nil {
		t.Fatalf("Pause: %v", err)
	}

	g := newTestInboundGateway(t, bus, nil, nil, pg)
	g.SetRuntimeIngress(controller)

	body := []byte(`{"type":"event_callback","event_id":"Ev123ABC456","event":{"type":"message","text":"hello"}}`)
	timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/slack", strings.NewReader(string(body)))
	req.Header.Set("X-Slack-Request-Timestamp", timestamp)
	req.Header.Set("X-Slack-Signature", slackWebhookSignature(webhookSecret, timestamp, body))
	rec := httptest.NewRecorder()
	handleBoundedProviderDelivery(t, g, bus, target, rec, req, provider, webhookSecret)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
	}
	if got := countInboundMarkers(t, ctx, pg, target, provider, providerEventID); got != 1 {
		t.Fatalf("inbound marker rows = %d, want 1", got)
	}
	eventID := loadInboundProviderEventID(t, ctx, pg, target, provider, providerEventName, providerEventID)
	if got := countInboundProviderEvents(t, ctx, pg, runID, providerEventName, providerEventID); got != 1 {
		t.Fatalf("provider event rows = %d, want 1", got)
	}
	requireNoInboundBusEvent(t, ch, "paused Slack webhook before resume")
	if got := countInboundAgentDeliveries(t, ctx, pg, eventID, agentID); got != 1 {
		t.Fatalf("agent delivery rows while paused = %d, want 1", got)
	}
	if got := loadInboundAgentDeliveryStatus(t, ctx, pg, eventID, agentID); got != "pending" {
		t.Fatalf("agent delivery status while paused = %q, want pending", got)
	}
	if got := countInboundPipelineReceipts(t, ctx, pg, runID, eventID); got != 0 {
		t.Fatalf("pipeline receipts while paused = %d, want 0", got)
	}
	if got := countInboundNonPlatformReceipts(t, ctx, pg, runID, eventID); got != 0 {
		t.Fatalf("agent receipts while paused = %d, want 0", got)
	}

	resumed, err := controller.Resume(context.Background(), runtimeingress.TransitionRequest{
		Reason:       "test_resume",
		ControlledBy: "test",
	})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if resumed.ReleasedCount != 1 {
		t.Fatalf("released count = %d, want 1", resumed.ReleasedCount)
	}
	got := requireInboundBusEvent(t, ch, "paused Slack webhook release after resume")
	if got.ID() != eventID {
		t.Fatalf("delivered event = %s, want %s", got.ID(), eventID)
	}
	if got.Type() != eventType {
		t.Fatalf("delivered event type = %s, want %s", got.Type(), eventType)
	}
	requireNoInboundBusEvent(t, ch, "paused Slack webhook releases exactly once")
	if got := countInboundPipelineReceipts(t, ctx, pg, runID, eventID); got != 1 {
		t.Fatalf("pipeline receipts after resume = %d, want 1", got)
	}
	if got := countInboundMarkers(t, ctx, pg, target, provider, providerEventID); got != 1 {
		t.Fatalf("inbound marker rows after resume = %d, want 1", got)
	}
	if got := countInboundProviderEvents(t, ctx, pg, runID, providerEventName, providerEventID); got != 1 {
		t.Fatalf("provider event rows after resume = %d, want 1", got)
	}
	unsubscribeAndWaitForInboundBusQuiescence(t, bus, runID, agentID, flowInstance)
}

func TestInboundGateway_StripePausedRuntimePersistsAndReleasesSubscribedDispatch(t *testing.T) {
	runID, entityID := boundedInboundTestCoordinates()
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	const (
		flowInstance      = boundedProviderFlowID
		entitySlug        = "customer-a"
		provider          = "stripe"
		webhookSecret     = "stripe-secret"
		providerEventID   = "evt_123"
		agentID           = "stripe-webhook-subscriber"
		providerEventName = "inbound.stripe"
	)
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	target := seedPostgresInboundGatewayRuntime(t, ctx, pg, runID, entityID, flowInstance, entitySlug, provider, webhookSecret, agentID)

	bus, err := newBoundedInboundTestEventBus(t, pg, runtimebus.EventBusOptions{}, providerEventName)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	installInboundStandingRecoveryOwner(t, bus, runID, provider)
	controller := runtimeingress.NewController(pg, bus, runtimeingress.Options{ExecutionPosture: executionposture.Live})
	t.Cleanup(runtimebus.ResumeRuntimeIngress)
	bus.SetRuntimeIngressDispatchGate(controller)

	eventType := events.EventType(providerEventName)
	ch := subscribeInboundGatewayAgent(t, bus, runID, agentID, flowInstance, eventType)

	if _, err := controller.Pause(context.Background(), runtimeingress.TransitionRequest{
		Reason:       "test_pause",
		ControlledBy: "test",
	}); err != nil {
		t.Fatalf("Pause: %v", err)
	}

	g := newTestInboundGateway(t, bus, nil, nil, pg)
	g.SetRuntimeIngress(controller)

	body := []byte(`{"id":"evt_123","type":"invoice.paid","data":{"object":{"id":"in_123"}}}`)
	timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/stripe", strings.NewReader(string(body)))
	req.Header.Set("Stripe-Signature", stripeWebhookSignature(webhookSecret, timestamp, body))
	rec := httptest.NewRecorder()
	handleBoundedProviderDelivery(t, g, bus, target, rec, req, provider, webhookSecret)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
	}
	if got := countInboundMarkers(t, ctx, pg, target, provider, providerEventID); got != 1 {
		t.Fatalf("inbound marker rows = %d, want 1", got)
	}
	eventID := loadInboundProviderEventID(t, ctx, pg, target, provider, providerEventName, providerEventID)
	if got := countInboundProviderEvents(t, ctx, pg, runID, providerEventName, providerEventID); got != 1 {
		t.Fatalf("provider event rows = %d, want 1", got)
	}
	if got := loadInboundProviderEventPayloadField(t, ctx, pg, eventID, "provider_event_type"); got != "invoice_paid" {
		t.Fatalf("provider_event_type = %q, want invoice_paid", got)
	}
	requireNoInboundBusEvent(t, ch, "paused Stripe webhook before resume")
	if got := countInboundAgentDeliveries(t, ctx, pg, eventID, agentID); got != 1 {
		t.Fatalf("agent delivery rows while paused = %d, want 1", got)
	}
	if got := loadInboundAgentDeliveryStatus(t, ctx, pg, eventID, agentID); got != "pending" {
		t.Fatalf("agent delivery status while paused = %q, want pending", got)
	}
	if got := countInboundPipelineReceipts(t, ctx, pg, runID, eventID); got != 0 {
		t.Fatalf("pipeline receipts while paused = %d, want 0", got)
	}
	if got := countInboundNonPlatformReceipts(t, ctx, pg, runID, eventID); got != 0 {
		t.Fatalf("agent receipts while paused = %d, want 0", got)
	}

	resumed, err := controller.Resume(context.Background(), runtimeingress.TransitionRequest{
		Reason:       "test_resume",
		ControlledBy: "test",
	})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if resumed.ReleasedCount != 1 {
		t.Fatalf("released count = %d, want 1", resumed.ReleasedCount)
	}
	got := requireInboundBusEvent(t, ch, "paused Stripe webhook release after resume")
	if got.ID() != eventID {
		t.Fatalf("delivered event = %s, want %s", got.ID(), eventID)
	}
	if got.Type() != eventType {
		t.Fatalf("delivered event type = %s, want %s", got.Type(), eventType)
	}
	requireNoInboundBusEvent(t, ch, "paused Stripe webhook releases exactly once")
	if got := countInboundPipelineReceipts(t, ctx, pg, runID, eventID); got != 1 {
		t.Fatalf("pipeline receipts after resume = %d, want 1", got)
	}
	if got := countInboundMarkers(t, ctx, pg, target, provider, providerEventID); got != 1 {
		t.Fatalf("inbound marker rows after resume = %d, want 1", got)
	}
	if got := countInboundProviderEvents(t, ctx, pg, runID, providerEventName, providerEventID); got != 1 {
		t.Fatalf("provider event rows after resume = %d, want 1", got)
	}
	unsubscribeAndWaitForInboundBusQuiescence(t, bus, runID, agentID, flowInstance)
}

func TestInboundGateway_StripeSQLitePersistsConfiguredManifestDelivery(t *testing.T) {
	runID, entityID := boundedInboundTestCoordinates()
	const (
		flowInstance      = boundedProviderFlowID
		entitySlug        = "customer-a"
		provider          = "stripe"
		webhookSecret     = "stripe-secret"
		providerEventID   = "evt_456"
		agentID           = "stripe-sqlite-webhook-subscriber"
		providerEventName = "inbound.stripe"
	)
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
	sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
	target := seedSQLiteInboundGatewayRuntime(t, ctx, sqliteStore, runID, entityID, flowInstance, entitySlug, provider, webhookSecret, agentID)

	bus, err := newBoundedInboundTestEventBus(t, sqliteStore, runtimebus.EventBusOptions{}, providerEventName)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := subscribeInboundGatewayAgent(t, bus, runID, agentID, flowInstance, events.EventType(providerEventName))

	g := newTestInboundGateway(t, bus, nil, nil, sqliteStore)

	body := []byte(`{"id":"evt_456","type":"customer.created","data":{"object":{"id":"cus_123"}}}`)
	timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/stripe", strings.NewReader(string(body)))
	req.Header.Set("Stripe-Signature", stripeWebhookSignature(webhookSecret, timestamp, body))
	rec := httptest.NewRecorder()
	handleBoundedProviderDelivery(t, g, bus, target, rec, req, provider, webhookSecret)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
	}
	if got := countInboundMarkers(t, ctx, sqliteStore, target, provider, providerEventID); got != 1 {
		t.Fatalf("inbound marker rows = %d, want 1", got)
	}
	eventID := loadInboundProviderEventID(t, ctx, sqliteStore, target, provider, providerEventName, providerEventID)
	if got := countInboundProviderEvents(t, ctx, sqliteStore, runID, providerEventName, providerEventID); got != 1 {
		t.Fatalf("provider event rows = %d, want 1", got)
	}
	if got := loadInboundProviderEventPayloadField(t, ctx, sqliteStore, eventID, "provider_event_type"); got != "customer_created" {
		t.Fatalf("provider_event_type = %q, want customer_created", got)
	}
	if got := countInboundAgentDeliveries(t, ctx, sqliteStore, eventID, agentID); got != 1 {
		t.Fatalf("agent delivery rows = %d, want 1", got)
	}
	select {
	case got := <-ch:
		if got.ID() != eventID || got.Type() != events.EventType(providerEventName) {
			t.Fatalf("delivered event = %s/%s, want %s/%s", got.ID(), got.Type(), eventID, providerEventName)
		}
		_ = got.Complete()
	case <-time.After(5 * time.Second):
		t.Fatal("Stripe SQLite post-commit dispatch did not arrive")
	}
	unsubscribeAndWaitForInboundBusQuiescence(t, bus, runID, agentID, flowInstance)
}

func TestInboundGateway_TwilioPostgresPersistsConfiguredManifestDelivery(t *testing.T) {
	runID, entityID := boundedInboundTestCoordinates()
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	const (
		flowInstance      = boundedProviderFlowID
		entitySlug        = "customer-a"
		provider          = "twilio"
		webhookSecret     = "twilio-secret"
		providerEventID   = "SM1234567890abcdef"
		agentID           = "twilio-webhook-subscriber"
		providerEventName = "inbound.twilio"
	)
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	target := seedPostgresInboundGatewayRuntime(t, ctx, pg, runID, entityID, flowInstance, entitySlug, provider, webhookSecret, agentID)

	bus, err := newBoundedInboundTestEventBus(t, pg, runtimebus.EventBusOptions{}, providerEventName)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := subscribeInboundGatewayAgent(t, bus, runID, agentID, flowInstance, events.EventType(providerEventName))

	g := newTestInboundGateway(t, bus, nil, nil, pg)

	requestURL := "https://example.com/webhooks/customer-a/twilio?tenant=alpha"
	form := url.Values{
		"Body":       {"hello from twilio"},
		"From":       {"+15551234567"},
		"MessageSid": {providerEventID},
		"To":         {"+15557654321"},
	}
	req := newSignedTwilioRequest(requestURL, webhookSecret, form)
	rec := httptest.NewRecorder()
	handleBoundedProviderDelivery(t, g, bus, target, rec, req, provider, webhookSecret)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
	}
	if got := countInboundMarkers(t, ctx, pg, target, provider, providerEventID); got != 1 {
		t.Fatalf("inbound marker rows = %d, want 1", got)
	}
	eventID := loadInboundProviderEventID(t, ctx, pg, target, provider, providerEventName, providerEventID)
	if got := countInboundProviderEvents(t, ctx, pg, runID, providerEventName, providerEventID); got != 1 {
		t.Fatalf("provider event rows = %d, want 1", got)
	}
	if got := loadInboundProviderEventPayloadField(t, ctx, pg, eventID, "provider_event_type"); got != "message_received" {
		t.Fatalf("provider_event_type = %q, want message_received", got)
	}
	if got := countInboundAgentDeliveries(t, ctx, pg, eventID, agentID); got != 1 {
		t.Fatalf("agent delivery rows = %d, want 1", got)
	}
	select {
	case got := <-ch:
		if got.ID() != eventID || got.Type() != events.EventType(providerEventName) {
			t.Fatalf("delivered event = %s/%s, want %s/%s", got.ID(), got.Type(), eventID, providerEventName)
		}
		_ = got.Complete()
	case <-time.After(5 * time.Second):
		t.Fatal("Twilio PostgreSQL post-commit dispatch did not arrive")
	}
	unsubscribeAndWaitForInboundBusQuiescence(t, bus, runID, agentID, flowInstance)
}

func TestInboundGateway_TwilioSQLitePersistsConfiguredManifestDelivery(t *testing.T) {
	runID, entityID := boundedInboundTestCoordinates()
	const (
		flowInstance      = boundedProviderFlowID
		entitySlug        = "customer-a"
		provider          = "twilio"
		webhookSecret     = "twilio-secret"
		providerEventID   = "SMabcdef1234567890"
		agentID           = "twilio-sqlite-webhook-subscriber"
		providerEventName = "inbound.twilio"
	)
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
	sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
	target := seedSQLiteInboundGatewayRuntime(t, ctx, sqliteStore, runID, entityID, flowInstance, entitySlug, provider, webhookSecret, agentID)

	bus, err := newBoundedInboundTestEventBus(t, sqliteStore, runtimebus.EventBusOptions{}, providerEventName)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := subscribeInboundGatewayAgent(t, bus, runID, agentID, flowInstance, events.EventType(providerEventName))

	g := newTestInboundGateway(t, bus, nil, nil, sqliteStore)

	requestURL := "https://example.com/webhooks/customer-a/twilio?tenant=alpha"
	form := url.Values{
		"Body":       {"hello from sqlite"},
		"From":       {"+15551234567"},
		"MessageSid": {providerEventID},
		"To":         {"+15557654321"},
	}
	req := newSignedTwilioRequest(requestURL, webhookSecret, form)
	rec := httptest.NewRecorder()
	handleBoundedProviderDelivery(t, g, bus, target, rec, req, provider, webhookSecret)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
	}
	if got := countInboundMarkers(t, ctx, sqliteStore, target, provider, providerEventID); got != 1 {
		t.Fatalf("inbound marker rows = %d, want 1", got)
	}
	eventID := loadInboundProviderEventID(t, ctx, sqliteStore, target, provider, providerEventName, providerEventID)
	if got := countInboundProviderEvents(t, ctx, sqliteStore, runID, providerEventName, providerEventID); got != 1 {
		t.Fatalf("provider event rows = %d, want 1", got)
	}
	if got := loadInboundProviderEventPayloadField(t, ctx, sqliteStore, eventID, "provider_event_type"); got != "message_received" {
		t.Fatalf("provider_event_type = %q, want message_received", got)
	}
	if got := countInboundAgentDeliveries(t, ctx, sqliteStore, eventID, agentID); got != 1 {
		t.Fatalf("agent delivery rows = %d, want 1", got)
	}
	select {
	case got := <-ch:
		if got.ID() != eventID || got.Type() != events.EventType(providerEventName) {
			t.Fatalf("delivered event = %s/%s, want %s/%s", got.ID(), got.Type(), eventID, providerEventName)
		}
		_ = got.Complete()
	case <-time.After(5 * time.Second):
		t.Fatal("Twilio SQLite post-commit dispatch did not arrive")
	}
	unsubscribeAndWaitForInboundBusQuiescence(t, bus, runID, agentID, flowInstance)
}

func TestInboundGateway_ShopifyPostgresPersistsConfiguredManifestDelivery(t *testing.T) {
	runID, entityID := boundedInboundTestCoordinates()
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	const (
		flowInstance      = boundedProviderFlowID
		entitySlug        = "customer-a"
		provider          = "shopify"
		webhookSecret     = "shopify-secret"
		providerEventID   = "webhook-123"
		agentID           = "shopify-webhook-subscriber"
		providerEventName = "inbound.shopify"
	)
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	target := seedPostgresInboundGatewayRuntime(t, ctx, pg, runID, entityID, flowInstance, entitySlug, provider, webhookSecret, agentID)

	bus, err := newBoundedInboundTestEventBus(t, pg, runtimebus.EventBusOptions{}, providerEventName)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := subscribeInboundGatewayAgent(t, bus, runID, agentID, flowInstance, events.EventType(providerEventName))

	g := newTestInboundGateway(t, bus, nil, nil, pg)

	body := []byte(`{"id":123,"line_items":[{"sku":"abc"}]}`)
	req := newSignedShopifyRequest("/webhooks/customer-a/shopify", webhookSecret, body)
	req.Header.Set("X-Shopify-Webhook-Id", providerEventID)
	req.Header.Set("X-Shopify-Topic", "orders/create")
	rec := httptest.NewRecorder()
	handleBoundedProviderDelivery(t, g, bus, target, rec, req, provider, webhookSecret)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
	}
	if got := countInboundMarkers(t, ctx, pg, target, provider, providerEventID); got != 1 {
		t.Fatalf("inbound marker rows = %d, want 1", got)
	}
	eventID := loadInboundProviderEventID(t, ctx, pg, target, provider, providerEventName, providerEventID)
	if got := countInboundProviderEvents(t, ctx, pg, runID, providerEventName, providerEventID); got != 1 {
		t.Fatalf("provider event rows = %d, want 1", got)
	}
	if got := loadInboundProviderEventPayloadField(t, ctx, pg, eventID, "provider_event_type"); got != "orders_create" {
		t.Fatalf("provider_event_type = %q, want orders_create", got)
	}
	if got := countInboundAgentDeliveries(t, ctx, pg, eventID, agentID); got != 1 {
		t.Fatalf("agent delivery rows = %d, want 1", got)
	}
	select {
	case got := <-ch:
		if got.ID() != eventID || got.Type() != events.EventType(providerEventName) {
			t.Fatalf("delivered event = %s/%s, want %s/%s", got.ID(), got.Type(), eventID, providerEventName)
		}
		_ = got.Complete()
	case <-time.After(5 * time.Second):
		t.Fatal("Shopify PostgreSQL post-commit dispatch did not arrive")
	}
	unsubscribeAndWaitForInboundBusQuiescence(t, bus, runID, agentID, flowInstance)
}

func TestInboundGateway_ShopifySQLitePersistsConfiguredManifestDelivery(t *testing.T) {
	runID, entityID := boundedInboundTestCoordinates()
	const (
		flowInstance      = boundedProviderFlowID
		entitySlug        = "customer-a"
		provider          = "shopify"
		webhookSecret     = "shopify-secret"
		providerEventID   = "webhook-456"
		agentID           = "shopify-sqlite-webhook-subscriber"
		providerEventName = "inbound.shopify"
	)
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
	sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
	target := seedSQLiteInboundGatewayRuntime(t, ctx, sqliteStore, runID, entityID, flowInstance, entitySlug, provider, webhookSecret, agentID)

	bus, err := newBoundedInboundTestEventBus(t, sqliteStore, runtimebus.EventBusOptions{}, providerEventName)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := subscribeInboundGatewayAgent(t, bus, runID, agentID, flowInstance, events.EventType(providerEventName))

	g := newTestInboundGateway(t, bus, nil, nil, sqliteStore)

	body := []byte(`{"id":456,"line_items":[{"sku":"xyz"}]}`)
	req := newSignedShopifyRequest("/webhooks/customer-a/shopify", webhookSecret, body)
	req.Header.Set("X-Shopify-Webhook-Id", providerEventID)
	req.Header.Set("X-Shopify-Topic", "orders/updated")
	rec := httptest.NewRecorder()
	handleBoundedProviderDelivery(t, g, bus, target, rec, req, provider, webhookSecret)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
	}
	if got := countInboundMarkers(t, ctx, sqliteStore, target, provider, providerEventID); got != 1 {
		t.Fatalf("inbound marker rows = %d, want 1", got)
	}
	eventID := loadInboundProviderEventID(t, ctx, sqliteStore, target, provider, providerEventName, providerEventID)
	if got := countInboundProviderEvents(t, ctx, sqliteStore, runID, providerEventName, providerEventID); got != 1 {
		t.Fatalf("provider event rows = %d, want 1", got)
	}
	if got := loadInboundProviderEventPayloadField(t, ctx, sqliteStore, eventID, "provider_event_type"); got != "orders_updated" {
		t.Fatalf("provider_event_type = %q, want orders_updated", got)
	}
	if got := countInboundAgentDeliveries(t, ctx, sqliteStore, eventID, agentID); got != 1 {
		t.Fatalf("agent delivery rows = %d, want 1", got)
	}
	select {
	case got := <-ch:
		if got.ID() != eventID || got.Type() != events.EventType(providerEventName) {
			t.Fatalf("delivered event = %s/%s, want %s/%s", got.ID(), got.Type(), eventID, providerEventName)
		}
		_ = got.Complete()
	case <-time.After(5 * time.Second):
		t.Fatal("Shopify SQLite post-commit dispatch did not arrive")
	}
	unsubscribeAndWaitForInboundBusQuiescence(t, bus, runID, agentID, flowInstance)
}

func TestInboundGateway_TelegramPostgresPersistsConfiguredManifestDelivery(t *testing.T) {
	runID, entityID := boundedInboundTestCoordinates()
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	const (
		flowInstance      = boundedProviderFlowID
		entitySlug        = "customer-a"
		provider          = "telegram"
		webhookSecret     = "telegram-secret"
		providerEventID   = "123456789"
		agentID           = "telegram-webhook-subscriber"
		providerEventName = "inbound.telegram"
	)
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	target := seedPostgresInboundGatewayRuntime(t, ctx, pg, runID, entityID, flowInstance, entitySlug, provider, webhookSecret, agentID)

	bus, err := newBoundedInboundTestEventBus(t, pg, runtimebus.EventBusOptions{}, providerEventName, "inbound.telegram.text_message")
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := subscribeInboundGatewayAgent(t, bus, runID, agentID, flowInstance, events.EventType(providerEventName))

	g := newTestInboundGateway(t, bus, nil, nil, pg)

	body := []byte(`{"update_id":123456789,"undeclared_root":"root-must-not-enter-author-story","message":{"message_id":7,"from":{"id":41},"chat":{"id":42,"type":"private"},"text":"/start@other_bot","undeclared_private":"private-must-not-enter-author-story"}}`)
	req := newSignedTelegramRequest("/webhooks/customer-a/telegram", webhookSecret, body)
	rec := httptest.NewRecorder()
	handleBoundedProviderDelivery(t, g, bus, target, rec, req, provider, webhookSecret)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), webhookSecret) {
		t.Fatal("Telegram secret token leaked into response")
	}
	if got := countInboundMarkers(t, ctx, pg, target, provider, providerEventID); got != 1 {
		t.Fatalf("inbound marker rows = %d, want 1", got)
	}
	eventID := loadInboundProviderEventID(t, ctx, pg, target, provider, providerEventName, providerEventID)
	if got := countInboundProviderEvents(t, ctx, pg, runID, providerEventName, providerEventID); got != 1 {
		t.Fatalf("provider event rows = %d, want 1", got)
	}
	if got := loadInboundProviderEventPayloadField(t, ctx, pg, eventID, "provider_event_type"); got != "update" {
		t.Fatalf("provider_event_type = %q, want update", got)
	}
	if got := countInboundAgentDeliveries(t, ctx, pg, eventID, agentID); got != 1 {
		t.Fatalf("agent delivery rows = %d, want 1", got)
	}
	record, found, err := pg.LoadInboundPublicationByIdentity(ctx, inboundTestReceiptIdentity(target, provider, providerEventID))
	if err != nil || !found {
		t.Fatalf("LoadInboundPublicationByIdentity = found:%v err:%v", found, err)
	}
	requireInboundGatewayAuthorProjection(t, ctx, pg, runID, record.PublicationID, "chat", "42")
	requireInboundTelegramPatternProjection(t, record, "start", "other_bot")
	requireInboundPostCommitSnapshot(t, requireInboundBusEvent(t, ch, "Telegram PostgreSQL post-commit dispatch"), inboundPublicationEvent(t, record, eventID))
	waitForInboundBusQuiescence(t, bus)

	eventtestsql.CorruptEventStore(t, ctx, db, authoractivityfixture.DialectPostgres, eventtestsql.EventCorruptionClaim{
		Invariant: "store.event_record.duplicate_integrity",
		Reason:    "prove inbound duplicate comparison rejects a schema-valid durable payload conflict",
	}, "", `UPDATE events SET payload = '{"corrupt":true}'::jsonb, payload_bytes = '{"corrupt":true}'::bytea WHERE event_id = $1::uuid`, eventID)
	duplicate := httptest.NewRecorder()
	handleBoundedProviderDelivery(t, g, bus, target, duplicate, newSignedTelegramRequest("/webhooks/customer-a/telegram", webhookSecret, body), provider, webhookSecret)
	if duplicate.Code != http.StatusServiceUnavailable {
		t.Fatalf("corrupt duplicate status = %d, want 503 body=%s", duplicate.Code, duplicate.Body.String())
	}
	requireNoInboundBusEvent(t, ch, "corrupt Telegram PostgreSQL duplicate")
	eventtestsql.RequireEventRowCount(t, ctx, db, authoractivityfixture.DialectPostgres, eventID, 1)
}

func TestInboundGateway_TelegramSQLitePersistsConfiguredManifestDelivery(t *testing.T) {
	runID, entityID := boundedInboundTestCoordinates()
	const (
		flowInstance      = boundedProviderFlowID
		entitySlug        = "customer-a"
		provider          = "telegram"
		webhookSecret     = "telegram-secret"
		providerEventID   = "987654321"
		agentID           = "telegram-sqlite-webhook-subscriber"
		providerEventName = "inbound.telegram"
	)
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
	sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
	target := seedSQLiteInboundGatewayRuntime(t, ctx, sqliteStore, runID, entityID, flowInstance, entitySlug, provider, webhookSecret, agentID)

	bus, err := newBoundedInboundTestEventBus(t, sqliteStore, runtimebus.EventBusOptions{}, providerEventName, "inbound.telegram.text_message")
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := subscribeInboundGatewayAgent(t, bus, runID, agentID, flowInstance, events.EventType(providerEventName))

	g := newTestInboundGateway(t, bus, nil, nil, sqliteStore)

	body := []byte(`{"update_id":987654321,"undeclared_root":"root-must-not-enter-author-story","message":{"message_id":8,"from":{"id":41},"chat":{"id":42,"type":"private"},"text":"/start@other_bot","undeclared_private":"private-must-not-enter-author-story"}}`)
	req := newSignedTelegramRequest("/webhooks/customer-a/telegram", webhookSecret, body)
	rec := httptest.NewRecorder()
	handleBoundedProviderDelivery(t, g, bus, target, rec, req, provider, webhookSecret)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), webhookSecret) {
		t.Fatal("Telegram secret token leaked into response")
	}
	if got := countInboundMarkers(t, ctx, sqliteStore, target, provider, providerEventID); got != 1 {
		t.Fatalf("inbound marker rows = %d, want 1", got)
	}
	eventID := loadInboundProviderEventID(t, ctx, sqliteStore, target, provider, providerEventName, providerEventID)
	if got := countInboundProviderEvents(t, ctx, sqliteStore, runID, providerEventName, providerEventID); got != 1 {
		t.Fatalf("provider event rows = %d, want 1", got)
	}
	if got := loadInboundProviderEventPayloadField(t, ctx, sqliteStore, eventID, "provider_event_type"); got != "update" {
		t.Fatalf("provider_event_type = %q, want update", got)
	}
	if got := countInboundAgentDeliveries(t, ctx, sqliteStore, eventID, agentID); got != 1 {
		t.Fatalf("agent delivery rows = %d, want 1", got)
	}
	record, found, err := sqliteStore.LoadInboundPublicationByIdentity(ctx, inboundTestReceiptIdentity(target, provider, providerEventID))
	if err != nil || !found {
		t.Fatalf("LoadInboundPublicationByIdentity = found:%v err:%v", found, err)
	}
	requireInboundGatewayAuthorProjection(t, ctx, sqliteStore, runID, record.PublicationID, "chat", "42")
	requireInboundTelegramPatternProjection(t, record, "start", "other_bot")
	requireInboundPostCommitSnapshot(t, requireInboundBusEvent(t, ch, "Telegram SQLite post-commit dispatch"), inboundPublicationEvent(t, record, eventID))
	waitForInboundBusQuiescence(t, bus)

	eventtestsql.CorruptEventStore(t, ctx, storetest.DatabaseForTest(sqliteStore), authoractivityfixture.DialectSQLite, eventtestsql.EventCorruptionClaim{
		Invariant: "store.event_record.duplicate_integrity",
		Reason:    "prove inbound duplicate comparison rejects a schema-valid durable payload conflict",
	}, `UPDATE events SET payload = '{"corrupt":true}', payload_bytes = CAST('{"corrupt":true}' AS BLOB) WHERE event_id = ?`, "", eventID)
	duplicate := httptest.NewRecorder()
	handleBoundedProviderDelivery(t, g, bus, target, duplicate, newSignedTelegramRequest("/webhooks/customer-a/telegram", webhookSecret, body), provider, webhookSecret)
	if duplicate.Code != http.StatusServiceUnavailable {
		t.Fatalf("corrupt duplicate status = %d, want 503 body=%s", duplicate.Code, duplicate.Body.String())
	}
	requireNoInboundBusEvent(t, ch, "corrupt Telegram SQLite duplicate")
	eventtestsql.RequireEventRowCount(t, ctx, storetest.DatabaseForTest(sqliteStore), authoractivityfixture.DialectSQLite, eventID, 1)
}

func requireInboundTelegramPatternProjection(t testing.TB, record runtimeinbound.Record, reference, address string) {
	t.Helper()
	for _, child := range record.Events {
		if child.EventName != "inbound.telegram.text_message" {
			continue
		}
		var payload struct {
			Command struct {
				Reference string `json:"reference"`
				Address   string `json:"address"`
			} `json:"command_invocation"`
		}
		if err := json.Unmarshal(child.Event.Payload(), &payload); err != nil {
			t.Fatal(err)
		}
		if payload.Command.Reference != reference || payload.Command.Address != address {
			t.Fatalf("durable generic-pattern projection changed: %+v", payload.Command)
		}
		return
	}
	t.Fatal("durable publication omitted normalized Telegram text")
}

func inboundPublicationEvent(t testing.TB, record runtimeinbound.Record, eventID string) events.Event {
	t.Helper()
	for _, child := range record.Events {
		if child.EventID == eventID {
			return child.Event
		}
	}
	t.Fatalf("inbound publication does not contain event %s: %#v", eventID, record.Events)
	return events.Event{}
}

func requireInboundPostCommitSnapshot(t testing.TB, got, want events.Event) {
	t.Helper()
	var gotPayload, wantPayload any
	if err := json.Unmarshal(got.Payload(), &gotPayload); err != nil {
		t.Fatalf("decode dispatched inbound payload: %v", err)
	}
	if err := json.Unmarshal(want.Payload(), &wantPayload); err != nil {
		t.Fatalf("decode persisted inbound payload: %v", err)
	}
	if got.ID() != want.ID() || got.Type() != want.Type() || !got.Producer().Equal(want.Producer()) ||
		got.TaskID() != want.TaskID() || got.ChainDepth() != want.ChainDepth() || got.RunID() != want.RunID() ||
		got.ParentEventID() != want.ParentEventID() || got.ExecutionMode() != want.ExecutionMode() ||
		!got.CreatedAt().Truncate(time.Microsecond).Equal(want.CreatedAt().Truncate(time.Microsecond)) ||
		!reflect.DeepEqual(gotPayload, wantPayload) || !reflect.DeepEqual(got.Envelope(), want.Envelope()) {
		t.Fatalf("post-commit inbound snapshot changed\n got: id=%s type=%s producer=%s/%s task=%s depth=%d run=%s parent=%s mode=%s at=%s payload=%s envelope=%#v\nwant: id=%s type=%s producer=%s/%s task=%s depth=%d run=%s parent=%s mode=%s at=%s payload=%s envelope=%#v",
			got.ID(), got.Type(), got.ProducerType(), got.SourceAgent(), got.TaskID(), got.ChainDepth(), got.RunID(), got.ParentEventID(), got.ExecutionMode(), got.CreatedAt(), got.Payload(), got.Envelope(),
			want.ID(), want.Type(), want.ProducerType(), want.SourceAgent(), want.TaskID(), want.ChainDepth(), want.RunID(), want.ParentEventID(), want.ExecutionMode(), want.CreatedAt(), want.Payload(), want.Envelope())
	}
}

type inboundAuthorActivityReader interface {
	ListAuthorActivity(context.Context, runtimeauthoractivity.ListOptions) (runtimeauthoractivity.ListResult, error)
}

func requireInboundGatewayAuthorProjection(
	t *testing.T,
	ctx context.Context,
	reader inboundAuthorActivityReader,
	runID string,
	publicationID string,
	wantSubjectType string,
	wantSubjectID string,
) {
	t.Helper()
	result, err := reader.ListAuthorActivity(ctx, runtimeauthoractivity.ListOptions{RunID: runID, Limit: 100})
	if err != nil {
		t.Fatalf("ListAuthorActivity: %v", err)
	}
	var matches []runtimeauthoractivity.Occurrence
	for _, occurrence := range result.Occurrences {
		if occurrence.Kind == runtimeauthoractivity.KindInboundReceived && occurrence.Transition == "received" {
			matches = append(matches, occurrence)
		}
	}
	if len(matches) != 1 {
		t.Fatalf("inbound author occurrences = %d, want one: %#v", len(matches), result.Occurrences)
	}
	occurrence := matches[0]
	if occurrence.EntityID != "" || occurrence.FlowID != boundedProviderFlowID ||
		occurrence.Projection.SubjectType != "inbound_publication" || occurrence.Projection.SubjectID != publicationID {
		t.Fatalf("inbound author scope must identify the binding publication without a receiver: %+v", occurrence)
	}
	if occurrence.Projection.AuthorSubjectType != wantSubjectType || occurrence.Projection.AuthorSubjectID != wantSubjectID {
		t.Fatalf(
			"inbound author subject = %q/%q, want declared %q/%q",
			occurrence.Projection.AuthorSubjectType,
			occurrence.Projection.AuthorSubjectID,
			wantSubjectType,
			wantSubjectID,
		)
	}
	encoded, err := json.Marshal(occurrence)
	if err != nil {
		t.Fatalf("marshal inbound author occurrence: %v", err)
	}
	for _, forbidden := range []string{"author_safe_summary", "hello", "root-must-not-enter-author-story", "private-must-not-enter-author-story", "undeclared_root", "undeclared_private"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("inbound author occurrence leaked undeclared payload marker %q: %s", forbidden, encoded)
		}
	}
}

func TestInboundGateway_TypeformAndIntercomPostgresPersistsConfiguredManifestDelivery(t *testing.T) {
	for _, tc := range []struct {
		name              string
		runID             string
		entityID          string
		flowInstance      string
		provider          string
		webhookSecret     string
		providerEventID   string
		providerEventType string
		agentID           string
		providerEventName string
		body              []byte
		newRequest        func(path string, secret string, body []byte) *http.Request
	}{
		{
			name:              "typeform",
			flowInstance:      boundedProviderFlowID,
			provider:          "typeform",
			webhookSecret:     "typeform-secret",
			providerEventID:   "tf-evt-pg-123",
			providerEventType: "form_response",
			agentID:           "typeform-webhook-subscriber",
			providerEventName: "inbound.typeform",
			body:              []byte(`{"event_id":"tf-evt-pg-123","event_type":"form_response","form_response":{"token":"abc"}}`),
			newRequest:        newSignedTypeformRequest,
		},
		{
			name:              "intercom",
			flowInstance:      boundedProviderFlowID,
			provider:          "intercom",
			webhookSecret:     "intercom-secret",
			providerEventID:   "notif_pg_123",
			providerEventType: "conversation_user_created",
			agentID:           "intercom-webhook-subscriber",
			providerEventName: "inbound.intercom",
			body:              []byte(`{"id":"notif_pg_123","topic":"conversation.user.created","data":{"item":{"id":"conv_1"}}}`),
			newRequest:        newSignedIntercomRequest,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)

			tc.runID, tc.entityID = boundedInboundTestCoordinates()
			ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), tc.runID)
			pg := storetest.AdmitPostgresRuntimeStore(t, db)
			target := seedPostgresInboundGatewayRuntime(t, ctx, pg, tc.runID, tc.entityID, tc.flowInstance, "customer-a", tc.provider, tc.webhookSecret, tc.agentID)

			bus, err := newBoundedInboundTestEventBus(t, pg, runtimebus.EventBusOptions{}, tc.providerEventName)
			if err != nil {
				t.Fatalf("NewEventBus: %v", err)
			}
			ch := subscribeInboundGatewayAgent(t, bus, tc.runID, tc.agentID, tc.flowInstance, events.EventType(tc.providerEventName))
			defer runtimebustest.Unsubscribe(bus, tc.agentID)

			g := newTestInboundGateway(t, bus, nil, nil, pg)

			req := tc.newRequest("/webhooks/customer-a/"+tc.provider, tc.webhookSecret, tc.body)
			rec := httptest.NewRecorder()
			handleBoundedProviderDelivery(t, g, bus, target, rec, req, tc.provider, tc.webhookSecret)

			if rec.Code != http.StatusAccepted {
				t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
			}
			if got := countInboundMarkers(t, ctx, pg, target, tc.provider, tc.providerEventID); got != 1 {
				t.Fatalf("inbound marker rows = %d, want 1", got)
			}
			eventID := loadInboundProviderEventID(t, ctx, pg, target, tc.provider, tc.providerEventName, tc.providerEventID)
			if got := countInboundProviderEvents(t, ctx, pg, tc.runID, tc.providerEventName, tc.providerEventID); got != 1 {
				t.Fatalf("provider event rows = %d, want 1", got)
			}
			if got := loadInboundProviderEventPayloadField(t, ctx, pg, eventID, "provider_event_type"); got != tc.providerEventType {
				t.Fatalf("provider_event_type = %q, want %s", got, tc.providerEventType)
			}
			if got := countInboundAgentDeliveries(t, ctx, pg, eventID, tc.agentID); got != 1 {
				t.Fatalf("agent delivery rows = %d, want 1", got)
			}
			select {
			case got := <-ch:
				if got.ID() != eventID || got.Type() != events.EventType(tc.providerEventName) {
					t.Fatalf("delivered event = %s/%s, want %s/%s", got.ID(), got.Type(), eventID, tc.providerEventName)
				}
				_ = got.Complete()
			case <-time.After(5 * time.Second):
				t.Fatalf("%s PostgreSQL post-commit dispatch did not arrive", tc.provider)
			}
			unsubscribeAndWaitForInboundBusQuiescence(t, bus, tc.runID, tc.agentID, tc.flowInstance)
		})
	}
}

func TestInboundGateway_TypeformAndIntercomSQLitePersistsConfiguredManifestDelivery(t *testing.T) {
	for _, tc := range []struct {
		name              string
		runID             string
		entityID          string
		flowInstance      string
		provider          string
		webhookSecret     string
		providerEventID   string
		providerEventType string
		agentID           string
		providerEventName string
		body              []byte
		newRequest        func(path string, secret string, body []byte) *http.Request
	}{
		{
			name:              "typeform",
			flowInstance:      boundedProviderFlowID,
			provider:          "typeform",
			webhookSecret:     "typeform-secret",
			providerEventID:   "tf-evt-sqlite-123",
			providerEventType: "form_response",
			agentID:           "typeform-sqlite-webhook-subscriber",
			providerEventName: "inbound.typeform",
			body:              []byte(`{"event_id":"tf-evt-sqlite-123","event_type":"form_response","form_response":{"token":"abc"}}`),
			newRequest:        newSignedTypeformRequest,
		},
		{
			name:              "intercom",
			flowInstance:      boundedProviderFlowID,
			provider:          "intercom",
			webhookSecret:     "intercom-secret",
			providerEventID:   "notif_sqlite_123",
			providerEventType: "conversation_user_created",
			agentID:           "intercom-sqlite-webhook-subscriber",
			providerEventName: "inbound.intercom",
			body:              []byte(`{"id":"notif_sqlite_123","topic":"conversation.user.created","data":{"item":{"id":"conv_1"}}}`),
			newRequest:        newSignedIntercomRequest,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.runID, tc.entityID = boundedInboundTestCoordinates()
			ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), tc.runID)
			sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
			target := seedSQLiteInboundGatewayRuntime(t, ctx, sqliteStore, tc.runID, tc.entityID, tc.flowInstance, "customer-a", tc.provider, tc.webhookSecret, tc.agentID)

			bus, err := newBoundedInboundTestEventBus(t, sqliteStore, runtimebus.EventBusOptions{}, tc.providerEventName)
			if err != nil {
				t.Fatalf("NewEventBus: %v", err)
			}
			ch := subscribeInboundGatewayAgent(t, bus, tc.runID, tc.agentID, tc.flowInstance, events.EventType(tc.providerEventName))
			defer runtimebustest.Unsubscribe(bus, tc.agentID)

			g := newTestInboundGateway(t, bus, nil, nil, sqliteStore)

			req := tc.newRequest("/webhooks/customer-a/"+tc.provider, tc.webhookSecret, tc.body)
			rec := httptest.NewRecorder()
			handleBoundedProviderDelivery(t, g, bus, target, rec, req, tc.provider, tc.webhookSecret)

			if rec.Code != http.StatusAccepted {
				t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
			}
			if got := countInboundMarkers(t, ctx, sqliteStore, target, tc.provider, tc.providerEventID); got != 1 {
				t.Fatalf("inbound marker rows = %d, want 1", got)
			}
			eventID := loadInboundProviderEventID(t, ctx, sqliteStore, target, tc.provider, tc.providerEventName, tc.providerEventID)
			if got := countInboundProviderEvents(t, ctx, sqliteStore, tc.runID, tc.providerEventName, tc.providerEventID); got != 1 {
				t.Fatalf("provider event rows = %d, want 1", got)
			}
			if got := loadInboundProviderEventPayloadField(t, ctx, sqliteStore, eventID, "provider_event_type"); got != tc.providerEventType {
				t.Fatalf("provider_event_type = %q, want %s", got, tc.providerEventType)
			}
			if got := countInboundAgentDeliveries(t, ctx, sqliteStore, eventID, tc.agentID); got != 1 {
				t.Fatalf("agent delivery rows = %d, want 1", got)
			}
			select {
			case got := <-ch:
				if got.ID() != eventID || got.Type() != events.EventType(tc.providerEventName) {
					t.Fatalf("delivered event = %s/%s, want %s/%s", got.ID(), got.Type(), eventID, tc.providerEventName)
				}
				_ = got.Complete()
			case <-time.After(5 * time.Second):
				t.Fatalf("%s SQLite post-commit dispatch did not arrive", tc.provider)
			}
			unsubscribeAndWaitForInboundBusQuiescence(t, bus, tc.runID, tc.agentID, tc.flowInstance)
		})
	}
}

func seedPostgresInboundGatewayRuntime(
	t *testing.T,
	ctx context.Context,
	pg *store.PostgresStore,
	runID string,
	entityID string,
	flowInstance string,
	entitySlug string,
	provider string,
	webhookSecret string,
	agentID string,
) runtimepkg.InboundTarget {
	t.Helper()
	target := seedBoundedStandingTarget(t, ctx, pg, entityID)
	if target.RunID != runID {
		t.Fatalf("bounded receiver run %s differs from admitted standing run %s", runID, target.RunID)
	}
	seedBoundedInboundFlow(t, ctx, pg, runID, entityID, flowInstance, entitySlug)
	if strings.TrimSpace(agentID) != "" {
		if err := storetest.UpsertStaticAgentFixture(t, ctx, pg, runtimemanager.PersistedAgent{
			Config: runtimeTestAgentConfig(t, runtimeactors.AgentConfig{
				ExecutionMode:      "live",
				ResolvedLLMBackend: "anthropic",
				ID:                 agentID,
				Identity:           inboundGatewayAgentIdentity(t, runID, agentID, flowInstance),
				Role:               "observer",
				FlowID:             boundedProviderFlowID,
				Type:               "stub",
				Model:              "regular",
				FlowPath:           flowInstance,
				EntityID:           entityID,
				Subscriptions:      []string{"inbound." + provider},
				Config:             []byte(`{}`),
			}),
			Status:    "active",
			HiredBy:   "test",
			StartedAt: time.Now().UTC(),
		}); err != nil {
			t.Fatalf("UpsertAgent(%s): %v", agentID, err)
		}
	}
	return target
}

func inboundGatewayAgentIdentity(t testing.TB, runID, agentID, flowInstance string) runtimeagentidentity.Identity {
	t.Helper()
	return runtimeagentidentitytest.RuntimeForRun(
		t,
		runID,
		agentID,
		"runtime-test/inbound-gateway",
		boundedProviderFlowID,
		flowInstance,
		flowInstance,
	)
}

func subscribeInboundGatewayAgent(
	t testing.TB,
	bus *runtimebus.EventBus,
	runID,
	agentID,
	flowInstance string,
	eventTypes ...events.EventType,
) <-chan *runtimebus.LocalDelivery {
	t.Helper()
	bindInboundGatewayAgentReadinessFinalizer(bus, runID)
	subscriptions := make([]string, 0, len(eventTypes))
	for _, eventType := range eventTypes {
		subscriptions = append(subscriptions, string(eventType))
	}
	flowEvents := make(map[string]runtimecontracts.EventCatalogEntry, len(subscriptions))
	for _, subscription := range subscriptions {
		flowEvents[subscription] = runtimecontracts.EventCatalogEntry{}
	}
	flow := runtimecontracts.FlowContractView{
		Path:   boundedProviderFlowID,
		Paths:  runtimecontracts.FlowContractPaths{FlowPath: boundedProviderFlowID},
		Events: flowEvents,
	}
	root := runtimecontracts.FlowContractView{Path: ".", Children: []runtimecontracts.FlowContractView{flow}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{boundedProviderFlowID: &root.Children[0]},
		},
	}
	if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
		t.Fatalf("compile inbound subscriber declarations: %v", err)
	}
	source := semanticview.Wrap(bundle)
	admission, err := semanticview.AdmitFlowOwnedAgentSubscriptions(source, semanticview.FlowOwnedAgentSubscriptionRequest{
		AgentID:       agentID,
		FlowID:        boundedProviderFlowID,
		FlowPath:      flowInstance,
		Subscriptions: subscriptions,
	})
	if err != nil {
		t.Fatalf("admit inbound gateway test agent route: %v", err)
	}
	return runtimebustest.SubscribeIdentity(t, bus, inboundGatewayAgentIdentity(t, runID, agentID, flowInstance), admission)
}

func bindInboundGatewayAgentReadinessFinalizer(bus *runtimebus.EventBus, runID string) {
	bus.SetCommittedAgentReadinessFinalizer(runtimebus.CommittedAgentReadinessFinalizerFunc(func(_ context.Context, event events.Event, routes []events.DeliveryRoute) error {
		if event.RunID() != runID {
			return errors.New("inbound test event escaped its admitted run")
		}
		for _, route := range routes {
			if !route.Recipient.IsAgent() {
				continue
			}
			if err := route.AgentIdentity.Validate(); err != nil {
				return err
			}
			if route.AgentIdentity.RunID != runID {
				return errors.New("inbound test agent route escaped its admitted run")
			}
		}
		return nil
	}))
}

func seedSQLiteInboundGatewayRuntime(
	t *testing.T,
	ctx context.Context,
	sqliteStore *store.SQLiteRuntimeStore,
	runID string,
	entityID string,
	flowInstance string,
	entitySlug string,
	provider string,
	webhookSecret string,
	agentID string,
) runtimepkg.InboundTarget {
	t.Helper()
	now := time.Now().UTC()
	target := seedBoundedStandingTarget(t, ctx, sqliteStore, entityID)
	if target.RunID != runID {
		t.Fatalf("bounded receiver run %s differs from admitted standing run %s", runID, target.RunID)
	}
	seedBoundedInboundFlow(t, ctx, sqliteStore, runID, entityID, flowInstance, entitySlug)
	if strings.TrimSpace(agentID) != "" {
		if err := storetest.UpsertStaticAgentFixture(t, ctx, sqliteStore, runtimemanager.PersistedAgent{
			Config: runtimeTestAgentConfig(t, runtimeactors.AgentConfig{
				ExecutionMode:      "live",
				ResolvedLLMBackend: "anthropic",
				ID:                 agentID,
				Identity:           inboundGatewayAgentIdentity(t, runID, agentID, flowInstance),
				Role:               "observer",
				FlowID:             boundedProviderFlowID,
				Type:               "stub",
				Model:              "regular",
				FlowPath:           flowInstance,
				EntityID:           entityID,
				Config:             []byte(`{}`),
				Subscriptions:      []string{"inbound." + provider},
			}),
			Status:    "active",
			HiredBy:   "test",
			StartedAt: now,
		}); err != nil {
			t.Fatalf("UpsertAgent(%s): %v", agentID, err)
		}
	}
	return target
}

func boundedInboundStandingOrigin(t *testing.T, _ string) runtimerunlifecycle.RunOrigin {
	t.Helper()
	serviceID := runtimeflowidentity.StandingServiceID(boundedProviderFlowID)
	origin, err := runtimerunlifecycle.StandingGenerationRunOrigin(serviceID, 1)
	if err != nil {
		t.Fatalf("construct bounded inbound standing origin: %v", err)
	}
	return origin
}

func inboundTestReceiptIdentity(target runtimepkg.InboundTarget, provider, deliveryID string) runtimeinbound.Identity {
	return runtimeinbound.Identity{ServiceID: target.ServiceID, RunID: target.RunID, Generation: target.Generation, Provider: provider, ProviderEventID: deliveryID}
}

type inboundStandingRecoveryOwner struct {
	occurrence *worklifetime.StandingOccurrence
}

func (o inboundStandingRecoveryOwner) BeginStandingRunRecovery(
	ctx context.Context,
	runID string,
	origin runtimerunlifecycle.RunOrigin,
) (*worklifetime.Lease, error) {
	identity := o.occurrence.Identity()
	if identity.RunID != runID ||
		identity.ServiceID != origin.ServiceID() ||
		identity.Generation != uint64(origin.Generation()) {
		return nil, errors.New("inbound standing recovery requested the wrong occurrence")
	}
	return o.occurrence.Begin(ctx)
}

func installInboundStandingRecoveryOwner(
	t *testing.T,
	bus *runtimebus.EventBus,
	runID string,
	provider string,
) {
	t.Helper()
	origin := boundedInboundStandingOrigin(t, provider)
	process := worklifetime.NewProcess()
	runtimeOwner, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: runID,
		BundleHash:        authorActivityTestSourceArtifactFact.BundleHash(),
	})
	if err != nil {
		t.Fatal(err)
	}
	standing, err := runtimeOwner.NewStanding(context.Background(), worklifetime.StandingIdentity{
		ServiceID:  origin.ServiceID(),
		RunID:      runID,
		Generation: uint64(origin.Generation()),
	})
	if err != nil {
		t.Fatal(err)
	}
	bus.SetStandingRunWorkOwner(inboundStandingRecoveryOwner{occurrence: standing})
	t.Cleanup(func() {
		if err := standing.RetireAndWait(context.Background()); err != nil {
			t.Errorf("retire inbound standing occurrence: %v", err)
		}
		if _, err := runtimeOwner.RetireAndWait(context.Background()); err != nil {
			t.Errorf("retire inbound runtime occurrence: %v", err)
		}
		process.Retire()
		if _, err := process.Join(context.Background()); err != nil {
			t.Errorf("join inbound process occurrence: %v", err)
		}
	})
}

func requireInboundBusEvent(t testing.TB, ch <-chan *runtimebus.LocalDelivery, context string) events.Event {
	t.Helper()
	select {
	case delivery := <-ch:
		evt := delivery.Event()
		_ = delivery.Complete()
		return evt
	case <-time.After(5 * time.Second):
		t.Fatalf("%s: expected queued bus event", context)
		return events.Event{}
	}
}

func requireNoInboundBusEvent(t testing.TB, ch <-chan *runtimebus.LocalDelivery, context string) {
	t.Helper()
	select {
	case delivery := <-ch:
		_ = delivery.Complete()
		t.Fatalf("%s: unexpected bus event: %#v", context, delivery.Event())
	default:
	}
}

func unsubscribeAndWaitForInboundBusQuiescence(t testing.TB, bus *runtimebus.EventBus, runID, agentID, flowInstance string) {
	t.Helper()
	runtimebustest.UnsubscribeIdentity(bus, inboundGatewayAgentIdentity(t, runID, agentID, flowInstance))
	waitForInboundBusQuiescence(t, bus)
}

func waitForInboundBusQuiescence(t testing.TB, bus *runtimebus.EventBus) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(testAuthorActivityContext(context.Background()), 5*time.Second)
	defer cancel()
	if err := bus.WaitForQuiescence(waitCtx); err != nil {
		t.Fatalf("WaitForQuiescence: %v", err)
	}
}

func githubWebhookSignature(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func slackWebhookSignature(secret string, timestamp string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte("v0:" + timestamp + ":" + string(body)))
	return "v0=" + hex.EncodeToString(mac.Sum(nil))
}

func stripeWebhookSignature(secret string, timestamp string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(timestamp + "." + string(body)))
	return "t=" + timestamp + ",v1=" + hex.EncodeToString(mac.Sum(nil))
}

func newSignedTwilioRequest(requestURL string, secret string, form url.Values) *http.Request {
	req := httptest.NewRequest(http.MethodPost, requestURL, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-Twilio-Signature", twilioWebhookSignature(secret, requestURL, form))
	return req
}

func twilioWebhookSignature(secret, requestURL string, form url.Values) string {
	mac := hmac.New(sha1.New, []byte(secret))
	_, _ = mac.Write([]byte(twilioSignedPayload(requestURL, form)))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func twilioSignedPayload(requestURL string, form url.Values) string {
	keys := make([]string, 0, len(form))
	for key := range form {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(requestURL)
	for _, key := range keys {
		b.WriteString(key)
		b.WriteString(form.Get(key))
	}
	return b.String()
}

func newSignedShopifyRequest(path string, secret string, body []byte) *http.Request {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(body)))
	req.Header.Set("X-Shopify-Hmac-Sha256", shopifyWebhookSignature(secret, body))
	return req
}

func shopifyWebhookSignature(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func newSignedTelegramRequest(path string, secret string, body []byte) *http.Request {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(body)))
	req.Header.Set("X-Telegram-Bot-Api-Secret-Token", secret)
	return req
}

func newSignedTypeformRequest(path string, secret string, body []byte) *http.Request {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(body)))
	req.Header.Set("Typeform-Signature", typeformWebhookSignature(secret, body))
	return req
}

func typeformWebhookSignature(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	return "sha256=" + base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func newSignedIntercomRequest(path string, secret string, body []byte) *http.Request {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(body)))
	req.Header.Set("X-Hub-Signature", intercomWebhookSignature(secret, body))
	return req
}

func intercomWebhookSignature(secret string, body []byte) string {
	mac := hmac.New(sha1.New, []byte(secret))
	_, _ = mac.Write(body)
	return "sha1=" + hex.EncodeToString(mac.Sum(nil))
}
