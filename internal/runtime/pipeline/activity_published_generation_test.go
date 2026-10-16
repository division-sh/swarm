package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/activityresult"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	contracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/effects"
	engine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

type absentPublishedActivityResultReader struct{}

func (absentPublishedActivityResultReader) LoadRecordedActivityResult(context.Context, activityresult.Query) (activityresult.Record, bool, error) {
	return activityresult.Record{}, false, nil
}

type currentPublishedActivityResultReader struct {
	record activityresult.Record
	fault  error
}

func (r currentPublishedActivityResultReader) LoadRecordedActivityResult(ctx context.Context, query activityresult.Query) (activityresult.Record, bool, error) {
	if err := ctx.Err(); err != nil {
		return activityresult.Record{}, false, err
	}
	if r.fault != nil {
		return activityresult.Record{}, false, r.fault
	}
	if r.record.EventID != query.SuccessEventID {
		return activityresult.Record{}, false, errors.New("current publication query changed its exact activity identity")
	}
	return r.record, true, nil
}

func TestReadOnlyPublishedAccountingAfterGenerationAdvance(t *testing.T) {
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, canonicalrouting.CopyLifecycleNestedTemplates(t), contracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"value":"fresh"}`))
	}))
	defer server.Close()
	intent, _, _, base := publishedActivityAccountingFixture(t)
	bundle.Tools[intent.Tool] = contracts.MustToolSchemaEntry(contracts.WithToolHandler(contracts.MustToolHandlerKind("http")), contracts.WithToolSchemas(contracts.MustToolInputSchema(contracts.ToolSchemaObject), contracts.MustToolInputSchema(contracts.ToolSchemaObject)), contracts.WithToolHTTP(contracts.HTTPToolSpec{Method: "GET", URL: server.URL}))
	source := semanticview.Wrap(bundle)
	parent := flowidentity.Derive(source, "outer/left/sink", "same-revision")
	child, err := flowidentity.KeylessChild(source, parent, parent.TemplateID+"/final")
	if err != nil {
		t.Fatal(err)
	}
	old, err := loopruntime.New(intent.SourceRunID, child.EntityID, child.TemplateID, "revision", "revision_id", uuid.NewString(), "review", 3, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	advanced := old
	if _, err := advanced.Repeat("review", uuid.NewString(), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	intent.ExecutionFlowID = identity.FlowID(child.TemplateID)
	intent.FlowInstance, intent.EntityID = child.InstancePath, identity.EntityID(child.EntityID)
	intent.Owner = activityidentity.MustNodeOwner(mustPipelineNode(child.TemplateID, "reader"))
	intent.SuccessEvent, intent.FailureEvent = "read.succeeded", "read.failed"
	intent.RoutingSource, err = events.NewConcreteTemplateInstanceRoutingSource(events.RouteIdentity{FlowID: child.TemplateID, FlowInstance: child.InstancePath, EntityID: child.EntityID})
	if err != nil {
		t.Fatal(err)
	}
	intent.Generation, intent.LoopStage = old.Generation(), "review"
	result, err := admitActivityPublication(intent, intent.SuccessEvent)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := canonicaljson.MarshalPreservingNumberKinds(activitySuccessPayload(intent, map[string]any{"value": "already-published"}))
	if err != nil {
		t.Fatal(err)
	}
	publication, err := activityresult.NewPublishedContinuation(activityIdentityFact(intent), uuid.NewString(), string(result.eventType), payload, intent.ExecutionMode, activityInputHash(intent.Input), "", time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name       string
		activation loopruntime.Activation
		published  bool
		fault      string
	}{
		{"retained_current", old, true, ""},
		{"retained_advanced", advanced, true, ""},
		{"fresh_current", old, false, ""},
		{"fresh_advanced", advanced, false, ""},
		{"current_published_current", old, false, ""},
		{"current_published_advanced", advanced, false, ""},
		{"current_published_canceled", advanced, false, "canceled"},
		{"current_published_read_failure", advanced, false, "reader"},
		{"retained_canceled", advanced, true, "canceled"},
		{"retained_foreign_authority", advanced, true, "authority"},
		{"retained_changed_input", advanced, true, "input"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, currentIntent := base, intent
			var publications []activityresult.PublishedContinuation
			if tc.published {
				publications = append(publications, publication)
			}
			ctx, err := WithSelectedActivityPublications(ctx, intent.SourceRunID, publications)
			if err != nil {
				t.Fatal(err)
			}
			switch tc.fault {
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "authority":
				ctx = effects.WithAuthority(ctx, effects.Authority{})
			case "input":
				currentIntent.Input = mustActivityInput(map[string]any{"url": "crossed-input"})
			}
			buckets := map[string]map[string]any{}
			if err := loopruntime.Store(buckets, tc.activation); err != nil {
				t.Fatal(err)
			}
			instance := WorkflowInstance{WorkflowName: child.TemplateID, InstanceID: child.InstanceID, StorageRef: child.InstancePath, EntityID: child.EntityID, ParentFlowID: child.ParentRoute.FlowID, ParentFlowInstance: child.ParentRoute.FlowInstance, ParentEntityID: child.ParentEntityID, CurrentState: tc.activation.CurrentStage, StateBuckets: engine.NewStateCarrier(nil, nil, buckets).PersistedStateBuckets()}
			reader := activityConstructionReader{load: func(_ flowidentity.RunScopedFlowInstance) (WorkflowInstance, bool, error) { return instance, true, nil }}
			bus := &recordingPipelineBus{}
			pc := &PipelineCoordinator{module: staticSemanticWorkflowModule{source: source}, executionPosture: executionposture.Live, bus: bus, workflowStore: &workflowInstanceStore{instanceReader: reader, activityResults: absentPublishedActivityResultReader{}}, entityLocks: map[string]*sync.Mutex{}}
			currentPublished := tc.name == "current_published_current" || tc.name == "current_published_advanced" || tc.name == "current_published_canceled" || tc.name == "current_published_read_failure"
			if currentPublished {
				current := currentPublishedActivityResultReader{record: activityresult.Record{EventID: result.eventID, EventType: string(result.eventType)}}
				if tc.fault == "reader" {
					current.fault = errors.New("native result read failed")
				}
				pc.workflowStore.activityResults = current
			}
			before := calls.Load()
			err = (pipelineActivityDispatcher{coordinator: pc}).executeActivityIntent(ctx, currentIntent)
			if (err != nil) != (tc.fault != "") {
				t.Fatalf("accounting admission: %v", err)
			}
			wantEmits, wantCalls := 0, int32(0)
			if !tc.published && !currentPublished {
				wantEmits = 1
				if tc.name == "fresh_current" {
					wantCalls = 1
				}
			}
			if len(bus.publishes) != wantEmits || calls.Load()-before != wantCalls {
				t.Fatalf("result emissions=%d want=%d provider calls=%d want=%d", len(bus.publishes), wantEmits, calls.Load()-before, wantCalls)
			}
			if tc.name == "fresh_advanced" {
				var failure struct {
					Failure struct {
						Class  string
						Detail struct{ Code string }
					} `json:"failure"`
				}
				if err := json.Unmarshal(bus.publishes[0].Payload(), &failure); err != nil || failure.Failure.Class != "platform.stale_arrival" || failure.Failure.Detail.Code != "activity_loop_generation_stale" {
					t.Fatalf("fresh stale-work refusal changed: %s error=%v", bus.publishes[0].Payload(), err)
				}
			}
		})
	}
}
