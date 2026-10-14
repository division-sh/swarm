package runtimepersistence

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/notifyallchildren"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestIndexedFlowInstanceAuthorityRefusesInvalidProvenanceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, fault := range []string{"missing_readiness", "wrong_run_readiness", "incomplete_plan", "stale_plan_version", "malformed_plan", "foreign_run_source", "exact"} {
			t.Run(backend+"/"+fault, func(t *testing.T) {
				original := notifyallchildren.LoadBundle(t, notifyallchildren.Options{})
				files := make(map[string]string)
				for _, entry := range original.SourceArtifact.Entries() {
					files[entry.Label()] = string(entry.Bytes())
				}
				f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, files, nil)
				runID := correlation.RunIDFromContext(f.ctx)
				source := semanticview.Wrap(f.bundle)
				plan, producer := constructR7PublicationReceiver(t, f, fault == "exact")
				payload := []byte(`{"account_id":"existing","command":"inspect"}`)
				fact, found := correlation.SourceArtifactFactFromContext(f.ctx)
				if !found {
					t.Fatal("fixture lost admitted source")
				}
				lookup, err := pipeline.NewExactFlowInstanceLookup(source, fact, flowidentity.RunScopedFlowInstance{RunID: runID, Route: plan.Identity.Route()})
				if err != nil {
					t.Fatal(err)
				}
				index := f.store.(pipeline.FlowInstanceIndexReader)
				if observed, found, err := index.LookupFlowInstance(f.ctx, lookup); err != nil || !found || observed.Identity() != plan.Identity || observed.InstanceKey() != "existing" {
					t.Fatalf("native positive before corruption: %+v found=%t err=%v", observed, found, err)
				}

				query, args := "", []any{runID, plan.Identity.InstancePath}
				switch fault {
				case "missing_readiness", "wrong_run_readiness":
					if fault == "wrong_run_readiness" {
						other := newReceiverConfigActivationFixtureForStore(t, f.store, false, files, nil, ownStoreTestAgentManager, nil)
						otherPlan, _ := constructR7PublicationReceiver(t, other, false)
						if otherPlan.Identity.InstancePath != plan.Identity.InstancePath || correlation.RunIDFromContext(other.ctx) == runID {
							t.Fatal("wrong-run proof requires the same path under another durable run")
						}
					}
					query = `DELETE FROM flow_instance_runtime_readiness WHERE run_id=$1 AND instance_path=$2`
				case "incomplete_plan", "stale_plan_version", "malformed_plan":
					query = `UPDATE flow_instance_runtime_readiness SET plan=$3 WHERE run_id=$1 AND instance_path=$2`
					raw := `{}`
					if fault == "stale_plan_version" {
						raw = `{"version":3}`
					} else if fault == "malformed_plan" {
						raw = `{"workflow_version":{"unexpected":true}}`
					}
					args = append(args, raw)
				case "foreign_run_source":
					foreign := sourceartifactfixture.New("agents.yaml", []byte("agents:\n  foreign: {}\n"))
					sourceartifactfixture.RequireArtifact(t, f.ctx, f.store.(sourceartifactfixture.Writer), foreign)
					if _, err := f.store.(runlifecycle.OperationOwner).ReviseRunSource(f.ctx, runlifecycle.SourceRevisionRequest{RunID: runID, Source: sourceartifactfixture.FactFor(foreign)}); err != nil {
						t.Fatal(err)
					}
				}
				if query != "" {
					if _, err := f.db.ExecContext(f.ctx, query, args...); err != nil {
						t.Fatal(err)
					}
				}
				if observed, found, err := index.LookupFlowInstance(f.ctx, lookup); (err == nil) != (fault == "exact") || fault != "exact" && (found || observed.Valid()) {
					t.Fatalf("index admitted %s: found=%t err=%v", fault, found, err)
				}
				publisher := f.newRuntimeEventBus(t, bus.EventBusOptions{})
				before, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store)
				if err != nil {
					t.Fatal(err)
				}
				for _, localEvent := range []string{"account.notify.requested", "account.registered"} {
					event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), events.EventType(source.ResolveFlowEventReference(notifyallchildren.OwnerFlowID, localEvent)), notifyallchildren.OwnerFlowID, "", json.RawMessage(payload), 0, runID, events.EventEnvelope{}, eventtest.StaticFlowRoutingSource(producer.TemplateID, producer.InstancePath, producer.EntityID), time.Now().UTC())
					recipients, err := publisher.CheckPublishRecipientPlan(f.ctx, event)
					if (err == nil) != (fault == "exact") {
						t.Fatalf("%s admitted %s: %v", localEvent, fault, err)
					}
					if fault == "exact" {
						want := 1
						if localEvent == "account.notify.requested" {
							want = 2
						}
						if len(recipients.DeliveryRoutes) != want || recipients.TargetFailure != "" {
							t.Fatalf("%s omitted its exact indexed consumers: %+v", localEvent, recipients)
						}
						for _, delivery := range recipients.DeliveryRoutes {
							if !delivery.Target.ExistingEntity() || delivery.Target.Route().EntityID != plan.Identity.EntityID || delivery.Target.Route().FlowInstance != plan.Identity.InstancePath {
								t.Fatalf("%s borrowed or recreated a receiver: %+v", localEvent, delivery)
							}
						}
					}
					if after, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store); err != nil || !reflect.DeepEqual(before, after) {
						t.Fatalf("%s planning changed durable evidence: %v", localEvent, err)
					}
				}
			})
		}
	}
}

func constructR7PublicationReceiver(t *testing.T, f receiverConfigActivationFixture, complete bool) (pipeline.FlowInstanceActivationPlan, flowidentity.Instance) {
	t.Helper()
	runID := correlation.RunIDFromContext(f.ctx)
	source := semanticview.Wrap(f.bundle)
	rootRequest := sqliteFlowActivationRequest(f.bundle, ".", runID, "", runID)
	rootRequest.Instance = flowidentity.Stored(source, ".", runID, runID, runID, "")
	root, err := f.manager.PrepareFlowInstanceActivation(f.ctx, rootRequest)
	if err != nil {
		t.Fatal(err)
	}
	committer := agentFixtureFlowActivationCommitter{store: f.store}
	committedRoot, err := committer.CommitFlowInstanceActivation(f.ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.manager.FinalizeCommittedFlowInstanceActivation(f.ctx, committedRoot); err != nil {
		t.Fatal(err)
	}
	var producer flowidentity.Instance
	for _, construction := range root.ConstructionPlans() {
		if construction.Identity.TemplateID == notifyallchildren.OwnerFlowID {
			producer = construction.Identity
		}
	}
	if producer.TemplateID == "" {
		t.Fatal("publication fixture requires its genuine static producer")
	}
	request := sqliteFlowActivationRequest(f.bundle, "account", "existing", "", "")
	request.Instance, err = flowidentity.KeyedChild(source, root.Identity, "account", "existing")
	if err != nil {
		t.Fatal(err)
	}
	request.ConstructorInput, request.ResolvedKey = "account.registered", "existing"
	request.TriggerEvent = eventtest.ExistingRunRootIngress(uuid.NewString(), "account.registered", "constructor-fixture", "", []byte(`{"account_id":"existing"}`), 0, runID, events.EventEnvelope{}, request.OccurredAt)
	plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	committed, err := committer.CommitFlowInstanceActivation(f.ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	if complete {
		if err := f.manager.FinalizeCommittedFlowInstanceActivation(f.ctx, committed); err != nil {
			t.Fatal(err)
		}
	}
	return plan, producer
}
