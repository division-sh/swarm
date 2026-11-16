package cataloge2e

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	eventtypes "github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestConnectionResolvedIntegerKeysReachConstructionBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			h := newRuntimeHarnessForBackend(t, canonicalrouting.CopyIntegerConnectionPolicies(t), backend, true)
			seedCatalogRootStateForRun(t, h, catalogRuntimeRunID)
			h.publishAndWait(catalogTriggerStep{Event: "work.requested", inputKind: catalogReplayInputRootIngress, eventID: uuid.NewString(), createdAt: time.Now().UTC(), sourceAgent: "cataloge2e", Payload: map[string]any{"creation_id": int64(42), "reuse_id": int64(43)}}, catalogRuntimePublishTimeout)
			h.waitForCatalogStoreQuiescence(catalogRuntimePublishTimeout)
			instances, err := h.workflow.ListWorkflowInstances(h.ctx, catalogRuntimeRunID)
			if err != nil {
				t.Fatal(err)
			}
			seen := map[int64]bool{}
			for _, instance := range instances {
				if instance.WorkflowName != "worker" {
					continue
				}
				key, valid := instance.Fields["worker_id"].(int64)
				if !valid || (key != 42 && key != 43) || seen[key] {
					t.Fatalf("typed keys changed at construction/readback: %#v", instance.Fields)
				}
				seen[key] = true
			}
			if len(seen) != 2 {
				t.Fatalf("distinct integer receivers=%v", seen)
			}
		})
	}
}

func TestReceiverSuppliedStateMutationReuseRestartAndSelectedHistoryBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			root := canonicalrouting.CopyReceiverStateMutationGeometry(t)

			h := newRuntimeHarnessForBackend(t, root, backend, true)
			seedCatalogRootStateForRun(t, h, catalogRuntimeRunID)
			initial := catalogTriggerStep{Event: "work.requested", inputKind: catalogReplayInputRootIngress, eventID: uuid.NewString(), createdAt: time.Now().UTC(), sourceAgent: "cataloge2e", Payload: map[string]any{"worker_id": "alpha", "label": "constructed", "count": int64(1)}}
			h.publishAndWait(initial, catalogRuntimePublishTimeout)
			h.waitForCatalogStoreQuiescence(catalogRuntimePublishTimeout)
			creatingOccurrence := func() string {
				lister, err := h.catalogOperatorEventLister()
				if err != nil {
					t.Fatal(err)
				}
				events, err := loadCatalogOperatorEvents(h.ctx, lister)
				if err != nil {
					t.Fatal(err)
				}
				var point string
				occurrences := 0
				for _, event := range events {
					if strings.HasSuffix(event.EventName, "/worker.ready") {
						point = event.EventID
						occurrences++
					}
				}
				if occurrences != 1 || point == "" {
					t.Fatalf("creating occurrences=%d point=%q, want exactly one", occurrences, point)
				}
				return point
			}
			point := creatingOccurrence()
			assert := func(current string) {
				instances, err := h.workflow.ListWorkflowInstances(h.ctx, catalogRuntimeRunID)
				if err != nil {
					t.Fatal(err)
				}
				workers := 0
				for _, instance := range instances {
					if instance.WorkflowName == "worker" {
						workers++
						if instance.Fields["label"] != current {
							t.Fatalf("current state was reinitialized: %+v", instance.Fields)
						}
					}
				}
				if workers != 1 {
					t.Fatalf("worker headers=%d, want one", workers)
				}
				plan, err := runScopedCatalogStore(t, h).PlanRunFork(h.ctx, runfork.RunForkPlanRequest{SourceRunID: catalogRuntimeRunID, At: point})
				if err != nil {
					t.Fatal(err)
				}
				found := false
				for _, entity := range plan.Entities {
					if entity.MaterializationMetadata != nil && entity.MaterializationMetadata.FlowTemplate == "worker" {
						found = true
						if entity.Fields["label"] != "constructed" {
							t.Fatalf("selected history consumed current state: %+v", entity.Fields)
						}
					}
				}
				if !found {
					t.Fatal("selected history omitted the constructed worker")
				}
				if got := creatingOccurrence(); got != point {
					t.Fatalf("creating occurrence changed: got=%s want=%s", got, point)
				}
			}
			assert("constructed")
			changed := catalogTriggerStep{Event: "work.requested", inputKind: catalogReplayInputRootIngress, eventID: uuid.NewString(), createdAt: time.Now().UTC(), sourceAgent: "cataloge2e", Payload: map[string]any{"worker_id": "alpha", "label": "lawfully-updated", "count": int64(9)}}
			h.publishAndWait(changed, catalogRuntimePublishTimeout)
			h.waitForCatalogStoreQuiescence(catalogRuntimePublishTimeout)
			assert("lawfully-updated")
			hash, err := runtimecontracts.BundleHash(h.bundle)
			if err != nil {
				t.Fatal(err)
			}
			digest, err := catalogReplayPlatformSpecDigest(repoRootFromCatalogE2E(t))
			if err != nil {
				t.Fatal(err)
			}
			h = h.reopenFromTranscript(&catalogExecutionTranscript{version: catalogReplayTranscriptVersion, platformSpecDigest: digest, bundleHash: hash, runID: catalogRuntimeRunID, groups: []catalogTranscriptGroup{{steps: []catalogTriggerStep{initial}}, {steps: []catalogTriggerStep{changed}}}})
			h.waitForCatalogStoreQuiescence(catalogRuntimePublishTimeout)
			assert("lawfully-updated")
		})
	}
}

func TestReceiverInitializationNestedGeometryBothStores(t *testing.T) {
	root := canonicalrouting.CopyReceiverInitializationGeometry(t)
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			h := newRuntimeHarnessForBackend(t, root, backend, true)
			seedCatalogRootStateForRun(t, h, catalogRuntimeRunID)
			var groups []catalogTranscriptGroup
			for _, item := range []struct {
				key, label string
				count      int64
			}{{"alpha", "parent-A", 0}, {"beta", "parent-B", 41}} {
				step := catalogTriggerStep{Event: "work.requested", inputKind: catalogReplayInputRootIngress,
					eventID: uuid.NewString(), createdAt: time.Now().UTC(), sourceAgent: "cataloge2e", Payload: map[string]any{
						"worker_id": item.key, "label": item.label, "count": item.count,
					}}
				groups = append(groups, catalogTranscriptGroup{steps: []catalogTriggerStep{step}})
				h.publishAndWait(step, catalogRuntimePublishTimeout)
			}
			h.waitForCatalogStoreQuiescence(catalogRuntimePublishTimeout)
			assertNestedReceiverInitialization(t, h)
			hash, err := runtimecontracts.BundleHash(h.bundle)
			if err != nil {
				t.Fatal(err)
			}
			digest, err := catalogReplayPlatformSpecDigest(repoRootFromCatalogE2E(t))
			if err != nil {
				t.Fatal(err)
			}
			h = h.reopenFromTranscript(&catalogExecutionTranscript{
				version: catalogReplayTranscriptVersion, platformSpecDigest: digest, bundleHash: hash, runID: catalogRuntimeRunID, groups: groups,
			})
			h.publishAndWait(catalogTriggerStep{Event: "work.requested", Payload: map[string]any{
				"worker_id": "alpha", "label": "must-not-reinitialize", "count": int64(99),
			}}, catalogRuntimePublishTimeout)
			h.waitForCatalogStoreQuiescence(catalogRuntimePublishTimeout)
			assertNestedReceiverInitialization(t, h)
			lister, err := h.catalogOperatorEventLister()
			if err != nil {
				t.Fatal(err)
			}
			events, err := loadCatalogOperatorEvents(h.ctx, lister)
			if err != nil {
				t.Fatal(err)
			}
			ready := 0
			for _, event := range events {
				if strings.HasSuffix(event.EventName, "/worker.ready") || strings.HasSuffix(event.EventName, "/leaf.ready") {
					ready++
				}
			}
			if ready != 4 {
				t.Fatalf("creation events=%d after restart and reuse, want exactly four", ready)
			}
			t.Run("absent_parent_no_route", func(t *testing.T) {
				root := canonicalrouting.CopyNestedKeyedConnectionSelection(t)
				path := filepath.Join(root, "schema.yaml")
				body, err := os.ReadFile(path)
				if err != nil || strings.Count(string(body), "to: parent, resolution: select-or-create") != 1 {
					t.Fatalf("absent-parent fixture: %s %v", body, err)
				}
				if err := os.WriteFile(path, []byte(strings.Replace(string(body), "to: parent, resolution: select-or-create", "to: parent, resolution: select", 1)), 0600); err != nil {
					t.Fatal(err)
				}
				h := newRuntimeHarnessForBackend(t, root, backend, true)
				seedCatalogRootStateForRun(t, h, catalogRuntimeRunID)
				ctx := catalogRunContext(h, catalogRuntimeRunID)
				before, err := h.workflow.ListWorkflowInstances(ctx, catalogRuntimeRunID)
				if err != nil || len(before) != 1 {
					t.Fatalf("absent-parent initial tree: %+v %v", before, err)
				}
				id := uuid.NewString()
				if err := h.publishRuntimeEventResultForStep(catalogTriggerStep{Event: "start", Payload: map[string]any{"parent_key": "missing", "leaf_key": "same"}, eventID: id, createdAt: time.Now().UTC(), sourceAgent: "cataloge2e", inputKind: catalogReplayInputRootIngress}, catalogRuntimePublishTimeout, true); err != nil {
					t.Fatal(err)
				}
				after, err := h.workflow.ListWorkflowInstances(ctx, catalogRuntimeRunID)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("absent ancestor manufactured receiver state: before=%+v after=%+v err=%v", before, after, err)
				}
				public, err := catalogRunScopedOperatorEvents(h, catalogRuntimeRunID)
				if err != nil || public[id].EventID != id || public[id].NoDelivery == nil || public[id].NoDelivery.Reason != eventtypes.NoDeliveryResolutionBlocked.Code() || len(public[id].Deliveries) != 0 || len(public[id].DeadLetters) != 1 || public[id].DeadLetters[0].Failure.Class != failures.ClassTargetUnreachable {
					t.Fatalf("absent-parent publication must be a counted no-route outcome: %+v err=%v", public[id], err)
				}
			})
			t.Run("C53_cross_account_delivery", func(t *testing.T) {
				proveCrossAccountDelivery(t, backend)
			})
			for _, shape := range []canonicalrouting.CrossInstanceConnectionShape{canonicalrouting.CrossInstanceSameKeyedFlow, canonicalrouting.CrossInstanceNestedKeyless} {
				for _, agent := range []bool{false, true} {
					t.Run(fmt.Sprintf("one_key_shape_%d_agent_%t", shape, agent), func(t *testing.T) {
						proveOneKeyCrossInstanceDelivery(t, backend, shape, agent)
					})
				}
			}
			for _, changed := range []string{"account", "intermediate"} {
				t.Run("nested_stale_"+changed, func(t *testing.T) {
					proveCrossInstanceStaleAncestor(t, backend, changed)
				})
			}
		})
	}
}

func proveCrossInstanceStaleAncestor(t *testing.T, backend catalogRuntimeBackend, changed string) {
	t.Helper()
	root := canonicalrouting.CopyCrossInstanceConnection(t, canonicalrouting.CrossInstanceNestedKeyless)
	canonicalrouting.AddCrossInstanceKeylessIntermediate(t, root)
	canonicalrouting.UseCrossInstanceFanOut(t, root)
	first := make(chan error, 1)
	retry := make(chan struct{}, 1)
	resume := make(chan struct{})
	var release sync.Once
	unblock := func() { release.Do(func() { close(resume) }) }
	var turns atomic.Int32
	var h *runtimeHarness
	var changedInstance pipeline.WorkflowInstance
	var receiverPath string
	observe := func(ctx context.Context, command pipeline.FanOutChunkCommand) {
		if command.Claim.Key.RunID != catalogRuntimeRunID {
			return
		}
		switch turns.Add(1) {
		case 1:
			if len(command.Outcomes) != 1 || command.Outcomes[0].Publication == nil {
				first <- fmt.Errorf("stale-ancestor fixture lacks its exact prepared ordinal: %+v", command.Outcomes)
				return
			}
			plan, ok := command.Outcomes[0].Publication.(runtimebus.EnginePublicationPlan)
			if !ok {
				first <- fmt.Errorf("unexpected publication plan %T", command.Outcomes[0].Publication)
				return
			}
			routes := plan.PublicationCommand().Commit.DeliveryRoutes
			if len(routes) != 1 || routes[0].Target.Route().FlowInstance != receiverPath {
				first <- fmt.Errorf("stale-ancestor fixture did not select the final receiver: %+v", routes)
				return
			}
			first <- h.rt.Pipeline.MarkTerminated(ctx, catalogExactWorkflowRoute(changedInstance.StorageRef), identity.NormalizeEntityID(changedInstance.EntityID), time.Now().UTC())
		case 2:
			retry <- struct{}{}
			select {
			case <-resume:
			case <-ctx.Done():
			}
		}
	}
	h = newLiveForkCatalogHarness(t, root, backend, observe)
	t.Cleanup(unblock)
	seedCatalogRootStateForRun(t, h, catalogRuntimeRunID)
	for _, key := range []string{"7", "8"} {
		h.publishAndWait(catalogTriggerStep{Event: "opened", Payload: map[string]any{"account_id": key, "ticket_id": "42"}}, catalogRuntimePublishTimeout)
	}
	h.waitForCatalogStoreQuiescence(catalogRuntimePublishTimeout)
	ctx := catalogRunContext(h, catalogRuntimeRunID)
	instances, err := h.workflow.ListWorkflowInstances(ctx, catalogRuntimeRunID)
	if err != nil {
		t.Fatal(err)
	}
	var account, bridge, receiver pipeline.WorkflowInstance
	for _, instance := range instances {
		if instance.WorkflowName == "account" && instance.Fields["account_id"] == "8" {
			account = instance
		}
	}
	for _, instance := range instances {
		if instance.WorkflowName == "account/bridge" && instance.ParentEntityID == account.EntityID {
			bridge = instance
		}
	}
	for _, instance := range instances {
		if instance.WorkflowName == "account/bridge/audit" && instance.ParentEntityID == bridge.EntityID {
			receiver = instance
		}
	}
	if account.EntityID == "" || bridge.EntityID == "" || receiver.EntityID == "" {
		t.Fatalf("stale-ancestor fixture lacks its native chain: account=%+v bridge=%+v receiver=%+v", account, bridge, receiver)
	}
	changedInstance, receiverPath = account, receiver.StorageRef
	if changed == "intermediate" {
		changedInstance = bridge
	}
	h.publishAndWait(catalogTriggerStep{Event: "cross.requested", eventID: uuid.NewString(), Payload: map[string]any{"source_account_id": "7", "ticket_id": "42", "target_account_id": "8", "targets": []any{"8"}}}, catalogRuntimePublishTimeout)
	phase, cancel := context.WithTimeout(ctx, catalogRuntimePublishTimeout)
	defer cancel()
	select {
	case err := <-first:
		if err != nil {
			t.Fatal(err)
		}
	case <-phase.Done():
		t.Fatalf("nested publication never reached its prepared native chunk: %v", phase.Err())
	}
	select {
	case <-retry:
	case <-phase.Done():
		t.Fatalf("changed ancestor did not cause native reobservation/retry: %v", phase.Err())
	}
	var logs interface {
		ListOperatorRuntimeLogs(context.Context, operatorread.OperatorRuntimeLogListOptions) (operatorread.OperatorRuntimeLogListResult, error)
	} = h.pg
	if h.sqlite != nil {
		logs = h.sqlite
	}
	page, err := logs.ListOperatorRuntimeLogs(phase, operatorread.OperatorRuntimeLogListOptions{RunID: catalogRuntimeRunID, ErrorCode: "flow_instance_planning_stale", Limit: 20})
	if err != nil || len(page.Logs) != 1 {
		t.Fatalf("native retry lost the exact stale-planning cause: logs=%+v err=%v", page.Logs, err)
	}
	public, err := catalogRunScopedOperatorEvents(h, catalogRuntimeRunID)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range public {
		if strings.HasSuffix(event.EventName, "/ticket.cross") {
			t.Fatalf("stale native plan leaked its ordinal before the fresh retry: %+v", event)
		}
	}
	instances, err = h.workflow.ListWorkflowInstances(phase, catalogRuntimeRunID)
	var current pipeline.WorkflowInstance
	for _, instance := range instances {
		if instance.StorageRef == receiver.StorageRef {
			current = instance
		}
	}
	if err != nil || !reflect.DeepEqual(receiver, current) {
		t.Fatalf("unchanged final receiver concealed changed ancestor: before=%+v after=%+v err=%v", receiver, current, err)
	}
	unblock()
	h.waitForCatalogStoreQuiescence(catalogRuntimePublishTimeout)
	public, err = catalogRunScopedOperatorEvents(h, catalogRuntimeRunID)
	if err != nil {
		t.Fatal(err)
	}
	refused := 0
	for _, event := range public {
		if strings.HasSuffix(event.EventName, "/ticket.cross") {
			refused++
			if event.NoDelivery == nil || len(event.Deliveries) != 0 {
				t.Fatalf("fresh retry ignored unavailable selected ancestry: %+v", event)
			}
		}
	}
	if refused != 1 {
		t.Fatalf("fresh retry publications=%d, want exactly one counted refusal", refused)
	}
}

func proveOneKeyCrossInstanceDelivery(t *testing.T, backend catalogRuntimeBackend, shape canonicalrouting.CrossInstanceConnectionShape, agent bool) {
	t.Helper()
	root := canonicalrouting.CopyCrossInstanceConnection(t, shape)
	if agent {
		canonicalrouting.UseCrossInstanceAgentReceiver(t, root, shape)
	}
	h := newRuntimeHarnessForBackend(t, root, backend, true)
	seedCatalogRootStateForRun(t, h, catalogRuntimeRunID)
	ctx := catalogRunContext(h, catalogRuntimeRunID)
	for _, key := range []string{"7", "8"} {
		h.publishAndWait(catalogTriggerStep{Event: "opened", Payload: map[string]any{"account_id": key, "ticket_id": "42"}}, catalogRuntimePublishTimeout)
	}
	h.waitForCatalogStoreQuiescence(catalogRuntimePublishTimeout)
	load := func() map[string]pipeline.WorkflowInstance {
		t.Helper()
		instances, err := h.workflow.ListWorkflowInstances(ctx, catalogRuntimeRunID)
		if err != nil {
			t.Fatal(err)
		}
		byPath := make(map[string]pipeline.WorkflowInstance, len(instances))
		for _, instance := range instances {
			byPath[instance.StorageRef] = instance
		}
		return byPath
	}
	before := load()
	accounts := map[string]pipeline.WorkflowInstance{}
	for _, instance := range before {
		if instance.WorkflowName == "account" {
			accounts[instance.Fields["account_id"].(string)] = instance
		}
	}
	if len(accounts) != 2 || accounts["7"].EntityID == "" || accounts["8"].EntityID == "" || accounts["7"].EntityID == accounts["8"].EntityID {
		t.Fatalf("one-key fixture lost distinct account owners: %+v", accounts)
	}
	sender, receiver := accounts["7"], accounts["8"]
	eventName := "account.cross"
	if shape == canonicalrouting.CrossInstanceNestedKeyless {
		eventName = "ticket.cross"
		for _, instance := range before {
			if instance.WorkflowName == "account/ticket" && instance.ParentEntityID == accounts["7"].EntityID && instance.Fields["ticket_id"] == "42" {
				sender = instance
			}
			if instance.WorkflowName == "account/audit" && instance.ParentEntityID == accounts["8"].EntityID {
				receiver = instance
			}
		}
		if sender.WorkflowName != "account/ticket" || receiver.WorkflowName != "account/audit" || receiver.ParentEntityID != accounts["8"].EntityID {
			t.Fatalf("nested source/receiver were not constructed under distinct accounts: sender=%+v receiver=%+v", sender, receiver)
		}
	}
	id := uuid.NewString()
	h.publishAndWait(catalogTriggerStep{Event: "cross.requested", eventID: id, Payload: map[string]any{"source_account_id": "7", "ticket_id": "42", "target_account_id": "8"}}, catalogRuntimePublishTimeout)
	h.waitForCatalogStoreQuiescence(catalogRuntimePublishTimeout)
	public, err := catalogRunScopedOperatorEvents(h, catalogRuntimeRunID)
	if err != nil {
		t.Fatal(err)
	}
	var crossID string
	var delivered eventtypes.DeliveryRoute
	wantTargetEntity := receiver.EntityID
	entityless := agent && shape == canonicalrouting.CrossInstanceNestedKeyless
	if entityless {
		wantTargetEntity = ""
	}
	for _, event := range public {
		if !strings.HasSuffix(event.EventName, "/"+eventName) {
			continue
		}
		if crossID != "" || event.SourceEventID != id || len(event.Deliveries) != 1 || event.Deliveries[0].Status != "delivered" || event.Deliveries[0].Route.Target.Route().FlowInstance != receiver.StorageRef || event.Deliveries[0].Route.Target.Route().EntityID != wantTargetEntity || event.Deliveries[0].Route.Recipient.IsAgent() != agent || len(event.DeadLetters) != 0 || event.NoDelivery != nil {
			t.Fatalf("one-key publication did not reach only its selected receiver: %+v", event)
		}
		delivered = event.Deliveries[0].Route
		if delivered.Target.EntitylessReceiver() != entityless || agent && delivered.AgentIdentity.FlowInstance() != receiver.StorageRef || entityless && (delivered.Initialization.ConstructionTarget().Route().FlowInstance != receiver.StorageRef || delivered.Initialization.ConstructionTarget().Route().EntityID != receiver.EntityID) {
			t.Fatalf("selected actor target lost its distinct exact construction: %+v", delivered)
		}
		crossID = event.EventID
		var selected any = h.pg
		if h.sqlite != nil {
			selected = h.sqlite
		}
		native, present, err := storetest.ReadCanonicalEventRecord(ctx, selected, event.EventID)
		if err != nil || !present || native.SourceRoute().FlowInstance != sender.StorageRef || native.SourceRoute().EntityID != sender.EntityID {
			t.Fatalf("cross-instance selection replaced the actual sender: %+v present=%t err=%v", native, present, err)
		}
	}
	if crossID == "" {
		t.Fatal("cross-instance source emitted no event")
	}
	after := load()
	wantCount := int64(1)
	if agent {
		wantCount = 0
		h.llm.mu.Lock()
		calls := 0
		for _, call := range h.llm.deliveryCalls {
			if call.EventID == crossID && call.RunID == catalogRuntimeRunID && call.TargetEntityID == wantTargetEntity && call.AgentIdentity == delivered.AgentIdentity.Normalize() {
				calls++
			}
		}
		h.llm.mu.Unlock()
		if calls != 1 {
			t.Fatalf("selected agent provider calls=%d, want one at the receiver owner", calls)
		}
	}
	if len(after) != len(before) || after[receiver.StorageRef].Fields["cross_count"] != wantCount {
		t.Fatalf("one-key delivery changed construction or executed the wrong receiver: %+v", after)
	}
	for path, old := range before {
		current := after[path]
		if current.EntityID != old.EntityID || current.Life != old.Life || current.ParentEntityID != old.ParentEntityID || path != receiver.StorageRef && !reflect.DeepEqual(current.Fields, old.Fields) {
			t.Fatalf("one-key delivery changed unrelated identity/fields: before=%+v after=%+v", old, current)
		}
	}
}

func proveCrossAccountDelivery(t *testing.T, backend catalogRuntimeBackend) {
	t.Helper()
	h := newRuntimeHarnessForBackend(t, canonicalrouting.CopyLexicalAddressing(t, true), backend, true)
	seedCatalogRootStateForRun(t, h, catalogRuntimeRunID)
	ctx := catalogRunContext(h, catalogRuntimeRunID)
	for _, key := range []string{"7", "8"} {
		h.publishAndWait(catalogTriggerStep{Event: "opened", Payload: map[string]any{"account_id": key, "ticket_id": "42"}}, catalogRuntimePublishTimeout)
	}
	h.waitForCatalogStoreQuiescence(catalogRuntimePublishTimeout)
	load := func() map[string]pipeline.WorkflowInstance {
		t.Helper()
		instances, err := h.workflow.ListWorkflowInstances(ctx, catalogRuntimeRunID)
		if err != nil {
			t.Fatal(err)
		}
		byPath := make(map[string]pipeline.WorkflowInstance, len(instances))
		for _, instance := range instances {
			byPath[instance.StorageRef] = instance
		}
		return byPath
	}
	before := load()
	accounts := map[string]pipeline.WorkflowInstance{}
	for _, instance := range before {
		if instance.WorkflowName == "account" {
			key, _ := instance.Fields["account_id"].(string)
			accounts[key] = instance
		}
	}
	if len(accounts) != 2 || accounts["7"].EntityID == "" || accounts["8"].EntityID == "" || accounts["7"].EntityID == accounts["8"].EntityID {
		t.Fatalf("cross-account proof lost distinct native owners: %+v", accounts)
	}
	tickets := map[string]pipeline.WorkflowInstance{}
	for _, instance := range before {
		if instance.WorkflowName == "account/ticket" && instance.Fields["ticket_id"] == "42" {
			tickets[instance.ParentEntityID] = instance
		}
	}
	sender, found := tickets[accounts["7"].EntityID]
	if !found || len(tickets) != 2 || tickets[accounts["8"].EntityID].EntityID == sender.EntityID {
		t.Fatalf("same ticket key collapsed structural parents: %+v", tickets)
	}
	id := uuid.NewString()
	h.publishAndWait(catalogTriggerStep{Event: "cross.requested", eventID: id, Payload: map[string]any{"source_account_id": "7", "ticket_id": "42", "target_account_id": "8"}}, catalogRuntimePublishTimeout)
	h.waitForCatalogStoreQuiescence(catalogRuntimePublishTimeout)
	public, err := catalogRunScopedOperatorEvents(h, catalogRuntimeRunID)
	if err != nil {
		t.Fatal(err)
	}
	crossCount := 0
	for _, event := range public {
		if !strings.HasSuffix(event.EventName, "/ticket.cross") {
			continue
		}
		crossCount++
		if event.SourceEventID != id || len(event.Deliveries) != 1 || event.Deliveries[0].Status != "delivered" || event.Deliveries[0].Route.Target.Route().FlowInstance != accounts["8"].StorageRef || len(event.DeadLetters) != 0 || event.NoDelivery != nil {
			t.Fatalf("cross-account publication selected a foreign or extra receiver: %+v", event)
		}
		var selected any = h.pg
		if h.sqlite != nil {
			selected = h.sqlite
		}
		native, present, err := storetest.ReadCanonicalEventRecord(ctx, selected, event.EventID)
		if err != nil || !present || native.SourceRoute().FlowInstance != sender.StorageRef || native.SourceRoute().EntityID != sender.EntityID {
			t.Fatalf("cross-account event lost its actual ticket sender: %+v present=%t err=%v", native, present, err)
		}
	}
	if crossCount != 1 {
		t.Fatalf("cross-account handler emitted %d publications, want one", crossCount)
	}
	after := load()
	if len(after) != len(before) || after[accounts["8"].StorageRef].Fields["cross_count"] != int64(1) {
		t.Fatalf("selected account did not execute exactly once: %+v", after)
	}
	for path, old := range before {
		current := after[path]
		if current.EntityID != old.EntityID || current.Life != old.Life || current.ParentEntityID != old.ParentEntityID || path != accounts["8"].StorageRef && !reflect.DeepEqual(current.Fields, old.Fields) {
			t.Fatalf("cross-account delivery changed unrelated identity/fields: before=%+v after=%+v", old, current)
		}
	}
}

func TestReceiverInitializationRestartBetweenNestedLevelsBothStores(t *testing.T) {
	root := canonicalrouting.CopyReceiverInitializationAgentGeometry(t)
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			h := newRuntimeHarnessForBackend(t, root, backend, true)
			seedCatalogRootStateForRun(t, h, catalogRuntimeRunID)
			started, release := make(chan struct{}, 1), make(chan struct{})
			h.llm.SetManagedRunBarrier(catalogRuntimeRunID, started, release)
			step := catalogTriggerStep{Event: "work.requested", inputKind: catalogReplayInputRootIngress,
				eventID: uuid.NewString(), createdAt: time.Now().UTC(), sourceAgent: "cataloge2e",
				Payload: map[string]any{"worker_id": "alpha", "label": "parent-A", "count": int64(0)}}
			published := make(chan error, 1)
			go func() { published <- h.publishRuntimeEventResultForStep(step, catalogRuntimePublishTimeout, true) }()
			select {
			case <-started:
			case <-time.After(catalogRuntimePublishTimeout):
				t.Fatal("parent agent never reached provider boundary")
			}
			instances, err := h.workflow.ListWorkflowInstances(h.ctx, catalogRuntimeRunID)
			if err != nil {
				t.Fatal(err)
			}
			parents := 0
			for _, instance := range instances {
				if instance.WorkflowName == "worker/leaf" {
					t.Fatal("grandchild was created before the managed provider ran")
				}
				if instance.WorkflowName == "worker" {
					parents++
					if instance.Fields["worker_id"] != "alpha" || instance.Fields["label"] != "parent-A" || instance.Fields["count"] != int64(0) {
						t.Fatalf("parent config at interrupted boundary: %#v", instance.Fields)
					}
				}
			}
			if parents != 1 {
				t.Fatalf("parents at interrupted boundary=%d, want 1", parents)
			}
			h.llm.mu.Lock()
			calls := append([]scriptedDeliveryCall(nil), h.llm.deliveryCalls...)
			h.llm.mu.Unlock()
			if len(calls) != 1 {
				t.Fatalf("provider calls=%+v, want one interrupted parent", calls)
			}
			lister, err := h.catalogOperatorEventLister()
			if err != nil {
				t.Fatal(err)
			}
			events, err := loadCatalogOperatorEvents(h.ctx, lister)
			if err != nil {
				t.Fatal(err)
			}
			ready, ok := events[calls[0].EventID]
			if !ok || !strings.HasSuffix(ready.EventName, "/worker.ready") {
				t.Fatalf("interrupted agent did not consume parent creation: %+v", ready)
			}
			hash, err := runtimecontracts.BundleHash(h.bundle)
			if err != nil {
				t.Fatal(err)
			}
			digest, err := catalogReplayPlatformSpecDigest(repoRootFromCatalogE2E(t))
			if err != nil {
				t.Fatal(err)
			}
			transcript := &catalogExecutionTranscript{version: catalogReplayTranscriptVersion,
				platformSpecDigest: digest, bundleHash: hash, runID: catalogRuntimeRunID,
				groups: []catalogTranscriptGroup{{steps: []catalogTriggerStep{step}}},
				agentFixtures: agentFixtureDoc{AgentFixtures: map[string][]agentFixtureStep{
					calls[0].AgentID: {{On: ready.EventName, Emits: []agentFixtureEmit{{Event: "leaf.requested",
						Payload: map[string]any{"worker_id": "alpha-leaf", "label": "leaf-parent-A", "count": int64(1)}}}}},
				}},
			}
			h.cancel()
			select {
			case <-published:
			case <-time.After(catalogRuntimePublishTimeout):
				t.Fatal("interrupted publisher did not exit")
			}
			h = h.reopenFromTranscript(transcript)
			h.waitForCatalogStoreQuiescence(catalogRuntimePublishTimeout)
			instances, err = h.workflow.ListWorkflowInstances(h.ctx, catalogRuntimeRunID)
			if err != nil {
				t.Fatal(err)
			}
			parents, leaves := 0, 0
			for _, instance := range instances {
				switch instance.WorkflowName {
				case "worker":
					parents++
					if instance.Fields["worker_id"] != "alpha" || instance.Fields["label"] != "parent-A" || instance.Fields["count"] != int64(0) {
						t.Fatalf("parent config changed after restart: %#v", instance.Fields)
					}
				case "worker/leaf":
					leaves++
					if instance.Fields["worker_id"] != "alpha-leaf" || instance.Fields["label"] != "leaf-parent-A" || instance.Fields["count"] != int64(1) {
						t.Fatalf("grandchild did not consume exact supplied state: fields=%#v", instance.Fields)
					}
				}
			}
			if parents != 1 || leaves != 1 {
				t.Fatalf("after restart parents=%d leaves=%d, want one each", parents, leaves)
			}
			lister, err = h.catalogOperatorEventLister()
			if err != nil {
				t.Fatal(err)
			}
			events, err = loadCatalogOperatorEvents(h.ctx, lister)
			if err != nil {
				t.Fatal(err)
			}
			counts := map[string]int{"/worker.ready": 0, "/leaf.requested": 0, "/leaf.ready": 0}
			for _, event := range events {
				for suffix := range counts {
					if strings.HasSuffix(event.EventName, suffix) {
						counts[suffix]++
					}
				}
			}
			for event, count := range counts {
				if count != 1 {
					t.Fatalf("%s events=%d, want exactly one across restart", event, count)
				}
			}
		})
	}
}

func assertNestedReceiverInitialization(t *testing.T, h *runtimeHarness) {
	t.Helper()
	instances, err := h.workflow.ListWorkflowInstances(h.ctx, catalogRuntimeRunID)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, instance := range instances {
		key, ok := instance.Fields["worker_id"].(string)
		if !ok {
			continue // Root flow has no worker state.
		}
		if seen[key] {
			t.Fatalf("duplicate receiver %s", key)
		}
		seen[key] = true
		want := map[string]struct {
			label, flow string
			count       int64
		}{
			"alpha": {"parent-A", "worker", 0}, "beta": {"parent-B", "worker", 41},
			"alpha-leaf": {"leaf-parent-A", "worker/leaf", 1}, "beta-leaf": {"leaf-parent-B", "worker/leaf", 42},
		}
		w, ok := want[key]
		if !ok || instance.WorkflowName != w.flow || instance.Fields["label"] != w.label || instance.Fields["count"] != w.count {
			t.Fatalf("receiver %s: flow=%s config=%#v want=%+v", key, instance.WorkflowName, instance.Fields, w)
		}
		if w.flow == "worker/leaf" && (instance.Fields["label"] != w.label || instance.Fields["count"] != w.count) {
			t.Fatalf("leaf final consumer did not observe initialized config: fields=%#v want=%+v", instance.Fields, w)
		}
	}
	if len(seen) != 4 {
		t.Fatalf("receivers=%v, want two parents and their two children", seen)
	}
}
