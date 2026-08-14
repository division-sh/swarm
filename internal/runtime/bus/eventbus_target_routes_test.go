package bus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/notifyallchildren"
	"github.com/google/uuid"
)

type targetRouteMemoryStore struct {
	mu           sync.Mutex
	events       map[string]events.Event
	settlements  map[string]events.RouteSettlement
	routes       map[string][]events.DeliveryRoute
	scopes       map[string]runtimepipelineobligation.CommittedScope
	missing      []events.PersistedReplayEvent
	receipts     map[string]string
	receiptErrs  map[string]*runtimefailures.Envelope
	claimIssuer  *runtimepipelineobligation.ClaimIssuer
	scanIssuer   *runtimepipelineobligation.ScanIssuer
	claims       map[string]runtimepipelineobligation.Claim
	scans        map[string]runtimepipelineobligation.ScanRequest
	active       map[string]bool
	flowRoutes   []FlowInstanceRouteRecord
	targetOwners []ActiveTargetDescriptor
}

func (s *targetRouteMemoryStore) ListSelectedRunTargetOwners(context.Context) ([]ActiveTargetDescriptor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]ActiveTargetDescriptor(nil), s.targetOwners...), nil
}

func (s *targetRouteMemoryStore) setTargetOwners(owners ...ActiveTargetDescriptor) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.targetOwners = append([]ActiveTargetDescriptor(nil), owners...)
}

func (s *targetRouteMemoryStore) setTargetOwnerRoutes(routes ...events.RouteIdentity) {
	owners := make([]ActiveTargetDescriptor, 0, len(routes))
	for index, route := range routes {
		route = route.Normalized()
		owners = append(owners, ActiveTargetDescriptor{
			ID: fmt.Sprintf("selected-owner-%d", index), EntityID: route.EntityID, FlowInstance: route.FlowInstance,
		})
	}
	s.setTargetOwners(owners...)
}

func testSelectedRunTargetOwner(id, flowInstance, entitySeed string) ActiveTargetDescriptor {
	return ActiveTargetDescriptor{
		ID: id, FlowInstance: flowInstance, EntityID: eventtest.UUID(entitySeed),
	}.Normalized()
}

func targetRouteIdentity(route runtimeflowidentity.Route) runtimeflowidentity.Route {
	return runtimeflowidentity.StoredRoute(route.ScopeKey, route.InstanceID, route.InstancePath)
}

func sameTargetRoute(left, right runtimeflowidentity.Route) bool {
	return targetRouteIdentity(left) == targetRouteIdentity(right)
}

func (s *targetRouteMemoryStore) UpsertFlowInstanceRoute(_ context.Context, route FlowInstanceRouteRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	route.Identity = targetRouteIdentity(route.Identity)
	for i, existing := range s.flowRoutes {
		if sameTargetRoute(existing.Identity, route.Identity) && existing.EventPattern == route.EventPattern && existing.SubscriberType == route.SubscriberType && existing.SubscriberID == route.SubscriberID {
			s.flowRoutes[i] = route
			return nil
		}
	}
	s.flowRoutes = append(s.flowRoutes, route)
	return nil
}

func (s *targetRouteMemoryStore) DeleteFlowInstanceRoute(_ context.Context, identity runtimeflowidentity.Route) error {
	return s.RollbackFlowInstanceRoute(context.Background(), identity)
}

func (s *targetRouteMemoryStore) ListFlowInstanceRoutes(context.Context) ([]runtimeflowidentity.Route, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	seen := make(map[runtimeflowidentity.Route]struct{}, len(s.flowRoutes))
	routes := make([]runtimeflowidentity.Route, 0, len(s.flowRoutes))
	for _, record := range s.flowRoutes {
		identity := targetRouteIdentity(record.Identity)
		if _, ok := seen[identity]; ok {
			continue
		}
		seen[identity] = struct{}{}
		routes = append(routes, identity)
	}
	return routes, nil
}

func (s *targetRouteMemoryStore) ReplaceFlowInstanceRouteRecords(_ context.Context, identity runtimeflowidentity.Route, routes []FlowInstanceRouteRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	identity = targetRouteIdentity(identity)
	retained := s.flowRoutes[:0]
	for _, existing := range s.flowRoutes {
		if !sameTargetRoute(existing.Identity, identity) {
			retained = append(retained, existing)
		}
	}
	s.flowRoutes = retained
	for _, route := range routes {
		route.Identity = targetRouteIdentity(route.Identity)
		s.flowRoutes = append(s.flowRoutes, route)
	}
	return nil
}

func (s *targetRouteMemoryStore) ListFlowInstanceRouteRecords(_ context.Context, identity runtimeflowidentity.Route) ([]FlowInstanceRouteRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	identity = targetRouteIdentity(identity)
	var routes []FlowInstanceRouteRecord
	for _, route := range s.flowRoutes {
		if sameTargetRoute(route.Identity, identity) {
			routes = append(routes, route)
		}
	}
	return routes, nil
}

func (s *targetRouteMemoryStore) RollbackFlowInstanceRoute(_ context.Context, identity runtimeflowidentity.Route) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	identity = targetRouteIdentity(identity)
	retained := s.flowRoutes[:0]
	for _, existing := range s.flowRoutes {
		if !sameTargetRoute(existing.Identity, identity) {
			retained = append(retained, existing)
		}
	}
	s.flowRoutes = retained
	return nil
}

func newTargetRouteMemoryStore() *targetRouteMemoryStore {
	return &targetRouteMemoryStore{
		events:      map[string]events.Event{},
		settlements: map[string]events.RouteSettlement{},
		routes:      map[string][]events.DeliveryRoute{},
		scopes:      map[string]runtimepipelineobligation.CommittedScope{},
		active:      map[string]bool{},
	}
}

func (*targetRouteMemoryStore) LoadRunOrigin(context.Context, string) (runtimerunlifecycle.RunOrigin, error) {
	return runtimerunlifecycle.ScenarioSetupRunOrigin(), nil
}

func (s *targetRouteMemoryStore) CommitPublication(_ context.Context, command PublicationCommand) (CommittedPublication, error) {
	if err := command.Validate(); err != nil {
		return CommittedPublication{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	event := command.Commit.Event.Event()
	if _, exists := s.events[event.ID()]; exists {
		result := CommittedPublication{
			AppendOutcome: EventAppendExactDuplicate,
			RouteTopology: cloneFlowInstanceRouteTopology(command.RouteTopology),
		}
		for _, plan := range command.Activations {
			result.Activations = append(result.Activations, CommittedFlowInstanceActivation{Plan: plan})
		}
		s.replaceFlowInstanceRouteTopologyLocked(command.RouteTopology)
		return result, result.Validate()
	}
	s.events[event.ID()] = event
	s.settlements[event.ID()] = command.Commit.RouteSettlement
	s.routes[event.ID()] = events.NormalizeDeliveryRoutes(command.Commit.DeliveryRoutes)
	s.scopes[event.ID()] = command.Commit.ReplayScope
	if command.Commit.Disposition != nil {
		if s.receipts == nil {
			s.receipts = map[string]string{}
		}
		if s.receiptErrs == nil {
			s.receiptErrs = map[string]*runtimefailures.Envelope{}
		}
		s.receipts[event.ID()] = string(command.Commit.Disposition.Kind())
		s.receiptErrs[event.ID()] = command.Commit.Disposition.Failure()
	}
	result := CommittedPublication{
		AppendOutcome: EventAppendInserted,
		RouteTopology: cloneFlowInstanceRouteTopology(command.RouteTopology),
	}
	for _, plan := range command.Activations {
		result.Activations = append(result.Activations, CommittedFlowInstanceActivation{Plan: plan, Created: true})
	}
	s.replaceFlowInstanceRouteTopologyLocked(command.RouteTopology)
	return result, result.Validate()
}

func (s *targetRouteMemoryStore) LoadPreparedPublishEvent(_ context.Context, eventID string) (PreparedPublishEvent, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	event, found := s.events[strings.TrimSpace(eventID)]
	if !found {
		return PreparedPublishEvent{}, false, nil
	}
	admitted, err := events.RevalidatePersistedEvent(event)
	if err != nil {
		return PreparedPublishEvent{}, false, err
	}
	routes := append([]events.DeliveryRoute(nil), s.routes[event.ID()]...)
	settlement := s.settlements[event.ID()]
	if err := settlement.Validate(routes); err != nil {
		return PreparedPublishEvent{}, false, err
	}
	return PreparedPublishEvent{Event: admitted, Settlement: settlement, DeliveryRoutes: routes}, true, nil
}

func (s *targetRouteMemoryStore) ReplaceFlowInstanceRouteTopology(_ context.Context, sets []FlowInstanceRouteRecordSet) error {
	if err := validateFlowInstanceRouteTopology(sets); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replaceFlowInstanceRouteTopologyLocked(sets)
	return nil
}

func (s *targetRouteMemoryStore) replaceFlowInstanceRouteTopologyLocked(sets []FlowInstanceRouteRecordSet) {
	for _, set := range sets {
		identity := targetRouteIdentity(set.Identity)
		retained := s.flowRoutes[:0]
		for _, existing := range s.flowRoutes {
			if !sameTargetRoute(existing.Identity, identity) {
				retained = append(retained, existing)
			}
		}
		s.flowRoutes = retained
		for _, route := range set.Routes {
			route.Identity = targetRouteIdentity(route.Identity)
			s.flowRoutes = append(s.flowRoutes, route)
		}
	}
}

func (s *targetRouteMemoryStore) ListEventDeliveryRecipients(_ context.Context, eventID string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, route := range s.routes[eventID] {
		if route.Recipient.IsAgent() {
			out = append(out, route.Recipient.ID())
		}
	}
	return uniqueStrings(out), nil
}

func (s *targetRouteMemoryStore) ListEventDeliveryRoutes(_ context.Context, eventID string) ([]events.DeliveryRoute, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]events.DeliveryRoute(nil), s.routes[eventID]...), nil
}

func (s *targetRouteMemoryStore) PipelineObligations() runtimepipelineobligation.Store {
	return s
}

func (s *targetRouteMemoryStore) ClaimPublication(_ context.Context, eventID string) (runtimepipelineobligation.Claim, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.issuePipelineClaim(eventID, runtimepipelineobligation.PurposePublication)
}

func (s *targetRouteMemoryStore) ClaimEvent(_ context.Context, eventID string, purpose runtimepipelineobligation.Purpose) (runtimepipelineobligation.ClaimedWork, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.claimPipelineWork(eventID, purpose)
}

func (s *targetRouteMemoryStore) OpenScan(_ context.Context, request runtimepipelineobligation.ScanRequest) (runtimepipelineobligation.Scan, error) {
	if err := request.Validate(); err != nil {
		return runtimepipelineobligation.Scan{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.scanIssuer == nil {
		s.scanIssuer = runtimepipelineobligation.NewScanIssuer()
	}
	if s.scans == nil {
		s.scans = map[string]runtimepipelineobligation.ScanRequest{}
	}
	scan, err := s.scanIssuer.Issue()
	if err == nil {
		token, tokenErr := s.scanIssuer.Token(scan)
		if tokenErr != nil {
			err = tokenErr
		} else {
			s.scans[token] = request
		}
	}
	return scan, err
}

func (s *targetRouteMemoryStore) ClaimBatch(_ context.Context, scan runtimepipelineobligation.Scan, limit int) (runtimepipelineobligation.ScanBatch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	token, err := s.scanIssuer.Token(scan)
	if err != nil {
		return runtimepipelineobligation.ScanBatch{}, err
	}
	request, ok := s.scans[token]
	if !ok {
		return runtimepipelineobligation.ScanBatch{}, runtimepipelineobligation.ErrStaleScan
	}
	batch := runtimepipelineobligation.ScanBatch{}
	for phase := 0; batch.Examined < limit; phase++ {
		query, ok := request.QueryAt(phase)
		if !ok {
			batch.Exhausted = true
			return batch, nil
		}
		if query.Purpose == runtimepipelineobligation.PurposeDecisionRoute {
			continue
		}
		for _, record := range s.missing {
			if strings.TrimSpace(query.RunID) != "" && record.Event.RunID() != strings.TrimSpace(query.RunID) {
				continue
			}
			batch.Examined++
			work, err := s.claimPipelineWork(record.Event.ID(), query.Purpose)
			if errors.Is(err, runtimepipelineobligation.ErrBusy) {
				batch.LocallyBlocked = true
				continue
			}
			if errors.Is(err, runtimepipelineobligation.ErrIneligible) {
				continue
			}
			if err != nil {
				return batch, err
			}
			batch.Work = append(batch.Work, work)
			if batch.Examined == limit {
				return batch, nil
			}
		}
	}
	return batch, nil
}

func (s *targetRouteMemoryStore) CloseScan(_ context.Context, scan runtimepipelineobligation.Scan) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	token, err := s.scanIssuer.Token(scan)
	if err != nil {
		return err
	}
	if _, ok := s.scans[token]; !ok {
		return runtimepipelineobligation.ErrStaleScan
	}
	delete(s.scans, token)
	return nil
}

func (s *targetRouteMemoryStore) issuePipelineClaim(eventID string, purpose runtimepipelineobligation.Purpose) (runtimepipelineobligation.Claim, error) {
	eventID = strings.TrimSpace(eventID)
	if s.claimIssuer == nil {
		s.claimIssuer = runtimepipelineobligation.NewClaimIssuer()
	}
	if s.claims == nil {
		s.claims = map[string]runtimepipelineobligation.Claim{}
	}
	if _, exists := s.claims[eventID]; exists {
		return runtimepipelineobligation.Claim{}, runtimepipelineobligation.ErrBusy
	}
	claim, err := s.claimIssuer.Issue(eventID, purpose)
	if err != nil {
		return runtimepipelineobligation.Claim{}, err
	}
	s.claims[eventID] = claim
	return claim, nil
}

func (s *targetRouteMemoryStore) claimPipelineWork(eventID string, purpose runtimepipelineobligation.Purpose) (runtimepipelineobligation.ClaimedWork, error) {
	eventID = strings.TrimSpace(eventID)
	evt, exists := s.events[eventID]
	if !exists {
		for _, record := range s.missing {
			if record.Event.ID() == eventID {
				evt, exists = record.Event, true
				break
			}
		}
	}
	if !exists || s.receipts[eventID] != "" {
		return runtimepipelineobligation.ClaimedWork{}, runtimepipelineobligation.ErrIneligible
	}
	scope, err := runtimepipelineobligation.ParseCommittedScope(string(s.scopes[eventID]))
	if err != nil {
		return runtimepipelineobligation.ClaimedWork{}, runtimepipelineobligation.ErrMissingScope
	}
	claim, err := s.issuePipelineClaim(eventID, purpose)
	if err != nil {
		return runtimepipelineobligation.ClaimedWork{}, err
	}
	return runtimepipelineobligation.ClaimedWork{Event: evt, Scope: scope, Claim: claim}, nil
}

func (s *targetRouteMemoryStore) MarkDecisionProcessed(context.Context, runtimepipelineobligation.Claim) error {
	return runtimepipelineobligation.ErrIneligible
}

func (s *targetRouteMemoryStore) Settle(_ context.Context, claim runtimepipelineobligation.Claim, disposition runtimepipelineobligation.Disposition) (runtimepipelineobligation.SettlementOutcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.verifyPipelineClaim(claim); err != nil {
		return runtimepipelineobligation.SettlementOutcome{}, err
	}
	if err := disposition.ValidateFor(claim.Purpose()); err != nil {
		return runtimepipelineobligation.SettlementOutcome{}, err
	}
	if s.receipts == nil {
		s.receipts = map[string]string{}
	}
	if s.receiptErrs == nil {
		s.receiptErrs = map[string]*runtimefailures.Envelope{}
	}
	status := "processed"
	if !disposition.Successful() {
		status = "dead_letter"
	}
	s.receipts[claim.EventID()] = status
	s.receiptErrs[claim.EventID()] = disposition.Failure()
	delete(s.claims, claim.EventID())
	return runtimepipelineobligation.CommittedSettlement(disposition.Successful()), nil
}

func (s *targetRouteMemoryStore) Release(_ context.Context, claim runtimepipelineobligation.Claim) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.verifyPipelineClaim(claim); err != nil {
		return err
	}
	delete(s.claims, claim.EventID())
	return nil
}

func (s *targetRouteMemoryStore) verifyPipelineClaim(claim runtimepipelineobligation.Claim) error {
	if s.claimIssuer == nil {
		return runtimepipelineobligation.ErrStaleClaim
	}
	stored, exists := s.claims[claim.EventID()]
	if !exists {
		return runtimepipelineobligation.ErrStaleClaim
	}
	if err := s.claimIssuer.Verify(claim, claim.EventID(), claim.Purpose()); err != nil {
		return err
	}
	storedToken, err := s.claimIssuer.Token(stored)
	if err != nil {
		return err
	}
	claimToken, err := s.claimIssuer.Token(claim)
	if err != nil {
		return err
	}
	if storedToken != claimToken {
		return runtimepipelineobligation.ErrStaleClaim
	}
	return nil
}

func (s *targetRouteMemoryStore) GlobalWorkPresence(context.Context) (runtimepipelineobligation.GlobalWorkPresence, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return runtimepipelineobligation.GlobalWorkPresence{ProcessingEligible: len(s.missing) > 0}, nil
}

func (s *targetRouteMemoryStore) SummarizeRun(_ context.Context, runID string) (runtimepipelineobligation.RunSummary, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	summary := runtimepipelineobligation.RunSummary{RunID: strings.TrimSpace(runID)}
	for _, record := range s.missing {
		if record.Event.RunID() == summary.RunID && s.receipts[record.Event.ID()] == "" {
			summary.Replayable++
		}
	}
	return summary, nil
}

func (s *targetRouteMemoryStore) TerminalizeRun(context.Context, string, runtimepipelineobligation.Disposition, time.Time) (int, error) {
	return 0, nil
}

type materializedRoutePersistedBeforeInterceptor struct {
	t       *testing.T
	store   *targetRouteMemoryStore
	eventID string
	want    events.DeliveryRoute
}

func (i materializedRoutePersistedBeforeInterceptor) Intercept(ctx context.Context, evt events.Event) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	i.t.Helper()
	if evt.ID() != i.eventID {
		i.t.Fatalf("interceptor event_id = %q, want %q", evt.ID(), i.eventID)
	}
	routes, err := i.store.ListEventDeliveryRoutes(ctx, i.eventID)
	if err != nil {
		i.t.Fatalf("ListEventDeliveryRoutes: %v", err)
	}
	if !deliveryRoutesContain(routes, i.want) {
		i.t.Fatalf("persisted routes before interceptor = %#v, want %#v", routes, i.want)
	}
	return true, nil, runtimepipelineobligation.Continue(), nil
}

type targetRouteConsumingInterceptor struct {
	targetCalls int
}

func (i *targetRouteConsumingInterceptor) Intercept(_ context.Context, evt events.Event) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	if evt.TargetRoute().Empty() {
		return true, nil, runtimepipelineobligation.Continue(), nil
	}
	i.targetCalls++
	return false, nil, runtimepipelineobligation.Continue(), nil
}

func (i *targetRouteConsumingInterceptor) InterceptDeliveryRoute(_ context.Context, delivery events.DeliveryEvent, route events.DeliveryRoute) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	evt := delivery.Event()
	if route.Target.Route().Normalized().Empty() {
		return true, nil, runtimepipelineobligation.Continue(), nil
	}
	if evt.TargetRoute().Normalized() != route.Target.Route().Normalized() {
		return true, nil, runtimepipelineobligation.Continue(), nil
	}
	i.targetCalls++
	return false, nil, runtimepipelineobligation.Continue(), nil
}

func TestEventBusRecipientPlanMaterializerPersistsRoutesBeforeInterceptors(t *testing.T) {
	store := newTargetRouteMemoryStore()
	eventID := uuid.NewString()
	source := semanticview.Wrap(materializedTargetBundle("review", "target-node", "task.started"))
	wantBlueprint := DeliveryRouteBlueprint{Recipient: events.MustNodeDeliveryRecipient("target-node"), Target: events.RouteIdentity{
		FlowID: "review", FlowInstance: "review/inst-1",
	}, Handler: runtimepipeline.MustDeliveryTargetHandler("review", "target-node").ForEvent("task.started")}
	want := events.DeliveryRoute{Recipient: wantBlueprint.Recipient, Target: events.MustEntitylessReceiverTarget(wantBlueprint.Target)}
	guardSawMaterializedRoute := false
	eb, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle: source,
		RecipientPlanMaterializer: func(ctx context.Context, evt events.Event, plan PublishRecipientPlan) ([]DeliveryRouteBlueprint, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if evt.ID() != eventID {
				t.Fatalf("materializer event_id = %q, want %q", evt.ID(), eventID)
			}
			if len(plan.DeliveryRoutes) != 0 {
				t.Fatalf("pre-materialized delivery routes = %#v, want none", plan.DeliveryRoutes)
			}
			return []DeliveryRouteBlueprint{wantBlueprint}, nil
		},
		RecipientPlanGuard: func(ctx context.Context, evt events.Event, plan PublishRecipientPlan) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if !deliveryRoutesContain(plan.DeliveryRoutes, want) {
				t.Fatalf("guard delivery routes = %#v, want materialized %#v", plan.DeliveryRoutes, want)
			}
			guardSawMaterializedRoute = true
			return nil
		},
		Interceptors: []EventInterceptor{materializedRoutePersistedBeforeInterceptor{
			t:       t,
			store:   store,
			eventID: eventID,
			want:    want,
		}},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.Publish(context.Background(), eventtest.RunCreatingRootIngress(eventID,
		events.EventType("review/inst-1/task.started"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC())); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if !guardSawMaterializedRoute {
		t.Fatal("recipient plan guard did not see materialized route")
	}
}

func TestEventBusRejectsEntitylessOwnershipForCompleteHandlerShapeBeforePersistence(t *testing.T) {
	tests := []struct {
		name    string
		handler runtimecontracts.SystemNodeEventHandler
	}{
		{name: "fan out", handler: runtimecontracts.SystemNodeEventHandler{FanOut: &runtimecontracts.FanOutSpec{
			ItemsFrom: "entity.items", As: "item", Identity: "item.id", Emit: runtimecontracts.EmitSpec{Event: "item.ready", From: "payload"},
		}}},
		{name: "group by", handler: runtimecontracts.SystemNodeEventHandler{GroupBy: &runtimecontracts.GroupBySpec{
			ItemsFrom: "entity.items", Key: "item.kind", StoreAs: "computed.groups",
		}}},
		{name: "on success emit", handler: runtimecontracts.SystemNodeEventHandler{OnSuccess: runtimecontracts.HandlerOnSuccessSpec{
			Emit: runtimecontracts.EmitSpec{Event: "work.done", From: "entity"},
		}}},
		{name: "join membership", handler: runtimecontracts.SystemNodeEventHandler{Join: &runtimecontracts.JoinSpec{
			ID: "all", Stage: "waiting", Members: runtimecontracts.JoinMembersSpec{From: "entity.expected", By: "payload.member_id"}, Output: "computed.members", CompleteWhen: "join.received_count == join.expected_count",
		}}},
		{name: "loop lifecycle", handler: runtimecontracts.SystemNodeEventHandler{Loop: &runtimecontracts.LoopOperationSpec{Admit: "revision", From: "waiting"}}},
		{name: "payload accumulator", handler: runtimecontracts.SystemNodeEventHandler{Accumulate: &runtimecontracts.AccumulateSpec{Into: "items", From: "payload"}}},
		{name: "approval activity", handler: runtimecontracts.SystemNodeEventHandler{Activity: runtimecontracts.ActivitySpec{
			Tool: "review", Approval: &runtimecontracts.ActivityApprovalSpec{Decision: "review_change"},
		}}},
		{name: "nested approval activity", handler: runtimecontracts.SystemNodeEventHandler{Rules: []runtimecontracts.HandlerRuleEntry{{
			Activity: runtimecontracts.ActivitySpec{Tool: "review", Approval: &runtimecontracts.ActivityApprovalSpec{Decision: "review_change"}},
		}}}},
		{name: "guard kill", handler: runtimecontracts.SystemNodeEventHandler{Guard: &runtimecontracts.GuardSpec{Check: "false", OnFail: "kill"}}},
		{name: "accumulator clear", handler: runtimecontracts.SystemNodeEventHandler{Clear: &runtimecontracts.ClearSpec{Targets: []string{"accumulator_state"}}}},
		{name: "pending dedup clear", handler: runtimecontracts.SystemNodeEventHandler{Clear: &runtimecontracts.ClearSpec{Targets: []string{"pending_dedup"}}}},
		{name: "unrooted entity field clear", handler: runtimecontracts.SystemNodeEventHandler{Clear: &runtimecontracts.ClearSpec{Targets: []string{"revision_count"}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			const eventType = "task.started"
			store := newTargetRouteMemoryStore()
			source := semanticview.Wrap(materializedTargetBundleWithHandler("review", "target-node", eventType, test.handler))
			eb, err := newScopedTestEventBus(store, EventBusOptions{
				ContractBundle: source,
				RecipientPlanMaterializer: func(context.Context, events.Event, PublishRecipientPlan) ([]DeliveryRouteBlueprint, error) {
					return []DeliveryRouteBlueprint{{
						Recipient: events.MustNodeDeliveryRecipient("target-node"),
						Target:    events.RouteIdentity{FlowID: "review", FlowInstance: "review/inst-1"},
						Handler:   runtimepipeline.MustDeliveryTargetHandler("review", "target-node").ForEvent(eventType),
					}}, nil
				},
			})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			eventID := uuid.NewString()
			evt := eventtest.RunCreatingRootIngress(eventID, "review/inst-1/"+eventType, "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC())
			if err := eb.Publish(context.Background(), evt); err == nil || !strings.Contains(err.Error(), "target owner is missing") {
				t.Fatalf("Publish error = %v, want missing typed target owner", err)
			}
			if _, persisted := store.events[eventID]; persisted || len(store.routes[eventID]) != 0 {
				t.Fatalf("failed publish persisted event/routes: event=%t routes=%#v", persisted, store.routes[eventID])
			}
		})
	}
}

func TestEventBusRejectsSelectedEntityForEntitylessHandlerBeforePersistence(t *testing.T) {
	const eventType = "task.started"
	store := newTargetRouteMemoryStore()
	store.setTargetOwnerRoutes(events.RouteIdentity{
		FlowID: "review", FlowInstance: "review/inst-1", EntityID: eventtest.UUID("selected-entityless-owner"),
	})
	source := semanticview.Wrap(materializedTargetBundleWithHandler(
		"review", "target-node", eventType, runtimecontracts.SystemNodeEventHandler{},
	))
	eb, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle: source,
		RecipientPlanMaterializer: func(context.Context, events.Event, PublishRecipientPlan) ([]DeliveryRouteBlueprint, error) {
			return []DeliveryRouteBlueprint{{
				Recipient: events.MustNodeDeliveryRecipient("target-node"),
				Target:    events.RouteIdentity{FlowID: "review", FlowInstance: "review/inst-1"},
				Handler:   runtimepipeline.MustDeliveryTargetHandler("review", "target-node").ForEvent(eventType),
			}}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	eventID := uuid.NewString()
	evt := eventtest.RunCreatingRootIngress(
		eventID, "review/inst-1/"+eventType, "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC(),
	)
	if err := eb.Publish(context.Background(), evt); err == nil || !strings.Contains(err.Error(), "entityless-safe handler has selected entity ownership evidence") {
		t.Fatalf("Publish error = %v, want selected/entityless contradiction", err)
	}
	if _, persisted := store.events[eventID]; persisted || len(store.routes[eventID]) != 0 {
		t.Fatalf("failed publish persisted event/routes: event=%t routes=%#v", persisted, store.routes[eventID])
	}
}

func TestEventBusPublish_TargetedNodeConsumeSuppressesLiveRecipientDelivery(t *testing.T) {
	const eventType = "worker/work.started"
	eventID := uuid.NewString()
	target := events.RouteIdentity{
		FlowID:       "worker",
		FlowInstance: "worker/inst-1",
		EntityID:     eventtest.UUID("ent-1"),
	}
	targetRoute := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient("target-node"), Target: events.MustExistingEntityTarget(target)}
	targetHandler := runtimepipeline.MustDeliveryTargetHandler("worker", "target-node")
	targetBlueprint := DeliveryRouteBlueprint{Recipient: targetRoute.Recipient, Target: target,
		Handler: targetHandler.ForEvent("work.started")}
	rt := newRouteTable(nil)
	rt.eventPath[eventType] = struct{}{}
	rt.routes[eventType] = []Subscriber{{Recipient: events.MustNodeDeliveryRecipient("target-node"), Path: "worker",
		LocalizedEvent: "work.started", handlerFlowID: "worker", handlerNodeID: "target-node", targetHandler: targetHandler,
		routeSource: subscriberRouteSourceSubscription,
	}}
	interceptor := &targetRouteConsumingInterceptor{}
	store := newTargetRouteMemoryStore()
	store.setTargetOwnerRoutes(target)
	eb, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle: semanticview.Wrap(materializedTargetBundleWithHandler(
			"worker", "target-node", "work.started", existingOwnerHandlerFixture(),
		)),
		RouteTable: rt,
		RecipientPlanMaterializer: func(context.Context, events.Event, PublishRecipientPlan) ([]DeliveryRouteBlueprint, error) {
			return []DeliveryRouteBlueprint{targetBlueprint}, nil
		},
		Interceptors: []EventInterceptor{interceptor},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	admission := testAgentSubscriptionAdmissionForFlow(t, "target-node", "worker", events.EventType(eventType))
	live := subscribeTestAgentAdmissionWithIdentity(
		t, eb, admission, testAgentRouteIdentity(t, "target-node", "worker"), target.EntityID,
	)
	defer unsubscribeTestAgent(eb, "target-node")
	evt := eventtest.RunCreatingRootIngress(
		eventID,
		events.EventType(eventType),
		"",
		"",
		[]byte(`{}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-1")),
		time.Now().UTC(),
	)
	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if len(plan.Recipients) != 1 || plan.Recipients[0] != "target-node" {
		t.Fatalf("recipients = %#v, want target-node live recipient", plan.Recipients)
	}
	if !deliveryRoutesContain(plan.DeliveryRoutes, targetRoute) {
		t.Fatalf("delivery routes = %#v, want target route %#v", plan.DeliveryRoutes, targetRoute)
	}
	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if interceptor.targetCalls != 1 {
		t.Fatalf("target interceptor calls = %d, want 1", interceptor.targetCalls)
	}
	select {
	case got := <-live:
		t.Fatalf("target-consuming node event leaked to live recipient: %#v", got)
	default:
	}
}

func TestEventBusRecipientPlanMaterializerNormalizesRoutePlanDirectly(t *testing.T) {
	source := semanticview.Wrap(materializedTargetBundle("review", "target-node", "task.started"))
	wantBlueprint := DeliveryRouteBlueprint{Recipient: events.MustNodeDeliveryRecipient("target-node"), Target: events.RouteIdentity{
		FlowID: "review", FlowInstance: "review/inst-1",
	}, Handler: runtimepipeline.MustDeliveryTargetHandler("review", "target-node").ForEvent("task.started")}
	want := events.DeliveryRoute{Recipient: wantBlueprint.Recipient, Target: events.MustEntitylessReceiverTarget(wantBlueprint.Target)}
	eb, err := newScopedTestEventBus(InMemoryEventStore{}, EventBusOptions{
		ContractBundle: source,
		RecipientPlanMaterializer: func(ctx context.Context, evt events.Event, plan PublishRecipientPlan) ([]DeliveryRouteBlueprint, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if len(plan.DeliveryRoutes) != 0 {
				t.Fatalf("pre-materialized delivery routes = %#v, want none", plan.DeliveryRoutes)
			}
			return []DeliveryRouteBlueprint{wantBlueprint}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	evt := eventtest.RunCreatingRootIngress(uuid.NewString(), events.EventType("review/inst-1/task.started"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{})
	emptyAgentPolicyPlan := routePlanFromManifest(evt, deliveryRecipientManifest{}, routeIntentProducerAgentPolicy)
	if emptyAgentPolicyPlan.AuthorityState != RoutePlanAuthorityNoCanonicalMatch || emptyAgentPolicyPlan.AuthorityOwner != 0 {
		t.Fatalf("empty agent-policy route plan authority = %q/%q, want no canonical match", emptyAgentPolicyPlan.AuthorityState, emptyAgentPolicyPlan.AuthorityOwner)
	}

	plan, err := eb.materializePublishRecipientPlan(context.Background(), evt, emptyAgentPolicyPlan)
	if err != nil {
		t.Fatalf("materializePublishRecipientPlan: %v", err)
	}
	if got, wantLen := len(plan.DeliveryIntents), 1; got != wantLen {
		t.Fatalf("route plan delivery intents = %d, want %d", got, wantLen)
	}
	if plan.AuthorityState != RoutePlanAuthorityLowerPrecedence || plan.AuthorityOwner != routePlanSourceRecipientMaterializer {
		t.Fatalf("route plan authority = %q/%q, want lower-precedence materializer", plan.AuthorityState, plan.AuthorityOwner)
	}
	intent := plan.DeliveryIntents[0]
	if intent.Recipient != want.Recipient || intent.TargetOwnership != want.Target {
		t.Fatalf("route plan delivery intent = %#v, want route %#v", intent, want)
	}
	if intent.Producer != routeIntentProducerRecipientMaterializer {
		t.Fatalf("route plan materializer producer = %q, want materializer route", routeIntentProducerCode(intent.Producer))
	}
	projected := eb.publishRecipientPlan(evt, plan)
	if !deliveryRoutesContain(projected.DeliveryRoutes, want) {
		t.Fatalf("projected delivery routes = %#v, want materialized route %#v", projected.DeliveryRoutes, want)
	}
}

func materializedTargetBundle(flowID, nodeID, eventType string) *runtimecontracts.WorkflowContractBundle {
	return materializedTargetBundleWithHandler(flowID, nodeID, eventType, runtimecontracts.SystemNodeEventHandler{})
}

func materializedTargetBundleWithHandler(flowID, nodeID, eventType string, handler runtimecontracts.SystemNodeEventHandler) *runtimecontracts.WorkflowContractBundle {
	flow := runtimecontracts.FlowContractView{
		Path: flowID, Paths: runtimecontracts.FlowContractPaths{ID: flowID, Flow: flowID},
		Schema: runtimecontracts.FlowSchemaDocument{Mode: "template"},
		Events: map[string]runtimecontracts.EventCatalogEntry{eventType: {}},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			nodeID: {
				ID: nodeID, ExecutionType: "system_node", SubscribesTo: []string{eventType},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{eventType: handler},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{flow}}
	return &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root, ByID: map[string]*runtimecontracts.FlowContractView{flowID: &root.Children[0]},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{flowID: {Mode: "template"}},
	}
}

func TestEventBusAgentDispatchDoesNotCrossSameIDNodeRouteTargets(t *testing.T) {
	store := newTargetRouteMemoryStore()
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := subscribeTestAgentAdmission(t, eb, testAgentSubscriptionAdmissionForFlow(t, "shared-subscriber", "review/inst-1", events.EventType("review/inst-1/task.started")))
	defer unsubscribeTestAgent(eb, "shared-subscriber")

	evt := eventtest.RunCreatingRootIngress(uuid.NewString(),
		events.EventType("review/inst-1/task.started"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	if err := eb.deliverToRecipientsWithRoutes(context.Background(), evt, []string{"shared-subscriber"}, []events.DeliveryRoute{{Recipient: events.MustNodeDeliveryRecipient("shared-subscriber"), Target: events.MustEntitylessReceiverTarget(events.RouteIdentity{
		FlowInstance: "review/inst-1",
	}),
	}}); err != nil {
		t.Fatalf("deliverToRecipientsWithRoutes: %v", err)
	}
	requireNoBusEvent(t, ch, "same-id node route crossing into agent subscriber class")
}

func TestEventBusWorkflowRuntimeCarrierPrefersConcreteNodeRouteOverPlaceholder(t *testing.T) {
	eb, err := newScopedTestEventBus(newTargetRouteMemoryStore())
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := subscribeInternalDeliveriesForTest(t, eb, workflowRuntimeInternalCarrierID, events.EventType("review/task.started"))
	defer unsubscribeTestAgent(eb, workflowRuntimeInternalCarrierID)

	contextRef := events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "reply-v1:route-carrier"}}
	evt := eventtest.RunCreatingRootIngress(uuid.NewString(), events.EventType("review/task.started"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	routes := []events.DeliveryRoute{
		{Recipient: events.MustNodeDeliveryRecipient("review-node"), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "review", FlowInstance: "review/inst-1", EntityID: eventtest.UUID("entity-1")}),
			Context: contextRef,
		},
		{Recipient: events.MustNodeDeliveryRecipient(workflowRuntimeInternalCarrierID), Target: events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowInstance: "review/inst-1"})},
	}
	if err := eb.deliverToRecipientsWithRoutes(context.Background(), evt, []string{workflowRuntimeInternalCarrierID}, routes); err != nil {
		t.Fatalf("deliverToRecipientsWithRoutes: %v", err)
	}
	got := requireBusEvent(t, ch, "workflow runtime concrete route delivery")
	if got.TargetRoute().Normalized() != routes[0].Target.Route().Normalized() {
		t.Fatalf("workflow runtime target = %#v, want %#v", got.TargetRoute(), routes[0].Target)
	}
	if got.DeliveryContext().ReplyContextID() != contextRef.ReplyContextID() {
		t.Fatalf("workflow runtime delivery context = %#v, want %#v", got.DeliveryContext(), contextRef)
	}
	select {
	case extra := <-ch:
		t.Fatalf("workflow runtime received placeholder delivery %#v", extra.TargetRoute())
	case <-time.After(25 * time.Millisecond):
	}
}

func deliveryRoutesContain(routes []events.DeliveryRoute, want events.DeliveryRoute) bool {
	want = want.Normalized()
	for _, got := range events.NormalizeDeliveryRoutes(routes) {
		if want.ConnectClaim.Empty() {
			got.ConnectClaim = events.ConnectExecutionClaim{}
		}
		if got == want {
			return true
		}
	}
	return false
}

func nodeOnlyDeliveryPlanner(t testing.TB, nodeID string, eventType events.EventType) deliveryPlanner {
	t.Helper()
	event := strings.TrimSpace(string(eventType))
	node := runtimecontracts.SystemNodeContract{
		ID: nodeID, ExecutionType: "system_node", SubscribesTo: []string{event},
		EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{event: {}},
	}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Package: runtimecontracts.ProjectPackageDocument{Name: "root", Version: "1.0.0"},
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name: "root", Version: "1.0.0",
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{nodeID: node.EventHandlers},
		},
		Nodes:  map[string]runtimecontracts.SystemNodeContract{nodeID: node},
		Events: map[string]runtimecontracts.EventCatalogEntry{event: {}},
	})
	handler, err := runtimepipeline.AdmitDeliveryTargetHandler(source, "root", nodeID)
	if err != nil {
		t.Fatalf("admit node-only target handler: %v", err)
	}
	planner := newDeliveryPlanner(
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber {
				return []Subscriber{{
					Recipient: events.MustNodeDeliveryRecipient(nodeID), LocalizedEvent: event,
					handlerFlowID: "root", handlerNodeID: nodeID, targetHandler: handler.ForEvent(eventType),
				}}
			},
			resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate {
				return []deliveryRecipientCandidate{{ID: nodeID, PersistAsDelivery: false}}
			},
			describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient {
				return nil
			},
		},
		deliveryRecipientPolicy{
			semanticSource: source,
			loadActiveAgentDescriptors: func(context.Context) (map[agentidentity.Identity]ActiveAgentDescriptor, bool, error) {
				return map[agentidentity.Identity]ActiveAgentDescriptor{}, true, nil
			},
		},
	)
	planner.rootFlowID = "root"
	return planner
}

func mixedNodeAgentDeliveryPlanner(t testing.TB, nodeID, agentID string, eventType events.EventType) deliveryPlanner {
	t.Helper()
	identity := agentidentitytest.RootRuntime(t, agentID, "eventbus-target-test")
	event := strings.TrimSpace(string(eventType))
	node := runtimecontracts.SystemNodeContract{
		ID: nodeID, ExecutionType: "system_node", SubscribesTo: []string{event},
		EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{event: {}},
	}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Package: runtimecontracts.ProjectPackageDocument{Name: "root", Version: "1.0.0"},
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name: "root", Version: "1.0.0",
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{nodeID: node.EventHandlers},
		},
		Nodes:  map[string]runtimecontracts.SystemNodeContract{nodeID: node},
		Events: map[string]runtimecontracts.EventCatalogEntry{event: {}},
	})
	handler, err := runtimepipeline.AdmitDeliveryTargetHandler(source, "root", nodeID)
	if err != nil {
		t.Fatalf("admit mixed-route target handler: %v", err)
	}
	planner := newDeliveryPlanner(
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber {
				return []Subscriber{{
					Recipient: events.MustNodeDeliveryRecipient(nodeID), LocalizedEvent: event,
					handlerFlowID: "root", handlerNodeID: nodeID, targetHandler: handler.ForEvent(eventType),
				}}
			},
			resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate {
				return []deliveryRecipientCandidate{
					{ID: nodeID, PersistAsDelivery: false},
					{ID: agentID, AgentIdentity: identity, PersistAsDelivery: true},
				}
			},
			describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient {
				return nil
			},
		},
		deliveryRecipientPolicy{
			semanticSource: source,
			loadActiveAgentDescriptors: func(context.Context) (map[agentidentity.Identity]ActiveAgentDescriptor, bool, error) {
				return map[agentidentity.Identity]ActiveAgentDescriptor{
					identity: {Identity: identity},
				}, true, nil
			},
		},
	)
	planner.rootFlowID = "root"
	return planner
}

func nodeOnlyDeliveryPlan(evt events.Event, nodeID string) RoutePlan {
	plan := newRoutePlan(evt)
	targetRoute := events.RouteIdentity{FlowID: "root", FlowInstance: "root"}
	target := events.MustEntitylessReceiverTarget(targetRoute)
	plan.AddLiveRecipients(RoutePlanLiveRecipient{Recipient: events.MustNodeDeliveryRecipient(nodeID), PersistAsDelivery: false,
		Producer: routeIntentProducerInternalTargetCarrier,
	})
	plan.AddDeliveryIntents(RoutePlanDeliveryIntent{Recipient: events.MustNodeDeliveryRecipient(nodeID), Producer: routeIntentProducerInternalTargetCarrier,
		TargetBlueprint: targetRoute, TargetOwnership: target, Persist: true,
	})
	return plan.Normalized()
}

func TestEventBusPublish_NodeOnlyRouteDoesNotRequireAgentChannel(t *testing.T) {
	store := newTargetRouteMemoryStore()
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	evt := eventtest.RunCreatingRootIngress(uuid.NewString(),
		events.EventType("custom.node_only"), "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	eb.deliveryPlanner = nodeOnlyDeliveryPlanner(t, "workflow-node", evt.Type())

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish node-only route without agent channel: %v", err)
	}
	if got := store.receipts[evt.ID()]; got != "processed" {
		t.Fatalf("pipeline receipt = %q err=%#v, want processed", got, store.receiptErrs[evt.ID()])
	}
	if routes := store.routes[evt.ID()]; len(routes) != 1 || !routes[0].Recipient.IsNode() || routes[0].Recipient.ID() != "workflow-node" {
		t.Fatalf("delivery routes = %#v, want node/workflow-node", routes)
	}
}

func TestEventBusCommittedPublishDispatch_NodeOnlyRouteDoesNotRequireAgentChannel(t *testing.T) {
	store := newTargetRouteMemoryStore()
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	evt := eventtest.RunCreatingRootIngress(uuid.NewString(),
		events.EventType("custom.node_only_tx"), "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	claim, err := store.ClaimPublication(context.Background(), evt.ID())
	if err != nil {
		t.Fatalf("claim node-only publication: %v", err)
	}
	publication := &pipelinePublicationClaim{bus: eb, eventID: evt.ID(), claim: claim}
	receiver, err := eb.receiverProjection(context.Background(), evt.DeliveryContext())
	if err != nil {
		t.Fatalf("project committed receiver: %v", err)
	}
	if err := eb.completeCommittedPublishDispatch(evt, nodeOnlyDeliveryPlan(evt, "workflow-node"), publication, receiver); err != nil {
		t.Fatalf("dispatch committed node-only publication: %v", err)
	}

	if got := store.receipts[evt.ID()]; got != "processed" {
		t.Fatalf("pipeline receipt = %q err=%#v, want processed", got, store.receiptErrs[evt.ID()])
	}
}

func TestEngineDispatcher_NodeOnlyRouteDoesNotRequireAgentChannel(t *testing.T) {
	store := newTargetRouteMemoryStore()
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	evt := eventtest.RunCreatingRootIngress(uuid.NewString(),
		events.EventType("custom.node_only_outbox"), "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

	store.events[evt.ID()] = evt
	store.routes[evt.ID()] = []events.DeliveryRoute{{
		Recipient: events.MustNodeDeliveryRecipient("workflow-node"),
		Target:    events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: "root", FlowInstance: "root"}),
	}}
	store.scopes[evt.ID()] = runtimepipelineobligation.ScopeSubscribed

	if err := eb.EngineDispatcher().DispatchPostCommit(context.Background(), []runtimeengine.EmitIntent{{Event: evt}}); err != nil {
		t.Fatalf("DispatchPostCommit node-only route without agent channel: %v", err)
	}
	if got := store.receipts[evt.ID()]; got != "processed" {
		t.Fatalf("pipeline receipt = %q err=%#v, want processed", got, store.receiptErrs[evt.ID()])
	}
}

func TestSweepPipelineObligations_NodeOnlyRouteDoesNotRequireAgentChannel(t *testing.T) {
	store := newTargetRouteMemoryStore()
	eventID := uuid.NewString()
	evt := eventtest.ExistingRunRootIngress(eventID,
		events.EventType("custom.node_only_sweep"), "", "", []byte(`{}`), 0, eventtest.UUID("run:"+eventID), events.EventEnvelope{}, time.Now().UTC())

	store.events[evt.ID()] = evt
	store.routes[evt.ID()] = []events.DeliveryRoute{{
		Recipient: events.MustNodeDeliveryRecipient("workflow-node"),
		Target:    events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: "root", FlowInstance: "root"}),
	}}
	store.scopes[evt.ID()] = runtimepipelineobligation.ScopeSubscribed
	store.missing = []events.PersistedReplayEvent{{Event: evt}}
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	eb.deliveryPlanner = nodeOnlyDeliveryPlanner(t, "workflow-node", evt.Type())

	result, err := eb.SweepPipelineObligations(context.Background(), 10)
	if err != nil {
		t.Fatalf("SweepPipelineObligations node-only route without agent channel: %v", err)
	}
	if result.Settled != 1 {
		t.Fatalf("swept count = %d, want 1", result.Settled)
	}
	if got := store.receipts[evt.ID()]; got != "processed" {
		t.Fatalf("pipeline receipt = %q err=%#v, want processed", got, store.receiptErrs[evt.ID()])
	}
}

func TestEventBusPublish_MixedNodeAgentRouteStillRequiresAgentChannel(t *testing.T) {
	store := newTargetRouteMemoryStore()
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	evt := eventtest.RunCreatingRootIngress(uuid.NewString(),
		events.EventType("custom.mixed_node_agent"), "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	eb.deliveryPlanner = mixedNodeAgentDeliveryPlanner(t, "workflow-node", "agent-missing", evt.Type())

	err = eb.Publish(context.Background(), evt)
	if err == nil {
		t.Fatal("Publish succeeded, want missing agent-channel failure")
	}
	failure, ok := runtimefailures.As(err)
	missing, _ := failure.Failure.Detail.Attributes["missing_recipients"].([]string)
	if !ok || failure.Failure.Detail.Code != "authoritative_delivery_incomplete" || len(missing) != 1 || !strings.Contains(missing[0], "agent-missing") {
		t.Fatalf("Publish failure = %#v, want missing agent only", failure)
	}
}

func TestEventBusPublish_TargetSetInternalDeliveryUsesPerTargetRoutes(t *testing.T) {
	store := newTargetRouteMemoryStore()
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	eb.deliveryPlanner = newDeliveryPlannerWithHandlers(t,
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber {
				return []Subscriber{
					{Recipient: events.MustNodeDeliveryRecipient("child-a-listener"), Path: "child-a/inst-1"},
					{Recipient: events.MustNodeDeliveryRecipient("child-b-listener"), Path: "child-b/inst-1"},
				}
			},
			resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate {
				return []deliveryRecipientCandidate{{ID: "workflow-runtime", PersistAsDelivery: false}}
			},
			describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient {
				return nil
			},
		},
		testSelectedOwnerPolicy(
			ActiveTargetDescriptor{ID: "child-a", FlowInstance: "child-a/inst-1", EntityID: eventtest.UUID("ent-a")},
			ActiveTargetDescriptor{ID: "child-b", FlowInstance: "child-b/inst-1", EntityID: eventtest.UUID("ent-b")},
		),
	)

	ch := subscribeInternalDeliveriesForTest(t, eb, "workflow-runtime", events.EventType("child/output.done"))
	evt := eventtest.RunCreatingRootIngress(
		uuid.NewString(),
		events.EventType("child/output.done"),
		"",
		"",
		[]byte(`{}`),
		0,
		uuid.NewString(),
		"",
		events.EnvelopeForTargetSet(events.EventEnvelope{}, []events.RouteIdentity{
			{FlowID: "child-a", FlowInstance: "child-a/inst-1", EntityID: eventtest.UUID("ent-a")},
			{FlowID: "child-b", FlowInstance: "child-b/inst-1", EntityID: eventtest.UUID("ent-b")},
		}),
		time.Now().UTC(),
	)

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	assertTargetRouteDeliveries(t, ch, eventtest.UUID("ent-a"), eventtest.UUID("ent-b"))

	persisted := store.events[evt.ID()]
	if got := persisted.EntityID(); got != "" {
		t.Fatalf("persisted EntityID() = %q, want empty target_set projection", got)
	}
	if got := persisted.FlowInstance(); got != "" {
		t.Fatalf("persisted FlowInstance() = %q, want empty target_set projection", got)
	}
	if got := store.routes[evt.ID()]; len(got) != 2 {
		t.Fatalf("persisted delivery routes = %#v, want 2 semantic node routes", got)
	}
	wantRoutes := []events.DeliveryRoute{
		{Recipient: events.MustNodeDeliveryRecipient("child-a-listener"), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "child-a", FlowInstance: "child-a/inst-1", EntityID: eventtest.UUID("ent-a")})},
		{Recipient: events.MustNodeDeliveryRecipient("child-b-listener"), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "child-b", FlowInstance: "child-b/inst-1", EntityID: eventtest.UUID("ent-b")})},
	}
	for _, wantRoute := range wantRoutes {
		if !deliveryRoutesContain(store.routes[evt.ID()], wantRoute) {
			t.Fatalf("persisted delivery routes = %#v, missing %#v", store.routes[evt.ID()], wantRoute)
		}
	}

	if _, err := eb.RecoverPersistedPipeline(context.Background(), runtimepipelineobligation.ClaimedWork{
		Event: evt, Scope: runtimepipelineobligation.ScopeSubscribed,
	}, nil); err != nil {
		t.Fatalf("RecoverPersistedPipeline: %v", err)
	}
	assertTargetRouteDeliveries(t, ch, eventtest.UUID("ent-a"), eventtest.UUID("ent-b"))
}

func TestEventBusPublish_TargetSetSameSemanticNodePersistsPerTargetRoutes(t *testing.T) {
	workerOneID := eventtest.UUID("worker/w-001")
	workerTwoID := eventtest.UUID("worker/w-002")
	store := newTargetRouteMemoryStore()
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	eb.deliveryPlanner = newDeliveryPlannerWithHandlers(t,
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber {
				return []Subscriber{
					{Recipient: events.MustNodeDeliveryRecipient("task-handler"), Path: "worker/w-001"},
					{Recipient: events.MustNodeDeliveryRecipient("task-handler"), Path: "worker/w-002"},
				}
			},
			resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate {
				return []deliveryRecipientCandidate{{ID: "workflow-runtime", PersistAsDelivery: false}}
			},
			describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient {
				return nil
			},
		},
		testSelectedOwnerPolicy(
			ActiveTargetDescriptor{ID: "worker-one", FlowInstance: "worker/w-001", EntityID: workerOneID},
			ActiveTargetDescriptor{ID: "worker-two", FlowInstance: "worker/w-002", EntityID: workerTwoID},
		),
	)

	ch := subscribeInternalDeliveriesForTest(t, eb, "workflow-runtime", events.EventType("worker/work.assign"))
	evt := eventtest.RunCreatingRootIngress(
		uuid.NewString(),
		events.EventType("worker/work.assign"),
		"",
		"",
		[]byte(`{}`),
		0,
		"",
		"",
		events.EnvelopeForTargetSet(events.EventEnvelope{}, []events.RouteIdentity{
			{FlowID: "worker", FlowInstance: "worker/w-001", EntityID: workerOneID},
			{FlowID: "worker", FlowInstance: "worker/w-002", EntityID: workerTwoID},
		}),
		time.Now().UTC(),
	)

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	assertTargetRouteDeliveries(t, ch, workerOneID, workerTwoID)
	wantRoutes := []events.DeliveryRoute{
		{Recipient: events.MustNodeDeliveryRecipient("task-handler"), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "worker", FlowInstance: "worker/w-001", EntityID: workerOneID})},
		{Recipient: events.MustNodeDeliveryRecipient("task-handler"), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "worker", FlowInstance: "worker/w-002", EntityID: workerTwoID})},
	}
	if got := len(store.routes[evt.ID()]); got != len(wantRoutes) {
		t.Fatalf("persisted delivery routes = %#v, want %d same-node target routes", store.routes[evt.ID()], len(wantRoutes))
	}
	for _, wantRoute := range wantRoutes {
		if !deliveryRoutesContain(store.routes[evt.ID()], wantRoute) {
			t.Fatalf("persisted delivery routes = %#v, missing %#v", store.routes[evt.ID()], wantRoute)
		}
	}
}

func TestEventBusPublish_TargetedRouteTableNodePersistsSemanticNodeRoute(t *testing.T) {
	workerID := eventtest.UUID("worker/w-001")
	store := newTargetRouteMemoryStore()
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	eb.deliveryPlanner = newDeliveryPlannerWithHandlers(t,
		deliveryRouteResolver{
			resolveRoutedSubscribers: func(events.Event) []Subscriber {
				return []Subscriber{{Recipient: events.MustNodeDeliveryRecipient("task-handler"), Path: "worker/w-001"}}
			},
			resolveSubscribedRecipients: func(string) []deliveryRecipientCandidate { return nil },
			describeSubscribersForEvent: func(string, []Subscriber) []PublishDiagnosticRecipient {
				return []PublishDiagnosticRecipient{{ID: "task-handler", Type: "node", Path: "worker/w-001"}}
			},
		},
		testSelectedOwnerPolicy(ActiveTargetDescriptor{ID: "worker-one", FlowInstance: "worker/w-001", EntityID: workerID}),
	)
	evt := eventtest.RunCreatingRootIngress(
		uuid.NewString(),
		events.EventType("worker/work.assign"),
		"",
		"",
		[]byte(`{}`),
		0,
		"",
		"",
		events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{FlowID: "worker", FlowInstance: "worker/w-001", EntityID: workerID}),
		time.Now().UTC(),
	)

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	wantRoute := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient("task-handler"), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "worker", FlowInstance: "worker/w-001", EntityID: workerID})}
	if got := store.routes[evt.ID()]; len(got) != 1 || !deliveryRoutesContain(got, wantRoute) {
		t.Fatalf("persisted delivery routes = %#v, want semantic node route %#v", got, wantRoute)
	}
}

func TestEventBusPublish_TargetedTemplateInstanceRouteTableNodePersistsSemanticNodeRoute(t *testing.T) {
	store := newTargetRouteMemoryStore()
	store.setTargetOwnerRoutes(events.RouteIdentity{FlowID: "operating", FlowInstance: "operating/inst-1", EntityID: eventtest.UUID("ent-operating")})
	source := semanticview.Wrap(routedNodeTemplateBundle())
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("operating", "inst-1")}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	evt := eventtest.RunCreatingRootIngress(
		uuid.NewString(),
		events.EventType("operating/opco.product_initialization_requested"),
		"",
		"",
		[]byte(`{}`),
		0,
		"",
		"",
		events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{FlowInstance: "operating/inst-1", EntityID: eventtest.UUID("ent-operating")}),
		time.Now().UTC(),
	)

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if len(plan.RoutedRecipients) != 1 || plan.RoutedRecipients[0].ID != "lifecycle-orchestrator" || plan.RoutedRecipients[0].Path != "operating/inst-1" {
		t.Fatalf("routed recipients = %#v, want targeted lifecycle-orchestrator concrete instance route", plan.RoutedRecipients)
	}
	wantRoute := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient("lifecycle-orchestrator"), Target: events.MustExistingEntityTarget(events.RouteIdentity{
		FlowID:       "operating",
		FlowInstance: "operating/inst-1",
		EntityID:     eventtest.UUID("ent-operating"),
	}),
	}
	if len(plan.DeliveryRoutes) != 1 || !deliveryRoutesContain(plan.DeliveryRoutes, wantRoute) {
		t.Fatalf("plan delivery routes = %#v, want semantic node route %#v", plan.DeliveryRoutes, wantRoute)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := store.routes[evt.ID()]; len(got) != 1 || !deliveryRoutesContain(got, wantRoute) {
		t.Fatalf("persisted delivery routes = %#v, want semantic node route %#v", got, wantRoute)
	}
}

func TestEventBusPublish_TargetedDynamicFlowFixtureRouteTableNodePersistsSemanticNodeRoute(t *testing.T) {
	store := newTargetRouteMemoryStore()
	store.setTargetOwnerRoutes(events.RouteIdentity{FlowID: "worker", FlowInstance: "worker/w-001", EntityID: runtimeflowidentity.EntityID(eventtest.UUID("worker/w-001"))})
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-dynamic-flow-instance")
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("load fixture bundle: %v", err)
	}
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: semanticview.Wrap(bundle)})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("worker", "w-001")}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	materialized := eb.RouteTable().MaterializedRoutes(runtimeflowidentity.DeriveRoute("worker", "w-001"))
	hasRoute := func(eventPattern string) bool {
		for _, route := range materialized {
			if route.EventPattern == eventPattern && route.SubscriberID == "task-handler" {
				return true
			}
		}
		return false
	}
	if !hasRoute("worker/w-001/work.assign") {
		t.Fatalf("materialized routes = %#v, want task-handler instance-scoped work.assign route; node entries=%v", materialized, sortedStringKeys(bundle.NodeEntries()))
	}
	if subscriberListContainsRouteSource(eb.RouteTable().Resolve("worker/w-001/work.assign"), "task-handler", "worker/w-001", "receiver_carrier") {
		t.Fatalf("Resolve(worker/w-001/work.assign) = %#v, want no receiver_carrier route for unrelated target-route fixture", eb.RouteTable().Resolve("worker/w-001/work.assign"))
	}
	if resolved := eb.RouteTable().Resolve("worker/w-001/work.assign"); subscriberListContainsRouteSource(resolved, "task-handler", "worker/w-001", "receiver_carrier") {
		t.Fatalf("Resolve(worker/w-001/work.assign) = %#v, want no receiver_carrier route for unrelated target-route fixture", resolved)
	}
	target := events.RouteIdentity{
		FlowID:       "worker",
		FlowInstance: "worker/w-001",
		EntityID:     runtimeflowidentity.EntityID(eventtest.UUID("worker/w-001")),
	}
	evt := eventtest.RunCreatingRootIngress(
		uuid.NewString(),
		events.EventType("worker/work.assign"),
		"",
		"",
		[]byte(`{"task_label":"route-me"}`),
		0,
		"",
		"",
		events.EnvelopeForTargetRoute(events.EventEnvelope{}, target),
		time.Now().UTC(),
	)

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	foundConcreteSubscriber := false
	for _, recipient := range plan.RoutedRecipients {
		if recipient.ID == "task-handler" && recipient.Path == "worker/w-001" {
			foundConcreteSubscriber = true
			break
		}
	}
	if !foundConcreteSubscriber {
		t.Fatalf("routed recipients = %#v, want targeted task-handler concrete worker route", plan.RoutedRecipients)
	}
	wantRoute := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient("task-handler"), Target: events.MustExistingEntityTarget(target)}
	if len(plan.DeliveryRoutes) != 1 || !deliveryRoutesContain(plan.DeliveryRoutes, wantRoute) {
		t.Fatalf("plan delivery routes = %#v, want semantic node route %#v", plan.DeliveryRoutes, wantRoute)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := store.routes[evt.ID()]; len(got) != 1 || !deliveryRoutesContain(got, wantRoute) {
		t.Fatalf("persisted delivery routes = %#v, want semantic node route %#v", got, wantRoute)
	}
}

func TestEventBusPublish_NoTargetConcreteRoutedNodePersistsSemanticNodeRoute(t *testing.T) {
	store := newTargetRouteMemoryStore()
	store.setTargetOwnerRoutes(events.RouteIdentity{FlowID: "operating", FlowInstance: "operating/inst-1", EntityID: eventtest.UUID("ent-operating")})
	source := semanticview.Wrap(routedNodeTemplateBundle())
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("operating", "inst-1")}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	ch := subscribeInternalDeliveriesForTest(t, eb, "workflow-runtime", events.EventType("operating/opco.product_initialization_requested"))
	evt := eventtest.RunCreatingRootIngress(
		uuid.NewString(),
		events.EventType("operating/inst-1/opco.product_initialization_requested"),
		"",
		"",
		[]byte(`{}`),
		0,
		"",
		"",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-operating")), "operating/inst-1"),
		time.Now().UTC(),
	)

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := requireBusEvent(t, ch, "concrete routed node event delivery")
	if got.FlowInstance() != "operating/inst-1" {
		t.Fatalf("delivered flow instance = %q, want operating/inst-1", got.FlowInstance())
	}

	routes := store.routes[evt.ID()]
	if len(routes) != 1 {
		t.Fatalf("persisted delivery routes = %#v, want one lifecycle-orchestrator semantic route", routes)
	}
	route := routes[0]
	if !route.Recipient.IsNode() || route.Recipient.ID() != "lifecycle-orchestrator" {
		t.Fatalf("delivery route = %#v, want node/lifecycle-orchestrator semantic authority", route)
	}
	if target := route.Target.Route(); target.FlowID != "operating" || target.FlowInstance != "operating/inst-1" || target.EntityID != eventtest.UUID("ent-operating") {
		t.Fatalf("delivery target = %#v, want operating/inst-1 ent-operating", route.Target)
	}

	live, internal, replayRoutes, err := eb.replayRecipientsForCommittedEvent(context.Background(), evt, nil, runtimepipelineobligation.ScopeSubscribed)
	if err != nil {
		t.Fatalf("replayRecipientsForCommittedEvent: %v", err)
	}
	if len(live) != 1 || live[0] != "workflow-runtime" {
		t.Fatalf("replay live recipients = %#v, want workflow-runtime carrier", live)
	}
	if len(internal) != 1 || internal[0] != "workflow-runtime" {
		t.Fatalf("replay internal recipients = %#v, want workflow-runtime carrier", internal)
	}
	if !deliveryRoutesContain(replayRoutes, events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient("lifecycle-orchestrator"), Target: events.MustExistingEntityTarget(events.RouteIdentity{
		FlowID:       "operating",
		FlowInstance: "operating/inst-1",
		EntityID:     eventtest.UUID("ent-operating"),
	}),
	}) {
		t.Fatalf("replay routes = %#v, want lifecycle-orchestrator semantic route", replayRoutes)
	}
	if len(replayRoutes) != 1 {
		t.Fatalf("replay routes = %#v, want only durable lifecycle-orchestrator route", replayRoutes)
	}
}

func TestEventBusPublish_SemanticScopeFlowInstanceResolvesConcreteRoute(t *testing.T) {
	store := newTargetRouteMemoryStore()
	store.setTargetOwnerRoutes(events.RouteIdentity{FlowInstance: "operating/inst-1", EntityID: eventtest.UUID("ent-operating")})
	source := semanticview.Wrap(routedNodeTemplateBundle())
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("operating", "inst-1")}); err != nil {
		t.Fatalf("AddFlowInstanceRoute: %v", err)
	}
	ch := subscribeInternalDeliveriesForTest(t, eb, "workflow-runtime", events.EventType("operating/opco.product_initialization_requested"))
	evt := eventtest.RunCreatingRootIngress(
		uuid.NewString(),
		events.EventType("operating/opco.product_initialization_requested"),
		"",
		"",
		[]byte(`{}`),
		0,
		"",
		"",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-operating")), "operating/inst-1"),
		time.Now().UTC(),
	)

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if len(plan.RoutedRecipients) != 1 || plan.RoutedRecipients[0].Path != "operating/inst-1" {
		t.Fatalf("routed recipients = %#v, want concrete operating/inst-1 route", plan.RoutedRecipients)
	}
	if len(plan.DeliveryRoutes) != 1 || plan.DeliveryRoutes[0].Target.Route().FlowInstance != "operating/inst-1" {
		t.Fatalf("delivery routes = %#v, want one concrete operating route", plan.DeliveryRoutes)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := requireBusEvent(t, ch, "semantic-scope routed node event delivery")
	if got.FlowInstance() != "operating/inst-1" {
		t.Fatalf("delivered flow instance = %q, want operating/inst-1", got.FlowInstance())
	}
	routes := store.routes[evt.ID()]
	if len(routes) != 1 || routes[0].Target.Route().FlowInstance != "operating/inst-1" {
		t.Fatalf("persisted delivery routes = %#v, want concrete operating route", routes)
	}
}

func TestEventBusPublish_RuntimeCallbackLocalEventPersistsSameFlowNodeRouteBeforeInternalCarrier(t *testing.T) {
	tests := []struct {
		name      string
		eventType string
	}{
		{name: "success callback", eventType: "repo_scaffold.repo_commit_succeeded"},
		{name: "failure callback", eventType: "repo_scaffold.repo_commit_failed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := newTargetRouteMemoryStore()
			store.setTargetOwnerRoutes(events.RouteIdentity{FlowID: "repo-scaffold", FlowInstance: "repo-scaffold/inst-1", EntityID: eventtest.UUID("ent-repo")})
			eventID := uuid.NewString()
			want := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient("repo-scaffold-node"), Target: events.MustExistingEntityTarget(events.RouteIdentity{
				FlowID:       "repo-scaffold",
				FlowInstance: "repo-scaffold/inst-1",
				EntityID:     eventtest.UUID("ent-repo"),
			}),
			}
			eb, err := newScopedTestEventBus(store, EventBusOptions{
				ContractBundle: semanticview.Wrap(routedCallbackTemplateBundle()),
				Interceptors: []EventInterceptor{materializedRoutePersistedBeforeInterceptor{
					t:       t,
					store:   store,
					eventID: eventID,
					want:    want,
				}},
			})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			if err := eb.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("repo-scaffold", "inst-1")}); err != nil {
				t.Fatalf("AddFlowInstanceRoute: %v", err)
			}
			concreteEventType := "repo-scaffold/inst-1/" + tc.eventType
			ch := subscribeInternalDeliveriesForTest(t, eb, "workflow-runtime", events.EventType(concreteEventType))
			defer unsubscribeTestAgent(eb, "workflow-runtime")
			evt := eventtest.RunCreatingRootIngress(
				eventID,
				events.EventType(tc.eventType),
				"workflow-runtime",
				"",
				[]byte(`{}`),
				0,
				"",
				"",
				events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-repo")), "repo-scaffold/inst-1"),
				time.Now().UTC(),
			)

			plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
			if err != nil {
				t.Fatalf("CheckPublishRecipientPlan: %v", err)
			}
			if got := plan.Recipients; len(got) != 1 || got[0] != "workflow-runtime" {
				t.Fatalf("recipients = %#v, want workflow-runtime live carrier", got)
			}
			if len(plan.PersistedRecipients) != 0 {
				t.Fatalf("persisted recipients = %#v, want none for internal carrier", plan.PersistedRecipients)
			}
			if len(plan.RoutedRecipients) != 1 || plan.RoutedRecipients[0].ID != "repo-scaffold-node" || plan.RoutedRecipients[0].Path != "repo-scaffold/inst-1" {
				t.Fatalf("routed recipients = %#v, want repo-scaffold-node concrete instance route", plan.RoutedRecipients)
			}
			if got := plan.DeliveryRoutes; len(got) != 1 || !deliveryRoutesContain(got, want) {
				t.Fatalf("delivery routes = %#v, want runtime callback node route %#v", got, want)
			}

			if err := eb.Publish(context.Background(), evt); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			got := requireBusEvent(t, ch, "runtime callback workflow-runtime carrier delivery")
			if got.Type() != events.EventType(tc.eventType) || got.FlowInstance() != "repo-scaffold/inst-1" || got.EntityID() != eventtest.UUID("ent-repo") {
				t.Fatalf("delivered event type=%q flow=%q entity=%q, want callback local event in repo-scaffold/inst-1 ent-repo", got.Type(), got.FlowInstance(), got.EntityID())
			}
			routes := store.routes[evt.ID()]
			if len(routes) != 1 || !deliveryRoutesContain(routes, want) {
				t.Fatalf("persisted delivery routes = %#v, want callback route %#v", routes, want)
			}
		})
	}
}

func TestEventBusCheckPublishRecipientPlan_SemanticScopeFlowInstanceMaterializesNodeRoute(t *testing.T) {
	store := newTargetRouteMemoryStore()
	store.setTargetOwnerRoutes(events.RouteIdentity{FlowInstance: "validation/inst-1", EntityID: eventtest.UUID("ent-validation")})
	eb, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle: semanticview.Wrap(routedNodeStaticValidationBundle()),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ch := subscribeInternalDeliveriesForTest(t, eb, "workflow-runtime", events.EventType("validation/thing.reviewed"))
	evt := eventtest.RunCreatingRootIngress(
		uuid.NewString(),
		events.EventType("validation/thing.reviewed"),
		"",
		"",
		[]byte(`{}`),
		0,
		"",
		"",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-validation")), "validation/inst-1"),
		time.Now().UTC(),
	)

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if len(plan.PersistedRecipients) != 0 {
		t.Fatalf("persisted recipients = %#v, want none for internal workflow node carrier", plan.PersistedRecipients)
	}
	if got := plan.DeliveryRoutes; len(got) != 1 {
		t.Fatalf("delivery routes = %#v, want one concrete node route", got)
	}
	route := plan.DeliveryRoutes[0]
	if !route.Recipient.IsNode() || route.Recipient.ID() != "entity-writer" {
		t.Fatalf("delivery route = %#v, want node/entity-writer", route)
	}
	if target := route.Target.Route(); target.FlowInstance != "validation/inst-1" || target.EntityID != eventtest.UUID("ent-validation") {
		t.Fatalf("delivery route target = %#v, want validation/inst-1 ent-validation", route.Target)
	}
	if len(plan.RoutedRecipients) != 1 || plan.RoutedRecipients[0].ID != "entity-writer" || plan.RoutedRecipients[0].Path != "validation" {
		t.Fatalf("routed recipients = %#v, want entity-writer at semantic validation scope", plan.RoutedRecipients)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := requireBusEvent(t, ch, "semantic-scope concrete route event delivery")
	if got.FlowInstance() != "validation/inst-1" || got.EntityID() != eventtest.UUID("ent-validation") {
		t.Fatalf("delivered route identity flow=%q entity=%q, want validation/inst-1 ent-validation", got.FlowInstance(), got.EntityID())
	}
	routes := store.routes[evt.ID()]
	if len(routes) != 1 || routes[0].Recipient.ID() != "entity-writer" || routes[0].Target.Route().FlowInstance != "validation/inst-1" {
		t.Fatalf("persisted routes = %#v, want entity-writer concrete validation route", routes)
	}
}

func TestEventBusCheckPublishRecipientPlan_SemanticScopeFlowInstanceMaterializesSystemNodeRouteWithoutLiveSubscription(t *testing.T) {
	store := newTargetRouteMemoryStore()
	store.setTargetOwnerRoutes(events.RouteIdentity{FlowInstance: "validation/inst-1", EntityID: eventtest.UUID("ent-validation")})
	eb, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle: semanticview.Wrap(routedNodeStaticValidationBundle()),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	evt := eventtest.RunCreatingRootIngress(
		uuid.NewString(),
		events.EventType("validation/thing.reviewed"),
		"",
		"",
		[]byte(`{}`),
		0,
		"",
		"",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-validation")), "validation/inst-1"),
		time.Now().UTC(),
	)

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if len(plan.Recipients) != 0 || len(plan.PersistedRecipients) != 0 {
		t.Fatalf("recipients=%#v persisted=%#v, want no live subscriber recipients", plan.Recipients, plan.PersistedRecipients)
	}
	if len(plan.RoutedRecipients) != 1 || plan.RoutedRecipients[0].ID != "entity-writer" || plan.RoutedRecipients[0].Path != "validation" || plan.RoutedRecipients[0].LocalizedEvent != "thing.reviewed" {
		t.Fatalf("routed recipients = %#v, want entity-writer local thing.reviewed at semantic validation scope", plan.RoutedRecipients)
	}
	if got := plan.DeliveryRoutes; len(got) != 1 {
		t.Fatalf("delivery routes = %#v, want one route-table system-node route", got)
	}
	route := plan.DeliveryRoutes[0]
	if !route.Recipient.IsNode() || route.Recipient.ID() != "entity-writer" {
		t.Fatalf("delivery route = %#v, want node/entity-writer", route)
	}
	if target := route.Target.Route(); target.FlowInstance != "validation/inst-1" || target.EntityID != eventtest.UUID("ent-validation") {
		t.Fatalf("delivery route target = %#v, want validation/inst-1 ent-validation", route.Target)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	routes := store.routes[evt.ID()]
	if len(routes) != 1 || routes[0].Recipient.ID() != "entity-writer" || routes[0].Target.Route().FlowInstance != "validation/inst-1" {
		t.Fatalf("persisted routes = %#v, want entity-writer concrete validation route", routes)
	}
}

func TestEventBusPublish_NoTargetScopedRoutedNodeWithoutFlowInstanceFailsBeforePersistence(t *testing.T) {
	store := newTargetRouteMemoryStore()
	store.setTargetOwnerRoutes(events.RouteIdentity{FlowID: "child", FlowInstance: "child", EntityID: eventtest.UUID("ent-child")})
	eb, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle: semanticview.Wrap(routedNodeStaticChildBundle()),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	live := subscribeInternalDeliveriesForTest(t, eb, "child-intake", events.EventType("child/child.start"))
	evt := eventtest.RunCreatingRootIngress(
		uuid.NewString(),
		events.EventType("child/child.start"),
		"",
		"",
		[]byte(`{}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-child")),
		time.Now().UTC(),
	)

	if _, err := eb.CheckPublishRecipientPlan(context.Background(), evt); err == nil || !strings.Contains(err.Error(), "without exact same-instance, explicit-target, or compiled-connect authority") {
		t.Fatalf("CheckPublishRecipientPlan error = %v, want missing target-owner authority", err)
	}
	if err := eb.Publish(context.Background(), evt); err == nil || !strings.Contains(err.Error(), "without exact same-instance, explicit-target, or compiled-connect authority") {
		t.Fatalf("Publish error = %v, want missing target-owner authority", err)
	}
	if _, ok := store.events[evt.ID()]; ok || len(store.routes[evt.ID()]) != 0 {
		t.Fatalf("store mutated for rejected event: event=%v routes=%#v", ok, store.routes[evt.ID()])
	}
	if _, ok := store.settlements[evt.ID()]; ok {
		t.Fatalf("settlement persisted for rejected event %q", evt.ID())
	}
	if _, ok := store.scopes[evt.ID()]; ok || store.receipts[evt.ID()] != "" {
		t.Fatalf("replay scope or receipt persisted for rejected event %q", evt.ID())
	}
	select {
	case delivery := <-live:
		t.Fatalf("rejected event reached live carrier: %#v", delivery)
	default:
	}
}

func TestEventBusPublish_MixedExactAndWildcardCrossFlowRoutesFailBeforePersistence(t *testing.T) {
	const (
		pattern   = "component-scaffold/*/opco.repo_scaffold_requested"
		eventType = "component-scaffold/component-a/opco.repo_scaffold_requested"
	)
	componentFlow := runtimecontracts.FlowContractView{
		Path: "component-scaffold", Paths: runtimecontracts.FlowContractPaths{ID: "component-scaffold", Flow: "component-scaffold"},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"component-node": {ID: "component-node", EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"opco.repo_scaffold_requested": existingOwnerHandlerFixture()}},
		},
	}
	repositoryFlow := runtimecontracts.FlowContractView{
		Path: "repo-scaffold", Paths: runtimecontracts.FlowContractPaths{ID: "repo-scaffold", Flow: "repo-scaffold"},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"repo-scaffold-node": {ID: "repo-scaffold-node", EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"opco.repo_scaffold_requested": existingOwnerHandlerFixture()}},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{componentFlow, repositoryFlow}}
	bundle := &runtimecontracts.WorkflowContractBundle{FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
		Root: &root,
		ByID: map[string]*runtimecontracts.FlowContractView{
			"component-scaffold": &root.Children[0],
			"repo-scaffold":      &root.Children[1],
		},
	}}
	source := semanticview.Wrap(bundle)
	componentHandler, err := runtimepipeline.AdmitDeliveryTargetHandler(source, "component-scaffold", "component-node")
	if err != nil {
		t.Fatalf("admit component target handler: %v", err)
	}
	repositoryHandler, err := runtimepipeline.AdmitDeliveryTargetHandler(source, "repo-scaffold", "repo-scaffold-node")
	if err != nil {
		t.Fatalf("admit repository target handler: %v", err)
	}
	rt := newRouteTable(source)
	rt.eventPath[eventType] = struct{}{}
	rt.patterns = []routePattern{
		{
			EventPattern: eventType,
			Subscriber: Subscriber{Recipient: events.MustNodeDeliveryRecipient("component-node"), Path: "component-scaffold/component-a",
				routeSource: subscriberRouteSourceSubscription, LocalizedEvent: "opco.repo_scaffold_requested",
				targetHandler: componentHandler,
			},
		},
		{
			EventPattern: pattern,
			Subscriber: Subscriber{Recipient: events.MustNodeDeliveryRecipient("repo-scaffold-node"), Path: "repo-scaffold",
				routeSource: subscriberRouteSourceSubscription, LocalizedEvent: "opco.repo_scaffold_requested",
				targetHandler: repositoryHandler,
			},
		},
	}
	rt.rebuildLocked()

	store := newTargetRouteMemoryStore()
	store.setTargetOwnerRoutes(
		events.RouteIdentity{FlowID: "repo-scaffold", FlowInstance: "repo-scaffold", EntityID: eventtest.UUID("ent-component")},
		events.RouteIdentity{FlowID: "component-scaffold", FlowInstance: "component-scaffold/component-a", EntityID: eventtest.UUID("ent-component")},
	)
	eventID := uuid.NewString()
	eb, err := newScopedTestEventBus(store, EventBusOptions{
		RouteTable: rt, ContractBundle: source,
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	evt := eventtest.RunCreatingRootIngress(
		eventID,
		events.EventType(eventType),
		"",
		"",
		[]byte(`{}`),
		0,
		"",
		"",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-component")), "component-scaffold/component-a"),
		time.Now().UTC(),
	)

	if _, err := eb.CheckPublishRecipientPlan(context.Background(), evt); err == nil || !strings.Contains(err.Error(), `routed node "repo-scaffold-node"`) {
		t.Fatalf("CheckPublishRecipientPlan error = %v, want wildcard cross-flow rejection", err)
	}
	if err := eb.Publish(context.Background(), evt); err == nil || !strings.Contains(err.Error(), `routed node "repo-scaffold-node"`) {
		t.Fatalf("Publish error = %v, want wildcard cross-flow rejection", err)
	}
	if _, ok := store.events[eventID]; ok || len(store.routes[eventID]) != 0 {
		t.Fatalf("store partially mutated for mixed valid/invalid routes: event=%v routes=%#v", ok, store.routes[eventID])
	}
	if _, ok := store.settlements[eventID]; ok {
		t.Fatalf("settlement persisted for rejected event %q", eventID)
	}
}

func TestEventBusPublish_DescendantWithoutConnectFailsBeforePersistence(t *testing.T) {
	grandchild := runtimecontracts.FlowContractView{
		Path: "child/grandchild", Paths: runtimecontracts.FlowContractPaths{ID: "grandchild", Flow: "grandchild"},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"grandchild-worker": {
				ID: "grandchild-worker", EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"micro.start": existingOwnerHandlerFixture(),
				},
			},
		},
	}
	child := runtimecontracts.FlowContractView{
		Path: "child", Paths: runtimecontracts.FlowContractPaths{ID: "child", Flow: "child"},
		Children: []runtimecontracts.FlowContractView{grandchild},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{child}}
	bundle := &runtimecontracts.WorkflowContractBundle{FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
		Root: &root,
		ByID: map[string]*runtimecontracts.FlowContractView{
			"child": &root.Children[0], "grandchild": &root.Children[0].Children[0],
		},
	}}
	source := semanticview.Wrap(bundle)
	handler, err := runtimepipeline.AdmitDeliveryTargetHandler(source, "grandchild", "grandchild-worker")
	if err != nil {
		t.Fatalf("admit grandchild target handler: %v", err)
	}
	routes := newRouteTable(source)
	routes.eventPath["child/grandchild/micro.start"] = struct{}{}
	routes.routes["child/grandchild/micro.start"] = []Subscriber{{
		Recipient: events.MustNodeDeliveryRecipient("grandchild-worker"), Path: "child/grandchild",
		MatchPattern: "child/grandchild/micro.start", routeSource: subscriberRouteSourceSubscription,
		LocalizedEvent: "micro.start", targetHandler: handler,
	}}
	store := newTargetRouteMemoryStore()
	eb, err := newScopedTestEventBus(store, EventBusOptions{RouteTable: routes, ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	currentRoute := events.RouteIdentity{
		FlowID: "child", FlowInstance: "child/instance-a", EntityID: eventtest.UUID("descendant-current-owner"),
	}.Normalized()
	ctx := runtimedelivery.WithRoute(context.Background(), events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient("child-worker"),
		Target:    events.MustExistingEntityTarget(currentRoute),
	})
	routingSource, err := events.NewConcreteTemplateInstanceRoutingSource(currentRoute)
	if err != nil {
		t.Fatalf("concrete child routing source: %v", err)
	}
	evt := eventtest.RunCreatingRootIngressWithRoutingSource(
		uuid.NewString(), events.EventType("child/grandchild/micro.start"), "child-worker", "", []byte(`{}`), 0, "", "",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, currentRoute.EntityID), currentRoute.FlowInstance),
		routingSource, time.Now().UTC(),
	)

	if _, err := eb.CheckPublishRecipientPlan(ctx, evt); err == nil || !strings.Contains(err.Error(), `routed node "grandchild-worker"`) {
		t.Fatalf("CheckPublishRecipientPlan error = %v, want descendant-without-connect rejection", err)
	}
	if err := eb.Publish(ctx, evt); err == nil || !strings.Contains(err.Error(), `routed node "grandchild-worker"`) {
		t.Fatalf("Publish error = %v, want descendant-without-connect rejection", err)
	}
	if len(store.events) != 0 || len(store.routes) != 0 || len(store.settlements) != 0 || len(store.scopes) != 0 || len(store.receipts) != 0 || len(store.flowRoutes) != 0 {
		t.Fatalf("rejected descendant publication mutated store: events=%#v routes=%#v settlements=%#v scopes=%#v receipts=%#v flow_routes=%#v",
			store.events, store.routes, store.settlements, store.scopes, store.receipts, store.flowRoutes)
	}
}

func TestRouteTableRootInputFlowNodeResolvesRootInputRoute(t *testing.T) {
	rt, err := DeriveRouteTable(semanticview.Wrap(routedRootInputFlowNodeBundle()))
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	got := rt.Resolve("thing.created")
	if len(got) != 1 {
		t.Fatalf("Resolve(thing.created) = %#v, want one root-input flow node route", got)
	}
	if got[0].Recipient.ID() != "entity-writer" || !got[0].Recipient.IsNode() || got[0].Path != "validation" {
		t.Fatalf("resolved subscriber = %#v, want validation/entity-writer node", got[0])
	}
	if got[0].MatchPattern != "thing.created" || got[0].RouteSourceCode() != "root_input_flow" {
		t.Fatalf("resolved subscriber metadata = %#v, want root_input_flow thing.created", got[0])
	}
}

func TestRouteTableQualifiedRootInputFlowUsesSemanticLocalizationOwner(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "internal", "releasee2e", "testdata", "claude_cli_managed_lifecycle")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot, fixtureRoot, runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	rt, err := DeriveRouteTable(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	got := rt.Resolve("worker/task.assigned")
	if len(got) != 1 {
		t.Fatalf("Resolve(worker/task.assigned) = %#v, want one exact worker input route", got)
	}
	if got[0].Recipient.ID() != "intake" || got[0].Path != "worker" || got[0].MatchPattern != "worker/task.assigned" {
		t.Fatalf("resolved subscriber = %#v, want worker/intake exact root input", got[0])
	}
	if got[0].RouteSourceCode() != "root_input_flow" || got[0].LocalizedEvent != "task.assigned" {
		t.Fatalf("resolved semantic authority = %#v, want root_input_flow localized to task.assigned", got[0])
	}
}

func TestEventBusPublish_RootInputFlowNodePersistsRouteBeforeDispatch(t *testing.T) {
	store := newTargetRouteMemoryStore()
	validationOwner := events.RouteIdentity{FlowID: "validation", FlowInstance: "validation", EntityID: eventtest.UUID("ent-validation-owner")}
	store.setTargetOwnerRoutes(validationOwner)
	eventID := uuid.NewString()
	want := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient("entity-writer"), Target: events.MustExistingEntityTarget(validationOwner)}
	eb, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle: semanticview.Wrap(routedRootInputFlowNodeBundle()),
		Interceptors: []EventInterceptor{materializedRoutePersistedBeforeInterceptor{
			t:       t,
			store:   store,
			eventID: eventID,
			want:    want,
		}},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ch := subscribeInternalDeliveriesForTest(t, eb, "workflow-runtime", events.EventType("thing.created"))
	evt := eventtest.RunCreatingRootIngress(
		eventID,
		events.EventType("thing.created"),
		"",
		"",
		[]byte(`{}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-root-input")),
		time.Now().UTC(),
	)

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if len(plan.PersistedRecipients) != 0 {
		t.Fatalf("persisted recipients = %#v, want none for internal root-input node carrier", plan.PersistedRecipients)
	}
	if got := plan.DeliveryRoutes; len(got) != 1 || !deliveryRoutesContain(got, want) {
		t.Fatalf("delivery routes = %#v, want exact validation-owner route", got)
	}
	if len(plan.RoutedRecipients) != 1 || plan.RoutedRecipients[0].ID != "entity-writer" || plan.RoutedRecipients[0].Path != "validation" || plan.RoutedRecipients[0].RouteSource != "root_input_flow" {
		t.Fatalf("routed recipients = %#v, want root-input validation/entity-writer", plan.RoutedRecipients)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := requireBusEvent(t, ch, "root-input flow-node carrier delivery")
	if got.FlowInstance() != "validation" || got.EntityID() != validationOwner.EntityID {
		t.Fatalf("delivered root input identity flow=%q entity=%q, want validation receiver owner", got.FlowInstance(), got.EntityID())
	}
	routes := store.routes[evt.ID()]
	if !deliveryRoutesContain(routes, want) {
		t.Fatalf("persisted delivery routes = %#v, want %#v", routes, want)
	}
	if got := store.scopes[evt.ID()]; got != runtimepipelineobligation.ScopeSubscribed {
		t.Fatalf("committed replay scope = %q, want subscribed", got)
	}
}

func TestEventBusPublish_RootInputFlowNodePersistsRouteBeforeInterceptorWithoutInternalCarrier(t *testing.T) {
	store := newTargetRouteMemoryStore()
	validationOwner := events.RouteIdentity{FlowID: "validation", FlowInstance: "validation", EntityID: eventtest.UUID("ent-validation-owner")}
	store.setTargetOwnerRoutes(validationOwner)
	eventID := uuid.NewString()
	want := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient("entity-writer"), Target: events.MustExistingEntityTarget(validationOwner)}
	eb, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle: semanticview.Wrap(routedRootInputFlowNodeBundle()),
		Interceptors: []EventInterceptor{materializedRoutePersistedBeforeInterceptor{
			t:       t,
			store:   store,
			eventID: eventID,
			want:    want,
		}},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	evt := eventtest.RunCreatingRootIngress(
		eventID,
		events.EventType("thing.created"),
		"",
		"",
		[]byte(`{}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-root-input")),
		time.Now().UTC(),
	)

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if len(plan.Recipients) != 0 || len(plan.PersistedRecipients) != 0 {
		t.Fatalf("recipients=%#v persisted=%#v, want no live carrier recipients", plan.Recipients, plan.PersistedRecipients)
	}
	if len(plan.RoutedRecipients) != 1 || plan.RoutedRecipients[0].ID != "entity-writer" || plan.RoutedRecipients[0].Path != "validation" || plan.RoutedRecipients[0].RouteSource != "root_input_flow" {
		t.Fatalf("routed recipients = %#v, want root-input validation/entity-writer", plan.RoutedRecipients)
	}
	if got := plan.DeliveryRoutes; len(got) != 1 || !deliveryRoutesContain(got, want) {
		t.Fatalf("delivery routes = %#v, want exact validation-owner route", got)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if routes := store.routes[evt.ID()]; !deliveryRoutesContain(routes, want) {
		t.Fatalf("persisted delivery routes = %#v, want %#v", routes, want)
	}
	if got := store.scopes[evt.ID()]; got != runtimepipelineobligation.ScopeSubscribed {
		t.Fatalf("committed replay scope = %q, want subscribed", got)
	}
}

func TestEventBusPublish_RootInputFlowRejectsInternalSameNameBeforePersistence(t *testing.T) {
	store := newTargetRouteMemoryStore()
	store.setTargetOwnerRoutes(events.RouteIdentity{
		FlowID: "validation", FlowInstance: "validation", EntityID: eventtest.UUID("internal-root-input-owner"),
	})
	eb, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle: semanticview.Wrap(routedRootInputFlowNodeBundle()),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	runID := uuid.NewString()
	sourceRoute := events.RouteIdentity{
		FlowID: "foreign", FlowInstance: "foreign/one", EntityID: eventtest.UUID("internal-root-input-source"),
	}.Normalized()
	evt := eventtest.PersistedChildForProducer(
		uuid.NewString(), events.EventType("thing.created"),
		eventtest.Producer(events.EventProducerNode, "foreign-node"), "", []byte(`{}`), 0,
		runID, uuid.NewString(), events.EnvelopeForSourceRoute(events.EventEnvelope{}, sourceRoute), time.Now().UTC(),
	)

	if _, err := eb.CheckPublishRecipientPlan(context.Background(), evt); err == nil || !strings.Contains(err.Error(), "root-input") {
		t.Fatalf("CheckPublishRecipientPlan error = %v, want root-input admission refusal", err)
	}
	if err := eb.Publish(context.Background(), evt); err == nil || !strings.Contains(err.Error(), "root-input") {
		t.Fatalf("Publish error = %v, want root-input admission refusal", err)
	}
	if len(store.events) != 0 || len(store.routes) != 0 || len(store.settlements) != 0 || len(store.scopes) != 0 ||
		len(store.receipts) != 0 || len(store.flowRoutes) != 0 {
		t.Fatalf("rejected internal root-input publication mutated store: events=%#v routes=%#v settlements=%#v scopes=%#v receipts=%#v flow_routes=%#v",
			store.events, store.routes, store.settlements, store.scopes, store.receipts, store.flowRoutes)
	}
}

func TestEventBusPublish_RootInputFlowRejectsExplicitlyTargetedInternalSameNameBeforePersistence(t *testing.T) {
	store := newTargetRouteMemoryStore()
	target := events.RouteIdentity{
		FlowID: "validation", FlowInstance: "validation", EntityID: eventtest.UUID("targeted-root-input-owner"),
	}.Normalized()
	store.setTargetOwnerRoutes(target)
	eb, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle: semanticview.Wrap(routedRootInputFlowNodeBundle()),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	runID := uuid.NewString()
	sourceRoute := events.RouteIdentity{
		FlowID: "foreign", FlowInstance: "foreign/one", EntityID: eventtest.UUID("targeted-root-input-source"),
	}.Normalized()
	envelope := events.EnvelopeForSourceRoute(events.EventEnvelope{}, sourceRoute)
	envelope = events.EnvelopeForTargetRoute(envelope, target)
	evt := eventtest.PersistedChildForProducer(
		uuid.NewString(), events.EventType("thing.created"),
		eventtest.Producer(events.EventProducerNode, "foreign-node"), "", []byte(`{}`), 0,
		runID, uuid.NewString(), envelope, time.Now().UTC(),
	)

	if _, err := eb.CheckPublishRecipientPlan(context.Background(), evt); err == nil || !strings.Contains(err.Error(), "root-input") {
		t.Fatalf("CheckPublishRecipientPlan error = %v, want explicit-target root-input admission refusal", err)
	}
	if err := eb.Publish(context.Background(), evt); err == nil || !strings.Contains(err.Error(), "root-input") {
		t.Fatalf("Publish error = %v, want explicit-target root-input admission refusal", err)
	}
	if len(store.events) != 0 || len(store.routes) != 0 || len(store.settlements) != 0 || len(store.scopes) != 0 ||
		len(store.receipts) != 0 || len(store.flowRoutes) != 0 {
		t.Fatalf("rejected targeted root-input publication mutated store: events=%#v routes=%#v settlements=%#v scopes=%#v receipts=%#v flow_routes=%#v",
			store.events, store.routes, store.settlements, store.scopes, store.receipts, store.flowRoutes)
	}
}

func TestEventBusPublish_ExplicitTargetSelectsOrdinaryFlowWithoutAuthorizingSameIDRootInputSibling(t *testing.T) {
	source, routes := duplicateIDScopedRootInputAuthorityFixture(t)
	ordinaryTarget := events.RouteIdentity{
		FlowID: "ordinary", FlowInstance: "ordinary", EntityID: eventtest.UUID("duplicate-id-ordinary-owner"),
	}.Normalized()
	rootInputTarget := events.RouteIdentity{
		FlowID: "validation", FlowInstance: "validation", EntityID: eventtest.UUID("duplicate-id-root-input-owner"),
	}.Normalized()
	store := newTargetRouteMemoryStore()
	store.setTargetOwnerRoutes(ordinaryTarget, rootInputTarget)
	eventBus, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source, RouteTable: routes})
	if err != nil {
		t.Fatalf("create duplicate-ID EventBus: %v", err)
	}
	sourceRoute := events.RouteIdentity{
		FlowID: "foreign", FlowInstance: "foreign/one", EntityID: eventtest.UUID("duplicate-id-ordinary-source"),
	}.Normalized()
	envelope := events.EnvelopeForSourceRoute(events.EventEnvelope{}, sourceRoute)
	envelope = events.EnvelopeForTargetRoute(envelope, ordinaryTarget)
	eventID := uuid.NewString()
	event := eventtest.PersistedChildForProducer(
		eventID, events.EventType("thing.created"), eventtest.Producer(events.EventProducerNode, "foreign-node"), "",
		[]byte(`{"scope":"ordinary"}`), 0, uuid.NewString(), uuid.NewString(), envelope, time.Now().UTC(),
	)

	plan, err := eventBus.CheckPublishRecipientPlan(context.Background(), event)
	if err != nil {
		t.Fatalf("preflight exact ordinary target with duplicate-ID root-input sibling: %v", err)
	}
	want := events.MustExistingEntityTarget(ordinaryTarget)
	if len(plan.DeliveryRoutes) != 1 || plan.DeliveryRoutes[0].Recipient.ID() != "shared-writer" || plan.DeliveryRoutes[0].Target != want {
		t.Fatalf("ordinary scoped routes = %#v, want only shared-writer at %#v", plan.DeliveryRoutes, want)
	}
	if err := eventBus.Publish(context.Background(), event); err != nil {
		t.Fatalf("publish exact ordinary target with duplicate-ID root-input sibling: %v", err)
	}
	if persisted := store.routes[eventID]; len(persisted) != 1 || persisted[0].Target != want {
		t.Fatalf("persisted ordinary scoped routes = %#v, want only %#v", persisted, want)
	}
}

func TestEventBusPublish_ExplicitTargetedRootInputSameIDSiblingRejectsBeforeMutation(t *testing.T) {
	source, routes := duplicateIDScopedRootInputAuthorityFixture(t)
	ordinaryTarget := events.RouteIdentity{
		FlowID: "ordinary", FlowInstance: "ordinary", EntityID: eventtest.UUID("duplicate-id-hostile-ordinary-owner"),
	}.Normalized()
	rootInputTarget := events.RouteIdentity{
		FlowID: "validation", FlowInstance: "validation", EntityID: eventtest.UUID("duplicate-id-hostile-root-input-owner"),
	}.Normalized()
	store := newTargetRouteMemoryStore()
	store.setTargetOwnerRoutes(ordinaryTarget, rootInputTarget)
	eventBus, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source, RouteTable: routes})
	if err != nil {
		t.Fatalf("create hostile duplicate-ID EventBus: %v", err)
	}
	sourceRoute := events.RouteIdentity{
		FlowID: "foreign", FlowInstance: "foreign/one", EntityID: eventtest.UUID("duplicate-id-hostile-source"),
	}.Normalized()
	envelope := events.EnvelopeForSourceRoute(events.EventEnvelope{}, sourceRoute)
	envelope = events.EnvelopeForTargetRoute(envelope, rootInputTarget)
	event := eventtest.PersistedChildForProducer(
		uuid.NewString(), events.EventType("thing.created"), eventtest.Producer(events.EventProducerNode, "foreign-node"), "",
		[]byte(`{"scope":"root-input"}`), 0, uuid.NewString(), uuid.NewString(), envelope, time.Now().UTC(),
	)

	if _, err := eventBus.CheckPublishRecipientPlan(context.Background(), event); err == nil || !strings.Contains(err.Error(), "root-input") {
		t.Fatalf("duplicate-ID root-input preflight error = %v, want typed admission refusal", err)
	}
	if err := eventBus.Publish(context.Background(), event); err == nil || !strings.Contains(err.Error(), "root-input") {
		t.Fatalf("duplicate-ID root-input publish error = %v, want typed admission refusal", err)
	}
	if len(store.events) != 0 || len(store.routes) != 0 || len(store.settlements) != 0 || len(store.scopes) != 0 ||
		len(store.receipts) != 0 || len(store.flowRoutes) != 0 {
		t.Fatalf("rejected duplicate-ID root-input publication mutated store: events=%#v routes=%#v settlements=%#v scopes=%#v receipts=%#v flow_routes=%#v",
			store.events, store.routes, store.settlements, store.scopes, store.receipts, store.flowRoutes)
	}
}

func TestEventBusRootInputAPIExplicitTargetPersistsOnlySelectedSameIDScopedRoute(t *testing.T) {
	bundle := routedRootInputFlowNodeBundle()
	addRoutedRootInputFlowNodeSibling(bundle)
	source := semanticview.Wrap(bundle)
	endpoint, err := NewRootInputAPIEventPublicationEndpoint(source, "thing.created")
	if err != nil {
		t.Fatalf("construct duplicate-ID root-input API endpoint: %v", err)
	}
	selected := events.RouteIdentity{
		FlowID: "validation", FlowInstance: "validation", EntityID: eventtest.UUID("duplicate-id-api-selected"),
	}.Normalized()
	unselected := events.RouteIdentity{
		FlowID: "audit", FlowInstance: "audit", EntityID: eventtest.UUID("duplicate-id-api-unselected"),
	}.Normalized()
	lifecycleStore := &connectRoutePlanLifecycleStore{connectRoutePlanDescriptorStore: &connectRoutePlanDescriptorStore{
		targetRouteMemoryStore: newTargetRouteMemoryStore(),
	}}
	store := &apiEventPublicationMemoryStore{connectRoutePlanLifecycleStore: lifecycleStore}
	store.setTargetOwnerRoutes(selected, unselected)
	eventBus, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("create duplicate-ID root-input API EventBus: %v", err)
	}
	lifecycleStore.bus = eventBus
	eventID := uuid.NewString()
	event := eventtest.OperatorInjected(
		eventID, events.EventType("thing.created"), "operator", "", []byte(`{"proof":"duplicate-id-api"}`), 0,
		uuid.NewString(), nil, events.EnvelopeForTargetRoute(events.EventEnvelope{}, selected), time.Now().UTC(),
	)

	plan, err := eventBus.CheckAPIEventPublishRecipientPlan(context.Background(), event, &endpoint)
	if err != nil {
		t.Fatalf("preflight duplicate-ID root-input API target: %v", err)
	}
	want := events.MustExistingEntityTarget(selected)
	if len(plan.DeliveryRoutes) != 1 || plan.DeliveryRoutes[0].Recipient.ID() != "entity-writer" || plan.DeliveryRoutes[0].Target != want {
		t.Fatalf("duplicate-ID API routes = %#v, want only selected scoped route %#v", plan.DeliveryRoutes, want)
	}
	completion, replay, err := eventBus.PublishAPIEventAcknowledged(
		testAuthorActivityContext(context.Background()), event, &endpoint,
		apiidempotency.Request{Method: "event.publish", ActorTokenID: "operator", IdempotencyKey: "duplicate-id-api", RequestHash: "duplicate-id-api-request"},
		apiidempotency.Completion{ResourceID: eventID, Response: json.RawMessage(`{"event_id":"` + eventID + `"}`)},
	)
	if err != nil {
		t.Fatalf("publish duplicate-ID root-input API target: %v", err)
	}
	if replay || completion.ResourceID != eventID {
		t.Fatalf("duplicate-ID API completion = %#v replay=%t, want fresh %s", completion, replay, eventID)
	}
	if persisted := store.routes[eventID]; len(persisted) != 1 || persisted[0].Target != want {
		t.Fatalf("persisted duplicate-ID API routes = %#v, want only %#v", persisted, want)
	}
}

func TestRoutedSubscriberAuthorityDoesNotTransferAcrossDuplicateNodeIDScopes(t *testing.T) {
	_, routes := duplicateIDScopedRootInputAuthorityFixture(t)
	ordinary := routes.routes["thing.created"][0]
	rootInput := routes.rootInputRoutes["thing.created"][0]
	event := eventtest.OperatorInjected(
		uuid.NewString(), events.EventType("thing.created"), "operator", "", nil, 0,
		uuid.NewString(), nil, events.EventEnvelope{}, time.Now().UTC(),
	)
	intent := RoutePlanDeliveryIntent{
		Recipient: ordinary.Recipient,
		TargetBlueprint: events.RouteIdentity{
			FlowID: "ordinary", FlowInstance: "ordinary",
		},
		Handler:  routedSubscriberTargetHandler(ordinary, event.Type()),
		Producer: routeIntentProducerInternalTargetRoute,
		Persist:  true,
	}

	ordinaryKey := newRoutedSubscriberAuthorityKey(event, ordinary)
	if !routePlanIntentAuthorizesRoutedSubscriber(intent, ordinaryKey, ordinary) {
		t.Fatal("planned ordinary-flow intent did not authorize its exact subscriber scope")
	}
	rootInputKey := newRoutedSubscriberAuthorityKey(event, rootInput)
	if routePlanIntentAuthorizesRoutedSubscriber(intent, rootInputKey, rootInput) {
		t.Fatal("planned ordinary-flow intent transferred authority to duplicate-ID root-input sibling")
	}
}

func TestEventBusRootInputFlowAcceptsExactOperatorAPIAdmission(t *testing.T) {
	for _, explicitTarget := range []bool{false, true} {
		name := "target_free"
		if explicitTarget {
			name = "explicit_exact_target"
		}
		t.Run(name, func(t *testing.T) {
			bundle := routedRootInputFlowNodeBundle()
			if explicitTarget {
				addRoutedRootInputFlowNodeSibling(bundle)
			}
			source := semanticview.Wrap(bundle)
			endpoint, err := NewRootInputAPIEventPublicationEndpoint(source, "thing.created")
			if err != nil {
				t.Fatalf("construct exact operator API endpoint: %v", err)
			}
			wantRoute := events.RouteIdentity{
				FlowID: "validation", FlowInstance: "validation", EntityID: eventtest.UUID("operator-root-input-owner-" + name),
			}.Normalized()
			unselectedRoute := events.RouteIdentity{
				FlowID: "audit", FlowInstance: "audit", EntityID: eventtest.UUID("operator-root-input-unselected-" + name),
			}.Normalized()
			lifecycleStore := &connectRoutePlanLifecycleStore{connectRoutePlanDescriptorStore: &connectRoutePlanDescriptorStore{
				targetRouteMemoryStore: newTargetRouteMemoryStore(),
			}}
			store := &apiEventPublicationMemoryStore{connectRoutePlanLifecycleStore: lifecycleStore}
			store.setTargetOwnerRoutes(wantRoute, unselectedRoute)
			eventBus, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
			if err != nil {
				t.Fatalf("create EventBus: %v", err)
			}
			lifecycleStore.bus = eventBus
			envelope := events.EventEnvelope{}
			if explicitTarget {
				envelope = events.EnvelopeForTargetRoute(envelope, wantRoute)
			}
			eventID := uuid.NewString()
			event := eventtest.OperatorInjected(
				eventID, events.EventType("thing.created"), "operator", "", []byte(`{"proof":"typed-api-endpoint"}`), 0,
				uuid.NewString(), nil, envelope, time.Now().UTC(),
			)

			plan, err := eventBus.CheckAPIEventPublishRecipientPlan(context.Background(), event, &endpoint)
			if err != nil {
				t.Fatalf("preflight exact operator API endpoint: %v", err)
			}
			want := events.MustExistingEntityTarget(wantRoute)
			if len(plan.DeliveryRoutes) != 1 || plan.DeliveryRoutes[0].Recipient.ID() != "entity-writer" || plan.DeliveryRoutes[0].Target != want {
				t.Fatalf("operator API delivery routes = %#v, want exact validation receiver %#v", plan.DeliveryRoutes, want)
			}
			if _, err := eventBus.CheckPublishRecipientPlan(context.Background(), event); err == nil || !strings.Contains(err.Error(), "root-input") {
				t.Fatalf("unadmitted operator preflight error = %v, want root-input admission refusal", err)
			}
			completion, replay, err := eventBus.PublishAPIEventAcknowledged(
				testAuthorActivityContext(context.Background()), event, &endpoint,
				apiidempotency.Request{
					Method: "event.publish", ActorTokenID: "operator", IdempotencyKey: "root-input-" + name, RequestHash: "root-input-request-" + name,
				},
				apiidempotency.Completion{ResourceID: eventID, Response: json.RawMessage(`{"event_id":"` + eventID + `"}`)},
			)
			if err != nil {
				t.Fatalf("publish exact operator API endpoint: %v", err)
			}
			if replay || completion.ResourceID != eventID {
				t.Fatalf("operator API completion = %#v replay=%t, want fresh %s", completion, replay, eventID)
			}
			if routes := store.routes[eventID]; len(routes) != 1 || routes[0].Recipient.ID() != "entity-writer" || routes[0].Target != want {
				t.Fatalf("persisted operator API routes = %#v, want only exact selected validation receiver %#v", routes, want)
			}
			if got := store.events[eventID].TargetRoute().Normalized(); explicitTarget && got != wantRoute {
				t.Fatalf("persisted explicitly targeted operator API event target = %#v, want %#v", got, wantRoute)
			} else if !explicitTarget && !got.Empty() {
				t.Fatalf("persisted target-free operator API event target = %#v, want immutable target-free payload", got)
			}
		})
	}
}

func addRoutedRootInputFlowNodeSibling(bundle *runtimecontracts.WorkflowContractBundle) {
	audit := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "audit", Flow: "audit"},
		Path:  "audit",
		Schema: runtimecontracts.FlowSchemaDocument{
			Mode: "static",
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{"thing.created"}},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{"thing.created": {}},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"entity-writer": {
				ID:            "entity-writer",
				ExecutionType: "system_node",
				SubscribesTo:  []string{"thing.created"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"thing.created": existingOwnerHandlerFixture(),
				},
			},
		},
	}
	validation := bundle.FlowTree.Root.Children[0]
	bundle.FlowTree.Root.Children = []runtimecontracts.FlowContractView{validation, audit}
	bundle.FlowTree.ByID = map[string]*runtimecontracts.FlowContractView{
		"validation": &bundle.FlowTree.Root.Children[0],
		"audit":      &bundle.FlowTree.Root.Children[1],
	}
	bundle.Semantics.FlowInputs["audit"] = []string{"thing.created"}
	bundle.FlowSchemas["audit"] = audit.Schema
}

func duplicateIDScopedRootInputAuthorityFixture(t testing.TB) (semanticview.Source, *RouteTable) {
	t.Helper()
	flow := func(id string) runtimecontracts.FlowContractView {
		return runtimecontracts.FlowContractView{
			Path: id, Paths: runtimecontracts.FlowContractPaths{ID: id, Flow: id},
			Schema: runtimecontracts.FlowSchemaDocument{
				Mode: "static",
				Pins: runtimecontracts.FlowPins{Inputs: runtimecontracts.FlowInputPins{Events: []string{"thing.created"}}},
			},
			Events: map[string]runtimecontracts.EventCatalogEntry{"thing.created": {}},
			Nodes: map[string]runtimecontracts.SystemNodeContract{
				"shared-writer": {
					ID: "shared-writer", ExecutionType: "system_node", SubscribesTo: []string{"thing.created"},
					EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"thing.created": existingOwnerHandlerFixture()},
				},
			},
		}
	}
	ordinary := flow("ordinary")
	validation := flow("validation")
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{ordinary, validation}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{Inputs: runtimecontracts.FlowInputPins{Events: []string{"thing.created"}}},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{
			FlowInputs: map[string][]string{"validation": {"thing.created"}},
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"ordinary": &root.Children[0], "validation": &root.Children[1],
			},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"ordinary": ordinary.Schema, "validation": validation.Schema,
		},
	}
	source := semanticview.Wrap(bundle)
	ordinaryHandler, err := runtimepipeline.AdmitDeliveryTargetHandler(source, "ordinary", "shared-writer")
	if err != nil {
		t.Fatalf("admit ordinary duplicate-ID handler: %v", err)
	}
	rootInputHandler, err := runtimepipeline.AdmitDeliveryTargetHandler(source, "validation", "shared-writer")
	if err != nil {
		t.Fatalf("admit root-input duplicate-ID handler: %v", err)
	}
	routes := newRouteTable(source)
	routes.routes = map[string][]Subscriber{
		"thing.created": {{
			Recipient: events.MustNodeDeliveryRecipient("shared-writer"), Path: "ordinary", MatchPattern: "thing.created",
			routeSource: subscriberRouteSourceSubscription, LocalizedEvent: "thing.created", handlerFlowID: "ordinary",
			handlerNodeID: "shared-writer", targetHandler: ordinaryHandler,
		}},
	}
	routes.rootInputRoutes = map[string][]Subscriber{
		"thing.created": {{
			Recipient: events.MustNodeDeliveryRecipient("shared-writer"), Path: "validation", MatchPattern: "thing.created",
			routeSource: subscriberRouteSourceRootInputFlow, LocalizedEvent: "thing.created", handlerFlowID: "validation",
			handlerNodeID: "shared-writer", targetHandler: rootInputHandler,
		}},
	}
	routes.patterns = nil
	routes.eventPath = map[string]struct{}{"thing.created": {}}
	return source, routes
}

func TestEventBusPublish_LoadedRootInputProjectEventPersistsRouteBeforeDispatch(t *testing.T) {
	store := newTargetRouteMemoryStore()
	eventID := uuid.NewString()
	runID := uuid.NewString()
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		canonicalrouting.RepoRoot(t),
		canonicalrouting.ExampleRoot(t, canonicalrouting.RootIngress),
		runtimecontracts.DefaultPlatformSpecFile(canonicalrouting.RepoRoot(t)),
	)
	if err != nil {
		t.Fatalf("load canonical root ingress: %v", err)
	}
	source := semanticview.Wrap(bundle)
	rootOwner := events.RouteIdentity{FlowID: source.WorkflowName(), FlowInstance: runID, EntityID: eventtest.UUID("loaded-root-owner")}
	store.setTargetOwnerRoutes(rootOwner)
	want := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient("item-handler"), Target: events.MustExistingEntityTarget(rootOwner)}
	rt, err := DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	resolved := rt.Resolve("item.received")
	if len(resolved) != 1 || resolved[0].Recipient.ID() != "item-handler" || resolved[0].Path != "" || resolved[0].RouteSourceCode() != "subscription" {
		t.Fatalf("resolved subscribers = %#v, want canonical same-flow item-handler subscription", resolved)
	}

	eb, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle: source,
		Interceptors: []EventInterceptor{materializedRoutePersistedBeforeInterceptor{
			t:       t,
			store:   store,
			eventID: eventID,
			want:    want,
		}},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ch := subscribeInternalDeliveriesForTest(t, eb, "workflow-runtime", events.EventType("item.received"))
	evt := eventtest.RunCreatingRootIngress(
		eventID,
		events.EventType("item.received"),
		"",
		"",
		[]byte(`{"item_id":"item-1"}`),
		0,
		runID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("ent-loaded-root-source")),
		time.Now().UTC(),
	)

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if len(plan.PersistedRecipients) != 0 {
		t.Fatalf("persisted recipients = %#v, want none for internal root-input node carrier", plan.PersistedRecipients)
	}
	if len(plan.RoutedRecipients) != 1 || plan.RoutedRecipients[0].ID != "item-handler" || plan.RoutedRecipients[0].Path != "" || plan.RoutedRecipients[0].RouteSource != "subscription" {
		t.Fatalf("routed recipients = %#v, want canonical same-flow item-handler", plan.RoutedRecipients)
	}
	if got := plan.DeliveryRoutes; len(got) != 1 || !deliveryRoutesContain(got, want) {
		t.Fatalf("delivery routes = %#v, want empty-target node/item-handler route", got)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := requireBusEvent(t, ch, "loaded root-input flow-node carrier delivery")
	if got.FlowInstance() != runID || got.EntityID() != rootOwner.EntityID {
		t.Fatalf("delivered root input identity flow=%q entity=%q, want exact root receiver owner", got.FlowInstance(), got.EntityID())
	}
	if routes := store.routes[evt.ID()]; !deliveryRoutesContain(routes, want) {
		t.Fatalf("persisted delivery routes = %#v, want %#v", routes, want)
	}
}

func TestRouteTableIntrinsicExternalFlowInputsCarryTypedIngressAuthority(t *testing.T) {
	for _, tc := range []struct {
		name          string
		source        semanticview.Source
		eventType     string
		recipient     string
		wantPath      string
		wantRouteCode string
	}{
		{
			name:      "singleton flow",
			source:    notifyallchildren.LoadSource(t, notifyallchildren.Options{}),
			eventType: "portfolio/portfolio.opened", recipient: "portfolio-coordinator",
			wantPath: "portfolio", wantRouteCode: "root_input_flow",
		},
		{
			name:      "ordinary static flow",
			source:    intrinsicExternalStaticFlowSource(t),
			eventType: "producer/validation.triggered", recipient: "producer-node",
			wantPath: "producer", wantRouteCode: "root_input_flow",
		},
		{
			name:      "non external subscription",
			source:    staticAPIEventPublicationSource(),
			eventType: "child/work.requested", recipient: "child-worker",
			wantPath: "child", wantRouteCode: "subscription",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			routes, err := DeriveRouteTable(tc.source)
			if err != nil {
				t.Fatalf("DeriveRouteTable: %v", err)
			}
			resolved := routes.Resolve(tc.eventType)
			if len(resolved) != 1 {
				t.Fatalf("Resolve(%s) = %#v, want one exact receiver", tc.eventType, resolved)
			}
			got := resolved[0]
			if got.Recipient.ID() != tc.recipient || got.Path != tc.wantPath || got.RouteSourceCode() != tc.wantRouteCode {
				t.Fatalf("Resolve(%s) = %#v, want recipient=%s path=%s authority=%s", tc.eventType, got, tc.recipient, tc.wantPath, tc.wantRouteCode)
			}
		})
	}
}

func intrinsicExternalStaticFlowSource(t testing.TB) semanticview.Source {
	t.Helper()
	repoRoot := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		filepath.Join(repoRoot, "examples", "routing", "template-create-minted-key"),
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load intrinsic external static-flow source: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func TestEventBusPublish_NodeProducedSameFlowOutputPersistsNodeDelivery(t *testing.T) {
	store := newTargetRouteMemoryStore()
	repoRoot := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		filepath.Join(repoRoot, "tests/tier8-boot-verification/test-boot-event-cycle"),
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load cycle fixture: %v", err)
	}
	source := semanticview.Wrap(bundle)
	runID := uuid.NewString()
	rootOwner := events.RouteIdentity{FlowID: source.WorkflowName(), FlowInstance: runID, EntityID: eventtest.UUID("cycle-root-owner")}
	store.setTargetOwnerRoutes(rootOwner)
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	eventID := uuid.NewString()
	evt := eventtest.PersistedChildForProducer(
		eventID,
		events.EventType("cycle.pong"),
		eventtest.Producer(events.EventProducerNode, "test-node"),
		"cycle-task",
		[]byte(`{}`),
		1,
		runID,
		uuid.NewString(),
		events.EnvelopeForEntityID(events.EventEnvelope{}, uuid.NewString()),
		time.Now().UTC(),
	)
	want := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient("test-node"), Target: events.MustExistingEntityTarget(rootOwner)}

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if !deliveryRoutesContain(plan.DeliveryRoutes, want) {
		t.Fatalf("delivery routes = %#v, want typed same-flow node route %#v", plan.DeliveryRoutes, want)
	}
	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if !deliveryRoutesContain(store.routes[eventID], want) {
		t.Fatalf("persisted delivery routes = %#v, want typed same-flow node route %#v", store.routes[eventID], want)
	}
}

func TestEventBusPublish_CanonicalParentConnectPersistsSingularStaticRoute(t *testing.T) {
	store := newTargetRouteMemoryStore()
	interceptor := &connectRoutePlanNodeInterceptor{}
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		canonicalrouting.RepoRoot(t),
		canonicalrouting.ExampleRoot(t, canonicalrouting.ParentConnect),
		runtimecontracts.DefaultPlatformSpecFile(canonicalrouting.RepoRoot(t)),
	)
	if err != nil {
		t.Fatalf("load canonical parent connect: %v", err)
	}
	eb, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle: semanticview.Wrap(bundle), Interceptors: []EventInterceptor{interceptor},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	sourceFact, err := events.NewStaticFlowRoutingSource(events.RouteIdentity{
		FlowID:       "producer",
		FlowInstance: "producer",
		EntityID:     eventtest.UUID("parent-connect-producer"),
	})
	if err != nil {
		t.Fatalf("build parent-connect routing source: %v", err)
	}
	evt := eventtest.RunCreatingRootIngressWithRoutingSource(
		uuid.NewString(), events.EventType("producer/work.ready"), "", "",
		[]byte(`{"work_id":"work-1"}`), 0, "", "", events.EventEnvelope{}, sourceFact, time.Now().UTC(),
	)
	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if plan.TargetFailure != "" || len(plan.DeliveryRoutes) != 1 {
		t.Fatalf("parent-connect plan failure/routes = %q/%#v", plan.TargetFailure, plan.DeliveryRoutes)
	}
	want := plan.DeliveryRoutes[0]
	target := want.Target.Route()
	if !want.Recipient.IsNode() || want.Recipient.ID() != "consumer-node" ||
		target.FlowID != "consumer" || target.FlowInstance != "consumer" || target.EntityID != "" || !want.Target.EntitylessReceiver() {
		t.Fatalf("parent-connect route = %#v, want singular entityless static consumer identity", want)
	}
	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if !deliveryRoutesContain(store.routes[evt.ID()], want) {
		t.Fatalf("persisted parent-connect routes = %#v, want %#v", store.routes[evt.ID()], want)
	}
	if got := interceptor.Count(); got != 1 {
		t.Fatalf("entityless receiver executions = %d, want 1", got)
	}
}

func TestEventBusPublish_NoTargetRootRoutedNodeUsesSemanticNodeDeliveryRoute(t *testing.T) {
	store := newTargetRouteMemoryStore()
	source := semanticview.Wrap(loadTargetRouteTempBundle(t, routedRootNodeFixtureFiles()))
	runID := uuid.NewString()
	rootOwner := events.RouteIdentity{FlowID: source.WorkflowName(), FlowInstance: runID, EntityID: eventtest.UUID("root-node-owner")}
	store.setTargetOwnerRoutes(rootOwner)
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ch := subscribeInternalDeliveriesForTest(t, eb, "portfolio-node", events.EventType("opco.spinup_requested"))
	evt := eventtest.RunCreatingRootIngress(
		uuid.NewString(),
		events.EventType("opco.spinup_requested"),
		"",
		"",
		[]byte(`{}`),
		0,
		runID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("root-node-source")),
		time.Now().UTC(),
	)

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := requireBusEvent(t, ch, "root routed node event delivery")
	if got.FlowInstance() != runID || got.EntityID() != rootOwner.EntityID {
		t.Fatalf("delivered target = %q/%q, want exact root owner", got.FlowInstance(), got.EntityID())
	}

	routes := store.routes[evt.ID()]
	if len(routes) != 1 {
		t.Fatalf("persisted delivery routes = %#v, want one semantic root node route", routes)
	}
	route := routes[0]
	if !route.Recipient.IsNode() || route.Recipient.ID() != "portfolio-node" {
		t.Fatalf("delivery route = %#v, want node/portfolio-node", route)
	}
	if route.Target.Route() != rootOwner {
		t.Fatalf("delivery target = %#v, want %#v", route.Target, rootOwner)
	}

	live, internal, replayRoutes, err := eb.replayRecipientsForCommittedEvent(context.Background(), evt, nil, runtimepipelineobligation.ScopeSubscribed)
	if err != nil {
		t.Fatalf("replayRecipientsForCommittedEvent: %v", err)
	}
	if len(live) != 1 || live[0] != "portfolio-node" {
		t.Fatalf("replay live recipients = %#v, want portfolio-node", live)
	}
	if len(internal) != 1 || internal[0] != "portfolio-node" {
		t.Fatalf("replay internal recipients = %#v, want portfolio-node", internal)
	}
	if len(replayRoutes) != 1 || replayRoutes[0].Recipient.ID() != "portfolio-node" || replayRoutes[0].Target.Route() != rootOwner {
		t.Fatalf("replay routes = %#v, want exact root node route", replayRoutes)
	}

	if _, err := eb.RecoverPersistedPipeline(context.Background(), runtimepipelineobligation.ClaimedWork{
		Event: evt, Scope: runtimepipelineobligation.ScopeSubscribed,
	}, nil); err != nil {
		t.Fatalf("RecoverPersistedPipeline: %v", err)
	}
	got = requireBusEvent(t, ch, "root routed node replay delivery")
	if got.FlowInstance() != runID || got.EntityID() != rootOwner.EntityID {
		t.Fatalf("replayed target = %q/%q, want exact root owner", got.FlowInstance(), got.EntityID())
	}
}

func TestEventBusPublish_NoTargetRootRoutedNodePersistsSemanticRouteWithoutInternalSubscription(t *testing.T) {
	store := newTargetRouteMemoryStore()
	source := semanticview.Wrap(loadTargetRouteTempBundle(t, routedRootNodeFixtureFiles()))
	runID := uuid.NewString()
	rootOwner := events.RouteIdentity{FlowID: source.WorkflowName(), FlowInstance: runID, EntityID: eventtest.UUID("root-node-owner")}
	store.setTargetOwnerRoutes(rootOwner)
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	evt := eventtest.RunCreatingRootIngress(
		uuid.NewString(),
		events.EventType("opco.spinup_requested"),
		"",
		"",
		[]byte(`{}`),
		0,
		runID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("root-node-source")),
		time.Now().UTC(),
	)

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	routes := store.routes[evt.ID()]
	if len(routes) != 1 {
		t.Fatalf("persisted delivery routes = %#v, want one semantic root node route without an internal subscription", routes)
	}
	route := routes[0]
	if !route.Recipient.IsNode() || route.Recipient.ID() != "portfolio-node" {
		t.Fatalf("delivery route = %#v, want node/portfolio-node", route)
	}
	if route.Target.Route() != rootOwner {
		t.Fatalf("delivery target = %#v, want %#v", route.Target, rootOwner)
	}
	if got := store.scopes[evt.ID()]; got != runtimepipelineobligation.ScopeSubscribed {
		t.Fatalf("committed replay scope = %q, want subscribed", got)
	}
	live, internal, replayRoutes, err := eb.replayRecipientsForCommittedEvent(context.Background(), evt, nil, runtimepipelineobligation.ScopeSubscribed)
	if err != nil {
		t.Fatalf("replayRecipientsForCommittedEvent: %v", err)
	}
	if len(live) != 1 || live[0] != "portfolio-node" {
		t.Fatalf("replay live recipients = %#v, want persisted portfolio-node recipient", live)
	}
	if len(internal) != 1 || internal[0] != "portfolio-node" {
		t.Fatalf("replay internal recipients = %#v, want persisted portfolio-node recipient", internal)
	}
	if len(replayRoutes) != 1 || !replayRoutes[0].Recipient.IsNode() || replayRoutes[0].Recipient.ID() != "portfolio-node" {
		t.Fatalf("replay routes = %#v, want retained semantic node/portfolio-node evidence", replayRoutes)
	}
	if _, err := eb.RecoverPersistedPipeline(context.Background(), runtimepipelineobligation.ClaimedWork{
		Event: evt, Scope: runtimepipelineobligation.ScopeSubscribed,
	}, nil); err != nil {
		t.Fatalf("RecoverPersistedPipeline without internal carrier: %v", err)
	}
}

func TestRouteTableTopLevelProjectNodeResolvesProgrammaticRootInputRoute(t *testing.T) {
	rt, err := DeriveRouteTable(semanticview.Wrap(routedTopLevelProjectNodeBundle()))
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	got := rt.Resolve("thing.created")
	if len(got) != 1 {
		t.Fatalf("Resolve(thing.created) = %#v, want one top-level project node route", got)
	}
	if got[0].Recipient.ID() != "reviewer" || !got[0].Recipient.IsNode() || got[0].Path != "" || got[0].MatchPattern != "thing.created" || got[0].RouteSourceCode() != "root_input_project" {
		t.Fatalf("resolved subscriber = %#v, want root input project reviewer route", got[0])
	}
}

func TestEventBusPublish_TopLevelProjectNodePersistsRouteBeforeInterceptor(t *testing.T) {
	store := newTargetRouteMemoryStore()
	eventID := uuid.NewString()
	runID := uuid.NewString()
	source := semanticview.Wrap(routedTopLevelProjectNodeBundle())
	reviewerOwner := events.RouteIdentity{FlowID: source.WorkflowName(), FlowInstance: runID, EntityID: eventtest.UUID("reviewer-root-owner")}
	store.setTargetOwnerRoutes(reviewerOwner)
	reviewerRoute := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient("reviewer"), Target: events.MustExistingEntityTarget(reviewerOwner)}
	workflowRuntimeIdentity := testAgentRouteIdentity(t, "workflow-runtime", "")
	workflowRuntimeRoute := events.DeliveryRoute{
		Recipient: events.MustAgentDeliveryRecipient("workflow-runtime"), AgentIdentity: workflowRuntimeIdentity,
	}
	eb, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle: source,
		Interceptors: []EventInterceptor{materializedRoutePersistedBeforeInterceptor{
			t:       t,
			store:   store,
			eventID: eventID,
			want:    reviewerRoute,
		}},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ch := subscribeTestAgentAdmissionWithIdentity(
		t,
		eb,
		testAgentSubscriptionAdmission(t, "workflow-runtime", events.EventType("thing.created")),
		workflowRuntimeIdentity,
		"",
	)
	evt := eventtest.RunCreatingRootIngress(
		eventID,
		events.EventType("thing.created"),
		"",
		"",
		[]byte(`{}`),
		0,
		runID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, eventtest.UUID("project-source-owner")),
		time.Now().UTC(),
	)

	plan, err := eb.CheckPublishRecipientPlan(context.Background(), evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if got := plan.PersistedRecipients; len(got) != 1 || got[0] != "workflow-runtime" {
		t.Fatalf("persisted recipients = %#v, want workflow-runtime carrier", got)
	}
	if !deliveryRoutesContain(plan.DeliveryRoutes, reviewerRoute) || !deliveryRoutesContain(plan.DeliveryRoutes, workflowRuntimeRoute) {
		t.Fatalf("delivery routes = %#v, want workflow-runtime agent and reviewer node routes", plan.DeliveryRoutes)
	}

	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := requireBusEvent(t, ch, "workflow-runtime carrier delivery")
	if got.EntityID() != eventtest.UUID("project-source-owner") {
		t.Fatalf("delivered entity_id = %q, want unchanged source entity", got.EntityID())
	}
	if routes := store.routes[eventID]; !deliveryRoutesContain(routes, reviewerRoute) || !deliveryRoutesContain(routes, workflowRuntimeRoute) {
		t.Fatalf("persisted delivery routes = %#v, want workflow-runtime agent and reviewer node routes", routes)
	}
}

func TestEventBusPublish_NodeRouteFailsClosedWithoutRouteSetPersistence(t *testing.T) {
	eb, err := newScopedTestEventBus(rejectingDeliveryRouteStore{}, EventBusOptions{
		ContractBundle: semanticview.Wrap(routedTopLevelProjectNodeBundle()),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.Publish(context.Background(), eventtest.RunCreatingRootIngress(uuid.NewString(),
		events.EventType("thing.created"), "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())); err == nil || !strings.Contains(err.Error(), "typed delivery route persistence") {
		t.Fatalf("Publish error = %v, want typed delivery route persistence failure", err)
	}
}

type rejectingDeliveryRouteStore struct{}

func (rejectingDeliveryRouteStore) CommitPublication(_ context.Context, command PublicationCommand) (CommittedPublication, error) {
	if len(command.Commit.DeliveryRoutes) > 0 {
		return CommittedPublication{}, errors.New("typed delivery route persistence is unavailable")
	}
	return CommittedPublication{AppendOutcome: EventAppendInserted}, nil
}

func (rejectingDeliveryRouteStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return nil, ErrAuthoritativeRecipientManifestUnavailable
}

func assertTargetRouteDeliveries(t *testing.T, ch <-chan *LocalDelivery, wantEntityIDs ...string) {
	t.Helper()
	seen := map[string]struct{}{}
	for range wantEntityIDs {
		got := requireBusEvent(t, ch, "target route delivery")
		if len(got.TargetRoutes()) != 0 {
			t.Fatalf("delivered event target_set = %#v, want singular delivery target", got.TargetRoutes())
		}
		target := got.TargetRoute()
		if target.Empty() {
			t.Fatalf("delivered target route is empty: %#v", got)
		}
		seen[target.EntityID] = struct{}{}
	}
	for _, want := range wantEntityIDs {
		if _, ok := seen[want]; !ok {
			t.Fatalf("missing target delivery for %q; saw %#v", want, seen)
		}
	}
}

func loadTargetRouteTempBundle(t *testing.T, files map[string]string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := t.TempDir()
	for rel, body := range files {
		path := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", path, err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, platformSpec)
	if err != nil {
		t.Fatalf("load target route temp bundle: %v", err)
	}
	return bundle
}

func routedRootNodeFixtureFiles() map[string]string {
	return map[string]string{
		"package.yaml": `name: test
version: 1.0.0
`,
		"events.yaml": `opco.spinup_requested:
  entity_id: string
`,
		"nodes.yaml": `portfolio-node:
  id: portfolio-node
  execution_type: system_node
  subscribes_to: [opco.spinup_requested]
  event_handlers:
    opco.spinup_requested:
      guard:
        id: selected_owner
        check: '_entity.id != ""'
`,
	}
}

func routedRootInputFlowNodeBundle() *runtimecontracts.WorkflowContractBundle {
	validation := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "validation", Flow: "validation"},
		Path:  "validation",
		Schema: runtimecontracts.FlowSchemaDocument{
			Mode: "static",
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{"thing.created"}},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"thing.created": {},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"entity-writer": {
				ID:            "entity-writer",
				ExecutionType: "system_node",
				SubscribesTo:  []string{"thing.created"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"thing.created": existingOwnerHandlerFixture(),
				},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{validation}}
	return &runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{"thing.created"}},
			},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{
			FlowInputs: map[string][]string{
				"validation": []string{"thing.created"},
			},
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"validation": &root.Children[0],
			},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"validation": validation.Schema,
		},
	}
}

func routedTopLevelProjectNodeBundle() *runtimecontracts.WorkflowContractBundle {
	handler := runtimecontracts.SystemNodeEventHandler{AdvancesTo: "done"}
	return &runtimecontracts.WorkflowContractBundle{
		Package: runtimecontracts.ProjectPackageDocument{
			Name:    "top-level-project-node",
			Version: "1.0.0",
		},
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{"thing.created"}},
			},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name:    "top-level-project-node",
			Version: "1.0.0",
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				"reviewer": {"thing.created": handler},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"thing.created": {},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"reviewer": {
				ID:            "reviewer",
				ExecutionType: "system_node",
				SubscribesTo:  []string{"thing.created"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"thing.created": handler,
				},
			},
		},
	}
}

func routedNodeTemplateBundle() *runtimecontracts.WorkflowContractBundle {
	operating := runtimecontracts.FlowContractView{
		Path:  "operating",
		Paths: runtimecontracts.FlowContractPaths{ID: "operating", Flow: "operating"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Mode: "template",
			AutoEmitOnCreate: runtimecontracts.AutoEmitOnCreateContract{
				Event: "opco.product_initialization_requested",
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"opco.product_initialization_requested": {},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"lifecycle-orchestrator": {
				ID:            "lifecycle-orchestrator",
				ExecutionType: "system_node",
				SubscribesTo:  []string{"opco.product_initialization_requested"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"opco.product_initialization_requested": existingOwnerHandlerFixture(),
				},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{operating}}
	return &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"operating": &root.Children[0],
			},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"operating": {
				Mode: "template",
				AutoEmitOnCreate: runtimecontracts.AutoEmitOnCreateContract{
					Event: "opco.product_initialization_requested",
				},
			},
		},
	}
}

func routedCallbackTemplateBundle() *runtimecontracts.WorkflowContractBundle {
	repoScaffold := runtimecontracts.FlowContractView{
		Path:  "repo-scaffold",
		Paths: runtimecontracts.FlowContractPaths{ID: "repo-scaffold", Flow: "repo-scaffold"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Mode: "template",
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"repo_scaffold.repo_commit_succeeded": {},
			"repo_scaffold.repo_commit_failed":    {},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"repo-scaffold-node": {
				ID:            "repo-scaffold-node",
				ExecutionType: "system_node",
				SubscribesTo: []string{
					"repo_scaffold.repo_commit_succeeded",
					"repo_scaffold.repo_commit_failed",
				},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"repo_scaffold.repo_commit_succeeded": existingOwnerHandlerFixture(),
					"repo_scaffold.repo_commit_failed":    existingOwnerHandlerFixture(),
				},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{repoScaffold}}
	return &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"repo-scaffold": &root.Children[0],
			},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"repo-scaffold": {Mode: "template"},
		},
	}
}

func routedNodeStaticValidationBundle() *runtimecontracts.WorkflowContractBundle {
	validation := runtimecontracts.FlowContractView{
		Path:  "validation",
		Paths: runtimecontracts.FlowContractPaths{ID: "validation", Flow: "validation"},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"thing.reviewed": {},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"entity-writer": {
				ID:            "entity-writer",
				ExecutionType: "system_node",
				SubscribesTo:  []string{"thing.reviewed"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"thing.reviewed": existingOwnerHandlerFixture(),
				},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{validation}}
	return &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"validation": &root.Children[0],
			},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"validation": {},
		},
	}
}

func routedNodeStaticChildBundle() *runtimecontracts.WorkflowContractBundle {
	child := runtimecontracts.FlowContractView{
		Path:  "child",
		Paths: runtimecontracts.FlowContractPaths{ID: "child", Flow: "child"},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"child.start": {},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"child-intake": {
				ID:            "child-intake",
				ExecutionType: "system_node",
				SubscribesTo:  []string{"child.start"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"child.start": existingOwnerHandlerFixture(),
				},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{child}}
	return &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"child": &root.Children[0],
			},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"child": {},
		},
	}
}
