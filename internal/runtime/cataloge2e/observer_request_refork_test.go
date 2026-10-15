package cataloge2e

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/store/storetest"
)

// A physical observer-only publication still belongs to the original request,
// but continuing it must neither recreate the provider nor reopen its reply.
func executeObserverOnlyRequestRefork(t *testing.T, h *runtimeHarness, selected unclaimedFanOutReplyStore,
	owner runforkexecution.SelectedContractExecutionOwner, loader runforkexecution.SelectedContractSourceLoader,
	selection runfork.RunForkContractSelection, options runforkexecution.SelectedContractAgentRuntimeOptions, sourceRun, cut string) (string, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(h.ctx), 20*time.Second)
	defer cancel()
	proveMixedRequestObserverOnlyReforkPartition(t, ctx, selected, sourceRun, cut)
	publication, found, err := selected.LoadPreparedPublishEvent(ctx, cut)
	if err != nil || !found || publication.Event.Event().RunID() != sourceRun || len(publication.DeliveryRoutes) != 1 {
		t.Fatalf("observer-only physical source: publication=%+v found=%t error=%v", publication, found, err)
	}
	beforeRows, err := storetest.ReadReplyContextRecords(ctx, selected, sourceRun)
	if err != nil || len(beforeRows) != 1 || beforeRows[0].State != replycontext.StateTerminal {
		t.Fatalf("observer-only source lacks its own terminal context: rows=%+v error=%v", beforeRows, err)
	}
	waitReplyReforkQuiescence(t, ctx, selected, sourceRun)
	before, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, sourceRun)
	if err != nil {
		t.Fatal(err)
	}
	operation := catalogStagedForkOperation(t, ctx, selected, selection, sourceRun, cut)
	staged := stageCatalogSelectedContractFork(t, ctx, selected, owner, loader, selection, options, sourceRun, cut, operation)
	rows, err := storetest.ReadReplyContextRecords(ctx, selected, staged.ForkRunID)
	if err != nil || len(rows) != 1 {
		t.Fatalf("observer-only child did not retain one context: rows=%+v error=%v", rows, err)
	}
	original, row := beforeRows[0], rows[0]
	origin, err := runfork.ProjectEntityOwnership(sourceRun, staged.ForkRunID, original.Origin.EntityID, original.Origin.FlowInstance)
	if err != nil || row.RunID != staged.ForkRunID || row.ID == original.ID || row.RequestEventID == original.RequestEventID ||
		row.AcceptedReplyEventID == original.AcceptedReplyEventID || row.State != replycontext.StateTerminal ||
		row.SourceRunID != original.SourceRunID || row.SourceRequestEventID != original.SourceRequestEventID ||
		row.Origin != (events.RouteIdentity{FlowID: original.Origin.FlowID, FlowInstance: origin.Fork.FlowInstance, EntityID: origin.Fork.EntityID}) ||
		row.RequestCorrelationID != original.RequestCorrelationID || row.CorrelationKey != original.CorrelationKey ||
		row.RequesterFlowID != original.RequesterFlowID || row.RequestOutputPin != original.RequestOutputPin || row.ReplyInputPin != original.ReplyInputPin ||
		row.ProviderFlowID != original.ProviderFlowID || row.ProviderInputPin != original.ProviderInputPin || row.ProviderOutputPin != original.ProviderOutputPin ||
		!row.CreatedAt.Equal(original.CreatedAt) || !row.UpdatedAt.Equal(original.UpdatedAt) ||
		row.TerminalAt == nil || original.TerminalAt == nil || !row.TerminalAt.Equal(*original.TerminalAt) {
		t.Fatalf("observer-only projection changed terminal meaning: source=%+v child=%+v error=%v", original, row, err)
	}
	executed, err := runforkexecution.ExecuteSelectedContractRunFork(ctx, runforkexecution.SelectedContractExecutionRequest{
		SourceRunID: sourceRun, At: cut, ForkOperation: &operation, AllowSourceFreeze: true, Owner: owner, SourceLoader: loader, ContractSelection: selection, AgentRuntime: options,
	})
	if err != nil || !executed.Activation.Activated || executed.Materialization.ForkRunID != staged.ForkRunID {
		t.Fatalf("activate observer-only child: activation=%+v error=%v", executed.Activation, err)
	}
	inputID := activityidentity.ForkLineageEventID(staged.ForkRunID, cut)
	for {
		input, found, err := selected.LoadPreparedPublishEvent(ctx, inputID)
		if err != nil || (found && len(input.DeliveryRoutes) != 1) {
			t.Fatalf("observer-only child publication: publication=%+v found=%t error=%v", input, found, err)
		}
		if !found {
			select {
			case <-ctx.Done():
				t.Fatalf("observer-only child was acknowledged but did not publish its input: %v", context.Cause(ctx))
			case <-time.After(10 * time.Millisecond):
				continue
			}
		}
		lineage, inherited := input.Event.Event().SelectedForkLineage()
		if !inherited || lineage.SourceRunID() != sourceRun || lineage.SourceEventID() != cut ||
			!reflect.DeepEqual(input.Event.Event().Payload(), publication.Event.Event().Payload()) {
			t.Fatal("observer-only publication skipped the immediate source or changed business data")
		}
		originalRoute, route := publication.DeliveryRoutes[0], input.DeliveryRoutes[0]
		projected, err := runfork.ProjectEntityOwnership(sourceRun, staged.ForkRunID, originalRoute.Target.Route().EntityID, originalRoute.Target.Route().FlowInstance)
		if err != nil || route.Recipient != originalRoute.Recipient || !route.ConnectClaim.Equal(originalRoute.ConnectClaim) ||
			!reflect.DeepEqual(route.Context, originalRoute.Context) || route.Context.ReplyContextID() != "" ||
			route.Target.Route() != (events.RouteIdentity{FlowID: originalRoute.Target.Route().FlowID, FlowInstance: projected.Fork.FlowInstance, EntityID: projected.Fork.EntityID}) {
			t.Fatalf("observer-only continuation changed its exact route: route=%+v error=%v", route, err)
		}
		id, err := deliverylifecycle.DeliveryID(inputID, route)
		if err != nil {
			t.Fatal(err)
		}
		delivery, err := selected.Snapshot(ctx, id)
		if err != nil || (delivery.Terminal() && delivery.Status != deliverylifecycle.StatusDelivered) {
			t.Fatalf("observer-only delivery refused: delivery=%+v error=%v", delivery, err)
		}
		observed, err := storetest.ReadLifecycleEventCardinality(ctx, selected, staged.ForkRunID, "request.observed")
		if err != nil || observed > 1 {
			t.Fatalf("observer-only output count=%d error=%v", observed, err)
		}
		pending, err := storetest.ReadServedIncompletePipelineHandoffCount(ctx, selected, staged.ForkRunID)
		if err != nil {
			t.Fatal(err)
		}
		if delivery.Status == deliverylifecycle.StatusDelivered && observed == 1 && pending == 0 {
			if delivery.ClaimVersion != 1 {
				t.Fatal("observer-only delivery executed more than once")
			}
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("observer-only child did not finish: delivery=%+v observed=%d pending=%d cause=%v", delivery, observed, pending, context.Cause(ctx))
		case <-time.After(10 * time.Millisecond):
		}
	}
	for _, name := range []string{"provider.replied", "request.finished"} {
		count, err := storetest.ReadLifecycleEventCardinality(ctx, selected, staged.ForkRunID, name)
		if err != nil || count != 0 {
			t.Fatalf("observer-only continuation recreated settled work %s: count=%d error=%v", name, count, err)
		}
	}
	waitReplyReforkQuiescence(t, ctx, selected, staged.ForkRunID)
	afterRows, err := storetest.ReadReplyContextRecords(ctx, selected, staged.ForkRunID)
	if err != nil || !reflect.DeepEqual(afterRows, rows) {
		t.Fatalf("observer-only execution reopened or rewrote the terminal context: rows=%+v error=%v", afterRows, err)
	}
	after, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, sourceRun)
	if err != nil || !reflect.DeepEqual(before, after) {
		for table, original := range before {
			if !reflect.DeepEqual(original, after[table]) {
				t.Logf("observer-only immediate source changed %s: before=%+v after=%+v", table, original, after[table])
			}
		}
		t.Fatalf("observer-only child changed its immediate source: %v", err)
	}
	sourceReplyRows, err := storetest.ReadReplyContextRecords(ctx, selected, sourceRun)
	if err != nil || !reflect.DeepEqual(beforeRows, sourceReplyRows) {
		t.Fatalf("reviewer: observer-only child changed source reply rows: before=%+v after=%+v error=%v", beforeRows, sourceReplyRows, err)
	}
	return staged.ForkRunID, inputID
}

func waitReplyReforkQuiescence(t *testing.T, ctx context.Context, selected any, runID string) {
	t.Helper()
	reader, ok := selected.(interface {
		LoadRunTestQuiescence(context.Context, string, time.Time) (operatorread.RunTestQuiescence, error)
	})
	if !ok {
		t.Fatal("reply refork requires the canonical run-quiescence reader")
	}
	// The delivery handoff witness excludes events with no recipients. Their
	// pipeline receipts must also settle before measuring source immutability.
	for {
		state, err := reader.LoadRunTestQuiescence(ctx, runID, time.Now().UTC())
		if err != nil {
			t.Fatalf("read exact reply-refork quiescence: state=%+v error=%v", state, err)
		}
		if state.Ready {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("reply refork did not complete all publication work: state=%+v cause=%v", state, context.Cause(ctx))
		case <-time.After(10 * time.Millisecond):
		}
	}
}
