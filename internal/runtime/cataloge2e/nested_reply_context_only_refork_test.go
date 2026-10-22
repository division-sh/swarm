package cataloge2e

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

// Native reconstruction of rows whose logical requests are absent from B's
// events. This proves materialization/readback, not execution of an empty cut.
func proveNativeNestedContextOnlyRefork(t *testing.T, h *runtimeHarness, selected copiedActivityReplyStore, sourceRun string, sourceRows []replycontext.Record, sourceInputID string,
	owner runforkexecution.SelectedContractExecutionOwner, loader runforkexecution.SelectedContractSourceLoader,
	selection runfork.RunForkContractSelection, options runforkexecution.SelectedContractAgentRuntimeOptions) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(h.ctx), 20*time.Second)
	defer cancel()
	if len(sourceRows) != 2 {
		t.Fatal("context-only refork requires the actual two-level source inventory")
	}
	state := sourceRows[0].State
	cut := ""
	for _, row := range sourceRows {
		if row.RunID != sourceRun || row.State != state || row.SourceRunID != catalogRuntimeRunID {
			t.Fatalf("context-only source changed its own lifecycle/original evidence: %+v", row)
		}
		if _, found, err := selected.LoadPreparedPublishEvent(ctx, row.RequestEventID); err != nil || found {
			t.Fatalf("source logical request was invented as an event: found=%t error=%v", found, err)
		}
		if state == replycontext.StateTerminal && row.EnclosingReplyContextID == "" {
			cut = row.AcceptedReplyEventID
		}
	}
	if state != replycontext.StateOpen && state != replycontext.StateTerminal || state == replycontext.StateTerminal && cut == "" {
		t.Fatal("context-only refork lacks its exact open-start or committed terminal cut")
	}
	if state == replycontext.StateTerminal {
		waitNestedReplySourceSettlement(t, ctx, selected, sourceRun, sourceInputID)
	}
	before, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, sourceRun)
	if err != nil {
		t.Fatal(err)
	}
	if state == replycontext.StateTerminal {
		deliveries, deliveryErr := storetest.ReadServedRunDeliverySummary(ctx, selected, sourceRun)
		handoffs, handoffErr := storetest.ReadServedIncompletePipelineHandoffCount(ctx, selected, sourceRun)
		t.Logf("terminal source before reconstruction: deliveries=%+v handoffs=%d errors=%v/%v", deliveries, handoffs, deliveryErr, handoffErr)
	}
	operation := runfork.ForkOperationRequest{OperationID: uuid.NewString(), Actor: "nested-context-only", IdempotencyKey: uuid.NewString(),
		TransportHash: "nested-context-only", SourceRunID: sourceRun, ForkEventID: cut, AtStart: state == replycontext.StateOpen,
		TargetBundleHash: selection.BundleHash, AllowSourceFreeze: true, ContractSelection: selection}
	prepared, err := owner.Prepare(ctx, runforkexecution.SelectedContractExecutionRequest{SourceRunID: sourceRun, At: cut, AtStart: operation.AtStart,
		AllowSourceFreeze: true, ForkOperation: &operation, SourceLoader: loader, ContractSelection: selection, AgentRuntime: options})
	if err != nil {
		t.Fatalf("prepare exact context-only source cut: %v", err)
	}
	defer func() {
		if err := prepared.Close(); err != nil {
			t.Error(err)
		}
	}()
	request, err := prepared.MaterializationRequest()
	if err != nil {
		t.Fatal(err)
	}
	request.ForkOperation = &operation
	child, err := selected.MaterializeRunForkForSelectedContractExecution(ctx, request)
	if err != nil || child.SourceRunID != sourceRun || child.ForkRunStatus != runfork.RunForkMaterializedStatus || child.SelectedContractBinding == nil ||
		child.ForkPoint.Kind == runfork.RunForkPointRunStart != operation.AtStart {
		t.Fatalf("materialize context-only descendant at exact source cut: %+v error=%v", child, err)
	}
	rows, err := storetest.ReadReplyContextRecords(ctx, selected, child.ForkRunID)
	if err != nil || len(rows) != 2 {
		t.Fatalf("context-only child lost the complete native inventory: %+v error=%v", rows, err)
	}
	bySource, byChild := map[string]replycontext.Record{}, map[string]replycontext.Record{}
	for _, source := range sourceRows {
		for _, row := range rows {
			if row.SourceRequestEventID == source.SourceRequestEventID {
				if _, duplicate := bySource[source.ID]; duplicate {
					t.Fatal("context-only projection repeated one source context")
				}
				bySource[source.ID], byChild[row.ID] = row, row
			}
		}
	}
	if len(bySource) != 2 || len(byChild) != 2 {
		t.Fatal("context-only projection omitted or repeated a request level")
	}
	for _, source := range sourceRows {
		row := bySource[source.ID]
		if row.ID == source.ID || row.RunID != child.ForkRunID || row.RequestEventID != projectedLogicalReplyEventID(child.ForkRunID, source.RequestEventID) ||
			row.SourceRunID != source.SourceRunID || row.SourceRequestEventID != source.SourceRequestEventID || row.State != source.State ||
			row.RequesterFlowID != source.RequesterFlowID || row.ProviderFlowID != source.ProviderFlowID || row.RequestCorrelationID != source.RequestCorrelationID ||
			row.RequestOutputPin != source.RequestOutputPin || row.ReplyInputPin != source.ReplyInputPin || row.ProviderInputPin != source.ProviderInputPin ||
			row.ProviderOutputPin != source.ProviderOutputPin || row.CorrelationKey != source.CorrelationKey || !row.CreatedAt.Equal(source.CreatedAt) || !row.UpdatedAt.Equal(source.UpdatedAt) {
			t.Fatalf("native context-only projection changed the captured row: source=%+v child=%+v", source, row)
		}
		expectedOrigin := source.Origin
		if source.Origin.EntityID != "" {
			projection, err := runfork.ProjectEntityOwnership(sourceRun, child.ForkRunID, source.Origin.EntityID, source.Origin.FlowInstance)
			if err != nil {
				t.Fatal(err)
			}
			expectedOrigin.EntityID, expectedOrigin.FlowInstance = projection.Fork.EntityID, projection.Fork.FlowInstance
		}
		if row.Origin != expectedOrigin || !reflect.DeepEqual(row.ReturnJoins, source.ReturnJoins) {
			t.Fatal("context-only projection changed the exact return owner/binding")
		}
		enclosing := ""
		if source.EnclosingReplyContextID != "" {
			enclosing = bySource[source.EnclosingReplyContextID].ID
			if enclosing == "" {
				t.Fatal("context-only projection omitted the captured enclosing level")
			}
		}
		if row.EnclosingReplyContextID != enclosing {
			t.Fatal("context-only return still refers to a source or other level")
		}
		if state == replycontext.StateOpen {
			if row.AcceptedReplyEventID != "" || row.TerminalAt != nil {
				t.Fatal("context-only materialization settled an open reply")
			}
		} else if row.AcceptedReplyEventID != projectedLogicalReplyEventID(child.ForkRunID, source.AcceptedReplyEventID) ||
			row.TerminalAt == nil || source.TerminalAt == nil || !row.TerminalAt.Equal(*source.TerminalAt) {
			t.Fatal("context-only materialization reopened or changed a terminal reply")
		}
		for _, logical := range []string{row.RequestEventID, row.AcceptedReplyEventID} {
			if logical == "" {
				continue
			}
			if _, found, err := selected.LoadPreparedPublishEvent(ctx, logical); err != nil || found {
				t.Fatalf("context-only materialization copied a logical event: found=%t error=%v", found, err)
			}
		}
	}
	afterRows, err := storetest.ReadReplyContextRecords(ctx, selected, sourceRun)
	after, stateErr := storetest.ReadSelectedForkSourceDomain(ctx, selected, sourceRun)
	if err != nil || stateErr != nil || !reflect.DeepEqual(sourceRows, afterRows) || !reflect.DeepEqual(before, after) {
		for table, prior := range before {
			if current := after[table]; !reflect.DeepEqual(prior, current) {
				t.Logf("context-only source %s changed %s: before=%+v after=%+v", sourceRun, table, prior, current)
			}
		}
		t.Fatalf("context-only refork changed source state/evidence: %v/%v", err, stateErr)
	}
	if row := bySource[sourceRows[0].ID]; row.Origin == (events.RouteIdentity{}) {
		t.Fatal("context-only projection discarded its return origin")
	}
}

// Logical reply references use the delivery/event replay namespace, even when
// there is no physical event to copy. Activity publication has a different ID.
func projectedLogicalReplyEventID(childRun, sourceEvent string) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm/run-fork/delivery-event-replay/event/"+childRun+"/"+sourceEvent)).String()
}

func waitNestedReplySourceSettlement(t *testing.T, ctx context.Context, selected any, sourceRun, inputID string) {
	t.Helper()
	if inputID == "" {
		t.Fatal("terminal source lacks its exact selected input")
	}
	for {
		deliveries, deliveryErr := storetest.ReadServedRunDeliverySummary(ctx, selected, sourceRun)
		handoffs, handoffErr := storetest.ReadServedIncompletePipelineHandoffCount(ctx, selected, sourceRun)
		diagnostic, logErr := storetest.ReadSelectedForkLifecycleDiagnosticStorage(ctx, selected, sourceRun)
		if deliveryErr != nil || handoffErr != nil || logErr != nil || deliveries.DeadLetter != 0 || deliveries.Canceled != 0 {
			t.Fatalf("read terminal source settlement: deliveries=%+v handoffs=%d errors=%v/%v/%v", deliveries, handoffs, deliveryErr, handoffErr, logErr)
		}
		dispatched := false
		for _, entry := range diagnostic.Logs {
			if entry.RunID != sourceRun {
				continue
			}
			var log struct {
				Details struct {
					Action  string `json:"action"`
					EventID string `json:"event_id"`
					RunID   string `json:"run_id"`
				} `json:"details"`
			}
			if err := json.Unmarshal(entry.Payload, &log); err != nil {
				t.Fatal(err)
			}
			dispatched = dispatched || log.Details.Action == "selected_contract_event_dispatched" && log.Details.EventID == inputID && log.Details.RunID == sourceRun
		}
		if dispatched && handoffs == 0 && deliveries.Pending == 0 && deliveries.InProgress == 0 && deliveries.RetryScheduled == 0 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("terminal source did not settle before the source snapshot within the original bound: deliveries=%+v handoffs=%d dispatched=%t cause=%v", deliveries, handoffs, dispatched, context.Cause(ctx))
		case <-time.After(10 * time.Millisecond):
		}
	}
}
