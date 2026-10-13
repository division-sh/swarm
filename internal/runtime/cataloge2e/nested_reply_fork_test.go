package cataloge2e

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

// This proves native row transfer. At the final event's cut its observer delivery
// is still unclaimed; the independent non-agent replay refusal remains mandatory.
// This is not positive activation, two-open or deadline/restart journey credit.
func TestIssue642TerminalNestedReplyNativeMaterializationBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			root := canonicalrouting.CopyNestedNodeReplyDepth3(t)
			h := newRuntimeHarnessForBackend(t, root, backend, true)
			step := catalogTriggerStep{Event: "request.started", Payload: map[string]any{"account_id": "same-account", "request_id": "same-business-key"},
				eventID: uuid.NewString(), createdAt: time.Now().UTC(), sourceAgent: "cataloge2e", inputKind: catalogReplayInputRootIngress}
			h.publishAndWait(step, 20*time.Second)
			h.waitForCatalogStoreQuiescence(20 * time.Second)
			proveNestedReplyRowAndReturnBindings(t, h, 1)
			reader, err := h.catalogOperatorEventLister()
			if err != nil {
				t.Fatal(err)
			}
			sourceRows, err := storetest.ReadReplyContextRecords(h.ctx, reader, catalogRuntimeRunID)
			if err != nil || len(sourceRows) != 3 {
				t.Fatalf("actual terminal source chain: %+v %v", sourceRows, err)
			}
			cut := ""
			for _, event := range catalogEventsSince(t, reader, h.startedAt) {
				if event.Name == "request.finished" {
					if cut != "" {
						t.Fatal("terminal source chain produced two final cuts")
					}
					cut = event.ID
				}
			}
			if cut == "" {
				t.Fatal("terminal source has no actual final publication")
			}
			var selected interface {
				runforkexecution.SelectedContractForkLifecycle
				storetest.DurableDataCatalogStore
				runforkexecution.SourceArtifactSelectedContractSourceStore
			} = h.pg
			if h.sqlite != nil {
				selected = h.sqlite
			}
			loader, selection, _ := selectedContractForkFixtureSelection(t, h.ctx, repoRootFromCatalogE2E(t), root, selected)
			owner := selectedContractExecutionOwnerForCatalogHarness(t, h)
			options := selectedContractAgentRuntimeOptionsForCatalogHarness(h, testRuntimeConfig())
			staged := stageCatalogSelectedContractFork(t, h.ctx, selected, owner, loader, selection, options, catalogRuntimeRunID, cut)
			if staged.ExecutionReady || staged.SelectedContractBinding == nil || staged.ForkRunStatus != runfork.RunForkMaterializedStatus {
				t.Fatalf("row projection created execution authority: %+v", staged)
			}
			childRows, err := storetest.ReadReplyContextRecords(h.ctx, reader, staged.ForkRunID)
			if err != nil || len(childRows) != len(sourceRows) {
				t.Fatalf("exact native child inventory: %+v %v", childRows, err)
			}
			var events bus.PreparedPublishEventReader = h.pg
			if h.sqlite != nil {
				events = h.sqlite
			}
			proveTerminalReplyRowProjection(t, h.ctx, events, sourceRows, childRows, staged.ForkRunID)
			after, err := storetest.ReadReplyContextRecords(h.ctx, reader, catalogRuntimeRunID)
			if err != nil || !reflect.DeepEqual(sourceRows, after) {
				t.Fatalf("native projection changed source reply authority: %+v %v", after, err)
			}
			// Reply readback must not erase an unrelated replay-policy refusal or
			// create execution authority. The cut is publication, not its later drain.
			recovered, err := runforkexecution.ActivateSelectedContractRunFork(correlation.WithRunID(h.ctx, staged.ForkRunID), runforkexecution.SelectedContractActivationGateRequest{
				ForkRunID: staged.ForkRunID, AllowSourceFreeze: true, Store: runScopedCatalogStore(t, h),
				ExecutionOwner: owner, SourceLoader: loader, AgentRuntime: options,
			})
			if err == nil || !strings.Contains(err.Error(), runfork.RunForkBlockerNonAgentDeliveryReplayUnsupported) ||
				recovered.Activated || recovered.ExecutedEventCount != 0 || recovered.ForkLocalRuntimeContainer != nil || len(recovered.ForkEvents) != 0 {
				t.Fatalf("terminal reply transfer bypassed the exact unrelated replay gate: %+v %v", recovered, err)
			}
			after, err = storetest.ReadReplyContextRecords(h.ctx, reader, staged.ForkRunID)
			if err != nil || !reflect.DeepEqual(childRows, after) {
				t.Fatalf("activation reopened or replaced terminal reply rows: %+v %v", after, err)
			}
			assertSourceRunLifecycle(t, runScopedCatalogStore(t, h), staged.ForkRunID, runfork.RunForkMaterializedStatus, false)
		})
	}
}

func proveTerminalReplyRowProjection(t *testing.T, ctx context.Context, events bus.PreparedPublishEventReader, source, child []replycontext.Record, childRun string) {
	t.Helper()
	bySource, byChild := map[string]replycontext.Record{}, map[string]replycontext.Record{}
	for _, record := range source {
		bySource[record.ID] = record
	}
	for _, record := range child {
		byChild[record.ID] = record
		var original *replycontext.Record
		for i := range source {
			if source[i].SourceRequestEventID == record.SourceRequestEventID {
				if original != nil {
					t.Fatal("child reply matched two original physical requests")
				}
				original = &source[i]
			}
		}
		if original == nil || record.ID == original.ID || record.RunID != childRun || record.SourceRunID != catalogRuntimeRunID ||
			record.State != replycontext.StateTerminal || record.RequestEventID == original.RequestEventID ||
			record.AcceptedReplyEventID == original.AcceptedReplyEventID || record.RequesterFlowID != original.RequesterFlowID ||
			record.ProviderFlowID != original.ProviderFlowID || record.RequestCorrelationID != original.RequestCorrelationID ||
			!record.CreatedAt.Equal(original.CreatedAt) || !record.UpdatedAt.Equal(original.UpdatedAt) ||
			record.TerminalAt == nil || original.TerminalAt == nil || !record.TerminalAt.Equal(*original.TerminalAt) {
			t.Fatalf("terminal captured lifecycle/original evidence changed: source=%+v child=%+v", original, record)
		}
		for _, logical := range []string{record.RequestEventID, record.AcceptedReplyEventID} {
			_, found, err := events.LoadPreparedPublishEvent(ctx, logical)
			if err != nil || found {
				t.Fatalf("context-only projection fabricated physical event %s: %t %v", logical, found, err)
			}
		}
	}
	for _, record := range child {
		if record.EnclosingReplyContextID == "" {
			continue
		}
		parent, found := byChild[record.EnclosingReplyContextID]
		if !found || parent.RunID != childRun || parent.ProviderFlowID != record.RequesterFlowID {
			t.Fatalf("child enclosing link borrowed source or sibling authority: %+v parent=%+v", record, parent)
		}
		if _, sourceRef := bySource[record.EnclosingReplyContextID]; sourceRef {
			t.Fatal("child still carries a source enclosing reference")
		}
	}
}
