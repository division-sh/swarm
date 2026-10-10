package cataloge2e

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestCatalogWorkflowStateDiagnosticUsesSelectedProjectionBothStores(t *testing.T) {
	if rows, err := workflowStateDebugRows(nil); err != nil || rows != "" {
		t.Fatalf("absent diagnostic reader changed: %q %v", rows, err)
	}
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			root := canonicalrouting.CopyReceiverOptionalChild(t, false)
			h := newRuntimeHarnessForBackend(t, root, backend, true)
			reader, err := h.catalogOperatorEventLister()
			if err != nil {
				t.Fatal(err)
			}
			if rows, err := workflowStateDebugRows(reader); err != nil || rows != "[]" {
				t.Fatalf("empty physical state diagnostic=%q err=%v", rows, err)
			}
			h.seedInitialState(catalogRuntimeRunID)
			var selected any = h.pg
			closeOwner := h.pg.Close
			if h.sqlite != nil {
				selected = h.sqlite
				closeOwner = h.sqlite.Close
			}
			probe := storetest.CollectTransactions(t, selected, storetest.TransactionProbeOptions{})
			rows, err := storetest.ReadWorkflowStateObservationRows(h.ctx, reader)
			if err != nil || len(rows) == 0 {
				t.Fatalf("canonical construction produced no physical state rows: %+v %v", rows, err)
			}
			if proof := probe.Snapshot(); proof.Total.ReadCommits != 1 || proof.Total.WriteCommits != 0 || proof.Active != 0 {
				t.Fatalf("physical state read escaped original coordinator: %+v", proof)
			}
			var parts []string
			foundRoot := false
			for _, row := range rows {
				parts = append(parts, fmt.Sprintf("{entity_id:%s flow_instance:%s state:%s}", row.EntityID, row.FlowInstance, row.CurrentState))
				if row.EntityID == catalogRuntimeRunID {
					foundRoot = true
					if row.FlowInstance != catalogRuntimeRunID || row.CurrentState != "waiting" {
						t.Fatalf("root physical diagnostic changed: %+v", row)
					}
				}
			}
			if !foundRoot {
				t.Fatal("physical diagnostic omitted the constructed root")
			}
			if dump, err := workflowStateDebugRows(reader); err != nil || dump != "["+strings.Join(parts, ", ")+"]" {
				t.Fatalf("row fields, order or formatting changed: %q %v", dump, err)
			}
			assertEntityState(t, reader, h.workflow, catalogRuntimeRunID, "waiting")
			canceled, cancel := context.WithCancel(h.ctx)
			cancel()
			if rows, err := storetest.ReadWorkflowStateObservationRows(canceled, reader); !errors.Is(err, context.Canceled) || rows != nil {
				t.Fatalf("canceled state read yielded rows=%+v err=%v", rows, err)
			}
			h.shutdown()
			if err := closeOwner(); err != nil {
				t.Fatal(err)
			}
			if rows, err := storetest.ReadWorkflowStateObservationRows(context.Background(), reader); err == nil || rows != nil {
				t.Fatalf("closed state read yielded rows=%+v err=%v", rows, err)
			}
		})
	}
	if rows, err := storetest.ReadWorkflowStateObservationRows(context.Background(), struct{}{}); err == nil || rows != nil {
		t.Fatalf("foreign state owner yielded rows=%+v err=%v", rows, err)
	}
}
