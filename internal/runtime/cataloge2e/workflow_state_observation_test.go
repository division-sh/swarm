package cataloge2e

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
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
				state := "null"
				if row.CurrentState != nil {
					state = strconv.Quote(*row.CurrentState)
				}
				parts = append(parts, fmt.Sprintf("{entity_id:%s flow_instance:%s state:%s}", row.EntityID, row.FlowInstance, state))
				if row.EntityID == catalogRuntimeRunID {
					foundRoot = true
					if row.RunID != catalogRuntimeRunID || row.FlowInstance != catalogRuntimeRunID || row.CurrentState == nil || *row.CurrentState != "waiting" || len(row.Fields) == 0 {
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
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		for _, tc := range []struct {
			stage string
			want  string
		}{
			{"null", `state:"null"`},
			{"unknown", `state:"unknown"`},
		} {
			t.Run(string(backend)+"/literal-"+tc.stage, func(t *testing.T) {
				h := newRuntimeHarnessForBackend(t, canonicalrouting.CopyWorkflowStateDiagnostic(t, tc.stage), backend, true)
				h.seedInitialState(catalogRuntimeRunID)
				h.shutdown()
				reader, err := h.catalogOperatorEventLister()
				if err != nil {
					t.Fatal(err)
				}
				ctx := testAuthorActivityContext(context.Background())
				rows, err := storetest.ReadWorkflowStateObservationRows(ctx, reader)
				if err != nil || len(rows) != 1 || rows[0].EntityID != catalogRuntimeRunID || rows[0].FlowInstance != catalogRuntimeRunID || rows[0].CurrentState == nil || *rows[0].CurrentState != tc.stage {
					t.Fatalf("exact constructed diagnostic row=%+v err=%v", rows, err)
				}
				fieldsBefore := append([]byte(nil), rows[0].Fields...)
				if rows[0].RunID != catalogRuntimeRunID || len(fieldsBefore) == 0 {
					t.Fatalf("physical diagnostic omitted actual data ownership/fields: %+v", rows[0])
				}
				probe := storetest.CollectTransactions(t, reader, storetest.TransactionProbeOptions{})
				prefix := "[{entity_id:" + catalogRuntimeRunID + " flow_instance:" + catalogRuntimeRunID + " "
				if dump, err := workflowStateDebugRows(reader); err != nil || dump != prefix+tc.want+"}]" {
					t.Fatalf("literal stage diagnostic=%q want=%q err=%v", dump, prefix+tc.want+"}]", err)
				}
				if proof := probe.Snapshot(); proof.Total.ReadCommits != 1 || proof.Total.WriteCommits != 0 || proof.Active != 0 {
					t.Fatalf("literal diagnostic escaped original read owner: %+v", proof)
				}
				if changed, err := storetest.RemoveWorkflowProjectionHeader(ctx, reader, catalogRuntimeRunID, catalogRuntimeRunID); err != nil || changed != 1 {
					t.Fatalf("exact missing-header diagnostic fault: rows=%d err=%v", changed, err)
				}
				if dump, err := workflowStateDebugRows(reader); err != nil || dump != prefix+"state:null}]" {
					t.Fatalf("absent header diagnostic=%q want=%q err=%v", dump, prefix+"state:null}]", err)
				}
				physical, err := storetest.ReadWorkflowStateObservationRows(ctx, reader)
				if err != nil || len(physical) != 1 || physical[0].RunID != catalogRuntimeRunID || physical[0].EntityID != catalogRuntimeRunID || physical[0].CurrentState != nil || !bytes.Equal(physical[0].Fields, fieldsBefore) {
					t.Fatalf("missing header hid or changed its independent field row: %+v err=%v", physical, err)
				}
				if proof := probe.Snapshot(); proof.Total.ReadCommits != 3 || proof.Total.WriteCommits != 1 || proof.Active != 0 {
					t.Fatalf("diagnostic reads or exact header fault escaped original owners: %+v", proof)
				}
			})
		}
	}
	if rows, err := storetest.ReadWorkflowStateObservationRows(context.Background(), struct{}{}); err == nil || rows != nil {
		t.Fatalf("foreign state owner yielded rows=%+v err=%v", rows, err)
	}
}
