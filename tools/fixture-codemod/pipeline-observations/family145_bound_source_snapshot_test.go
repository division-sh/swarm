package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func family145BoundSourceSnapshotProofError(before, after string) error {
	want, err := family145DescriptorBusinessAssertions(before)
	if err != nil {
		return err
	}
	got, err := family145DescriptorBusinessAssertions(after)
	if err != nil || !reflect.DeepEqual(want, got) {
		return fmt.Errorf("bound source snapshot lost original empty-path refusal: %v", err)
	}
	for _, cut := range []string{"snapshot, err := storetest.ReadBoundReplySourceStorage(ctx, selected.events, runID, concretePath)", "return snapshot"} {
		if strings.Count(after, cut) != 1 {
			return fmt.Errorf("bound source snapshot changed owner/context/run/path or destination: %s", cut)
		}
	}
	for _, retired := range []string{"selected.db", "QueryContext", "rows.Columns()", "rows.Scan", "rows.Close", "table.path", "table.order"} {
		if strings.Contains(after, retired) {
			return fmt.Errorf("bound source retained escaped SQL/row protocol: %s", retired)
		}
	}
	return nil
}

func TestFamily145BoundSourceSnapshotRetainsAllPhysicalColumnsAndSeparateCuts(t *testing.T) {
	var rows []struct{ Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var original, current string
	for _, row := range rows {
		if row.Function == "a2BoundReplySourceSnapshot" {
			if original != "" {
				t.Fatal("ambiguous bound-source snapshot helper")
			}
			original, current = row.Before, row.After
		}
	}
	if original == "" {
		t.Fatal("missing whole bound-source snapshot transition")
	}
	if err := family145BoundSourceSnapshotProofError(original, current); err != nil {
		t.Fatal(err)
	}
	for _, lost := range []string{"ctx, selected.events", "selected.events, runID, concretePath)", "return snapshot", `concretePath == ""`} {
		if family145BoundSourceSnapshotProofError(original, strings.Replace(current, lost, "foreignSource", 1)) == nil {
			t.Fatalf("lost source scope or original refusal passed: %s", lost)
		}
	}
	for _, scope := range []struct{ table, path, order, function string }{
		{"flow_instances", "instance_path", "instance_path", "ReadBoundReplyFlowHeadersStorageTx"},
		{"entity_state", "flow_instance", "entity_id", "ReadBoundReplyEntityStatesStorageTx"},
	} {
		if strings.Count(original, fmt.Sprintf(`{"%s", "%s", "%s"}`, scope.table, scope.path, scope.order)) != 1 {
			t.Fatal("immutable original scoped table/column/order catalog changed")
		}
		physical := family145ConversationForkOwnerSource(t, "internal/backend/pipelinepersistence/workflow_projection_observation.go", scope.function)
		call := fmt.Sprintf("return readBoundReplyPhysicalRows(ctx, tx, `SELECT * FROM %s WHERE run_id=$1 AND %s=$2 ORDER BY %s`, runID, path)", scope.table, scope.path, scope.order)
		if strings.Count(physical, call) != 1 {
			t.Fatalf("%s narrowed columns, widened scope or changed row order/argument binding", scope.function)
		}
	}
	assertFamily145BoundSourceSnapshotProtocol(t, original)
}

func assertFamily145BoundSourceSnapshotProtocol(t *testing.T, original string) {
	t.Helper()
	physical := family145ConversationForkOwnerSource(t, "internal/backend/pipelinepersistence/workflow_projection_observation.go", "readBoundReplyPhysicalRows")
	for _, cut := range []string{"columns, err := rows.Columns()", "for rows.Next() {", "values, destinations := make([]any, len(columns)), make([]any, len(columns))", "destinations[index] = &values[index]", "rows.Scan(destinations...)", "if raw, ok := value.([]byte); ok {", "values[index] = string(raw)"} {
		if strings.Count(original, cut) != 1 || strings.Count(physical, cut) != 1 {
			t.Fatalf("full physical row, NULL/type/bytes or column-order protocol changed: %s", cut)
		}
	}
	for _, cut := range []string{"rows, err := tx.QueryContext(ctx, query, runID, path)", "errors.Join(err, rows.Close())", "out = nil", "return out, rows.Err()"} {
		if strings.Count(physical, cut) != 1 {
			t.Fatalf("bounded row protocol lost original scope or safe error/close handling: %s", cut)
		}
	}
	bridge := family145ConversationForkOwnerSource(t, "internal/runtimepersistence/test_workflow_projection_storage.go", "ReadBoundReplySourceStorageForTest")
	if strings.Count(bridge, "readFanOutSemanticStorage(ctx, selected, []string{runID},") != 2 || !strings.Contains(bridge, `return map[string][][]any{"flow_instances": headers, "entity_state": fields}, nil`) {
		t.Fatal("snapshot merged temporal cuts, escaped native readers or changed table-key/empty-row representation")
	}
	first := strings.Index(bridge, "ReadBoundReplyFlowHeadersStorageTx(ctx, tx, runID, path)")
	second := strings.Index(bridge, "ReadBoundReplyEntityStatesStorageTx(ctx, tx, runID, path)")
	if first < 0 || second <= first || strings.Count(bridge, "return nil, err") != 2 {
		t.Fatal("snapshot changed read order or exposed provisional first-read evidence")
	}
}
