package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

var family145RootAccumulatorWireCuts = map[string][]string{
	"TestA2PersistedJoinHydrationRefusesCorruptionOnBothStores": {
		"original, err := storetest.ReadRootFlowAccumulatorStorage(p.ctx, p.selected.events, p.runID)",
		"count, err := storetest.SetRootFlowAccumulatorWire(p.ctx, p.selected.events, p.runID, raw)",
		"if err != nil || count != 1",
		"t.Cleanup(func() {\n\t\t\t\t\t\twrite(original)\n\t\t\t\t\t})",
		"write(raw)",
	},
	"TestA2AccumulatorPersistedDuplicatedKeyEvidenceRefusesOnBothStores": {
		"original, err := storetest.ReadRootFlowAccumulatorStorage(proof.ctx, proof.selected.events, proof.runID)",
		"rows, err := storetest.SetRootFlowAccumulatorWire(proof.ctx, proof.selected.events, proof.runID, raw)",
		"if err != nil || rows != 1",
		"t.Cleanup(func() {\n\t\t\t\twrite(original)\n\t\t\t})",
		"write(hostile)",
	},
}

func family145RootAccumulatorWireProofError(function, before, after string) error {
	want, err := family145DescriptorBusinessAssertions(before)
	if err != nil {
		return err
	}
	got, err := family145DescriptorBusinessAssertions(after)
	if err != nil || !reflect.DeepEqual(want, got) {
		return fmt.Errorf("root accumulator wire migration lost original corruption, exact-one, refusal, restoration or replay assertions: %s: %v", function, err)
	}
	cursor := -1
	for _, cut := range family145RootAccumulatorWireCuts[function] {
		at := strings.Index(after, cut)
		if at < 0 || at <= cursor || strings.Count(after, cut) != 1 {
			return fmt.Errorf("root accumulator wire changed context, retained bytes, scope, cardinality or pre-fault restoration: %s", cut)
		}
		cursor = at
	}
	if strings.Contains(after, "UPDATE flow_instances SET accumulator") || strings.Contains(after, "SELECT accumulator FROM flow_instances") {
		return fmt.Errorf("root accumulator consumer retains its old read/write interpreter: %s", function)
	}
	return nil
}

func TestFamily145RootAccumulatorWireProtocolKeepsBothConsumersAndPreFaultCleanup(t *testing.T) {
	var rows []struct{ Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for _, row := range rows {
		cuts := family145RootAccumulatorWireCuts[row.Function]
		if cuts == nil {
			continue
		}
		if found[row.Function] {
			t.Fatalf("ambiguous retained wire consumer: %s", row.Function)
		}
		found[row.Function] = true
		if err := family145RootAccumulatorWireProofError(row.Function, row.Before, row.After); err != nil {
			t.Fatal(err)
		}
		for _, cut := range cuts {
			if family145RootAccumulatorWireProofError(row.Function, row.Before, strings.Replace(row.After, cut, "foreignRetainedWire()", 1)) == nil {
				t.Fatalf("lost exact scope/retained wire/restoration passed: %s", cut)
			}
		}
		assertions, err := family145DescriptorBusinessAssertions(row.After)
		if err != nil {
			t.Fatal(err)
		}
		for _, assertion := range assertions {
			condition := strings.SplitN(assertion, " => ", 2)[0]
			if family145RootAccumulatorWireProofError(row.Function, row.Before, strings.Replace(row.After, condition, "false", 1)) == nil {
				t.Fatalf("lost original corrupt-evidence assertion passed: %s", condition)
			}
		}
	}
	if len(found) != 2 {
		t.Fatalf("retained root wire caller inventory=%d/2", len(found))
	}
	read := family145ConversationForkOwnerSource(t, "internal/backend/pipelinepersistence/workflow_projection_observation.go", "ReadRootFlowAccumulatorStorageTx")
	if queries := family145A2ClaimLiteralQueries(t, read); len(queries) != 1 || queries[0] != "SELECT accumulator FROM flow_instances WHERE run_id=$1 AND entity_id=$1" || !strings.Contains(read, "runID).Scan(&raw)") {
		t.Fatalf("root wire reader changed native bytes/NULL or exact run/entity equality: %v", queries)
	}
	write := family145ConversationForkOwnerSource(t, "internal/backend/pipelinepersistence/workflow_projection_shape_fault.go", "SetRootFlowAccumulatorWireForTest")
	for _, cut := range []string{"workflowProjectionFaultResult(tx.ExecContext(ctx,", "UPDATE flow_instances SET accumulator=$1 WHERE run_id=$2 AND entity_id=$2", "string(raw), run)"} {
		if strings.Count(write, cut) != 1 {
			t.Fatalf("root wire fault changed exact original payload/predicate/row-count owner: %s", cut)
		}
	}
}
