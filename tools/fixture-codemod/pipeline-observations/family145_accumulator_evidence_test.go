package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func family145AccumulatorEvidenceProofError(before, after string) error {
	want, err := family145DescriptorBusinessAssertions(before)
	if err != nil {
		return err
	}
	got, err := family145DescriptorBusinessAssertions(after)
	if err != nil || !reflect.DeepEqual(want, got) {
		return fmt.Errorf("accumulator evidence lost original failure handling: %v", err)
	}
	cursor := -1
	for _, cut := range []string{
		"state, err := storetest.ReadWorkflowAccumulatorStorage(p.ctx, p.selected.events, p.runID, p.runID)",
		"evidence.Revision = state.Revision",
		"evidence.Fields = a2AccumulatorJSONHash(t, state.Fields)",
		"evidence.Accumulator = a2AccumulatorJSONHash(t, state.Accumulator)",
		"mutations, err := storetest.CountRunMutationStorage(p.ctx, p.selected.events, p.runID)",
		"evidence.Mutations = mutations",
		`publications, err := storetest.CountRunEventNameStorage(p.ctx, p.selected.events, p.runID, "item.recorded")`,
		"evidence.Publications = publications",
		"return evidence",
	} {
		at := strings.Index(after, cut)
		if at < 0 || at <= cursor || strings.Count(after, cut) != 1 {
			return fmt.Errorf("accumulator evidence lost ordered independent scope, hash or destination: %s", cut)
		}
		cursor = at
	}
	if strings.Contains(after, "selected.db") || strings.Contains(after, "QueryRowContext") {
		return fmt.Errorf("accumulator evidence retained escaped raw authority")
	}
	return nil
}

func TestFamily145AccumulatorEvidenceRetainsHashingThreeReadCutsAndExactOwner(t *testing.T) {
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, row := range rows {
		if row.File != "internal/runtime/pipeline/a2_accumulator_persistence_external_test.go" || row.Function != "evidence" {
			continue
		}
		found++
		if err := family145AccumulatorEvidenceProofError(row.Before, row.After); err != nil {
			t.Fatal(err)
		}
		for _, lost := range []string{"p.selected.events, p.runID, p.runID)", "p.selected.events, p.runID)", "state.Revision", "state.Fields", "state.Accumulator", "evidence.Mutations = mutations", "evidence.Publications = publications", "return evidence"} {
			if family145AccumulatorEvidenceProofError(row.Before, strings.Replace(row.After, lost, "foreignEvidence", 1)) == nil {
				t.Fatalf("lost exact scope, original hash or destination passed: %s", lost)
			}
		}
		for _, original := range []string{
			`SELECT revision, fields, accumulator FROM entity_state WHERE run_id=$1 AND flow_instance=$2`,
			`SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1`,
			"evidence.Fields = a2AccumulatorJSONHash(t, fields)",
			"evidence.Accumulator = a2AccumulatorJSONHash(t, buckets)",
		} {
			if strings.Count(row.Before, original) != 1 {
				t.Fatalf("immutable physical scope or original hashing changed: %s", original)
			}
		}
	}
	if found != 1 {
		t.Fatalf("accumulator helper inventory=%d, want one complete shared dependency", found)
	}
	physical := family145ConversationForkOwnerSource(t, "internal/backend/pipelinepersistence/workflow_projection_observation.go", "ReadWorkflowAccumulatorStorageTx")
	for _, cut := range []string{"`SELECT revision, fields, accumulator FROM entity_state WHERE run_id=$1 AND flow_instance=$2`, runID, path", "Scan(&out.Revision, &out.Fields, &out.Accumulator)", "return WorkflowAccumulatorStorage{}, err"} {
		if strings.Count(physical, cut) != 1 {
			t.Fatalf("physical accumulator scope, scan destination or error evidence changed: %s", cut)
		}
	}
	bridge := family145ConversationForkOwnerSource(t, "internal/runtimepersistence/test_workflow_projection_storage.go", "ReadWorkflowAccumulatorStorageForTest")
	if strings.Count(bridge, "readFanOutSemanticStorage(ctx, selected, []string{runID},") != 1 || strings.Count(bridge, "pipelinepersistence.ReadWorkflowAccumulatorStorageTx(ctx, tx, runID, path)") != 1 {
		t.Fatal("accumulator bridge changed original selected coordinator, scope or single independent read")
	}
}
