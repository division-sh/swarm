package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

var family145FanOutMetadataCuts = []string{
	"capsuleRaw, err := storetest.ReadFanOutRunCapsuleWire(p.ctx, p.selected.events, p.runID)",
	"history, err := storetest.ReadA2MapHistoryFootprintStorage(p.ctx, p.selected.events, p.runID)",
	"mutations, facts, revisions := history.Mutations, history.Facts, history.Revisions",
	"metadata, err := storetest.ReadFanOutRunClaimMetadataStorage(p.ctx, p.selected.events, p.runID)",
	"cursor, cardinality := metadata.Cursor, metadata.Cardinality",
	"status, owner, generation, lease, retained := metadata.Status, metadata.Owner, metadata.Generation, metadata.Lease, metadata.Capsule",
	"count, err := storetest.SetRetainedFanOutCapsuleWire(p.ctx, p.selected.events, p.runID, raw)",
	"t.Cleanup(func() {\n\t\t\t\t\t\tsetCapsule(t, capsuleRaw)\n\t\t\t\t\t})",
	"setCapsule(t, raw)",
	"storetest.SetRetainedFanOutCardinality(p.ctx, p.selected.events, p.runID, 3)",
	"storetest.SetRetainedFanOutCardinality(p.ctx, p.selected.events, p.runID, 4)",
}

func family145FanOutMetadataProofError(before, after string) error {
	want, err := family145DescriptorBusinessAssertions(before)
	if err != nil {
		return err
	}
	got, err := family145DescriptorBusinessAssertions(after)
	if err != nil || !reflect.DeepEqual(want, got) {
		return fmt.Errorf("retained metadata migration lost original projection/range, claim, no-mutation or refusal assertions: %v", err)
	}
	cursor := -1
	for _, cut := range family145FanOutMetadataCuts {
		at := strings.Index(after, cut)
		if at < 0 || at <= cursor || strings.Count(after, cut) != 1 {
			return fmt.Errorf("metadata migration lost exact wire/claim destinations, read order or pre-fault restoration: %s", cut)
		}
		cursor = at
	}
	for _, old := range []string{"UPDATE fan_out_intents SET capsule", "UPDATE fan_out_intents SET cardinality", "SELECT capsule FROM fan_out_intents", "CAST(claim_generation AS TEXT)"} {
		if strings.Contains(after, old) {
			return fmt.Errorf("consumer retains a non-authoritative metadata interpreter: %s", old)
		}
	}
	return nil
}

func TestFamily145RetainedFanOutMetadataKeepsClaimsRefusalsAndPreFaultRestoration(t *testing.T) {
	var rows []struct{ Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Function != "TestA2MapFanOutRetainedProjectionAndRangeRefuseOnBothStores" {
			continue
		}
		matched++
		if err := family145FanOutMetadataProofError(row.Before, row.After); err != nil {
			t.Fatal(err)
		}
		for _, cut := range family145FanOutMetadataCuts {
			if family145FanOutMetadataProofError(row.Before, strings.Replace(row.After, cut, "foreignMetadataEvidence()", 1)) == nil {
				t.Fatalf("lost metadata scope/clock/cleanup binding passed: %s", cut)
			}
		}
		assertions, err := family145DescriptorBusinessAssertions(row.After)
		if err != nil {
			t.Fatal(err)
		}
		for _, assertion := range assertions {
			condition := strings.SplitN(assertion, " => ", 2)[0]
			if family145FanOutMetadataProofError(row.Before, strings.Replace(row.After, condition, "false", 1)) == nil {
				t.Fatalf("lost original metadata/range/refusal assertion passed: %s", condition)
			}
		}
	}
	if matched != 1 {
		t.Fatalf("retained metadata caller inventory=%d/1", matched)
	}
	for _, tc := range []struct{ function, query, destination string }{
		{"ReadFanOutRunCapsuleWireTx", "SELECT capsule FROM fan_out_intents WHERE run_id=$1", "runID).Scan(&raw)"},
		{"ReadFanOutRunClaimMetadataStorageTx", "SELECT cursor,cardinality,status,claim_owner, CAST(claim_generation AS TEXT),CAST(lease_expires_at AS TEXT),CAST(capsule AS TEXT) FROM fan_out_intents WHERE run_id=$1", "runID).Scan(&out.Cursor, &out.Cardinality, &out.Status, &out.Owner, &out.Generation, &out.Lease, &out.Capsule)"},
	} {
		owner := family145ConversationForkOwnerSource(t, "internal/backend/pipelinepersistence/fan_out_semantic_storage.go", tc.function)
		if queries := family145A2ClaimLiteralQueries(t, owner); len(queries) != 1 || queries[0] != tc.query || strings.Count(owner, tc.destination) != 1 {
			t.Fatalf("%s changed physical scope, strict NULLs, cast clock or scan destination: %v", tc.function, queries)
		}
	}
	for _, tc := range []struct{ function, query, arguments string }{
		{"SetRetainedFanOutCapsuleWireForTest", "UPDATE fan_out_intents SET capsule=$1 WHERE run_id=$2", "string(raw), runID"},
		{"SetRetainedFanOutCardinalityForTest", "UPDATE fan_out_intents SET cardinality=$1 WHERE run_id=$2", "cardinality, runID"},
	} {
		owner := family145ConversationForkOwnerSource(t, "internal/backend/pipelinepersistence/fan_out_metadata_fault.go", tc.function)
		binding := "retainedFanOutMetadataFaultResult(tx.ExecContext(ctx, `" + tc.query + "`, " + tc.arguments + "))"
		if strings.Count(owner, binding) != 1 {
			t.Fatalf("%s changed exact fault scope, payload or result validation", tc.function)
		}
		bridge := family145ConversationForkOwnerSource(t, "internal/runtimepersistence/test_fan_out_metadata_fault.go", tc.function)
		for _, cut := range []string{"validateWorkflowProjectionFaultRun(runID)", "runWorkflowProjectionFault(ctx, selected", "pipelinepersistence." + tc.function + "(ctx, tx, runID,"} {
			if strings.Count(bridge, cut) != 1 {
				t.Fatalf("%s lost original coordinator or exact identity: %s", tc.function, cut)
			}
		}
	}
}
