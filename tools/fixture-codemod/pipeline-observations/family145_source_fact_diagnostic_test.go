package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

var family145SourceFactDiagnosticCuts = []string{
	"before := f.footprint(t)",
	"originalEvidence, err := storetest.ReadLatestFanOutObligationFactStorage(f.p.ctx, f.p.selected.events, f.p.runID)",
	"originalFactKey, originalFact = originalEvidence.Key, originalEvidence.Fact",
	"\n\t\t\t\t_, err := f.p.pc.ServeFanOutCandidate(f.p.ctx, f.handoff.owner, f.handoff.key)\n",
	"waitForGateRecoveryQuiescence(t, f.p.bus, f.p.ctx)",
	"returned, typed := runtimefailures.EnvelopeFromError(err)",
	"blocked, err := storetest.ReadFanOutRunBlockedClaimStorage(f.p.ctx, f.p.selected.events, f.p.runID)",
	"diagnostic, released = blocked.Diagnostic, blocked.Released",
	"persisted, decodeErr := runtimefailures.UnmarshalEnvelope(diagnostic)",
	"revisionEvidence, err := storetest.ReadRunRevisionFactStorage(f.p.ctx, f.p.selected.events, f.p.runID, want.LatestRevision)",
	"family, factKey = revisionEvidence.Family, revisionEvidence.Key",
	"fact, present = revisionEvidence.Fact, revisionEvidence.Present",
	"beforeRetry := f.footprint(t)",
}

func family145SourceFactDiagnosticProofError(before, after string) error {
	want, err := family145DescriptorBusinessAssertions(before)
	if err != nil {
		return err
	}
	got, err := family145DescriptorBusinessAssertions(after)
	if err != nil || !reflect.DeepEqual(want, got) {
		return fmt.Errorf("source fact/diagnostic migration lost original refusal, history, no-progress, retry or real-pump assertions: %v", err)
	}
	cursor := -1
	for _, cut := range family145SourceFactDiagnosticCuts {
		at := strings.Index(after, cut)
		if at < 0 || at <= cursor || strings.Count(after, cut) != 1 {
			return fmt.Errorf("source fact/diagnostic migration lost exact scope, physical destination or actual failure cut: %s", cut)
		}
		cursor = at
	}
	if strings.Contains(after, "QueryRow") || strings.Contains(after, "FROM run_fork_fact_revisions") || strings.Contains(after, "claim_owner IS NULL") {
		return fmt.Errorf("source refusal consumer retains its old fact/claim interpreter")
	}
	return nil
}

func TestFamily145SourceFactDiagnosticChainPreservesEveryAssertionAndExactReadCut(t *testing.T) {
	var rows []struct{ Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Function != "TestA2MapFanOutRawBusinessSourcePumpRefusesWithoutProgressOnBothStores" {
			continue
		}
		matched++
		if err := family145SourceFactDiagnosticProofError(row.Before, row.After); err != nil {
			t.Fatal(err)
		}
		for _, cut := range family145SourceFactDiagnosticCuts {
			if family145SourceFactDiagnosticProofError(row.Before, strings.Replace(row.After, cut, "foreignSourceFactCut()", 1)) == nil {
				t.Fatalf("lost source fact/diagnostic/pump binding passed: %s", cut)
			}
		}
		assertions, err := family145DescriptorBusinessAssertions(row.After)
		if err != nil {
			t.Fatal(err)
		}
		for _, assertion := range assertions {
			condition := strings.SplitN(assertion, " => ", 2)[0]
			if family145SourceFactDiagnosticProofError(row.Before, strings.Replace(row.After, condition, "false", 1)) == nil {
				t.Fatalf("lost original source fact/no-progress assertion passed: %s", condition)
			}
		}
		original := family145A2ClaimLiteralQueries(t, row.Before)
		var migrated []string
		for _, tc := range []struct{ file, function, destination string }{
			{"internal/backend/runforkrevision/postgres.go", "ReadLatestFanOutObligationFactStorageTx", "runID).Scan(&out.Key, &out.Fact)"},
			{"internal/backend/pipelinepersistence/fan_out_semantic_storage.go", "ReadFanOutRunBlockedClaimStorageTx", "runID).Scan(&out.Diagnostic, &out.Released)"},
			{"internal/backend/runforkrevision/postgres.go", "ReadRunRevisionFactStorageTx", "runID, revision).Scan(&out.Family, &out.Key, &out.Fact, &out.Present)"},
		} {
			owner := family145ConversationForkOwnerSource(t, tc.file, tc.function)
			queries := family145A2ClaimLiteralQueries(t, owner)
			if len(queries) != 1 || strings.Count(owner, tc.destination) != 1 {
				t.Fatalf("%s changed one physical SQL cut or scan destination: %v", tc.function, queries)
			}
			migrated = append(migrated, queries[0])
		}
		if !reflect.DeepEqual(original, migrated) {
			t.Fatalf("fact/diagnostic owner changed original predicate, ordering, limit, bytes or NULL semantics: %v/%v", original, migrated)
		}
	}
	if matched != 1 {
		t.Fatalf("source diagnostic/fact caller inventory=%d/1", matched)
	}
	for _, tc := range []struct{ function, binding string }{
		{"ReadLatestFanOutObligationFactStorageForTest", "runforkrevision.ReadLatestFanOutObligationFactStorageTx(ctx, tx, runID)"},
		{"ReadFanOutRunBlockedClaimStorageForTest", "pipelinepersistence.ReadFanOutRunBlockedClaimStorageTx(ctx, tx, runID)"},
		{"ReadRunRevisionFactStorageForTest", "runforkrevision.ReadRunRevisionFactStorageTx(ctx, tx, runID, revision)"},
	} {
		owner := family145ConversationForkOwnerSource(t, "internal/runtimepersistence/test_reporter_storage_observation.go", tc.function)
		if strings.Count(owner, "readFanOutSemanticStorage(ctx, selected, []string{runID},") != 1 || strings.Count(owner, tc.binding) != 1 {
			t.Fatalf("%s lost original selected coordinator or exact storage coordinate", tc.function)
		}
	}
}
