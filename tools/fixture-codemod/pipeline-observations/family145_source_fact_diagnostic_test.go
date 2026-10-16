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
