package pipeline_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestA2SourceRefusalObserversKeepExactFactRevisionAndClaimCutsBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		t.Run(backend.name, func(t *testing.T) {
			f := newA2CorruptMapSource(t, backend.open(t))
			initial := f.footprint(t)
			before, err := storetest.ReadSelectedForkApplicationStorageSnapshot(f.p.ctx, f.p.selected.events)
			if err != nil {
				t.Fatal(err)
			}
			probe := storetest.CollectTransactions(t, f.p.selected.events, storetest.TransactionProbeOptions{})
			original, err := storetest.ReadLatestFanOutObligationFactStorage(f.p.ctx, f.p.selected.events, f.p.runID)
			if err != nil || original.Key == "" || len(original.Fact) == 0 {
				t.Fatalf("genuine intent has no positive latest fact: %+v %v", original, err)
			}
			physical, err := storetest.ReadRunRevisionFactStorage(f.p.ctx, f.p.selected.events, f.p.runID, initial.LatestRevision)
			if err != nil || physical.Family == "" || physical.Key == "" || !physical.Present || len(physical.Fact) == 0 {
				t.Fatalf("genuine global revision lost its unfiltered physical witness: %+v %v", physical, err)
			}
			claim, err := storetest.ReadFanOutRunBlockedClaimStorage(f.p.ctx, f.p.selected.events, f.p.runID)
			if err != nil || claim.Released || len(claim.Diagnostic) != 0 {
				t.Fatalf("live genuine claim was marked released/blocked: %+v %v", claim, err)
			}
			if counts := probe.Snapshot(); counts.Active != 0 || counts.Total.Begun != 3 || counts.Total.ReadCommits != 3 || counts.Total.WriteCommits != 0 {
				t.Fatalf("initial fact/revision/claim cuts escaped original coordinator: %+v", counts)
			}
			probe.Stop()
			after, err := storetest.ReadSelectedForkApplicationStorageSnapshot(f.p.ctx, f.p.selected.events)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("initial source observations changed data/history/claim/clocks: %v", err)
			}
			if err := f.writeRaw([]byte("null")); err != nil {
				t.Fatal(err)
			}
			if result, err := f.handoff.owner.ReleaseFanOutClaim(f.p.ctx, f.claim); err != nil || !result.Acknowledged {
				t.Fatalf("release initial native claim before actual pump: %+v %v", result, err)
			}
			_, pumpErr := f.p.pc.ServeFanOutCandidate(f.p.ctx, f.handoff.owner, f.handoff.key)
			waitForGateRecoveryQuiescence(t, f.p.bus, f.p.ctx)
			failure, typed := runtimefailures.EnvelopeFromError(pumpErr)
			failureRaw, err := runtimefailures.MarshalEnvelope(failure)
			if pumpErr == nil || !typed || err != nil {
				t.Fatalf("actual pump did not publish its typed source refusal: %v %v", pumpErr, err)
			}
			final := f.footprint(t)
			before, err = storetest.ReadSelectedForkApplicationStorageSnapshot(f.p.ctx, f.p.selected.events)
			if err != nil {
				t.Fatal(err)
			}
			probe = storetest.CollectTransactions(t, f.p.selected.events, storetest.TransactionProbeOptions{})
			blocked, err := storetest.ReadFanOutRunBlockedClaimStorage(f.p.ctx, f.p.selected.events, f.p.runID)
			if err != nil || !blocked.Released || a2AccumulatorJSONHash(t, blocked.Diagnostic) != a2AccumulatorJSONHash(t, failureRaw) {
				t.Fatalf("blocked observer changed exact returned diagnostic/lease release: %+v %v", blocked, err)
			}
			latest, err := storetest.ReadLatestFanOutObligationFactStorage(f.p.ctx, f.p.selected.events, f.p.runID)
			if err != nil || latest.Key != original.Key {
				t.Fatalf("latest observer lost the exact existing intent key: %+v %v", latest, err)
			}
			revision, err := storetest.ReadRunRevisionFactStorage(f.p.ctx, f.p.selected.events, f.p.runID, final.LatestRevision)
			if err != nil || revision.Family != "fan_out_obligations" || revision.Key != original.Key || !revision.Present || !reflect.DeepEqual(revision.Fact, latest.Fact) {
				t.Fatalf("exact diagnostic revision borrowed another history row: %+v %v", revision, err)
			}
			var expected map[string]any
			if err := json.Unmarshal(original.Fact, &expected); err != nil {
				t.Fatal(err)
			}
			expected["status"], expected["blocked_reason"] = "blocked", string(blocked.Diagnostic)
			expectedRaw, err := json.Marshal(expected)
			if err != nil || a2AccumulatorJSONHash(t, revision.Fact) != a2AccumulatorJSONHash(t, expectedRaw) {
				t.Fatalf("observers lost complete original fact/diagnostic semantics: %s %v", revision.Fact, err)
			}
			if counts := probe.Snapshot(); counts.Active != 0 || counts.Total.Begun != 3 || counts.Total.ReadCommits != 3 || counts.Total.WriteCommits != 0 {
				t.Fatalf("final claim/fact/revision cuts escaped original coordinator: %+v", counts)
			}
			if got, err := storetest.ReadRunRevisionFactStorage(f.p.ctx, f.p.selected.events, f.p.runID, final.LatestRevision+1000); !errors.Is(err, sql.ErrNoRows) || !reflect.DeepEqual(got, storetest.RunRevisionFactStorage{}) {
				t.Fatalf("missing exact revision borrowed the latest fact: %+v %v", got, err)
			}
			cancelled, cancel := context.WithCancel(f.p.ctx)
			cancel()
			if got, err := storetest.ReadLatestFanOutObligationFactStorage(cancelled, f.p.selected.events, f.p.runID); !errors.Is(err, context.Canceled) || !reflect.DeepEqual(got, storetest.FanOutObligationHistoryStorage{}) {
				t.Fatalf("cancelled latest fact returned partial evidence: %+v %v", got, err)
			}
			if got, err := storetest.ReadRunRevisionFactStorage(cancelled, f.p.selected.events, f.p.runID, final.LatestRevision); !errors.Is(err, context.Canceled) || !reflect.DeepEqual(got, storetest.RunRevisionFactStorage{}) {
				t.Fatalf("cancelled exact revision returned partial evidence: %+v %v", got, err)
			}
			if got, err := storetest.ReadFanOutRunBlockedClaimStorage(cancelled, f.p.selected.events, f.p.runID); !errors.Is(err, context.Canceled) || !reflect.DeepEqual(got, storetest.FanOutRunBlockedClaimStorage{}) {
				t.Fatalf("cancelled blocked claim returned partial evidence: %+v %v", got, err)
			}
			probe.Stop()
			after, err = storetest.ReadSelectedForkApplicationStorageSnapshot(f.p.ctx, f.p.selected.events)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("blocked/revision observations changed application/history/lease/clocks: %v", err)
			}
		})
	}
}
