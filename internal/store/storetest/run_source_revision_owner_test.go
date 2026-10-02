package storetest

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type sourceRevisionProofStore interface {
	runtimerunlifecycle.OperationOwner
	runtimerunlifecycle.CandidateStore
	runtimebus.RunLifecycleReadPersistence
	sourceartifactfixture.Writer
}

func TestSourceRevisionFixtureUsesExactSelectedOwner(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) sourceRevisionProofStore
	}{
		{"sqlite", func(t *testing.T) sourceRevisionProofStore { return StartSQLiteRuntimeStore(t) }},
		{"postgres", func(t *testing.T) sourceRevisionProofStore {
			_, db, _ := testutil.StartPostgres(t)
			return AdmitPostgresRuntimeStore(t, db)
		}},
	} {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t)
			ctx := context.Background()
			original := sourceartifactfixture.Require(t, ctx, selected)
			ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(uuid.NewString(), original.BundleHash()))
			changed := sourceartifactfixture.RequireArtifact(t, ctx, selected,
				sourceartifactfixture.New("agents.yaml", []byte("agents: {revised: {}}\n")))
			runID := uuid.NewString()
			RequireRun(t, ctx, selected, RunFixture{RunID: runID, Origin: ScenarioSetupOrigin()})
			before, err := selected.LoadRunLifecycleSnapshot(ctx, runID)
			if err != nil {
				t.Fatal(err)
			}
			predecessor, err := selected.ListCompletionCandidates(ctx, runtimerunlifecycle.CandidateScope{BundleHash: original.BundleHash()}, runtimerunlifecycle.CandidateCursor{}, 128)
			if err != nil || len(predecessor.Candidates) != 1 {
				t.Fatalf("initial candidate=%+v err=%v", predecessor, err)
			}
			collector := CollectTransactions(t, selected, TransactionProbeOptions{})
			request := runtimerunlifecycle.SourceRevisionRequest{RunID: runID, Source: changed}
			disposition, err := selected.ReviseRunSource(ctx, request)
			if err != nil || disposition != runtimerunlifecycle.MutationApplied {
				t.Fatalf("revise source: disposition=%s err=%v", disposition, err)
			}
			if proof := collector.Snapshot(); proof.Total.WriteCommits != 1 || proof.Active != 0 {
				t.Fatalf("source revision bypassed the original selected coordinator: %+v", proof)
			}
			after, err := selected.LoadRunLifecycleSnapshot(ctx, runID)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("source revision changed unrelated run facts: before=%+v after=%+v err=%v", before, after, err)
			}
			actual, err := selected.RequirePresentRunSource(ctx, runID)
			if err != nil || !actual.Matches(changed) {
				t.Fatalf("source revision readback=%v err=%v", actual, err)
			}
			candidate, err := selected.ListCompletionCandidates(ctx, runtimerunlifecycle.CandidateScope{BundleHash: changed.BundleHash()}, runtimerunlifecycle.CandidateCursor{}, 128)
			if err != nil || len(candidate.Candidates) != 1 || candidate.Candidates[0].RunID != runID || candidate.Candidates[0].Revision <= predecessor.Candidates[0].Revision {
				t.Fatalf("revision did not atomically rearm the new source candidate: %+v err=%v", candidate, err)
			}
			old, err := selected.ListCompletionCandidates(ctx, runtimerunlifecycle.CandidateScope{BundleHash: original.BundleHash()}, runtimerunlifecycle.CandidateCursor{}, 128)
			if err != nil || len(old.Candidates) != 0 {
				t.Fatalf("revision left a candidate in the predecessor source scope: %+v err=%v", old, err)
			}
			disposition, err = selected.ReviseRunSource(ctx, request)
			if err != nil || disposition != runtimerunlifecycle.MutationExactNoop {
				t.Fatalf("exact revision replay: disposition=%s err=%v", disposition, err)
			}
			replayed, err := selected.ListCompletionCandidates(ctx, runtimerunlifecycle.CandidateScope{BundleHash: changed.BundleHash()}, runtimerunlifecycle.CandidateCursor{}, 128)
			if err != nil || !reflect.DeepEqual(candidate, replayed) {
				t.Fatalf("exact replay changed completion facts: before=%+v after=%+v err=%v", candidate, replayed, err)
			}
			for _, refusal := range []struct {
				name    string
				ctx     context.Context
				request runtimerunlifecycle.SourceRevisionRequest
			}{
				{"missing_artifact", ctx, runtimerunlifecycle.SourceRevisionRequest{RunID: runID, Source: sourceartifactfixture.FactFor(sourceartifactfixture.New("missing.yaml", []byte("absent")))}},
				{"missing_run", ctx, runtimerunlifecycle.SourceRevisionRequest{RunID: uuid.NewString(), Source: changed}},
				{"invalid_source", ctx, runtimerunlifecycle.SourceRevisionRequest{RunID: runID}},
			} {
				t.Run(refusal.name, func(t *testing.T) {
					prior := collector.Snapshot()
					if _, err := selected.ReviseRunSource(refusal.ctx, refusal.request); err == nil {
						t.Fatal("invalid revision unexpectedly succeeded")
					}
					proof := collector.Snapshot()
					if proof.Total.WriteCommits != prior.Total.WriteCommits || proof.Active != 0 {
						t.Fatalf("refused revision committed or retained work: before=%+v after=%+v", prior, proof)
					}
				})
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			priorCancellation := collector.Snapshot()
			if _, err := selected.ReviseRunSource(canceled, request); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled revision lost its cause: %v", err)
			}
			unchangedCandidate, err := selected.ListCompletionCandidates(ctx, runtimerunlifecycle.CandidateScope{BundleHash: changed.BundleHash()}, runtimerunlifecycle.CandidateCursor{}, 128)
			proofAfterCancellation := collector.Snapshot()
			if err != nil || !reflect.DeepEqual(candidate, unchangedCandidate) || proofAfterCancellation.Total.WriteCommits != priorCancellation.Total.WriteCommits || proofAfterCancellation.Active != 0 {
				t.Fatalf("refusal/cancellation changed candidate facts or committed: candidates=%+v err=%v proof=%+v", unchangedCandidate, err, proofAfterCancellation)
			}
			if _, _, err := selected.MarkTerminalRun(ctx, runtimerunlifecycle.TerminalRequest{RunID: runID, State: runtimerunlifecycle.StateCancelled, EndedAt: time.Now().UTC()}); err != nil {
				t.Fatal(err)
			}
			terminal, err := selected.LoadRunLifecycleSnapshot(ctx, runID)
			if err != nil {
				t.Fatal(err)
			}
			prior := collector.Snapshot()
			if _, err := selected.ReviseRunSource(ctx, runtimerunlifecycle.SourceRevisionRequest{RunID: runID, Source: original}); err == nil {
				t.Fatal("terminal source revision unexpectedly succeeded")
			}
			actual, err = selected.RequirePresentRunSource(ctx, runID)
			unchanged, snapshotErr := selected.LoadRunLifecycleSnapshot(ctx, runID)
			proof := collector.Snapshot()
			if err != nil || !actual.Matches(changed) || snapshotErr != nil || !reflect.DeepEqual(terminal, unchanged) || proof.Total.WriteCommits != prior.Total.WriteCommits || proof.Active != 0 {
				t.Fatalf("refusal changed terminal/source facts or committed: source=%v err=%v snapshot=%+v err=%v proof=%+v", actual, err, unchanged, snapshotErr, proof)
			}
		})
	}
}
