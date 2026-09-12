package runtimepersistence

import (
	"errors"
	"testing"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
)

func TestSQLiteSessionMutationRetainsAcknowledgedHandoffOutcome(t *testing.T) {
	for _, operation := range []string{"acquire", "rotate", "release", "adopt", "reset"} {
		for _, phase := range []string{"healthy", "handoff_failure"} {
			t.Run(operation+"/"+phase, func(t *testing.T) {
				store := newBootstrappedSQLiteRuntimeStoreForTest(t)
				fixture := newCompletionSettlementFixture(t, store, store.backend.ConstructionHandle(), true)
				ctx := runtimeeffects.WithDifferentOwner(testAuthorActivityContext(), runtimeeffects.OwnerBuildTestInfrastructure)
				identity := fixture.authority.Target.AgentIdentity
				var bundleHash string
				if err := fixture.db.QueryRowContext(ctx, `SELECT bundle_hash FROM runs WHERE run_id=?`, identity.RunID).Scan(&bundleHash); err != nil {
					t.Fatal(err)
				}
				injected := errors.New("injected sqlite session postcommit handoff failure")
				submits := 0
				sink := &completionHandoffEvidenceProbeSink{submit: func(candidate runtimerunlifecycle.Candidate) error {
					submits++
					if candidate.RunID != identity.RunID {
						t.Errorf("foreign candidate: %+v", candidate)
					}
					if phase == "handoff_failure" {
						return injected
					}
					return nil
				}}
				registration, err := store.RegisterCompletionCandidateSink(ctx, runtimerunlifecycle.CandidateScope{BundleHash: bundleHash}, sink)
				if err != nil {
					t.Fatal(err)
				}
				defer registration.Release()
				var lease *runtimesessions.Lease
				var summary runtimesessions.ResetSummary
				switch operation {
				case "acquire":
					var recordSessionID string
					acquired, record, acquireErr := store.AcquireLiveSession(ctx, identity, fixture.leaseHolder)
					lease, err = acquired, acquireErr
					recordSessionID = record.SessionID
					if recordSessionID != fixture.sessionID {
						t.Fatalf("lost committed conversation: %+v", record)
					}
				case "rotate":
					lease, err = store.Rotate(ctx, identity, fixture.leaseHolder, runtimesessions.RotationMetadata{RetryReason: "outcome-proof"})
				case "release":
					err = store.Release(ctx, &runtimesessions.Lease{SessionID: fixture.sessionID, Identity: identity, LockOwner: fixture.leaseHolder})
				case "adopt":
					err = store.AdoptSessionID(ctx, identity, fixture.leaseHolder, "provider-outcome")
				case "reset":
					summary, err = store.ResetAll(runtimesessions.ResetMetadata{Source: "outcome-proof"})
				}
				if phase == "handoff_failure" && !errors.Is(err, injected) || phase == "healthy" && err != nil {
					t.Fatalf("unexpected handoff error: %v", err)
				}
				if submits != 1 {
					t.Fatalf("handoff submissions=%d want=1", submits)
				}
				var status, holder, provider, successor string
				if err := fixture.db.QueryRowContext(ctx, `SELECT status, COALESCE(lease_holder,''), COALESCE(json_extract(runtime_state,'$.provider_session_id'),''), COALESCE(successor_session_id,'') FROM agent_sessions WHERE session_id=?`, fixture.sessionID).Scan(&status, &holder, &provider, &successor); err != nil {
					t.Fatal(err)
				}
				switch operation {
				case "acquire":
					if lease == nil || lease.SessionID != fixture.sessionID || holder != fixture.leaseHolder || status != "active" {
						t.Fatalf("lost acquired lease: %+v status=%s holder=%s", lease, status, holder)
					}
				case "rotate":
					if lease == nil || lease.SessionID != successor || successor == "" || status != "terminated" || holder != "" {
						t.Fatalf("lost successor lease: %+v successor=%s status=%s holder=%s", lease, successor, status, holder)
					}
					var active int
					if err := fixture.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_sessions WHERE session_id=? AND status='active' AND lease_holder=?`, lease.SessionID, fixture.leaseHolder).Scan(&active); err != nil || active != 1 {
						t.Fatalf("successor count=%d error=%v", active, err)
					}
				case "release":
					if holder != "" || status != "active" {
						t.Fatalf("lease not durably released: status=%s holder=%s", status, holder)
					}
				case "adopt":
					if provider != "provider-outcome" {
						t.Fatalf("provider session=%s", provider)
					}
				case "reset":
					if len(summary.OrphanedSessions) != 1 || summary.OrphanedSessions[0].SessionID != fixture.sessionID || status != "terminated" || holder != "" {
						t.Fatalf("lost reset summary=%+v status=%s holder=%s", summary, status, holder)
					}
				}
			})
		}
	}
}
