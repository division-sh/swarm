package runtimepersistence

import (
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

func TestAgentRecoverySeedNativeInterruptionFencesLateSuccessBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cut := range []string{"read", "publication"} {
			t.Run(backend+"/"+cut, func(t *testing.T) {
				f := newAgentClaimProfileFixture(t, backend)
				f.claim(t)
				process, grant := agentRecoveryFixtureOwners(t, f)
				stamp, err := process.Evidence()
				if err != nil {
					t.Fatal(err)
				}
				before := profileFixtureSnapshot(t, f)
				if err := startupownership.InterruptAgentRecoverySeedSessionForTest(f.ctx, process, stamp); err == nil {
					t.Fatal("native interruption was available without its exact active seed cut")
				}
				entered, unblock := holdAgentRecoverySeedCut(t, process, cut)
				result := make(chan error, 1)
				go func() { result <- startupownership.SeedAgentRecovery(f.ctx, process) }()
				awaitAgentRecoverySeedCut(t, entered)
				foreign := stamp
				foreign.AuthorityID = uuid.NewString()
				if err := startupownership.InterruptAgentRecoverySeedSessionForTest(f.ctx, process, foreign); err == nil {
					t.Fatal("foreign stamp interrupted the held native seed")
				}
				if err := startupownership.InterruptAgentRecoverySeedSessionForTest(f.ctx, process, stamp); err != nil {
					t.Fatalf("interrupt exact original retained session: %v", err)
				}
				if err := startupownership.InterruptAgentRecoverySeedSessionForTest(f.ctx, process, stamp); err == nil {
					t.Fatal("native interruption was reusable")
				}
				select {
				case err := <-result:
					t.Fatalf("native interruption returned before the original read/close joined: %v", err)
				default:
				}
				unblock()
				seedErr := <-result
				if seedErr == nil || (backend == "sqlite" && !errors.Is(seedErr, sql.ErrConnDone)) {
					t.Fatalf("interrupted seed returned late success or lost its native error: %v", seedErr)
				}
				select {
				case <-process.Done():
				default:
					t.Fatal("native interruption did not fence the original process")
				}
				if _, err := grant.Evidence(); err == nil {
					t.Fatal("native interruption left its original grant live")
				}
				if err := startupownership.SeedAgentRecovery(f.ctx, process); err == nil || strings.Contains(err.Error(), "already installed") {
					t.Fatalf("terminal owner exposed an installed/partial seed instead of refusing authority: %v", err)
				}
				if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(before, after) {
					t.Fatal("native interruption/late refusal changed complete durable storage")
				}
				assertAgentProfileProcessNoAttempts(t, f)
				if backend == "sqlite" {
					t.Log("native loss is retained proof-connection failure, not release of the OS possession lock; original release/cleanup remains required")
				}
			})
		}
	}
}
