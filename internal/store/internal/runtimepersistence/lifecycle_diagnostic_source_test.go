package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestLifecycleDiagnosticDisjointSourcesIgnoreAmbientAuthority(t *testing.T) {
	for _, sqlite := range []bool{true, false} {
		name := "postgres"
		if sqlite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			var selected diagnosticProjectionTestStore
			var db *sql.DB
			if sqlite {
				s := newBootstrappedSQLiteRuntimeStoreForTest(t)
				selected, db = s, s.backend.ConstructionHandle()
			} else {
				_, db, _ = testutil.StartPostgres(t)
				selected = admitTestPostgresStore(t, db)
			}
			want := map[string]string{}
			for _, sourceName := range []string{"diagnostic-source-left", "diagnostic-source-right"} {
				artifact := storeTestSourceArtifact(sourceName)
				seedStoreTestPersistedArtifact(t, db, artifact)
				source := mustStoreTestSourceArtifactFact(artifact.BundleHash())
				runID := uuid.NewString()
				identity := mustTestAgentIdentityForRun(runID, "same-worker", "global")
				config := testOperatorAgentConfigForIdentity(identity, "worker")
				config.ExecutionMode = executionmode.Live
				config = withRuntimePersistenceTestIntent(t, config)
				if err := agentfixture.UpsertStaticForSource(t, testAuthorActivityContextForBundle(artifact.BundleHash()), selected, runtimemanager.PersistedAgent{
					Config: config, Status: "active", StartedAt: time.Now().UTC(),
				}, source); err != nil {
					t.Fatal(err)
				}
				want[runID] = source.BundleHash()
			}
			pending, err := selected.ListPendingAgentLifecycleDiagnostics(context.Background(), 100)
			if err != nil || len(pending) < 2 {
				t.Fatalf("pending=%d error=%v", len(pending), err)
			}
			// Caller facts name neither subject, and must not choose attribution or mode.
			foreign := runtimecorrelation.WithRunID(testAuthorActivityContextForBundle(authorActivityTestBundleHash), uuid.NewString())
			if _, err := projectDiagnosticTestBatch(foreign, selected, 100); err != nil {
				t.Fatal(err)
			}
			seen := map[string]bool{}
			for _, item := range pending {
				var runID, mode string
				var parent sql.NullString
				var payload []byte
				if err := db.QueryRow(`SELECT run_id,execution_mode,source_event_id,payload FROM events WHERE event_id=$1`, diagnosticEventID(item.OutboxID)).Scan(&runID, &mode, &parent, &payload); err != nil {
					t.Fatal(err)
				}
				var log struct {
					Details struct {
						runtimemanager.AgentLifecycleTransitionResult
					} `json:"details"`
				}
				if err := json.Unmarshal(payload, &log); err != nil {
					t.Fatal(err)
				}
				if runID != item.Identity.RunID || want[runID] == "" || log.Details.ProcessBinding.BundleHash != want[runID] || mode != "live" || parent.Valid {
					t.Fatalf("borrowed attribution run=%s source=%s mode=%s parent=%v", runID, log.Details.ProcessBinding.BundleHash, mode, parent)
				}
				assertDiagnosticCounts(t, db, item.OutboxID, 1, 0)
				seen[runID] = true
			}
			if len(seen) != 2 {
				t.Fatalf("source coverage=%d", len(seen))
			}
		})
	}
}
