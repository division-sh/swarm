package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

type llmResetFixture struct {
	runs     []string
	live     map[string]sessions.ResetDisposition
	terminal []string
}

func seedLLMExactSession(t *testing.T, s exactFactStore, runID, agent, status string) (string, agentmemory.Identity) {
	t.Helper()
	ctx := testAuthorActivityContext()
	id := uuid.NewString()
	identity := mustTestAgentIdentityForRun(runID, agent, "global")
	fields := testAgentIdentityStorageFields(t, identity)
	seedTestAgentRow(t, ctx, s.db, s.postgres, identity, "active")
	at := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	var reason, terminated any
	if status == "terminated" {
		reason, terminated = "normal", at
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO agent_sessions (
		session_id,run_id,agent_id,agent_name_owner,agent_name_source,agent_route_presence,
		flow_scope_key,flow_instance_id,flow_instance,memory_enabled,memory_source,
		conversation,turn_count,runtime_state,status,termination_reason,terminated_at,created_at,updated_at
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,TRUE,'authored','[]',0,'{}',$10,$11,$12,$13,$13)`,
		id, runID, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath, status, reason, terminated, at)
	if err != nil {
		t.Fatal(err)
	}
	return id, identity
}

func newLLMResetFixture(t *testing.T, s exactFactStore) llmResetFixture {
	t.Helper()
	f := llmResetFixture{live: make(map[string]sessions.ResetDisposition)}
	for i := 0; i < 2; i++ {
		run := newExactFactFixture(t, s).runID
		f.runs = append(f.runs, run)
		effects := runforkrevision.NewEffects()
		for _, status := range []string{"active", "suspended", "terminated"} {
			id, identity := seedLLMExactSession(t, s, run, "reset-"+status, status)
			if status == "terminated" {
				// Deliberately unrevisioned, unaffected terminal fixture evidence:
				// a reset must neither mutate it nor capture its history incidentally.
				f.terminal = append(f.terminal, id)
				continue
			}
			f.live[id] = sessions.ResetDisposition{SessionID: id, RunID: run, AgentID: identity.AgentID(),
				FlowInstance: identity.FlowInstance(), PreviousStatus: status,
				TerminationReason: "orphaned", TerminationDetail: "exact-reset"}
			if err := effects.AddFact(run, runforkrevision.FamilyAgentSessions, id); err != nil {
				t.Fatal(err)
			}
		}
		exactTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
			if _, err := finalizeRunForkRevisionMatrix(ctx, tx, s.postgres, effects); err != nil {
				t.Fatal(err)
			}
		})
	}
	return f
}

func assertLLMResetSummary(t *testing.T, f llmResetFixture, summary sessions.ResetSummary) {
	t.Helper()
	got := make(map[string]sessions.ResetDisposition)
	for _, d := range summary.OrphanedSessions {
		if _, exists := got[d.SessionID]; exists {
			t.Fatalf("duplicate reset disposition after retry: %+v", d)
		}
		got[d.SessionID] = d
	}
	if !reflect.DeepEqual(got, f.live) {
		t.Fatalf("reset dispositions=%+v want=%+v", got, f.live)
	}
}

func llmTerminalSnapshot(t *testing.T, s exactFactStore, ids []string) map[string]string {
	t.Helper()
	result := make(map[string]string)
	for _, id := range ids {
		var status, reason, detail, terminated, updated, holder, expiry string
		if err := s.db.QueryRow(`SELECT status,COALESCE(termination_reason,''),COALESCE(termination_detail,''),
			COALESCE(CAST(terminated_at AS TEXT),''),CAST(updated_at AS TEXT),COALESCE(lease_holder,''),
			COALESCE(CAST(lease_expires_at AS TEXT),'') FROM agent_sessions WHERE session_id=$1`, id).
			Scan(&status, &reason, &detail, &terminated, &updated, &holder, &expiry); err != nil {
			t.Fatal(err)
		}
		result[id] = fmt.Sprint(status, reason, detail, terminated, updated, holder, expiry)
	}
	return result
}

func assertLLMResetHistory(t *testing.T, s exactFactStore, f llmResetFixture, reset bool) {
	t.Helper()
	for id, disposition := range f.live {
		wantStatus, wantFacts := disposition.PreviousStatus, 1
		if reset {
			wantStatus, wantFacts = "terminated", 2
		}
		var status string
		if err := s.db.QueryRow(`SELECT status FROM agent_sessions WHERE session_id=$1`, id).Scan(&status); err != nil || status != wantStatus {
			t.Fatalf("session %s status=%s want=%s err=%v", id, status, wantStatus, err)
		}
		var count int
		if err := s.db.QueryRow(`SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1 AND family='agent_sessions' AND fact_key=$2`, disposition.RunID, id).Scan(&count); err != nil || count != wantFacts {
			t.Fatalf("session %s history=%d want=%d err=%v", id, count, wantFacts, err)
		}
		var body []byte
		if err := s.db.QueryRow(`SELECT fact FROM run_fork_fact_revisions WHERE run_id=$1 AND family='agent_sessions' AND fact_key=$2 ORDER BY revision DESC LIMIT 1`, disposition.RunID, id).Scan(&body); err != nil || !strings.Contains(string(body), `"`+wantStatus+`"`) {
			t.Fatalf("session %s final history=%s err=%v", id, body, err)
		}
	}
	for _, id := range f.terminal {
		var count int
		if err := s.db.QueryRow(`SELECT COUNT(*) FROM run_fork_fact_revisions WHERE family='agent_sessions' AND fact_key=$1`, id).Scan(&count); err != nil || count != 0 {
			t.Fatalf("unaffected terminal %s captured: count=%d err=%v", id, count, err)
		}
	}
}

func registerLLMResetSink(t *testing.T, s exactFactStore, run string, submitted *[]runlifecycle.Candidate) {
	t.Helper()
	var bundle string
	if err := s.db.QueryRow(`SELECT bundle_hash FROM runs WHERE run_id=$1`, run).Scan(&bundle); err != nil {
		t.Fatal(err)
	}
	store := s.selected.(interface {
		RegisterCompletionCandidateSink(context.Context, runlifecycle.CandidateScope, runlifecycle.CandidateSink) (runlifecycle.CandidateRegistration, error)
	})
	registration, err := store.RegisterCompletionCandidateSink(testAuthorActivityContext(), runlifecycle.CandidateScope{BundleHash: bundle},
		&completionHandoffEvidenceProbeSink{submit: func(c runlifecycle.Candidate) error {
			*submitted = append(*submitted, c)
			return nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(registration.Release)
}

func TestLLMResetExactSessionsAndRollbackBothStores(t *testing.T) {
	eachExactFactStore(t, func(t *testing.T, s exactFactStore) {
		f := newLLMResetFixture(t, s)
		terminal := llmTerminalSnapshot(t, s, f.terminal)
		var submitted []runlifecycle.Candidate
		registerLLMResetSink(t, s, f.runs[0], &submitted)
		reset := s.selected.(interface {
			ResetAll(sessions.ResetMetadata) (sessions.ResetSummary, error)
		})
		if s.postgres {
			if _, err := s.db.Exec(`CREATE FUNCTION fail_llm_reset_revision() RETURNS trigger LANGUAGE plpgsql AS $$
				BEGIN RAISE EXCEPTION 'injected reset revision failure'; END $$`); err != nil {
				t.Fatal(err)
			}
			if _, err := s.db.Exec(`CREATE TRIGGER fail_llm_reset_revision BEFORE INSERT ON run_fork_fact_revisions
				FOR EACH ROW WHEN (NEW.family='agent_sessions') EXECUTE FUNCTION fail_llm_reset_revision()`); err != nil {
				t.Fatal(err)
			}
		} else if _, err := s.db.Exec(`CREATE TRIGGER fail_llm_reset_revision BEFORE INSERT ON run_fork_fact_revisions
			WHEN NEW.family='agent_sessions' BEGIN SELECT RAISE(ABORT,'injected reset revision failure'); END`); err != nil {
			t.Fatal(err)
		}
		failed, err := reset.ResetAll(sessions.ResetMetadata{Source: "exact-reset"})
		if err == nil || !strings.Contains(err.Error(), "injected reset revision failure") || len(failed.OrphanedSessions) != 0 || len(submitted) != 0 {
			t.Fatalf("rollback leaked summary/candidate: summary=%+v submitted=%+v err=%v", failed, submitted, err)
		}
		assertLLMResetHistory(t, s, f, false)
		drop := `DROP TRIGGER fail_llm_reset_revision`
		if s.postgres {
			drop += ` ON run_fork_fact_revisions`
		}
		if _, err := s.db.Exec(drop); err != nil {
			t.Fatal(err)
		}
		summary, err := reset.ResetAll(sessions.ResetMetadata{Source: "exact-reset"})
		if err != nil {
			t.Fatal(err)
		}
		assertLLMResetSummary(t, f, summary)
		assertLLMResetHistory(t, s, f, true)
		if got := llmTerminalSnapshot(t, s, f.terminal); !reflect.DeepEqual(got, terminal) {
			t.Fatalf("terminal evidence changed: got=%v want=%v", got, terminal)
		}
		seen := make(map[string]int)
		for _, c := range submitted {
			seen[c.RunID]++
		}
		if len(submitted) != 2 || seen[f.runs[0]] != 1 || seen[f.runs[1]] != 1 {
			t.Fatalf("completion handoff must deduplicate runs, not session effects: %+v", submitted)
		}
		replay, err := reset.ResetAll(sessions.ResetMetadata{Source: "exact-reset"})
		if err != nil || len(replay.OrphanedSessions) != 0 || len(submitted) != 2 {
			t.Fatalf("reset replay: summary=%+v candidates=%+v err=%v", replay, submitted, err)
		}
		assertLLMResetHistory(t, s, f, true)
	})
}
