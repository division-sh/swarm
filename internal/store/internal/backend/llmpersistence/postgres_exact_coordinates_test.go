package llmpersistence

import (
	"context"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestPostgresLLMExactRevisionFactsUseStoredUUIDCoordinates(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	backend, err := postgresbackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	name, err := agentidentity.DeclaredName("uuid-agent", "uuid-owner")
	if err != nil {
		t.Fatal(err)
	}
	for _, spelling := range []string{"canonical", "uppercase", "compact"} {
		t.Run(spelling, func(t *testing.T) {
			spell := func(id string) string {
				switch spelling {
				case "uppercase":
					return strings.ToUpper(id)
				case "compact":
					return strings.ReplaceAll(id, "-", "")
				default:
					return id
				}
			}
			sessionID, firstRunID, nextRunID := uuid.NewString(), uuid.NewString(), uuid.NewString()
			for _, runID := range []string{firstRunID, nextRunID} {
				if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id,status,bundle_hash,origin_kind) VALUES ($1::uuid,'running',$2,'scenario_setup')`, runID, "bundle-v2:sha256:"+strings.Repeat("e", 64)); err != nil {
					t.Fatal(err)
				}
			}
			identity, err := agentidentity.New(spell(firstRunID), name, agentidentity.RootRoute())
			if err != nil {
				t.Fatal(err)
			}
			record := runtimellm.AgentTurnRecord{
				SessionID: spell(sessionID), RunID: identity.RunID, Identity: identity,
				AgentID: identity.AgentID(), FlowInstance: identity.FlowInstance(), Memory: agentmemory.PlatformDefault(),
			}
			owner := &LLMPostgresOwner{}
			write := func() error {
				result := mutationprotocol.RunPostgres(ctx, backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
					return struct{}{}, owner.EnsureCompletionTurnMemoryTx(ctx, attempt, record)
				})
				return result.Err()
			}
			if err := write(); err != nil {
				t.Fatalf("first audit: %v", err)
			}
			if err := write(); err != nil {
				t.Fatalf("same-owner audit: %v", err)
			}
			record.Identity.RunID, record.RunID = spell(nextRunID), spell(nextRunID)
			if err := write(); err != nil {
				t.Fatalf("moved audit: %v", err)
			}
			var storedSessionID, storedRunID string
			var turns int
			if err := db.QueryRowContext(ctx, `SELECT session_id::text, run_id::text, turn_count FROM agent_conversation_audits WHERE session_id=$1::uuid`, sessionID).Scan(&storedSessionID, &storedRunID, &turns); err != nil {
				t.Fatal(err)
			}
			if storedSessionID != sessionID || storedRunID != nextRunID || turns != 3 {
				t.Fatalf("audit readback: session=%s run=%s turns=%d", storedSessionID, storedRunID, turns)
			}
			for _, check := range []struct {
				runID   string
				present bool
			}{{firstRunID, false}, {nextRunID, true}} {
				var factKey string
				var present bool
				err := db.QueryRowContext(ctx, `SELECT fact_key,present FROM run_fork_fact_revisions WHERE run_id=$1::uuid AND family='agent_conversation_audits' ORDER BY revision DESC LIMIT 1`, check.runID).Scan(&factKey, &present)
				if err != nil || factKey != sessionID || present != check.present {
					t.Fatalf("revision coordinate run=%s: key=%s present=%v err=%v", check.runID, factKey, present, err)
				}
			}
		})
	}
}
