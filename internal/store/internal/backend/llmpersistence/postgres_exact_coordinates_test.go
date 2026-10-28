package llmpersistence_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/platform"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/construction"
	"github.com/division-sh/swarm/internal/store/internal/backend/llmpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	"github.com/division-sh/swarm/internal/store/internal/schemastore"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

func TestPostgresLLMExactRevisionFactsUseStoredUUIDCoordinates(t *testing.T) {
	dsn, _, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	selected, db, err := construction.OpenPostgresRuntime(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := selected.Close(); err != nil {
			t.Error(err)
		}
	})
	backend, err := postgresbackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	var spec contracts.PlatformSpecDocument
	if err := yaml.Unmarshal(platform.PlatformSpecYAML(), &spec); err != nil {
		t.Fatal(err)
	}
	plans, err := schemastore.GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := selected.BootstrapSchema(ctx, schemastore.SchemaBootstrapRequest{
		PlatformPlans: plans,
		Origin:        schemastore.RuntimeStoreOrigin{SwarmVersion: "llm-coordinate-proof", PlatformVersion: spec.Platform.Version, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}
	runtimeID := uuid.NewString()
	setupCtx := authoractivity.WithScope(ctx, authoractivity.BundleScope(runtimeID, sourceartifactfixture.BundleHash))
	if _, err := selected.EnsureSourceArtifact(setupCtx, sourceartifactfixture.Artifact()); err != nil {
		t.Fatal(err)
	}
	process, err := selected.AcquireProcessCapability(setupCtx, startupownership.AcquireRequest{
		OwnerID: "llm-coordinate-fixture", BootID: uuid.NewString(), RuntimeInstanceID: runtimeID,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := process.Release(context.Background()); err != nil {
			t.Error(err)
		}
	})
	ctx, err = startupownership.WithProcessMutationAuthority(setupCtx, process)
	if err != nil {
		t.Fatal(err)
	}
	setupCtx = ctx
	rooted, _ := sourceartifactfixture.Rooted(t, "llm-proof", sourceartifactfixture.Artifact())
	if _, err := selected.EnsureRootedSource(setupCtx, rooted); err != nil {
		t.Fatal(err)
	}
	setupCtx = correlation.WithRootAdmission(correlation.WithSourceArtifactFact(setupCtx, sourceartifactfixture.Fact()), rooted.Admission())
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
				if _, err := selected.CreateRun(setupCtx, runlifecycle.CreateRequest{
					RunID: runID, Origin: runlifecycle.ScenarioSetupRunOrigin(), Source: sourceartifactfixture.Fact(), RootAdmission: rooted.Admission(), StartedAt: time.Now().UTC(),
				}); err != nil {
					t.Fatal(err)
				}
			}
			identity, err := agentidentity.New(spell(firstRunID), name, agentidentity.RootRoute())
			if err != nil {
				t.Fatal(err)
			}
			record := runtimellm.AgentTurnRecord{
				SessionID: spell(sessionID), RunID: identity.RunID, Identity: identity,
				AgentID: identity.AgentID(), FlowInstance: identity.FlowInstance(), Memory: agentmemory.Plan{},
			}
			owner := &llmpersistence.LLMPostgresOwner{}
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
