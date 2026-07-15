package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type neutralEffectParityStore interface {
	runtimeeffects.Store
	runtimeeffects.RecoveryStore
}

type neutralEffectParityFixture struct {
	store     neutralEffectParityStore
	db        *sql.DB
	sqlite    bool
	authority runtimeeffects.Authority
	ctx       context.Context
}

var neutralEffectPrimitiveCounts = map[string]int{
	"authored_http_tool":       1,
	"managed_credential":       2,
	"native_web_search":        1,
	"mcp_tools_call_http":      1,
	"mcp_tools_call_stdio":     1,
	"native_bash":              1,
	"native_read_file":         1,
	"native_write_file":        7,
	"tool_result_relay":        7,
	"claude_tool_result_relay": 1,
}

func TestRuntimeEffectsNeutralSchemaRegisteredAdapterParity(t *testing.T) {
	t.Run("sqlite", func(t *testing.T) {
		store := newBootstrappedSQLiteRuntimeStoreForTest(t)
		proveRuntimeEffectsNeutralSchemaRegisteredAdapterParity(t, newNeutralEffectParityFixture(t, store, store.DB, true))
	})
	t.Run("postgres", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		proveRuntimeEffectsNeutralSchemaRegisteredAdapterParity(t, newNeutralEffectParityFixture(t, &PostgresStore{DB: db}, db, false))
	})
}

func proveRuntimeEffectsNeutralSchemaRegisteredAdapterParity(t *testing.T, fixture neutralEffectParityFixture) {
	t.Helper()
	requireLegacyEffectTablesAbsent(t, fixture)
	registrations := nonCompletionRegistrationsForParity(t)
	controller := runtimeeffects.NewController(fixture.store)
	for _, registration := range registrations {
		registration := registration
		t.Run(registration.Adapter, func(t *testing.T) {
			for primitiveIndex, primitiveKey := range registration.PrimitiveKeys {
				logicalID := fmt.Sprintf("neutral:%s:primitive:%d", registration.Adapter, primitiveIndex)
				ctx := runtimeeffects.WithLogicalOperationIdentity(fixture.ctx, logicalID)
				handle, err := runtimeeffects.Begin(ctx, registration.Adapter, []byte(primitiveKey), map[string]string{"primitive": primitiveKey})
				if err != nil {
					t.Fatalf("authorize %s: %v", primitiveKey, err)
				}
				if err := handle.MarkLaunched(ctx); err != nil {
					t.Fatalf("launch %s: %v", primitiveKey, err)
				}
				if err := handle.MarkResponseObserved(ctx, map[string]any{"primitive": primitiveKey, "stage": "observed"}); err != nil {
					t.Fatalf("observe %s: %v", primitiveKey, err)
				}
				if err := handle.Succeed(ctx, map[string]any{"primitive": primitiveKey, "terminal": "original"}); err != nil {
					t.Fatalf("settle %s: %v", primitiveKey, err)
				}
				if err := handle.Succeed(ctx, map[string]any{"primitive": primitiveKey, "terminal": "changed"}); err != nil {
					t.Fatalf("replay settlement %s: %v", primitiveKey, err)
				}
				requireNeutralAttemptEvidence(t, fixture, handle.Attempt(), primitiveKey)
				if _, err := runtimeeffects.Begin(ctx, registration.Adapter, []byte(primitiveKey), map[string]string{"primitive": primitiveKey}); err == nil {
					t.Fatalf("terminal %s replay was admitted", primitiveKey)
				}
				if _, err := runtimeeffects.Begin(ctx, registration.Adapter, []byte(primitiveKey+":changed"), map[string]string{"primitive": primitiveKey}); err == nil {
					t.Fatalf("changed request for %s was admitted", primitiveKey)
				}
			}

			authorized := beginNeutralRecoveryAttempt(t, fixture, registration.Adapter, "authorized", false, false)
			launched := beginNeutralRecoveryAttempt(t, fixture, registration.Adapter, "launched", true, false)
			observed := beginNeutralRecoveryAttempt(t, fixture, registration.Adapter, "response-observed", true, true)
			summary, err := fixture.store.ReconcileExternalEffectAttempts(context.Background(), time.Now().UTC().Add(time.Minute))
			if err != nil {
				t.Fatalf("reconcile %s attempts: %v", registration.Adapter, err)
			}
			if summary.PrelaunchTerminal != 1 || summary.OutcomeUncertain != 2 {
				t.Fatalf("%s recovery summary=%#v, want 1 terminal/2 uncertain", registration.Adapter, summary)
			}
			requireExternalAttemptState(t, fixture.db, fixture.sqlite, authorized.Attempt().AttemptID, runtimeeffects.StateTerminalFailure)
			requireExternalAttemptState(t, fixture.db, fixture.sqlite, launched.Attempt().AttemptID, runtimeeffects.StateOutcomeUncertain)
			requireExternalAttemptState(t, fixture.db, fixture.sqlite, observed.Attempt().AttemptID, runtimeeffects.StateOutcomeUncertain)
			requireRecoveredExternalEffectStory(t, fixture, authorized.Attempt().AttemptID, runtimeeffects.StateTerminalFailure)
			requireRecoveredExternalEffectStory(t, fixture, launched.Attempt().AttemptID, runtimeeffects.StateOutcomeUncertain)
			requireRecoveredExternalEffectStory(t, fixture, observed.Attempt().AttemptID, runtimeeffects.StateOutcomeUncertain)

			proveCompletionOnlyAuthorityRejectsRegistration(t, fixture, controller, registration, runtimeeffects.AuthoritySelectedContractFork)
			proveCompletionOnlyAuthorityRejectsRegistration(t, fixture, controller, registration, runtimeeffects.AuthorityConversationForkChat)
		})
	}
}

func requireRecoveredExternalEffectStory(t *testing.T, fixture neutralEffectParityFixture, attemptID string, state runtimeeffects.State) {
	t.Helper()
	query := `SELECT COUNT(*) FROM author_activity_occurrences WHERE source_owner = ? AND source_identity = ? AND transition = ?`
	if !fixture.sqlite {
		query = `SELECT COUNT(*) FROM author_activity_occurrences WHERE source_owner = $1 AND source_identity = $2 AND transition = $3`
	}
	var count int
	if err := fixture.db.QueryRow(query, "runtime_external_effect_attempts", attemptID+":"+string(state), string(state)).Scan(&count); err != nil {
		t.Fatalf("count recovered external effect story: %v", err)
	}
	if count != 1 {
		t.Fatalf("recovered external effect story count=%d, want 1 for %s/%s", count, attemptID, state)
	}
}

func nonCompletionRegistrationsForParity(t *testing.T) []runtimeeffects.Registration {
	t.Helper()
	actual := make(map[string]int)
	var registrations []runtimeeffects.Registration
	for _, registration := range runtimeeffects.Registrations() {
		if registration.Kind == runtimeeffects.KindProviderTurn {
			continue
		}
		if _, duplicate := actual[registration.Adapter]; duplicate {
			t.Fatalf("duplicate non-completion registration %q", registration.Adapter)
		}
		actual[registration.Adapter] = len(registration.PrimitiveKeys)
		registrations = append(registrations, registration)
	}
	if len(actual) != len(neutralEffectPrimitiveCounts) {
		t.Fatalf("non-completion registration count=%d, classified=%d", len(actual), len(neutralEffectPrimitiveCounts))
	}
	for adapter, wantPrimitives := range neutralEffectPrimitiveCounts {
		if got, ok := actual[adapter]; !ok || got != wantPrimitives {
			t.Fatalf("registration %q primitive count=%d present=%v, want %d", adapter, got, ok, wantPrimitives)
		}
	}
	for adapter := range actual {
		if _, classified := neutralEffectPrimitiveCounts[adapter]; !classified {
			t.Fatalf("non-completion registration %q is not classified by the neutral-schema proof", adapter)
		}
	}
	return registrations
}

func newNeutralEffectParityFixture(t *testing.T, store neutralEffectParityStore, db *sql.DB, sqlite bool) neutralEffectParityFixture {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC()
	agentID := "neutral-effect-parity-agent"
	if sqlite {
		if _, err := db.ExecContext(ctx, `INSERT INTO agents (agent_id,flow_instance,role,model,llm_backend,memory_enabled,memory_source,status,lifecycle_runtime_epoch,lifecycle_generation,lifecycle_phase,created_at) VALUES (?,'neutral','worker','regular','mock',0,'platform_default','active',7,3,'running',?)`, agentID, now); err != nil {
			t.Fatalf("seed neutral effect agent: %v", err)
		}
	} else {
		if _, err := db.ExecContext(ctx, `INSERT INTO agents (agent_id,flow_instance,role,model,llm_backend,memory_enabled,memory_source,status,lifecycle_runtime_epoch,lifecycle_generation,lifecycle_phase,created_at) VALUES ($1,'neutral','worker','regular','mock',FALSE,'platform_default','active',7,3,'running',$2)`, agentID, now); err != nil {
			t.Fatalf("seed neutral effect agent: %v", err)
		}
	}
	token := runtimeeffects.LifecycleToken{RuntimeEpoch: 7, AgentID: agentID, Generation: 3}
	authority := runtimeeffects.NormalAgentAuthority(token, fmt.Sprintf("agent:%s:%d:%d", agentID, token.RuntimeEpoch, token.Generation), now.Add(5*time.Minute))
	ctx = runtimeeffects.WithController(runtimeeffects.WithLifecycleToken(ctx, token), runtimeeffects.NewController(store))
	return neutralEffectParityFixture{store: store, db: db, sqlite: sqlite, authority: authority, ctx: ctx}
}

func beginNeutralRecoveryAttempt(t *testing.T, fixture neutralEffectParityFixture, adapter, suffix string, launch, observe bool) *runtimeeffects.Handle {
	t.Helper()
	ctx := runtimeeffects.WithLogicalOperationIdentity(fixture.ctx, "neutral:"+adapter+":recovery:"+suffix)
	handle, err := runtimeeffects.Begin(ctx, adapter, []byte(suffix), nil)
	if err != nil {
		t.Fatalf("authorize %s recovery %s: %v", adapter, suffix, err)
	}
	if launch {
		if err := handle.MarkLaunched(ctx); err != nil {
			t.Fatalf("launch %s recovery %s: %v", adapter, suffix, err)
		}
	}
	if observe {
		if err := handle.MarkResponseObserved(ctx, map[string]any{"recovery": suffix}); err != nil {
			t.Fatalf("observe %s recovery %s: %v", adapter, suffix, err)
		}
	}
	return handle
}

func requireNeutralAttemptEvidence(t *testing.T, fixture neutralEffectParityFixture, attempt runtimeeffects.Attempt, primitiveKey string) {
	t.Helper()
	query := `SELECT o.authority_kind,o.authority_id,a.adapter,a.state,a.evidence FROM runtime_external_effect_operations o JOIN runtime_external_effect_attempts a ON a.operation_id=o.operation_id WHERE a.attempt_id=?`
	if !fixture.sqlite {
		query = `SELECT o.authority_kind,o.authority_id,a.adapter,a.state,a.evidence::text FROM runtime_external_effect_operations o JOIN runtime_external_effect_attempts a ON a.operation_id=o.operation_id WHERE a.attempt_id=$1::uuid`
	}
	var authorityKind, authorityID, adapter, state, raw string
	if err := fixture.db.QueryRow(query, attempt.AttemptID).Scan(&authorityKind, &authorityID, &adapter, &state, &raw); err != nil {
		t.Fatalf("load neutral attempt evidence: %v", err)
	}
	var evidence map[string]any
	if err := json.Unmarshal([]byte(raw), &evidence); err != nil {
		t.Fatalf("decode neutral attempt evidence: %v", err)
	}
	if authorityKind != string(runtimeeffects.AuthorityNormalAgent) || authorityID != fixture.authority.ID || adapter != attempt.Adapter || state != string(runtimeeffects.StateSettled) || evidence["primitive"] != primitiveKey || evidence["terminal"] != "original" {
		t.Fatalf("neutral attempt row authority=%s/%s adapter=%s state=%s evidence=%v", authorityKind, authorityID, adapter, state, evidence)
	}
}

func proveCompletionOnlyAuthorityRejectsRegistration(t *testing.T, fixture neutralEffectParityFixture, controller *runtimeeffects.Controller, registration runtimeeffects.Registration, kind runtimeeffects.AuthorityKind) {
	t.Helper()
	authority := completionOnlyAuthorityForParity(kind)
	operationID := uuid.NewString()
	ctx := runtimeeffects.WithAuthority(context.Background(), authority)
	_, err := controller.Authorize(ctx, runtimeeffects.AuthorizeRequest{
		OperationID: operationID, Adapter: registration.Adapter,
		RequestFingerprint: runtimeeffects.Fingerprint([]byte("hostile:" + registration.Adapter + ":" + string(kind))),
	})
	if err == nil {
		t.Fatalf("%s authority admitted non-provider adapter %s", kind, registration.Adapter)
	}
	placeholder := "?"
	if !fixture.sqlite {
		placeholder = "$1::uuid"
	}
	var count int
	if err := fixture.db.QueryRow(`SELECT COUNT(*) FROM runtime_external_effect_operations WHERE operation_id=`+placeholder, operationID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("%s rejection persisted launchable operation count=%d err=%v", kind, count, err)
	}
}

func completionOnlyAuthorityForParity(kind runtimeeffects.AuthorityKind) runtimeeffects.Authority {
	now := time.Now().UTC()
	switch kind {
	case runtimeeffects.AuthoritySelectedContractFork:
		executionID := uuid.NewString()
		return runtimeeffects.Authority{
			Kind: kind, ID: executionID, ExecutionOwner: "hostile-selected", LeaseExpiresAt: now.Add(time.Minute), FenceGeneration: 1,
			SelectedFork: runtimeeffects.SelectedContractForkAuthority{
				ExecutionID: executionID, ForkRunID: uuid.NewString(), Generation: 1,
				AdmissionFingerprint: "admission", ContainerPlanFingerprint: "container",
				ActorCensusFingerprint: "actors", EffectiveConfigFingerprint: "config",
			},
		}
	case runtimeeffects.AuthorityConversationForkChat:
		forkTurnID := uuid.NewString()
		return runtimeeffects.Authority{
			Kind: kind, ID: forkTurnID, ExecutionOwner: "hostile-forkchat", LeaseExpiresAt: now.Add(time.Minute), FenceGeneration: 1,
			ForkChat: runtimeeffects.ConversationForkChatAuthority{
				ForkTurnID: forkTurnID, ForkID: uuid.NewString(), ActorTokenID: "actor",
				RequestOccurrenceID: uuid.NewString(), RequestHash: "request",
			},
		}
	default:
		return runtimeeffects.Authority{}
	}
}

func requireLegacyEffectTablesAbsent(t *testing.T, fixture neutralEffectParityFixture) {
	t.Helper()
	if fixture.sqlite {
		var count int
		if err := fixture.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type IN ('table','view') AND name IN ('agent_external_effect_operations','agent_external_effect_attempts')`).Scan(&count); err != nil || count != 0 {
			t.Fatalf("legacy SQLite effect tables/views=%d err=%v, want 0", count, err)
		}
		return
	}
	var absent bool
	if err := fixture.db.QueryRow(`SELECT to_regclass('agent_external_effect_operations') IS NULL AND to_regclass('agent_external_effect_attempts') IS NULL`).Scan(&absent); err != nil || !absent {
		t.Fatalf("legacy PostgreSQL effect tables/views absent=%v err=%v, want true", absent, err)
	}
}
