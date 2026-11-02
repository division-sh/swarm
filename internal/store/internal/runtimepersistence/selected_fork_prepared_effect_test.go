package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/google/uuid"
)

func TestPreparedSelectedForkProbeEffectBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			store := fixture.store.(startupAuthorityParityStore)
			ctx := testAuthorActivityContextForBundle(testCanonicalBundleHash)
			acquire := testStartupAcquireRequest("prepared-probe-process")
			capability, err := store.AcquireProcessCapability(ctx, acquire)
			if err != nil {
				t.Fatal(err)
			}
			ctx, err = startupownership.WithProcessMutationAuthority(ctx, capability)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := capability.Release(context.Background()); err != nil {
					t.Error(err)
				}
			})
			process, err := capability.Evidence()
			if err != nil {
				t.Fatal(err)
			}
			name, err := agentidentity.DeclaredName("worker", "prepared-target")
			if err != nil {
				t.Fatal(err)
			}
			plan, err := agentidentity.NewPlan(name, agentidentity.RootRoute())
			if err != nil {
				t.Fatal(err)
			}
			actor, err := plan.Fingerprint()
			if err != nil {
				t.Fatal(err)
			}
			preparation := managedcapabilities.PreparedSelectedForkProbeAuthority{
				SelectedForkPreparationCoordinates: managedcapabilities.SelectedForkPreparationCoordinates{
					ProcessAuthorityID: process.AuthorityID, ProcessOwnerID: process.OwnerID, ProcessBootID: process.BootID,
					BundleHash: testCanonicalBundleHash, SourceFingerprint: strings.Repeat("b", 64),
					AdmittedPlanFingerprint: strings.Repeat("c", 64), ConfigurationFingerprint: strings.Repeat("d", 64), CatalogFingerprint: strings.Repeat("e", 64),
				},
				ActorPlanFingerprint: actor,
			}
			probeID, preparationID := uuid.NewString(), uuid.NewString()
			surface, err := managedcapabilities.New(managedcapabilities.Plan{
				ActorPlan: plan, RuntimeMode: "startup_probe", Provider: "claude_cli", Transport: "cli", ProviderContract: "prepared-probe-test",
				Authority: managedcapabilities.Authority{Kind: managedcapabilities.AuthorityStartupProbe, ID: probeID, ExecutionKind: managedcapabilities.ExecutionSelectedForkPreparation, ExecutionAuthorityID: preparationID, Preparation: &preparation}, CreatedAt: time.Now().UTC(),
			})
			if err != nil {
				t.Fatal(err)
			}
			authority := effects.Authority{Kind: effects.AuthorityStartupProbe, ID: probeID, ExecutionOwner: process.OwnerID,
				LeaseExpiresAt: time.Now().UTC().Add(time.Minute), FenceGeneration: process.AuthorityGeneration, ExecutionMode: effects.ExecutionModeLive,
				StartupProbe: effects.StartupProbeAuthority{ProbeID: probeID, ActorID: plan.AgentID(), ExecutionKind: string(managedcapabilities.ExecutionSelectedForkPreparation), ExecutionAuthorityID: preparationID, Preparation: &preparation}}
			registration, ok := effects.RegistrationFor("claude_cli_startup_probe")
			if !ok {
				t.Fatal("startup probe adapter missing")
			}
			request := effects.AuthorizeRequest{OperationID: uuid.NewString(), Kind: registration.Kind, Class: registration.Class, Adapter: registration.Adapter, Transport: registration.Transport, RequestFingerprint: "prepared-probe", CapabilitySurface: &surface}
			controller := effects.NewController(store).WithExecutionPosture(executionposture.Live)
			attempt, err := controller.Authorize(effects.WithAuthority(ctx, authority), request)
			if err != nil {
				t.Fatal(err)
			}
			var raw []byte
			if err := fixture.db.QueryRowContext(ctx, `SELECT authority_evidence FROM runtime_external_effect_operations WHERE operation_id=$1`, attempt.OperationID).Scan(&raw); err != nil {
				t.Fatal(err)
			}
			var evidence map[string]json.RawMessage
			if err := json.Unmarshal(raw, &evidence); err != nil {
				t.Fatal(err)
			}
			var persisted managedcapabilities.PreparedSelectedForkProbeAuthority
			if err := json.Unmarshal(evidence["preparation"], &persisted); err != nil || persisted != preparation {
				t.Fatalf("lost exact preparation: %+v %v", persisted, err)
			}
			for _, table := range []string{"runs", "agents", "runtime_generation_grants", "run_fork_selected_contract_runtime_executions"} {
				var count int
				if err := fixture.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table).Scan(&count); err != nil || count != 0 {
					t.Fatalf("probe created %s: count=%d err=%v", table, count, err)
				}
			}
			for _, adapter := range []string{"anthropic_api", "authored_http_tool", "native_read_file"} {
				invalid := request
				invalid.OperationID, invalid.AttemptID, invalid.Adapter = uuid.NewString(), uuid.NewString(), adapter
				business, ok := effects.RegistrationFor(adapter)
				if !ok {
					t.Fatalf("business adapter %s missing", adapter)
				}
				invalid.Kind, invalid.Class, invalid.Transport = business.Kind, business.Class, business.Transport
				if _, err := store.AuthorizeExternalAttempt(ctx, authority, invalid); err == nil {
					t.Fatalf("direct store admitted business adapter %s", adapter)
				}
			}
			for _, coordinate := range []string{"process", "owner", "boot", "generation"} {
				t.Run(coordinate, func(t *testing.T) {
					crossed := preparation
					invalidAuthority := authority
					switch coordinate {
					case "process":
						crossed.ProcessAuthorityID = uuid.NewString()
					case "owner":
						crossed.ProcessOwnerID = "another-process"
					case "boot":
						crossed.ProcessBootID = uuid.NewString()
					case "generation":
						invalidAuthority.FenceGeneration++
					}
					invalidAuthority.StartupProbe.Preparation = &crossed
					invalidAuthority.ExecutionOwner = crossed.ProcessOwnerID
					crossedSurface, err := managedcapabilities.New(managedcapabilities.Plan{
						ActorPlan: plan, RuntimeMode: "startup_probe", Provider: "claude_cli", Transport: "cli", ProviderContract: "prepared-probe-test",
						Authority: managedcapabilities.Authority{Kind: managedcapabilities.AuthorityStartupProbe, ID: probeID, ExecutionKind: managedcapabilities.ExecutionSelectedForkPreparation, ExecutionAuthorityID: preparationID, Preparation: &crossed}, CreatedAt: time.Now().UTC(),
					})
					if err != nil {
						t.Fatal(err)
					}
					invalid := request
					invalid.OperationID, invalid.AttemptID, invalid.CapabilitySurface = uuid.NewString(), uuid.NewString(), &crossedSurface
					// Request and surface agree. Only the selected store's real process
					// head can reject this otherwise valid crossed possession evidence.
					if err := invalidAuthority.ValidatePreparedProbeRequest(invalid); err != nil {
						t.Fatal(err)
					}
					if _, err := store.AuthorizeExternalAttempt(ctx, invalidAuthority, invalid); err == nil {
						t.Fatal("crossed process admitted")
					}
					var count int
					if err := fixture.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_external_effect_operations`).Scan(&count); err != nil || count != 1 {
						t.Fatalf("refusal mutated operations: %d %v", count, err)
					}
				})
			}
			if _, err := store.MarkExternalAttemptLaunched(ctx, attempt, time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			if err := store.MarkExternalAttemptResponseObserved(ctx, attempt, map[string]any{"probe": true}, time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			// A completed result entry cannot lend admission to another transaction.
			resultRequest := effects.Settlement{OperationID: attempt.OperationID, AttemptID: attempt.AttemptID, Authority: attempt.Authority}
			capture := func(ctx context.Context, _ *mutationprotocol.Attempt) (context.Context, error) { return ctx, nil }
			var captured mutationprotocol.Result[context.Context]
			var mutate func(context.Context, func(context.Context, *sql.Tx) error) error
			if fixture.postgres {
				native, err := postgresbackend.New(fixture.db)
				if err != nil {
					t.Fatal(err)
				}
				captured = mutationprotocol.RunPostgresExternalEffectResult(ctx, native, resultRequest, mutationprotocol.RevisionOnly, nil, capture)
				mutate = native.RunTransaction
			} else {
				native, err := sqlitebackend.New(fixture.db)
				if err != nil {
					t.Fatal(err)
				}
				captured = mutationprotocol.RunSQLiteExternalEffectResult(ctx, native, "capture result admission", resultRequest, mutationprotocol.RevisionOnly, nil, capture)
				mutate = func(ctx context.Context, write func(context.Context, *sql.Tx) error) error {
					return native.RunTransaction(ctx, "refuse retained result admission", write)
				}
			}
			retained, found := captured.Value()
			if !captured.Acknowledged() || captured.Err() != nil || !found {
				t.Fatalf("capture exact result admission: found=%v err=%v", found, captured.Err())
			}
			calls := 0
			if err := mutate(context.WithoutCancel(retained), func(context.Context, *sql.Tx) error { calls++; return nil }); err == nil || calls != 0 {
				t.Fatalf("retained result entered another transaction: calls=%d err=%v", calls, err)
			}
			if err := store.SettleExternalAttempt(ctx, effects.Settlement{OperationID: attempt.OperationID, AttemptID: attempt.AttemptID, Authority: authority, State: effects.StateSettled, Now: time.Now().UTC()}); err != nil {
				t.Fatal(err)
			}
			var state string
			if err := fixture.db.QueryRowContext(ctx, `SELECT state FROM runtime_external_effect_attempts WHERE attempt_id=$1`, attempt.AttemptID).Scan(&state); err != nil || state != string(effects.StateSettled) {
				t.Fatalf("prepared probe did not settle: %q %v", state, err)
			}
			request.OperationID, request.AttemptID = uuid.NewString(), uuid.NewString()
			attempt, err = controller.Authorize(effects.WithAuthority(ctx, authority), request)
			if err != nil {
				t.Fatal(err)
			}
			acceptedRequest := request
			acceptedRequest.OperationID, acceptedRequest.AttemptID = uuid.NewString(), uuid.NewString()
			accepted, err := controller.Authorize(effects.WithAuthority(ctx, authority), acceptedRequest)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.MarkExternalAttemptLaunched(ctx, accepted, time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			resultCtx, err := startupownership.ExternalEffectResultContext(ctx, effects.Settlement{OperationID: accepted.OperationID, AttemptID: accepted.AttemptID, Authority: accepted.Authority})
			if err != nil {
				t.Fatal(err)
			}
			fresh := request
			fresh.OperationID, fresh.AttemptID = uuid.NewString(), uuid.NewString()
			if _, err := controller.Authorize(effects.WithAuthority(resultCtx, authority), fresh); err == nil {
				t.Error("accepted-result context admitted another probe while the process was live")
			}
			if _, err := store.MarkExternalAttemptLaunched(resultCtx, attempt, time.Now().UTC()); err == nil {
				t.Error("accepted-result context launched another probe while the process was live")
			}
			if err := capability.Release(ctx); err != nil {
				t.Fatal(err)
			}
			inspection, err := store.InspectAuthority(ctx)
			if err != nil || inspection.Authority == nil || *inspection.Authority != process {
				t.Errorf("release changed last-admitted process evidence: %+v %v", inspection, err)
			}
			if current, err := store.IsExternalEffectAuthorityCurrent(ctx, authority); err != nil || current {
				t.Errorf("released process remains current: %v %v", current, err)
			}
			if _, err := store.MarkExternalAttemptLaunched(ctx, attempt, time.Now().UTC()); err == nil {
				t.Error("released process launched prepared probe")
			}
			outcomes := fixture.store.(effects.OutcomeStore)
			outcome, found, err := outcomes.GetExternalEffectOutcome(ctx, attempt.OperationID)
			if err != nil || !found || outcome.State != effects.StateAuthorized || outcome.AttemptState != effects.StateAuthorized {
				t.Errorf("refused launch changed authorized attempt: %+v found=%v err=%v", outcome, found, err)
			}
			request.OperationID, request.AttemptID = uuid.NewString(), uuid.NewString()
			if _, err := controller.Authorize(effects.WithAuthority(ctx, authority), request); err == nil {
				t.Error("released process admitted another probe")
			}
			if outcome, found, err := outcomes.GetExternalEffectOutcome(ctx, request.OperationID); err != nil || found {
				t.Errorf("refused authorization persisted an operation: %+v found=%v err=%v", outcome, found, err)
			}
			foreignOperation, foreignAttempt, foreignAuthority := accepted, accepted, accepted
			foreignOperation.OperationID = uuid.NewString()
			foreignAttempt.AttemptID = uuid.NewString()
			foreignAuthority.Authority.ExecutionOwner += "-foreign"
			for _, refused := range []effects.Attempt{foreignOperation, foreignAttempt, foreignAuthority, attempt} {
				if err := store.MarkExternalAttemptResponseObserved(ctx, refused, map[string]any{"refused": true}, time.Now().UTC()); err == nil {
					t.Error("released process observed a foreign or unlaunched attempt")
				}
				if err := store.SettleExternalAttempt(ctx, effects.Settlement{OperationID: refused.OperationID, AttemptID: refused.AttemptID, Authority: refused.Authority, State: effects.StateSettled, Now: time.Now().UTC()}); err == nil {
					t.Error("released process settled a foreign or unlaunched attempt")
				}
			}
			if outcome, found, err := outcomes.GetExternalEffectOutcome(ctx, accepted.OperationID); err != nil || !found || outcome.State != effects.StateLaunched || outcome.AttemptState != effects.StateLaunched {
				t.Fatalf("refused results changed the accepted attempt: %+v found=%v err=%v", outcome, found, err)
			}
			if err := store.MarkExternalAttemptResponseObserved(ctx, accepted, map[string]any{"accepted_before_release": true}, time.Now().UTC()); err != nil {
				t.Fatalf("Release refused already accepted response evidence: %v", err)
			}
			if err := store.SettleExternalAttempt(ctx, effects.Settlement{OperationID: accepted.OperationID, AttemptID: accepted.AttemptID, Authority: authority, State: effects.StateSettled, Now: time.Now().UTC()}); err != nil {
				t.Fatalf("Release refused already accepted result settlement: %v", err)
			}
			if outcome, found, err := outcomes.GetExternalEffectOutcome(ctx, accepted.OperationID); err != nil || !found || !outcome.TerminalSuccess() {
				t.Errorf("accepted settlement evidence lost after Release: %+v found=%v err=%v", outcome, found, err)
			}
			// The same owner/boot/runtime inputs still mint a new native lease.
			// A retained prepared value must never redirect onto that successor.
			successor, err := store.AcquireProcessCapability(testAuthorActivityContextForBundle(testCanonicalBundleHash), acquire)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = successor.Release(context.Background()) })
			next, err := successor.Evidence()
			if err != nil || next.AuthorityID == process.AuthorityID || next.AuthorityGeneration != process.AuthorityGeneration+1 {
				t.Fatalf("reacquisition reused native identity: %+v %v", next, err)
			}
			if current, err := store.IsExternalEffectAuthorityCurrent(ctx, authority); err != nil || current {
				t.Errorf("stale preparation redirected currentness onto successor: %v %v", current, err)
			}
			if _, err := controller.Authorize(effects.WithAuthority(ctx, authority), request); err == nil {
				t.Error("stale preparation authorized through successor")
			}
			if _, err := store.MarkExternalAttemptLaunched(ctx, attempt, time.Now().UTC()); err == nil {
				t.Error("stale preparation launched through successor")
			}
		})
	}
}
