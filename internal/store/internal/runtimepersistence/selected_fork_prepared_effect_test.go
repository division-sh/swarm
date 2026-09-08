package runtimepersistence

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/google/uuid"
)

func TestPreparedSelectedForkProbeEffectBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			store := fixture.store.(startupAuthorityParityStore)
			ctx := testAuthorActivityContextForBundle(testCanonicalBundleHash)
			capability, err := store.AcquireProcessCapability(ctx, testStartupAcquireRequest("prepared-probe-process"))
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
				ProcessAuthorityID: process.AuthorityID, ProcessOwnerID: process.OwnerID, ProcessBootID: process.BootID,
				BundleHash: testCanonicalBundleHash, SourceFingerprint: strings.Repeat("b", 64),
				AdmittedPlanFingerprint: strings.Repeat("c", 64), ConfigurationFingerprint: strings.Repeat("d", 64), CatalogFingerprint: strings.Repeat("e", 64), ActorPlanFingerprint: actor,
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
			if err := store.MarkExternalAttemptLaunched(ctx, attempt, time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			if err := store.MarkExternalAttemptResponseObserved(ctx, attempt, map[string]any{"probe": true}, time.Now().UTC()); err != nil {
				t.Fatal(err)
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
			if err := capability.Release(ctx); err != nil {
				t.Fatal(err)
			}
			if current, err := store.IsExternalEffectAuthorityCurrent(ctx, authority); err != nil || current {
				t.Fatalf("released process remains current: %v %v", current, err)
			}
			if err := store.MarkExternalAttemptLaunched(ctx, attempt, time.Now().UTC()); err == nil {
				t.Fatal("released process launched prepared probe")
			}
			request.OperationID, request.AttemptID = uuid.NewString(), uuid.NewString()
			if _, err := controller.Authorize(effects.WithAuthority(ctx, authority), request); err == nil {
				t.Fatal("released process admitted another probe")
			}
		})
	}
}
