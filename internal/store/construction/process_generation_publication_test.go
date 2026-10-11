package construction_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/construction"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type generationPublicationStore interface {
	store.SchemaBootstrapper
	startupownership.Store
	InspectAuthority(context.Context) (startupownership.AuthorityInspection, error)
	IsExternalEffectAuthorityCurrent(context.Context, effects.Authority) (bool, error)
	AuthorizeExternalAttempt(context.Context, effects.Authority, effects.AuthorizeRequest) (effects.Attempt, error)
	Close() error
}

func generationPublicationFixture(t *testing.T, postgres bool) (generationPublicationStore, *sql.DB) {
	t.Helper()
	ctx := context.Background()
	var selected generationPublicationStore
	var db *sql.DB
	var err error
	if postgres {
		dsn, _, _ := testutil.StartEmptyPostgres(t)
		selected, db, err = construction.OpenPostgresRuntime(ctx, dsn)
	} else {
		selected, db, err = construction.OpenSQLiteRuntimeForServing(ctx, filepath.Join(t.TempDir(), "publication.db"))
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := selected.Close(); err != nil {
			t.Error(err)
		}
	})
	spec, err := contracts.LoadPlatformSpecDocument(filepath.Join("..", "..", "..", "platform-spec.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	plans, err := store.GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := selected.BootstrapSchema(ctx, store.SchemaBootstrapRequest{PlatformPlans: plans, Origin: store.RuntimeStoreOrigin{
		SwarmVersion: "generation-publication-test", PlatformVersion: spec.Platform.Version, CreatedAt: time.Now().UTC(),
	}}); err != nil {
		t.Fatal(err)
	}
	return selected, db
}

func TestProcessGenerationPublicationAndJoinedReleaseBothStores(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		t.Run(map[bool]string{false: "sqlite", true: "postgres"}[postgres], func(t *testing.T) {
			selected, db := generationPublicationFixture(t, postgres)
			ctx := context.Background()
			inspection, err := selected.InspectAuthority(ctx)
			if err != nil || inspection.Status != startupownership.AuthorityInspectionEmpty || inspection.Authority != nil || inspection.Possession != nil {
				t.Fatalf("fresh admitted snapshot claimed a holder: %#v, %v", inspection, err)
			}
			query := `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('runtime_startup_authority_facts','runtime_startup_authority_repairs')`
			if postgres {
				query = `SELECT COUNT(*) FROM pg_tables WHERE schemaname='public' AND tablename IN ('runtime_startup_authority_facts','runtime_startup_authority_repairs')`
			}
			var retired int
			if err := db.QueryRowContext(ctx, query).Scan(&retired); err != nil || retired != 0 {
				t.Fatalf("fresh schema contains process journal: %d, %v", retired, err)
			}
			req := startupownership.AcquireRequest{OwnerID: "same-boot", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString()}
			var previous startupownership.Authority
			for generation := uint64(1); generation <= 3; generation++ {
				capability, err := selected.AcquireProcessCapability(ctx, req)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = capability.Release(context.Background()) })
				authority, err := capability.Evidence()
				if err != nil || authority.Validate() != nil || authority.AuthorityGeneration != generation || authority.AuthorityID == previous.AuthorityID {
					t.Fatalf("new native lease reused an old generation: previous=%#v current=%#v, %v", previous, authority, err)
				}
				if err := capability.ProveCurrent(ctx); err != nil {
					t.Fatal(err)
				}
				if err := capability.Release(ctx); err != nil {
					t.Fatal(err)
				}
				if _, err := capability.Evidence(); err == nil {
					t.Fatal("released capability reissued evidence")
				}
				stamp, err := generationauthority.ReadProcessGeneration(ctx, db, !postgres)
				if err != nil || stamp != authority.LeaseStamp() {
					t.Fatalf("joined release rewrote admitted stamp: %#v, %v", stamp, err)
				}
				inspection, err = selected.InspectAuthority(ctx)
				if err != nil || inspection.Status != startupownership.AuthorityInspectionValid || inspection.Authority == nil || *inspection.Authority != authority || inspection.Possession != nil {
					t.Fatalf("snapshot confused admitted history with possession: %#v, %v", inspection, err)
				}
				previous = authority
			}
			var rows int
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_process_generation`).Scan(&rows); err != nil || rows != 1 {
				t.Fatalf("process evidence grew into a journal: rows=%d, %v", rows, err)
			}
		})
	}
}

func TestGenerationEffectAdmissionRequiresCurrentProcessBothStores(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		for _, field := range []string{"lease_id", "owner_id", "boot_id"} {
			t.Run(map[bool]string{false: "sqlite", true: "postgres"}[postgres]+"/"+field, func(t *testing.T) {
				selected, db := generationPublicationFixture(t, postgres)
				ctx := context.Background()
				req := startupownership.AcquireRequest{OwnerID: "effect-owner", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString()}
				capability, err := selected.AcquireProcessCapability(ctx, req)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = capability.Release(context.Background()) })
				plan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{{BundleHash: "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}, nil)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := capability.InstallCompleteSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}); err != nil {
					t.Fatal(err)
				}
				grant, err := capability.IssueGenerationGrant(ctx, startupownership.GrantRequest{BundleHash: plan.Sources[0].BundleHash,
					RuntimeInstanceID: req.RuntimeInstanceID, RuntimeGeneration: 1, SourceSetRevision: plan.Revision})
				if err != nil {
					t.Fatal(err)
				}
				evidence, err := grant.Evidence()
				if err != nil {
					t.Fatal(err)
				}
				probeGrant, err := capability.IssueGenerationGrant(ctx, startupownership.GrantRequest{BundleHash: plan.Sources[0].BundleHash,
					RuntimeInstanceID: req.RuntimeInstanceID, RuntimeGeneration: 2, SourceSetRevision: plan.Revision})
				if err != nil {
					t.Fatal(err)
				}
				probeEvidence, err := probeGrant.Evidence()
				if err != nil {
					t.Fatal(err)
				}
				probeID := uuid.NewString()
				probe := effects.Authority{Kind: effects.AuthorityStartupProbe, ID: probeID, ExecutionOwner: req.OwnerID,
					FenceGeneration: 2, ExecutionMode: effects.ExecutionModeLive, LeaseExpiresAt: time.Now().Add(time.Minute),
					StartupProbe: effects.StartupProbeAuthority{ProbeID: probeID, StartupAuthorityID: probeEvidence.GrantID,
						StartupStateVersion: probeEvidence.StateVersion, ActorID: "probe-actor", ExecutionKind: "normal_runtime", ExecutionAuthorityID: probeEvidence.GrantID}}
				if current, err := selected.IsExternalEffectAuthorityCurrent(ctx, probe); err != nil || !current {
					t.Fatalf("current startup probe refused: current=%t, %v", current, err)
				}
				if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
					t.Fatal(err)
				}
				evidence, err = grant.AdmitExecution(ctx)
				if err != nil {
					t.Fatal(err)
				}
				intentID := uuid.NewString()
				registration := effects.Authority{Kind: effects.AuthorityServeRegistration, ID: intentID, ExecutionOwner: req.OwnerID,
					FenceGeneration: 1, ExecutionMode: effects.ExecutionModeLive, LeaseExpiresAt: time.Now().Add(time.Minute),
					ServeRegistration: effects.ServeRegistrationAuthority{IntentID: intentID, StartupAuthorityID: evidence.GrantID, StartupStateVersion: evidence.StateVersion}}
				if current, err := selected.IsExternalEffectAuthorityCurrent(ctx, registration); err != nil || !current {
					t.Fatalf("current registration refused: current=%t, %v", current, err)
				}
				if current, err := selected.IsExternalEffectAuthorityCurrent(ctx, probe); err != nil || !current {
					t.Fatalf("independent prepared probe stopped being current before process fault: current=%t, %v", current, err)
				}
				// Inject a well-formed crossed process stamp without touching the grant.
				placeholder := "?"
				if postgres {
					placeholder = "$1"
				}
				if _, err := db.ExecContext(ctx, `UPDATE runtime_process_generation SET `+field+`=`+placeholder, uuid.NewString()); err != nil {
					t.Fatal(err)
				}
				for _, authority := range []effects.Authority{probe, registration} {
					if current, err := selected.IsExternalEffectAuthorityCurrent(ctx, authority); err != nil || current {
						t.Fatalf("crossed process admitted %s: current=%t, %v", authority.Kind, current, err)
					}
				}
				definition, ok := effects.RegistrationFor("provider_registration")
				if !ok {
					t.Fatal("missing provider-registration definition")
				}
				_, err = selected.AuthorizeExternalAttempt(ctx, registration, effects.AuthorizeRequest{
					OperationID: uuid.NewString(), AttemptID: uuid.NewString(), Kind: definition.Kind, Class: definition.Class,
					Adapter: definition.Adapter, Transport: definition.Transport, RequestFingerprint: effects.Fingerprint([]byte("crossed-process")),
					Lineage: map[string]string{"intent_id": intentID}, Now: time.Now().UTC(),
				})
				failure, ok := failures.EnvelopeFromError(err)
				if !ok || failure.Detail.Code != "external_effect_authority_stale" || failure.Detail.Attributes["reason"] != "generation_grant_process_not_current" {
					t.Fatalf("wrong mutation refusal: %v", err)
				}
				var operations, attempts int
				if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_external_effect_operations`).Scan(&operations); err != nil {
					t.Fatal(err)
				}
				if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_external_effect_attempts`).Scan(&attempts); err != nil {
					t.Fatal(err)
				}
				if operations != 0 || attempts != 0 {
					t.Fatalf("refused process changed effects: operations=%d attempts=%d", operations, attempts)
				}
			})
		}
	}
}
