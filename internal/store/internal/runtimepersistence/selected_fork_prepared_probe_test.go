package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/google/uuid"
)

func TestPreparedSelectedForkProbeSurfaceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			store := fixture.store.(managedCapabilityTestStore)
			name, err := agentidentity.DeclaredName("prepared-worker", "selected-target")
			if err != nil {
				t.Fatal(err)
			}
			actor, err := agentidentity.NewPlan(name, agentidentity.RootRoute())
			if err != nil {
				t.Fatal(err)
			}
			fingerprint, err := actor.Fingerprint()
			if err != nil {
				t.Fatal(err)
			}
			surface, err := managedcapabilities.New(managedcapabilities.Plan{
				ActorPlan: actor, RuntimeMode: "startup_probe", Provider: "claude_cli", Transport: "cli", ProviderContract: "prepared-probe-test",
				Authority: managedcapabilities.Authority{
					Kind: managedcapabilities.AuthorityStartupProbe, ID: uuid.NewString(),
					ExecutionKind: managedcapabilities.ExecutionSelectedForkPreparation, ExecutionAuthorityID: uuid.NewString(),
					Preparation: &managedcapabilities.PreparedSelectedForkProbeAuthority{
						ProcessAuthorityID: uuid.NewString(), ProcessOwnerID: "probe-process", ProcessBootID: uuid.NewString(),
						BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64), SourceFingerprint: strings.Repeat("b", 64),
						AdmittedPlanFingerprint: strings.Repeat("c", 64), ConfigurationFingerprint: strings.Repeat("d", 64),
						CatalogFingerprint: strings.Repeat("e", 64), ActorPlanFingerprint: fingerprint,
					},
				}, CreatedAt: time.Unix(1, 0).UTC(),
			})
			if err != nil {
				t.Fatal(err)
			}
			ctx := testAuthorActivityContext()
			if err := store.SaveManagedCapabilitySurface(ctx, surface); err != nil {
				t.Fatalf("save non-executable preparation diagnostic: %v", err)
			}
			read := func() managedcapabilities.Surface {
				t.Helper()
				query := `SELECT surface, run_id FROM managed_agent_capability_surfaces WHERE surface_id=?`
				if fixture.postgres {
					query = `SELECT surface::text, run_id::text FROM managed_agent_capability_surfaces WHERE surface_id=$1::uuid`
				}
				var raw string
				var run sql.NullString
				if err := fixture.db.QueryRowContext(context.Background(), query, surface.ID).Scan(&raw, &run); err != nil {
					t.Fatal(err)
				}
				var persisted managedcapabilities.Surface
				if err := json.Unmarshal([]byte(raw), &persisted); err != nil {
					t.Fatal(err)
				}
				if err := persisted.Validate(); err != nil {
					t.Fatal(err)
				}
				if run.Valid || !persisted.ActorIdentity.IsZero() || !persisted.MatchesActorPlan(actor) || !reflect.DeepEqual(persisted, surface) {
					t.Fatalf("preparation diagnostic readback differs: %#v, run=%#v", persisted, run)
				}
				return persisted
			}
			read()
			for _, test := range []struct {
				name   string
				mutate func(*managedcapabilities.Surface)
			}{
				{"provider_turn", func(s *managedcapabilities.Surface) { s.Authority.Kind = managedcapabilities.AuthorityProviderTurn }},
				{"execution", func(s *managedcapabilities.Surface) {
					s.Authority.ExecutionKind = managedcapabilities.ExecutionSelectedContractFork
				}},
				{"process", func(s *managedcapabilities.Surface) { s.Authority.Preparation.ProcessAuthorityID = uuid.NewString() }},
				{"catalog", func(s *managedcapabilities.Surface) {
					s.Authority.Preparation.CatalogFingerprint = strings.Repeat("f", 64)
				}},
				{"same_name_actor", func(s *managedcapabilities.Surface) { s.ActorPlan.Name.Owner = "other-target" }},
			} {
				t.Run(test.name, func(t *testing.T) {
					invalid := surface.Clone()
					test.mutate(&invalid)
					if err := store.SaveManagedCapabilitySurface(ctx, invalid); err == nil {
						t.Fatal("accepted crossed or executable preparation evidence")
					}
					read()
				})
			}
			query := `UPDATE managed_agent_capability_surfaces SET authority_kind='provider_turn' WHERE surface_id=?`
			if fixture.postgres {
				query = `UPDATE managed_agent_capability_surfaces SET authority_kind='provider_turn' WHERE surface_id=$1::uuid`
			}
			if _, err := fixture.db.ExecContext(context.Background(), query, surface.ID); err == nil {
				t.Fatal("DDL accepted provider-turn projection for preparation")
			}
			read()
			var runs int
			if err := fixture.db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM runs`).Scan(&runs); err != nil || runs != 0 {
				t.Fatalf("diagnostic persistence created run state: runs=%d err=%v", runs, err)
			}
			// Supply a real referenced run so only the preparation constraint can
			// reject the changed run projection, not the foreign-key constraint.
			runID := uuid.NewString()
			ensureRunLifecycleCandidateParityRun(t, fixture, ctx, runID, time.Unix(1, 0).UTC())
			query = `UPDATE managed_agent_capability_surfaces SET run_id=? WHERE surface_id=?`
			if fixture.postgres {
				query = `UPDATE managed_agent_capability_surfaces SET run_id=$1::uuid WHERE surface_id=$2::uuid`
			}
			if _, err := fixture.db.ExecContext(context.Background(), query, runID, surface.ID); err == nil {
				t.Fatal("DDL accepted a real execution run for preparation")
			}
			read()
		})
	}
}
