package channelonboarding

import (
	"database/sql"
	"encoding/json"
	"strings"
	"testing"

	domain "github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
	"github.com/google/uuid"
)

func logoutCodecFixture(t *testing.T, d dialect) domain.SessionLogoutTarget {
	t.Helper()
	plan, err := plangeneration.FromCanonicalValue(map[string]string{"test": "logout-codec"})
	if err != nil {
		t.Fatal(err)
	}
	coordinate := domain.ChannelRuntimeContextCoordinate{BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64),
		BundleIdentity: "logout-codec", PackInventoryGeneration: "sha256:logout-codec", RuntimeInstanceID: uuid.NewString(), ContextPublicationGeneration: 1, PlanGeneration: plan}
	binding := processbinding.Binding{ProcessAuthorityID: uuid.NewString(), ProcessOwnerID: "logout-codec", ProcessBootID: uuid.NewString(),
		GenerationGrantID: uuid.NewString(), BundleHash: coordinate.BundleHash, RuntimeInstanceID: coordinate.RuntimeInstanceID, RuntimeGeneration: 1}
	backend := processbinding.PostgresLeaseBackend
	if d == dialectSQLite {
		backend = processbinding.SQLiteLeaseBackend
	}
	return domain.SessionLogoutTarget{SessionLogoutPreparation: domain.SessionLogoutPreparation{
		OperationID: uuid.NewString(), OperationRevision: 4, OccurrenceID: uuid.NewString(), TargetSelector: "ingress:.:whatsapp",
		Account:    operatorchannel.SessionAccountAdmission{Provider: "whatsapp", ConnectionID: uuid.NewString(), AccountRef: "15551234567@s.whatsapp.net", AdmissionID: uuid.NewString(), Revision: 1},
		Coordinate: coordinate, ProcessBinding: binding}, ProcessStamp: processbinding.LeaseStamp{LeaseIdentity: processbinding.LeaseIdentity{
		LeaseID: binding.ProcessAuthorityID, OwnerID: binding.ProcessOwnerID, BootID: binding.ProcessBootID, RuntimeInstanceID: binding.RuntimeInstanceID, Backend: backend}, Generation: 2}}
}

func TestLogoutTargetCodecRejectsMissingOrContradictoryOriginalStamp(t *testing.T) {
	for _, d := range []dialect{dialectSQLite, dialectPostgres} {
		name := "postgres"
		if d == dialectSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			target := logoutCodecFixture(t, d)
			decode := func(raw []byte) (*domain.SessionLogoutTarget, error) {
				return decodeLogoutTarget(domain.TeardownLogout, sql.NullString{String: target.Account.ConnectionID, Valid: true}, sql.NullString{String: string(raw), Valid: true}, d)
			}
			raw, err := json.Marshal(target)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := decode(raw)
			if err != nil || decoded == nil || *decoded != target {
				t.Fatal("strict target lost original grant/stamp", err)
			}
			for _, cell := range []struct {
				name string
				edit func(*domain.SessionLogoutTarget)
			}{
				{"missing_stamp", func(t *domain.SessionLogoutTarget) { t.ProcessStamp = processbinding.LeaseStamp{} }},
				{"zero_generation", func(t *domain.SessionLogoutTarget) { t.ProcessStamp.Generation = 0 }},
				{"malformed_lease", func(t *domain.SessionLogoutTarget) { t.ProcessStamp.LeaseID = "invalid" }},
				{"different_lease", func(t *domain.SessionLogoutTarget) { t.ProcessStamp.LeaseID = uuid.NewString() }},
				{"different_boot", func(t *domain.SessionLogoutTarget) { t.ProcessStamp.BootID = uuid.NewString() }},
				{"different_runtime", func(t *domain.SessionLogoutTarget) { t.ProcessStamp.RuntimeInstanceID = uuid.NewString() }},
				{"missing_binding", func(t *domain.SessionLogoutTarget) { t.ProcessBinding = processbinding.Binding{} }},
				{"opposite_backend", func(t *domain.SessionLogoutTarget) {
					if d == dialectSQLite {
						t.ProcessStamp.Backend = processbinding.PostgresLeaseBackend
					} else {
						t.ProcessStamp.Backend = processbinding.SQLiteLeaseBackend
					}
				}},
			} {
				t.Run(cell.name, func(t *testing.T) {
					changed := target
					cell.edit(&changed)
					bad, err := json.Marshal(changed)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := decode(bad); err == nil {
						t.Fatal("contradictory original target accepted")
					}
				})
			}
			old, err := json.Marshal(target.SessionLogoutPreparation)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := decode(old); err == nil {
				t.Fatal("legacy target accepted without original stamp")
			}
			if _, err := decode(append(raw, []byte(` {}`)...)); err == nil {
				t.Fatal("trailing evidence accepted")
			}
		})
	}
}
