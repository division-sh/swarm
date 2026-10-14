package channelonboarding

import (
	"testing"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/google/uuid"
)

func TestSessionLogoutTargetRequiresConsistentOriginalProcessAttribution(t *testing.T) {
	coordinate := testCandidate("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "support").Coordinate
	binding := processbinding.Binding{ProcessAuthorityID: uuid.NewString(), ProcessOwnerID: "logout-owner", ProcessBootID: uuid.NewString(),
		GenerationGrantID: uuid.NewString(), BundleHash: coordinate.BundleHash, RuntimeInstanceID: coordinate.RuntimeInstanceID, RuntimeGeneration: 1}
	target := SessionLogoutTarget{SessionLogoutPreparation: SessionLogoutPreparation{
		OperationID: uuid.NewString(), OperationRevision: 3, OccurrenceID: uuid.NewString(), TargetSelector: "ingress:.:whatsapp",
		Account:    operatorchannel.SessionAccountAdmission{Provider: "whatsapp", ConnectionID: uuid.NewString(), AccountRef: "15551234567@s.whatsapp.net", AdmissionID: uuid.NewString(), Revision: 1},
		Coordinate: coordinate, ProcessBinding: binding},
		ProcessStamp: processbinding.LeaseStamp{LeaseIdentity: processbinding.LeaseIdentity{LeaseID: binding.ProcessAuthorityID,
			OwnerID: binding.ProcessOwnerID, BootID: binding.ProcessBootID, RuntimeInstanceID: binding.RuntimeInstanceID,
			Backend: processbinding.SQLiteLeaseBackend}, Generation: 1}}
	if err := target.Validate(); err != nil {
		t.Fatal("consistent original target refused", err)
	}
	for _, cell := range []struct {
		name string
		edit func(*SessionLogoutTarget)
	}{
		{"missing_stamp", func(t *SessionLogoutTarget) { t.ProcessStamp = processbinding.LeaseStamp{} }},
		{"zero_generation", func(t *SessionLogoutTarget) { t.ProcessStamp.Generation = 0 }},
		{"malformed_stamp", func(t *SessionLogoutTarget) { t.ProcessStamp.LeaseID = "not-a-uuid" }},
		{"foreign_lease", func(t *SessionLogoutTarget) { t.ProcessStamp.LeaseID = uuid.NewString() }},
		{"foreign_owner", func(t *SessionLogoutTarget) { t.ProcessStamp.OwnerID = "another-owner" }},
		{"foreign_boot", func(t *SessionLogoutTarget) { t.ProcessStamp.BootID = uuid.NewString() }},
		{"foreign_runtime", func(t *SessionLogoutTarget) { t.ProcessStamp.RuntimeInstanceID = uuid.NewString() }},
		{"missing_grant", func(t *SessionLogoutTarget) { t.ProcessBinding.GenerationGrantID = "" }},
		{"foreign_source", func(t *SessionLogoutTarget) {
			t.ProcessBinding.BundleHash = "bundle-v2:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		}},
	} {
		t.Run(cell.name, func(t *testing.T) {
			changed := target
			cell.edit(&changed)
			if changed.Validate() == nil {
				t.Fatal("contradictory original process target admitted")
			}
		})
	}
}
