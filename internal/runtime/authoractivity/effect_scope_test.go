package authoractivity

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/channelnative"
	"github.com/google/uuid"
)

func TestDeploymentEffectLifecycleRequiresOriginalParent(t *testing.T) {
	id := uuid.NewString()
	parent := channelnative.TransportParent{Kind: "deployment_registration", ID: uuid.NewString(), Revision: 4,
		BindingOperationID: uuid.NewString(), BindingOperationRevision: 6}
	evidence := map[string]any{"transport_parent": parent, "effect_operation_id": id,
		"authority_kind": "channel_confirmation", "authority_id": id,
		"onboarding_operation_id": parent.ID, "onboarding_revision": parent.Revision}
	raw, _ := json.Marshal(evidence)
	attempt := 1
	draft := Draft{Kind: KindEffectLifecycle, Transition: "launched", SourceOwner: "runtime_external_effect_attempts",
		SourceIdentity: uuid.NewString() + ":launched", DedupKey: "effect:test", OccurredAt: time.Now().UTC(),
		Scope: RuntimeScope(uuid.NewString()), EffectAuthorityEvidence: raw,
		Projection: Projection{EffectClass: "write_or_unknown", Attempt: &attempt, Adapter: "channel_confirmation_whatsapp",
			Transport: "in_process", AuthorityKind: "channel_confirmation", AuthorityID: id, ExecutionMode: "live"}}
	if err := ValidateDraft(draft); err != nil {
		t.Fatal("exact deployment effect refused", err)
	}
	for name, change := range map[string]func(*Draft){
		"missing":        func(d *Draft) { d.EffectAuthorityEvidence = nil },
		"wrong kind":     func(d *Draft) { d.Projection.AuthorityKind = "channel_delivery" },
		"wrong identity": func(d *Draft) { d.Projection.AuthorityID = uuid.NewString() },
		"mixed scope":    func(d *Draft) { d.Scope.BundleHash = "bundle" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := draft
			change(&changed)
			if err := ValidateDraft(changed); err == nil {
				t.Fatal("runtime lifecycle admitted without closed authority")
			}
		})
	}
	for _, field := range []string{"activation_id", "bundle_hash", "runtime_instance_id", "plan_generation"} {
		evidence[field] = ""
		changed := draft
		changed.EffectAuthorityEvidence, _ = json.Marshal(evidence)
		if err := ValidateDraft(changed); err == nil {
			t.Fatal("mixed evidence admitted", field)
		}
		delete(evidence, field)
	}
}
