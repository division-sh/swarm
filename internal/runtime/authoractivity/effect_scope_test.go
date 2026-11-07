package authoractivity

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
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

func TestDeploymentEffectActivityRequiresOriginalParentWithoutPublicBundle(t *testing.T) {
	parent := channelnative.TransportParent{Kind: channelnative.TransportDeploymentRegistration,
		ID: uuid.NewString(), Revision: 3, BindingOperationID: uuid.NewString(), BindingOperationRevision: 2}
	for _, kind := range []string{"channel_native_setting", "channel_delivery", "channel_action_ack", "serve_registration"} {
		t.Run(kind, func(t *testing.T) {
			id := uuid.NewString()
			evidence := map[string]any{"authority_kind": kind, "authority_id": id, "transport_parent": parent}
			raw, err := json.Marshal(evidence)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := DeploymentEffectParent(kind, id, raw)
			if err != nil || decoded == nil || *decoded != parent {
				t.Fatalf("original deployment parent: %v", err)
			}
			draft := testDraft(KindEffectLifecycle, "launched", time.Now())
			draft.Projection.AuthorityKind, draft.Projection.AuthorityID = kind, id
			draft.EffectAuthorityEvidence, draft.Scope = raw, RuntimeScope(uuid.NewString())
			admitted, err := AdmitDraft(context.Background(), draft)
			if err != nil {
				t.Fatal(err)
			}
			occurrence := Occurrence{OccurrenceID: admitted.OccurrenceID, Kind: admitted.Kind,
				Version: admitted.Version, Transition: admitted.Transition, SourceOwner: admitted.SourceOwner,
				SourceIdentity: admitted.SourceIdentity, DedupKey: admitted.DedupKey, OccurredAt: admitted.OccurredAt,
				Scope: admitted.Scope, Projection: admitted.Projection, EffectAuthorityEvidence: admitted.EffectAuthorityEvidence}
			var output bytes.Buffer
			if err := Render(&output, []Occurrence{occurrence}, RenderOptions{Mode: RenderNDJSON}); err != nil {
				t.Fatal(err)
			}
			for _, forbidden := range []string{"bundle_hash", "transport_parent", "binding_operation_id", "EffectAuthorityEvidence"} {
				if strings.Contains(output.String(), forbidden) {
					t.Fatalf("readback exposed private or invented %s", forbidden)
				}
			}
			withoutParent := draft
			withoutParent.EffectAuthorityEvidence = nil
			if err := ValidateDraft(withoutParent); err == nil {
				t.Fatal("missing bundle selected deployment without parent")
			}
			withBundle := draft
			withBundle.Scope = BundleScope(draft.Scope.RuntimeInstanceID, "borrowed-bundle")
			if err := ValidateDraft(withBundle); err == nil {
				t.Fatal("deployment effect accepted an author bundle")
			}
			evidence["bundle_hash"] = ""
			raw, err = json.Marshal(evidence)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := DeploymentEffectParent(kind, id, raw); err == nil {
				t.Fatal("mixed evidence accepted even with empty authored coordinate")
			}
			if _, err := DeploymentEffectParent("channel_confirmation", id, raw); err == nil {
				t.Fatal("unreleased operation accepted deployment parent")
			}
		})
	}
}
