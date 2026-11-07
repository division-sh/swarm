package effectpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/channelnative"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/google/uuid"
)

func TestStoredEffectScopeDoesNotInferDeploymentFromMissingBundle(t *testing.T) {
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
			actual, err := validateExternalEffectStoredScope(kind, id, raw, sql.NullString{})
			if err != nil || actual != parent {
				t.Fatalf("exact original parent: %v", err)
			}
			for _, bundle := range []string{"", "holder-bundle"} {
				if _, err := validateExternalEffectStoredScope(kind, id, raw, sql.NullString{String: bundle, Valid: true}); err == nil {
					t.Fatal("deployment operation accepted authored bundle column")
				}
			}
			if _, err := validateExternalEffectStoredScope(kind, uuid.NewString(), raw, sql.NullString{}); err == nil {
				t.Fatal("changed operation adopted original parent")
			}
			delete(evidence, "transport_parent")
			raw, err = json.Marshal(evidence)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := validateExternalEffectStoredScope(kind, id, raw, sql.NullString{}); err == nil {
				t.Fatal("missing parent and bundle selected deployment")
			}
			if actual, err := validateExternalEffectStoredScope(kind, id, raw, sql.NullString{String: "author-bundle", Valid: true}); err != nil || actual != (channelnative.TransportParent{}) {
				t.Fatalf("authored operation lost exact bundle: %v", err)
			}
		})
	}
}

func TestDeploymentRegistrationRetainsCanonicalEffectOperationIdentity(t *testing.T) {
	parent := channelnative.TransportParent{Kind: channelnative.TransportDeploymentRegistration,
		ID: uuid.NewString(), Revision: 3, BindingOperationID: uuid.NewString(), BindingOperationRevision: 2}
	intent := uuid.NewString()
	authority := runtimeeffects.Authority{Kind: runtimeeffects.AuthorityServeRegistration, ID: intent,
		ExecutionOwner: "registration", FenceGeneration: 1, LeaseExpiresAt: time.Now().Add(time.Minute),
		ExecutionMode: runtimeeffects.ExecutionModeLive, ServeRegistration: runtimeeffects.ServeRegistrationAuthority{
			Parent: parent, IntentID: intent, StartupAuthorityID: uuid.NewString(), StartupStateVersion: 1,
			OnboardingOperationID: parent.ID, OnboardingRevision: parent.Revision}}
	ctx := authoractivity.WithScope(context.Background(), authoractivity.RuntimeScope(uuid.NewString()))
	req := runtimeeffects.AuthorizeRequest{OperationID: uuid.NewString(), Kind: runtimeeffects.KindServeRegistration,
		Class: runtimeeffects.EffectWriteOrUnknown, Adapter: "provider_registration", Transport: "http"}
	if bundle, err := externalEffectOperationBundle(ctx, authority, req); err != nil || bundle != nil {
		t.Fatal("registration replaced its canonical effect ID with the intent ID", bundle, err)
	}
	req.Kind = runtimeeffects.KindChannelDelivery
	if _, err := externalEffectOperationBundle(ctx, authority, req); err == nil {
		t.Fatal("registration accepted another operation kind")
	}
}
