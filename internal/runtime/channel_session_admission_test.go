package runtime

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/sessionprovider/authority"
)

type unissuedChannelSessionOwner struct{}

func (unissuedChannelSessionOwner) AdmitSessionAccount(context.Context, operatorchannel.SessionAccountAdmission) (authority.Admission, error) {
	return authority.Admission{}, nil
}

func TestNativePublicationCannotUseMissingOrUnissuedSDKAuthority(t *testing.T) {
	activation := channelonboarding.CompiledActivation{OnboardingOperationID: "11111111-1111-4111-8111-111111111111", OnboardingRevision: 1,
		SessionAccount: operatorchannel.SessionAccountAdmission{Provider: "whatsapp", ConnectionID: "22222222-2222-4222-8222-222222222222",
			AccountRef: "15551234567@s.whatsapp.net", AdmissionID: "33333333-3333-4333-8333-333333333333", Revision: 1}}
	rt := &Runtime{}
	var unavailable *operatorchannel.SessionProviderUnavailableError
	if err := rt.validateNativeChannelPublication(context.Background(), activation); !errors.As(err, &unavailable) {
		t.Fatal("missing SDK issuer did not refuse native publication", err)
	}
	if err := rt.bindChannelSessionAdmission(&channelSessionAdmission{owner: unissuedChannelSessionOwner{}}); err != nil {
		t.Fatal(err)
	}
	if err := rt.validateNativeChannelPublication(context.Background(), activation); !errors.Is(err, channelonboarding.ErrRevisionConflict) {
		t.Fatal("caller-defined account reader issued native publication authority", err)
	}
}

func TestChannelSessionInstallationKeepsOneOriginalIssuer(t *testing.T) {
	rt := &Runtime{}
	first := &channelSessionAdmission{owner: unissuedChannelSessionOwner{}}
	if err := rt.bindChannelSessionAdmission(first); err != nil {
		t.Fatal(err)
	}
	if err := rt.bindChannelSessionAdmission(first); err != nil {
		t.Fatal("same original installation did not survive visibility publication", err)
	}
	if err := rt.bindChannelSessionAdmission(&channelSessionAdmission{owner: unissuedChannelSessionOwner{}}); err == nil || rt.channelSessions.Load() != first {
		t.Fatal("runtime replaced its original native issuer", err)
	}
	manager := &RuntimeContextManager{contexts: map[string]*runtimeContextEntry{"source": {runtime: &Runtime{}}}}
	if err := manager.BindChannelSessionAdmissionOwner(unissuedChannelSessionOwner{}); err != nil {
		t.Fatal(err)
	}
	if err := manager.BindChannelSessionAdmissionOwner(unissuedChannelSessionOwner{}); err == nil {
		t.Fatal("manager replaced its original native issuer")
	}
	incoming := &runtimeContextEntry{runtime: &Runtime{}}
	if err := manager.publishRuntimeContextVisibilityLocked(runtimeContextVisibilityUpdate{entry: incoming, state: RuntimeContextStateLoaded}); err != nil || incoming.runtime.channelSessions.Load() != manager.channelSessions {
		t.Fatal("replacement runtime did not retain the original issuer", err)
	}
	foreign := &runtimeContextEntry{runtime: &Runtime{}}
	if err := foreign.runtime.bindChannelSessionAdmission(&channelSessionAdmission{owner: unissuedChannelSessionOwner{}}); err != nil {
		t.Fatal(err)
	}
	if err := manager.publishRuntimeContextVisibilityLocked(runtimeContextVisibilityUpdate{entry: foreign, state: RuntimeContextStateLoaded}); err == nil || foreign.state == RuntimeContextStateLoaded {
		t.Fatal("foreign issuer became a visible executable context", err)
	}
}
