package effectpersistence

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/channelnative"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/registration"
	"github.com/division-sh/swarm/tests/support/testutil/packfixture"
	"github.com/google/uuid"
)

func TestRegisteredAcknowledgementNullBundleRequiresExactNativeRequest(t *testing.T) {
	parent := channelnative.TransportParent{Kind: "deployment_registration", ID: uuid.NewString(), Revision: 3,
		BindingOperationID: uuid.NewString(), BindingOperationRevision: 2}
	id := effects.RegisteredOperatorAcknowledgementID(parent.BindingOperationID, 1)
	destination := "15550000000@s.whatsapp.net"
	a := effects.Authority{Kind: effects.AuthorityChannelConfirmation, ID: id, ExecutionOwner: uuid.NewString(),
		LeaseExpiresAt: time.Now().Add(time.Minute), FenceGeneration: 3, ExecutionMode: effects.ExecutionModeLive,
		ChannelConfirmation: effects.ChannelConfirmationAuthority{TransportParent: &parent, EffectOperationID: id,
			OnboardingOperationID: parent.ID, OnboardingRevision: parent.Revision, PrincipalID: uuid.NewString(), BindingRevision: 1, Destination: destination}}
	ctx := authoractivity.WithScope(context.Background(), authoractivity.RuntimeScope(uuid.NewString()))
	req := effects.AuthorizeRequest{OperationID: id, Kind: effects.KindChannelConfirmation, Class: effects.EffectWriteOrUnknown,
		Adapter: "channel_confirmation_whatsapp", Transport: "in_process",
		RequestFingerprint: effects.Fingerprint([]byte(destination + "\nSwarm operator successfully connected"))}
	if bundle, err := externalEffectOperationBundle(ctx, a, req); err != nil || bundle != nil {
		t.Fatal("closed registered request did not select SQL NULL", bundle, err)
	}
	for _, mutate := range []func(*effects.AuthorizeRequest){
		func(r *effects.AuthorizeRequest) { r.OperationID = uuid.NewString() },
		func(r *effects.AuthorizeRequest) { r.Kind = effects.KindChannelDelivery },
		func(r *effects.AuthorizeRequest) { r.Class = effects.EffectReadOnly },
		func(r *effects.AuthorizeRequest) { r.Adapter = "channel_confirmation" },
		func(r *effects.AuthorizeRequest) { r.Transport = "http" },
		func(r *effects.AuthorizeRequest) {
			r.RequestFingerprint = effects.Fingerprint([]byte(destination + "\nanother message"))
		},
		func(r *effects.AuthorizeRequest) {
			r.RequestFingerprint = effects.Fingerprint([]byte("foreign@s.whatsapp.net\nSwarm operator successfully connected"))
		},
	} {
		changed := req
		mutate(&changed)
		if _, err := externalEffectOperationBundle(ctx, a, changed); err == nil {
			t.Fatal("registered acknowledgement permit admitted another request", changed)
		}
	}
	if _, err := externalEffectOperationBundle(context.Background(), a, req); err == nil {
		t.Fatal("bundle absence inferred runtime authority")
	}
	borrowed := authoractivity.WithScope(context.Background(), authoractivity.BundleScope(uuid.NewString(), "holder-bundle"))
	if _, err := externalEffectOperationBundle(borrowed, a, req); err == nil {
		t.Fatal("registered acknowledgement borrowed a holder bundle")
	}
	t.Run("exact Telegram HTTP request", func(t *testing.T) {
		installed := packfixture.ConnectorTool(t, "telegram", "telegram.send_interactive")
		request, err := registration.ChannelWriteFingerprint(installed.ToolID, installed.Tool, map[string]any{
			"chat_id": "12345", "text": effects.RegisteredOperatorAcknowledgementText,
			"reply_markup": map[string]any{"inline_keyboard": []any{}},
		})
		if err != nil {
			t.Fatal(err)
		}
		a.ChannelConfirmation.Destination, a.ChannelConfirmation.RegisteredRequest = "12345", string(request)
		req.Adapter, req.Transport, req.RequestFingerprint = "channel_confirmation", "http", effects.Fingerprint(request)
		if bundle, err := externalEffectOperationBundle(ctx, a, req); err != nil || bundle != nil {
			t.Fatal("exact Telegram request did not select SQL NULL", bundle, err)
		}
		for _, mutate := range []func(*effects.AuthorizeRequest){
			func(r *effects.AuthorizeRequest) { r.Adapter = "another_http_adapter" },
			func(r *effects.AuthorizeRequest) { r.Transport = "in_process" },
			func(r *effects.AuthorizeRequest) {
				r.RequestFingerprint = effects.Fingerprint([]byte("another request"))
			},
		} {
			changed := req
			mutate(&changed)
			if _, err := externalEffectOperationBundle(ctx, a, changed); err == nil {
				t.Fatal("Telegram permit admitted another request", changed)
			}
		}
		a.ChannelConfirmation.Destination = "67890"
		if _, err := externalEffectOperationBundle(ctx, a, req); err == nil {
			t.Fatal("Telegram permit admitted a foreign destination")
		}
	})
}
