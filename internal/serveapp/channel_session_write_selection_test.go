//go:build linux || darwin

package serveapp

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/google/uuid"
)

func TestServeSessionWriteSelectionUsesOriginalOwnerBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, op, _ := pairedServeSessionObserver(t, backend)
			plan, err := packs.NewOutboundBindingPlan("owned_native_write", f.candidate.Plan, "100000000002@s.whatsapp.net", nil)
			if err != nil {
				t.Fatal(err)
			}
			// This qualifies the real adapter's private execution handle. Other
			// activation facts remain component inputs, not a served send proof.
			executor, err := selectServeChannelWrite(f.ctx, f.adapter, op, plan)
			if err != nil || reflect.ValueOf(executor).IsZero() || f.selections.Load() != 2 {
				t.Fatal("owned paired connection did not select execution, or reconnected", err)
			}
			activation := channelonboarding.ConnectedChannelActivation{OperationID: op.OperationID, PrincipalID: op.PrincipalID,
				SessionAccount: op.SessionAccount, Coordinate: op.Coordinate, Revision: op.ActivationRevision, BindingRevision: op.BindingRevision}
			dispatcher := &serveChannelDeliveryDispatcher{activations: f.adapter.store, sessions: f.adapter}
			if _, err := dispatcher.channelWriteExecutor(f.ctx, activation, plan); err != nil {
				t.Fatal("delivery did not consume the original native selector", err)
			}
			for _, mutate := range []func(*channelonboarding.Operation){
				func(op *channelonboarding.Operation) { op.Revision++ },
				func(op *channelonboarding.Operation) { op.PrincipalID = uuid.NewString() },
				func(op *channelonboarding.Operation) { op.SessionAccount.AdmissionID = uuid.NewString() },
				func(op *channelonboarding.Operation) { op.Coordinate.ContextPublicationGeneration++ },
				func(op *channelonboarding.Operation) { op.Coordinate.TargetGeneration++ },
				func(op *channelonboarding.Operation) { op.Interface.ChannelManifestHash = "foreign" },
				func(op *channelonboarding.Operation) { op.Posture = channelonboarding.ActivationWebhookRegistration },
			} {
				changed := op
				mutate(&changed)
				value, err := selectServeChannelWrite(f.ctx, f.adapter, changed, plan)
				if err == nil || !reflect.ValueOf(value).IsZero() {
					t.Fatal("write selection adopted contradictory selected authority", err)
				}
			}
			changed := activation
			changed.SessionAccount.AdmissionID = uuid.NewString()
			if _, err := dispatcher.channelWriteExecutor(f.ctx, changed, plan); !errors.Is(err, channelonboarding.ErrRevisionConflict) {
				t.Fatal("delivery did not join its exact activation account", err)
			}
			var unavailable *operatorchannel.SessionProviderUnavailableError
			if _, err := selectServeChannelWrite(f.ctx, nil, op, plan); !errors.As(err, &unavailable) {
				t.Fatal("uninstalled native execution granted permission", err)
			}
			caller, cancel := context.WithCancel(f.ctx)
			cancel()
			if _, err := selectServeChannelWrite(caller, f.adapter, op, plan); !errors.Is(err, context.Canceled) {
				t.Fatal("canceled selection disclosed execution", err)
			}
			if err := f.adapter.connection(op.OperationID).Close(f.ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := selectServeChannelWrite(f.ctx, f.adapter, op, plan); err == nil || f.selections.Load() != 2 {
				t.Fatal("closed owner was used or automatically reopened", err)
			}
		})
	}
}
