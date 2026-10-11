package serveapp

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

type serveReadinessActivationProbe struct {
	channelonboarding.Store
	activation channelonboarding.ConnectedChannelActivation
	err        error
	onRead     func()
	reads      int
}

func (p *serveReadinessActivationProbe) GetConnectedChannelActivation(context.Context, string) (channelonboarding.ConnectedChannelActivation, error) {
	p.reads++
	if p.onRead != nil {
		p.onRead()
	}
	return p.activation, p.err
}

func TestServeReadinessPendingCeremonyDoesNotObservePredecessor(t *testing.T) {
	for _, posture := range []channelonboarding.ActivationPosture{
		channelonboarding.ActivationSessionConnection, channelonboarding.ActivationWebhookRegistration,
	} {
		t.Run(string(posture), func(t *testing.T) {
			for _, phase := range []channelonboarding.Phase{
				channelonboarding.PhaseActivatingProvider, channelonboarding.PhaseAwaitingExternalIdentity,
				channelonboarding.PhaseAwaitingOperatorConfirmation,
			} {
				t.Run(string(phase), func(t *testing.T) {
					probe := &serveReadinessActivationProbe{activation: channelonboarding.ConnectedChannelActivation{
						OperationID: "predecessor", Posture: posture, Status: channelonboarding.ActivationCurrent}}
					owner := serveReadinessProjectionProbe(probe)
					result, found, err := owner.ProjectConnectedChannelReadiness(context.Background(),
						channelonboarding.Operation{OperationID: "pending", Posture: posture, Phase: phase}, channelonboarding.Candidate{})
					if err != nil || found || !reflect.DeepEqual(result, channelonboarding.ConnectedChannelReadiness{}) || probe.reads != 1 {
						t.Fatal("pending ceremony consumed or disclosed predecessor readiness", result, found, err, probe.reads)
					}
				})
			}
		})
	}
}

func TestServeReadinessPendingCeremonyPreservesReadFailuresAndCancellation(t *testing.T) {
	cause := errors.New("selected activation read failed")
	for _, test := range []struct {
		name   string
		err    error
		cancel string
		want   error
		reads  int
	}{
		{name: "observation error", err: cause, want: cause, reads: 1},
		{name: "pre-canceled", cancel: "before", want: context.Canceled},
		{name: "canceled during predecessor lookup", cancel: "during", want: context.Canceled, reads: 1},
		{name: "canceled during absent lookup", cancel: "during", err: channelonboarding.ErrNotFound, want: context.Canceled, reads: 1},
		{name: "canceled during failed lookup", cancel: "during", err: cause, want: context.Canceled, reads: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			probe := &serveReadinessActivationProbe{activation: channelonboarding.ConnectedChannelActivation{OperationID: "predecessor"}, err: test.err}
			if test.cancel == "before" {
				cancel()
			} else if test.cancel == "during" {
				probe.onRead = cancel
			}
			result, found, err := serveReadinessProjectionProbe(probe).ProjectConnectedChannelReadiness(ctx,
				channelonboarding.Operation{OperationID: "pending"}, channelonboarding.Candidate{})
			if !errors.Is(err, test.want) || found || !reflect.DeepEqual(result, channelonboarding.ConnectedChannelReadiness{}) || probe.reads != test.reads {
				t.Fatal("failed or canceled lookup became pending readiness", result, found, err, probe.reads)
			}
			if test.err == cause && !errors.Is(err, cause) {
				t.Fatal("lookup lost its original cause", err)
			}
		})
	}
}

func serveReadinessProjectionProbe(probe *serveReadinessActivationProbe) *serveConnectedChannelReadiness {
	// These dependency sentinels grant no authority. A pending ceremony must
	// stop at its exact activation lookup, before any downstream owner is used.
	return &serveConnectedChannelReadiness{store: probe, manager: &runtimepkg.RuntimeContextManager{},
		identities: &operatorchannel.Service{}, credentials: &credentials.SnapshotOwner{},
		effects: struct{ runtimeeffects.OutcomeStore }{}}
}
