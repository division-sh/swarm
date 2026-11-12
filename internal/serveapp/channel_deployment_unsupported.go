//go:build !linux && !darwin

package serveapp

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/publicingress"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/sessionprovider"
	"github.com/division-sh/swarm/internal/store/selected"
)

func qualifyWhatsAppPlan(packs.SatisfactionPlan) error {
	return errors.New("native channel registration requires Linux or macOS")
}

func OpenChannelRegistration(context.Context, cliapp.ChannelRegistrationOptions) (cliapp.ChannelRegistrationSession, error) {
	return nil, errors.New("native channel registration requires Linux or macOS")
}

func openChannelRegistration(ctx context.Context, opts cliapp.ChannelRegistrationOptions, _ installedChannelBinding) (cliapp.ChannelRegistrationSession, error) {
	return OpenChannelRegistration(ctx, opts)
}

func openTelegramChannelRegistration(ctx context.Context, opts cliapp.ChannelRegistrationOptions) (cliapp.ChannelRegistrationSession, error) {
	return OpenChannelRegistration(ctx, opts)
}

func openWhatsAppChannelRegistration(ctx context.Context, opts cliapp.ChannelRegistrationOptions) (cliapp.ChannelRegistrationSession, error) {
	return OpenChannelRegistration(ctx, opts)
}

func openWhatsAppDeploymentConnection(context.Context, sessionprovider.DeploymentConnectionOptions) (serveSessionConnection, error) {
	return nil, errors.New("native channel registration requires Linux or macOS")
}

func openWhatsAppRuntimeConnection(context.Context, sessionprovider.RuntimeConnectionOptions, bool) (serveSessionConnection, error) {
	return nil, errors.New("native channel registration requires Linux or macOS")
}

func deploymentChannelHandlers(string, string, string, *selected.Owner, *worklifetime.Process, startupownership.ProcessCapability, func(context.Context, string, string) bool, deploymentRuntimeOptions) (apiv1.MethodHandler, publicingress.PendingDeploymentClaim, func(context.Context) error, func() error) {
	return nil, nil, func(context.Context) error { return nil }, func() error { return nil }
}
