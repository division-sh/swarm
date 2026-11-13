//go:build !linux && !darwin

package serveapp

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/sessionprovider"
)

func qualifyWhatsAppPlan(packs.SatisfactionPlan) error {
	return errors.New("native channel registration requires Linux or macOS")
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
