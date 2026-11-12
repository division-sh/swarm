//go:build linux || darwin

package serveapp

import (
	"context"

	"github.com/division-sh/swarm/internal/channelprovider"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/sessionprovider"
)

type installedChannelBinding struct {
	facts        channelprovider.Binding
	registration cliapp.ChannelRegistrationFactory
	deployment   func(context.Context, sessionprovider.DeploymentConnectionOptions) (serveSessionConnection, error)
	runtime      func(context.Context, sessionprovider.RuntimeConnectionOptions, bool) (serveSessionConnection, error)
}

func openTelegramChannelRegistration(_ context.Context, opts cliapp.ChannelRegistrationOptions) (cliapp.ChannelRegistrationSession, error) {
	return &telegramDeploymentSession{opts: opts}, nil
}

func openWhatsAppChannelRegistration(_ context.Context, opts cliapp.ChannelRegistrationOptions) (cliapp.ChannelRegistrationSession, error) {
	return &deploymentChannelSession{opts: opts}, nil
}

func openWhatsAppDeploymentConnection(ctx context.Context, opts sessionprovider.DeploymentConnectionOptions) (serveSessionConnection, error) {
	connection, err := sessionprovider.OpenDeploymentConnection(ctx, opts)
	if connection == nil {
		return nil, err
	}
	return connection, err
}

func openWhatsAppRuntimeConnection(ctx context.Context, opts sessionprovider.RuntimeConnectionOptions, bootstrap bool) (serveSessionConnection, error) {
	var connection *sessionprovider.RuntimeConnection
	var err error
	if bootstrap {
		connection, err = sessionprovider.OpenRuntimeBootstrap(ctx, opts)
	} else {
		connection, err = sessionprovider.OpenRuntimeConnection(ctx, opts)
	}
	if connection == nil {
		return nil, err
	}
	return connection, err
}
