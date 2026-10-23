//go:build !linux && !darwin

package serveapp

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/publicingress"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/selected"
)

func OpenChannelRegistration(context.Context, cliapp.ChannelRegistrationOptions) (cliapp.ChannelRegistrationSession, error) {
	return nil, errors.New("native channel registration requires Linux or macOS")
}

func deploymentChannelHandlers(string, string, string, *selected.Owner, *worklifetime.Process, startupownership.ProcessCapability, func(context.Context, string, string) bool) (apiv1.MethodHandler, publicingress.PendingDeploymentClaim, func() error) {
	return nil, nil, func() error { return nil }
}
