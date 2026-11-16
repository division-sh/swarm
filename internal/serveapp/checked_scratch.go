package serveapp

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/store/devscratch"
)

func prepareCheckedServeScratch(ctx context.Context, req runtimeCompositionRequest, stage *cliapp.ServeConstructionChecks) (authority *devscratch.EpochAuthority, registration *cliapp.ServeProjectContextRegistration, retErr error) {
	scratch, err := cliapp.ResolveDevScratch(req.LocalState)
	if err != nil {
		return nil, nil, err
	}
	authority, err = devscratch.RetainForChecks(scratch.Coordinate)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, authority.AbortBeforeStoreOpen())
		}
	}()
	target, err := bootverify.SQLiteConstructionTarget(scratch.Selection.SQLitePath)
	if err != nil {
		return authority, nil, err
	}
	if _, err := stage.InspectConstruction(ctx, target); err != nil {
		return authority, nil, err
	}
	if err := authority.BeginEpoch(); err != nil {
		return authority, nil, err
	}
	grant, err := authority.RegistrationGrant()
	if err != nil {
		return authority, nil, err
	}
	root, err := cliapp.NewInvocationRoot(req.Repo)
	if err != nil {
		return authority, nil, err
	}
	registration, err = cliapp.PrepareServeProjectContextRegistration(root, req.Options, req.ResolvedPaths, grant)
	if err != nil {
		return authority, nil, err
	}
	if err := authority.PrepareFreshStore(); err != nil {
		return authority, nil, err
	}
	return authority, registration, nil
}
