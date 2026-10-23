package selected

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/sourceartifact"
)

func (s *AdmissionSnapshot) LoadRunRootedSource(ctx context.Context, runID string) (sourceartifact.RootedSource, error) {
	return s.readRootedSource(ctx, func(ctx context.Context) (sourceartifact.RootedSource, error) {
		return s.store.LoadRunRootedSource(ctx, runID)
	})
}

func (s *AdmissionSnapshot) LoadSelectedForkRootedSource(ctx context.Context, forkRunID string) (sourceartifact.RootedSource, error) {
	return s.readRootedSource(ctx, func(ctx context.Context) (sourceartifact.RootedSource, error) {
		return s.store.LoadSelectedForkRootedSource(ctx, forkRunID)
	})
}

func (s *AdmissionSnapshot) GetRootedSource(ctx context.Context, reference sourceartifact.RootAdmissionReference) (sourceartifact.RootedSource, error) {
	return s.readRootedSource(ctx, func(ctx context.Context) (sourceartifact.RootedSource, error) {
		return s.store.GetRootedSource(ctx, reference)
	})
}

func (s *AdmissionSnapshot) ResolveUnboundRootedSource(ctx context.Context, hash string) (sourceartifact.RootedSource, error) {
	return s.readRootedSource(ctx, func(ctx context.Context) (sourceartifact.RootedSource, error) {
		return s.store.ResolveUnboundRootedSource(ctx, hash)
	})
}

func (s *AdmissionSnapshot) readRootedSource(caller context.Context, read func(context.Context) (sourceartifact.RootedSource, error)) (sourceartifact.RootedSource, error) {
	ctx, done, err := s.readContext(caller)
	if err != nil {
		return sourceartifact.RootedSource{}, err
	}
	defer done()
	rooted, err := read(ctx)
	if err := errors.Join(err, ctx.Err(), caller.Err()); err != nil {
		return sourceartifact.RootedSource{}, err
	}
	return rooted, nil
}
