package selected

import (
	"context"

	"github.com/division-sh/swarm/internal/sourceartifact"
)

func (s *AdmissionSnapshot) LoadRunRootedSource(ctx context.Context, runID string) (sourceartifact.RootedSource, error) {
	ctx, done, err := s.readContext(ctx)
	if err != nil {
		return sourceartifact.RootedSource{}, err
	}
	defer done()
	return s.store.LoadRunRootedSource(ctx, runID)
}

func (s *AdmissionSnapshot) LoadSelectedForkRootedSource(ctx context.Context, forkRunID string) (sourceartifact.RootedSource, error) {
	ctx, done, err := s.readContext(ctx)
	if err != nil {
		return sourceartifact.RootedSource{}, err
	}
	defer done()
	return s.store.LoadSelectedForkRootedSource(ctx, forkRunID)
}

func (s *AdmissionSnapshot) GetRootedSource(ctx context.Context, reference sourceartifact.RootAdmissionReference) (sourceartifact.RootedSource, error) {
	ctx, done, err := s.readContext(ctx)
	if err != nil {
		return sourceartifact.RootedSource{}, err
	}
	defer done()
	return s.store.GetRootedSource(ctx, reference)
}

func (s *AdmissionSnapshot) ResolveUnboundRootedSource(ctx context.Context, hash string) (sourceartifact.RootedSource, error) {
	ctx, done, err := s.readContext(ctx)
	if err != nil {
		return sourceartifact.RootedSource{}, err
	}
	defer done()
	return s.store.ResolveUnboundRootedSource(ctx, hash)
}
