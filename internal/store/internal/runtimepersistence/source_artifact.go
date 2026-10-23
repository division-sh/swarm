package runtimepersistence

import (
	"context"

	"github.com/division-sh/swarm/internal/sourceartifact"
)

func (s *PostgresStore) EnsureSourceArtifact(ctx context.Context, artifact *sourceartifact.AdmittedSourceArtifact) (sourceartifact.EnsureResult, error) {
	return s.sourceArtifactOwner.EnsureSourceArtifact(ctx, artifact)
}

func (s *PostgresStore) GetSourceArtifact(ctx context.Context, bundleHash string) (sourceartifact.Persisted, error) {
	return s.sourceArtifactOwner.GetSourceArtifact(ctx, bundleHash)
}

func (s *SQLiteRuntimeStore) EnsureSourceArtifact(ctx context.Context, artifact *sourceartifact.AdmittedSourceArtifact) (sourceartifact.EnsureResult, error) {
	return s.sourceArtifactOwner.EnsureSourceArtifact(ctx, artifact)
}

func (s *SQLiteRuntimeStore) GetSourceArtifact(ctx context.Context, bundleHash string) (sourceartifact.Persisted, error) {
	return s.sourceArtifactOwner.GetSourceArtifact(ctx, bundleHash)
}

func (s *PostgresStore) EnsureRootedSource(ctx context.Context, source sourceartifact.RootedSource) (sourceartifact.RootedEnsureResult, error) {
	return s.sourceArtifactOwner.EnsureRootedSource(ctx, source)
}

func (s *SQLiteRuntimeStore) EnsureRootedSource(ctx context.Context, source sourceartifact.RootedSource) (sourceartifact.RootedEnsureResult, error) {
	return s.sourceArtifactOwner.EnsureRootedSource(ctx, source)
}

func (s *PostgresStore) GetRootedSource(ctx context.Context, reference sourceartifact.RootAdmissionReference) (sourceartifact.RootedSource, error) {
	return s.sourceArtifactOwner.GetRootedSource(ctx, reference)
}

func (s *SQLiteRuntimeStore) GetRootedSource(ctx context.Context, reference sourceartifact.RootAdmissionReference) (sourceartifact.RootedSource, error) {
	return s.sourceArtifactOwner.GetRootedSource(ctx, reference)
}

func (s *PostgresStore) ResolveUnboundRootedSource(ctx context.Context, hash string) (sourceartifact.RootedSource, error) {
	return s.sourceArtifactOwner.ResolveUnboundRootedSource(ctx, hash)
}

func (s *SQLiteRuntimeStore) ResolveUnboundRootedSource(ctx context.Context, hash string) (sourceartifact.RootedSource, error) {
	return s.sourceArtifactOwner.ResolveUnboundRootedSource(ctx, hash)
}

func (s *PostgresStore) LoadRunRootedSource(ctx context.Context, runID string) (sourceartifact.RootedSource, error) {
	return s.runLifecyclePostgresOwner.LoadRunRootedSource(ctx, runID)
}

func (s *SQLiteRuntimeStore) LoadRunRootedSource(ctx context.Context, runID string) (sourceartifact.RootedSource, error) {
	return s.runLifecycleSQLiteOwner.LoadRunRootedSource(ctx, runID)
}

func (s *PostgresStore) LoadSelectedForkRootedSource(ctx context.Context, forkRunID string) (sourceartifact.RootedSource, error) {
	return s.runForkPostgresOwner.LoadSelectedForkRootedSource(ctx, forkRunID)
}

func (s *SQLiteRuntimeStore) LoadSelectedForkRootedSource(ctx context.Context, forkRunID string) (sourceartifact.RootedSource, error) {
	return s.runForkSQLiteOwner.LoadSelectedForkRootedSource(ctx, forkRunID)
}
