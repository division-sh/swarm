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
