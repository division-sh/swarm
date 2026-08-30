package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	runtimedata "github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/sourceartifact"
	storedurabledata "github.com/division-sh/swarm/internal/store/internal/durabledata"
	storesourceartifact "github.com/division-sh/swarm/internal/store/internal/sourceartifact"
)

func (s *PostgresStore) EnsureSourceArtifactWithData(ctx context.Context, artifact *sourceartifact.AdmittedSourceArtifact, catalog runtimedata.Catalog) (sourceartifact.EnsureResult, error) {
	if s == nil || s.backend == nil || s.sourceArtifactOwner == nil || s.durableDataOwner == nil {
		return sourceartifact.EnsureResult{}, fmt.Errorf("postgres source artifact and data catalog owners are required")
	}
	if artifact == nil || artifact.BundleHash() != catalog.BundleHash {
		return sourceartifact.EnsureResult{}, fmt.Errorf("source artifact and data catalog hashes must match")
	}
	var result sourceartifact.EnsureResult
	err := s.backend.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		result, err = storesourceartifact.EnsurePostgresSourceArtifactTx(txctx, tx, artifact, time.Now().UTC())
		if err != nil {
			return err
		}
		return storedurabledata.RegisterCatalogTx(s.durableDataOwner, txctx, tx, catalog, time.Now().UTC())
	})
	return result, err
}

func (s *SQLiteRuntimeStore) EnsureSourceArtifactWithData(ctx context.Context, artifact *sourceartifact.AdmittedSourceArtifact, catalog runtimedata.Catalog) (sourceartifact.EnsureResult, error) {
	if s == nil || s.backend == nil || s.sourceArtifactOwner == nil || s.durableDataOwner == nil {
		return sourceartifact.EnsureResult{}, fmt.Errorf("sqlite source artifact and data catalog owners are required")
	}
	if artifact == nil || artifact.BundleHash() != catalog.BundleHash {
		return sourceartifact.EnsureResult{}, fmt.Errorf("source artifact and data catalog hashes must match")
	}
	var result sourceartifact.EnsureResult
	err := s.backend.RunTransaction(ctx, "sqlite source artifact and data catalog ensure", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		result, err = storesourceartifact.EnsureSQLiteSourceArtifactTx(txctx, tx, artifact, time.Now().UTC())
		if err != nil {
			return err
		}
		return storedurabledata.RegisterCatalogTx(s.durableDataOwner, txctx, tx, catalog, time.Now().UTC())
	})
	return result, err
}

func (s *PostgresStore) ExecuteDataSourceOperation(ctx context.Context, command runtimedata.SourceCommand) (runtimedata.SourceOperationResult, error) {
	return s.durableDataOwner.ExecuteSourceOperation(ctx, command)
}

func (s *SQLiteRuntimeStore) ExecuteDataSourceOperation(ctx context.Context, command runtimedata.SourceCommand) (runtimedata.SourceOperationResult, error) {
	return s.durableDataOwner.ExecuteSourceOperation(ctx, command)
}

func (s *PostgresStore) ShowDataResource(ctx context.Context, bundleHash string, ref runtimedata.DeclarationRef) (runtimedata.ResourceSnapshot, error) {
	return s.durableDataOwner.Show(ctx, bundleHash, ref)
}

func (s *SQLiteRuntimeStore) ShowDataResource(ctx context.Context, bundleHash string, ref runtimedata.DeclarationRef) (runtimedata.ResourceSnapshot, error) {
	return s.durableDataOwner.Show(ctx, bundleHash, ref)
}

func (s *PostgresStore) PruneDataResource(ctx context.Context, command runtimedata.PruneCommand) (runtimedata.PruneOperationResult, error) {
	return s.durableDataOwner.Prune(ctx, command)
}

func (s *SQLiteRuntimeStore) PruneDataResource(ctx context.Context, command runtimedata.PruneCommand) (runtimedata.PruneOperationResult, error) {
	return s.durableDataOwner.Prune(ctx, command)
}

func (s *PostgresStore) ListDataDeclarationSummaries(ctx context.Context, bundleHash string) ([]runtimedata.DeclarationSummary, error) {
	return s.durableDataOwner.ListDeclarationSummaries(ctx, bundleHash)
}

func (s *SQLiteRuntimeStore) ListDataDeclarationSummaries(ctx context.Context, bundleHash string) ([]runtimedata.DeclarationSummary, error) {
	return s.durableDataOwner.ListDeclarationSummaries(ctx, bundleHash)
}

func (s *PostgresStore) ListDataVersionSummaries(ctx context.Context, ref runtimedata.DeclarationRef, afterSequence uint64, limit int) ([]runtimedata.VersionSummary, error) {
	return s.durableDataOwner.ListVersionSummaries(ctx, ref, afterSequence, limit)
}

func (s *SQLiteRuntimeStore) ListDataVersionSummaries(ctx context.Context, ref runtimedata.DeclarationRef, afterSequence uint64, limit int) ([]runtimedata.VersionSummary, error) {
	return s.durableDataOwner.ListVersionSummaries(ctx, ref, afterSequence, limit)
}

func (s *PostgresStore) ResolveDataVersionSummary(ctx context.Context, ref runtimedata.DeclarationRef, selector runtimedata.VersionSelector) (runtimedata.VersionSummary, error) {
	return s.durableDataOwner.ResolveVersionSummary(ctx, ref, selector)
}

func (s *SQLiteRuntimeStore) ResolveDataVersionSummary(ctx context.Context, ref runtimedata.DeclarationRef, selector runtimedata.VersionSelector) (runtimedata.VersionSummary, error) {
	return s.durableDataOwner.ResolveVersionSummary(ctx, ref, selector)
}

func (s *PostgresStore) ResolveDataVersionPayload(ctx context.Context, ref runtimedata.DeclarationRef, selector runtimedata.VersionSelector) (runtimedata.VersionSummary, runtimedata.Version, error) {
	return s.durableDataOwner.ResolveVersionPayload(ctx, ref, selector)
}

func (s *SQLiteRuntimeStore) ResolveDataVersionPayload(ctx context.Context, ref runtimedata.DeclarationRef, selector runtimedata.VersionSelector) (runtimedata.VersionSummary, runtimedata.Version, error) {
	return s.durableDataOwner.ResolveVersionPayload(ctx, ref, selector)
}

func (s *PostgresStore) ListDataVersionProvenance(ctx context.Context, versionID runtimedata.VersionID, afterSequence uint64, limit int) ([]runtimedata.Provenance, error) {
	return s.durableDataOwner.ListVersionProvenance(ctx, versionID, afterSequence, limit)
}

func (s *SQLiteRuntimeStore) ListDataVersionProvenance(ctx context.Context, versionID runtimedata.VersionID, afterSequence uint64, limit int) ([]runtimedata.Provenance, error) {
	return s.durableDataOwner.ListVersionProvenance(ctx, versionID, afterSequence, limit)
}

func (s *PostgresStore) ListDataPins(ctx context.Context, versionID runtimedata.VersionID, afterRunID string, limit int) ([]runtimedata.Pin, error) {
	return s.durableDataOwner.ListPins(ctx, versionID, afterRunID, limit)
}

func (s *SQLiteRuntimeStore) ListDataPins(ctx context.Context, versionID runtimedata.VersionID, afterRunID string, limit int) ([]runtimedata.Pin, error) {
	return s.durableDataOwner.ListPins(ctx, versionID, afterRunID, limit)
}

func (s *PostgresStore) ListDataHeadHistory(ctx context.Context, ref runtimedata.DeclarationRef, afterRevision uint64, limit int) ([]runtimedata.HeadHistory, error) {
	return s.durableDataOwner.ListHeadHistory(ctx, ref, afterRevision, limit)
}

func (s *SQLiteRuntimeStore) ListDataHeadHistory(ctx context.Context, ref runtimedata.DeclarationRef, afterRevision uint64, limit int) ([]runtimedata.HeadHistory, error) {
	return s.durableDataOwner.ListHeadHistory(ctx, ref, afterRevision, limit)
}

func (s *PostgresStore) LoadDataSourceOperation(ctx context.Context, id string) (runtimedata.SourceOperationRecord, error) {
	return s.durableDataOwner.LoadSourceOperation(ctx, id)
}

func (s *SQLiteRuntimeStore) LoadDataSourceOperation(ctx context.Context, id string) (runtimedata.SourceOperationRecord, error) {
	return s.durableDataOwner.LoadSourceOperation(ctx, id)
}

func (s *PostgresStore) LoadDataPruneOperation(ctx context.Context, id string) (runtimedata.PruneOperationResult, error) {
	return s.durableDataOwner.LoadPruneOperation(ctx, id)
}

func (s *SQLiteRuntimeStore) LoadDataPruneOperation(ctx context.Context, id string) (runtimedata.PruneOperationResult, error) {
	return s.durableDataOwner.LoadPruneOperation(ctx, id)
}

func (s *PostgresStore) LoadDataPruneOperationPins(ctx context.Context, id string) ([]runtimedata.Pin, error) {
	return s.durableDataOwner.LoadPruneOperationPins(ctx, id)
}

func (s *SQLiteRuntimeStore) LoadDataPruneOperationPins(ctx context.Context, id string) ([]runtimedata.Pin, error) {
	return s.durableDataOwner.LoadPruneOperationPins(ctx, id)
}

func (s *PostgresStore) LoadDataPins(ctx context.Context, versionID runtimedata.VersionID) ([]runtimedata.Pin, error) {
	return s.durableDataOwner.LoadPins(ctx, versionID)
}

func (s *SQLiteRuntimeStore) LoadDataPins(ctx context.Context, versionID runtimedata.VersionID) ([]runtimedata.Pin, error) {
	return s.durableDataOwner.LoadPins(ctx, versionID)
}

func (s *PostgresStore) LoadDataHeadHistory(ctx context.Context, ref runtimedata.DeclarationRef) ([]runtimedata.HeadHistory, error) {
	return s.durableDataOwner.LoadHeadHistory(ctx, ref)
}

func (s *SQLiteRuntimeStore) LoadDataHeadHistory(ctx context.Context, ref runtimedata.DeclarationRef) ([]runtimedata.HeadHistory, error) {
	return s.durableDataOwner.LoadHeadHistory(ctx, ref)
}

func (s *PostgresStore) LoadDataRunCreationOperation(ctx context.Context, runID string) (runtimedata.RunCreationOperationRecord, error) {
	return s.durableDataOwner.LoadRunCreationOperation(ctx, runID)
}

func (s *SQLiteRuntimeStore) LoadDataRunCreationOperation(ctx context.Context, runID string) (runtimedata.RunCreationOperationRecord, error) {
	return s.durableDataOwner.LoadRunCreationOperation(ctx, runID)
}

func (s *PostgresStore) LoadRunResourceAccess(ctx context.Context, runID string, declarations []runtimedata.DeclarationRef) ([]runtimedata.ResourceAccessItem, error) {
	return s.durableDataOwner.LoadRunResourceAccess(ctx, runID, declarations)
}

func (s *SQLiteRuntimeStore) LoadRunResourceAccess(ctx context.Context, runID string, declarations []runtimedata.DeclarationRef) ([]runtimedata.ResourceAccessItem, error) {
	return s.durableDataOwner.LoadRunResourceAccess(ctx, runID, declarations)
}
