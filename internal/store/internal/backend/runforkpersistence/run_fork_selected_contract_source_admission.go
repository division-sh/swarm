package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

type selectedForkSourceAdmission uint8

const (
	selectedForkUnfrozenSource selectedForkSourceAdmission = iota + 1
	selectedForkHistoricalSource
)

// The caller holds the source lifecycle lock. Its original freeze successor is
// lineage evidence, not the mutable executor for this independently bound child.
func admitSelectedForkSource(ctx context.Context, tx *sql.Tx, sourceRunID string, source runlifecycle.Snapshot, readSnapshot runForkLifecycleSnapshotLoader, postgres bool) (selectedForkSourceAdmission, error) {
	if source.RunID != sourceRunID {
		return 0, fmt.Errorf("selected-contract source lifecycle identity disagrees with requested source")
	}
	if err := source.Validate(); err != nil {
		return 0, fmt.Errorf("selected-contract source lifecycle: %w", err)
	}
	if source.State != runlifecycle.StateForked {
		if !runForkSelectedContractBranchSourceStatusSupported(string(source.State)) {
			return 0, fmt.Errorf("selected-contract fork source state %s is unsupported", source.State)
		}
		return selectedForkUnfrozenSource, nil
	}
	if readSnapshot == nil {
		return 0, fmt.Errorf("selected-contract frozen source requires canonical successor observation")
	}
	// Do not lock the original successor after the source: ordinary activation
	// locks child before source. Its immutable origin is sufficient here.
	successor, err := readSnapshot(ctx, tx, source.ContinuedAsRunID)
	if err != nil {
		return 0, fmt.Errorf("read selected-contract source freeze successor: %w", err)
	}
	if err := successor.Validate(); err != nil {
		return 0, fmt.Errorf("selected-contract source freeze successor: %w", err)
	}
	if successor.RunID != source.ContinuedAsRunID || successor.Origin.Kind() != runlifecycle.OriginForkMaterialization || successor.Origin.SourceRunID() != sourceRunID {
		return 0, fmt.Errorf("selected-contract frozen source has foreign freeze successor lineage")
	}
	point := runfork.RunForkPoint{Kind: successor.Origin.ForkPointKind(), Revision: successor.Origin.ForkRevision(), EventID: successor.Origin.SourceEventID()}
	resolve := resolveSQLiteRunForkRevisionPoint
	if postgres {
		resolve = resolveRunForkRevisionPoint
	}
	if _, err := resolveFixedRunForkRevisionPoint(ctx, tx, sourceRunID, point, resolve); err != nil {
		return 0, fmt.Errorf("selected-contract source freeze successor cut: %w", err)
	}
	return selectedForkHistoricalSource, nil
}
