package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	data "github.com/division-sh/swarm/internal/store/internal/durabledata"
)

type ResourceReceiptFaultCase = data.ReceiptFaultCase

type resourceReceiptFaultRow interface {
	CapturedReceipt() *data.CapturedResourceReceipt
}

type ResourceReceiptFault struct {
	mu              sync.Mutex
	selected        any
	row             resourceReceiptFaultRow
	postgres        bool
	cleanupRequired bool
}

func CaptureResourceReceiptFaultForTest(ctx context.Context, selected any, id, child string, kind ResourceReceiptFaultCase) (*ResourceReceiptFault, error) {
	return captureResourceReceiptFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) (resourceReceiptFaultRow, error) {
		return data.CaptureReceiptCorruptionTx(ctx, tx, id, child, kind)
	})
}

func CaptureImportProvenanceRemovalForTest(ctx context.Context, selected any, source string) (*ResourceReceiptFault, error) {
	return captureResourceReceiptFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) (resourceReceiptFaultRow, error) {
		return data.CaptureImportProvenanceRemovalTx(ctx, tx, source)
	})
}

func CaptureResourcePayloadCorruptionForTest(ctx context.Context, selected any, version, kind string) (*ResourceReceiptFault, error) {
	return captureResourceReceiptFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) (resourceReceiptFaultRow, error) {
		return data.CaptureResourcePayloadCorruptionTx(ctx, tx, version, kind)
	})
}

func CaptureSourceCandidateCorruptionForTest(ctx context.Context, selected any, id string, candidate durabledata.CompiledVersion) (*ResourceReceiptFault, error) {
	return captureResourceReceiptFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) (resourceReceiptFaultRow, error) {
		return data.CaptureSourceCandidateCorruptionTx(ctx, tx, id, candidate)
	})
}

func CaptureSourceExactInputCorruptionForTest(ctx context.Context, selected any, id string) (*ResourceReceiptFault, durabledata.SourceCommand, error) {
	var row *data.SourceExactInputCorruption
	fault, err := captureResourceReceiptFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) (resourceReceiptFaultRow, error) {
		var err error
		row, err = data.CaptureSourceExactInputCorruptionTx(ctx, tx, id)
		return row, err
	})
	if err != nil {
		return nil, durabledata.SourceCommand{}, err
	}
	return fault, row.Command, nil
}

func CaptureSourceClockRewriteForTest(ctx context.Context, selected any, id string, completed time.Time) (*ResourceReceiptFault, error) {
	source, err := ReadSourceEvaluationResultWireForTest(ctx, selected, id)
	if err != nil {
		return nil, err
	}
	provenance, err := ReadImportProvenanceWireForTest(ctx, selected, id)
	if err != nil {
		return nil, err
	}
	return captureResourceReceiptFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) (resourceReceiptFaultRow, error) {
		return data.CaptureSourceClockRewriteTx(ctx, tx, id, source, provenance, completed)
	})
}

func CaptureProvenanceReadArmForTest(ctx context.Context, selected any, version, kind, producer string, completed time.Time) (*ResourceReceiptFault, error) {
	return captureResourceReceiptFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) (resourceReceiptFaultRow, error) {
		return data.CaptureProvenanceReadArmTx(ctx, tx, version, kind, producer, completed)
	})
}

func CaptureNonMutatingProvenanceForTest(ctx context.Context, selected any, version, source string) (*ResourceReceiptFault, error) {
	return captureResourceReceiptFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) (resourceReceiptFaultRow, error) {
		return data.CaptureNonMutatingProvenanceTx(ctx, tx, version, source)
	})
}

func CaptureProvenanceCorruptionForTest(ctx context.Context, selected any, version, kind string) (*ResourceReceiptFault, error) {
	return captureResourceReceiptFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) (resourceReceiptFaultRow, error) {
		return data.CaptureProvenanceCorruptionTx(ctx, tx, version, kind)
	})
}

func CaptureSourceHeadCommitFaultForTest(ctx context.Context, selected any, id, kind string) (*ResourceReceiptFault, error) {
	return captureResourceReceiptFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) (resourceReceiptFaultRow, error) {
		return data.CaptureSourceHeadCommitFaultTx(ctx, tx, id, kind)
	})
}

func CapturePrunePinEvidenceRemovalForTest(ctx context.Context, selected any, id string, ordinal int) (*ResourceReceiptFault, error) {
	return captureResourceReceiptFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) (resourceReceiptFaultRow, error) {
		return data.CapturePrunePinEvidenceRemovalTx(ctx, tx, id, ordinal)
	})
}

func CaptureNonMutatingHeadHistoryForTest(ctx context.Context, selected any, source string) (*ResourceReceiptFault, error) {
	return captureResourceReceiptFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) (resourceReceiptFaultRow, error) {
		return data.CaptureNonMutatingHeadHistoryTx(ctx, tx, source)
	})
}

func MaterializePruneWitnessPinsForTest(ctx context.Context, selected any, version durabledata.VersionID, ref durabledata.DeclarationRef, schema durabledata.SchemaDigest) error {
	return runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		switch owner := selected.(type) {
		case *PostgresStore:
			return data.MaterializePruneWitnessPinsTx(ctx, owner.durableDataOwner, tx, version, ref, schema)
		case *SQLiteRuntimeStore:
			return data.MaterializePruneWitnessPinsTx(ctx, owner.durableDataOwner, tx, version, ref, schema)
		default:
			return errors.New("prune pin witness requires the exact selected native owner")
		}
	})
}

func CaptureSourceContextCorruptionForTest(ctx context.Context, selected any, id, kind string) (*ResourceReceiptFault, durabledata.SourceOperationResult, error) {
	var row *data.SourceContextCorruption
	fault, err := captureResourceReceiptFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) (resourceReceiptFaultRow, error) {
		var err error
		row, err = data.CaptureSourceContextCorruptionTx(ctx, tx, id, kind)
		return row, err
	})
	if err != nil {
		return nil, durabledata.SourceOperationResult{}, err
	}
	return fault, row.Result, nil
}

func CaptureFailedFusedContextCorruptionForTest(ctx context.Context, selected any, run, source string) (*ResourceReceiptFault, durabledata.FusedChildEvaluation, []durabledata.ValidationDefect, error) {
	contextWire, err := ReadRunCreationChildContextWireForTest(ctx, selected, run, source)
	if err != nil {
		return nil, durabledata.FusedChildEvaluation{}, nil, err
	}
	parentWire, err := ReadRunCreationRequestEvidenceWireForTest(ctx, selected, run)
	if err != nil {
		return nil, durabledata.FusedChildEvaluation{}, nil, err
	}
	var row *data.FailedFusedContextCorruption
	fault, err := captureResourceReceiptFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) (resourceReceiptFaultRow, error) {
		var err error
		row, err = data.CaptureFailedFusedContextCorruptionTx(ctx, tx, run, source, contextWire, parentWire)
		return row, err
	})
	if err != nil {
		return nil, durabledata.FusedChildEvaluation{}, nil, err
	}
	return fault, row.Projection, row.Defects, nil
}

func captureResourceReceiptFault(ctx context.Context, selected any, capture func(context.Context, *sql.Tx) (resourceReceiptFaultRow, error)) (*ResourceReceiptFault, error) {
	postgres, err := workflowProjectionNativePostgres(selected)
	if err != nil {
		return nil, err
	}
	f := &ResourceReceiptFault{selected: selected, postgres: postgres}
	err = readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		f.row, err = capture(ctx, tx)
		return err
	})
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (f *ResourceReceiptFault) InstallForTest(ctx context.Context) error {
	if f == nil || f.row == nil {
		return errors.New("resource receipt fault requires its exact capture")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.cleanupRequired {
		return errors.New("resource receipt fault still owns pending cleanup")
	}
	return runWorkflowProjectionFault(ctx, f.selected, func(ctx context.Context, tx *sql.Tx) error {
		f.cleanupRequired = true
		return data.InstallCapturedResourceReceiptTx(ctx, tx, f.row.CapturedReceipt(), f.postgres)
	})
}

func (f *ResourceReceiptFault) RestoreForTest(ctx context.Context) error {
	if f == nil || f.row == nil {
		return errors.New("resource receipt cleanup requires its exact capture")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.cleanupRequired {
		return nil
	}
	err := runWorkflowProjectionFault(ctx, f.selected, func(ctx context.Context, tx *sql.Tx) error {
		return data.RestoreCapturedResourceReceiptTx(ctx, tx, f.row.CapturedReceipt(), f.postgres)
	})
	if err == nil {
		f.cleanupRequired = false
	}
	return err
}
