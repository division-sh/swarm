package runstate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func LoadSelectedContractBinding(ctx context.Context, querier RowQueryer, forkRunID string) (runfork.RunForkSelectedContractBinding, error) {
	if querier == nil || uuid.Validate(forkRunID) != nil {
		return runfork.RunForkSelectedContractBinding{}, fmt.Errorf("selected binding reader requires query authority and exact run identity")
	}
	var binding runfork.RunForkSelectedContractBinding
	var selection runfork.RunForkContractSelection
	var pointKind string
	var forkRevision int64
	var createdAt any
	err := querier.QueryRowContext(ctx, `
		SELECT
			CAST(binding_id AS TEXT),
			CAST(fork_run_id AS TEXT),
			CAST(source_run_id AS TEXT),
			fork_point_kind,
			fork_revision,
			COALESCE(CAST(fork_event_id AS TEXT), ''),
			mode,
			COALESCE(bundle_hash, ''),
			created_at
		FROM run_fork_selected_contract_bindings
		WHERE fork_run_id = $1
	`, forkRunID).Scan(
		&binding.BindingID,
		&binding.ForkRunID,
		&binding.SourceRunID,
		&pointKind,
		&forkRevision,
		&binding.ForkEventID,
		&selection.Mode,
		&selection.BundleHash,
		&createdAt,
	)
	if err != nil {
		return runfork.RunForkSelectedContractBinding{}, err
	}
	parsedCreatedAt, ok, err := DecodeBindingTimestamp(createdAt)
	if err != nil {
		return runfork.RunForkSelectedContractBinding{}, fmt.Errorf("decode selected contract binding created_at: %w", err)
	}
	if !ok {
		return runfork.RunForkSelectedContractBinding{}, fmt.Errorf("selected contract binding created_at is required")
	}
	binding.Owner = runfork.RunForkSelectedContractBindingOwner
	binding.ForkPoint = runfork.RunForkPoint{Kind: runfork.RunForkPointKind(pointKind), Revision: forkRevision, EventID: binding.ForkEventID}
	if err := binding.ForkPoint.Validate(); err != nil {
		return runfork.RunForkSelectedContractBinding{}, fmt.Errorf("decode selected contract binding point: %w", err)
	}
	binding.ContractSelection = selection
	binding.CreatedAt = parsedCreatedAt
	return binding, nil
}

// RequireNormalControlTx is the transactional consumer of the same binding
// reader used by the selected execution owner.
func RequireNormalControlTx(ctx context.Context, tx *sql.Tx, runID string, operation runfork.SelectedControl) error {
	if tx == nil {
		return fmt.Errorf("selected control requires selected transaction")
	}
	if err := operation.Validate(); err != nil {
		return err
	}
	binding, err := LoadSelectedContractBinding(ctx, tx, runID)
	if errors.Is(err, sql.ErrNoRows) {
		return runfork.RequireNormalControlForBinding(runID, operation, runfork.RunForkSelectedContractBinding{}, false)
	}
	if err != nil {
		return err
	}
	return runfork.RequireNormalControlForBinding(runID, operation, binding, true)
}

func DecodeBindingTimestamp(raw any) (time.Time, bool, error) {
	switch value := raw.(type) {
	case nil:
		return time.Time{}, false, nil
	case time.Time:
		return value.UTC(), !value.IsZero(), nil
	case string:
		return parseBindingTimestamp(value)
	case []byte:
		return parseBindingTimestamp(string(value))
	default:
		return time.Time{}, false, fmt.Errorf("unsupported SQLite time value %T", raw)
	}
}

func parseBindingTimestamp(raw string) (time.Time, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false, nil
	}
	formats := []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999 -0700 MST",
		"2006-01-02 15:04:05.999999 -0700 MST",
		"2006-01-02 15:04:05 -0700 MST",
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05-07:00",
		"2006-01-02 15:04:05Z07:00",
		"2006-01-02 15:04:05",
	}
	var lastErr error
	for _, layout := range formats {
		parsed, err := time.Parse(layout, raw)
		if err == nil {
			return parsed.UTC(), true, nil
		}
		lastErr = err
	}
	return time.Time{}, false, lastErr
}
