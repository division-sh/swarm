package startupownership

import (
	"context"
	"database/sql"
	"errors"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	storeagent "github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
)

func proveSelectedForkGrantTx(ctx context.Context, tx *sql.Tx, evidence runtimeownership.GrantEvidence, sqlite bool) error {
	if err := storeagent.ProveSelectedForkGenerationGrantTx(ctx, tx, evidence, sqlite); err != nil {
		return err
	}
	query := `SELECT snapshot FROM runtime_generation_grants WHERE grant_id = $1 ORDER BY state_version DESC LIMIT 1`
	if !sqlite {
		query += ` FOR UPDATE`
	}
	var raw []byte
	if err := tx.QueryRowContext(ctx, query, evidence.GrantID).Scan(&raw); err != nil {
		return err
	}
	var current runtimeownership.GrantEvidence
	if err := canonicaljson.DecodeInto(raw, &current); err != nil {
		return err
	}
	if err := current.Validate(); err != nil {
		return err
	}
	actual, err := canonicaljson.Bytes(current)
	if err != nil {
		return err
	}
	expected, err := canonicaljson.Bytes(evidence)
	if err != nil {
		return err
	}
	if string(actual) != string(expected) {
		return errors.New("selected-fork generation grant is no longer current")
	}
	return nil
}
