package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
)

func decodeManagedCapabilitySurface(raw []byte) (managedcapabilities.Surface, error) {
	if len(raw) == 0 {
		return managedcapabilities.Surface{}, fmt.Errorf("managed capability surface is required")
	}
	var surface managedcapabilities.Surface
	if err := json.Unmarshal(raw, &surface); err != nil {
		return managedcapabilities.Surface{}, fmt.Errorf("decode managed capability surface: %w", err)
	}
	if err := surface.Validate(); err != nil {
		return managedcapabilities.Surface{}, fmt.Errorf("validate managed capability surface: %w", err)
	}
	return surface, nil
}

func (s *PostgresStore) SaveManagedCapabilitySurface(ctx context.Context, surface managedcapabilities.Surface) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required for managed capability persistence")
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	raw, err := json.Marshal(surface)
	if err != nil {
		return err
	}
	if _, err := insertManagedCapabilitySurfacePostgres(ctx, tx, raw); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SQLiteRuntimeStore) SaveManagedCapabilitySurface(ctx context.Context, surface managedcapabilities.Surface) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("sqlite store is required for managed capability persistence")
	}
	return s.runRuntimeMutation(ctx, "sqlite save managed capability surface", func(txctx context.Context, tx *sql.Tx) error {
		raw, err := json.Marshal(surface)
		if err != nil {
			return err
		}
		_, err = insertManagedCapabilitySurfaceSQLite(txctx, tx, raw)
		return err
	})
}

func insertManagedCapabilitySurfacePostgres(ctx context.Context, tx *sql.Tx, raw []byte) (managedcapabilities.Surface, error) {
	surface, err := decodeManagedCapabilitySurface(raw)
	if err != nil {
		return managedcapabilities.Surface{}, err
	}
	encoded, _ := json.Marshal(surface)
	var existingRaw []byte
	err = tx.QueryRowContext(ctx, `SELECT surface FROM managed_agent_capability_surfaces WHERE surface_id=$1::uuid FOR UPDATE`, surface.ID).Scan(&existingRaw)
	if err != nil && err != sql.ErrNoRows {
		return managedcapabilities.Surface{}, fmt.Errorf("load existing managed capability surface: %w", err)
	}
	if err == nil {
		existing, decodeErr := decodeManagedCapabilitySurface(existingRaw)
		if decodeErr != nil {
			return managedcapabilities.Surface{}, decodeErr
		}
		if advanceErr := surface.CanAdvanceFrom(existing); advanceErr != nil {
			return managedcapabilities.Surface{}, advanceErr
		}
		if existing.IntegrityHash == surface.IntegrityHash {
			return surface, nil
		}
		res, updateErr := tx.ExecContext(ctx, `UPDATE managed_agent_capability_surfaces SET integrity_hash=$2,surface=$3::jsonb WHERE surface_id=$1::uuid AND integrity_hash=$4`, surface.ID, surface.IntegrityHash, string(encoded), existing.IntegrityHash)
		if updateErr != nil {
			return managedcapabilities.Surface{}, fmt.Errorf("advance managed capability surface: %w", updateErr)
		}
		if rows, rowsErr := res.RowsAffected(); rowsErr != nil || rows != 1 {
			return managedcapabilities.Surface{}, fmt.Errorf("advance managed capability surface: concurrent evidence conflict")
		}
		return surface, nil
	}
	res, err := tx.ExecContext(ctx, `
		INSERT INTO managed_agent_capability_surfaces (
			surface_id, integrity_hash, authority_kind, authority_id, execution_kind,
			execution_authority_id, run_id, actor_id, provider, transport, surface, created_at
		) VALUES ($1::uuid,$2,$3,$4::uuid,$5,$6,NULLIF($7,'')::uuid,$8,$9,$10,$11::jsonb,$12)
		ON CONFLICT (surface_id) DO NOTHING
	`, surface.ID, surface.IntegrityHash, surface.Authority.Kind, surface.Authority.ID, surface.Authority.ExecutionKind,
		surface.Authority.ExecutionAuthorityID, strings.TrimSpace(surface.Authority.RunID), surface.ActorID, surface.Provider,
		surface.Transport, string(encoded), surface.CreatedAt.UTC())
	if err != nil {
		return managedcapabilities.Surface{}, fmt.Errorf("insert managed capability surface: %w", err)
	}
	if rows, rowsErr := res.RowsAffected(); rowsErr != nil {
		return managedcapabilities.Surface{}, fmt.Errorf("inspect managed capability surface insert: %w", rowsErr)
	} else if rows != 1 {
		return advanceConflictingManagedCapabilitySurfacePostgres(ctx, tx, surface, encoded)
	}
	return surface, nil
}

func advanceConflictingManagedCapabilitySurfacePostgres(ctx context.Context, tx *sql.Tx, surface managedcapabilities.Surface, encoded []byte) (managedcapabilities.Surface, error) {
	var existingRaw []byte
	if err := tx.QueryRowContext(ctx, `SELECT surface FROM managed_agent_capability_surfaces WHERE surface_id=$1::uuid FOR UPDATE`, surface.ID).Scan(&existingRaw); err != nil {
		return managedcapabilities.Surface{}, fmt.Errorf("load conflicting managed capability surface: %w", err)
	}
	existing, err := decodeManagedCapabilitySurface(existingRaw)
	if err != nil {
		return managedcapabilities.Surface{}, err
	}
	if err := surface.CanAdvanceFrom(existing); err != nil {
		return managedcapabilities.Surface{}, err
	}
	if existing.IntegrityHash == surface.IntegrityHash {
		return surface, nil
	}
	res, err := tx.ExecContext(ctx, `UPDATE managed_agent_capability_surfaces SET integrity_hash=$2,surface=$3::jsonb WHERE surface_id=$1::uuid AND integrity_hash=$4`, surface.ID, surface.IntegrityHash, string(encoded), existing.IntegrityHash)
	if err != nil {
		return managedcapabilities.Surface{}, fmt.Errorf("advance conflicting managed capability surface: %w", err)
	}
	if rows, rowsErr := res.RowsAffected(); rowsErr != nil || rows != 1 {
		return managedcapabilities.Surface{}, fmt.Errorf("advance conflicting managed capability surface: concurrent evidence conflict")
	}
	return surface, nil
}

func insertManagedCapabilitySurfaceSQLite(ctx context.Context, tx *sql.Tx, raw []byte) (managedcapabilities.Surface, error) {
	surface, err := decodeManagedCapabilitySurface(raw)
	if err != nil {
		return managedcapabilities.Surface{}, err
	}
	encoded, _ := json.Marshal(surface)
	var existingRaw []byte
	err = tx.QueryRowContext(ctx, `SELECT surface FROM managed_agent_capability_surfaces WHERE surface_id=?`, surface.ID).Scan(&existingRaw)
	if err != nil && err != sql.ErrNoRows {
		return managedcapabilities.Surface{}, fmt.Errorf("load existing sqlite managed capability surface: %w", err)
	}
	if err == nil {
		existing, decodeErr := decodeManagedCapabilitySurface(existingRaw)
		if decodeErr != nil {
			return managedcapabilities.Surface{}, decodeErr
		}
		if advanceErr := surface.CanAdvanceFrom(existing); advanceErr != nil {
			return managedcapabilities.Surface{}, advanceErr
		}
		if existing.IntegrityHash == surface.IntegrityHash {
			return surface, nil
		}
		res, updateErr := tx.ExecContext(ctx, `UPDATE managed_agent_capability_surfaces SET integrity_hash=?,surface=? WHERE surface_id=? AND integrity_hash=?`, surface.IntegrityHash, string(encoded), surface.ID, existing.IntegrityHash)
		if updateErr != nil {
			return managedcapabilities.Surface{}, fmt.Errorf("advance sqlite managed capability surface: %w", updateErr)
		}
		if rows, _ := res.RowsAffected(); rows != 1 {
			return managedcapabilities.Surface{}, fmt.Errorf("advance sqlite managed capability surface: concurrent evidence conflict")
		}
		return surface, nil
	}
	res, err := tx.ExecContext(ctx, `
		INSERT INTO managed_agent_capability_surfaces (
			surface_id, integrity_hash, authority_kind, authority_id, execution_kind,
			execution_authority_id, run_id, actor_id, provider, transport, surface, created_at
		) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT (surface_id) DO NOTHING
	`, surface.ID, surface.IntegrityHash, surface.Authority.Kind, surface.Authority.ID, surface.Authority.ExecutionKind,
		surface.Authority.ExecutionAuthorityID, sqliteNullString(surface.Authority.RunID), surface.ActorID, surface.Provider,
		surface.Transport, string(encoded), surface.CreatedAt.UTC())
	if err != nil {
		return managedcapabilities.Surface{}, fmt.Errorf("insert sqlite managed capability surface: %w", err)
	}
	if rows, rowsErr := res.RowsAffected(); rowsErr != nil {
		return managedcapabilities.Surface{}, fmt.Errorf("inspect sqlite managed capability surface insert: %w", rowsErr)
	} else if rows != 1 {
		return advanceConflictingManagedCapabilitySurfaceSQLite(ctx, tx, surface, encoded)
	}
	return surface, nil
}

func advanceConflictingManagedCapabilitySurfaceSQLite(ctx context.Context, tx *sql.Tx, surface managedcapabilities.Surface, encoded []byte) (managedcapabilities.Surface, error) {
	var existingRaw []byte
	if err := tx.QueryRowContext(ctx, `SELECT surface FROM managed_agent_capability_surfaces WHERE surface_id=?`, surface.ID).Scan(&existingRaw); err != nil {
		return managedcapabilities.Surface{}, fmt.Errorf("load conflicting sqlite managed capability surface: %w", err)
	}
	existing, err := decodeManagedCapabilitySurface(existingRaw)
	if err != nil {
		return managedcapabilities.Surface{}, err
	}
	if err := surface.CanAdvanceFrom(existing); err != nil {
		return managedcapabilities.Surface{}, err
	}
	if existing.IntegrityHash == surface.IntegrityHash {
		return surface, nil
	}
	res, err := tx.ExecContext(ctx, `UPDATE managed_agent_capability_surfaces SET integrity_hash=?,surface=? WHERE surface_id=? AND integrity_hash=?`, surface.IntegrityHash, string(encoded), surface.ID, existing.IntegrityHash)
	if err != nil {
		return managedcapabilities.Surface{}, fmt.Errorf("advance conflicting sqlite managed capability surface: %w", err)
	}
	if rows, rowsErr := res.RowsAffected(); rowsErr != nil || rows != 1 {
		return managedcapabilities.Surface{}, fmt.Errorf("advance conflicting sqlite managed capability surface: concurrent evidence conflict")
	}
	return surface, nil
}

func validateManagedAgentTurnSurface(surface managedcapabilities.Surface, agentID, sessionID, runID string) error {
	if surface.Authority.Kind != managedcapabilities.AuthorityProviderTurn {
		return fmt.Errorf("agent turn capability surface authority kind %q is not provider_turn", surface.Authority.Kind)
	}
	if surface.ActorID != strings.TrimSpace(agentID) || surface.Authority.SessionID != strings.TrimSpace(sessionID) {
		return fmt.Errorf("agent turn capability surface owner does not match persisted turn")
	}
	if surface.Authority.RunID != strings.TrimSpace(runID) {
		return fmt.Errorf("agent turn capability surface run does not match persisted turn")
	}
	return nil
}
