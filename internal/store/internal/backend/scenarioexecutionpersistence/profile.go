package scenarioexecutionpersistence

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
	"github.com/google/uuid"
)

type storedProfile struct {
	profileID             string
	profileDigest         string
	effectiveSourceDigest string
	profileBytes          []byte
}

func EnsurePostgres(ctx context.Context, tx *sql.Tx, runID string, profile scenarioexecution.Profile, createdAt time.Time) error {
	if tx == nil {
		return fmt.Errorf("postgres scenario execution profile transaction is required")
	}
	if err := validateWrite(runID, profile); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO run_scenario_execution_profiles (
			run_id, profile_id, profile_digest, effective_source_digest, profile_bytes, created_at
		) VALUES ($1::uuid, $2, $3, $4, $5::bytea, $6)
		ON CONFLICT (run_id) DO NOTHING
	`, runID, profile.ID(), profile.Digest(), profile.EffectiveSourceIdentity().Digest(), profile.CanonicalBytes(), createdAt.UTC())
	if err != nil {
		return fmt.Errorf("insert postgres scenario execution profile for run %s: %w", runID, err)
	}
	stored, err := loadPostgresTx(ctx, tx, runID)
	if err != nil {
		return err
	}
	return requireExact(runID, profile, stored)
}

func EnsureSQLite(ctx context.Context, tx *sql.Tx, runID string, profile scenarioexecution.Profile, createdAt time.Time) error {
	if tx == nil {
		return fmt.Errorf("sqlite scenario execution profile transaction is required")
	}
	if err := validateWrite(runID, profile); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO run_scenario_execution_profiles (
			run_id, profile_id, profile_digest, effective_source_digest, profile_bytes, created_at
		) VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT (run_id) DO NOTHING
	`, runID, profile.ID(), profile.Digest(), profile.EffectiveSourceIdentity().Digest(), profile.CanonicalBytes(), createdAt.UTC())
	if err != nil {
		return fmt.Errorf("insert sqlite scenario execution profile for run %s: %w", runID, err)
	}
	stored, err := loadSQLiteTx(ctx, tx, runID)
	if err != nil {
		return err
	}
	return requireExact(runID, profile, stored)
}

func EnsurePostgresFromContext(ctx context.Context, tx *sql.Tx, runID string, createdAt time.Time) error {
	profile, ok := scenarioexecution.AdmittedProfileFromContext(ctx)
	if !ok {
		return nil
	}
	return EnsurePostgres(ctx, tx, runID, profile, createdAt)
}

func EnsureSQLiteFromContext(ctx context.Context, tx *sql.Tx, runID string, createdAt time.Time) error {
	profile, ok := scenarioexecution.AdmittedProfileFromContext(ctx)
	if !ok {
		return nil
	}
	return EnsureSQLite(ctx, tx, runID, profile, createdAt)
}

func RequirePostgresFromContext(ctx context.Context, tx *sql.Tx, runID string) error {
	profile, ok := scenarioexecution.AdmittedProfileFromContext(ctx)
	if !ok {
		return nil
	}
	if err := validateWrite(runID, profile); err != nil {
		return err
	}
	stored, err := loadPostgresTx(ctx, tx, runID)
	if err != nil {
		return fmt.Errorf("existing run %s cannot install a scenario execution profile: %w", runID, err)
	}
	return requireExact(runID, profile, stored)
}

func RequireSQLiteFromContext(ctx context.Context, tx *sql.Tx, runID string) error {
	profile, ok := scenarioexecution.AdmittedProfileFromContext(ctx)
	if !ok {
		return nil
	}
	if err := validateWrite(runID, profile); err != nil {
		return err
	}
	stored, err := loadSQLiteTx(ctx, tx, runID)
	if err != nil {
		return fmt.Errorf("existing run %s cannot install a scenario execution profile: %w", runID, err)
	}
	return requireExact(runID, profile, stored)
}

func LoadPostgres(ctx context.Context, db *sql.DB, runID string) (scenarioexecution.Profile, bool, error) {
	if db == nil {
		return scenarioexecution.Profile{}, false, fmt.Errorf("postgres scenario execution profile database is required")
	}
	if err := validateRunID(runID); err != nil {
		return scenarioexecution.Profile{}, false, err
	}
	var stored storedProfile
	err := db.QueryRowContext(ctx, `
		SELECT profile_id, profile_digest, effective_source_digest, profile_bytes
		FROM run_scenario_execution_profiles WHERE run_id = $1::uuid
	`, runID).Scan(&stored.profileID, &stored.profileDigest, &stored.effectiveSourceDigest, &stored.profileBytes)
	if err == sql.ErrNoRows {
		return scenarioexecution.Profile{}, false, nil
	}
	if err != nil {
		return scenarioexecution.Profile{}, false, fmt.Errorf("load postgres scenario execution profile for run %s: %w", runID, err)
	}
	profile, err := decodeStored(runID, stored)
	return profile, err == nil, err
}

func LoadPostgresTx(ctx context.Context, tx *sql.Tx, runID string) (scenarioexecution.Profile, bool, error) {
	if tx == nil {
		return scenarioexecution.Profile{}, false, fmt.Errorf("postgres scenario execution profile transaction is required")
	}
	if err := validateRunID(runID); err != nil {
		return scenarioexecution.Profile{}, false, err
	}
	stored, err := loadPostgresTx(ctx, tx, runID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return scenarioexecution.Profile{}, false, nil
		}
		return scenarioexecution.Profile{}, false, err
	}
	profile, err := decodeStored(runID, stored)
	return profile, err == nil, err
}

func LoadSQLiteTx(ctx context.Context, tx *sql.Tx, runID string) (scenarioexecution.Profile, bool, error) {
	if tx == nil {
		return scenarioexecution.Profile{}, false, fmt.Errorf("sqlite scenario execution profile transaction is required")
	}
	if err := validateRunID(runID); err != nil {
		return scenarioexecution.Profile{}, false, err
	}
	stored, err := loadSQLiteTx(ctx, tx, runID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return scenarioexecution.Profile{}, false, nil
		}
		return scenarioexecution.Profile{}, false, err
	}
	profile, err := decodeStored(runID, stored)
	return profile, err == nil, err
}

func RequirePostgresExact(ctx context.Context, tx *sql.Tx, runID string, profile scenarioexecution.Profile) error {
	if err := validateWrite(runID, profile); err != nil {
		return err
	}
	stored, err := loadPostgresTx(ctx, tx, runID)
	if err != nil {
		return err
	}
	return requireExact(runID, profile, stored)
}

func RequireSQLiteExact(ctx context.Context, tx *sql.Tx, runID string, profile scenarioexecution.Profile) error {
	if err := validateWrite(runID, profile); err != nil {
		return err
	}
	stored, err := loadSQLiteTx(ctx, tx, runID)
	if err != nil {
		return err
	}
	return requireExact(runID, profile, stored)
}

func LoadSQLite(ctx context.Context, db *sql.DB, runID string) (scenarioexecution.Profile, bool, error) {
	if db == nil {
		return scenarioexecution.Profile{}, false, fmt.Errorf("sqlite scenario execution profile database is required")
	}
	if err := validateRunID(runID); err != nil {
		return scenarioexecution.Profile{}, false, err
	}
	var stored storedProfile
	err := db.QueryRowContext(ctx, `
		SELECT profile_id, profile_digest, effective_source_digest, profile_bytes
		FROM run_scenario_execution_profiles WHERE run_id = ?
	`, runID).Scan(&stored.profileID, &stored.profileDigest, &stored.effectiveSourceDigest, &stored.profileBytes)
	if err == sql.ErrNoRows {
		return scenarioexecution.Profile{}, false, nil
	}
	if err != nil {
		return scenarioexecution.Profile{}, false, fmt.Errorf("load sqlite scenario execution profile for run %s: %w", runID, err)
	}
	profile, err := decodeStored(runID, stored)
	return profile, err == nil, err
}

func loadPostgresTx(ctx context.Context, tx *sql.Tx, runID string) (storedProfile, error) {
	var stored storedProfile
	err := tx.QueryRowContext(ctx, `
		SELECT profile_id, profile_digest, effective_source_digest, profile_bytes
		FROM run_scenario_execution_profiles WHERE run_id = $1::uuid
	`, runID).Scan(&stored.profileID, &stored.profileDigest, &stored.effectiveSourceDigest, &stored.profileBytes)
	if err != nil {
		return storedProfile{}, fmt.Errorf("load postgres scenario execution profile for run %s: %w", runID, err)
	}
	return stored, nil
}

func loadSQLiteTx(ctx context.Context, tx *sql.Tx, runID string) (storedProfile, error) {
	var stored storedProfile
	err := tx.QueryRowContext(ctx, `
		SELECT profile_id, profile_digest, effective_source_digest, profile_bytes
		FROM run_scenario_execution_profiles WHERE run_id = ?
	`, runID).Scan(&stored.profileID, &stored.profileDigest, &stored.effectiveSourceDigest, &stored.profileBytes)
	if err != nil {
		return storedProfile{}, fmt.Errorf("load sqlite scenario execution profile for run %s: %w", runID, err)
	}
	return stored, nil
}

func validateWrite(runID string, profile scenarioexecution.Profile) error {
	if err := validateRunID(runID); err != nil {
		return err
	}
	if err := profile.Validate(); err != nil {
		return fmt.Errorf("scenario execution profile for run %s: %w", runID, err)
	}
	return nil
}

func validateRunID(runID string) error {
	if runID != strings.TrimSpace(runID) {
		return fmt.Errorf("scenario execution profile run_id must be canonical UUID")
	}
	if _, err := uuid.Parse(runID); err != nil {
		return fmt.Errorf("scenario execution profile run_id must be UUID")
	}
	return nil
}

func requireExact(runID string, profile scenarioexecution.Profile, stored storedProfile) error {
	if stored.profileID != profile.ID() || stored.profileDigest != profile.Digest() ||
		stored.effectiveSourceDigest != profile.EffectiveSourceIdentity().Digest() ||
		!bytes.Equal(stored.profileBytes, profile.CanonicalBytes()) {
		return fmt.Errorf("run %s already has a different scenario execution profile", runID)
	}
	_, err := decodeStored(runID, stored)
	return err
}

func decodeStored(runID string, stored storedProfile) (scenarioexecution.Profile, error) {
	profile, err := scenarioexecution.DecodeProfile(stored.profileBytes, stored.profileDigest)
	if err != nil {
		return scenarioexecution.Profile{}, fmt.Errorf("verify scenario execution profile for run %s: %w", runID, err)
	}
	if profile.ID() != stored.profileID || profile.EffectiveSourceIdentity().Digest() != stored.effectiveSourceDigest {
		return scenarioexecution.Profile{}, fmt.Errorf("scenario execution profile projections for run %s do not match opaque bytes", runID)
	}
	return profile, nil
}
