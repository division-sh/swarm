package scenarioexecutionpersistence

import (
	"context"
	"database/sql"
)

// PhysicalProfile preserves the stored image rather than re-encoding a decoded profile.
type PhysicalProfile struct {
	RunID, ProfileID, ProfileDigest, EffectiveSourceDigest string
	Bytes                                                  []byte
}

func ReadPhysicalProfileTx(ctx context.Context, tx *sql.Tx) (PhysicalProfile, error) {
	var out PhysicalProfile
	err := tx.QueryRowContext(ctx, `SELECT run_id, profile_id, profile_digest, effective_source_digest, profile_bytes FROM run_scenario_execution_profiles`).Scan(
		&out.RunID, &out.ProfileID, &out.ProfileDigest, &out.EffectiveSourceDigest, &out.Bytes,
	)
	if err != nil {
		return PhysicalProfile{}, err
	}
	return out, nil
}

func CountPhysicalProfilesTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_scenario_execution_profiles`).Scan(&count)
	return count, err
}
