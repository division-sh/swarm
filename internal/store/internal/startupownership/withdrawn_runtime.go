package startupownership

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
)

// RuntimeAuthorityWithdrawnTx proves an exact frozen process stamp has a newer
// native acquisition. Physical release alone never supplies successor evidence.
func RuntimeAuthorityWithdrawnTx(ctx context.Context, tx *sql.Tx, original processbinding.LeaseStamp, sqlite bool) (bool, error) {
	backend := processbinding.PostgresLeaseBackend
	if sqlite {
		backend = processbinding.SQLiteLeaseBackend
	}
	if original.Validate() != nil || original.Backend != backend {
		return false, fmt.Errorf("runtime withdrawal requires its exact original native stamp")
	}
	current, err := generationauthority.ReadProcessGenerationTx(ctx, tx, sqlite)
	if err != nil {
		return false, err
	}
	return current.Validate() == nil && current.Generation > original.Generation && current.LeaseID != original.LeaseID, ctx.Err()
}
