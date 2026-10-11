package startupownership

import (
	"context"
	"database/sql"
	"errors"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
)

func PreparedProbeProcessCurrent(ctx context.Context, q authorityQueryer, preparation managedcapabilities.PreparedSelectedForkProbeAuthority, generation uint64, sqlite, lock bool) (bool, error) {
	if err := preparation.Validate(); err != nil {
		return false, err
	}
	return PreparedProcessCurrent(ctx, q, preparation.SelectedForkPreparationCoordinates, generation, sqlite, lock)
}

func PreparedProcessCurrent(ctx context.Context, q authorityQueryer, preparation managedcapabilities.SelectedForkPreparationCoordinates, generation uint64, sqlite, lock bool) (bool, error) {
	if err := preparation.Validate(); err != nil {
		return false, err
	}
	if generation == 0 {
		return false, errors.New("prepared probe requires process generation")
	}
	var current processbinding.LeaseStamp
	var err error
	if lock {
		tx, ok := q.(*sql.Tx)
		if !ok {
			return false, errors.New("prepared process mutation proof requires its transaction")
		}
		current, err = generationauthority.ReadProcessGenerationTx(ctx, tx, sqlite)
	} else {
		current, err = generationauthority.ReadProcessGeneration(ctx, q, sqlite)
	}
	return err == nil && current.Generation == generation && current.Coordinates() == (processbinding.LeaseCoordinates{
		LeaseID: preparation.ProcessAuthorityID, OwnerID: preparation.ProcessOwnerID, BootID: preparation.ProcessBootID,
	}), err
}
