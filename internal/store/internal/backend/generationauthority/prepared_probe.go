package generationauthority

import (
	"context"
	"database/sql"
	"errors"

	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
)

func PreparedProbeProcessCurrent(ctx context.Context, q processGenerationReader, preparation managedcapabilities.PreparedSelectedForkProbeAuthority, generation uint64, sqlite, lock bool) (bool, error) {
	if err := preparation.Validate(); err != nil {
		return false, err
	}
	return PreparedProcessCurrent(ctx, q, preparation.SelectedForkPreparationCoordinates, generation, sqlite, lock)
}

func PreparedProcessCurrent(ctx context.Context, q processGenerationReader, preparation managedcapabilities.SelectedForkPreparationCoordinates, generation uint64, sqlite, lock bool) (bool, error) {
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
		current, err = ReadProcessGenerationTx(ctx, tx, sqlite)
	} else {
		current, err = ReadProcessGeneration(ctx, q, sqlite)
	}
	return err == nil && current.Generation == generation && current.Coordinates() == (processbinding.LeaseCoordinates{
		LeaseID: preparation.ProcessAuthorityID, OwnerID: preparation.ProcessOwnerID, BootID: preparation.ProcessBootID,
	}), err
}
