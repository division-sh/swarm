package startupownership

import (
	"context"
	"database/sql"
	"errors"

	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

// DisplacedHTTPActivityCut is issued only inside the retained successor's
// native recovery mutation. Neither attribution nor a process stamp can mint it.
type DisplacedHTTPActivityCut struct {
	attempt   *mutationprotocol.Attempt
	successor processbinding.LeaseStamp
	sqlite    bool
}

func newDisplacedHTTPActivityCut(ctx context.Context, attempt *mutationprotocol.Attempt, successor processbinding.LeaseStamp, sqlite bool) (DisplacedHTTPActivityCut, error) {
	cut := DisplacedHTTPActivityCut{attempt: attempt, successor: successor, sqlite: sqlite}
	if err := cut.proveSuccessorTx(ctx, attempt); err != nil {
		return DisplacedHTTPActivityCut{}, err
	}
	return cut, nil
}

func (cut DisplacedHTTPActivityCut) proveSuccessorTx(ctx context.Context, attempt *mutationprotocol.Attempt) error {
	if attempt == nil || cut.attempt == nil || attempt != cut.attempt {
		return errors.New("activity recovery cut requires its exact original mutation")
	}
	if err := cut.successor.Validate(); err != nil {
		return err
	}
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		current, err := generationauthority.ReadProcessGenerationTx(ctx, tx, cut.sqlite)
		if err != nil {
			return err
		}
		if current != cut.successor {
			return errors.New("activity recovery successor is no longer current")
		}
		return nil
	})
}

// ProveDisplacedGrantTx observes a supported displaced normal process. A false
// result preserves current/selected work; an error grants no recovery permission.
func (cut DisplacedHTTPActivityCut) ProveDisplacedGrantTx(ctx context.Context, attempt *mutationprotocol.Attempt, binding processbinding.ActivityExecutionBinding) (runtimeownership.GrantEvidence, bool, error) {
	var absent runtimeownership.GrantEvidence
	if err := binding.Validate(); err != nil {
		return absent, false, err
	}
	if err := cut.proveSuccessorTx(ctx, attempt); err != nil {
		return absent, false, err
	}
	var grant runtimeownership.GrantEvidence
	err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		grant, err = ReadActivityExecutionGrantTx(ctx, tx, binding.GrantID, binding.StateVersion)
		if err != nil {
			return err
		}
		if grant.State != runtimeownership.GrantAdmitted {
			return errors.New("activity recovery reference was not an admitted execution grant")
		}
		return nil
	})
	if err != nil {
		return absent, false, err
	}
	if grant.SelectedFork != nil || grant.ProcessCoordinates() == cut.successor.Coordinates() {
		return grant, false, nil
	}
	return grant, true, nil
}
