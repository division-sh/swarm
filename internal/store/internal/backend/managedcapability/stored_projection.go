package managedcapabilitystore

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/google/uuid"
)

// StoredProjection is read with the original surface, never reconstructed from
// its payload when a stored projection is absent or contradictory.
type StoredProjection struct {
	ExecutionKind        sql.NullString
	ExecutionAuthorityID sql.NullString
	RunID                sql.NullString
	SelectedExecutionID  sql.NullString
	SelectedForkRunID    sql.NullString
}

func DecodeStored(raw []byte, stored StoredProjection) (managedcapabilities.Surface, error) {
	surface, err := Decode(raw)
	if err != nil {
		return managedcapabilities.Surface{}, err
	}
	authority := surface.Authority
	if !stored.ExecutionKind.Valid || stored.ExecutionKind.String != string(authority.ExecutionKind) ||
		!stored.ExecutionAuthorityID.Valid || stored.ExecutionAuthorityID.String != authority.ExecutionAuthorityID {
		return managedcapabilities.Surface{}, fmt.Errorf("managed capability surface stored execution authority disagrees with its original surface")
	}
	if authority.ExecutionKind == managedcapabilities.ExecutionSelectedContractFork {
		if stored.RunID.Valid || !sameStoredUUID(stored.SelectedExecutionID, authority.ExecutionAuthorityID) || !sameStoredUUID(stored.SelectedForkRunID, authority.RunID) {
			return managedcapabilities.Surface{}, fmt.Errorf("managed capability surface selected execution projection disagrees with its original surface")
		}
	} else if stored.SelectedExecutionID.Valid || stored.SelectedForkRunID.Valid || !sameStoredUUID(stored.RunID, authority.RunID) {
		return managedcapabilities.Surface{}, fmt.Errorf("managed capability surface run projection disagrees with its original surface")
	}
	return surface, nil
}

func sameStoredUUID(stored sql.NullString, original string) bool {
	original = strings.TrimSpace(original)
	if original == "" {
		return !stored.Valid
	}
	if !stored.Valid {
		return false
	}
	left, leftErr := uuid.Parse(stored.String)
	right, rightErr := uuid.Parse(original)
	return leftErr == nil && rightErr == nil && left == right
}

func surfaceRunProjections(surface managedcapabilities.Surface) (run, execution, child any) {
	if surface.Authority.ExecutionKind == managedcapabilities.ExecutionSelectedContractFork {
		return nil, uuid.MustParse(strings.TrimSpace(surface.Authority.ExecutionAuthorityID)).String(), uuid.MustParse(strings.TrimSpace(surface.Authority.RunID)).String()
	}
	return nullString(surface.Authority.RunID), nil, nil
}
