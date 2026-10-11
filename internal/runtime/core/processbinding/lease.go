package processbinding

import (
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/google/uuid"
)

const (
	PostgresLeaseBackend = "postgres_retained_session"
	SQLiteLeaseBackend   = "sqlite_retained_owner"
)

// LeaseIdentity is an immutable stamp, not proof of native lock possession.
type LeaseIdentity struct {
	LeaseID           string `json:"lease_id"`
	OwnerID           string `json:"owner_id"`
	BootID            string `json:"boot_id"`
	RuntimeInstanceID string `json:"runtime_instance_id"`
	Backend           string `json:"backend"`
}

// LeaseCoordinates are the process attribution retained by both ordinary and
// selected grants; their runtime/source coordinates remain separate concepts.
type LeaseCoordinates struct {
	LeaseID string
	OwnerID string
	BootID  string
}

func (i LeaseCoordinates) Validate() error {
	for _, field := range []struct{ name, value string }{
		{"lease_id", i.LeaseID},
		{"boot_id", i.BootID},
	} {
		id, err := uuid.Parse(field.value)
		if err != nil || id == uuid.Nil || id.String() != field.value {
			return fmt.Errorf("process lease %s must be a canonical nonzero UUID", field.name)
		}
	}
	if i.OwnerID == "" || i.OwnerID != strings.TrimSpace(i.OwnerID) {
		return errors.New("process lease owner_id must be nonempty and canonical")
	}
	return nil
}

func (i LeaseIdentity) Coordinates() LeaseCoordinates {
	return LeaseCoordinates{LeaseID: i.LeaseID, OwnerID: i.OwnerID, BootID: i.BootID}
}

func (i LeaseIdentity) Validate() error {
	if err := i.Coordinates().Validate(); err != nil {
		return err
	}
	id, err := uuid.Parse(i.RuntimeInstanceID)
	if err != nil || id == uuid.Nil || id.String() != i.RuntimeInstanceID {
		return errors.New("process lease runtime_instance_id must be a canonical nonzero UUID")
	}
	if i.Backend != PostgresLeaseBackend && i.Backend != SQLiteLeaseBackend {
		return fmt.Errorf("process lease backend %q is unsupported", i.Backend)
	}
	return nil
}

type LeaseStamp struct {
	LeaseIdentity
	Generation uint64 `json:"generation"`
}

func (s LeaseStamp) Validate() error {
	if err := s.LeaseIdentity.Validate(); err != nil {
		return err
	}
	if s.Generation == 0 || s.Generation > math.MaxInt64 {
		return errors.New("process lease generation must be a positive signed 64-bit value")
	}
	return nil
}
