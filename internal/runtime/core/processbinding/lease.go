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

func (i LeaseIdentity) Validate() error {
	for _, field := range []struct{ name, value string }{
		{"lease_id", i.LeaseID},
		{"boot_id", i.BootID},
		{"runtime_instance_id", i.RuntimeInstanceID},
	} {
		id, err := uuid.Parse(field.value)
		if err != nil || id == uuid.Nil || id.String() != field.value {
			return fmt.Errorf("process lease %s must be a canonical nonzero UUID", field.name)
		}
	}
	if i.OwnerID == "" || i.OwnerID != strings.TrimSpace(i.OwnerID) {
		return errors.New("process lease owner_id must be nonempty and canonical")
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
