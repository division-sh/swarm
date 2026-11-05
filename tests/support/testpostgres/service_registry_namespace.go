package testpostgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
)

func (r *ServiceRegistry) managedNamespaceOwner() (string, error) {
	var owner string
	err := r.readRegistry(func(doc registryDocument) error {
		var err error
		owner, err = r.validateRegistryOwner(doc)
		return err
	})
	return owner, err
}

// The registry lock binds owner initialization to the absence of existing rows.
func (r *ServiceRegistry) validateRegistryOwner(doc registryDocument) (string, error) {
	if _, err := os.Lstat(filepath.Join(r.StateRoot, "owner-id")); os.IsNotExist(err) {
		if err := r.requireUnownedNamespaceEmpty(doc); err != nil {
			return "", err
		}
	} else if err != nil {
		return "", fmt.Errorf("read Postgres registry owner authority: %w", err)
	}
	owner, err := r.ownerID()
	if err != nil {
		return "", err
	}
	if id, err := uuid.Parse(owner); err != nil || id == uuid.Nil {
		return "", fmt.Errorf("invalid Postgres registry owner authority; local evidence retained")
	}
	for _, record := range doc.Services {
		if record.OwnerID != owner {
			return "", fmt.Errorf("Postgres service %s owner disagrees with registry authority; local evidence retained", record.LeaseID)
		}
	}
	return owner, nil
}

func (r *ServiceRegistry) requireUnownedNamespaceEmpty(doc registryDocument) error {
	if len(doc.Services) != 0 {
		return fmt.Errorf("Postgres registry owner authority missing; local evidence retained")
	}
	for _, name := range []string{"leases", "creators", "handoff"} {
		entries, err := os.ReadDir(filepath.Join(r.StateRoot, name))
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		if len(entries) != 0 {
			return fmt.Errorf("Postgres registry owner authority missing with %s evidence; resources left untouched", name)
		}
	}
	return nil
}

func (r *ServiceRegistry) belongsToManagedNamespace(owner string, got dockerInspect) (bool, error) {
	localOwner, err := uuid.Parse(owner)
	if err != nil || localOwner == uuid.Nil {
		return false, fmt.Errorf("invalid Postgres registry owner authority; resources left untouched")
	}
	candidate, err := uuid.Parse(got.Config.Labels["com.division.swarm.test-postgres.owner-id"])
	if err != nil || candidate == uuid.Nil {
		return false, fmt.Errorf("managed Postgres service %s has an invalid owner-id label; left untouched", got.ID)
	}
	if candidate == localOwner {
		return true, nil
	}
	err = r.readRegistry(func(doc registryDocument) error {
		current, err := r.validateRegistryOwner(doc)
		if err != nil {
			return err
		}
		if current != owner {
			return fmt.Errorf("Postgres registry owner changed during discovery; resources left untouched")
		}
		// Lease IDs are owner-scoped; only a local CID or name binds this candidate.
		for _, record := range doc.Services {
			if record.ContainerID != "" && record.ContainerID == got.ID || record.Name == strings.TrimPrefix(got.Name, "/") {
				return fmt.Errorf("managed Postgres service %s owner label conflicts with local service %s; left untouched", got.ID, record.LeaseID)
			}
		}
		return nil
	})
	return false, err
}

// Discovery is a worklist, not authority: construction and retirement may advance
// while Docker is queried. Registry locks cover only current local validation.
func (r *ServiceRegistry) validateManagedNamespace(ctx context.Context, candidates map[string][]dockerInspect) error {
	for leaseID, values := range candidates {
		if len(values) != 1 {
			return fmt.Errorf("managed Postgres service lease %s has %d candidates; all left untouched", leaseID, len(values))
		}
		missing := false
		err := r.readRegistry(func(doc registryDocument) error {
			if _, err := r.validateRegistryOwner(doc); err != nil {
				return err
			}
			record, ok := doc.Services[leaseID]
			if !ok {
				missing = true
				return nil
			}
			return r.validateNamespaceCandidate(record, values[0])
		})
		if err != nil {
			return err
		}
		if missing {
			if err := r.verifyContainerAbsent(ctx, values[0].ID); err != nil {
				return fmt.Errorf("managed Postgres service lease %s has no registry row; resource left untouched: %w", leaseID, err)
			}
		}
	}
	return nil
}

func (r *ServiceRegistry) validateNamespaceCandidate(record ServiceRecord, got dockerInspect) error {
	if err := validateContainerConstructor(record, got); err != nil {
		return err
	}
	if record.ContainerID != "" {
		switch record.State {
		case ServiceCreateSucceeded, ServiceStarting, ServiceReady, ServiceChildRunning, ServiceTearingDown:
			if got.ID != record.ContainerID {
				return fmt.Errorf("managed Postgres service lease %s does not match its registry container; left untouched", record.LeaseID)
			}
			return validateCIDFile(record)
		default:
			return fmt.Errorf("Postgres service %s publishes a container in state %q; left untouched", record.LeaseID, record.State)
		}
	}
	if record.State != ServiceCreating {
		return fmt.Errorf("Postgres service %s has a pre-ID container in state %q; left untouched", record.LeaseID, record.State)
	}
	for _, path := range []string{r.leasePath(record.LeaseID), r.creatorPath(record.LeaseID)} {
		probe, free, err := acquireExistingFileLock(path, true)
		if err != nil {
			return err
		}
		if free {
			return errors.Join(fmt.Errorf("Postgres service %s pre-ID creation authority is not held: %s; left untouched", record.LeaseID, path), probe.Close())
		}
	}
	return nil
}

func validateContainerConstructor(record ServiceRecord, got dockerInspect) error {
	if got.ID == "" || strings.TrimPrefix(got.Name, "/") != record.Name || got.Image != record.ImageID {
		return fmt.Errorf("Postgres service %s identity mismatch; left untouched", record.LeaseID)
	}
	for key, want := range record.Labels {
		if got.Config.Labels[key] != want {
			return fmt.Errorf("Postgres service %s label %s mismatch; left untouched", record.LeaseID, key)
		}
	}
	return nil
}
