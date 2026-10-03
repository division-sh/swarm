package testpostgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// Discovery is a worklist, not authority: construction and retirement may advance
// while Docker is queried. Registry locks cover only current local validation.
func (r *ServiceRegistry) validateManagedNamespace(ctx context.Context, candidates map[string][]dockerInspect) error {
	for leaseID, values := range candidates {
		if len(values) != 1 {
			return fmt.Errorf("managed Postgres service lease %s has %d candidates; all left untouched", leaseID, len(values))
		}
		missing := false
		err := r.readRegistry(func(doc registryDocument) error {
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
