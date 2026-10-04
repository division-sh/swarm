package runbundle

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

// AdmitPinnedSources checks source bindings without acquiring execution authority.
// Standing-run exclusions are owned by the reader, not reconstructed here.
func AdmitPinnedSources(ctx context.Context, reader ActiveAvailabilityReader, bootIdentity string, pinnedHashes []string) error {
	if ctx == nil {
		return errors.New("pinned source admission context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	allowed := make(map[string]struct{}, len(pinnedHashes))
	for _, hash := range pinnedHashes {
		hash = strings.TrimSpace(hash)
		if err := sourceartifact.ValidateHash(hash); err != nil {
			return runtimefailures.Wrap(runtimefailures.ClassSchemaInvalid, "pinned_source_hash_invalid", "run-bundle", "admit_sources", nil, err)
		}
		allowed[hash] = struct{}{}
	}
	if len(allowed) == 0 {
		return nil
	}
	if strings.TrimSpace(bootIdentity) == "" {
		return runtimefailures.Wrap(runtimefailures.ClassSchemaInvalid, "boot_source_identity_required", "run-bundle", "admit_sources", nil, errors.New("boot bundle identity is required"))
	}
	if reader == nil {
		return runtimefailures.Wrap(runtimefailures.ClassDependencyUnavailable, "pinned_source_reader_unavailable", "run-bundle", "admit_sources", nil, errors.New("active run source reader is required"))
	}
	availabilities, err := reader.ActiveNonStandingRunBundleAvailabilities(ctx)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if err != nil {
		return runtimefailures.Wrap(runtimefailures.ClassDependencyUnavailable, "pinned_source_reader_unavailable", "run-bundle", "admit_sources", nil, err)
	}
	var details []string
	for _, availability := range availabilities {
		if !availability.Available() {
			continue // Artifact integrity is checked by startuprecovery.Inspect.
		}
		if _, ok := allowed[strings.TrimSpace(availability.BundleHash)]; !ok {
			details = append(details, availability.DetailString())
		}
	}
	if len(details) == 0 {
		return nil
	}
	hashes := make([]string, 0, len(allowed))
	for hash := range allowed {
		hashes = append(hashes, hash)
	}
	sort.Strings(hashes)
	cause := fmt.Errorf("active non-standing run pinned bundle_hash conflict: DB-loaded serve bundle_hash set %s cannot resume %d active non-standing run(s) with different bundle_hash: %s", strings.Join(hashes, ","), len(details), strings.Join(details, "; "))
	return fmt.Errorf("%s: %w", cause, runtimefailures.Wrap(runtimefailures.ClassLifecycleConflict, "pinned_source_conflict", "run-bundle", "admit_sources", nil, cause))
}
