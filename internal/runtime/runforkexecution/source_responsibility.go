package runforkexecution

import (
	"context"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/google/uuid"
)

type selectedSourceKind uint8

const (
	originalRunSource selectedSourceKind = iota + 1
	retainedForkSource
	freshSelectedSource
	retainedFanOutSource
)

// These are responsibility selectors, not grants. Only the selected-store
// owners can restore the opaque source returned to the compiler.
type SelectedSourceResponsibility struct {
	kind selectedSourceKind
	key  string
	root sourceartifact.RootAdmission
}

func OriginalRunSource(runID string) SelectedSourceResponsibility {
	return SelectedSourceResponsibility{kind: originalRunSource, key: runID}
}

func RetainedForkSource(forkRunID string) SelectedSourceResponsibility {
	return SelectedSourceResponsibility{kind: retainedForkSource, key: forkRunID}
}

func FreshSelectedSource(hash string) SelectedSourceResponsibility {
	return SelectedSourceResponsibility{kind: freshSelectedSource, key: hash}
}

func retainedFanOutExecutionSource(source sourceartifact.RootedSource) SelectedSourceResponsibility {
	return SelectedSourceResponsibility{kind: retainedFanOutSource, root: source.Admission()}
}

func (r SelectedSourceResponsibility) load(ctx context.Context, store SourceArtifactSelectedContractSourceStore) (sourceartifact.RootedSource, error) {
	if err := ctx.Err(); err != nil {
		return sourceartifact.RootedSource{}, err
	}
	rooted, err := r.resolve(ctx, store)
	if err := errors.Join(err, ctx.Err()); err != nil {
		return sourceartifact.RootedSource{}, err
	}
	if err := rooted.Validate(); err != nil {
		return sourceartifact.RootedSource{}, err
	}
	if r.kind == freshSelectedSource && rooted.Admission().BundleHash() != r.key {
		return sourceartifact.RootedSource{}, fmt.Errorf("fresh target read differs from its requested content")
	}
	return rooted, nil
}

func (r SelectedSourceResponsibility) resolve(ctx context.Context, store SourceArtifactSelectedContractSourceStore) (sourceartifact.RootedSource, error) {
	if r.kind == retainedFanOutSource {
		if err := r.root.Validate(); err != nil {
			return sourceartifact.RootedSource{}, err
		}
		rooted, err := store.GetRootedSource(ctx, r.root.Reference())
		if err != nil {
			return sourceartifact.RootedSource{}, err
		}
		if !rooted.Admission().Equal(r.root) {
			return sourceartifact.RootedSource{}, fmt.Errorf("retained fan-out read differs from its proven execution root")
		}
		return rooted, nil
	}
	if r.kind == freshSelectedSource {
		if err := sourceartifact.ValidateHash(r.key); err != nil {
			return sourceartifact.RootedSource{}, fmt.Errorf("fresh source requires an exact admitted target hash: %w", err)
		}
		root, present, err := correlation.RootAdmissionFromContext(ctx)
		if err != nil {
			return sourceartifact.RootedSource{}, err
		}
		if present {
			if root.BundleHash() != r.key {
				return sourceartifact.RootedSource{}, fmt.Errorf("fresh target differs from its selected rooted context")
			}
			selected, err := store.GetRootedSource(ctx, root.Reference())
			if err != nil {
				return sourceartifact.RootedSource{}, err
			}
			if err := selected.Validate(); err != nil {
				return sourceartifact.RootedSource{}, err
			}
			if selected.Admission().Reference() != root.Reference() {
				return sourceartifact.RootedSource{}, fmt.Errorf("fresh target read differs from its selected rooted context")
			}
			return selected, nil
		}
		return store.ResolveUnboundRootedSource(ctx, r.key)
	}
	if r.kind != originalRunSource && r.kind != retainedForkSource {
		return sourceartifact.RootedSource{}, fmt.Errorf("selected source requires an explicit original, fresh or retained responsibility")
	}
	id, err := uuid.Parse(r.key)
	if err != nil || id == uuid.Nil || id.String() != r.key {
		return sourceartifact.RootedSource{}, fmt.Errorf("selected source responsibility requires an exact run identity")
	}
	if r.kind == originalRunSource {
		return store.LoadRunRootedSource(ctx, r.key)
	}
	return store.LoadSelectedForkRootedSource(ctx, r.key)
}
