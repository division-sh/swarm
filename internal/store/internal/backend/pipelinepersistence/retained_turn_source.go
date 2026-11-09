package pipelinepersistence

import (
	"context"
	"sync"

	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/platform"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

// Only compiler output is reused. Native source validation precedes this memo,
// and ownership/currentness observations remain local to each transaction.
type retainedTurnSourceMemo struct {
	mu         sync.Mutex
	root       sourceartifact.RootAdmissionReference
	specDigest string
	source     semanticview.Source
}

func (m *retainedTurnSourceMemo) load(ctx context.Context, rooted sourceartifact.RootedSource) (semanticview.Source, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := rooted.Validate(); err != nil {
		return nil, err
	}
	root, specDigest := rooted.Admission().Reference(), platform.PlatformSpecDigest()
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if m.source != nil && m.root == root && m.specDigest == specDigest {
		return m.source, nil
	}
	spec, err := platform.MaterializePlatformSpecFile()
	if err != nil {
		return nil, err
	}
	// The default loader uses this binary's immutable embedded pack inventory;
	// no resolver, ambient pack override or caller-owned bundle enters the memo.
	bundle, err := contracts.LoadWorkflowContractBundleFromRootedSource("", rooted, spec, contracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
	if err != nil {
		return nil, err
	}
	// Wrap prepares mutable fan-out plans, so finish it before sharing the source.
	source := semanticview.Wrap(bundle)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.root, m.specDigest, m.source = root, specDigest, source
	return source, nil
}
