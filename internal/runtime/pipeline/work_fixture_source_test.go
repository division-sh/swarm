package pipeline_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
)

func TestNativePipelineWorkFixtureKeepsRequestedSourceIdentity(t *testing.T) {
	firstFact := authorActivityTestSourceArtifactFact
	secondFact := mustAuthorActivityTestSourceArtifactFactForHash("bundle-v2:sha256:" + strings.Repeat("b", 64))
	first := pipelineExternalTestWorkOwnerForSource(t, firstFact)
	second := pipelineExternalTestWorkOwnerForSource(t, secondFact)
	if first.Identity().BundleHash != firstFact.BundleHash() || second.Identity().BundleHash != secondFact.BundleHash() {
		t.Fatalf("source-specific fixture returned foreign occurrence: first=%+v second=%+v expected second=%s", first.Identity(), second.Identity(), secondFact.BundleHash())
	}
	if first == second {
		t.Fatal("distinct requested source facts reused one runtime occurrence")
	}
	if pipelineExternalTestWorkOwnerForSource(t, firstFact) != first || pipelineExternalTestWorkOwnerForSource(t, secondFact) != second {
		t.Fatal("source-specific work ownership was not stable")
	}
}

func TestNativePipelineWorkFixtureSharesOneProcessAndJoinsAllSources(t *testing.T) {
	var process *worklifetime.Process
	var first, second *worklifetime.RuntimeOccurrence
	t.Run("fixture", func(t *testing.T) {
		first = pipelineExternalTestWorkOwnerForSource(t, authorActivityTestSourceArtifactFact)
		other := mustAuthorActivityTestSourceArtifactFactForHash("bundle-v2:sha256:" + strings.Repeat("b", 64))
		second = pipelineExternalTestWorkOwnerForSource(t, other)
		for _, owner := range []*worklifetime.RuntimeOccurrence{first, second} {
			lease, err := owner.Begin(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := lease.Done(); err != nil {
					t.Error(err)
				}
			})
			root, found := worklifetime.ProcessFromContext(lease.Context())
			if !found || (process != nil && process != root) {
				t.Fatal("source correction replaced the fixture process root")
			}
			process = root
		}
	})
	if process == nil || process.ActiveCount() != 0 {
		t.Fatal("fixture cleanup did not join every source before process release")
	}
	for _, owner := range []*worklifetime.RuntimeOccurrence{first, second} {
		if lease, err := owner.Begin(context.Background()); err == nil {
			lease.Done()
			t.Fatal("retired fixture runtime still admits work")
		}
	}
}

func TestNativePipelineWorkFixtureFailedJoinRetainsResponsibility(t *testing.T) {
	first := pipelineExternalTestWorkOwnerForSource(t, authorActivityTestSourceArtifactFact)
	other := mustAuthorActivityTestSourceArtifactFactForHash("bundle-v2:sha256:" + strings.Repeat("b", 64))
	second := pipelineExternalTestWorkOwnerForSource(t, other)
	lease, err := second.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	settled := false
	defer func() {
		if !settled {
			if err := lease.Done(); err != nil {
				t.Error(err)
			}
		}
	}()
	value, found := pipelineExternalTestWorkFixtures.Load(t)
	if !found {
		t.Fatal("missing fixture close owner")
	}
	fixture := value.(*pipelineExternalTestWorkFixture)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := fixture.retireAndWait(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("held source lease allowed close: %v", err)
	}
	if value, found := pipelineExternalTestWorkFixtures.Load(t); !found || value != fixture || fixture.process.ActiveCount() == 0 {
		t.Fatal("failed join discarded live fixture responsibility")
	}
	for _, owner := range []*worklifetime.RuntimeOccurrence{first, second} {
		if escaped, err := owner.Begin(context.Background()); err == nil {
			escaped.Done()
			t.Fatal("join did not fence every source before waiting")
		}
	}
	if err := lease.Done(); err != nil {
		t.Fatal(err)
	}
	settled = true
	if err := fixture.retireAndWait(context.Background()); err != nil || fixture.process.ActiveCount() != 0 {
		t.Fatalf("settled lease did not allow exact joined cleanup: %v", err)
	}
}

func TestNativePipelineWorkFixtureConcurrentLookupDoesNotDuplicateOccurrences(t *testing.T) {
	var workers sync.WaitGroup
	owners := make(chan *worklifetime.RuntimeOccurrence, 16)
	for i := 0; i < cap(owners); i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			owners <- pipelineExternalTestWorkOwnerForSource(t, authorActivityTestSourceArtifactFact)
		}()
	}
	workers.Wait()
	close(owners)
	want := pipelineExternalTestWorkOwnerForSource(t, authorActivityTestSourceArtifactFact)
	for owner := range owners {
		if owner != want {
			t.Fatal("concurrent fixture lookup constructed another runtime occurrence")
		}
	}
	value, _ := pipelineExternalTestWorkFixtures.Load(t)
	fixture := value.(*pipelineExternalTestWorkFixture)
	fixture.mu.Lock()
	count := len(fixture.runtimes)
	fixture.mu.Unlock()
	if count != 1 {
		t.Fatalf("fixture occurrence count=%d, want one", count)
	}
}
