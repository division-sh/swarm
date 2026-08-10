package bus

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimepipelinefixture "github.com/division-sh/swarm/internal/testutil/runtimepipelinefixture"
)

type publishAndWaitCommitSpy struct {
	InMemoryEventStore
	commitCalls int
}

func (s *publishAndWaitCommitSpy) CommitPublication(ctx context.Context, command PublicationCommand) (CommittedPublication, error) {
	s.commitCalls++
	return s.InMemoryEventStore.CommitPublication(ctx, command)
}

func TestEventBusBundleSourceAdmissionRejectsConflictingOwnersBeforeCommit(t *testing.T) {
	const ownedHash = "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	owned, err := runtimecorrelation.NewPersistedBundleSourceFact(ownedHash)
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := runtimecorrelation.NewPersistedBundleSourceFact("bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	if err != nil {
		t.Fatal(err)
	}
	wrongSource, err := runtimecorrelation.NewEphemeralBundleSourceFact(ownedHash)
	if err != nil {
		t.Fatal(err)
	}

	for name, contextFact := range map[string]runtimecorrelation.BundleSourceFact{
		"different hash":             foreign,
		"same hash different source": wrongSource,
	} {
		t.Run(name, func(t *testing.T) {
			store := &publishAndWaitCommitSpy{}
			eb, err := newScopedTestEventBus(store, EventBusOptions{BundleSourceFact: owned})
			if err != nil {
				t.Fatalf("create event bus: %v", err)
			}
			ctx := runtimecorrelation.WithBundleSourceFact(context.Background(), contextFact)

			err = eb.Publish(ctx, completionTreeEvent("11111111-1111-4111-8111-111111111144", "custom.root"))
			if err == nil || !strings.Contains(err.Error(), "bundle source fact conflicts") {
				t.Fatalf("Publish error = %v, want bundle source conflict", err)
			}
			if store.commitCalls != 0 {
				t.Fatalf("commit calls = %d, want 0", store.commitCalls)
			}
		})
	}
}

func TestEventBusBundleSourceAdmissionAllowsContextOwnedSelectedForkBus(t *testing.T) {
	sourceFact, err := runtimecorrelation.NewEphemeralBundleSourceFact("bundle-v1:sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
	if err != nil {
		t.Fatal(err)
	}
	eb, err := NewEphemeralEventBusWithOptions(InMemoryEventStore{}, EventBusOptions{
		ExecutionPosture:  executionposture.Live,
		ReceiverExecution: eventreceiver.NormalExecution(),
	})
	if err != nil {
		t.Fatalf("create ownerless selected-fork event bus: %v", err)
	}
	ctx := runtimecorrelation.WithBundleSourceFact(context.Background(), sourceFact)

	admitted, err := eb.AdmitBundleSourceFact(ctx)
	if err != nil {
		t.Fatalf("AdmitBundleSourceFact: %v", err)
	}
	got, ok := runtimecorrelation.BundleSourceFactFromContext(admitted)
	if !ok || !got.Matches(sourceFact) {
		t.Fatalf("admitted source fact = %#v/%v, want exact context-owned fact", got, ok)
	}
}

func TestExplicitEphemeralEventBusAllowsOwnerlessPublish(t *testing.T) {
	store := &publishAndWaitCommitSpy{}
	process := worklifetime.NewProcess()
	runtimeOwner, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: "ownerless-source-runtime",
		BundleHash:        "ownerless-source-bundle",
	})
	if err != nil {
		t.Fatalf("create runtime occurrence: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if _, err := runtimeOwner.RetireAndWait(ctx); err != nil {
			t.Errorf("retire runtime occurrence: %v", err)
		}
		if _, err := process.Join(ctx); err != nil {
			t.Errorf("join process occurrence: %v", err)
		}
	})
	eb, err := NewEphemeralEventBusWithOptions(store, EventBusOptions{
		ExecutionPosture:  executionposture.Live,
		RuntimeInstanceID: "ownerless-source-runtime",
		WorkOwner:         runtimeOwner,
		ReceiverExecution: eventreceiver.NormalExecution(),
	})
	if err != nil {
		t.Fatalf("create ownerless event bus: %v", err)
	}

	err = eb.Publish(context.Background(), completionTreeEvent("11111111-1111-4111-8111-111111111145", "custom.root"))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if store.commitCalls != 1 {
		t.Fatalf("commit calls = %d, want 1", store.commitCalls)
	}
}

func TestEventBusPublishAndWaitIgnoresAmbientSQLTransactionAuthority(t *testing.T) {
	store := &publishAndWaitCommitSpy{}
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("create event bus: %v", err)
	}
	ctx := runtimepipelinefixture.WithSQLTx(context.Background(), &sql.Tx{})

	if err := eb.PublishAndWait(ctx, completionTreeEvent("11111111-1111-4111-8111-111111111143", "custom.root")); err != nil {
		t.Fatalf("PublishAndWait: %v", err)
	}
	if store.commitCalls != 1 {
		t.Fatalf("closed commit calls = %d, want 1", store.commitCalls)
	}
}

func TestEventBusPublishAndWaitJoinsOnlyAcceptedDeliveryTree(t *testing.T) {
	process := worklifetime.NewProcess()
	runtimeOwner, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: "publish-and-wait-runtime",
		BundleHash:        "publish-and-wait-bundle",
	})
	if err != nil {
		t.Fatalf("create runtime occurrence: %v", err)
	}
	unrelated, err := runtimeOwner.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin unrelated runtime work: %v", err)
	}
	t.Cleanup(func() {
		if err := unrelated.Done(); err != nil && !errors.Is(err, worklifetime.ErrAlreadySettled) {
			t.Errorf("settle unrelated runtime work: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if _, err := runtimeOwner.RetireAndWait(ctx); err != nil {
			t.Errorf("retire runtime occurrence: %v", err)
		}
		if _, err := process.Join(ctx); err != nil {
			t.Errorf("join process occurrence: %v", err)
		}
	})

	eb, err := newScopedTestEventBus(InMemoryEventStore{}, EventBusOptions{WorkOwner: runtimeOwner})
	if err != nil {
		t.Fatalf("create event bus: %v", err)
	}
	rootEvents := subscribeInternalDeliveriesForTest(t, eb, "root-handler", events.EventType("custom.root"))
	leafEvents := subscribeInternalDeliveriesForTest(t, eb, "leaf-handler", events.EventType("custom.leaf"))
	leafAccepted := make(chan struct{})
	releaseLeaf := make(chan struct{})
	handlerErr := make(chan error, 1)

	go func() {
		root := <-rootEvents
		if err := eb.Publish(root.Context(), completionTreeEvent("11111111-1111-4111-8111-111111111142", "custom.leaf")); err != nil {
			handlerErr <- err
			_ = root.Complete()
			return
		}
		_ = root.Complete()
		leaf := <-leafEvents
		close(leafAccepted)
		<-releaseLeaf
		handlerErr <- leaf.Complete()
	}()

	publishDone := make(chan error, 1)
	go func() {
		publishDone <- eb.PublishAndWait(context.Background(), completionTreeEvent("11111111-1111-4111-8111-111111111141", "custom.root"))
	}()
	select {
	case <-leafAccepted:
	case err := <-publishDone:
		t.Fatalf("publish and wait returned before descendant acceptance: %v", err)
	case err := <-handlerErr:
		t.Fatalf("root handler failed before descendant acceptance: %v", err)
	case <-time.After(time.Second):
		t.Fatal("descendant delivery was not accepted")
	}
	assertCompletionGroupBlocked(t, publishDone, "descendant delivery")
	close(releaseLeaf)
	select {
	case err := <-handlerErr:
		if err != nil {
			t.Fatalf("complete delivery tree: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("descendant handler did not complete")
	}
	select {
	case err := <-publishDone:
		if err != nil {
			t.Fatalf("publish and wait: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("publish and wait retained unrelated runtime work")
	}
}

func TestLocalDeliveryCompletionGroupJoinsDescendantTreeWithoutPolling(t *testing.T) {
	process := worklifetime.NewProcess()
	runtimeOwner, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: "completion-tree-runtime",
		BundleHash:        "completion-tree-bundle",
	})
	if err != nil {
		t.Fatalf("create runtime occurrence: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if _, err := runtimeOwner.RetireAndWait(ctx); err != nil {
			t.Errorf("retire runtime occurrence: %v", err)
		}
		if _, err := process.Join(ctx); err != nil {
			t.Errorf("join process occurrence: %v", err)
		}
	})

	group := newLocalDeliveryCompletionGroup()
	ctx := withLocalDeliveryCompletionGroup(context.Background(), group)
	root, err := runtimeOwner.NewEventDelivery(ctx, completionTreeTestEvent("completion-root"))
	if err != nil {
		t.Fatalf("create root delivery: %v", err)
	}
	if err := trackLocalDeliveryCompletion(ctx, root); err != nil {
		t.Fatalf("track root delivery: %v", err)
	}
	group.releaseDispatch()

	drained := make(chan error, 1)
	go func() { drained <- group.wait(context.Background()) }()
	assertCompletionGroupBlocked(t, drained, "root delivery")

	child, err := runtimeOwner.NewEventDelivery(root.Context(), completionTreeTestEvent("completion-child"))
	if err != nil {
		t.Fatalf("create descendant delivery: %v", err)
	}
	if err := trackLocalDeliveryCompletion(root.Context(), child); err != nil {
		t.Fatalf("track descendant delivery: %v", err)
	}
	if err := root.Complete(); err != nil {
		t.Fatalf("complete root delivery: %v", err)
	}
	assertCompletionGroupBlocked(t, drained, "descendant delivery")

	if err := child.Complete(); err != nil {
		t.Fatalf("complete descendant delivery: %v", err)
	}
	select {
	case err := <-drained:
		if err != nil {
			t.Fatalf("wait for delivery tree: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("delivery tree did not drain after descendant completion")
	}
}

func completionTreeTestEvent(id string) events.Event {
	return completionTreeEvent(id, events.EventType("test.completion"))
}

func completionTreeEvent(id string, eventType events.EventType) events.Event {
	return eventtest.RuntimeControl(id, eventType, "test", "", []byte(`{}`), 0, "11111111-1111-4111-8111-111111111140", "", events.EventEnvelope{}, time.Now().UTC())
}

func assertCompletionGroupBlocked(t *testing.T, drained <-chan error, label string) {
	t.Helper()
	select {
	case err := <-drained:
		t.Fatalf("completion group drained before %s completed: %v", label, err)
	default:
	}
}
