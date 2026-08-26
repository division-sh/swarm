package channelactivation

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestOwnerPublishesWholeExecutableSnapshot(t *testing.T) {
	owner, err := NewOwner(testEmptyPublication(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, found := owner.AcquireRuntimeOperation("channel.missing.deliver"); found {
		t.Fatal("empty owner exposed a runtime operation")
	}
	if err := owner.Replace(testEmptyPublication(t)); err != nil {
		t.Fatal(err)
	}
	presentation, available := owner.AcquirePresentation()
	if !available || len(presentation.Activations()) != 0 {
		t.Fatalf("empty publication presentation = %#v", presentation)
	}
	presentation.Release()
}

func TestOwnerRejectsDeclaredOnlyPublication(t *testing.T) {
	publication, err := channelonboarding.NewDeclaredOnlyChannelActivationPublication(nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewOwner(publication); err == nil {
		t.Fatal("declared-only publication granted executable authority")
	}
}

func TestOwnerReplacementFencesAndAwaitsPredecessorLease(t *testing.T) {
	owner, err := NewOwner(testEmptyPublication(t))
	if err != nil {
		t.Fatal(err)
	}
	owner.mu.Lock()
	owner.current.runtime["channel.test.deliver"] = Operation{Name: "deliver"}
	owner.mu.Unlock()
	lease, found := owner.AcquireRuntimeOperation("channel.test.deliver")
	if !found {
		t.Fatal("predecessor operation was not acquired")
	}

	done := make(chan error, 1)
	successor := testEmptyPublication(t)
	go func() { done <- owner.Replace(successor) }()
	deadline := time.Now().Add(time.Second)
	for {
		if blocked, ok := owner.AcquireRuntimeOperation("channel.test.deliver"); !ok {
			if !owner.HasRuntimeTool("channel.test.deliver") {
				t.Fatal("fenced predecessor lost its runtime-tool ownership before replacement completed")
			}
			break
		} else {
			blocked.Release()
		}
		if time.Now().After(deadline) {
			t.Fatal("replacement did not fence new predecessor leases")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-done:
		t.Fatalf("replacement completed before predecessor release: %v", err)
	default:
	}
	lease.Release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, found := owner.AcquireRuntimeOperation("channel.test.deliver"); found {
		t.Fatal("successor retained predecessor route")
	}
	if owner.HasRuntimeTool("channel.test.deliver") {
		t.Fatal("successor retained predecessor runtime-tool ownership")
	}
}

func TestOwnerCancelledReplacementRestoresPredecessorAdmission(t *testing.T) {
	owner, err := NewOwner(testEmptyPublication(t))
	if err != nil {
		t.Fatal(err)
	}
	owner.mu.Lock()
	owner.current.runtime["channel.test.deliver"] = Operation{Name: "deliver"}
	owner.mu.Unlock()
	lease, found := owner.AcquireRuntimeOperation("channel.test.deliver")
	if !found {
		t.Fatal("predecessor operation was not acquired")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := owner.ReplaceContext(ctx, testEmptyPublication(t)); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled replacement = %v", err)
	}
	lease.Release()
	reopened, found := owner.AcquireRuntimeOperation("channel.test.deliver")
	if !found {
		t.Fatal("cancelled replacement did not restore predecessor admission")
	}
	reopened.Release()
}

func TestOwnerPresentationLeaseFencesSuccessorPublication(t *testing.T) {
	owner, err := NewOwner(testEmptyPublication(t))
	if err != nil {
		t.Fatal(err)
	}
	presentation, found := owner.AcquirePresentation()
	if !found || !presentation.Generation().Valid() {
		t.Fatal("current publication presentation was not pinned")
	}
	done := make(chan error, 1)
	successor := testEmptyPublication(t)
	go func() { done <- owner.Replace(successor) }()
	deadline := time.Now().Add(time.Second)
	for {
		probe, open := owner.AcquirePresentation()
		if !open {
			break
		}
		probe.Release()
		if time.Now().After(deadline) {
			t.Fatal("replacement did not fence new presentation leases")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-done:
		t.Fatalf("replacement completed before presentation release: %v", err)
	default:
	}
	presentation.Release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestOwnerPresentationAcquisitionWaitsThroughReplacementFence(t *testing.T) {
	owner, err := NewOwner(testEmptyPublication(t))
	if err != nil {
		t.Fatal(err)
	}
	predecessor, found := owner.AcquirePresentation()
	if !found {
		t.Fatal("predecessor presentation was not acquired")
	}

	replaced := make(chan error, 1)
	go func() { replaced <- owner.Replace(testEmptyPublication(t)) }()
	deadline := time.Now().Add(time.Second)
	for {
		probe, open := owner.AcquirePresentation()
		if !open {
			break
		}
		probe.Release()
		if time.Now().After(deadline) {
			t.Fatal("replacement did not fence new presentation leases")
		}
		time.Sleep(time.Millisecond)
	}

	acquired := make(chan *Lease, 1)
	acquireErr := make(chan error, 1)
	go func() {
		lease, acquire := owner.AcquirePresentationContext(context.Background())
		if acquire != nil {
			acquireErr <- acquire
			return
		}
		acquired <- lease
	}()
	select {
	case lease := <-acquired:
		lease.Release()
		t.Fatal("presentation acquisition crossed the active replacement fence")
	case err := <-acquireErr:
		t.Fatalf("presentation acquisition failed while replacement was in progress: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	predecessor.Release()
	if err := <-replaced; err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-acquireErr:
		t.Fatalf("successor presentation acquisition failed: %v", err)
	case successor := <-acquired:
		if !successor.Generation().Valid() {
			t.Fatal("successor presentation has no generation")
		}
		successor.Release()
	case <-time.After(time.Second):
		t.Fatal("presentation acquisition did not resume after replacement")
	}
}

func TestConnectedChannelActivationSupersessionFencesEveryPredecessorConsumer(t *testing.T) {
	owner, err := NewOwner(testEmptyPublication(t))
	if err != nil {
		t.Fatal(err)
	}
	objectSchema := runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)
	tool := runtimecontracts.MustToolSchemaEntry(
		runtimecontracts.WithToolCategory("channel_operation"),
		runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerChannel),
		runtimecontracts.WithToolSchemas(objectSchema, objectSchema),
	)
	owner.mu.Lock()
	owner.current.runtime["channel.test.deliver"] = Operation{Name: "deliver"}
	owner.current.activities["platform.channel_activity.test.deliver"] = Operation{Name: "deliver"}
	owner.current.tools["channel.test.deliver"] = tool
	owner.mu.Unlock()
	runtimeLease, found := owner.AcquireRuntimeOperation("channel.test.deliver")
	if !found {
		t.Fatal("predecessor runtime route was not acquired")
	}
	predecessorGeneration := runtimeLease.Generation()
	activityLease, found := owner.AcquireActivityOperation("platform.channel_activity.test.deliver", predecessorGeneration)
	if !found {
		t.Fatal("predecessor private activity route was not acquired")
	}

	done := make(chan error, 1)
	successor := testEmptyPublication(t)
	go func() { done <- owner.Replace(successor) }()
	deadline := time.Now().Add(time.Second)
	for {
		runtimeProbe, runtimeOpen := owner.AcquireRuntimeOperation("channel.test.deliver")
		activityProbe, activityOpen := owner.AcquireActivityOperation("platform.channel_activity.test.deliver", predecessorGeneration)
		if runtimeOpen {
			runtimeProbe.Release()
		}
		if activityOpen {
			activityProbe.Release()
		}
		if !runtimeOpen && !activityOpen {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("supersession did not fence every predecessor consumer")
		}
		time.Sleep(time.Millisecond)
	}
	runtimeLease.Release()
	select {
	case err := <-done:
		t.Fatalf("supersession completed with a private activity lease live: %v", err)
	default:
	}
	activityLease.Release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, found := owner.AcquireRuntimeOperation("channel.test.deliver"); found {
		t.Fatal("successor retained predecessor runtime route")
	}
	if _, found := owner.AcquireActivityOperation("platform.channel_activity.test.deliver", predecessorGeneration); found {
		t.Fatal("successor retained predecessor private activity route")
	}
	presentation, available := owner.AcquirePresentation()
	if !available {
		t.Fatal("successor publication unavailable")
	}
	defer presentation.Release()
	if _, found := presentation.ToolEntries()["channel.test.deliver"]; found {
		t.Fatal("successor retained predecessor tool projection")
	}
}

func testEmptyPublication(t *testing.T) channelonboarding.ChannelActivationPublication {
	t.Helper()
	publication, err := channelonboarding.NewChannelActivationPublication(nil)
	if err != nil {
		t.Fatal(err)
	}
	return publication
}
