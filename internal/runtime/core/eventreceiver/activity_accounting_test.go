package eventreceiver

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/activityresult"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/google/uuid"
)

func receiverPublishedActivity(t *testing.T, runID string) activityresult.PublishedContinuation {
	t.Helper()
	publication, err := activityresult.NewPublishedContinuation(activityidentity.Fact{
		RunID: runID, SourceEventID: uuid.NewString(), EntityID: uuid.NewString(),
		Owner:           activityidentity.MustNodeOwner(identitytest.RootNode(t, "receiver")),
		ExecutionFlowID: ".", HandlerEventKey: "work.started", ActivityID: "compute", Tool: "compute", Attempt: 1,
	}, uuid.NewString(), "compute.succeeded", []byte(`{"result":"recorded"}`), executionmode.Live,
		contracts.ActivityEffectClassNonIdempotentWrite, "exact-input", "", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	return publication
}

func TestSelectedReceiverRetainsActivityAccountingAcrossFreshContexts(t *testing.T) {
	authority, admission, controller := selectedReceiverState(t, executionmode.Live)
	base, err := SelectedContractForkExecution(authority, admission, controller, correlation.RuntimeLineage{})
	if err != nil {
		t.Fatal(err)
	}
	publication := receiverPublishedActivity(t, authority.SelectedFork.ForkRunID)
	inputs := []activityresult.PublishedContinuation{publication}
	variant, err := base.WithSelectedActivityPublications(inputs)
	if err != nil {
		t.Fatal(err)
	}
	inputs[0] = activityresult.PublishedContinuation{}
	variant, err = variant.WithSelectedAdmission(admission)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		fresh, cleanup := NewContext(context.WithValue(context.Background(), receiverContextKey{}, "publisher-secret"))
		bound, err := variant.Bind(fresh, executionmode.Live)
		if err != nil || variant.ValidateBound(bound, executionmode.Live) != nil {
			cleanup()
			t.Fatalf("fresh receiver lost its owned accounting: %v", err)
		}
		retained, found := activityresult.PublishedContinuationFromContext(bound, publication.RequestID())
		if !found || retained.SourceResultID() != publication.SourceResultID() || bound.Value(receiverContextKey{}) != nil {
			cleanup()
			t.Fatal("fresh receiver lost immutable accounting or inherited publisher values")
		}
		empty, err := base.Bind(bound, executionmode.Live)
		if err != nil {
			cleanup()
			t.Fatal(err)
		}
		if _, found := activityresult.PublishedContinuationFromContext(empty, publication.RequestID()); found {
			cleanup()
			t.Fatal("empty selected owner inherited another publication inventory")
		}
		if err := variant.ValidateBound(empty, executionmode.Live); err == nil {
			cleanup()
			t.Fatal("receiver accepted another owner's empty inventory")
		}
		cleanup()
	}
	for _, inputs := range [][]activityresult.PublishedContinuation{
		{publication, publication}, {{}}, {receiverPublishedActivity(t, uuid.NewString())},
	} {
		if _, err := base.WithSelectedActivityPublications(inputs); err == nil {
			t.Fatal("invalid or foreign selected accounting accepted")
		}
	}
	if _, err := NormalExecution().WithSelectedActivityPublications([]activityresult.PublishedContinuation{publication}); err == nil {
		t.Fatal("normal receiver retained selected accounting")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := variant.Bind(canceled, executionmode.Live); err == nil {
		t.Fatal("canceled receiver bound selected accounting")
	}
}
