package cataloge2e

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestConnectionReplyRootRequesterBothStores(t *testing.T) {
	proveRootReplyBoundary(t, true)
}

func TestConnectionReplyRootProviderBothStores(t *testing.T) {
	proveRootReplyBoundary(t, false)
}

func proveRootReplyBoundary(t *testing.T, rootRequester bool) {
	t.Helper()
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		for _, explicit := range []bool{false, true} {
			name := string(backend) + "/event-id"
			if explicit {
				name = string(backend) + "/explicit"
			}
			t.Run(name, func(t *testing.T) {
				root := canonicalrouting.CopyRootReplyBoundary(t, rootRequester, explicit)
				h := newRuntimeHarnessForBackend(t, root, backend, true)
				t.Cleanup(func() {
					if !t.Failed() {
						return
					}
					rows, err := h.db.Query(`SELECT CAST(e.event_id AS TEXT),e.event_name,
						COALESCE(r.outcome,''),COALESCE(r.reason_code,'')
						FROM events e LEFT JOIN event_receipts r ON r.event_id=e.event_id
						AND r.subscriber_type='platform' AND r.subscriber_id='pipeline'
						ORDER BY e.created_at,e.event_id LIMIT 100`)
					if err != nil {
						t.Logf("reply publication diagnostic: %v", err)
						return
					}
					defer rows.Close()
					for rows.Next() {
						var id, name, outcome, reason string
						if err := rows.Scan(&id, &name, &outcome, &reason); err != nil {
							t.Logf("reply publication diagnostic: %v", err)
							return
						}
						t.Logf("reply publication: event=%s name=%s outcome=%s reason=%s", id, name, outcome, reason)
					}
				})
				steps := []catalogTriggerStep{
					{Event: "request.started", Payload: map[string]any{"account_id": "account-a", "request_id": "request-one"}},
					{Event: "request.started", Payload: map[string]any{"account_id": "account-a", "request_id": "request-two"}},
					{Event: "request.started", Payload: map[string]any{"account_id": "account-b", "request_id": "request-three"}},
				}
				for i := range steps {
					steps[i].eventID, steps[i].createdAt, steps[i].sourceAgent = uuid.NewString(), time.Now().UTC(), "cataloge2e"
					steps[i].inputKind = catalogReplayInputRootIngress
				}
				h.publishConcurrentAndWait(steps, 20*time.Second)
				var expected catalogExpectedDocument
				expected.Expected.EmittedEvents = []string{"provider.requested", "provider.replied", "request.finished"}
				if rootRequester {
					expected.Expected.EmittedEvents[1] = "provider/provider.replied"
				} else {
					expected.Expected.EmittedEvents[0] = "requester/provider.requested"
					expected.Expected.EmittedEvents[2] = "requester/request.finished"
				}
				h.waitForExpectedEmittedEvents(expected, 20*time.Second)
				h.waitForCatalogStoreQuiescence(20 * time.Second)
				assertRootReplyEvidence(t, h, expected, 3)
				if rootRequester {
					assertConstructedRootReplyOrigins(t, h)
				}
				assertRootReplyRefusals(t, h, explicit)
				// Discarding the original acknowledgments and replaying the same
				// requests must consume committed evidence, never resend effects.
				for _, step := range steps {
					h.publishAndWait(step, 20*time.Second)
				}
				assertRootReplyEvidence(t, h, expected, 3)
				hash, err := runtimecontracts.BundleHash(h.bundle)
				if err != nil {
					t.Fatal(err)
				}
				digest, err := catalogReplayPlatformSpecDigest(repoRootFromCatalogE2E(t))
				if err != nil {
					t.Fatal(err)
				}
				h = h.reopenFromTranscript(&catalogExecutionTranscript{version: catalogReplayTranscriptVersion,
					platformSpecDigest: digest, bundleHash: hash, runID: catalogRuntimeRunID,
					groups: []catalogTranscriptGroup{{steps: steps}}})
				for _, step := range steps {
					h.publishAndWait(step, 20*time.Second)
				}
				h.waitForCatalogStoreQuiescence(20 * time.Second)
				assertRootReplyEvidence(t, h, expected, 3)
				assertRootReplyRefusals(t, h, explicit)
			})
		}
	}
}

func assertConstructedRootReplyOrigins(t *testing.T, h *runtimeHarness) {
	t.Helper()
	var reader bus.PreparedPublishEventReader = h.pg
	if h.sqlite != nil {
		reader = h.sqlite
	}
	rows, err := h.db.QueryContext(h.ctx, `SELECT CAST(request_event_id AS TEXT)
		FROM reply_contexts WHERE run_id=$1 ORDER BY request_event_id`, catalogRuntimeRunID)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	want := events.RouteIdentity{FlowID: ".", FlowInstance: catalogRuntimeRunID, EntityID: catalogRuntimeRunID}
	for _, id := range ids {
		request, found, err := reader.LoadPreparedPublishEvent(h.ctx, id)
		if err != nil || !found || request.Event.Event().RoutingSource().Kind() != events.RoutingSourceStaticFlow || request.Event.Event().SourceRoute() != want {
			t.Fatalf("root request lost exact construction: id=%s found=%v source=%+v err=%v", id, found, request.Event.Event().SourceRoute(), err)
		}
	}
}

func assertRootReplyRefusals(t *testing.T, h *runtimeHarness, explicit bool) {
	t.Helper()
	var backend interface {
		bus.PreparedPublishEventReader
		replycontext.Store
	} = h.pg
	if h.sqlite != nil {
		backend = h.sqlite
	}
	var contextID, requestID, acceptedID string
	if err := h.db.QueryRowContext(h.ctx, `SELECT reply_context_id, request_event_id, accepted_reply_event_id
		FROM reply_contexts WHERE run_id=$1 ORDER BY reply_context_id LIMIT 1`, catalogRuntimeRunID).Scan(&contextID, &requestID, &acceptedID); err != nil {
		t.Fatal(err)
	}
	request, found, err := backend.LoadPreparedPublishEvent(h.ctx, requestID)
	if err != nil || !found || len(request.DeliveryRoutes) != 1 {
		t.Fatalf("request readback=%+v found=%v error=%v", request, found, err)
	}
	accepted, found, err := backend.LoadPreparedPublishEvent(h.ctx, acceptedID)
	if err != nil || !found {
		t.Fatalf("reply readback=%+v found=%v error=%v", accepted, found, err)
	}
	carried := request.DeliveryRoutes[0].Context
	if carried.ReplyContextID() != contextID {
		t.Fatal("reply context lost in request readback")
	}
	for _, tc := range []struct {
		name, failure string
		stale, wrong  bool
	}{
		{name: "distinct-second", failure: runtimepinrouting.FailureReplyAlreadyTerminal.Code()},
		{name: "stale", failure: runtimepinrouting.FailureStaleArrival.Code(), stale: true},
		{name: "wrong-correlation", failure: runtimepinrouting.FailureStaleArrival.Code(), wrong: true},
	} {
		if tc.wrong && !explicit {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			original := accepted.Event.Event()
			payload := original.Payload()
			if tc.wrong {
				var object map[string]any
				if err := json.Unmarshal(payload, &object); err != nil {
					t.Fatal(err)
				}
				object["request_id"] = "not-the-original-request"
				payload, err = json.Marshal(object)
				if err != nil {
					t.Fatal(err)
				}
			}
			event := eventtest.ChildForProducerWithRoutingSource(uuid.NewString(), original.Type(), original.Producer(), "", payload,
				original.ChainDepth(), events.LineageFromEvent(original), events.EnvelopeForSourceRoute(events.EventEnvelope{}, original.Envelope().Source), original.RoutingSource(), time.Now().UTC())
			context := carried
			if tc.stale {
				context = events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "reply-v1:missing"}}
			}
			ctx := events.WithDeliveryContext(h.ctx, context)
			var before, after int
			if err := h.db.QueryRowContext(h.ctx, `SELECT count(*) FROM events WHERE run_id=$1`, catalogRuntimeRunID).Scan(&before); err != nil {
				t.Fatal(err)
			}
			plan, err := h.rt.Bus.CheckPublishRecipientPlan(ctx, event)
			if err != nil || len(plan.DeliveryRoutes) != 0 || plan.TargetFailure != tc.failure {
				t.Fatalf("late reply preflight=%+v error=%v want=%s", plan, err, tc.failure)
			}
			if err := h.db.QueryRowContext(h.ctx, `SELECT count(*) FROM events WHERE run_id=$1`, catalogRuntimeRunID).Scan(&after); err != nil {
				t.Fatal(err)
			}
			if before != after {
				t.Fatalf("preflight mutated event count: %d -> %d", before, after)
			}
			record, err := backend.LoadReplyContext(h.ctx, contextID)
			if err != nil || record.State != replycontext.StateTerminal || record.AcceptedReplyEventID != acceptedID {
				t.Fatalf("preflight changed terminal reply: %+v error=%v", record, err)
			}
		})
	}
}

func assertRootReplyEvidence(t *testing.T, h *runtimeHarness, expected catalogExpectedDocument, want int) {
	t.Helper()
	var total, accepted, distinctRequests, distinctReplies int
	if err := h.db.QueryRowContext(h.ctx, `SELECT count(*), count(accepted_reply_event_id),
		count(DISTINCT request_event_id), count(DISTINCT accepted_reply_event_id)
		FROM reply_contexts WHERE run_id=$1`, catalogRuntimeRunID).Scan(&total, &accepted, &distinctRequests, &distinctReplies); err != nil {
		t.Fatal(err)
	}
	if total != want || accepted != want || distinctRequests != want || distinctReplies != want {
		t.Fatalf("reply contexts=%d accepted=%d requests=%d replies=%d want=%d", total, accepted, distinctRequests, distinctReplies, want)
	}
	lister, err := h.catalogOperatorEventLister()
	if err != nil {
		t.Fatal(err)
	}
	public, err := loadCatalogOperatorEvents(h.ctx, lister)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range append([]string{"request.started"}, expected.Expected.EmittedEvents...) {
		var publications int
		if err := h.db.QueryRowContext(h.ctx, `SELECT count(*) FROM events WHERE run_id=$1 AND event_name=$2`, catalogRuntimeRunID, name).Scan(&publications); err != nil {
			t.Fatal(err)
		}
		visible := 0
		for _, event := range public {
			if event.EventName == name {
				visible++
			}
		}
		if publications != want || visible != want {
			t.Fatalf("%s persisted=%d public=%d want=%d; replay/restart must not resend", name, publications, visible, want)
		}
	}
}
