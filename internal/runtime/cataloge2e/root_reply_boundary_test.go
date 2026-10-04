package cataloge2e

import (
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
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
			})
		}
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
