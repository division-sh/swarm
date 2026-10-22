package genericschedule

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/forkpoint"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/google/uuid"
)

func TestForkJoinOriginDecoderRefusesPartialEvidence(t *testing.T) {
	if absent, err := DecodeForkJoinOrigin(ForkJoinOrigin{}); err != nil || absent != nil {
		t.Fatal("ordinary schedule acquired inherited provenance")
	}
	valid := ForkJoinOrigin{
		SourceActivationID: uuid.NewString(), SourceRunID: uuid.NewString(),
		PointKind: forkpoint.RunStart, PointRevision: 1,
		SourceAdmittedAt: time.Now().UTC().Truncate(time.Microsecond), Owner: ForkJoinReconstructionOwner,
	}
	if origin, err := DecodeForkJoinOrigin(valid); err != nil || origin == nil || *origin != valid {
		t.Fatalf("exact origin rejected: %v %v", origin, err)
	}
	for _, edit := range []struct {
		name  string
		apply func(*ForkJoinOrigin)
	}{
		{"source_activation", func(o *ForkJoinOrigin) { o.SourceActivationID = "" }},
		{"source_run", func(o *ForkJoinOrigin) { o.SourceRunID = "" }},
		{"point_kind", func(o *ForkJoinOrigin) { o.PointKind = "" }},
		{"point_revision", func(o *ForkJoinOrigin) { o.PointRevision = 0 }},
		{"point_event", func(o *ForkJoinOrigin) { o.PointKind = forkpoint.Event }},
		{"start_event", func(o *ForkJoinOrigin) { o.PointEventID = uuid.NewString() }},
		{"source_admission", func(o *ForkJoinOrigin) { o.SourceAdmittedAt = time.Time{} }},
		{"owner", func(o *ForkJoinOrigin) { o.Owner = "another-owner" }},
	} {
		t.Run(edit.name, func(t *testing.T) {
			bad := valid
			edit.apply(&bad)
			if _, err := DecodeForkJoinOrigin(bad); err == nil {
				t.Fatal("partial inherited evidence was treated as ordinary admission")
			}
		})
	}
}

func forkJoinOriginTestActivation(t *testing.T) Activation {
	t.Helper()
	command := testJoinScheduleCommand(t, "orders", "orders/order-1", attemptgeneration.Generation{})
	_, ref, ok := timeridentity.ParseJoinHandle(command.Payload.Interface().(map[string]any))
	if !ok {
		t.Fatal("fixture lacks exact entry")
	}
	entry := ref.StageEntry()
	entry.OriginRunID = uuid.NewString()
	ref, err := ref.Declaration().BindStageEntry(entry, ref.Generation())
	if err != nil {
		t.Fatal(err)
	}
	handle, err := timeridentity.JoinTimeoutHandle(ref)
	if err != nil {
		t.Fatal(err)
	}
	command.Payload, err = canonicaljson.FromGo(handle.PayloadMetadata())
	if err != nil {
		t.Fatal(err)
	}
	command.ScheduleKey, command.TaskID = handle.TaskID(), handle.TaskID()
	activation := exactWorkflowJoinSchedule(t, command)
	activation.ForkJoinOrigin = &ForkJoinOrigin{
		SourceActivationID: uuid.NewString(), SourceRunID: entry.OriginRunID,
		PointKind: forkpoint.RunStart, PointRevision: 1,
		SourceAdmittedAt: activation.AdmittedAt.Add(-time.Hour), Owner: ForkJoinReconstructionOwner,
	}
	if err := activation.Validate(); err != nil {
		t.Fatal(err)
	}
	return activation
}

func TestForkJoinOriginBindsDecodedActivationAndDigest(t *testing.T) {
	activation := forkJoinOriginTestActivation(t)
	before := *activation.ForkJoinOrigin
	want, err := activation.EvidenceDigest()
	if err != nil {
		t.Fatal(err)
	}
	copy := activation.Canonical()
	copy.ForkJoinOrigin.PointRevision++
	got, err := copy.EvidenceDigest()
	if err != nil || got == want || *activation.ForkJoinOrigin != before {
		t.Fatal("canonical provenance aliases the source or escapes evidence digest")
	}
	copy.ForkJoinOrigin.SourceRunID = activation.Command.RunID
	if err := copy.Validate(); err == nil {
		t.Fatal("same-run source lineage was admitted")
	}
}

func TestForkJoinReplayRetainsCauseThroughCanonicalProgress(t *testing.T) {
	expected := forkJoinOriginTestActivation(t)
	for _, name := range []string{"active", "prepared", "fired", "cancelled", "failed", "parked", "fired_without_occurrence", "changed_birth", "changed_due", "changed_source", "changed_cut", "ordinary", "before_birth", "before_due", "bad_occurrence", "changed_payload"} {
		t.Run(name, func(t *testing.T) {
			actual := expected.Canonical()
			at := actual.CurrentDueAt.Add(time.Minute)
			switch name {
			case "prepared", "fired", "before_due", "bad_occurrence":
				actual.CurrentEventID = OccurrenceEventID(actual.ID, actual.CurrentDueAt)
				actual.CurrentEventAdmittedAt = at
				if name == "fired" {
					actual.Status, actual.FiredAt, actual.AcceptedAt = StatusFired, at, at
				}
				if name == "before_due" {
					actual.CurrentEventAdmittedAt = actual.CurrentDueAt.Add(-time.Second)
				}
				if name == "bad_occurrence" {
					actual.CurrentEventID = uuid.NewString()
				}
			case "cancelled", "before_birth":
				actual.Status, actual.CancelCause, actual.CancelledAt = StatusCancelled, "join_stage_exit", at
				if name == "before_birth" {
					actual.CancelledAt = actual.AdmittedAt.Add(-time.Second)
				}
			case "failed":
				actual.Status, actual.FailedAt, actual.Failure = StatusFailed, at, Failure{Code: "publication_failed"}
			case "parked":
				actual.Status = StatusParked
			case "fired_without_occurrence":
				actual.Status, actual.FiredAt, actual.AcceptedAt = StatusFired, at, at
			case "changed_birth":
				actual.AdmittedAt = actual.AdmittedAt.Add(time.Second)
			case "changed_due":
				actual.CurrentDueAt, actual.InitialDueAt = at, at
				actual.Command.Due = AbsoluteDue(at)
				actual.ImmutableHash, _ = actual.Command.ImmutableHash()
			case "changed_source":
				actual.ForkJoinOrigin.SourceActivationID = uuid.NewString()
			case "changed_cut":
				actual.ForkJoinOrigin.PointRevision++
			case "ordinary":
				actual.ForkJoinOrigin = nil
			case "changed_payload":
				payload := actual.Command.Payload.Interface().(map[string]any)
				payload["extra"] = "different_business_data"
				actual.Command.Payload, _ = canonicaljson.FromGo(payload)
				actual.ImmutableHash, _ = actual.Command.ImmutableHash()
			}
			err := actual.ValidateForkJoinReplay(expected)
			allowed := name == "active" || name == "prepared" || name == "fired" || name == "cancelled" || name == "failed"
			if (err == nil) != allowed {
				t.Fatalf("%s progress: err=%v", name, err)
			}
		})
	}
	if err := expected.Validate(); err != nil {
		t.Fatal("replay mutated expected cause", err)
	}
	closed := expected.Canonical()
	closed.Status, closed.CancelCause, closed.CancelledAt = StatusCancelled, "join_closed", closed.AdmittedAt
	if err := closed.ValidateForkJoinReplay(closed); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"reopened", "changed_cause", "changed_time"} {
		actual := closed.Canonical()
		switch name {
		case "reopened":
			actual.Status, actual.CancelCause, actual.CancelledAt = StatusActive, "", time.Time{}
		case "changed_cause":
			actual.CancelCause = "join_stage_exit"
		case "changed_time":
			actual.CancelledAt = actual.CancelledAt.Add(time.Second)
		}
		if err := actual.ValidateForkJoinReplay(closed); err == nil {
			t.Fatal("canceled inherited disposition changed", name)
		}
	}
}
