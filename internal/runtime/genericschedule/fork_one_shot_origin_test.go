package genericschedule

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/forkpoint"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/google/uuid"
)

func oneShotOriginActivation(t *testing.T, delay bool) Activation {
	t.Helper()
	basis := time.Date(2026, 10, 10, 8, 0, 0, 0, time.UTC)
	runID := uuid.NewString()
	due := AbsoluteDue(basis.Add(time.Hour))
	if delay {
		due = DelayDue(time.Hour)
	}
	route, err := events.NewRootRoutingSource(runID)
	if err != nil {
		t.Fatal(err)
	}
	command := AdmissionCommand{
		ScheduleKey: "agent-one-shot", RunID: runID, EntityID: runID, OwnerKind: OwnerAgent, OwnerID: "agent-a",
		AgentIdentity: agentidentitytest.RootDeclaredForRun(t, runID, "agent-a", "test/agents.yaml"),
		EventType:     "platform.generic_schedule_proof", Payload: semanticvalue.EmptyObject(), RoutingSource: route,
		ExecutionMode: executionmode.Live, ReplyContext: uuid.NewString(), Due: due, TaskID: "agent-one-shot",
	}
	hash, err := command.ImmutableHash()
	if err != nil {
		t.Fatal(err)
	}
	a := Activation{
		ID: uuid.NewString(), Command: command, ImmutableHash: hash, AdmittedAt: basis.Add(2 * time.Hour),
		InitialDueAt: basis.Add(time.Hour), CurrentDueAt: basis.Add(time.Hour), Status: StatusActive,
		ForkOneShotOrigin: &ForkOneShotOrigin{
			SourceActivationID: uuid.NewString(), SourceRunID: uuid.NewString(),
			PointKind: forkpoint.RunStart, PointRevision: 1, DueBasisAt: basis, Owner: ForkOneShotReconstructionOwner,
		},
	}
	if err := a.Validate(); err != nil {
		t.Fatal(err)
	}
	return a
}

func TestForkOneShotOriginDecodeRefusesPartialAndForeignEvidence(t *testing.T) {
	if absent, err := DecodeForkOneShotOrigin(ForkOneShotOrigin{}); err != nil || absent != nil {
		t.Fatal("ordinary schedule acquired one-shot provenance")
	}
	valid := *oneShotOriginActivation(t, true).ForkOneShotOrigin
	if got, err := DecodeForkOneShotOrigin(valid); err != nil || got == nil || *got != valid {
		t.Fatalf("exact origin: got=%v err=%v", got, err)
	}
	for _, tc := range []struct {
		name string
		edit func(*ForkOneShotOrigin)
	}{
		{"activation", func(o *ForkOneShotOrigin) { o.SourceActivationID = "" }},
		{"run", func(o *ForkOneShotOrigin) { o.SourceRunID = "" }},
		{"nil_uuid", func(o *ForkOneShotOrigin) { o.SourceRunID = uuid.Nil.String() }},
		{"kind", func(o *ForkOneShotOrigin) { o.PointKind = "" }},
		{"revision", func(o *ForkOneShotOrigin) { o.PointRevision = 0 }},
		{"missing_event", func(o *ForkOneShotOrigin) { o.PointKind = forkpoint.Event }},
		{"extra_event", func(o *ForkOneShotOrigin) { o.PointEventID = uuid.NewString() }},
		{"basis", func(o *ForkOneShotOrigin) { o.DueBasisAt = time.Time{} }},
		{"noncanonical_basis", func(o *ForkOneShotOrigin) { o.DueBasisAt = o.DueBasisAt.Add(time.Nanosecond) }},
		{"join_owner", func(o *ForkOneShotOrigin) { o.Owner = ForkJoinReconstructionOwner }},
		{"foreign_owner", func(o *ForkOneShotOrigin) { o.Owner = "other" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := valid
			tc.edit(&bad)
			if _, err := DecodeForkOneShotOrigin(bad); err == nil {
				t.Fatal("partial origin decoded")
			}
		})
	}
}

func TestForkOneShotOriginAuthenticatesOriginalBasisAndDigest(t *testing.T) {
	for _, delay := range []bool{false, true} {
		a := oneShotOriginActivation(t, delay)
		want, err := a.EvidenceDigest()
		if err != nil {
			t.Fatal(err)
		}
		copy := a.Canonical()
		copy.ForkOneShotOrigin.PointRevision++
		got, err := copy.EvidenceDigest()
		if err != nil || got == want || a.ForkOneShotOrigin.PointRevision != 1 {
			t.Fatal("canonical origin aliases source or escapes digest")
		}
		copy = a.Canonical()
		copy.ForkOneShotOrigin.DueBasisAt = a.AdmittedAt
		if delay && copy.Validate() == nil {
			t.Fatal("child birth restarted delay")
		}
		if !delay {
			got, err = copy.EvidenceDigest()
			if err != nil || got == want {
				t.Fatal("absolute command lost authenticated original basis")
			}
		}
		copy = a.Canonical()
		copy.ForkOneShotOrigin.SourceRunID = a.Command.RunID
		if copy.Validate() == nil {
			t.Fatal("same-run lineage accepted")
		}
		copy = a.Canonical()
		copy.ForkJoinOrigin = &ForkJoinOrigin{}
		if copy.Validate() == nil {
			t.Fatal("multiple origins accepted")
		}
		copy = a.Canonical()
		copy.ForkOneShotOrigin = nil
		if delay && copy.Validate() == nil {
			t.Fatal("inherited delay passed without authenticated basis")
		}
	}
	join := forkJoinOriginTestActivation(t)
	if join.ValidateForkOneShotRestorationSource() == nil {
		t.Fatal("one-shot variant admitted arrival join")
	}
}

func TestForkOneShotReplayPreservesCauseAcrossLifecycleProgress(t *testing.T) {
	expected := oneShotOriginActivation(t, true)
	for _, name := range []string{"active", "prepared", "fired", "cancelled", "failed", "parked", "fired_without_occurrence",
		"before_birth", "before_due", "wrong_occurrence", "birth", "basis", "cut", "source", "command", "ordinary"} {
		t.Run(name, func(t *testing.T) {
			a := expected.Canonical()
			at := a.AdmittedAt.Add(time.Minute)
			switch name {
			case "prepared", "fired", "before_due", "wrong_occurrence":
				a.CurrentEventID, a.CurrentEventAdmittedAt = OccurrenceEventID(a.ID, a.CurrentDueAt), at
				if name == "fired" {
					a.Status, a.FiredAt, a.AcceptedAt = StatusFired, at, at
				}
				if name == "before_due" {
					a.CurrentEventAdmittedAt = a.CurrentDueAt.Add(-time.Second)
				}
				if name == "wrong_occurrence" {
					a.CurrentEventID = uuid.NewString()
				}
			case "cancelled", "before_birth":
				a.Status, a.CancelCause, a.CancelledAt = StatusCancelled, "cancelled", at
				if name == "before_birth" {
					a.CancelledAt = a.AdmittedAt.Add(-time.Second)
				}
			case "failed":
				a.Status, a.Failure, a.FailedAt = StatusFailed, Failure{Code: "publication_failed"}, at
			case "parked":
				a.Status = StatusParked
			case "fired_without_occurrence":
				a.Status, a.FiredAt, a.AcceptedAt = StatusFired, at, at
			case "birth":
				a.AdmittedAt = a.AdmittedAt.Add(time.Second)
			case "basis":
				a.ForkOneShotOrigin.DueBasisAt = a.ForkOneShotOrigin.DueBasisAt.Add(time.Second)
			case "cut":
				a.ForkOneShotOrigin.PointRevision++
			case "source":
				a.ForkOneShotOrigin.SourceActivationID = uuid.NewString()
			case "command":
				a.Command.Payload, _ = canonicaljson.FromGo(map[string]any{"changed": true})
				a.ImmutableHash, _ = a.Command.ImmutableHash()
			case "ordinary":
				a.ForkOneShotOrigin = nil
			}
			allowed := name == "active" || name == "prepared" || name == "fired" || name == "cancelled" || name == "failed"
			if err := a.ValidateForkOneShotReplay(expected); (err == nil) != allowed {
				t.Fatalf("%s replay: err=%v", name, err)
			}
		})
	}
	for _, status := range []Status{StatusCancelled, StatusFailed} {
		closed := expected.Canonical()
		closed.Status = status
		if status == StatusCancelled {
			closed.CancelCause, closed.CancelledAt = "retained_cancel", closed.AdmittedAt
		} else {
			closed.Failure, closed.FailedAt = Failure{Code: "retained_failure", Message: "original evidence"}, closed.AdmittedAt
		}
		if err := closed.ValidateForkOneShotReplay(closed); err != nil {
			t.Fatal(err)
		}
		reopened := expected.Canonical()
		if reopened.ValidateForkOneShotReplay(closed) == nil {
			t.Fatal("retained terminal row rearmed")
		}
		changed := closed.Canonical()
		if status == StatusCancelled {
			changed.CancelCause = "different_cancel"
		} else {
			changed.Failure.Message = "different_failure"
		}
		if changed.ValidateForkOneShotReplay(closed) == nil {
			t.Fatal("retained terminal evidence changed")
		}
	}
}
