package genericschedule

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/google/uuid"
)

func TestTransferredJoinCommittedCauseSurvivesUnpublishedRefork(t *testing.T) {
	for _, kind := range []timeridentity.TimerHandleKind{timeridentity.TimerHandleJoinComplete, timeridentity.TimerHandleJoinTimeout} {
		t.Run(string(kind), func(t *testing.T) {
			_, parent, _ := transferredJoinOccurrenceFixture(t, "orders", kind, executionmode.Mock)
			ancestorRun, ancestorEvent := parent.Publication.SourceRunID, parent.Publication.SourceEventID
			for depth := 0; depth < 3; depth++ {
				child := publishedJoinContinuationChild(t, parent.Command)
				projected, err := parent.Project(child, "selection:unpublished-refork")
				if err != nil {
					t.Fatal(err)
				}
				if projected.Publication.SourceRunID != parent.Command.RunID || projected.Publication.SourceEventID != parent.Publication.EventID {
					t.Fatal("unpublished projection skipped its immediate parent's pending obligation")
				}
				raw, err := json.Marshal(projected.Publication)
				if err != nil {
					t.Fatal(err)
				}
				var record struct {
					Cause struct {
						RunID   string `json:"run_id"`
						EventID string `json:"event_id"`
					} `json:"committed_cause"`
				}
				if err := json.Unmarshal(raw, &record); err != nil {
					t.Fatal(err)
				}
				if record.Cause.RunID != ancestorRun || record.Cause.EventID != ancestorEvent {
					t.Fatalf("unpublished refork lost its committed ancestor cause: %s", raw)
				}
				if continuation, err := ProjectTransferredJoinContinuation(parent, events.Event{}, child); err == nil || continuation.Present() {
					t.Fatal("retained committed cause fabricated an immediate-parent publication")
				}
				parent = projected
			}
		})
	}
}

func TestTransferredJoinCommittedCauseRejectsHostileCoordinates(t *testing.T) {
	_, occurrence, _ := transferredJoinOccurrenceFixture(t, "orders", timeridentity.TimerHandleJoinComplete, executionmode.Mock)
	for _, fault := range []string{"missing", "null", "missing_run", "missing_event", "nil_run", "noncanonical_run", "noncanonical_event", "same_destination", "contradictory_source"} {
		t.Run(fault, func(t *testing.T) {
			raw, err := json.Marshal(occurrence.Publication)
			if err != nil {
				t.Fatal(err)
			}
			var record map[string]any
			if err := json.Unmarshal(raw, &record); err != nil {
				t.Fatal(err)
			}
			cause := map[string]any{"run_id": occurrence.Publication.SourceRunID, "event_id": occurrence.Publication.SourceEventID}
			record["committed_cause"] = cause
			switch fault {
			case "missing":
				delete(record, "committed_cause")
			case "null":
				record["committed_cause"] = nil
			case "missing_run":
				delete(cause, "run_id")
			case "missing_event":
				delete(cause, "event_id")
			case "nil_run":
				cause["run_id"] = uuid.Nil.String()
			case "noncanonical_run":
				cause["run_id"] = " " + occurrence.Publication.SourceRunID
			case "noncanonical_event":
				cause["event_id"] = strings.ReplaceAll(occurrence.Publication.SourceEventID, "-", "")
			case "same_destination":
				cause["run_id"] = occurrence.Command.RunID
			case "contradictory_source":
				cause["event_id"] = uuid.NewString()
			}
			raw, err = json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			changed := occurrence
			changed.Publication = joinruntime.TransferredPublication{}
			if err := json.Unmarshal(raw, &changed.Publication); err != nil {
				t.Fatal(err)
			}
			if err := changed.Validate(); err == nil {
				t.Fatal("unproven or contradictory committed cause was accepted")
			}
		})
	}
}

func TestTransferredJoinEvidenceDigestBindsCommittedCause(t *testing.T) {
	_, occurrence, _ := transferredJoinOccurrenceFixture(t, "orders", timeridentity.TimerHandleJoinTimeout, executionmode.Mock)
	child := publishedJoinContinuationChild(t, occurrence.Command)
	projected, err := occurrence.Project(child, "selection:unpublished-refork")
	if err != nil {
		t.Fatal(err)
	}
	before, err := projected.EvidenceDigest()
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"run", "event"} {
		t.Run(field, func(t *testing.T) {
			changed := projected
			if field == "run" {
				changed.Publication.CommittedCause.RunID = uuid.NewString()
			} else {
				changed.Publication.CommittedCause.EventID = uuid.NewString()
			}
			after, err := changed.EvidenceDigest()
			if err != nil || before == after {
				t.Fatalf("evidence digest omitted committed cause %s: %v", field, err)
			}
			if projected.Publication.CommittedCause != occurrence.Publication.CommittedCause {
				t.Fatal("edited projection aliased the retained source cause")
			}
		})
	}
}
