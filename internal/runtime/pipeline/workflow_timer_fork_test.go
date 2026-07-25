package pipeline

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/google/uuid"
)

func TestRemintWorkflowTimerActivationForForkPreservesOccurrenceLattice(t *testing.T) {
	createdAt := canonicalWorkflowTimerTime(time.Date(2026, time.July, 1, 12, 0, 0, 0, time.UTC))
	lineage := WorkflowTimerForkLineage{
		ForkRunID: uuid.NewString(), ForkEventID: uuid.NewString(), ReconstructionOwner: "selected_contract_execution",
	}
	for _, test := range []struct {
		name      string
		recurring bool
		fireAt    time.Time
		firedAt   time.Time
	}{
		{name: "one_shot", fireAt: createdAt.Add(time.Hour)},
		{name: "recurring_after_accepted_occurrence", recurring: true, fireAt: createdAt.Add(2 * time.Hour), firedAt: createdAt.Add(time.Hour)},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := WorkflowTimerActivation{
				Ref: timeridentity.WorkflowTimerActivationRef{
					ActivationID: uuid.NewString(), Declaration: "waiting.timeout",
				},
				RunID: uuid.NewString(), EntityID: uuid.NewString(), FlowInstance: "proof/1",
				OwnerAgent: "runtime", EventType: "platform.stage_timer", Payload: []byte(`{"source":true}`),
				FireAt: test.fireAt, Recurring: test.recurring, Status: workflowTimerStatusActive,
				CreatedAt: createdAt, FiredAt: test.firedAt,
			}
			if test.recurring {
				source.RecurrenceInterval = time.Hour
			}
			forked, err := RemintWorkflowTimerActivationForFork(source, lineage)
			if err != nil {
				t.Fatalf("RemintWorkflowTimerActivationForFork: %v", err)
			}
			if forked.Ref.ActivationID == source.Ref.ActivationID || forked.RunID != lineage.ForkRunID {
				t.Fatalf("fork identity = %#v, source=%#v lineage=%#v", forked.Ref, source.Ref, lineage)
			}
			if forked.SourceTimerID != source.Ref.ActivationID || forked.ForkedFromRunID != source.RunID ||
				forked.ForkedFromEventID != lineage.ForkEventID || forked.ReconstructionOwner != lineage.ReconstructionOwner {
				t.Fatalf("fork lineage = %#v, want source timer/run/event/owner", forked)
			}
			if !forked.CreatedAt.Equal(source.CreatedAt) || !forked.FireAt.Equal(source.FireAt) || !forked.FiredAt.IsZero() {
				t.Fatalf("fork occurrence coordinates = created:%s fire:%s fired:%s, want %s/%s/zero",
					forked.CreatedAt, forked.FireAt, forked.FiredAt, source.CreatedAt, source.FireAt)
			}
			if forked.Status != workflowTimerStatusActive || forked.Recurring != source.Recurring ||
				forked.RecurrenceInterval != source.RecurrenceInterval {
				t.Fatalf("fork recurrence/status = %#v, want source recurrence and active status", forked)
			}
		})
	}
}

func TestRemintPersistedWorkflowTimerForForkOwnsStrictDecode(t *testing.T) {
	createdAt := canonicalWorkflowTimerTime(time.Date(2026, time.July, 1, 12, 0, 0, 0, time.UTC))
	ref := timeridentity.WorkflowTimerActivationRef{
		ActivationID: uuid.NewString(),
		Declaration:  "waiting.timeout",
	}
	validSource := func() PersistedWorkflowTimerForkSource {
		return PersistedWorkflowTimerForkSource{
			ActivationID:         ref.ActivationID,
			SerializedActivation: ref.TaskID(),
			RunID:                uuid.NewString(),
			EntityID:             uuid.NewString(),
			FlowInstance:         "proof/1",
			OwnerAgent:           "runtime",
			EventType:            "platform.stage_timer",
			Payload:              []byte(`{"source":true}`),
			FireAt:               createdAt.Add(time.Hour),
			Status:               workflowTimerStatusActive,
			CreatedAt:            createdAt,
		}
	}
	lineage := WorkflowTimerForkLineage{
		ForkRunID:           uuid.NewString(),
		ForkEventID:         uuid.NewString(),
		ReconstructionOwner: "selected_contract_execution",
	}

	forked, err := RemintPersistedWorkflowTimerForFork(validSource(), lineage)
	if err != nil {
		t.Fatalf("RemintPersistedWorkflowTimerForFork(valid): %v", err)
	}
	if forked.SourceTimerID != ref.ActivationID || forked.RunID != lineage.ForkRunID {
		t.Fatalf("forked activation = %#v, want source identity and fork run", forked)
	}

	tests := []struct {
		name   string
		mutate func(*PersistedWorkflowTimerForkSource)
	}{
		{
			name: "malformed serialized activation",
			mutate: func(source *PersistedWorkflowTimerForkSource) {
				source.SerializedActivation = "workflow_timer:v1:invalid"
			},
		},
		{
			name: "activation id mismatch",
			mutate: func(source *PersistedWorkflowTimerForkSource) {
				source.ActivationID = uuid.NewString()
			},
		},
		{
			name: "node owner",
			mutate: func(source *PersistedWorkflowTimerForkSource) {
				source.OwnerNode = "timer-node"
			},
		},
		{
			name: "cron recurrence",
			mutate: func(source *PersistedWorkflowTimerForkSource) {
				source.Recurring = true
				source.RecurrenceCron = "* * * * *"
			},
		},
		{
			name: "invalid recurrence interval",
			mutate: func(source *PersistedWorkflowTimerForkSource) {
				source.Recurring = true
				source.RecurrenceInterval = "tomorrow"
			},
		},
		{
			name: "missing exact scope",
			mutate: func(source *PersistedWorkflowTimerForkSource) {
				source.FlowInstance = ""
			},
		},
		{
			name: "non-object payload",
			mutate: func(source *PersistedWorkflowTimerForkSource) {
				source.Payload = []byte(`[]`)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := validSource()
			test.mutate(&source)
			if _, err := RemintPersistedWorkflowTimerForFork(source, lineage); err == nil {
				t.Fatal("RemintPersistedWorkflowTimerForFork succeeded, want fail-closed decode")
			}
		})
	}
}
