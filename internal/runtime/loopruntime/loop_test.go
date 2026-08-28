package loopruntime

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestActivationLifecycleAndForkIdentity(t *testing.T) {
	now := time.Date(2026, time.July, 11, 12, 0, 0, 0, time.UTC)
	activation, err := New("run-a", "entity-a", "validation", "revision", "revision_id", "event-a", "drafting", 3, now)
	if err != nil {
		t.Fatal(err)
	}
	first := activation.Generation()
	if escaped, err := activation.Repeat("drafting", "event-b", now.Add(time.Minute)); err != nil || escaped {
		t.Fatalf("repeat = escaped %v err %v", escaped, err)
	}
	if activation.Attempt != 2 || activation.RevisionID == first.RevisionID {
		t.Fatalf("repeated activation = %#v", activation)
	}
	forked, err := Fork(activation, "run-fork", "entity-a")
	if err != nil {
		t.Fatal(err)
	}
	if forked.Attempt != activation.Attempt || forked.MaxAttempts != activation.MaxAttempts || forked.ActivationID == activation.ActivationID || forked.RevisionID == activation.RevisionID {
		t.Fatalf("forked activation = %#v, source %#v", forked, activation)
	}
	if err := activation.Close("approved", "event-c", now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if activation.Status != StatusClosed || activation.CloseReason != CloseReasonCompleted {
		t.Fatalf("closed activation = %#v", activation)
	}
}

func TestActivationPersistenceKeyIsJSONBPortable(t *testing.T) {
	activation, err := New("run-a", "entity-a", "validation", "revision", "revision_id", "event-a", "drafting", 3, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	buckets := map[string]map[string]any{}
	if err := Store(buckets, activation); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(buckets)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), `\u0000`) {
		t.Fatalf("loop persistence contains PostgreSQL jsonb-incompatible NUL escape: %s", raw)
	}
	loaded, ok, err := Load(buckets, activation.FlowID, activation.LoopID)
	if err != nil || !ok || loaded.ActivationID != activation.ActivationID {
		t.Fatalf("loaded activation = %#v found=%v err=%v", loaded, ok, err)
	}
}

func TestGenerationCurrentRequiresExactOpenActivationAndOptionalStage(t *testing.T) {
	now := time.Date(2026, time.July, 11, 12, 0, 0, 0, time.UTC)
	activation, err := New("run-a", "entity-a", "validation", "revision", "revision_id", "event-a", "drafting", 3, now)
	if err != nil {
		t.Fatal(err)
	}
	first := activation.Generation()
	buckets := map[string]map[string]any{}
	if err := Store(buckets, activation); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		stage string
		want  bool
	}{
		{name: "exact generation", want: true},
		{name: "exact generation and stage", stage: "drafting", want: true},
		{name: "wrong stage", stage: "review"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := GenerationCurrent(buckets, first, tc.stage)
			if err != nil || got != tc.want {
				t.Fatalf("GenerationCurrent() = %v, %v, want %v", got, err, tc.want)
			}
		})
	}
	if _, err := activation.Repeat("drafting", "event-b", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := Store(buckets, activation); err != nil {
		t.Fatal(err)
	}
	if current, err := GenerationCurrent(buckets, first, ""); err != nil || current {
		t.Fatalf("superseded generation current = %v, %v, want false", current, err)
	}
	if current, err := GenerationCurrent(buckets, activation.Generation(), ""); err != nil || !current {
		t.Fatalf("replacement generation current = %v, %v, want true", current, err)
	}
	if err := activation.Close("approved", "event-c", now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := Store(buckets, activation); err != nil {
		t.Fatal(err)
	}
	if current, err := GenerationCurrent(buckets, activation.Generation(), ""); err != nil || current {
		t.Fatalf("closed generation current = %v, %v, want false", current, err)
	}
}
