package fanoutobligation

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

func TestFanOutReadbackStateAndUnavailableMetrics(t *testing.T) {
	now := time.Now().UTC()
	request := validIntentRequest(t)
	base := Intent{Request: request, Source: request.Source, Status: StatusOpen, NextChunkSize: MaxChunkSize, CreatedAt: now, UpdatedAt: now}
	retry := runtimefailures.Normalize(runtimefailures.New(runtimefailures.ClassDependencyUnavailable, "dependency_down", "runtime.fan_out", "read-test", nil), "runtime.fan_out", "read-test")
	blocked := runtimefailures.Normalize(errors.New("invariant"), "runtime.fan_out", "read-test")
	blockedRaw, err := runtimefailures.MarshalEnvelope(blocked)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, state string
		change      func(*Intent)
	}{
		{"ready", "eligible", func(*Intent) {}},
		{"leased", "leased", func(i *Intent) {
			i.ClaimOwner = "worker"
			i.ClaimGeneration = 1
			i.LeaseExpiresAt = now.Add(time.Second)
		}},
		{"expired", "eligible", func(i *Intent) { i.ClaimOwner = "worker"; i.ClaimGeneration = 1; i.LeaseExpiresAt = now }},
		{"retry", "retry_wait", func(i *Intent) { i.Retry = &RetryWait{ReadyAt: now.Add(time.Second), Failure: retry} }},
		{"due", "eligible", func(i *Intent) { i.Retry = &RetryWait{ReadyAt: now, Failure: retry} }},
		{"blocked", "blocked", func(i *Intent) { i.Status = StatusBlocked; i.BlockedReason = string(blockedRaw) }},
		{"closed", "closed", func(i *Intent) { i.Status = StatusClosed; i.Cursor = i.Request.Cardinality }},
		{"canceled", "canceled", func(i *Intent) { i.Status = StatusCanceled; i.BlockedReason = "run_stopped" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			intent := base
			tc.change(&intent)
			row, err := intent.ReadbackAt(now)
			if err != nil || row.DurableState != tc.state {
				t.Fatalf("row=%+v err=%v", row, err)
			}
			if row.Key != request.Key || row.NextChunkSize != MaxChunkSize {
				t.Fatalf("lost durable identity/budget: %+v", row)
			}
			if row.Runtime != UnavailableRuntimeReadback() {
				t.Fatalf("fabricated runtime evidence: %+v", row.Runtime)
			}
			if (intent.Status == StatusClosed || intent.Status == StatusCanceled) && row.Owed != 0 {
				t.Fatalf("terminal owed=%d", row.Owed)
			}
			if intent.Status == StatusBlocked && (row.Failure == nil || row.Failure.Detail.Code != blocked.Detail.Code) {
				t.Fatal("lost typed blockage")
			}
			raw, err := json.Marshal(row)
			if err != nil {
				t.Fatal(err)
			}
			for _, field := range []string{`"observed_at":null`, `"last_commit_ms":null`, `"workers":null`, `"active_workers":null`, `"eligible":null`} {
				if !strings.Contains(string(raw), field) {
					t.Fatalf("missing unavailable field %s: %s", field, raw)
				}
			}
		})
	}
	base.Cursor = request.Cardinality + 1
	if _, err := base.ReadbackAt(now); err == nil {
		t.Fatal("corrupt intent became a diagnostic row")
	}
}

func TestFanOutReadRuntimeObservation(t *testing.T) {
	at := time.Now().UTC()
	zero := time.Time{}
	eligible, workers, active, latency := true, 8, 3, 2.5
	live := RuntimeReadback{ObservedAt: &at, Availability: "available", Reason: "eligible", Eligible: &eligible, Workers: &workers, ActiveWorkers: &active, LastCommitMS: &latency}
	for _, tc := range []struct {
		name   string
		change func(*RuntimeReadback)
		valid  bool
	}{
		{"live", func(*RuntimeReadback) {}, true},
		{"nonlast_or_restart", func(r *RuntimeReadback) { r.LastCommitMS = nil }, true},
		{"missing_clock", func(r *RuntimeReadback) { r.ObservedAt = nil }, false},
		{"missing_reason", func(r *RuntimeReadback) { r.Reason = "" }, false},
		{"zero_clock", func(r *RuntimeReadback) { r.ObservedAt = &zero }, false},
		{"unavailable_with_clock", func(r *RuntimeReadback) { *r = UnavailableRuntimeReadback(); r.ObservedAt = &at }, false},
		{"retired", func(r *RuntimeReadback) { *r = RuntimeReadback{Availability: "retired", Reason: "grant_retired"} }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			readback := live
			tc.change(&readback)
			if err := readback.Validate(); (err == nil) != tc.valid {
				t.Fatalf("valid=%v readback=%+v err=%v", tc.valid, readback, err)
			}
		})
	}
}

func TestFanOutReadQueryLimitsAndIdentity(t *testing.T) {
	key := validIntentRequest(t).Key
	for _, limit := range []int{0, 1, DefaultListLimit, MaxListLimit} {
		if err := (ListQuery{RunID: key.RunID, Limit: limit}).Validate(); err != nil {
			t.Fatal(err)
		}
	}
	for _, q := range []ListQuery{
		{RunID: key.RunID, Limit: -1}, {RunID: key.RunID, Limit: MaxListLimit + 1}, {RunID: "bad"},
		{RunID: key.RunID, Filter: ListFilter{Status: "leased"}},
		{RunID: key.RunID, Filter: ListFilter{TriggeringDeliveryID: "bad"}},
		{RunID: key.RunID, Filter: ListFilter{FlowPath: " root "}},
		{RunID: key.RunID, Cursor: strings.Repeat("x", 16385)},
	} {
		if q.Validate() == nil {
			t.Fatalf("accepted query %+v", q)
		}
	}
}
