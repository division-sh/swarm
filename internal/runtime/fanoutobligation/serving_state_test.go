package fanoutobligation

import (
	"errors"
	"testing"
	"time"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

func TestServingProjectionAndClaimAdmissionBoundaries(t *testing.T) {
	now := time.Now().UTC()
	request := validIntentRequest(t)
	base := Intent{Request: request, Source: request.Source, Status: StatusOpen, NextChunkSize: MaxChunkSize, CreatedAt: now, UpdatedAt: now}
	retry := runtimefailures.Normalize(runtimefailures.New(runtimefailures.ClassDependencyUnavailable, "dependency_down", "runtime.fan_out", "test", nil), "runtime.fan_out", "test")
	blocked := runtimefailures.Normalize(errors.New("invariant"), "runtime.fan_out", "test")
	blockedRaw, err := runtimefailures.MarshalEnvelope(blocked)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		change func(*Intent)
		want   ServingState
	}{
		{"unclaimed", func(*Intent) {}, ServingEligible},
		{"leased", func(i *Intent) {
			i.ClaimOwner, i.ClaimGeneration, i.LeaseExpiresAt = "worker", 1, now.Add(time.Nanosecond)
		}, ServingLeased},
		{"expiry equality", func(i *Intent) { i.ClaimOwner, i.ClaimGeneration, i.LeaseExpiresAt = "worker", 1, now }, ServingEligible},
		{"expired", func(i *Intent) {
			i.ClaimOwner, i.ClaimGeneration, i.LeaseExpiresAt = "worker", 1, now.Add(-time.Nanosecond)
		}, ServingEligible},
		{"retry wait", func(i *Intent) { i.Retry = &RetryWait{ReadyAt: now.Add(time.Nanosecond), Failure: retry} }, ServingRetryWait},
		{"retry equality", func(i *Intent) { i.Retry = &RetryWait{ReadyAt: now, Failure: retry} }, ServingEligible},
		{"retry due", func(i *Intent) { i.Retry = &RetryWait{ReadyAt: now.Add(-time.Nanosecond), Failure: retry} }, ServingEligible},
		{"blocked", func(i *Intent) { i.Status, i.BlockedReason = StatusBlocked, string(blockedRaw) }, ServingBlocked},
		{"closed", func(i *Intent) { i.Status, i.Cursor = StatusClosed, request.Cardinality }, ServingClosed},
		{"canceled", func(i *Intent) { i.Status, i.BlockedReason = StatusCanceled, "run_stopped" }, ServingCanceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			intent := base
			tc.change(&intent)
			state, err := intent.ServingAt(now)
			if err != nil || state != tc.want {
				t.Fatalf("state=%v want=%v err=%v", state, tc.want, err)
			}
		})
	}
	for _, tc := range []struct {
		name   string
		change func(*Intent)
	}{
		{"missing retry cause", func(i *Intent) { i.Retry = &RetryWait{ReadyAt: now} }},
		{"missing retry due", func(i *Intent) { i.Retry = &RetryWait{Failure: retry} }},
		{"permanent retry cause", func(i *Intent) { i.Retry = &RetryWait{ReadyAt: now, Failure: blocked} }},
		{"retry with claim", func(i *Intent) {
			i.Retry = &RetryWait{ReadyAt: now, Failure: retry}
			i.ClaimOwner, i.ClaimGeneration, i.LeaseExpiresAt = "worker", 1, now.Add(time.Minute)
		}},
		{"retry after cancel", func(i *Intent) {
			i.Retry = &RetryWait{ReadyAt: now, Failure: retry}
			i.Status, i.BlockedReason = StatusCanceled, "run_stopped"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			intent := base
			tc.change(&intent)
			if _, err := intent.ServingAt(now); err == nil {
				t.Fatal("contradictory serving facts accepted")
			}
		})
	}
	base.ClaimOwner, base.ClaimGeneration, base.LeaseExpiresAt = "worker", 2, now.Add(time.Second)
	claim := Claim{Key: request.Key, Owner: "worker", Generation: 2, LeaseUntil: now.Add(time.Hour)}
	if err := base.AdmitClaim(claim, now); err != nil {
		t.Fatal(err)
	}
	if err := base.AdmitClaim(claim, base.LeaseExpiresAt); !errors.Is(err, ErrStaleClaim) {
		t.Fatalf("caller lease overrode persisted expiry: %v", err)
	}
	claim.Generation--
	if err := base.AdmitClaim(claim, now); !errors.Is(err, ErrStaleClaim) {
		t.Fatalf("wrong generation admitted: %v", err)
	}
}
