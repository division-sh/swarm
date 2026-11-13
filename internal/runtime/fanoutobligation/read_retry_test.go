package fanoutobligation

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/failures"
)

func TestReadRetryRetainsPacingAcrossRestartWithoutInventingFailure(t *testing.T) {
	now := time.Now().UTC()
	failure := failures.Normalize(failures.New(failures.ClassDependencyUnavailable, "read_unavailable", "test", "read", nil), "test", "read")
	var episode *ReadRetry
	for attempt := 0; attempt < 20; attempt++ {
		next := episode.NextAttempt(now)
		if next.NotBefore.Before(now.Add(InitialReadRetryDelay)) || next.NotBefore.After(now.Add(MaxReadRetryDelay)) {
			t.Fatalf("attempt %d pace %+v", attempt, next)
		}
		if attempt == 0 && (next.Since != nil || next.Failure != nil) {
			t.Fatal("admitted attempt fabricated observed failure")
		}
		if err := next.ObserveFailure(now, failure); err != nil {
			t.Fatal(err)
		}
		if episode != nil && !next.Since.Equal(*episode.Since) {
			t.Fatal("retry reset observed failure age")
		}
		raw, err := json.Marshal(next)
		if err != nil {
			t.Fatal(err)
		}
		episode = new(ReadRetry)
		if err := json.Unmarshal(raw, episode); err != nil {
			t.Fatal(err)
		}
		if err := episode.Validate(); err != nil {
			t.Fatal(err)
		}
		if !episode.NotBefore.Equal(next.NotBefore) || episode.Delay != next.Delay {
			t.Fatal("restart reset pacing")
		}
		now = episode.NotBefore.Add(time.Second)
	}
	if episode.Delay != MaxReadRetryDelay {
		t.Fatal("unlimited retry failed to saturate delay")
	}
	for _, invalid := range []ReadRetry{
		{}, {NotBefore: now, Delay: 0}, {NotBefore: now, Delay: MaxReadRetryDelay + 1},
		{NotBefore: now, Delay: time.Second, Failure: &failure},
		{NotBefore: now, Delay: time.Second, Since: &now},
	} {
		if invalid.Validate() == nil {
			t.Fatalf("accepted malformed pacing %+v", invalid)
		}
	}
}
