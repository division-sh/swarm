package runbundle

import (
	"context"
	"errors"
	"strings"
	"testing"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

type admissionAvailabilityReader func(context.Context) ([]Availability, error)

func (r admissionAvailabilityReader) ActiveNonStandingRunBundleAvailabilities(ctx context.Context) ([]Availability, error) {
	return r(ctx)
}

func TestAdmitPinnedSourcesRequiresCompleteObservation(t *testing.T) {
	hash := "bundle-v2:sha256:" + strings.Repeat("a", 64)
	readErr := errors.New("private selected-store address")
	for _, test := range []struct {
		name   string
		reader ActiveAvailabilityReader
		pins   []string
		boot   string
		class  runtimefailures.Class
	}{
		{name: "not applicable"},
		{name: "missing reader", pins: []string{hash}, boot: hash, class: runtimefailures.ClassDependencyUnavailable},
		{name: "failed read", reader: admissionAvailabilityReader(func(context.Context) ([]Availability, error) { return nil, readErr }), pins: []string{hash}, boot: hash, class: runtimefailures.ClassDependencyUnavailable},
		{name: "missing identity", pins: []string{hash}, class: runtimefailures.ClassSchemaInvalid},
		{name: "invalid hash", pins: []string{"invalid"}, boot: hash, class: runtimefailures.ClassSchemaInvalid},
		{name: "empty hash is not omission", pins: []string{""}, boot: hash, class: runtimefailures.ClassSchemaInvalid},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := AdmitPinnedSources(context.Background(), test.reader, test.boot, test.pins)
			if test.class == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			failure, ok := runtimefailures.As(err)
			if !ok || failure.Failure.Class != test.class {
				t.Fatalf("error = %v, want %s", err, test.class)
			}
			if test.name == "failed read" && (!errors.Is(err, readErr) || strings.Contains(err.Error(), readErr.Error())) {
				t.Fatalf("read failure must retain its cause without leaking private detail: %v", err)
			}
		})
	}
}

func TestAdmitPinnedSourcesUsesExactActiveSourceOwner(t *testing.T) {
	hash := "bundle-v2:sha256:" + strings.Repeat("a", 64)
	other := "bundle-v2:sha256:" + strings.Repeat("b", 64)
	for _, mismatch := range []bool{false, true} {
		calls := 0
		reader := admissionAvailabilityReader(func(context.Context) ([]Availability, error) {
			calls++
			active := Availability{RunID: "ordinary-run", Status: "paused", BundleHash: hash, SourceArtifactPresent: true}
			if mismatch {
				active.BundleHash = other
			}
			return []Availability{active, {RunID: "missing-artifact", BundleHash: other}}, nil
		})
		err := AdmitPinnedSources(context.Background(), reader, hash, []string{hash, hash})
		if calls != 1 || (err != nil) != mismatch {
			t.Fatalf("mismatch %t: reads %d, error %v", mismatch, calls, err)
		}
		if mismatch {
			failure, ok := runtimefailures.As(err)
			if !ok || failure.Failure.Class != runtimefailures.ClassLifecycleConflict {
				t.Fatalf("conflict classification lost: %v", err)
			}
			for _, detail := range []string{"active non-standing run pinned bundle_hash conflict", hash, other, "ordinary-run"} {
				if !strings.Contains(err.Error(), detail) {
					t.Fatalf("conflict detail %q lost: %v", detail, err)
				}
			}
			if strings.Contains(err.Error(), "missing-artifact") {
				t.Fatalf("source integrity was reinterpreted as a pin mismatch: %v", err)
			}
		}
	}
}

func TestAdmitPinnedSourcesCancellationWins(t *testing.T) {
	hash := "bundle-v2:sha256:" + strings.Repeat("a", 64)
	ctx, cancel := context.WithCancel(context.Background())
	reader := admissionAvailabilityReader(func(context.Context) ([]Availability, error) {
		cancel()
		return nil, errors.New("late dependency failure")
	})
	if err := AdmitPinnedSources(ctx, reader, hash, []string{hash}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled observation = %v", err)
	}
	if err := AdmitPinnedSources(ctx, nil, "", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation was treated as non-applicability: %v", err)
	}
	if err := AdmitPinnedSources(nil, nil, "", nil); err == nil {
		t.Fatal("missing context admitted")
	}
}
