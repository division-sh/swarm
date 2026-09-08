package startuprecovery

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

func TestRecoverFailsClosedWhenAnyActiveRunLacksItsSourceArtifact(t *testing.T) {
	artifact := recoveryArtifact(t)
	reader := fakeAvailabilityReader{items: []runbundle.Availability{
		{
			RunID:      "11111111-1111-1111-1111-111111111111",
			Status:     "running",
			BundleHash: "bundle-v2:sha256:2222222222222222222222222222222222222222222222222222222222222222",
			ErrorCode:  runbundle.CodeBundleDataIntegrityError,
			Cause:      "missing_source_artifact",
		},
		{
			RunID:                 "22222222-2222-2222-2222-222222222222",
			Status:                "running",
			BundleHash:            artifact.BundleHash,
			SourceArtifactPresent: true,
		},
	}}

	result, err := Recover(context.Background(), Request{AvailabilityReader: reader, ArtifactReader: &fakeArtifactReader{artifact: artifact}})
	if err == nil || !IsDataIntegrityError(err) || !strings.Contains(err.Error(), runbundle.CodeBundleDataIntegrityError) {
		t.Fatalf("Recover err = %v, want data integrity error", err)
	}
	if len(result.CheckedAvailabilities) != 2 || len(result.DataIntegrityErrors) != 1 {
		t.Fatalf("result = %#v, want complete census and one data-integrity conflict", result)
	}
}

func TestRecoverAcceptsOnlyAvailableSourceArtifacts(t *testing.T) {
	artifact := recoveryArtifact(t)
	reader := fakeAvailabilityReader{items: []runbundle.Availability{
		{
			RunID:                 "11111111-1111-1111-1111-111111111111",
			Status:                "running",
			BundleHash:            artifact.BundleHash,
			SourceArtifactPresent: true,
		},
	}}

	result, err := Recover(context.Background(), Request{AvailabilityReader: reader, ArtifactReader: &fakeArtifactReader{artifact: artifact}})
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(result.CheckedAvailabilities) != 1 || len(result.DataIntegrityErrors) != 0 {
		t.Fatalf("result = %#v, want one verified source artifact", result)
	}
}

func TestRecoverValidatesStoredBytesAndNeverTreatsPresenceAsIntegrity(t *testing.T) {
	valid := recoveryArtifact(t)
	readErr := errors.New("selected store unavailable")
	for _, name := range []string{"valid", "missing after census", "corrupt blob", "hash mismatch", "read failure"} {
		t.Run(name, func(t *testing.T) {
			reader := &fakeArtifactReader{artifact: valid}
			switch name {
			case "missing after census":
				reader.err = sourceartifact.ErrNotFound
			case "corrupt blob":
				reader.artifact.SourceBlob = []byte{0}
			case "hash mismatch":
				reader.artifact.BundleHash = "bundle-v2:sha256:" + strings.Repeat("f", 64)
			case "read failure":
				reader.err = readErr
			}
			availability := runbundle.Availability{RunID: "first", Status: "running", BundleHash: valid.BundleHash, SourceArtifactPresent: true}
			peer := availability
			peer.RunID = "second"
			_, err := Recover(context.Background(), Request{
				AvailabilityReader: fakeAvailabilityReader{items: []runbundle.Availability{availability, peer}}, ArtifactReader: reader,
			})
			if (err == nil) != (name == "valid") {
				t.Fatalf("Recover = %v", err)
			}
			if reader.calls != 1 {
				t.Fatalf("artifact reads = %d, want one per exact hash", reader.calls)
			}
			if name == "read failure" && !errors.Is(err, readErr) {
				t.Fatalf("read error lost: %v", err)
			}
		})
	}
}

func recoveryArtifact(t *testing.T) sourceartifact.Persisted {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "schema.yaml"), []byte("name: startup-integrity\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	artifact, err := sourceartifact.AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	persisted, err := sourceartifact.PersistedFromArtifact(artifact, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return persisted
}

type fakeArtifactReader struct {
	artifact sourceartifact.Persisted
	err      error
	calls    int
}

func (r *fakeArtifactReader) GetSourceArtifact(context.Context, string) (sourceartifact.Persisted, error) {
	r.calls++
	return r.artifact, r.err
}

type fakeAvailabilityReader struct {
	items []runbundle.Availability
	err   error
}

func (r fakeAvailabilityReader) ActiveNonStandingRunBundleAvailabilities(context.Context) ([]runbundle.Availability, error) {
	if r.err != nil {
		return nil, r.err
	}
	return append([]runbundle.Availability(nil), r.items...), nil
}
