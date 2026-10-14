package serveapp

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/division-sh/swarm/internal/sourceartifact"
)

func readServedPersistedSourceBlob(ctx context.Context, reader sourceArtifactReader, hash string) ([]byte, error) {
	if reader == nil {
		return nil, fmt.Errorf("persisted source bytes require the current selected artifact reader")
	}
	record, err := reader.GetSourceArtifact(ctx, hash)
	if err != nil {
		return nil, err
	}
	return record.SourceBlob, nil
}

// This is only a failed-return probe, not a native store or transaction protocol.
type sourceBlobFailedReturnProbe struct {
	ctx   context.Context
	hash  string
	cause error
}

func (p sourceBlobFailedReturnProbe) GetSourceArtifact(ctx context.Context, hash string) (sourceartifact.Persisted, error) {
	if ctx != p.ctx || hash != p.hash {
		return sourceartifact.Persisted{}, fmt.Errorf("source read changed context or hash")
	}
	return sourceartifact.Persisted{SourceBlob: []byte("provisional")}, p.cause
}

func TestServedPersistedSourceBlobRefusesMissingOwnerAndFailedPartialRecord(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if blob, err := readServedPersistedSourceBlob(ctx, nil, "original-hash"); err == nil || blob != nil {
		t.Fatal("missing artifact owner supplied source bytes")
	}
	cause := errors.New("source record read failed")
	if blob, err := readServedPersistedSourceBlob(ctx, sourceBlobFailedReturnProbe{ctx, "original-hash", cause}, "original-hash"); !errors.Is(err, cause) || blob != nil {
		t.Fatalf("failed artifact read leaked bytes or changed context/hash/error: %q %v", blob, err)
	}
}
