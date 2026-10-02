package runtimepersistence

import (
	"bytes"
	"context"
	"database/sql/driver"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

func Test2376SourceArtifactCommitUncertaintyBothStores(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "schema.yaml"), []byte("description: commit evidence\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	artifact, err := sourceartifact.AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, withData := range []bool{false, true} {
			for _, committed := range []bool{false, true} {
				path := backend + "/source"
				if withData {
					path += "_with_data"
				}
				if committed {
					path += "/commit_before_lost_ack"
				} else {
					path += "/rollback_before_lost_ack"
				}
				t.Run(path, func(t *testing.T) {
					owner, db, connector := newStopCommitStore(t, backend)
					selected := owner.(interface {
						selectedSourceArtifactStore
						EnsureSourceArtifactWithData(context.Context, *sourceartifact.AdmittedSourceArtifact, durabledata.Catalog) (sourceartifact.EnsureResult, error)
					})
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					ensure := func() (sourceartifact.EnsureResult, error) {
						if withData {
							return selected.EnsureSourceArtifactWithData(ctx, artifact, durabledata.Catalog{BundleHash: artifact.BundleHash()})
						}
						return selected.EnsureSourceArtifact(ctx, artifact)
					}
					cause := errors.New("injected source COMMIT acknowledgement unavailable")
					var calls atomic.Int32
					// Reuse the physical driver boundary: SQL and owner behavior remain real.
					connector.arm(func(tx driver.Tx) error {
						calls.Add(1)
						if committed {
							return errors.Join(cause, tx.Commit())
						}
						return errors.Join(cause, tx.Rollback())
					})
					if _, err := ensure(); !errors.Is(err, cause) || calls.Load() != 1 {
						t.Fatalf("lost uncertainty or automatically retried: err=%v calls=%d", err, calls.Load())
					}
					stored, readErr := selected.GetSourceArtifact(ctx, artifact.BundleHash())
					if committed {
						if readErr != nil || !bytes.Equal(stored.SourceBlob, artifact.LogicalBlob()) {
							t.Fatalf("committed exact source unavailable after lost ack: %v", readErr)
						}
					} else if !errors.Is(readErr, sourceartifact.ErrNotFound) {
						t.Fatalf("uncommitted source became available: %v", readErr)
					}
					// A separate explicit attempt reconciles the exact row, never a fallback source.
					retried, err := ensure()
					if err != nil || retried.Created == committed || !bytes.Equal(retried.Artifact.SourceBlob, artifact.LogicalBlob()) {
						t.Fatalf("exact retry = %+v, %v", retried, err)
					}
					if _, err := ensure(); err != nil {
						t.Fatal(err)
					}
					var count int
					if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM source_artifacts").Scan(&count); err != nil || count != 1 {
						t.Fatalf("source duplicated after explicit retry: count=%d err=%v", count, err)
					}
				})
			}
		}
	}
}
