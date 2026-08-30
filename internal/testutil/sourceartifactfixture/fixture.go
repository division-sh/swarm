package sourceartifactfixture

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

const BundleHash = "bundle-v2:sha256:475110a469b91fc27e36bf58b6a0bfda6dc0eb100b10692299019b487c866cc4"

type Writer interface {
	EnsureSourceArtifactWithData(context.Context, *sourceartifact.AdmittedSourceArtifact, durabledata.Catalog) (sourceartifact.EnsureResult, error)
}

type Reader interface {
	GetSourceArtifact(context.Context, string) (sourceartifact.Persisted, error)
}

func Artifact() *sourceartifact.AdmittedSourceArtifact {
	artifact := New("agents.yaml", []byte("agents: {}\n"))
	if artifact.BundleHash() != BundleHash {
		panic("source artifact fixture hash drift: " + artifact.BundleHash())
	}
	return artifact
}

func New(label string, body []byte) *sourceartifact.AdmittedSourceArtifact {
	var logical bytes.Buffer
	logical.WriteString("swarm-bundle-v2\x00")
	writeU64(&logical, 1)
	logical.WriteByte(byte(sourceartifact.DispositionDeclaration))
	writeU64(&logical, uint64(len(label)))
	logical.WriteString(label)
	writeU64(&logical, uint64(len(body)))
	logical.Write(body)
	artifact, err := sourceartifact.DecodeLogical(logical.Bytes())
	if err != nil {
		panic(err)
	}
	return artifact
}

func Fact() runtimecorrelation.SourceArtifactFact {
	return FactFor(Artifact())
}

func FactFor(artifact *sourceartifact.AdmittedSourceArtifact) runtimecorrelation.SourceArtifactFact {
	if artifact == nil {
		panic("source artifact fixture is required")
	}
	fact, err := runtimecorrelation.NewSourceArtifactFact(artifact.BundleHash())
	if err != nil {
		panic(err)
	}
	return fact
}

func Require(t testing.TB, ctx context.Context, writer Writer) runtimecorrelation.SourceArtifactFact {
	t.Helper()
	if err := EnsureArtifact(ctx, writer, Artifact()); err != nil {
		t.Fatalf("persist selected-store source artifact fixture: %v", err)
	}
	return Fact()
}

func Ensure(ctx context.Context, writer Writer) error {
	return EnsureArtifact(ctx, writer, Artifact())
}

func RequireArtifact(t testing.TB, ctx context.Context, writer Writer, artifact *sourceartifact.AdmittedSourceArtifact) runtimecorrelation.SourceArtifactFact {
	t.Helper()
	if err := EnsureArtifact(ctx, writer, artifact); err != nil {
		t.Fatalf("persist selected-store source artifact fixture: %v", err)
	}
	return FactFor(artifact)
}

func EnsureArtifact(ctx context.Context, writer Writer, artifact *sourceartifact.AdmittedSourceArtifact) error {
	if writer == nil {
		return errors.New("selected-store source artifact writer is required")
	}
	if artifact == nil {
		return errors.New("admitted source artifact fixture is required")
	}
	if reader, ok := writer.(Reader); ok {
		persisted, err := reader.GetSourceArtifact(ctx, artifact.BundleHash())
		switch {
		case err == nil:
			requested, err := sourceartifact.PersistedFromArtifact(artifact, time.Unix(1, 0).UTC())
			if err != nil {
				return err
			}
			if !bytes.Equal(persisted.SourceBlob, requested.SourceBlob) ||
				persisted.MemberCount != requested.MemberCount ||
				persisted.TotalBytes != requested.TotalBytes {
				return &sourceartifact.ConflictError{BundleHash: artifact.BundleHash()}
			}
			return nil
		case errors.Is(err, sourceartifact.ErrNotFound):
		default:
			return err
		}
	}
	_, err := writer.EnsureSourceArtifactWithData(ctx, artifact, durabledata.Catalog{BundleHash: artifact.BundleHash()})
	return err
}

func writeU64(target *bytes.Buffer, value uint64) {
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], value)
	target.Write(raw[:])
}
