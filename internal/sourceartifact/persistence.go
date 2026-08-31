package sourceartifact

import (
	"errors"
	"fmt"
	"time"
)

var ErrNotFound = errors.New("source artifact not found")

type Persisted struct {
	BundleHash  string
	SourceBlob  []byte
	MemberCount int
	TotalBytes  int64
	CreatedAt   time.Time
}

type EnsureResult struct {
	Artifact Persisted
	Created  bool
}

type ConflictError struct {
	BundleHash string
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("source artifact %s already exists with different canonical bytes", e.BundleHash)
}

func PersistedFromArtifact(artifact *AdmittedSourceArtifact, createdAt time.Time) (Persisted, error) {
	if artifact == nil {
		return Persisted{}, fmt.Errorf("admitted source artifact is required")
	}
	return validatePersisted(Persisted{
		BundleHash:  artifact.BundleHash(),
		SourceBlob:  artifact.LogicalBlob(),
		MemberCount: len(artifact.Entries()),
		TotalBytes:  artifactTotalBytes(artifact),
		CreatedAt:   createdAt.UTC(),
	})
}

func (p Persisted) Validate() error {
	_, err := validatePersisted(p)
	return err
}

func (p Persisted) Decode() (*AdmittedSourceArtifact, error) {
	normalized, err := validatePersisted(p)
	if err != nil {
		return nil, err
	}
	return DecodeLogical(normalized.SourceBlob)
}

func validatePersisted(p Persisted) (Persisted, error) {
	if err := ValidateHash(p.BundleHash); err != nil {
		return Persisted{}, err
	}
	artifact, err := DecodeLogical(p.SourceBlob)
	if err != nil {
		return Persisted{}, fmt.Errorf("decode source artifact %s: %w", p.BundleHash, err)
	}
	if artifact.BundleHash() != p.BundleHash {
		return Persisted{}, fmt.Errorf("source artifact hash mismatch: row %s, blob %s", p.BundleHash, artifact.BundleHash())
	}
	if got := len(artifact.entries); p.MemberCount != got {
		return Persisted{}, fmt.Errorf("source artifact %s member_count %d does not match blob %d", p.BundleHash, p.MemberCount, got)
	}
	if got := artifactTotalBytes(artifact); p.TotalBytes != got {
		return Persisted{}, fmt.Errorf("source artifact %s total_bytes %d does not match blob %d", p.BundleHash, p.TotalBytes, got)
	}
	if p.CreatedAt.IsZero() {
		return Persisted{}, fmt.Errorf("source artifact %s created_at is required", p.BundleHash)
	}
	p.SourceBlob = append([]byte(nil), p.SourceBlob...)
	p.CreatedAt = p.CreatedAt.UTC()
	return p, nil
}

func artifactTotalBytes(artifact *AdmittedSourceArtifact) int64 {
	var total int64
	if artifact != nil {
		for _, entry := range artifact.entries {
			total += int64(len(entry.body))
		}
	}
	return total
}
