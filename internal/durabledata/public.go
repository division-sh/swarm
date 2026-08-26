package durabledata

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// DeclarationSummary is the bounded public projection of one declaration and
// its retained version inventory.
type DeclarationSummary struct {
	Declaration              DeclarationRef `json:"declaration"`
	LocalName                string         `json:"local_name"`
	SchemaDigest             SchemaDigest   `json:"schema_digest"`
	Head                     ExpectedHead   `json:"head"`
	VersionCount             int            `json:"version_count"`
	MaterializedVersionCount int            `json:"materialized_version_count"`
	MaterializedBytes        int            `json:"materialized_bytes"`
}

func (s DeclarationSummary) Validate() error {
	if err := s.Declaration.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(s.LocalName) == "" {
		return fmt.Errorf("declaration summary requires local_name")
	}
	if err := s.SchemaDigest.Validate(); err != nil {
		return err
	}
	if err := s.Head.Validate(); err != nil {
		return err
	}
	if s.VersionCount < 0 || s.MaterializedVersionCount < 0 || s.MaterializedBytes < 0 || s.MaterializedVersionCount > s.VersionCount {
		return fmt.Errorf("declaration summary counts are contradictory")
	}
	if (s.VersionCount == 0) != (s.Head.State == "absent") {
		return fmt.Errorf("declaration summary head contradicts version inventory")
	}
	return nil
}

type VersionSummary struct {
	Declaration       DeclarationRef `json:"declaration"`
	VersionID         VersionID      `json:"version_id"`
	Alias             string         `json:"alias"`
	Manifest          Manifest       `json:"manifest"`
	BusinessKey       string         `json:"business_key,omitempty"`
	PayloadState      string         `json:"payload_state"`
	MaterializedBytes int            `json:"materialized_bytes"`
}

func (s VersionSummary) Validate() error {
	if err := s.Declaration.Validate(); err != nil {
		return err
	}
	if err := s.VersionID.Validate(); err != nil {
		return err
	}
	sequence, err := ParseVersionAlias(s.Alias)
	if err != nil {
		return err
	}
	derived, err := s.Manifest.VersionID()
	if err != nil || derived != s.VersionID || s.Manifest.Declaration != s.Declaration {
		return fmt.Errorf("version summary manifest is contradictory")
	}
	if sequence == 0 {
		return fmt.Errorf("version summary alias must be positive")
	}
	if s.MaterializedBytes < 0 {
		return fmt.Errorf("version summary materialized_bytes must be non-negative")
	}
	switch s.PayloadState {
	case "materialized":
		return nil
	case "pruned":
		if s.MaterializedBytes != 0 {
			return fmt.Errorf("pruned version summary must have zero materialized bytes")
		}
		return nil
	default:
		return fmt.Errorf("version summary payload_state must be materialized or pruned")
	}
}

// Summary returns the canonical public metadata projection of one stored
// version. Provenance is intentionally outside this projection.
func (v Version) Summary() VersionSummary {
	state := "materialized"
	materializedBytes := len(v.CanonicalJSONL)
	if v.PrunedAt != nil {
		state = "pruned"
		materializedBytes = 0
	}
	return VersionSummary{
		Declaration:       v.Manifest.Declaration,
		VersionID:         v.VersionID,
		Alias:             fmt.Sprintf("v%d", v.SequenceAlias),
		Manifest:          v.Manifest,
		BusinessKey:       v.BusinessKey,
		PayloadState:      state,
		MaterializedBytes: materializedBytes,
	}
}

func (s VersionSummary) ValidateVersion(version Version) error {
	if err := s.Validate(); err != nil {
		return err
	}
	if version.PrunedAt == nil && version.CanonicalJSONL == nil {
		return fmt.Errorf("materialized version requires payload bytes")
	}
	if version.PrunedAt != nil && version.CanonicalJSONL != nil {
		return fmt.Errorf("pruned version forbids payload bytes")
	}
	actual := version.Summary()
	if err := actual.Validate(); err != nil {
		return err
	}
	if s != actual {
		return fmt.Errorf("version summary contradicts payload")
	}
	return nil
}

// VersionSelector is the closed selected-store version lookup contract. Alias
// text is parsed at the API boundary so the store never accepts alternate
// spellings for one sequence.
type VersionSelector struct {
	Kind          string
	VersionID     VersionID
	SequenceAlias uint64
}

func (s VersionSelector) Validate() error {
	switch s.Kind {
	case "head":
		if s.VersionID != "" || s.SequenceAlias != 0 {
			return fmt.Errorf("head selector forbids version and alias facts")
		}
	case "version":
		if err := s.VersionID.Validate(); err != nil {
			return err
		}
		if s.SequenceAlias != 0 {
			return fmt.Errorf("version selector forbids alias fact")
		}
	case "alias":
		if s.VersionID != "" || s.SequenceAlias == 0 {
			return fmt.Errorf("alias selector requires only a positive sequence")
		}
	default:
		return fmt.Errorf("version selector kind must be head, version, or alias")
	}
	return nil
}

type RowDTO struct {
	Declaration DeclarationRef `json:"declaration"`
	VersionID   VersionID      `json:"version_id"`
	Ordinal     uint64         `json:"ordinal"`
	Key         *BusinessKey   `json:"key,omitempty"`
	Value       any            `json:"value"`
}

type SourceOperationRef struct {
	Kind               string `json:"kind"`
	SourceInvocationID string `json:"source_invocation_id"`
}

type PruneOperationRef struct {
	Kind              string `json:"kind"`
	PruneInvocationID string `json:"prune_invocation_id"`
}

type RunCreationOperationRef struct {
	Kind  string `json:"kind"`
	RunID string `json:"run_id"`
}

type HeadHistoryDTO struct {
	Revision     uint64             `json:"revision"`
	Before       ExpectedHead       `json:"before"`
	After        ExpectedHead       `json:"after"`
	OperationRef SourceOperationRef `json:"operation_ref"`
	CommittedAt  time.Time          `json:"committed_at"`
}

type ExportChunk struct {
	Declaration   DeclarationRef   `json:"declaration"`
	VersionID     VersionID        `json:"version_id"`
	ContentDigest ContentDigest    `json:"content_digest"`
	TotalRows     int              `json:"total_rows"`
	FirstOrdinal  uint64           `json:"first_ordinal"`
	RowCount      int              `json:"row_count"`
	ChunkBase64   string           `json:"chunk_base64"`
	ChunkBytes    int              `json:"chunk_bytes"`
	ChunkSHA256   string           `json:"chunk_sha256"`
	Continuation  PageContinuation `json:"continuation"`
}

// OperationSummary is a closed tagged union. Exactly one arm is populated.
type OperationSummary struct {
	Kind        string                       `json:"kind"`
	Source      *SourceOperationSummary      `json:"source,omitempty"`
	Prune       *PruneOperationResult        `json:"prune,omitempty"`
	RunCreation *RunCreationOperationSummary `json:"run_creation,omitempty"`
}

// RawPageResult retains the PageResult wire contract for a detail whose item
// type is selected by the operation/detail compatibility matrix.
type RawPageResult struct {
	Items             []json.RawMessage `json:"items"`
	ItemCount         int               `json:"item_count"`
	EncodedItemsBytes int               `json:"encoded_items_bytes"`
	Continuation      PageContinuation  `json:"continuation"`
}
