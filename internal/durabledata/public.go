package durabledata

import (
	"encoding/json"
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

type VersionSummary struct {
	Declaration       DeclarationRef `json:"declaration"`
	VersionID         VersionID      `json:"version_id"`
	Alias             string         `json:"alias"`
	Manifest          Manifest       `json:"manifest"`
	BusinessKey       string         `json:"business_key,omitempty"`
	PayloadState      string         `json:"payload_state"`
	MaterializedBytes int            `json:"materialized_bytes"`
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
