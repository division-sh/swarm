// Package durabledata owns immutable named business-data identity and wire-
// independent semantic records. Public surfaces and selected stores consume
// these types; paths, display names, and current bundle state are never inputs.
package durabledata

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
)

const (
	ManifestFormat = "swarm.resource.version.manifest.v1"
	RowCodec       = "swarm.resource.rows.v1"

	MaxRPCMessageBytes           = 1 << 20
	MaxDecodedImportBytes        = 720 << 10
	MaxDataDeclarationsPerBundle = 256
	MaxResourceRows              = 100_000
	MaxCanonicalRowBytes         = 128 << 10
	MaxBusinessKeyBytes          = 4 << 10
	MaxPublicPageItems           = 1_000
	DefaultPublicPageItems       = 100
	MaxPublicPageBytes           = 512 << 10
	DefaultPublicPageBytes       = 64 << 10
	MaxToolPageBytes             = 128 << 10
)

var digestPattern = regexp.MustCompile(`^[a-z][a-z0-9-]*-v1:sha256:[0-9a-f]{64}$`)

type DeclarationRef struct {
	PackageKey string `json:"package_key"`
	EventName  string `json:"event"`
}

func ParseDeclarationRef(packageKey, eventName string) (DeclarationRef, error) {
	pkg, err := runtimeidentity.ParsePackageKey(packageKey)
	if err != nil {
		return DeclarationRef{}, err
	}
	canonicalEvent := eventidentity.Normalize(eventName)
	if canonicalEvent != eventName || !eventidentity.IsValidName(canonicalEvent) {
		return DeclarationRef{}, fmt.Errorf("event name %q must be one canonical qualified event name", eventName)
	}
	return DeclarationRef{PackageKey: pkg.String(), EventName: canonicalEvent}, nil
}

func (r DeclarationRef) Validate() error {
	_, err := ParseDeclarationRef(r.PackageKey, r.EventName)
	return err
}

func (r DeclarationRef) Key() string {
	if r.Validate() != nil {
		return ""
	}
	return r.PackageKey + "\x00" + r.EventName
}

func CompareDeclarationRef(left, right DeclarationRef) int {
	if cmp := strings.Compare(left.PackageKey, right.PackageKey); cmp != 0 {
		return cmp
	}
	return strings.Compare(left.EventName, right.EventName)
}

type SchemaDigest string
type ContentDigest string
type VersionID string
type StaticDataID string
type BusinessKey string

func BusinessKeyFromValue(value any) (BusinessKey, error) {
	admitted, err := canonicaljson.FromGo(value)
	if err != nil {
		return "", err
	}
	switch admitted.Kind() {
	case semanticvalue.KindBool, semanticvalue.KindNumber, semanticvalue.KindString:
	default:
		return "", fmt.Errorf("business key must be a non-null scalar")
	}
	canonical, err := canonicaljson.Encode(admitted)
	if err != nil {
		return "", err
	}
	key := BusinessKey(canonical)
	if err := key.Validate(); err != nil {
		return "", err
	}
	return key, nil
}

func (k BusinessKey) Validate() error {
	if k == "" {
		return fmt.Errorf("business key is absent")
	}
	if len(k) > MaxBusinessKeyBytes {
		return fmt.Errorf("business key exceeds %d bytes", MaxBusinessKeyBytes)
	}
	value, err := canonicaljson.Decode([]byte(k))
	if err != nil {
		return fmt.Errorf("business key is not canonical JSON: %w", err)
	}
	switch value.Kind() {
	case semanticvalue.KindBool, semanticvalue.KindNumber, semanticvalue.KindString:
	default:
		return fmt.Errorf("business key must be a non-null scalar")
	}
	canonical, err := canonicaljson.Encode(value)
	if err != nil || string(canonical) != string(k) {
		return fmt.Errorf("business key must use canonical JSON encoding")
	}
	return nil
}

func (k BusinessKey) MarshalJSON() ([]byte, error) {
	if err := k.Validate(); err != nil {
		return nil, err
	}
	return []byte(k), nil
}

func (k *BusinessKey) UnmarshalJSON(raw []byte) error {
	if k == nil {
		return fmt.Errorf("business key destination is nil")
	}
	value, err := canonicaljson.Decode(raw)
	if err != nil {
		return err
	}
	canonical, err := canonicaljson.Encode(value)
	if err != nil {
		return err
	}
	parsed := BusinessKey(canonical)
	if err := parsed.Validate(); err != nil {
		return err
	}
	*k = parsed
	return nil
}

func (k BusinessKey) Value() (any, error) {
	if err := k.Validate(); err != nil {
		return nil, err
	}
	value, err := canonicaljson.Decode([]byte(k))
	if err != nil {
		return nil, err
	}
	return value.Interface(), nil
}

func validateDigest(raw, prefix string) error {
	raw = strings.TrimSpace(raw)
	if !digestPattern.MatchString(raw) || !strings.HasPrefix(raw, prefix) {
		return fmt.Errorf("%s must be %s<64 lowercase hex>", prefix, prefix)
	}
	return nil
}

func (d SchemaDigest) Validate() error {
	return validateDigest(string(d), "resource-schema-v1:sha256:")
}
func (d ContentDigest) Validate() error {
	return validateDigest(string(d), "resource-content-v1:sha256:")
}
func (id VersionID) Validate() error {
	return validateDigest(string(id), "resource-version-v1:sha256:")
}
func (id StaticDataID) Validate() error { return validateDigest(string(id), "static-data-v1:sha256:") }

type Manifest struct {
	ManifestFormat string         `json:"manifest_format"`
	Declaration    DeclarationRef `json:"declaration"`
	SchemaDigest   SchemaDigest   `json:"schema_digest"`
	RowCodec       string         `json:"row_codec"`
	ContentDigest  ContentDigest  `json:"content_digest"`
	RowCount       uint64         `json:"row_count"`
}

func (m Manifest) Validate() error {
	if m.ManifestFormat != ManifestFormat {
		return fmt.Errorf("manifest_format must be %q", ManifestFormat)
	}
	if err := m.Declaration.Validate(); err != nil {
		return err
	}
	if err := m.SchemaDigest.Validate(); err != nil {
		return err
	}
	if m.RowCodec != RowCodec {
		return fmt.Errorf("row_codec must be %q", RowCodec)
	}
	if err := m.ContentDigest.Validate(); err != nil {
		return err
	}
	if m.RowCount > MaxResourceRows {
		return fmt.Errorf("row_count exceeds %d", MaxResourceRows)
	}
	return nil
}

func (m Manifest) CanonicalBytes() ([]byte, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return canonicaljson.Bytes(m)
}

func (m Manifest) VersionID() (VersionID, error) {
	raw, err := m.CanonicalBytes()
	if err != nil {
		return "", err
	}
	return VersionID(prefixedDigest("resource-version-v1:sha256:", "swarm.resource.version.v1", raw)), nil
}

func SchemaDigestFor(canonicalSchema []byte) SchemaDigest {
	return SchemaDigest(prefixedDigest("resource-schema-v1:sha256:", "swarm.resource.schema.v1", canonicalSchema))
}

func prefixedDigest(prefix, domain string, payload []byte) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte(domain))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(payload)
	return prefix + hex.EncodeToString(hash.Sum(nil))
}

type ExpectedHead struct {
	State     string    `json:"state"`
	VersionID VersionID `json:"version_id,omitempty"`
}

func AbsentHead() ExpectedHead              { return ExpectedHead{State: "absent"} }
func VersionHead(id VersionID) ExpectedHead { return ExpectedHead{State: "version", VersionID: id} }

func (h ExpectedHead) Validate() error {
	switch h.State {
	case "absent":
		if h.VersionID != "" {
			return fmt.Errorf("absent head forbids version_id")
		}
	case "version":
		if err := h.VersionID.Validate(); err != nil {
			return err
		}
	default:
		return fmt.Errorf("expected head state must be absent or version")
	}
	return nil
}

func (h ExpectedHead) Equal(other ExpectedHead) bool {
	return h.State == other.State && h.VersionID == other.VersionID
}

type Row struct {
	Ordinal     uint64      `json:"ordinal"`
	BusinessKey BusinessKey `json:"-"`
	Canonical   []byte      `json:"-"`
}

type ValidationDefect struct {
	Row     uint64 `json:"row"`
	Path    string `json:"path"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

type DeltaSummary struct {
	Added        uint64 `json:"added"`
	Removed      uint64 `json:"removed"`
	Changed      uint64 `json:"changed"`
	OrderChanged bool   `json:"order_changed,omitempty"`
}

type DeltaRowIdentity string

const (
	DeltaRowIdentityPosition    DeltaRowIdentity = "position"
	DeltaRowIdentityBusinessKey DeltaRowIdentity = "business_key"
)

type DeltaResult struct {
	State       string           `json:"state"`
	RowIdentity DeltaRowIdentity `json:"row_identity,omitempty"`
	Against     *ExpectedHead    `json:"against,omitempty"`
	Summary     *DeltaSummary    `json:"summary,omitempty"`
	Reason      string           `json:"reason,omitempty"`
}

func ComputedDelta(against ExpectedHead, summary DeltaSummary, rowIdentity DeltaRowIdentity) DeltaResult {
	return DeltaResult{State: "computed", RowIdentity: rowIdentity, Against: &against, Summary: &summary}
}

func UncomputedDelta(reason string) DeltaResult {
	return DeltaResult{State: "not_computed", Reason: reason}
}

func NotComparableDelta(against ExpectedHead, reason string) DeltaResult {
	return DeltaResult{State: "not_comparable", Against: &against, Reason: reason}
}

func (d DeltaResult) Validate() error {
	switch d.State {
	case "computed":
		if d.Against == nil || d.Summary == nil || d.Reason != "" {
			return fmt.Errorf("computed delta requires against and summary only")
		}
		if d.RowIdentity != DeltaRowIdentityPosition && d.RowIdentity != DeltaRowIdentityBusinessKey {
			return fmt.Errorf("computed delta row_identity must be position or business_key")
		}
		return d.Against.Validate()
	case "not_computed":
		if d.RowIdentity != "" || d.Against != nil || d.Summary != nil || (d.Reason != "validation_rejected" && d.Reason != "head_conflict") {
			return fmt.Errorf("not_computed delta requires validation_rejected or head_conflict")
		}
		return nil
	case "not_comparable":
		if d.RowIdentity != "" || d.Against == nil || d.Summary != nil || d.Reason != "schema_changed" {
			return fmt.Errorf("not_comparable delta requires against and schema_changed")
		}
		return d.Against.Validate()
	default:
		return fmt.Errorf("delta state must be computed, not_computed, or not_comparable")
	}
}

type PageRequest struct {
	Limit     int    `json:"limit,omitempty"`
	ByteLimit int    `json:"byte_limit,omitempty"`
	Cursor    string `json:"cursor,omitempty"`
}

func (p PageRequest) WithDefaults() (PageRequest, error) {
	if p.Limit == 0 {
		p.Limit = DefaultPublicPageItems
	}
	if p.ByteLimit == 0 {
		p.ByteLimit = DefaultPublicPageBytes
	}
	if p.Limit < 1 || p.Limit > MaxPublicPageItems {
		return PageRequest{}, fmt.Errorf("page limit must be between 1 and %d", MaxPublicPageItems)
	}
	if p.ByteLimit < 1 || p.ByteLimit > MaxPublicPageBytes {
		return PageRequest{}, fmt.Errorf("page byte_limit must be between 1 and %d", MaxPublicPageBytes)
	}
	if len(p.Cursor) > MaxBusinessKeyBytes {
		return PageRequest{}, fmt.Errorf("page cursor exceeds %d bytes", MaxBusinessKeyBytes)
	}
	return p, nil
}

type PageContinuation struct {
	State  string `json:"state"`
	Cursor string `json:"cursor,omitempty"`
}

func EndContinuation() PageContinuation { return PageContinuation{State: "end"} }

type PageResult[T any] struct {
	Items             []T              `json:"items"`
	ItemCount         int              `json:"item_count"`
	EncodedItemsBytes int              `json:"encoded_items_bytes"`
	Continuation      PageContinuation `json:"continuation"`
}

func (p PageResult[T]) Validate() error {
	if p.Items == nil {
		return fmt.Errorf("page items must be an array")
	}
	if p.ItemCount != len(p.Items) {
		return fmt.Errorf("page item_count contradicts items")
	}
	if p.ItemCount > MaxPublicPageItems {
		return fmt.Errorf("page item_count exceeds %d", MaxPublicPageItems)
	}
	raw, err := json.Marshal(p.Items)
	if err != nil {
		return fmt.Errorf("encode page items: %w", err)
	}
	if p.EncodedItemsBytes != len(raw) {
		return fmt.Errorf("page encoded_items_bytes contradicts items")
	}
	if p.EncodedItemsBytes > MaxPublicPageBytes {
		return fmt.Errorf("page encoded_items_bytes exceeds %d", MaxPublicPageBytes)
	}
	switch p.Continuation.State {
	case "end":
		if p.Continuation.Cursor != "" {
			return fmt.Errorf("end page continuation forbids cursor")
		}
	case "more":
		if p.Continuation.Cursor == "" {
			return fmt.Errorf("more page continuation requires cursor")
		}
	default:
		return fmt.Errorf("page continuation state must be end or more")
	}
	return nil
}

type ProvenanceRef struct {
	Kind                  string `json:"kind"`
	SourceInvocationID    string `json:"source_invocation_id,omitempty"`
	RunID                 string `json:"run_id,omitempty"`
	PromotionInvocationID string `json:"promotion_invocation_id,omitempty"`
}

type Provenance struct {
	Sequence    uint64        `json:"-"`
	VersionID   VersionID     `json:"version_id"`
	ProducerRef ProvenanceRef `json:"producer_ref"`
	Actor       string        `json:"actor"`
	CommittedAt time.Time     `json:"committed_at"`
}

func (p ProvenanceRef) Validate() error {
	var id string
	switch p.Kind {
	case "import":
		if p.SourceInvocationID == "" || p.RunID != "" || p.PromotionInvocationID != "" {
			return fmt.Errorf("import provenance requires source_invocation_id only")
		}
		id = p.SourceInvocationID
	case "normal_run":
		if p.RunID == "" || p.SourceInvocationID != "" || p.PromotionInvocationID != "" {
			return fmt.Errorf("normal_run provenance requires run_id only")
		}
		id = p.RunID
	case "fork_candidate_promotion":
		if p.PromotionInvocationID == "" || p.SourceInvocationID != "" || p.RunID != "" {
			return fmt.Errorf("fork_candidate_promotion provenance requires promotion_invocation_id only")
		}
		id = p.PromotionInvocationID
	default:
		return fmt.Errorf("unknown resource provenance kind %q", p.Kind)
	}
	return validateCanonicalUUID(id, "resource provenance producer_id")
}

func NewProvenanceRef(kind, producerID string) (ProvenanceRef, error) {
	var ref ProvenanceRef
	switch kind {
	case "import":
		ref = ProvenanceRef{Kind: kind, SourceInvocationID: producerID}
	case "normal_run":
		ref = ProvenanceRef{Kind: kind, RunID: producerID}
	case "fork_candidate_promotion":
		ref = ProvenanceRef{Kind: kind, PromotionInvocationID: producerID}
	default:
		return ProvenanceRef{}, fmt.Errorf("unknown resource provenance kind %q", kind)
	}
	if err := ref.Validate(); err != nil {
		return ProvenanceRef{}, err
	}
	return ref, nil
}

func (p ProvenanceRef) ProducerID() (string, error) {
	if err := p.Validate(); err != nil {
		return "", err
	}
	switch p.Kind {
	case "import":
		return p.SourceInvocationID, nil
	case "normal_run":
		return p.RunID, nil
	default:
		return p.PromotionInvocationID, nil
	}
}

func (p Provenance) Validate() error {
	if p.Sequence == 0 {
		return fmt.Errorf("resource provenance sequence must be positive")
	}
	if err := p.VersionID.Validate(); err != nil {
		return err
	}
	if err := p.ProducerRef.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(p.Actor) == "" {
		return fmt.Errorf("resource provenance actor is required")
	}
	if err := validateCompletedAt(p.CommittedAt); err != nil {
		return fmt.Errorf("resource provenance committed_at is invalid: %w", err)
	}
	return nil
}

type Pin struct {
	RunID        string         `json:"run_id"`
	RunState     string         `json:"run_state"`
	Declaration  DeclarationRef `json:"declaration"`
	SchemaDigest SchemaDigest   `json:"schema_digest"`
	VersionID    VersionID      `json:"version_id"`
	Selection    string         `json:"selection"`
}

func SortPins(pins []Pin) {
	sort.Slice(pins, func(i, j int) bool {
		if cmp := CompareDeclarationRef(pins[i].Declaration, pins[j].Declaration); cmp != 0 {
			return cmp < 0
		}
		return pins[i].RunID < pins[j].RunID
	})
}
