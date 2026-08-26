package durabledata

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/google/uuid"
)

type DataInput struct {
	Format        string `json:"format"`
	ContentBase64 string `json:"content_base64"`
}

type SourceCommand struct {
	Operation          string         `json:"operation"`
	SourceInvocationID string         `json:"source_invocation_id"`
	ParentRunID        string         `json:"parent_run_id,omitempty"`
	Actor              string         `json:"actor"`
	BundleHash         string         `json:"bundle_hash"`
	Declaration        DeclarationRef `json:"declaration"`
	ExpectedHead       ExpectedHead   `json:"expected_head"`
	InputFormat        string         `json:"input_format"`
	Input              []byte         `json:"input"`
}

func (c SourceCommand) Validate() error {
	if c.Operation != "check" && c.Operation != "import" {
		return fmt.Errorf("source operation must be check or import")
	}
	if parsed, err := uuid.Parse(c.SourceInvocationID); err != nil || parsed == uuid.Nil || parsed.String() != c.SourceInvocationID {
		return fmt.Errorf("source_invocation_id must be one canonical non-zero UUID")
	}
	if c.ParentRunID != "" {
		parsed, err := uuid.Parse(c.ParentRunID)
		if err != nil || parsed == uuid.Nil || parsed.String() != c.ParentRunID || c.Operation != "import" {
			return fmt.Errorf("parent_run_id must be one canonical non-zero UUID on fused import")
		}
	}
	if strings.TrimSpace(c.Actor) == "" || strings.TrimSpace(c.BundleHash) == "" {
		return fmt.Errorf("source operation requires actor and bundle_hash")
	}
	if c.InputFormat != "jsonl" {
		return fmt.Errorf("source input format must be jsonl")
	}
	if err := c.Declaration.Validate(); err != nil {
		return err
	}
	return c.ExpectedHead.Validate()
}

func (c SourceCommand) RequestHash() (string, []byte, error) {
	if err := c.Validate(); err != nil {
		return "", nil, err
	}
	raw, err := canonicaljson.Bytes(c)
	if err != nil {
		return "", nil, err
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte("swarm.resource.source.request.v1"))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(raw)
	return "resource-source-request-v1:sha256:" + hex.EncodeToString(hash.Sum(nil)), raw, nil
}

type Declaration struct {
	Name            string         `json:"name"`
	Ref             DeclarationRef `json:"ref"`
	OwnerFlowID     string         `json:"owner_flow_id"`
	BusinessKey     string         `json:"business_key,omitempty"`
	SchemaDigest    SchemaDigest   `json:"schema_digest"`
	CanonicalSchema []byte         `json:"canonical_schema"`
}

type StaticData struct {
	StaticID      StaticDataID  `json:"static_id"`
	Ref           StaticDataRef `json:"ref"`
	PackageKey    string        `json:"package_key"`
	OwnerFlowID   string        `json:"owner_flow_id"`
	RelativePath  string        `json:"relative_path"`
	ContentDigest string        `json:"content_digest"`
	ContentType   string        `json:"content_type"`
	Content       []byte        `json:"content"`
}

type Catalog struct {
	BundleHash   string        `json:"bundle_hash"`
	Declarations []Declaration `json:"declarations"`
	StaticData   []StaticData  `json:"static_data"`
}

type CandidateVersion struct {
	State     string    `json:"state"`
	VersionID VersionID `json:"version_id,omitempty"`
	Alias     string    `json:"alias,omitempty"`
	Manifest  *Manifest `json:"manifest,omitempty"`
}

func (c CandidateVersion) Validate() error {
	switch c.State {
	case "none":
		if c.VersionID != "" || c.Alias != "" || c.Manifest != nil {
			return fmt.Errorf("none candidate must not carry version facts")
		}
		return nil
	case "candidate", "version":
		if err := c.VersionID.Validate(); err != nil {
			return err
		}
		if c.Manifest == nil {
			return fmt.Errorf("%s candidate requires manifest", c.State)
		}
		if err := c.Manifest.Validate(); err != nil {
			return err
		}
		versionID, err := c.Manifest.VersionID()
		if err != nil || versionID != c.VersionID {
			return fmt.Errorf("candidate version_id contradicts manifest")
		}
	default:
		return fmt.Errorf("candidate state must be none, candidate, or version")
	}
	if c.State == "candidate" {
		if c.Alias != "" {
			return fmt.Errorf("uncommitted candidate must not carry alias")
		}
		return nil
	}
	_, err := ParseVersionAlias(c.Alias)
	return err
}

func ParseVersionAlias(alias string) (uint64, error) {
	if !strings.HasPrefix(alias, "v") {
		return 0, fmt.Errorf("version alias must be canonical vN")
	}
	sequence, err := strconv.ParseUint(strings.TrimPrefix(alias, "v"), 10, 64)
	if err != nil || sequence == 0 || alias != fmt.Sprintf("v%d", sequence) {
		return 0, fmt.Errorf("version alias must be canonical vN")
	}
	return sequence, nil
}

type HeadResult struct {
	Before   ExpectedHead `json:"before"`
	After    ExpectedHead `json:"after"`
	Changed  bool         `json:"changed"`
	Revision uint64       `json:"revision"`
}

type SourceOperationResult struct {
	SourceInvocationID string                       `json:"source_invocation_id"`
	Operation          string                       `json:"operation"`
	Outcome            string                       `json:"outcome"`
	BundleHash         string                       `json:"bundle_hash"`
	SchemaDigest       SchemaDigest                 `json:"schema_digest"`
	Declaration        DeclarationRef               `json:"declaration"`
	ExpectedHead       ExpectedHead                 `json:"expected_head"`
	ObservedHead       ExpectedHead                 `json:"observed_head"`
	Candidate          CandidateVersion             `json:"candidate"`
	Head               HeadResult                   `json:"head"`
	Delta              DeltaResult                  `json:"delta"`
	Defects            PageResult[ValidationDefect] `json:"defects"`
	CompletedAt        time.Time                    `json:"completed_at"`
}

func (r SourceOperationResult) ValidateCandidateState() error {
	return validateSourceCandidateState(r.Operation, r.Outcome, r.Candidate)
}

func (r SourceOperationResult) Validate() error {
	return validateSourceOperationResult(r)
}

func validateSourceCandidateState(operation, outcome string, candidate CandidateVersion) error {
	if err := candidate.Validate(); err != nil {
		return err
	}
	if operation != "check" && operation != "import" {
		return fmt.Errorf("source operation must be check or import")
	}
	want := ""
	switch outcome {
	case "validation_rejected":
		want = "none"
	case "head_conflict":
		want = "candidate"
	case "accepted":
		if operation == "check" {
			want = "candidate"
		} else {
			want = "version"
		}
	default:
		return fmt.Errorf("source outcome is unsupported")
	}
	if candidate.State != want {
		return fmt.Errorf("source %s %s requires candidate state %s", operation, outcome, want)
	}
	return nil
}

type DeltaKey struct {
	Key BusinessKey `json:"key"`
}

type SourceEvidence struct {
	Defects      []ValidationDefect `json:"defects"`
	DeltaAdded   []DeltaKey         `json:"delta_added"`
	DeltaRemoved []DeltaKey         `json:"delta_removed"`
	DeltaChanged []DeltaKey         `json:"delta_changed"`
}

type PruneCommand struct {
	PruneInvocationID string         `json:"prune_invocation_id"`
	Actor             string         `json:"actor"`
	Declaration       DeclarationRef `json:"declaration"`
	VersionID         VersionID      `json:"version_id"`
	ExpectedHead      ExpectedHead   `json:"expected_head"`
}

func (c PruneCommand) RequestHash() (string, []byte, error) {
	parsed, err := uuid.Parse(c.PruneInvocationID)
	if err != nil || parsed.String() != c.PruneInvocationID {
		return "", nil, fmt.Errorf("prune_invocation_id must be one canonical UUID")
	}
	if strings.TrimSpace(c.Actor) == "" {
		return "", nil, fmt.Errorf("prune operation requires actor")
	}
	if err := c.Declaration.Validate(); err != nil {
		return "", nil, err
	}
	if err := c.VersionID.Validate(); err != nil {
		return "", nil, err
	}
	if err := c.ExpectedHead.Validate(); err != nil {
		return "", nil, err
	}
	raw, err := canonicaljson.Bytes(c)
	if err != nil {
		return "", nil, err
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte("swarm.resource.prune.request.v1"))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(raw)
	return "resource-prune-request-v1:sha256:" + hex.EncodeToString(hash.Sum(nil)), raw, nil
}

type Version struct {
	VersionID       VersionID    `json:"version_id"`
	SequenceAlias   uint64       `json:"sequence_alias"`
	Manifest        Manifest     `json:"manifest"`
	BusinessKey     string       `json:"business_key,omitempty"`
	CanonicalSchema []byte       `json:"canonical_schema"`
	Provenance      []Provenance `json:"provenance"`
	CanonicalJSONL  []byte       `json:"canonical_jsonl,omitempty"`
	PrunedAt        *time.Time   `json:"pruned_at,omitempty"`
}

type ResourceSnapshot struct {
	Declaration Declaration `json:"declaration"`
	Head        HeadResult  `json:"head"`
	Versions    []Version   `json:"versions"`
}

type SourceOperationRecord struct {
	Evaluation SourceEvaluationContext `json:"-"`
	Result     SourceOperationResult   `json:"result"`
	Evidence   SourceEvidence          `json:"evidence"`
}

type HeadHistory struct {
	Declaration DeclarationRef `json:"declaration"`
	Revision    uint64         `json:"revision"`
	Before      ExpectedHead   `json:"before"`
	After       ExpectedHead   `json:"after"`
	Operation   string         `json:"operation"`
	OperationID string         `json:"operation_id"`
	CommittedAt time.Time      `json:"committed_at"`
}

type FusedImport struct {
	SourceInvocationID string         `json:"source_invocation_id"`
	Declaration        DeclarationRef `json:"declaration"`
	ExpectedHead       ExpectedHead   `json:"expected_head"`
	InputFormat        string         `json:"input_format"`
	Input              []byte         `json:"input"`
}

func (i FusedImport) validate() error {
	parsed, err := uuid.Parse(i.SourceInvocationID)
	if err != nil || parsed == uuid.Nil || parsed.String() != i.SourceInvocationID {
		return fmt.Errorf("source_invocation_id must be one canonical non-zero UUID")
	}
	if err := i.Declaration.Validate(); err != nil {
		return err
	}
	if err := i.ExpectedHead.Validate(); err != nil {
		return err
	}
	if i.InputFormat != "jsonl" {
		return fmt.Errorf("fused import format must be jsonl")
	}
	return nil
}

type ExplicitPin struct {
	Declaration DeclarationRef `json:"declaration"`
	VersionID   VersionID      `json:"version_id"`
}

func (p ExplicitPin) validate() error {
	if err := p.Declaration.Validate(); err != nil {
		return err
	}
	return p.VersionID.Validate()
}

func CanonicalExplicitPins(pins []ExplicitPin) ([]ExplicitPin, error) {
	if len(pins) > MaxDataDeclarationsPerBundle {
		return nil, fmt.Errorf("explicit pins are capped at %d items", MaxDataDeclarationsPerBundle)
	}
	canonical := append([]ExplicitPin(nil), pins...)
	for index := range canonical {
		if err := canonical[index].validate(); err != nil {
			return nil, err
		}
	}
	sort.Slice(canonical, func(i, j int) bool {
		return CompareDeclarationRef(canonical[i].Declaration, canonical[j].Declaration) < 0
	})
	for index := 1; index < len(canonical); index++ {
		if canonical[index-1].Declaration == canonical[index].Declaration {
			return nil, fmt.Errorf("explicit pins repeat declaration %s", canonical[index].Declaration.Key())
		}
	}
	return canonical, nil
}

type RunCreationDataEnvelope struct {
	Imports []FusedImport `json:"imports"`
	Pins    []ExplicitPin `json:"pins"`
}

func (e RunCreationDataEnvelope) Canonical() (RunCreationDataEnvelope, error) {
	if len(e.Imports) > MaxDataDeclarationsPerBundle || len(e.Pins) > MaxDataDeclarationsPerBundle {
		return RunCreationDataEnvelope{}, fmt.Errorf("run data imports and pins are capped at %d items each", MaxDataDeclarationsPerBundle)
	}
	totalBytes := 0
	selected := make(map[string]string, len(e.Imports)+len(e.Pins))
	invocations := make(map[string]struct{}, len(e.Imports))
	imports := append([]FusedImport(nil), e.Imports...)
	for _, item := range imports {
		if err := item.validate(); err != nil {
			return RunCreationDataEnvelope{}, err
		}
		if _, duplicate := invocations[item.SourceInvocationID]; duplicate {
			return RunCreationDataEnvelope{}, fmt.Errorf("fused imports repeat source_invocation_id %s", item.SourceInvocationID)
		}
		invocations[item.SourceInvocationID] = struct{}{}
		totalBytes += len(item.Input)
		key := item.Declaration.Key()
		if prior := selected[key]; prior != "" {
			return RunCreationDataEnvelope{}, fmt.Errorf("data declaration %s is selected by both %s and fused import", key, prior)
		}
		selected[key] = "fused import"
	}
	if totalBytes > MaxDecodedImportBytes {
		return RunCreationDataEnvelope{}, fmt.Errorf("aggregate fused import bytes exceed %d", MaxDecodedImportBytes)
	}
	pins, err := CanonicalExplicitPins(e.Pins)
	if err != nil {
		return RunCreationDataEnvelope{}, err
	}
	for _, item := range pins {
		key := item.Declaration.Key()
		if prior := selected[key]; prior != "" {
			return RunCreationDataEnvelope{}, fmt.Errorf("data declaration %s is selected by both %s and explicit pin", key, prior)
		}
		selected[key] = "explicit pin"
	}
	sort.Slice(imports, func(i, j int) bool { return CompareDeclarationRef(imports[i].Declaration, imports[j].Declaration) < 0 })
	return RunCreationDataEnvelope{Imports: imports, Pins: pins}, nil
}

// RunCreationCommand is the method-neutral durable parent operation consumed
// by event.publish and run.start. InitialEvent contains canonical semantic
// publication input, never wrapper or transport fields.
type RunCreationCommand struct {
	RunID        string                  `json:"run_id"`
	Actor        string                  `json:"actor"`
	BundleHash   string                  `json:"bundle_hash"`
	EventID      string                  `json:"event_id"`
	InitialEvent json.RawMessage         `json:"initial_event"`
	Data         RunCreationDataEnvelope `json:"data"`
}

func (c RunCreationCommand) RequestHash() (string, []byte, RunCreationCommand, error) {
	runID, err := uuid.Parse(c.RunID)
	if err != nil || runID == uuid.Nil || runID.String() != c.RunID {
		return "", nil, RunCreationCommand{}, fmt.Errorf("run_id must be one canonical non-zero UUID")
	}
	eventID, err := uuid.Parse(c.EventID)
	if err != nil || eventID == uuid.Nil || eventID.String() != c.EventID {
		return "", nil, RunCreationCommand{}, fmt.Errorf("event_id must be one canonical non-zero UUID")
	}
	if strings.TrimSpace(c.Actor) == "" || strings.TrimSpace(c.BundleHash) == "" || len(c.InitialEvent) == 0 {
		return "", nil, RunCreationCommand{}, fmt.Errorf("run creation requires actor, bundle_hash, and initial_event")
	}
	canonicalData, err := c.Data.Canonical()
	if err != nil {
		return "", nil, RunCreationCommand{}, err
	}
	semantic, err := canonicaljson.Decode(c.InitialEvent)
	if err != nil {
		return "", nil, RunCreationCommand{}, fmt.Errorf("initial event semantic request is invalid: %w", err)
	}
	canonicalEvent, err := canonicaljson.Encode(semantic)
	if err != nil {
		return "", nil, RunCreationCommand{}, err
	}
	c.Data = canonicalData
	c.InitialEvent = canonicalEvent
	raw, err := canonicaljson.Bytes(c)
	if err != nil {
		return "", nil, RunCreationCommand{}, err
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte("swarm.resource.run-creation.request.v1"))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(raw)
	return "resource-run-creation-request-v1:sha256:" + hex.EncodeToString(hash.Sum(nil)), raw, c, nil
}

type FusedChildEvaluation struct {
	SourceInvocationID string           `json:"source_invocation_id"`
	BundleHash         string           `json:"bundle_hash"`
	SchemaDigest       SchemaDigest     `json:"schema_digest"`
	Declaration        DeclarationRef   `json:"declaration"`
	ExpectedHead       ExpectedHead     `json:"expected_head"`
	ObservedHead       ExpectedHead     `json:"observed_head"`
	Candidate          CandidateVersion `json:"candidate"`
	Delta              DeltaResult      `json:"delta"`
	DefectCount        int              `json:"defect_count"`
	Outcome            string           `json:"outcome"`
}

func (e FusedChildEvaluation) ValidateCandidateState() error {
	if err := e.Candidate.Validate(); err != nil {
		return err
	}
	want := ""
	switch e.Outcome {
	case "ready", "head_conflict":
		want = "candidate"
	case "validation_rejected":
		want = "none"
	default:
		return fmt.Errorf("fused child outcome is unsupported")
	}
	if e.Candidate.State != want {
		return fmt.Errorf("fused child %s requires candidate state %s", e.Outcome, want)
	}
	return nil
}

type SourceOperationSummary struct {
	SourceInvocationID string           `json:"source_invocation_id"`
	Operation          string           `json:"operation"`
	Outcome            string           `json:"outcome"`
	BundleHash         string           `json:"bundle_hash"`
	SchemaDigest       SchemaDigest     `json:"schema_digest"`
	Declaration        DeclarationRef   `json:"declaration"`
	ExpectedHead       ExpectedHead     `json:"expected_head"`
	ObservedHead       ExpectedHead     `json:"observed_head"`
	Candidate          CandidateVersion `json:"candidate"`
	Head               HeadResult       `json:"head"`
	Delta              DeltaResult      `json:"delta"`
	DefectCount        int              `json:"defect_count"`
	CompletedAt        time.Time        `json:"completed_at"`
}

func (s SourceOperationSummary) ValidateCandidateState() error {
	return validateSourceCandidateState(s.Operation, s.Outcome, s.Candidate)
}

func SummarizeSource(record SourceOperationRecord) SourceOperationSummary {
	r := record.Result
	return SourceOperationSummary{
		SourceInvocationID: r.SourceInvocationID, Operation: r.Operation, Outcome: r.Outcome,
		BundleHash: r.BundleHash, SchemaDigest: r.SchemaDigest, Declaration: r.Declaration,
		ExpectedHead: r.ExpectedHead, ObservedHead: r.ObservedHead, Candidate: r.Candidate,
		Head: r.Head, Delta: r.Delta, DefectCount: len(record.Evidence.Defects), CompletedAt: r.CompletedAt,
	}
}

type RunCreationOperationSummary struct {
	Kind        string               `json:"kind"`
	Outcome     string               `json:"outcome"`
	RunID       string               `json:"run_id"`
	BundleHash  string               `json:"bundle_hash"`
	EventID     string               `json:"event_id,omitempty"`
	Status      string               `json:"status,omitempty"`
	PinCount    int                  `json:"pin_count"`
	ImportCount int                  `json:"import_count"`
	Rejection   RunCreationRejection `json:"rejection"`
	CompletedAt time.Time            `json:"completed_at"`
}

type RunCreationRejection struct {
	State                string          `json:"state"`
	Code                 string          `json:"code,omitempty"`
	Declaration          *DeclarationRef `json:"declaration,omitempty"`
	VersionID            VersionID       `json:"version_id,omitempty"`
	ExpectedSchemaDigest SchemaDigest    `json:"expected_schema_digest,omitempty"`
	SelectedSchemaDigest SchemaDigest    `json:"selected_schema_digest,omitempty"`
}

const (
	RunCreationRejectionFusedValidation = "fused_validation_rejected"
	RunCreationRejectionFusedHead       = "fused_head_conflict"
	RunCreationRejectionVersionMissing  = "explicit_pin_version_missing"
	RunCreationRejectionVersionPruned   = "explicit_pin_version_pruned"
	RunCreationRejectionSchemaMismatch  = "explicit_pin_schema_mismatch"
)

func NoRunCreationRejection() RunCreationRejection {
	return RunCreationRejection{State: "none"}
}

func (r RunCreationRejection) ValidateForOutcome(outcome string) error {
	if r.State == "none" {
		if outcome != "created" || r.Code != "" || r.Declaration != nil || r.VersionID != "" ||
			r.ExpectedSchemaDigest != "" || r.SelectedSchemaDigest != "" {
			return fmt.Errorf("run-creation rejection none contradicts outcome or rejection facts")
		}
		return nil
	}
	if r.State != "rejected" || (outcome != "data_rejected" && outcome != "head_conflict") {
		return fmt.Errorf("run-creation rejection state contradicts outcome")
	}
	noTargetFacts := func() bool {
		return r.Declaration == nil && r.VersionID == "" && r.ExpectedSchemaDigest == "" && r.SelectedSchemaDigest == ""
	}
	switch r.Code {
	case RunCreationRejectionFusedValidation:
		if outcome != "data_rejected" || !noTargetFacts() {
			return fmt.Errorf("fused validation rejection facts contradict outcome")
		}
	case RunCreationRejectionFusedHead:
		if outcome != "head_conflict" || !noTargetFacts() {
			return fmt.Errorf("fused head rejection facts contradict outcome")
		}
	case RunCreationRejectionVersionMissing, RunCreationRejectionVersionPruned:
		if outcome != "data_rejected" || r.Declaration == nil || r.Declaration.Validate() != nil || r.VersionID.Validate() != nil ||
			r.ExpectedSchemaDigest != "" || r.SelectedSchemaDigest != "" {
			return fmt.Errorf("explicit pin version rejection facts are incomplete or contradictory")
		}
	case RunCreationRejectionSchemaMismatch:
		if outcome != "data_rejected" || r.Declaration == nil || r.Declaration.Validate() != nil || r.VersionID.Validate() != nil ||
			r.ExpectedSchemaDigest.Validate() != nil || r.SelectedSchemaDigest.Validate() != nil ||
			r.ExpectedSchemaDigest == r.SelectedSchemaDigest {
			return fmt.Errorf("explicit pin schema rejection facts are incomplete or contradictory")
		}
	default:
		return fmt.Errorf("unknown run-creation rejection code %q", r.Code)
	}
	return nil
}

type RunCreationDataItem struct {
	Kind   string                  `json:"kind"`
	Pin    *Pin                    `json:"pin,omitempty"`
	Import *SourceOperationSummary `json:"import,omitempty"`
}

type DataBinding struct {
	State       string                           `json:"state"`
	RunID       string                           `json:"run_id,omitempty"`
	PinCount    int                              `json:"pin_count,omitempty"`
	ImportCount int                              `json:"import_count,omitempty"`
	Evidence    *PageResult[RunCreationDataItem] `json:"evidence,omitempty"`
}

type RunCreationEvidence struct {
	ChildEvaluations []FusedChildEvaluation `json:"child_evaluations"`
	ChildDefects     []FusedChildDefect     `json:"child_defects"`
	RunBinding       []RunCreationDataItem  `json:"run_binding"`
}

type FusedChildDefect struct {
	SourceInvocationID string           `json:"source_invocation_id"`
	Defect             ValidationDefect `json:"defect"`
}

type RunCreationOperationRecord struct {
	Summary  RunCreationOperationSummary `json:"summary"`
	Binding  DataBinding                 `json:"data_binding"`
	Evidence RunCreationEvidence         `json:"evidence"`
}

type PruneDefect struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type PruneOperationResult struct {
	Outcome           string                   `json:"outcome"`
	PruneInvocationID string                   `json:"prune_invocation_id"`
	Declaration       DeclarationRef           `json:"declaration"`
	VersionID         VersionID                `json:"version_id"`
	ExpectedHead      ExpectedHead             `json:"expected_head"`
	ObservedHead      ExpectedHead             `json:"observed_head"`
	CurrentVersionID  VersionID                `json:"current_version_id,omitempty"`
	PayloadBefore     string                   `json:"payload_before,omitempty"`
	PayloadAfter      string                   `json:"payload_after,omitempty"`
	PinCount          int                      `json:"pin_count,omitempty"`
	Pins              *PageResult[Pin]         `json:"pins,omitempty"`
	Defects           *PageResult[PruneDefect] `json:"defects,omitempty"`
	CompletedAt       time.Time                `json:"completed_at"`
}

type Store interface {
	ExecuteSourceOperation(context.Context, SourceCommand) (SourceOperationResult, error)
	Prune(context.Context, PruneCommand) (PruneOperationResult, error)
	Show(context.Context, string, DeclarationRef) (ResourceSnapshot, error)
	ListDeclarationSummaries(context.Context, string) ([]DeclarationSummary, error)
	ListVersionSummaries(context.Context, DeclarationRef, uint64, int) ([]VersionSummary, error)
	ResolveVersionSummary(context.Context, DeclarationRef, VersionSelector) (VersionSummary, error)
	LoadVersionPayload(context.Context, DeclarationRef, VersionID) (Version, error)
	ListVersionProvenance(context.Context, VersionID, uint64, int) ([]Provenance, error)
	ListPins(context.Context, VersionID, string, int) ([]Pin, error)
	ListHeadHistory(context.Context, DeclarationRef, uint64, int) ([]HeadHistory, error)
	LoadSourceOperation(context.Context, string) (SourceOperationRecord, error)
	LoadPruneOperation(context.Context, string) (PruneOperationResult, error)
	LoadPruneOperationPins(context.Context, string) ([]Pin, error)
	LoadPins(context.Context, VersionID) ([]Pin, error)
	LoadHeadHistory(context.Context, DeclarationRef) ([]HeadHistory, error)
	LoadRunCreationOperation(context.Context, string) (RunCreationOperationRecord, error)
	LoadRunResourceAccess(context.Context, string, []DeclarationRef) ([]ResourceAccessItem, error)
}
