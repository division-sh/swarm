package contracts

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/paths"
	flowmodel "github.com/division-sh/swarm/internal/runtime/flowmodel"
	managedcredentialmodel "github.com/division-sh/swarm/internal/runtime/managedcredentials/model"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	"gopkg.in/yaml.v3"
)

type ContractPaths struct {
	ContractsRoot         string
	WorkflowDir           string
	RootSchemaFile        string
	RootTypesFile         string
	RootEntitiesFile      string
	ProjectPackageFile    string
	ProjectPackages       []ProjectPackagePaths
	ProjectNodesFile      string
	ProjectEventsFile     string
	ProjectAgentsFile     string
	ProjectToolsFile      string
	ProjectPolicyFile     string
	ProjectPromptsDir     string
	PlatformSpecFile      string
	VerificationGatesFile string
	ToolingLockFile       string
	DDLFile               string
	AgentConfigMapFile    string
	Flows                 []FlowContractPaths
}
type WorkflowContractBundle struct {
	Paths                 ContractPaths
	Package               ProjectPackageDocument
	PackageTree           []LoadedProjectPackage
	projectContracts      map[string]ProjectContractView
	flowTypes             map[string]TypeCatalogDocument
	flowEntities          map[string]EntityContractsDocument
	scopedNodes           map[string]SystemNodeContract
	scopedEvents          map[string]EventCatalogEntry
	scopedAgents          map[string]AgentRegistryEntry
	scopedTools           map[string]ToolSchemaEntry
	scopedNodeSources     map[string]ContractItemSource
	scopedEventSources    map[string]ContractItemSource
	scopedAgentSources    map[string]ContractItemSource
	scopedToolSources     map[string]ContractItemSource
	nodeSources           map[string]ContractItemSource
	eventSources          map[string]ContractItemSource
	agentSources          map[string]ContractItemSource
	toolSources           map[string]ContractItemSource
	ambiguousNodeAliases  map[string]struct{}
	ambiguousEventAliases map[string]struct{}
	ambiguousAgentAliases map[string]struct{}
	ambiguousToolAliases  map[string]struct{}
	Semantics             WorkflowSemanticView
	Nodes                 map[string]SystemNodeContract
	Events                map[string]EventCatalogEntry
	Agents                map[string]AgentRegistryEntry
	Tools                 map[string]ToolSchemaEntry
	Policy                PolicyDocument
	Platform              PlatformSpecDocument
	RootSchema            *FlowSchemaDocument
	RootTypes             TypeCatalogDocument
	RootEntities          EntityContractsDocument
	FlowSchemas           map[string]FlowSchemaDocument
	FlowTree              FlowTree
	URIRegistry           ContractURIRegistry
}
type WorkflowSemanticView struct {
	Name                   string
	Version                string
	InitialStage           string
	EntitySchema           EntitySchema
	Stages                 []WorkflowStageContract
	TerminalStages         []string
	Transitions            []WorkflowTransitionContract
	Timers                 []WorkflowTimerContract
	Joins                  []WorkflowJoinPlan
	Loops                  []WorkflowLoopPlan
	Gates                  []WorkflowGatePlan
	Guards                 []GuardActionEntry
	Actions                []GuardActionEntry
	GuardByID              map[string]GuardActionEntry
	ActionByID             map[string]GuardActionEntry
	FlowInitial            map[string]string
	FlowStates             map[string][]string
	FlowTerminal           map[string][]string
	FlowNamespace          map[string]string
	FlowPrefix             map[string]string
	FlowRules              map[string]string
	FlowInputs             map[string][]string
	FlowOutputs            map[string][]string
	FlowInputEventPins     map[string][]FlowInputEventPin
	FlowOutputEventPins    map[string][]FlowOutputEventPin
	FlowReads              map[string][]string
	FlowWrites             map[string][]string
	CompositionConnects    []FlowPackageConnect
	FlowAgents             map[string][]FlowRequiredAgent
	RootAgentFacts         []RequiredAgentFact
	FlowAgentFacts         map[string][]RequiredAgentFact
	WritePinOwners         map[string][]string
	EffectiveNodes         map[string]SystemNodeEffectiveSemantics
	NodeHandlers           map[string]map[string]SystemNodeEventHandler
	EventOwners            map[string][]string
	HandlerTransitions     []HandlerTransitionSemantic
	HandlerTransitionIndex map[string]map[string]HandlerTransitionSemantic
	StageTopologies        map[string]WorkflowStageTopology
}

type SystemNodeEffectiveSemantics struct {
	ID                   string
	ExecutionType        string
	RuntimeSubscriptions []string
	Produces             []string
}

type FlowInputAutoWireResolution struct {
	EventType     string
	Patterns      []string
	ProducerFlows []string
	Evidence      []FlowInputProducerEvidence
}

type FlowInputProducerResolution struct {
	FlowID    string
	EventType string
	Evidence  []FlowInputProducerEvidence
}

type FlowInputProducerEvidence struct {
	Kind      string
	FlowID    string
	EventType string
	Pin       string
	Pattern   string
	Detail    string
}

const (
	FlowInputProducerBoundaryExternalIngress  = "boundary_external_ingress"
	FlowInputProducerBoundaryIntrinsicIngress = "boundary_intrinsic_ingress"
	FlowInputProducerBoundaryParentConnect    = "boundary_parent_connect"
	FlowInputProducerBoundaryHarnessInjection = "boundary_harness_injection"
	FlowInputProducerPlatformSource           = "platform_source"
	FlowInputProducerInternalTopology         = "internal_topology_producer"
	FlowInputProducerMissing                  = "missing"
	FlowInputProducerAmbiguous                = "ambiguous"
	FlowInputProducerInvalidContext           = "invalid_context"
)

type FlowInputProducerResolutionOptions struct {
	HarnessInjections  []FlowInputProducerInjection
	AllowNonInputEvent bool
}

type FlowInputProducerInjection struct {
	FlowID    string
	EventType string
}

func (r FlowInputProducerResolution) HasEvidence() bool {
	for _, evidence := range r.Evidence {
		if FlowInputProducerEvidenceKindIsProof(evidence.Kind) {
			return true
		}
	}
	return false
}

func (r FlowInputProducerResolution) HasEvidenceKind(kind string) bool {
	kind = strings.TrimSpace(kind)
	if kind == "" {
		return false
	}
	for _, evidence := range r.Evidence {
		if strings.TrimSpace(evidence.Kind) == kind {
			return true
		}
	}
	return false
}

func FlowInputProducerEvidenceKindIsProof(kind string) bool {
	switch strings.TrimSpace(kind) {
	case FlowInputProducerBoundaryExternalIngress,
		FlowInputProducerBoundaryIntrinsicIngress,
		FlowInputProducerBoundaryParentConnect,
		FlowInputProducerBoundaryHarnessInjection,
		FlowInputProducerPlatformSource,
		FlowInputProducerInternalTopology:
		return true
	default:
		return false
	}
}

func (r FlowInputProducerResolution) BoundaryEvidence() []FlowInputProducerEvidence {
	out := make([]FlowInputProducerEvidence, 0)
	for _, evidence := range r.Evidence {
		switch strings.TrimSpace(evidence.Kind) {
		case FlowInputProducerBoundaryExternalIngress,
			FlowInputProducerBoundaryIntrinsicIngress,
			FlowInputProducerBoundaryParentConnect,
			FlowInputProducerBoundaryHarnessInjection:
			out = append(out, evidence)
		}
	}
	return out
}

func (r FlowInputProducerResolution) HasAmbiguousBoundaryEvidence() bool {
	seen := map[string]struct{}{}
	for _, evidence := range r.BoundaryEvidence() {
		key := strings.TrimSpace(evidence.Kind) + "|" + strings.TrimSpace(evidence.FlowID) + "|" + strings.TrimSpace(evidence.Pin) + "|" + strings.TrimSpace(evidence.EventType)
		if key == "" {
			continue
		}
		seen[key] = struct{}{}
	}
	return len(seen) > 1
}

func (r FlowInputProducerResolution) ProducerPatterns() []string {
	seen := map[string]struct{}{}
	for _, evidence := range r.Evidence {
		if !FlowInputProducerEvidenceKindIsProof(evidence.Kind) {
			continue
		}
		pattern := strings.TrimSpace(evidence.Pattern)
		if pattern == "" {
			continue
		}
		seen[pattern] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for pattern := range seen {
		out = append(out, pattern)
	}
	sort.Strings(out)
	return out
}

func (r FlowInputProducerResolution) ProducerFlows() []string {
	seen := map[string]struct{}{}
	for _, evidence := range r.Evidence {
		if !FlowInputProducerEvidenceKindIsProof(evidence.Kind) {
			continue
		}
		if strings.TrimSpace(evidence.Kind) == FlowInputProducerInternalTopology {
			continue
		}
		flowID := strings.TrimSpace(evidence.FlowID)
		if flowID == "" {
			continue
		}
		seen[flowID] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for flowID := range seen {
		out = append(out, flowID)
	}
	sort.Strings(out)
	return out
}

func (r FlowInputProducerResolution) AutoWireResolution() FlowInputAutoWireResolution {
	return FlowInputAutoWireResolution{
		EventType:     strings.TrimSpace(r.EventType),
		Patterns:      r.ProducerPatterns(),
		ProducerFlows: r.ProducerFlows(),
		Evidence:      append([]FlowInputProducerEvidence{}, r.Evidence...),
	}
}

type HandlerTransitionSemantic struct {
	ID                   string
	NodeID               string
	FlowID               string
	EventType            string
	CreateEntity         bool
	Action               ActionSpec
	Activity             ActivitySpec
	SelectEntity         *SelectEntitySpec
	SelectOrCreateEntity *SelectOrCreateEntitySpec
	Guard                *GuardSpec
	AdvancesTo           string
	SetsGate             *GateSpec
	ClearGates           []string
	DataAccumulation     WorkflowDataAccumulation
	Emit                 EmitSpec
	OnSuccess            HandlerOnSuccessSpec
	Condition            string
	Loop                 *LoopOperationSpec
	OnComplete           []HandlerRuleEntry
	Rules                []HandlerRuleEntry
	Accumulate           *AccumulateSpec
	Join                 *JoinSpec
	Compute              *ComputeSpec
	Query                *QuerySpec
	FanOut               *FanOutSpec
	GroupBy              *GroupBySpec
	Filter               *FilterSpec
	Reduce               *ReduceSpec
	Count                *CountSpec
	Clear                *ClearSpec
}
type HandlerRuleEntry struct {
	ID               string                   `yaml:"id"`
	Description      string                   `yaml:"description"`
	Condition        string                   `yaml:"condition"`
	PolicyRow        PolicySheetRowMetadata   `yaml:"-"`
	AdvancesTo       string                   `yaml:"advances_to"`
	Emit             EmitSpec                 `yaml:"emit"`
	Action           ActionSpec               `yaml:"action"`
	Activity         ActivitySpec             `yaml:"activity"`
	DataAccumulation WorkflowDataAccumulation `yaml:"data_accumulation"`
	Compute          *ComputeSpec             `yaml:"compute"`
	FanOut           *FanOutSpec              `yaml:"fan_out"`
}

type WorkflowJoinPlan struct {
	FlowID       string
	NodeID       string
	HandlerEvent string
	Spec         JoinSpec
	ResultType   CatalogTypeReference
}

type WorkflowLoopPlan struct {
	FlowID        string
	ID            string
	RevisionField string
	MaxAttempts   LoopAttemptLimit
	Escape        LoopEscapeSpec
	EntryStage    string
	RegionStages  []string
	Operations    []WorkflowLoopOperationPlan
}

type WorkflowGatePlan struct {
	FlowID   string
	Stage    string
	Decision string
	Title    string
	Context  map[string]ExpressionValue
	Outcomes map[string]WorkflowGateOutcomePlan
}

type WorkflowGateOutcomePlan struct {
	Verdict    string
	Label      string
	Input      map[string]WorkflowGateInputField
	AdvancesTo string
	Emit       EmitSpec
}

type WorkflowGateInputField struct {
	Type     string `yaml:"type" json:"type"`
	Required bool   `yaml:"required" json:"required"`
	Label    string `yaml:"label,omitempty" json:"label,omitempty"`
}

// WorkflowStageTopology is the canonical lowered lifecycle graph for one flow.
// Verifier, runtime loop plans, and authoring projections consume this graph.
type WorkflowStageTopology struct {
	FlowID         string
	InitialStage   string
	Stages         []string
	TerminalStages []string
	Edges          []WorkflowStageTopologyEdge
	Handlers       []WorkflowHandlerStageScope
}

type WorkflowHandlerStageScope struct {
	NodeID    string
	EventType string
	Stages    []string
}

type WorkflowStageTopologyEdge struct {
	From          string
	To            string
	Source        string
	NodeID        string
	HandlerEvent  string
	EventType     string
	LoopID        string
	LoopOperation LoopOperationKind
	TimerID       string
	After         string
	Timed         bool
	DecisionID    string
	Verdict       string
}

type WorkflowLoopOperationPlan struct {
	NodeID       string
	HandlerEvent string
	Kind         LoopOperationKind
	LoopID       string
	From         string
	AdvancesTo   string
	Emit         EmitSpec
}

type PolicySheetRowKind string

const (
	PolicySheetRowKindWhen     PolicySheetRowKind = "when"
	PolicySheetRowKindCase     PolicySheetRowKind = "case"
	PolicySheetRowKindRange    PolicySheetRowKind = "range"
	PolicySheetRowKindLookup   PolicySheetRowKind = "lookup"
	PolicySheetRowKindValidate PolicySheetRowKind = "validate"
	PolicySheetRowKindModule   PolicySheetRowKind = "compute_module"
	PolicySheetRowKindDefault  PolicySheetRowKind = "default"
)

type PolicySheetRowMetadata struct {
	Kind         PolicySheetRowKind
	Selectors    []string
	CaseValues   []string
	RangeValue   string
	RangeLower   PolicySheetRangeBound
	RangeUpper   PolicySheetRangeBound
	Monotonicity []string
	Lookup       *ComputeLookupSpec
	Validation   *ComputeValidationSpec
	Module       *ComputeModuleSpec
}

type PolicySheetRangeBound struct {
	Operator string
	Value    string
	Kind     string
}

type ActivityEffectClass string

const (
	ActivityEffectClassReadOnly           ActivityEffectClass = "read_only"
	ActivityEffectClassIdempotentWrite    ActivityEffectClass = "idempotent_write"
	ActivityEffectClassNonIdempotentWrite ActivityEffectClass = "non_idempotent_write"
	ActivityEffectClassLongRunning        ActivityEffectClass = "long_running"
)

type ActivitySpec struct {
	ID    string                     `yaml:"id,omitempty"`
	Tool  string                     `yaml:"tool"`
	Input map[string]ExpressionValue `yaml:"input"`
}

func (a ActivitySpec) Empty() bool {
	return strings.TrimSpace(a.ID) == "" && strings.TrimSpace(a.Tool) == "" && len(a.Input) == 0
}

func NormalizeActivityEffectClass(raw string) ActivityEffectClass {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case string(ActivityEffectClassReadOnly):
		return ActivityEffectClassReadOnly
	case string(ActivityEffectClassIdempotentWrite):
		return ActivityEffectClassIdempotentWrite
	case string(ActivityEffectClassNonIdempotentWrite):
		return ActivityEffectClassNonIdempotentWrite
	case string(ActivityEffectClassLongRunning):
		return ActivityEffectClassLongRunning
	default:
		return ""
	}
}

type GuardSpec struct {
	ID         string           `yaml:"id"`
	Check      string           `yaml:"check"`
	OnFail     string           `yaml:"on_fail"`
	OnFailSpec GuardFailureSpec `yaml:"-"`
	Checks     []GuardCheck     `yaml:"checks"`
	PolicyRef  string           `yaml:"policy_ref"`
}
type GuardCheck struct {
	ID    string `yaml:"id"`
	Check string `yaml:"check"`
}

type GuardFailureAction string

const (
	GuardFailureActionReject   GuardFailureAction = "reject"
	GuardFailureActionDiscard  GuardFailureAction = "discard"
	GuardFailureActionKill     GuardFailureAction = "kill"
	GuardFailureActionEscalate GuardFailureAction = "escalate"
)

type GuardFailureSpec struct {
	Action          GuardFailureAction `yaml:"action"`
	Escalation      EmitSpec           `yaml:"escalate"`
	AuthoredMapping bool               `yaml:"-"`
}

func (s GuardFailureSpec) Empty() bool {
	return strings.TrimSpace(string(s.Action)) == "" && s.Escalation.Empty() && len(s.Escalation.Fields) == 0
}

func (s GuardFailureSpec) EscalationEmitSpec() EmitSpec {
	if s.Action != GuardFailureActionEscalate {
		return EmitSpec{}
	}
	return cloneEmitSpec(s.Escalation)
}

func (g *GuardSpec) FailureSpec() (GuardFailureSpec, error) {
	if g == nil {
		return ParseGuardFailureSpec("")
	}
	if !g.OnFailSpec.Empty() || g.OnFailSpec.AuthoredMapping {
		return g.OnFailSpec, nil
	}
	return ParseGuardFailureSpec(g.OnFail)
}

func ParseGuardFailureSpec(action string) (GuardFailureSpec, error) {
	normalized := strings.TrimSpace(strings.ToLower(action))
	switch normalized {
	case "", "reject":
		return GuardFailureSpec{Action: GuardFailureActionReject}, nil
	case "discard":
		return GuardFailureSpec{Action: GuardFailureActionDiscard}, nil
	case "kill":
		return GuardFailureSpec{Action: GuardFailureActionKill}, nil
	}
	if strings.HasPrefix(normalized, "escalate:") {
		eventType := strings.TrimSpace(strings.TrimPrefix(normalized, "escalate:"))
		if eventType == "" {
			return GuardFailureSpec{}, fmt.Errorf("guard on_fail escalate requires event type")
		}
		return GuardFailureSpec{
			Action:     GuardFailureActionEscalate,
			Escalation: EmitSpec{Event: eventType},
		}, nil
	}
	return GuardFailureSpec{}, fmt.Errorf("unsupported guard on_fail action %q", action)
}

type AccumulateSpec struct {
	Into         string               `yaml:"into"`
	ExpectedFrom string               `yaml:"expected_from"`
	From         string               `yaml:"from"`
	Description  string               `yaml:"description"`
	Window       string               `yaml:"window"`
	WindowPath   paths.Path           `yaml:"-"`
	ExpectedPath paths.Path           `yaml:"-"`
	DedupBy      string               `yaml:"dedup_by"`
	DedupPath    paths.Path           `yaml:"-"`
	Threshold    int                  `yaml:"threshold"`
	TimeoutMS    int                  `yaml:"timeout_ms"`
	Completion   AccumulateCompletion `yaml:"completion"`
	OnComplete   []HandlerRuleEntry   `yaml:"on_complete"`
	OnTimeout    *HandlerRuleEntry    `yaml:"on_timeout"`
}
type ComputeSpec struct {
	Operation   ComputeOperation       `yaml:"operation"`
	Tiers       []ComputeTier          `yaml:"tiers"`
	Keys        ComputeKeyConfig       `yaml:"keys"`
	Params      map[string]any         `yaml:"params"`
	StoreAs     string                 `yaml:"store_as"`
	Description string                 `yaml:"description"`
	ValueField  string                 `yaml:"value_field"`
	WeightField string                 `yaml:"weight_field"`
	Lookup      *ComputeLookupSpec     `yaml:"-"`
	Validation  *ComputeValidationSpec `yaml:"-"`
	Module      *ComputeModuleSpec     `yaml:"-"`
}

type ComputeLookupSpec struct {
	RowID           string               `yaml:"-"`
	On              []string             `yaml:"-"`
	OnPaths         []paths.Path         `yaml:"-"`
	Entries         []ComputeLookupEntry `yaml:"-"`
	DefaultFail     bool                 `yaml:"-"`
	DefaultDeclared bool                 `yaml:"-"`
}

type ComputeLookupEntry struct {
	Key          []ComputeLookupLiteral `yaml:"-"`
	Value        any                    `yaml:"-"`
	ValueKind    string                 `yaml:"-"`
	ValueSummary string                 `yaml:"-"`
}

type ComputeLookupLiteral struct {
	Value     any    `yaml:"-"`
	Kind      string `yaml:"-"`
	Canonical string `yaml:"-"`
	Summary   string `yaml:"-"`
}

type ComputeValidationSpec struct {
	RowID      string                `yaml:"-"`
	Set        string                `yaml:"-"`
	Into       string                `yaml:"-"`
	Input      map[string]string     `yaml:"-"`
	InputPaths map[string]paths.Path `yaml:"-"`
}

type ComputeModuleSpec struct {
	RowID      string                `yaml:"-"`
	Module     string                `yaml:"-"`
	Into       string                `yaml:"-"`
	Input      map[string]string     `yaml:"-"`
	InputPaths map[string]paths.Path `yaml:"-"`
}

type ComputeTier struct {
	Dimensions []string `yaml:"dimensions"`
	Weight     float64  `yaml:"weight"`
}
type ComputeKeyConfig struct {
	DimensionKey string   `yaml:"dimension_key"`
	ScoreKeys    []string `yaml:"score_keys"`
	NumericKeys  []string `yaml:"numeric_keys"`
}

const (
	DefaultFanOutMaxItems       = 1000
	FanOutBoundExceededCode     = "platform.fan_out_bound_exceeded"
	FanOutValidationCheckID     = "fan_out_validation"
	FanOutPlatformIndexName     = "fan_out.index"
	FanOutLegacyItemName        = "fan_out.item"
	FanOutLegacyBareItemName    = "item"
	FanOutUnsupportedIdentityNS = "fan_out.identity"
)

type FanOutSpec struct {
	ItemsFrom   string     `yaml:"items_from"`
	ItemsPath   paths.Path `yaml:"-"`
	As          string     `yaml:"as"`
	Identity    string     `yaml:"identity"`
	MaxItems    int        `yaml:"max_items"`
	MaxItemsSet bool       `yaml:"-"`
	Emit        EmitSpec   `yaml:"emit"`
}

func EffectiveFanOutMaxItems(spec FanOutSpec) int {
	if spec.MaxItems > 0 && spec.MaxItems < DefaultFanOutMaxItems {
		return spec.MaxItems
	}
	return DefaultFanOutMaxItems
}

func ValidateFanOutItemsSource(spec FanOutSpec) (paths.Path, error) {
	path := spec.ItemsPath
	if path.IsZero() {
		path = paths.Parse(spec.ItemsFrom)
	}
	raw := strings.TrimSpace(spec.ItemsFrom)
	if raw == "" || path.IsZero() {
		return paths.Path{}, fmt.Errorf("fan_out.items_from is required")
	}
	if !path.HasExplicitRoot() {
		return paths.Path{}, fmt.Errorf("fan_out.items_from %q must use an explicit payload.* or entity.* root", raw)
	}
	if path.Root != paths.RootPayload && path.Root != paths.RootEntity {
		return paths.Path{}, fmt.Errorf("fan_out.items_from %q must read a declared payload.* or entity.* collection", raw)
	}
	if len(path.Segments) != 1 || strings.TrimSpace(path.Segments[0]) == "" {
		return paths.Path{}, fmt.Errorf("fan_out.items_from %q must reference exactly one declared top-level collection field", raw)
	}
	return path, nil
}

func ValidateFanOutMaxItems(spec FanOutSpec) error {
	if spec.MaxItems < 0 || (spec.MaxItemsSet && spec.MaxItems == 0) {
		return fmt.Errorf("fan_out.max_items must be a positive integer when set")
	}
	if spec.MaxItems > DefaultFanOutMaxItems {
		return fmt.Errorf("fan_out.max_items %d exceeds platform ceiling %d; max_items may only tighten the ceiling", spec.MaxItems, DefaultFanOutMaxItems)
	}
	return nil
}

func ValidateFanOutAlias(alias string) error {
	alias = strings.TrimSpace(alias)
	if alias == "" {
		return fmt.Errorf("as is required")
	}
	if !simpleFanOutIdentifier(alias) {
		return fmt.Errorf("as %q must be a simple identifier", alias)
	}
	switch alias {
	case "payload", "entity", "_entity", "event", "policy", "computed", "fan_out", "accumulated", "query_entities", FanOutLegacyBareItemName:
		return fmt.Errorf("as %q collides with a reserved expression root", alias)
	}
	return nil
}

func simpleFanOutIdentifier(value string) bool {
	if value == "" {
		return false
	}
	for i, r := range value {
		switch {
		case r == '_':
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case i > 0 && r >= '0' && r <= '9':
		default:
			return false
		}
	}
	return true
}

type HandlerOnSuccessSpec struct {
	Emit EmitSpec `yaml:"emit"`
}

func (s HandlerOnSuccessSpec) Empty() bool {
	return s.Emit.Empty()
}

type GroupBySpec struct {
	ItemsFrom string     `yaml:"items_from"`
	ItemsPath paths.Path `yaml:"-"`
	Key       string     `yaml:"key"`
	KeyPath   paths.Path `yaml:"-"`
	StoreAs   string     `yaml:"store_as"`
	StorePath paths.Path `yaml:"-"`
}
type FilterSpec struct {
	Predicate  string     `yaml:"predicate"`
	Source     string     `yaml:"source"`
	SourcePath paths.Path `yaml:"-"`
	ItemsFrom  string     `yaml:"items_from"`
	ItemsPath  paths.Path `yaml:"-"`
	Condition  string     `yaml:"condition"`
	StoreAs    string     `yaml:"store_as"`
	StorePath  paths.Path `yaml:"-"`
}
type ReduceSpec struct {
	Operation  string                     `yaml:"operation"`
	Source     string                     `yaml:"source"`
	SourcePath paths.Path                 `yaml:"-"`
	StoreAs    string                     `yaml:"store_as"`
	StorePath  paths.Path                 `yaml:"-"`
	ItemsFrom  string                     `yaml:"items_from"`
	ItemsPath  paths.Path                 `yaml:"-"`
	Params     map[string]ExpressionValue `yaml:"params"`
}
type CountSpec struct {
	Source     string     `yaml:"source"`
	SourcePath paths.Path `yaml:"-"`
	StoreAs    string     `yaml:"store_as"`
	StorePath  paths.Path `yaml:"-"`
	ItemsFrom  string     `yaml:"items_from"`
	ItemsPath  paths.Path `yaml:"-"`
	Condition  string     `yaml:"condition"`
}
type ClearSpec struct {
	Targets []string `yaml:"targets"`
}

type ConfigFromSpec struct {
	PolicyKeys []string          `yaml:"policy_keys"`
	Bindings   map[string]string `yaml:",inline"`
	Entries    []ConfigBinding   `yaml:"-"`
}
type ConfigBinding struct {
	Key     string
	Ref     string
	RefPath paths.Path
}

type SelectEntitySpec struct {
	By       map[string]string        `yaml:"by"`
	Bindings []SelectEntityKeyBinding `yaml:"-"`
}

type SelectOrCreateEntitySpec struct {
	By       map[string]string        `yaml:"by"`
	Bindings []SelectEntityKeyBinding `yaml:"-"`
}

type SelectEntityKeyBinding struct {
	Field   string
	Ref     string
	RefPath paths.Path
}

func (s *SelectEntitySpec) Empty() bool {
	return s == nil || len(s.Bindings) == 0
}

func (s *SelectOrCreateEntitySpec) Empty() bool {
	return s == nil || len(s.Bindings) == 0
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

type GateSpec struct {
	Name  string `yaml:"name"`
	Value any    `yaml:"value"`
}
type QuerySpec struct {
	Operation    string      `yaml:"operation"`
	Source       string      `yaml:"source"`
	SourcePath   paths.Path  `yaml:"-"`
	StoreAs      string      `yaml:"store_as"`
	StorePath    paths.Path  `yaml:"-"`
	Entities     string      `yaml:"entities"`
	EntitiesPath paths.Path  `yaml:"-"`
	Filter       string      `yaml:"filter"`
	GroupBy      string      `yaml:"group_by"`
	GroupByPath  paths.Path  `yaml:"-"`
	Count        bool        `yaml:"count"`
	Select       []string    `yaml:"select"`
	Queries      []QuerySpec `yaml:"-"`
}

func (s *QuerySpec) hydratePaths() {
	if s == nil {
		return
	}
	s.Operation = strings.TrimSpace(s.Operation)
	s.Source = strings.TrimSpace(s.Source)
	s.SourcePath = paths.Parse(s.Source)
	s.StoreAs = strings.TrimSpace(s.StoreAs)
	s.StorePath = paths.Parse(s.StoreAs)
	s.Entities = strings.TrimSpace(s.Entities)
	s.EntitiesPath = paths.Parse(s.Entities)
	s.Filter = strings.TrimSpace(s.Filter)
	s.GroupBy = strings.TrimSpace(s.GroupBy)
	s.GroupByPath = paths.Parse(s.GroupBy)
	for i := range s.Queries {
		s.Queries[i].hydratePaths()
	}
}

type ActionSpec struct {
	ID             string            `yaml:"id"`
	Template       string            `yaml:"template"`
	InstanceIDFrom string            `yaml:"instance_id_from"`
	InstanceIDPath paths.Path        `yaml:"-"`
	ConfigFrom     *ConfigFromSpec   `yaml:"config_from"`
	Mailbox        *MailboxWriteSpec `yaml:"mailbox"`
	ArtifactRepo   *ArtifactRepoSpec `yaml:"artifact_repo"`
}

type MailboxWriteSpec struct {
	ItemType     ExpressionValue            `yaml:"item_type"`
	Severity     ExpressionValue            `yaml:"severity"`
	Summary      ExpressionValue            `yaml:"summary"`
	EntityID     ExpressionValue            `yaml:"entity_id"`
	FlowInstance ExpressionValue            `yaml:"flow_instance"`
	Payload      map[string]ExpressionValue `yaml:"payload"`
}

type ArtifactRepoSpec struct {
	Provider       string                     `yaml:"provider"`
	RepoID         ExpressionValue            `yaml:"repo_id"`
	Namespace      ExpressionValue            `yaml:"namespace"`
	PartitionKey   ExpressionValue            `yaml:"partition_key"`
	DisplaySlug    ExpressionValue            `yaml:"display_slug"`
	RequestID      ExpressionValue            `yaml:"request_id"`
	Author         ExpressionValue            `yaml:"author"`
	Provenance     map[string]ExpressionValue `yaml:"provenance"`
	AllowedPaths   []string                   `yaml:"allowed_paths"`
	Files          []ArtifactRepoFileSpec     `yaml:"files"`
	Output         ArtifactRepoOutputSpec     `yaml:"output"`
	Limits         ArtifactRepoLimitsSpec     `yaml:"limits"`
	SuccessEvent   string                     `yaml:"success_event"`
	SuccessPayload map[string]ExpressionValue `yaml:"success_payload"`
	FailureEvent   string                     `yaml:"failure_event"`
	FailurePayload map[string]ExpressionValue `yaml:"failure_payload"`
}

type ArtifactRepoFileSpec struct {
	Path        ExpressionValue        `yaml:"path"`
	Content     ExpressionValue        `yaml:"content"`
	ContentType string                 `yaml:"content_type"`
	Schema      ArtifactRepoSchemaSpec `yaml:"schema"`
	MaxBytes    int                    `yaml:"max_bytes"`
}

type ArtifactRepoSchemaSpec struct {
	Type           string   `yaml:"type"`
	RequiredFields []string `yaml:"required_fields"`
}

type ArtifactRepoOutputSpec struct {
	RepoURL           string `yaml:"repo_url"`
	CurrentRef        string `yaml:"current_ref"`
	FileManifest      string `yaml:"file_manifest"`
	Status            string `yaml:"status"`
	Failure           string `yaml:"failure"`
	LastRequestID     string `yaml:"last_request_id"`
	LastSourceEventID string `yaml:"last_source_event_id"`
}

type ArtifactRepoLimitsSpec struct {
	MaxYAMLBytes     int `yaml:"max_yaml_bytes"`
	MaxMarkdownBytes int `yaml:"max_markdown_bytes"`
	MaxTextBytes     int `yaml:"max_text_bytes"`
	MaxRepoBytes     int `yaml:"max_repo_bytes"`
}

type EntitySchema struct {
	Groups []EntitySchemaGroup `yaml:"groups"`
}

func (s EntitySchema) Empty() bool {
	return len(s.Groups) == 0
}

type EntitySchemaGroup struct {
	Name   string              `yaml:"name"`
	Fields []EntitySchemaField `yaml:"fields"`
}
type EntitySchemaField struct {
	Name        string `yaml:"name"`
	Type        string `yaml:"type"`
	Initial     any    `yaml:"initial"`
	Primary     bool   `yaml:"primary"`
	Indexed     bool   `yaml:"indexed"`
	Nullable    bool   `yaml:"nullable"`
	Description string `yaml:"description"`
}
type NodeStateSchema struct {
	Description string           `yaml:"description"`
	Fields      []NodeStateField `yaml:"fields"`
}
type NodeGateStateSchema struct {
	Description string          `yaml:"description"`
	Gates       []NodeGateField `yaml:"gates"`
	Storage     string          `yaml:"storage"`
}
type NodeStateField struct {
	Name    string `yaml:"name"`
	Type    string `yaml:"type"`
	Default any    `yaml:"default"`
}
type NodeGateField struct {
	Name        string `yaml:"name"`
	Description string `yaml:"description"`
}
type PolicyDocument = flowmodel.PolicyDocument
type PolicyValue = flowmodel.PolicyValue
type PolicyCriteriaSet = flowmodel.PolicyCriteriaSet
type PolicyCriteriaClass = flowmodel.PolicyCriteriaClass
type PolicyCriteriaRule = flowmodel.PolicyCriteriaRule
type PolicyCriteriaParam = flowmodel.PolicyCriteriaParam
type PolicyValidationSet = flowmodel.PolicyValidationSet
type PolicyValidationClass = flowmodel.PolicyValidationClass
type PolicyValidationRule = flowmodel.PolicyValidationRule
type PolicyValidationCheck = flowmodel.PolicyValidationCheck
type PolicyValidationEqualCheck = flowmodel.PolicyValidationEqualCheck
type PolicyModule = flowmodel.PolicyModule
type PolicyModuleRuntime = flowmodel.PolicyModuleRuntime
type PolicyModuleLimits = flowmodel.PolicyModuleLimits
type ContractURIRegistry = flowmodel.URIRegistry
type ContractURIRef = flowmodel.URIRef
type ExpressionKind string

const (
	ExpressionKindLiteral ExpressionKind = "literal"
	ExpressionKindRef     ExpressionKind = "ref"
	ExpressionKindCEL     ExpressionKind = "cel"
)

type WorkflowDataOperation string

const (
	WorkflowDataOperationSet    WorkflowDataOperation = "set"
	WorkflowDataOperationMerge  WorkflowDataOperation = "merge"
	WorkflowDataOperationDelete WorkflowDataOperation = "delete"
	WorkflowDataOperationAppend WorkflowDataOperation = "append"
	WorkflowDataOperationUpdate WorkflowDataOperation = "update"
)

type ExpressionValue struct {
	Kind    ExpressionKind `yaml:"kind,omitempty"`
	Literal any            `yaml:"literal,omitempty"`
	Ref     string         `yaml:"ref,omitempty"`
	RefPath paths.Path     `yaml:"-"`
	CEL     string         `yaml:"cel,omitempty"`
}

func (e ExpressionValue) IsZero() bool {
	return e.Kind == "" && strings.TrimSpace(e.CEL) == "" && strings.TrimSpace(e.Ref) == "" && e.Literal == nil
}

func LiteralExpression(value any) ExpressionValue {
	return ExpressionValue{Kind: ExpressionKindLiteral, Literal: value}
}

func RefExpression(ref string) ExpressionValue {
	expr := ExpressionValue{Kind: ExpressionKindRef, Ref: strings.TrimSpace(ref)}
	expr.hydrate()
	return expr
}

func CELExpression(expression string) ExpressionValue {
	return ExpressionValue{Kind: ExpressionKindCEL, CEL: strings.TrimSpace(expression)}
}

func (e *ExpressionValue) hydrate() {
	if e == nil {
		return
	}
	e.Ref = strings.TrimSpace(e.Ref)
	e.CEL = strings.TrimSpace(e.CEL)
	e.RefPath = paths.Parse(e.Ref)
}

func (e ExpressionValue) HasLiteralValue() bool {
	return e.Kind == ExpressionKindLiteral
}

func (e ExpressionValue) HasRefValue() bool {
	return e.Kind == ExpressionKindRef
}

func (e ExpressionValue) HasCELValue() bool {
	return e.Kind == ExpressionKindCEL
}

type FlowVariable struct {
	Type        string `yaml:"type"`
	Default     any    `yaml:"default"`
	Description string `yaml:"description"`
}
type EventEmitterRef struct {
	AgentID string `yaml:"agent_id"`
	NodeID  string `yaml:"node_id"`
}
type EventPayloadSpec struct {
	Type       string                    `yaml:"type"`
	Properties map[string]EventFieldSpec `yaml:"properties"`
	Required   []string                  `yaml:"required"`
}
type EventFieldSpec struct {
	Type        string            `yaml:"type"`
	Description string            `yaml:"description"`
	Refinements SchemaRefinements `yaml:"-"`
	Citation    CriteriaCitation  `yaml:"citation"`
}
type CriteriaCitation struct {
	Criteria       string   `yaml:"criteria"`
	AllowedClasses []string `yaml:"allowed_classes"`
}
type SchemaLengthRefinement struct {
	Min *int `yaml:"min"`
	Max *int `yaml:"max"`
}
type SchemaRangeRefinement struct {
	Min *float64 `yaml:"min"`
	Max *float64 `yaml:"max"`
}
type SchemaRefinements struct {
	Pattern string                 `yaml:"pattern"`
	Length  SchemaLengthRefinement `yaml:"length"`
	Range   SchemaRangeRefinement  `yaml:"range"`
	EqualTo string                 `yaml:"equal_to"`
}
type SchemaLiteral struct {
	Node yaml.Node
}
type ToolAdditionalProperties struct {
	Allowed *bool            `yaml:"allowed,omitempty"`
	Schema  *ToolInputSchema `yaml:"schema,omitempty"`
}
type HTTPToolSpec struct {
	Method         string            `yaml:"method"`
	URL            string            `yaml:"url"`
	Headers        map[string]string `yaml:"headers,omitempty"`
	Body           any               `yaml:"body,omitempty"`
	TimeoutSeconds int               `yaml:"timeout_seconds,omitempty"`
}
type ManagedCredentialRef struct {
	Key                 string                                     `yaml:"key"`
	Header              string                                     `yaml:"header,omitempty"`
	Prefix              string                                     `yaml:"prefix,omitempty"`
	GrantType           string                                     `yaml:"grant_type,omitempty"`
	Scopes              []string                                   `yaml:"scopes,omitempty"`
	GrantModel          string                                     `yaml:"grant_model,omitempty"`
	TokenRequest        managedcredentialmodel.TokenRequestProfile `yaml:"token_request,omitempty"`
	InstallationIDInput string                                     `yaml:"installation_id_input,omitempty"`
}
type ToolInputSchema struct {
	Type                 string                     `yaml:"type"`
	Description          string                     `yaml:"description,omitempty"`
	Properties           map[string]ToolInputSchema `yaml:"properties,omitempty"`
	Required             []string                   `yaml:"required,omitempty"`
	Items                *ToolInputSchema           `yaml:"items,omitempty"`
	Enum                 []SchemaLiteral            `yaml:"enum,omitempty"`
	AdditionalProperties ToolAdditionalProperties   `yaml:"additionalProperties,omitempty"`
	Minimum              *float64                   `yaml:"minimum,omitempty"`
	Maximum              *float64                   `yaml:"maximum,omitempty"`
}
type ProjectPackagePaths struct {
	Key               string
	ParentKey         string
	Depth             int
	Dir               string
	PackageFile       string
	ProjectNodesFile  string
	ProjectEventsFile string
	ProjectAgentsFile string
	ProjectToolsFile  string
	ProjectPolicyFile string
	ProjectPromptsDir string
	Flows             []FlowContractPaths
}
type FlowContractPaths struct {
	ID           string
	Flow         string
	Mode         string
	Namespace    string
	PackageKey   string
	PackageDir   string
	Dir          string
	DataDir      string
	SchemaFile   string
	TypesFile    string
	EntitiesFile string
	NodesFile    string
	EventsFile   string
	AgentsFile   string
	ToolsFile    string
	PolicyFile   string
	PromptsDir   string
}
type ProjectPackageDocument struct {
	Name            string               `yaml:"name"`
	Version         string               `yaml:"version"`
	PlatformVersion string               `yaml:"platform_version"`
	Author          string               `yaml:"author"`
	Description     string               `yaml:"description"`
	Keywords        []string             `yaml:"keywords"`
	License         string               `yaml:"license"`
	Repository      string               `yaml:"repository"`
	Extra           map[string]string    `yaml:"extra"`
	Requires        FlowPackageRequires  `yaml:"requires"`
	Flows           []ProjectFlowRef     `yaml:"flows"`
	Packages        []ProjectPackageRef  `yaml:"packages"`
	Children        []ProjectPackageRef  `yaml:"children"`
	Subpackages     []ProjectPackageRef  `yaml:"subpackages"`
	Connect         []FlowPackageConnect `yaml:"connect"`
	ConnectorPacks  ConnectorPackImports `yaml:"connector_packs"`
	Handoffs        []ProjectHandoff     `yaml:"handoffs"`
	EntitySchema    EntitySchema         `yaml:"entity_schema"`
}

type ConnectorPackImports struct {
	Imports []ConnectorPackImport `yaml:"imports"`
}

type ConnectorPackImport struct {
	Provider string `yaml:"provider"`
	Tool     string `yaml:"tool"`
}

type TypeCatalogDocument struct {
	Scalars map[string]ScalarTypeDecl `yaml:"scalars"`
	Enums   map[string]EnumTypeDecl   `yaml:"enums"`
	Types   map[string]NamedTypeDecl  `yaml:"types"`
}

type ScalarTypeDecl struct {
	Base string `yaml:"-"`
}

type EnumTypeDecl struct {
	Values []string `yaml:"-"`
}

type NamedTypeDecl struct {
	Description string                   `yaml:"-"`
	Fields      map[string]TypeFieldSpec `yaml:"-"`
}

type TypeFieldSpec struct {
	Type        string            `yaml:"type"`
	Description string            `yaml:"description"`
	Refinements SchemaRefinements `yaml:"-"`
}

type EntityContractsDocument map[string]EntityContract

type EntityContract struct {
	Description string                     `yaml:"-"`
	Owner       string                     `yaml:"-"`
	Fields      map[string]EntityFieldDecl `yaml:"-"`
}

type EntityFieldDecl struct {
	Type               string            `yaml:"type"`
	Initial            any               `yaml:"initial"`
	Indexed            bool              `yaml:"indexed"`
	Immutable          bool              `yaml:"immutable"`
	Description        string            `yaml:"description"`
	Refinements        SchemaRefinements `yaml:"-"`
	MaterializeFrom    string            `yaml:"materialize_from"`
	Project            map[string]any    `yaml:"project"`
	UnusedReason       string            `yaml:"_unused_reason"`
	UnusedReaderReason string            `yaml:"_unused_reader_reason"`
}
type ProjectPackageRef struct {
	ID      string          `yaml:"id"`
	Path    string          `yaml:"path"`
	Package string          `yaml:"package"`
	Dir     string          `yaml:"dir"`
	Bind    FlowPackageBind `yaml:"bind"`
}
type ProjectFlowRef struct {
	ID         string              `yaml:"id"`
	Flow       string              `yaml:"flow"`
	Namespace  string              `yaml:"namespace"`
	Mode       string              `yaml:"mode"`
	Activation string              `yaml:"activation"`
	Ingress    *ProjectFlowIngress `yaml:"ingress"`
	Bind       FlowPackageBind     `yaml:"bind"`
}

const ProjectFlowActivationStanding = "standing"

func (r ProjectFlowRef) HasStandingActivation() bool {
	return strings.EqualFold(strings.TrimSpace(r.Activation), ProjectFlowActivationStanding)
}

type ProjectFlowIngress struct {
	Alias     string                       `yaml:"alias"`
	Providers []ProjectFlowIngressProvider `yaml:"providers"`
}

type ProjectFlowIngressProvider struct {
	Provider      string                      `yaml:"provider"`
	SigningSecret string                      `yaml:"signing_secret"`
	Admission     ProjectFlowIngressAdmission `yaml:"admission"`
}

type ProjectFlowIngressAdmission struct {
	Kind           string                            `yaml:"kind"`
	Pack           *ProjectFlowIngressAdmissionPack  `yaml:"pack"`
	Acknowledge    string                            `yaml:"acknowledge"`
	Authentication *ProjectFlowIngressAuthentication `yaml:"authentication"`
	Event          string                            `yaml:"event"`
	DeliveryID     *ProjectFlowIngressDeliveryID     `yaml:"delivery_id"`
	Payload        string                            `yaml:"payload"`
}

type ProjectFlowIngressAdmissionPack struct {
	ID string `yaml:"id"`
}

type ProjectFlowIngressAuthentication struct {
	Kind     string `yaml:"kind"`
	Header   string `yaml:"header"`
	Prefix   string `yaml:"prefix"`
	Encoding string `yaml:"encoding"`
}

type ProjectFlowIngressDeliveryID struct {
	Source   string `yaml:"source"`
	Header   string `yaml:"header"`
	JSONPath string `yaml:"json_path"`
}
type FlowPackageRequires struct {
	Inputs          []string               `yaml:"inputs"`
	Outputs         []string               `yaml:"outputs"`
	Policy          []string               `yaml:"policy"`
	PolicyDefaults  map[string]PolicyValue `yaml:"-"`
	Credentials     []string               `yaml:"credentials"`
	PlatformVersion string                 `yaml:"platform_version"`
}
type FlowPackageBind struct {
	Inputs      map[string]string         `yaml:"inputs"`
	Outputs     map[string]string         `yaml:"outputs"`
	Policy      map[string]string         `yaml:"policy"`
	Credentials map[string]string         `yaml:"credentials"`
	Observe     []FlowPackageObserveGrant `yaml:"observe"`
}
type FlowPackageObserveGrant struct {
	Source string   `yaml:"source"`
	Events []string `yaml:"events"`
}
type ProjectHandoff struct {
	Event       string `yaml:"event"`
	CreatesFlow string `yaml:"creates_flow"`
	Namespace   string `yaml:"namespace"`
}
type LoadedProjectPackage struct {
	Key       string
	ParentKey string
	Depth     int
	Paths     ProjectPackagePaths
	Manifest  ProjectPackageDocument
}
type ProjectContractView = flowmodel.PackageView[
	ProjectPackagePaths,
	ProjectPackageDocument,
	SystemNodeContract,
	EventCatalogEntry,
	AgentRegistryEntry,
	ToolSchemaEntry,
]
type FlowContractView = flowmodel.View[
	FlowContractPaths,
	FlowSchemaDocument,
	SystemNodeContract,
	EventCatalogEntry,
	AgentRegistryEntry,
	ToolSchemaEntry,
]
type FlowTree = flowmodel.Tree[FlowContractView]
type ContractItemSource struct {
	PackageKey string
	FlowID     string
	Layer      string
	File       string
}
type FlowSchemaDocument struct {
	Name                   string                          `yaml:"name"`
	Mode                   string                          `yaml:"mode"`
	Entity                 string                          `yaml:"entity"`
	Instance               FlowTemplateInstanceDeclaration `yaml:"instance"`
	InitialState           string                          `yaml:"initial_state"`
	InitialStateDeclared   bool                            `yaml:"-"`
	NamespacePrefix        string                          `yaml:"-"`
	NamespaceRule          string                          `yaml:"-"`
	TerminalStates         []string                        `yaml:"terminal_states"`
	TerminalStatesDeclared bool                            `yaml:"-"`
	States                 []string                        `yaml:"states"`
	StatesDeclared         bool                            `yaml:"-"`
	StageDeclarations      FlowStageDeclarations           `yaml:"stages"`
	LoopDeclarations       FlowLoopDeclarations            `yaml:"loops"`
	Pins                   FlowPins                        `yaml:"pins"`
	ToolSurface            FlowToolSurfaceContract         `yaml:"tool_surface"`
	RequiredAgents         []FlowRequiredAgent             `yaml:"required_agents"`
	RequiredAgentsDeclared bool                            `yaml:"-"`
	InstanceVariables      FlowInstanceVariables           `yaml:"instance_variables"`
	AutoEmitOnCreate       AutoEmitOnCreateContract        `yaml:"auto_emit_on_create"`
}

const (
	FlowModeStatic    = "static"
	FlowModeTemplate  = "template"
	FlowModeSingleton = "singleton"
)

const (
	FlowInputResolutionModeCreate         = "create"
	FlowInputResolutionModeSelect         = "select"
	FlowInputResolutionModeSelectOrCreate = "select-or-create"
	FlowInputResolutionModeFanIn          = "fan-in"
	FlowInputResolutionModeFanOut         = "fan-out"
	FlowInputResolutionModeReply          = "reply"
)

const (
	FlowInputResolutionMintUUID    = "uuid"
	FlowInputResolutionMintEventID = "event_id"
)

type FlowToolSurfaceContract struct {
	RoleScopedEntityTools bool `yaml:"role_scoped_entity_tools"`
}

type FlowTemplateInstanceDeclaration struct {
	Declared           bool     `yaml:"-"`
	By                 []string `yaml:"by"`
	OnMissing          string   `yaml:"on_missing"`
	OnMissingDeclared  bool     `yaml:"-"`
	OnConflict         string   `yaml:"on_conflict"`
	OnConflictDeclared bool     `yaml:"-"`
}

func (i FlowTemplateInstanceDeclaration) Empty() bool {
	return !i.Declared && len(i.By) == 0 && strings.TrimSpace(i.OnMissing) == "" && !i.OnMissingDeclared && strings.TrimSpace(i.OnConflict) == "" && !i.OnConflictDeclared
}

type TemplateInstanceContract struct {
	FlowID        string
	By            []string
	OnMissing     string
	OnConflict    string
	PrimaryEntity PrimaryEntityContract
}

type SingletonCoordinatorContract struct {
	FlowID         string
	PrimaryEntity  PrimaryEntityContract
	ContainedState []SingletonCoordinatorContainedField
}

type SingletonCoordinatorContainedField struct {
	Name string
	Type string
	Kind string
}

type TemplateInstanceKeyValue struct {
	Field string
	Value string
}

func (c TemplateInstanceContract) CanonicalKeyMaterial(values map[string]any) ([]TemplateInstanceKeyValue, error) {
	if len(c.By) == 0 {
		return nil, fmt.Errorf("INVALID-TEMPLATE-INSTANCE: flow %s instance.by is required", defaultPrimaryEntityFlowLabel(c.FlowID))
	}
	out := make([]TemplateInstanceKeyValue, 0, len(c.By))
	for _, field := range c.By {
		field = strings.TrimSpace(field)
		if field == "" {
			return nil, fmt.Errorf("INVALID-TEMPLATE-INSTANCE: flow %s instance.by contains an empty field", defaultPrimaryEntityFlowLabel(c.FlowID))
		}
		value, ok := values[field]
		if !ok || value == nil {
			return nil, fmt.Errorf("INVALID-TEMPLATE-INSTANCE: flow %s instance key field %q is missing", defaultPrimaryEntityFlowLabel(c.FlowID), field)
		}
		valueText := strings.TrimSpace(fmt.Sprint(value))
		if valueText == "" {
			return nil, fmt.Errorf("INVALID-TEMPLATE-INSTANCE: flow %s instance key field %q is empty", defaultPrimaryEntityFlowLabel(c.FlowID), field)
		}
		out = append(out, TemplateInstanceKeyValue{Field: field, Value: valueText})
	}
	return out, nil
}

type FlowInstanceVariables struct {
	Description string                  `yaml:"description"`
	Variables   map[string]FlowVariable `yaml:"variables"`
}
type AutoEmitOnCreateContract struct {
	Event       string `yaml:"event"`
	Description string `yaml:"description"`
}
type FlowPins struct {
	Inputs  FlowInputPins  `yaml:"inputs"`
	Outputs FlowOutputPins `yaml:"outputs"`
}
type FlowInputPins struct {
	Events    []string            `yaml:"events"`
	EventPins []FlowInputEventPin `yaml:"-"`
	Reads     []string            `yaml:"reads"`
}
type FlowOutputPins struct {
	Events    []string             `yaml:"events"`
	EventPins []FlowOutputEventPin `yaml:"-"`
	Writes    []string             `yaml:"writes"`
}
type FlowInputEventPin struct {
	Name       string                 `yaml:"name"`
	Event      string                 `yaml:"event"`
	Source     string                 `yaml:"source"`
	Address    *FlowInputPinAddress   `yaml:"address"`
	Resolution FlowInputPinResolution `yaml:"resolution"`
	Carries    FlowInputPinCarries    `yaml:"carries"`
}
type FlowOutputEventPin struct {
	Name    string   `yaml:"name"`
	Event   string   `yaml:"event"`
	Key     string   `yaml:"key"`
	Carries []string `yaml:"carries"`
}
type FlowInputPinCarries map[string]FlowInputPinCarry
type FlowInputPinCarry struct {
	From     string `yaml:"from"`
	Type     string `yaml:"type"`
	Optional bool   `yaml:"optional,omitempty"`
	Convert  string `yaml:"convert,omitempty"`
}
type FlowInputPinAddress struct {
	By          string `yaml:"by"`
	Source      string `yaml:"source"`
	Target      string `yaml:"target"`
	Cardinality string `yaml:"cardinality"`
	Mode        string `yaml:"mode"`
}
type FlowInputPinResolution struct {
	Mode           string                            `yaml:"mode"`
	InstanceKey    FlowInputPinResolutionInstanceKey `yaml:"instance_key"`
	Aggregation    string                            `yaml:"aggregation"`
	Window         string                            `yaml:"window"`
	DedupBy        []string                          `yaml:"dedup_by"`
	Singleton      string                            `yaml:"singleton"`
	RepliesTo      string                            `yaml:"replies_to"`
	CorrelationKey string                            `yaml:"correlation_key"`
}
type FlowInputPinResolutionInstanceKey struct {
	From string `yaml:"from"`
	Mint string `yaml:"mint"`
	As   string `yaml:"as"`
}
type FlowPackageConnect struct {
	PackageKey string                           `yaml:"-"`
	SourceFile string                           `yaml:"-"`
	SourceLine int                              `yaml:"-"`
	From       string                           `yaml:"from"`
	To         string                           `yaml:"to"`
	Adapter    string                           `yaml:"adapter"`
	Using      FlowPackageConnectUsing          `yaml:"using"`
	Map        map[string]FlowPackageConnectMap `yaml:"map"`
	Delivery   string                           `yaml:"delivery"`
	Reply      map[string]string                `yaml:"reply"`
}
type FlowPackageConnectUsing struct {
	Instance FlowPackageConnectInstanceAdapter `yaml:"instance"`
}
type FlowPackageConnectInstanceAdapter struct {
	Declared bool     `yaml:"-"`
	Source   []string `yaml:"source"`
	Target   []string `yaml:"target"`
}
type FlowPackageConnectMap struct {
	Source string `yaml:"source"`
	Target string `yaml:"target"`
}
type FlowPackagePinRef struct {
	Root   bool
	FlowID string
	Pin    string
}
type FlowRequiredAgent struct {
	Role         string   `yaml:"role"`
	SubscribesTo []string `yaml:"subscribes_to"`
	Emits        []string `yaml:"emits"`
	Description  string   `yaml:"description"`
}
type RequiredAgentFact struct {
	Role         string   `json:"role"`
	SubscribesTo []string `json:"subscribes_to,omitempty"`
	Emits        []string `json:"emits,omitempty"`
	Description  string   `json:"description,omitempty"`
	Source       string   `json:"source"`
	SourceFile   string   `json:"source_file,omitempty"`
}
type WorkflowStageContract struct {
	ID          string `yaml:"id"`
	Phase       string `yaml:"phase"`
	Description string `yaml:"description"`
}
type WorkflowTransitionContract struct {
	ID                string                   `yaml:"id"`
	From              []string                 `yaml:"from"`
	To                string                   `yaml:"to"`
	Trigger           string                   `yaml:"trigger"`
	Node              string                   `yaml:"node"`
	Guards            []string                 `yaml:"guards"`
	Actions           []string                 `yaml:"actions"`
	DataAccumulation  WorkflowDataAccumulation `yaml:"data_accumulation"`
	AllowTerminalExit bool                     `yaml:"allow_terminal_exit"`
}
type WorkflowDataAccumulation struct {
	Writes      []WorkflowDataWrite `yaml:"writes"`
	SourceEvent string              `yaml:"source_event"`
}
type WorkflowDataWrite struct {
	Field         string                `yaml:"-" json:"field,omitempty"`
	SourceField   string                `yaml:"source_field,omitempty" json:"source_field,omitempty"`
	SourcePath    paths.Path            `yaml:"-" json:"-"`
	Operation     WorkflowDataOperation `yaml:"op,omitempty" json:"op,omitempty"`
	TargetRef     string                `yaml:"target,omitempty" json:"target,omitempty"`
	TargetField   string                `yaml:"target_field,omitempty" json:"target_field,omitempty"`
	TargetPathRef string                `yaml:"target_path,omitempty" json:"target_path,omitempty"`
	TargetPath    paths.Path            `yaml:"-" json:"-"`
	Value         ExpressionValue       `yaml:"value,omitempty" json:"value,omitempty"`
	Key           ExpressionValue       `yaml:"key,omitempty" json:"key,omitempty"`
	Index         ExpressionValue       `yaml:"index,omitempty" json:"index,omitempty"`
}

func (w WorkflowDataWrite) Source() string {
	switch {
	case strings.TrimSpace(w.SourceField) != "":
		return strings.TrimSpace(w.SourceField)
	case strings.TrimSpace(w.Field) != "":
		return strings.TrimSpace(w.Field)
	default:
		return strings.TrimSpace(w.TargetField)
	}
}
func (w WorkflowDataWrite) Target() string {
	switch {
	case strings.TrimSpace(w.TargetRef) != "":
		return strings.TrimSpace(w.TargetRef)
	case strings.TrimSpace(w.TargetPathRef) != "":
		return strings.TrimSpace(w.TargetPathRef)
	case strings.TrimSpace(w.TargetField) != "":
		return strings.TrimSpace(w.TargetField)
	case strings.TrimSpace(w.Field) != "":
		return strings.TrimSpace(w.Field)
	default:
		return strings.TrimSpace(w.SourceField)
	}
}
func (w WorkflowDataWrite) HasLiteralValue() bool {
	return w.Value.HasLiteralValue()
}

func (w WorkflowDataWrite) IsContainedOperation() bool {
	return strings.TrimSpace(string(w.Operation)) != ""
}

func (w WorkflowDataWrite) SourceExpression() ExpressionValue {
	if !w.Value.IsZero() {
		return w.Value
	}
	source := strings.TrimSpace(w.Source())
	if source == "" {
		return ExpressionValue{}
	}
	expr := RefExpression("payload." + source)
	return expr
}
func (a WorkflowDataAccumulation) TargetFields() []string {
	if len(a.Writes) == 0 {
		return nil
	}
	out := make([]string, 0, len(a.Writes))
	for _, write := range a.Writes {
		if target := strings.TrimSpace(write.Target()); target != "" {
			out = append(out, target)
		}
	}
	return out
}
func (a WorkflowDataAccumulation) HasWrites() bool {
	return len(a.Writes) > 0
}

type WorkflowTimerContract struct {
	ID           string `yaml:"id"`
	Stage        string `yaml:"stage"`
	Event        string `yaml:"event"`
	Owner        string `yaml:"owner"`
	FlowID       string `yaml:"-"`
	NodeID       string `yaml:"-"`
	StageOwned   bool   `yaml:"-"`
	AdvancesTo   string `yaml:"-"`
	Action       string `yaml:"action"`
	Cancellation string `yaml:"cancellation"`
	Delay        string `yaml:"delay"`
	StartOn      string `yaml:"start_on"`
	CancelOn     string `yaml:"cancel_on"`
	Recurring    bool   `yaml:"recurring"`
}
type EventEmission struct {
	Single string   `yaml:"-" json:"single,omitempty"`
	Many   []string `yaml:"-" json:"many,omitempty"`
}

func (e EventEmission) First() string {
	if strings.TrimSpace(e.Single) != "" {
		return strings.TrimSpace(e.Single)
	}
	if len(e.Many) > 0 {
		return strings.TrimSpace(e.Many[0])
	}
	return ""
}
func (e EventEmission) Values() []string {
	if len(e.Many) > 0 {
		return append([]string{}, normalizeStrings(e.Many)...)
	}
	if first := strings.TrimSpace(e.Single); first != "" {
		return []string{first}
	}
	return nil
}
func (e EventEmission) Empty() bool {
	return len(e.Values()) == 0
}

type EmitSpec struct {
	Event     string                     `yaml:"event"`
	From      string                     `yaml:"from,omitempty"`
	Fields    map[string]ExpressionValue `yaml:"fields"`
	Target    EmitTargetSpec             `yaml:"target"`
	Broadcast bool                       `yaml:"broadcast"`
}

func (e EmitSpec) EventType() string {
	return strings.TrimSpace(e.Event)
}

func (e EmitSpec) Empty() bool {
	return strings.TrimSpace(e.Event) == "" && strings.TrimSpace(e.From) == "" && len(e.Fields) == 0 && e.Target.Empty() && !e.Broadcast
}

func (e EmitSpec) HasFields() bool {
	return len(e.Fields) > 0
}

func (e EmitSpec) HasTarget() bool {
	return !e.Target.Empty()
}

type EmitTargetKind string

const (
	EmitTargetKindSender     EmitTargetKind = "sender"
	EmitTargetKindInstanceID EmitTargetKind = "instance_id"
	EmitTargetKindFlowMatch  EmitTargetKind = "flow_match"
)

type EmitTargetSpec struct {
	Kind       EmitTargetKind             `yaml:"-"`
	InstanceID string                     `yaml:"instance_id,omitempty"`
	Flow       string                     `yaml:"flow,omitempty"`
	Match      map[string]ExpressionValue `yaml:"match,omitempty"`
}

func (t EmitTargetSpec) Empty() bool {
	t = t.Normalized()
	return t.Kind == "" && t.InstanceID == "" && t.Flow == "" && len(t.Match) == 0
}

func (t EmitTargetSpec) Normalized() EmitTargetSpec {
	t.Kind = EmitTargetKind(strings.TrimSpace(string(t.Kind)))
	t.InstanceID = strings.TrimSpace(t.InstanceID)
	t.Flow = strings.TrimSpace(t.Flow)
	if len(t.Match) > 0 {
		out := make(map[string]ExpressionValue, len(t.Match))
		for key, value := range t.Match {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			value.hydrate()
			out[key] = value
		}
		t.Match = out
	}
	switch {
	case t.Kind == "" && t.InstanceID != "":
		t.Kind = EmitTargetKindInstanceID
	case t.Kind == "" && (t.Flow != "" || len(t.Match) > 0):
		t.Kind = EmitTargetKindFlowMatch
	}
	return t
}

func cloneExpressionValueMap(in map[string]ExpressionValue) map[string]ExpressionValue {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]ExpressionValue, len(in))
	for key, value := range in {
		value.hydrate()
		out[key] = value
	}
	return out
}

func cloneEmitSpec(spec EmitSpec) EmitSpec {
	target := spec.Target.Normalized()
	return EmitSpec{
		Event:     strings.TrimSpace(spec.Event),
		From:      strings.TrimSpace(spec.From),
		Fields:    cloneExpressionValueMap(spec.Fields),
		Target:    cloneEmitTargetSpec(target),
		Broadcast: spec.Broadcast,
	}
}

func cloneEmitTargetSpec(spec EmitTargetSpec) EmitTargetSpec {
	spec = spec.Normalized()
	return EmitTargetSpec{
		Kind:       spec.Kind,
		InstanceID: strings.TrimSpace(spec.InstanceID),
		Flow:       strings.TrimSpace(spec.Flow),
		Match:      cloneExpressionValueMap(spec.Match),
	}
}

type GuardActionEntry struct {
	ID              string `yaml:"id"`
	Category        string `yaml:"category"`
	Description     string `yaml:"description"`
	Input           string `yaml:"input"`
	Check           string `yaml:"check"`
	PolicyRef       string `yaml:"policy_ref"`
	PlatformBuiltin string `yaml:"platform_builtin"`
	Effect          string `yaml:"effect"`
	Emits           string `yaml:"emits"`
}
type SystemNodeContract struct {
	ID               string                            `yaml:"id"`
	Description      string                            `yaml:"description"`
	ExecutionType    string                            `yaml:"execution_type"`
	Implementation   string                            `yaml:"-"`
	SubscribesTo     []string                          `yaml:"subscribes_to"`
	Produces         []string                          `yaml:"produces"`
	ProducesDeclared bool                              `yaml:"-" json:"-"`
	OwnedTransitions []string                          `yaml:"-"`
	StateTable       string                            `yaml:"state_table"`
	IdempotencyTable string                            `yaml:"-"`
	Timers           []WorkflowTimerContract           `yaml:"timers"`
	EventHandlers    map[string]SystemNodeEventHandler `yaml:"event_handlers"`
	StateSchema      NodeStateSchema                   `yaml:"state_schema"`
	GateState        NodeGateStateSchema               `yaml:"gate_state"`
}
type SystemNodeEventHandler struct {
	Action               ActionSpec                `yaml:"action"`
	Activity             ActivitySpec              `yaml:"activity"`
	CreateEntity         bool                      `yaml:"create_entity"`
	SelectEntity         *SelectEntitySpec         `yaml:"select_entity"`
	SelectOrCreateEntity *SelectOrCreateEntitySpec `yaml:"select_or_create_entity"`
	Description          string                    `yaml:"description"`
	EvidenceTarget       string                    `yaml:"evidence_target"`
	Emit                 EmitSpec                  `yaml:"emit"`
	OnSuccess            HandlerOnSuccessSpec      `yaml:"on_success"`
	Guard                *GuardSpec                `yaml:"guard"`
	AdvancesTo           string                    `yaml:"advances_to"`
	SetsGate             *GateSpec                 `yaml:"sets_gate"`
	ClearGates           []string                  `yaml:"clear_gates"`
	DataAccumulation     WorkflowDataAccumulation  `yaml:"data_accumulation"`
	Condition            string                    `yaml:"condition"`
	Logic                string                    `yaml:"logic"`
	Loop                 *LoopOperationSpec        `yaml:"loop"`
	OnComplete           []HandlerRuleEntry        `yaml:"on_complete"`
	Rules                []HandlerRuleEntry        `yaml:"rules"`
	Accumulate           *AccumulateSpec           `yaml:"accumulate"`
	Join                 *JoinSpec                 `yaml:"join"`
	Compute              *ComputeSpec              `yaml:"compute"`
	Query                *QuerySpec                `yaml:"query"`
	FanOut               *FanOutSpec               `yaml:"fan_out"`
	GroupBy              *GroupBySpec              `yaml:"group_by"`
	Filter               *FilterSpec               `yaml:"filter"`
	Reduce               *ReduceSpec               `yaml:"reduce"`
	Count                *CountSpec                `yaml:"count"`
	Clear                *ClearSpec                `yaml:"clear"`
}
type EventCatalogEntry struct {
	Swarm             EventSwarmMetadata `yaml:"swarm"`
	Note              string             `yaml:"_note"`
	Emitter           EventEmitterRef    `yaml:"emitter"`
	EmitterType       string             `yaml:"emitter_type"`
	Producer          []string           `yaml:"producer"`
	AlternateEmitters []string           `yaml:"alternate_emitters"`
	Consumer          []string           `yaml:"consumer"`
	ConsumerType      []string           `yaml:"consumer_type"`
	Source            string             `yaml:"_source"`
	Status            string             `yaml:"_status"`
	Intercepted       bool               `yaml:"intercepted"`
	Passthrough       bool               `yaml:"passthrough"`
	RuntimeHandling   string             `yaml:"runtime_handling"`
	OwningNode        string             `yaml:"owning_node"`
	DeliveryChannel   string             `yaml:"delivery_channel"`
	Payload           EventPayloadSpec   `yaml:"payload"`
	Required          []string           `yaml:"required"`
}
type EventSwarmMetadata struct {
	Note     string   `yaml:"note,omitempty"`
	Source   string   `yaml:"source,omitempty"`
	Producer []string `yaml:"producer,omitempty"`
	Consumer []string `yaml:"consumer,omitempty"`
	Status   string   `yaml:"status,omitempty"`
}

func (e EventCatalogEntry) SwarmNote() string {
	return strings.TrimSpace(e.Swarm.Note)
}

func (e EventCatalogEntry) SwarmSource() string {
	return strings.TrimSpace(e.Swarm.Source)
}

func (e EventCatalogEntry) SwarmProducer() []string {
	return normalizeStrings(e.Swarm.Producer)
}

func (e EventCatalogEntry) SwarmConsumer() []string {
	return normalizeStrings(e.Swarm.Consumer)
}

func (e EventCatalogEntry) SwarmStatus() string {
	return strings.TrimSpace(e.Swarm.Status)
}

type AgentRegistryEntry struct {
	ID                     string                          `yaml:"id"`
	Type                   string                          `yaml:"type"`
	Role                   string                          `yaml:"role"`
	PromptRef              string                          `yaml:"prompt_ref"`
	EntityWrites           map[string]AgentEntityWriteDecl `yaml:"entity_writes"`
	Permissions            []string                        `yaml:"permissions" json:"permissions,omitempty"`
	PermissionsBundle      string                          `yaml:"permissions_bundle" json:"permissions_bundle,omitempty"`
	WorkspaceClass         string                          `yaml:"workspace_class"`
	ManagerFallback        string                          `yaml:"manager_fallback"`
	NodeType               string                          `yaml:"node_type"`
	Model                  string                          `yaml:"model"`
	Mode                   string                          `yaml:"mode"`
	ConversationMode       string                          `yaml:"conversation_mode"`
	SessionScope           string                          `yaml:"session_scope"`
	MaxTurnsPerTask        int                             `yaml:"max_turns_per_task"`
	PromptInputs           []string                        `yaml:"prompt_inputs" json:"prompt_inputs,omitempty"`
	Subscriptions          []string                        `yaml:"subscriptions"`
	SubscriptionsBootstrap []string                        `yaml:"subscriptions_bootstrap"`
	SubscribesTo           []string                        `yaml:"subscribes_to"`
	Tools                  []string                        `yaml:"tools"`
	ToolsTier2             []string                        `yaml:"tools_tier2"`
	NativeTools            map[string]any                  `yaml:"native_tools"`
	FlowDataAccess         []string                        `yaml:"flow_data_access" json:"flow_data_access,omitempty"`
	Criteria               []string                        `yaml:"criteria" json:"criteria,omitempty"`
	EmitEvents             []string                        `yaml:"emit_events"`
	Implementation         string                          `yaml:"implementation"`
	AuthoredFields         map[string]bool                 `yaml:"-" json:"-"`
	EffectiveFieldSources  map[string]string               `yaml:"-" json:"-"`
}

type agentRegistryEntryYAML AgentRegistryEntry

func (e *AgentRegistryEntry) UnmarshalYAML(value *yaml.Node) error {
	authoredFields, err := validateAgentRegistryEntryYAMLFields(value)
	if err != nil {
		return err
	}
	if authoredFields["max_turns_per_task"] {
		for i := 0; value != nil && value.Kind == yaml.MappingNode && i+1 < len(value.Content); i += 2 {
			if strings.TrimSpace(value.Content[i].Value) != "max_turns_per_task" {
				continue
			}
			if strings.TrimSpace(value.Content[i+1].Value) == "" {
				return fmt.Errorf("agent field max_turns_per_task must be positive when authored")
			}
		}
	}
	var decoded agentRegistryEntryYAML
	if err := value.Decode(&decoded); err != nil {
		return err
	}
	if authoredFields["max_turns_per_task"] && decoded.MaxTurnsPerTask <= 0 {
		return fmt.Errorf("agent field max_turns_per_task must be positive when authored")
	}
	if authoredFields["mode"] {
		mode, scope, err := runtimesessions.ResolveAuthoredAgentMemoryMode(decoded.Mode)
		if err != nil {
			return fmt.Errorf("agent field mode: %w", err)
		}
		decoded.Mode = mode.String()
		decoded.ConversationMode = mode.String()
		decoded.SessionScope = scope.String()
	}
	*e = AgentRegistryEntry(decoded)
	e.AuthoredFields = authoredFields
	return nil
}

func validateAgentRegistryEntryYAMLFields(value *yaml.Node) (map[string]bool, error) {
	authoredFields := map[string]bool{}
	if value != nil && value.Kind == yaml.MappingNode {
		for i := 0; i+1 < len(value.Content); i += 2 {
			field := strings.TrimSpace(value.Content[i].Value)
			authoredFields[field] = true
			switch field {
			case "model_tier":
				return nil, fmt.Errorf("RETIRED: agent field model_tier is retired; use model")
			case "conversation_mode":
				return nil, fmt.Errorf("RETIRED: agent field conversation_mode is retired; use mode")
			case "session_scope":
				return nil, fmt.Errorf("RETIRED: agent field session_scope is runtime-derived from mode")
			case "session_scope_authority":
				return nil, fmt.Errorf("RETIRED: agent field session_scope_authority is platform-internal runtime state")
			case "tools_tier2":
				return nil, fmt.Errorf("RETIRED: agent field tools_tier2 is retired; use tools")
			case "subscriptions_bootstrap":
				return nil, fmt.Errorf("RETIRED: agent field subscriptions_bootstrap is retired; use subscriptions")
			case "subscribes_to":
				return nil, fmt.Errorf("RETIRED: agent field subscribes_to is retired for agents.yaml; use subscriptions")
			case "profile", "agent_defaults", "agent_profiles", "runtime_id_template":
				return nil, fmt.Errorf("UNSUPPORTED: agent field %s is reserved for future agent-defaults/profile support and is not accepted by Layer 1 platform defaults", field)
			default:
				if !supportedAgentRegistryEntryField(field) {
					return nil, NewUndefinedFieldDiagnostic("agent", field, agentRegistryEntryFieldOptions)
				}
			}
		}
	}
	return authoredFields, nil
}

var agentRegistryEntryFieldOptions = map[string]struct{}{
	"id":                 {},
	"type":               {},
	"role":               {},
	"prompt_ref":         {},
	"entity_writes":      {},
	"permissions":        {},
	"permissions_bundle": {},
	"workspace_class":    {},
	"manager_fallback":   {},
	"node_type":          {},
	"model":              {},
	"mode":               {},
	"max_turns_per_task": {},
	"subscriptions":      {},
	"prompt_inputs":      {},
	"tools":              {},
	"native_tools":       {},
	"flow_data_access":   {},
	"criteria":           {},
	"emit_events":        {},
	"implementation":     {},
}

func supportedAgentRegistryEntryField(field string) bool {
	_, ok := agentRegistryEntryFieldOptions[strings.TrimSpace(field)]
	return ok
}

func (e AgentRegistryEntry) ConfiguredTools() []string {
	if len(e.Tools) > 0 {
		return append([]string{}, e.Tools...)
	}
	return nil
}

type ToolSchemaEntry struct {
	Category           string                `yaml:"category,omitempty"`
	Description        string                `yaml:"description,omitempty"`
	HandlerType        string                `yaml:"handler_type,omitempty"`
	EffectClass        string                `yaml:"effect_class,omitempty"`
	Permission         string                `yaml:"permission,omitempty"`
	RequiredPermission string                `yaml:"required_permission,omitempty"`
	RateLimit          string                `yaml:"rate_limit,omitempty"`
	RateLimitMaxWait   string                `yaml:"rate_limit_max_wait,omitempty"`
	InputSchema        ToolInputSchema       `yaml:"input_schema,omitempty"`
	OutputSchema       ToolInputSchema       `yaml:"output_schema,omitempty"`
	HTTP               *HTTPToolSpec         `yaml:"http,omitempty"`
	ResponseMapping    map[string]any        `yaml:"response_mapping,omitempty"`
	ResponseSuccess    *HTTPResponseSuccess  `yaml:"response_success,omitempty"`
	Credentials        []string              `yaml:"credentials,omitempty"`
	ManagedCredential  *ManagedCredentialRef `yaml:"managed_credential,omitempty"`
}

type HTTPResponseSuccess struct {
	Kind   string `yaml:"kind"`
	Path   string `yaml:"path,omitempty"`
	Equals any    `yaml:"equals,omitempty"`
}
type PlatformSpecDocument struct {
	Platform struct {
		Name    string `yaml:"name"`
		Version string `yaml:"version"`
	} `yaml:"platform"`
	PlatformEvents struct {
		Catalog map[string]yaml.Node `yaml:"catalog"`
	} `yaml:"platform_events"`
	PermissionsModel struct {
		Permissions []string `yaml:"permissions"`
	} `yaml:"permissions_model"`
	Vocabulary struct {
		Participant struct {
			Types map[string]struct {
				Execution string `yaml:"execution"`
			} `yaml:"types"`
		} `yaml:"participant"`
	} `yaml:"vocabulary"`
	ContractFormats yaml.Node `yaml:"contract_formats"`
	WorkflowState   struct {
		DDL    string `yaml:"ddl"`
		Fields map[string]struct {
			Type string `yaml:"type"`
		} `yaml:"fields"`
	} `yaml:"workflow_state"`
	PlatformTables struct {
		Tables map[string]struct {
			Description string `yaml:"description"`
			DDL         string `yaml:"ddl"`
		} `yaml:"tables"`
	} `yaml:"platform_tables"`
	BuiltinHooks struct {
		Guards []struct {
			ID string `yaml:"id"`
		} `yaml:"guards"`
		Actions []struct {
			ID string `yaml:"id"`
		} `yaml:"actions"`
	} `yaml:"builtin_hooks"`
	APISpecification yaml.Node `yaml:"api_specification"`
	ComplianceRules  yaml.Node `yaml:"compliance_rules"`
	FileLayout       struct {
		MigrationNote string `yaml:"migration_note"`
	} `yaml:"file_layout"`
}

func (s ConfigFromSpec) ConfigEntries() []ConfigBinding {
	out := make([]ConfigBinding, 0, len(s.Bindings))
	for key, value := range s.Bindings {
		cleanKey := strings.TrimSpace(key)
		cleanValue := strings.TrimSpace(value)
		if cleanKey == "" || cleanValue == "" {
			continue
		}
		out = append(out, ConfigBinding{
			Key:     cleanKey,
			Ref:     cleanValue,
			RefPath: paths.Parse(cleanValue),
		})
	}
	return out
}
