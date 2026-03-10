package contracts

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

type ContractPaths struct {
	ContractsRoot         string
	WorkflowDir           string
	ProjectPackageFile    string
	ProjectPackages       []ProjectPackagePaths
	ProjectNodesFile      string
	ProjectEventsFile     string
	ProjectAgentsFile     string
	ProjectToolsFile      string
	ProjectPolicyFile     string
	ProjectPromptsDir     string
	WorkflowSchemaFile    string
	GuardRegistryFile     string
	SystemNodesFile       string
	EventCatalogFile      string
	AgentRegistryFile     string
	ToolSchemasFile       string
	PolicyFile            string
	PromptsDir            string
	PlatformSpecFile      string
	VerificationGatesFile string
	ToolingLockFile       string
	DDLFile               string
	AgentConfigMapFile    string
	RuntimeBridge         RuntimeBridgePaths
	Flows                 []FlowContractPaths
}

type WorkflowContractBundle struct {
	Paths            ContractPaths
	Package          ProjectPackageDocument
	PackageTree      []LoadedProjectPackage
	ProjectContracts map[string]ProjectContractView
	FlowContracts    map[string]FlowContractView
	MergedNodes      map[string]SystemNodeContract
	MergedEvents     map[string]EventCatalogEntry
	MergedAgents     map[string]AgentRegistryEntry
	MergedTools      map[string]ToolSchemaEntry
	MergedPolicy     map[string]any
	NodeSources      map[string]ContractItemSource
	EventSources     map[string]ContractItemSource
	AgentSources     map[string]ContractItemSource
	ToolSources      map[string]ContractItemSource
	PolicySources    map[string]ContractItemSource
	Semantics        WorkflowSemanticView
	Workflow         WorkflowSchemaDocument
	Hooks            GuardActionRegistryDocument
	Nodes            map[string]SystemNodeContract
	Events           map[string]EventCatalogEntry
	Agents           map[string]AgentRegistryEntry
	Tools            map[string]ToolSchemaEntry
	Policy           map[string]any
	Platform         PlatformSpecDocument
	FlowSchemas      map[string]FlowSchemaDocument
}

type WorkflowSemanticView struct {
	Name           string
	Version        string
	InitialStage   string
	EntitySchema   map[string]any
	Stages         []WorkflowStageContract
	TerminalStages []string
	Transitions    []WorkflowTransitionContract
	Timers         []WorkflowTimerContract
	Guards         []GuardActionEntry
	Actions        []GuardActionEntry
	GuardByID      map[string]GuardActionEntry
	ActionByID     map[string]GuardActionEntry
	FlowInitial    map[string]string
	FlowStates     map[string][]string
	FlowTerminal   map[string][]string
	FlowNamespace  map[string]string
	FlowPrefix     map[string]string
	FlowRules      map[string]string
	FlowInputs     map[string][]string
	FlowOutputs    map[string][]string
	FlowReads      map[string][]string
	FlowWrites     map[string][]string
	FlowAgents     map[string][]FlowRequiredAgent
	WritePinOwners map[string][]string
	NodeHandlers   map[string]map[string]SystemNodeEventHandler
	EventOwners    map[string][]string
	HandlerTransitions []HandlerTransitionSemantic
	HandlerTransitionIndex map[string]map[string]HandlerTransitionSemantic
}

type HandlerTransitionSemantic struct {
	ID               string
	NodeID           string
	FlowID           string
	EventType        string
	Action           string
	Guard            any
	AdvancesTo       string
	SetsGate         any
	DataAccumulation WorkflowDataAccumulation
	Emits            string
	Condition        string
	CompletionRule   string
	OnComplete       map[string]any
	Rules            map[string]any
}

func (b *WorkflowContractBundle) WorkflowName() string {
	if b == nil {
		return ""
	}
	return strings.TrimSpace(b.Semantics.Name)
}

func (b *WorkflowContractBundle) WorkflowVersion() string {
	if b == nil {
		return ""
	}
	return strings.TrimSpace(b.Semantics.Version)
}

func (b *WorkflowContractBundle) WorkflowEntitySchema() map[string]any {
	if b == nil {
		return nil
	}
	return b.Semantics.EntitySchema
}

func (b *WorkflowContractBundle) WorkflowStages() []WorkflowStageContract {
	if b == nil {
		return nil
	}
	return b.Semantics.Stages
}

func (b *WorkflowContractBundle) WorkflowTerminalStages() []string {
	if b == nil {
		return nil
	}
	return b.Semantics.TerminalStages
}

func (b *WorkflowContractBundle) WorkflowTransitions() []WorkflowTransitionContract {
	if b == nil {
		return nil
	}
	return b.Semantics.Transitions
}

func (b *WorkflowContractBundle) WorkflowInitialStage() string {
	if b == nil {
		return ""
	}
	return strings.TrimSpace(b.Semantics.InitialStage)
}

func (b *WorkflowContractBundle) WorkflowTimers() []WorkflowTimerContract {
	if b == nil {
		return nil
	}
	return b.Semantics.Timers
}

func (b *WorkflowContractBundle) GuardEntries() []GuardActionEntry {
	if b == nil {
		return nil
	}
	return b.Semantics.Guards
}

func (b *WorkflowContractBundle) ActionEntries() []GuardActionEntry {
	if b == nil {
		return nil
	}
	return b.Semantics.Actions
}

func (b *WorkflowContractBundle) GuardEntryByID(id string) (GuardActionEntry, bool) {
	id = strings.TrimSpace(id)
	if b == nil || id == "" {
		return GuardActionEntry{}, false
	}
	entry, ok := b.Semantics.GuardByID[id]
	return entry, ok
}

func (b *WorkflowContractBundle) ActionEntryByID(id string) (GuardActionEntry, bool) {
	id = strings.TrimSpace(id)
	if b == nil || id == "" {
		return GuardActionEntry{}, false
	}
	entry, ok := b.Semantics.ActionByID[id]
	return entry, ok
}

func (b *WorkflowContractBundle) FlowInitialStage(flowID string) string {
	if b == nil {
		return ""
	}
	return strings.TrimSpace(b.Semantics.FlowInitial[strings.TrimSpace(flowID)])
}

func (b *WorkflowContractBundle) FlowStates(flowID string) []string {
	if b == nil {
		return nil
	}
	return append([]string{}, b.Semantics.FlowStates[strings.TrimSpace(flowID)]...)
}

func (b *WorkflowContractBundle) FlowTerminalStages(flowID string) []string {
	if b == nil {
		return nil
	}
	return append([]string{}, b.Semantics.FlowTerminal[strings.TrimSpace(flowID)]...)
}

func (b *WorkflowContractBundle) FlowNamespace(flowID string) string {
	if b == nil {
		return ""
	}
	return strings.TrimSpace(b.Semantics.FlowNamespace[strings.TrimSpace(flowID)])
}

func (b *WorkflowContractBundle) FlowNamespacePrefix(flowID string) string {
	if b == nil {
		return ""
	}
	return strings.TrimSpace(b.Semantics.FlowPrefix[strings.TrimSpace(flowID)])
}

func (b *WorkflowContractBundle) FlowNamespaceRule(flowID string) string {
	if b == nil {
		return ""
	}
	return strings.TrimSpace(b.Semantics.FlowRules[strings.TrimSpace(flowID)])
}

func (b *WorkflowContractBundle) FlowInputEvents(flowID string) []string {
	if b == nil {
		return nil
	}
	return append([]string{}, b.Semantics.FlowInputs[strings.TrimSpace(flowID)]...)
}

func (b *WorkflowContractBundle) FlowOutputEvents(flowID string) []string {
	if b == nil {
		return nil
	}
	return append([]string{}, b.Semantics.FlowOutputs[strings.TrimSpace(flowID)]...)
}

func (b *WorkflowContractBundle) FlowReadPins(flowID string) []string {
	if b == nil {
		return nil
	}
	return append([]string{}, b.Semantics.FlowReads[strings.TrimSpace(flowID)]...)
}

func (b *WorkflowContractBundle) FlowWritePins(flowID string) []string {
	if b == nil {
		return nil
	}
	return append([]string{}, b.Semantics.FlowWrites[strings.TrimSpace(flowID)]...)
}

func (b *WorkflowContractBundle) FlowRequiredAgents(flowID string) []FlowRequiredAgent {
	if b == nil {
		return nil
	}
	agents := b.Semantics.FlowAgents[strings.TrimSpace(flowID)]
	out := make([]FlowRequiredAgent, len(agents))
	copy(out, agents)
	return out
}

func (b *WorkflowContractBundle) WritePinOwners(pin string) []string {
	if b == nil {
		return nil
	}
	return append([]string{}, b.Semantics.WritePinOwners[strings.TrimSpace(pin)]...)
}

func (b *WorkflowContractBundle) NodeContractSource(nodeID string) (ContractItemSource, bool) {
	if b == nil {
		return ContractItemSource{}, false
	}
	source, ok := b.NodeSources[strings.TrimSpace(nodeID)]
	return source, ok
}

func (b *WorkflowContractBundle) NodeEventHandlers(nodeID string) map[string]SystemNodeEventHandler {
	if b == nil {
		return nil
	}
	nodeID = strings.TrimSpace(nodeID)
	handlers, ok := b.Semantics.NodeHandlers[nodeID]
	if !ok {
		return nil
	}
	out := make(map[string]SystemNodeEventHandler, len(handlers))
	for eventType, handler := range handlers {
		out[eventType] = handler
	}
	return out
}

func (b *WorkflowContractBundle) NodeEventHandler(nodeID, eventType string) (SystemNodeEventHandler, bool) {
	if b == nil {
		return SystemNodeEventHandler{}, false
	}
	nodeID = strings.TrimSpace(nodeID)
	eventType = strings.TrimSpace(eventType)
	handlers, ok := b.Semantics.NodeHandlers[nodeID]
	if !ok {
		return SystemNodeEventHandler{}, false
	}
	if handler, ok := handlers[eventType]; ok {
		return handler, true
	}
	for pattern, handler := range handlers {
		if handlerPatternMatches(pattern, eventType) {
			return handler, true
		}
	}
	return SystemNodeEventHandler{}, false
}

func (b *WorkflowContractBundle) RuntimeEventOwners(eventType string) []string {
	if b == nil {
		return nil
	}
	eventType = strings.TrimSpace(eventType)
	owners := append([]string{}, b.Semantics.EventOwners[eventType]...)
	for nodeID, handlers := range b.Semantics.NodeHandlers {
		for pattern := range handlers {
			if strings.TrimSpace(pattern) == eventType {
				continue
			}
			if handlerPatternMatches(pattern, eventType) {
				owners = appendIfMissingString(owners, nodeID)
				break
			}
		}
	}
	return owners
}

func (b *WorkflowContractBundle) DerivedHandlerTransitions() []HandlerTransitionSemantic {
	if b == nil {
		return nil
	}
	out := make([]HandlerTransitionSemantic, len(b.Semantics.HandlerTransitions))
	copy(out, b.Semantics.HandlerTransitions)
	return out
}

func (b *WorkflowContractBundle) DerivedHandlerTransition(nodeID, eventType string) (HandlerTransitionSemantic, bool) {
	if b == nil {
		return HandlerTransitionSemantic{}, false
	}
	nodeID = strings.TrimSpace(nodeID)
	eventType = strings.TrimSpace(eventType)
	transitions, ok := b.Semantics.HandlerTransitionIndex[nodeID]
	if !ok {
		return HandlerTransitionSemantic{}, false
	}
	transition, ok := transitions[eventType]
	return transition, ok
}

type RuntimeBridgePaths struct {
	Dir        string
	NodesFile  string
	EventsFile string
	AgentsFile string
	ToolsFile  string
	PolicyFile string
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
	RuntimeBridge     RuntimeBridgePaths
	Flows             []FlowContractPaths
}

type FlowContractPaths struct {
	ID         string
	Flow       string
	Namespace  string
	PackageKey string
	PackageDir string
	Dir        string
	SchemaFile string
	NodesFile  string
	EventsFile string
	AgentsFile string
	ToolsFile  string
	PolicyFile string
	PromptsDir string
}

type ProjectPackageDocument struct {
	Name             string              `yaml:"name"`
	Version          string              `yaml:"version"`
	PlatformVersion  string              `yaml:"platform_version"`
	Author           string              `yaml:"author"`
	Description      string              `yaml:"description"`
	Flows            []ProjectFlowRef    `yaml:"flows"`
	Packages         []ProjectPackageRef `yaml:"packages"`
	Children         []ProjectPackageRef `yaml:"children"`
	Subpackages      []ProjectPackageRef `yaml:"subpackages"`
	Handoffs         []ProjectHandoff    `yaml:"handoffs"`
	RuntimeContracts struct {
		Nodes  string `yaml:"nodes"`
		Events string `yaml:"events"`
		Agents string `yaml:"agents"`
		Tools  string `yaml:"tools"`
		Policy string `yaml:"policy"`
	} `yaml:"runtime_contracts"`
	TargetContracts struct {
		Nodes  string `yaml:"nodes"`
		Events string `yaml:"events"`
		Agents string `yaml:"agents"`
		Tools  string `yaml:"tools"`
		Policy string `yaml:"policy"`
		Note   string `yaml:"note"`
	} `yaml:"target_contracts"`
	EntitySchema map[string]any `yaml:"entity_schema"`
}

type ProjectPackageRef struct {
	ID      string `yaml:"id"`
	Path    string `yaml:"path"`
	Package string `yaml:"package"`
	Dir     string `yaml:"dir"`
}

type ProjectFlowRef struct {
	ID        string `yaml:"id"`
	Flow      string `yaml:"flow"`
	Namespace string `yaml:"namespace"`
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

type ProjectContractView struct {
	Paths    ProjectPackagePaths
	Manifest ProjectPackageDocument
	Nodes    map[string]SystemNodeContract
	Events   map[string]EventCatalogEntry
	Agents   map[string]AgentRegistryEntry
	Tools    map[string]ToolSchemaEntry
	Policy   map[string]any
}

type FlowContractView struct {
	Paths   FlowContractPaths
	Schema  FlowSchemaDocument
	Nodes   map[string]SystemNodeContract
	Events  map[string]EventCatalogEntry
	Agents  map[string]AgentRegistryEntry
	Tools   map[string]ToolSchemaEntry
	Policy  map[string]any
}

type ContractItemSource struct {
	PackageKey string
	FlowID     string
	Layer      string
	File       string
}

type FlowSchemaDocument struct {
	Name            string   `yaml:"name"`
	InitialState    string   `yaml:"initial_state"`
	NamespacePrefix string   `yaml:"namespace_prefix"`
	NamespaceRule   string   `yaml:"namespace_rule"`
	TerminalStates  []string `yaml:"terminal_states"`
	States          []string `yaml:"states"`
	Pins            FlowPins            `yaml:"pins"`
	RequiredAgents  []FlowRequiredAgent `yaml:"required_agents"`
}

type FlowPins struct {
	Inputs  FlowInputPins  `yaml:"inputs"`
	Outputs FlowOutputPins `yaml:"outputs"`
}

type FlowInputPins struct {
	Events []string `yaml:"events"`
	Reads  []string `yaml:"reads"`
}

type FlowOutputPins struct {
	Events []string `yaml:"events"`
	Writes []string `yaml:"writes"`
}

type FlowRequiredAgent struct {
	Role         string   `yaml:"role"`
	SubscribesTo []string `yaml:"subscribes_to"`
	Emits        []string `yaml:"emits"`
	Description  string   `yaml:"description"`
}

type WorkflowSchemaDocument struct {
	Workflow struct {
		Name           string                       `yaml:"name"`
		Version        string                       `yaml:"version"`
		Entity         string                       `yaml:"entity"`
		EntityTable    string                       `yaml:"entity_table"`
		StateField     string                       `yaml:"state_field"`
		InitialStage   string                       `yaml:"initial_stage"`
		Stages         []WorkflowStageContract      `yaml:"stages"`
		TerminalStages []string                     `yaml:"terminal_stages"`
		Transitions    []WorkflowTransitionContract `yaml:"transitions"`
		Timers         []WorkflowTimerContract      `yaml:"timers"`
		EntitySchema   map[string]any               `yaml:"entity_schema"`
	} `yaml:"workflow"`
}

type WorkflowStageContract struct {
	ID          string `yaml:"id"`
	Phase       string `yaml:"phase"`
	Description string `yaml:"description"`
}

type WorkflowTransitionContract struct {
	ID                string                   `yaml:"id"`
	From              any                      `yaml:"from"`
	To                string                   `yaml:"to"`
	Trigger           string                   `yaml:"trigger"`
	Node              string                   `yaml:"node"`
	Guards            []string                 `yaml:"guards"`
	Actions           []string                 `yaml:"actions"`
	DataAccumulation  WorkflowDataAccumulation `yaml:"data_accumulation"`
	AllowTerminalExit bool                     `yaml:"allow_terminal_exit"`
}

type WorkflowDataAccumulation struct {
	Writes      []string `yaml:"writes"`
	SourceEvent string   `yaml:"source_event"`
}

type WorkflowTimerContract struct {
	ID           string `yaml:"id"`
	Stage        string `yaml:"stage"`
	Event        string `yaml:"event"`
	Owner        string `yaml:"owner"`
	Action       string `yaml:"action"`
	Cancellation string `yaml:"cancellation"`
	DelaySeconds int    `yaml:"delay_seconds"`
	DelayMinutes int    `yaml:"delay_minutes"`
	DelayHours   int    `yaml:"delay_hours"`
	DelayDays    int    `yaml:"delay_days"`
	Recurring    bool   `yaml:"recurring"`
}

type GuardActionRegistryDocument struct {
	Guards  []GuardActionEntry `yaml:"guards"`
	Actions []GuardActionEntry `yaml:"actions"`
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
	Implementation   string                            `yaml:"implementation"`
	SubscribesTo     []string                          `yaml:"subscribes_to"`
	Produces         []string                          `yaml:"produces"`
	OwnedTransitions []string                          `yaml:"owned_transitions"`
	StateTable       string                            `yaml:"state_table"`
	IdempotencyTable string                            `yaml:"idempotency_table"`
	Timers           []string                          `yaml:"timers"`
	EventHandlers    map[string]SystemNodeEventHandler `yaml:"event_handlers"`
	StateSchema      map[string]any                    `yaml:"state_schema"`
}

type SystemNodeEventHandler struct {
	Action           string                   `yaml:"action"`
	Description      string                   `yaml:"description"`
	Emits            string                   `yaml:"emits"`
	Guard            any                      `yaml:"guard"`
	AdvancesTo       string                   `yaml:"advances_to"`
	SetsGate         any                      `yaml:"sets_gate"`
	DataAccumulation WorkflowDataAccumulation `yaml:"data_accumulation"`
	Condition        string                   `yaml:"condition"`
	CompletionRule   string                   `yaml:"completion_rule"`
	Logic            string                   `yaml:"logic"`
	PolicyRef        string                   `yaml:"policy_ref"`
	OnComplete       map[string]any           `yaml:"on_complete"`
	Rules            map[string]any           `yaml:"rules"`
	Branch           []any                    `yaml:"branch"`
	ModeToScanners   map[string]any           `yaml:"mode_to_scanners"`
}

type EventCatalogEntry struct {
	Emitter           any      `yaml:"emitter"`
	EmitterType       string   `yaml:"emitter_type"`
	AlternateEmitters []string `yaml:"alternate_emitters"`
	Consumer          any      `yaml:"consumer"`
	ConsumerType      any      `yaml:"consumer_type"`
	Intercepted       any      `yaml:"intercepted"`
	Passthrough       any      `yaml:"passthrough"`
	RuntimeHandling   string   `yaml:"runtime_handling"`
	OwningNode        string   `yaml:"owning_node"`
	DeliveryChannel   any      `yaml:"delivery_channel"`
	Payload           any      `yaml:"payload"`
	Required          []string `yaml:"required"`
}

type AgentRegistryEntry struct {
	ID                     string   `yaml:"id"`
	Type                   string   `yaml:"type"`
	Role                   string   `yaml:"role"`
	NodeType               string   `yaml:"node_type"`
	ModelTier              string   `yaml:"model_tier"`
	ConversationMode       string   `yaml:"conversation_mode"`
	MaxTurnsPerTask        int      `yaml:"max_turns_per_task"`
	Subscriptions          []string `yaml:"subscriptions"`
	SubscriptionsBootstrap []string `yaml:"subscriptions_bootstrap"`
	SubscribesTo           []string `yaml:"subscribes_to"`
	ToolsTier2             []string `yaml:"tools_tier2"`
	EmitEvents             []string `yaml:"emit_events"`
	Implementation         string   `yaml:"implementation"`
}

type ToolSchemaEntry struct {
	Category    string         `yaml:"category"`
	Description string         `yaml:"description"`
	InputSchema map[string]any `yaml:"input_schema"`
}

type PlatformSpecDocument struct {
	Platform struct {
		Name    string `yaml:"name"`
		Version string `yaml:"version"`
	} `yaml:"platform"`
	Vocabulary struct {
		Participant struct {
			Types map[string]struct {
				Execution string `yaml:"execution"`
			} `yaml:"types"`
		} `yaml:"participant"`
	} `yaml:"vocabulary"`
	ContractFormats map[string]any `yaml:"contract_formats"`
	WorkflowState   struct {
		Fields map[string]struct {
			Type string `yaml:"type"`
		} `yaml:"fields"`
	} `yaml:"workflow_state"`
	BuiltinHooks struct {
		Guards []struct {
			ID string `yaml:"id"`
		} `yaml:"guards"`
		Actions []struct {
			ID string `yaml:"id"`
		} `yaml:"actions"`
	} `yaml:"builtin_hooks"`
	ComplianceRules map[string][]struct {
		ID string `yaml:"id"`
	} `yaml:"compliance_rules"`
	FileLayout struct {
		MigrationNote string `yaml:"migration_note"`
	} `yaml:"file_layout"`
}

func ResolveWorkflowContractPaths(repoRoot string) ContractPaths {
	contractsRoot := filepath.Join(repoRoot, "contracts")
	workflowDir := filepath.Join(contractsRoot, "empire")
	paths := ContractPaths{
		ContractsRoot:         contractsRoot,
		WorkflowDir:           workflowDir,
		ProjectPackageFile:    existingFile(filepath.Join(workflowDir, "package.yaml")),
		ProjectNodesFile:      existingFile(filepath.Join(workflowDir, "nodes.yaml")),
		ProjectEventsFile:     existingFile(filepath.Join(workflowDir, "events.yaml")),
		ProjectAgentsFile:     existingFile(filepath.Join(workflowDir, "agents.yaml")),
		ProjectToolsFile:      existingFile(filepath.Join(workflowDir, "tools.yaml")),
		ProjectPolicyFile:     existingFile(filepath.Join(workflowDir, "policy.yaml")),
		ProjectPromptsDir:     existingDir(filepath.Join(workflowDir, "prompts")),
		WorkflowSchemaFile:    preferredContractPath(filepath.Join(workflowDir, "workflow-empire.yaml"), filepath.Join(contractsRoot, "workflow-schema.yaml")),
		GuardRegistryFile:     preferredContractPath(filepath.Join(workflowDir, "hooks-empire.yaml"), filepath.Join(contractsRoot, "guard-action-registry.yaml")),
		SystemNodesFile:       preferredContractPath(filepath.Join(workflowDir, "nodes-empire.yaml"), filepath.Join(contractsRoot, "system-nodes.yaml")),
		EventCatalogFile:      preferredContractPath(filepath.Join(workflowDir, "events-empire.yaml"), filepath.Join(contractsRoot, "event-catalog.yaml")),
		AgentRegistryFile:     preferredContractPath(filepath.Join(workflowDir, "agents-empire.yaml"), filepath.Join(contractsRoot, "agent-tools.yaml")),
		ToolSchemasFile:       preferredContractPath(filepath.Join(workflowDir, "tools-empire.yaml"), filepath.Join(contractsRoot, "tool-schemas.yaml")),
		PolicyFile:            preferredContractPath(filepath.Join(workflowDir, "policy-empire.yaml"), filepath.Join(contractsRoot, "prompt-variables.yaml")),
		PromptsDir:            preferredContractDir(filepath.Join(workflowDir, "prompts"), filepath.Join(contractsRoot, "prompts")),
		PlatformSpecFile:      filepath.Join(contractsRoot, "platform", "platform-spec.yaml"),
		VerificationGatesFile: filepath.Join(contractsRoot, "verification-gates.yaml"),
		ToolingLockFile:       filepath.Join(contractsRoot, "tooling.lock"),
		DDLFile:               filepath.Join(contractsRoot, "ddl-canonical.sql"),
		AgentConfigMapFile:    filepath.Join(contractsRoot, "agent-config-map.yaml"),
	}
	if paths.ProjectPackageFile != "" {
		paths.ProjectPackages = discoverProjectPackagePaths(paths.ProjectPackageFile, workflowDir)
		if len(paths.ProjectPackages) > 0 {
			rootPackage := paths.ProjectPackages[0]
			paths.RuntimeBridge = rootPackage.RuntimeBridge
			if paths.RuntimeBridge.NodesFile != "" {
				paths.SystemNodesFile = paths.RuntimeBridge.NodesFile
			}
			if paths.RuntimeBridge.EventsFile != "" {
				paths.EventCatalogFile = paths.RuntimeBridge.EventsFile
			}
			if paths.RuntimeBridge.AgentsFile != "" {
				paths.AgentRegistryFile = paths.RuntimeBridge.AgentsFile
			}
			if paths.RuntimeBridge.ToolsFile != "" {
				paths.ToolSchemasFile = paths.RuntimeBridge.ToolsFile
			}
			if paths.RuntimeBridge.PolicyFile != "" {
				paths.PolicyFile = paths.RuntimeBridge.PolicyFile
			}
		}
		for _, pkg := range paths.ProjectPackages {
			paths.Flows = append(paths.Flows, pkg.Flows...)
		}
		sort.Slice(paths.Flows, func(i, j int) bool {
			if paths.Flows[i].ID == paths.Flows[j].ID {
				if paths.Flows[i].PackageKey == paths.Flows[j].PackageKey {
					return strings.Compare(paths.Flows[i].Flow, paths.Flows[j].Flow) < 0
				}
				return strings.Compare(paths.Flows[i].PackageKey, paths.Flows[j].PackageKey) < 0
			}
			return strings.Compare(paths.Flows[i].ID, paths.Flows[j].ID) < 0
		})
	}
	return paths
}

func LoadWorkflowContractBundle(repoRoot string) (*WorkflowContractBundle, error) {
	paths := ResolveWorkflowContractPaths(repoRoot)
	bundle := &WorkflowContractBundle{
		Paths:            paths,
		ProjectContracts: map[string]ProjectContractView{},
		FlowContracts:    map[string]FlowContractView{},
		MergedNodes:      map[string]SystemNodeContract{},
		MergedEvents:     map[string]EventCatalogEntry{},
		MergedAgents:     map[string]AgentRegistryEntry{},
		MergedTools:      map[string]ToolSchemaEntry{},
		MergedPolicy:     map[string]any{},
		NodeSources:      map[string]ContractItemSource{},
		EventSources:     map[string]ContractItemSource{},
		AgentSources:     map[string]ContractItemSource{},
		ToolSources:      map[string]ContractItemSource{},
		PolicySources:    map[string]ContractItemSource{},
		FlowSchemas:      map[string]FlowSchemaDocument{},
	}
	if paths.ProjectPackageFile != "" {
		for i, pkgPaths := range paths.ProjectPackages {
			var manifest ProjectPackageDocument
			if err := loadYAMLFile(pkgPaths.PackageFile, &manifest); err != nil {
				return nil, err
			}
			if i == 0 {
				bundle.Package = manifest
			}
			bundle.PackageTree = append(bundle.PackageTree, LoadedProjectPackage{
				Key:       pkgPaths.Key,
				ParentKey: pkgPaths.ParentKey,
				Depth:     pkgPaths.Depth,
				Paths:     pkgPaths,
				Manifest:  manifest,
			})
			projectView, err := loadProjectContractView(pkgPaths, manifest)
			if err != nil {
				return nil, err
			}
			bundle.ProjectContracts[pkgPaths.Key] = projectView
		}
		if err := validateDiscoveredPackageTree(bundle.PackageTree); err != nil {
			return nil, err
		}
		for _, flow := range paths.Flows {
			if strings.TrimSpace(flow.ID) == "" || strings.TrimSpace(flow.SchemaFile) == "" {
				continue
			}
			if _, exists := bundle.FlowSchemas[flow.ID]; exists {
				return nil, fmt.Errorf("duplicate flow id %q discovered in package tree", flow.ID)
			}
			var schema FlowSchemaDocument
			if err := loadYAMLFile(flow.SchemaFile, &schema); err != nil {
				return nil, err
			}
			bundle.FlowSchemas[flow.ID] = schema
			flowView, err := loadFlowContractView(flow, schema)
			if err != nil {
				return nil, err
			}
			bundle.FlowContracts[flow.ID] = flowView
		}
		if err := populateMergedPackageViews(bundle); err != nil {
			return nil, err
		}
	}
	if err := loadYAMLFile(paths.WorkflowSchemaFile, &bundle.Workflow); err != nil {
		return nil, err
	}
	if err := loadYAMLFile(paths.GuardRegistryFile, &bundle.Hooks); err != nil {
		return nil, err
	}
	if err := loadYAMLFile(paths.SystemNodesFile, &bundle.Nodes); err != nil {
		return nil, err
	}
	if err := loadYAMLFile(paths.EventCatalogFile, &bundle.Events); err != nil {
		return nil, err
	}
	if err := loadYAMLFile(paths.AgentRegistryFile, &bundle.Agents); err != nil {
		return nil, err
	}
	if err := loadYAMLFile(paths.ToolSchemasFile, &bundle.Tools); err != nil {
		return nil, err
	}
	if err := loadYAMLFile(paths.PolicyFile, &bundle.Policy); err != nil {
		return nil, err
	}
	if err := loadYAMLFile(paths.PlatformSpecFile, &bundle.Platform); err != nil {
		return nil, err
	}
	populateWorkflowSemantics(bundle)
	return bundle, nil
}

func populateWorkflowSemantics(bundle *WorkflowContractBundle) {
	if bundle == nil {
		return
	}
	name := strings.TrimSpace(bundle.Workflow.Workflow.Name)
	if name == "" {
		name = strings.TrimSpace(bundle.Package.Name)
	}
	version := strings.TrimSpace(bundle.Package.Version)
	if version == "" {
		version = strings.TrimSpace(bundle.Workflow.Workflow.Version)
	}
	entitySchema := bundle.Package.EntitySchema
	if len(entitySchema) == 0 {
		entitySchema = bundle.Workflow.Workflow.EntitySchema
	}
	semantics := WorkflowSemanticView{
		Name:           name,
		Version:        version,
		InitialStage:   strings.TrimSpace(bundle.Workflow.Workflow.InitialStage),
		EntitySchema:   entitySchema,
		Stages:         append([]WorkflowStageContract{}, bundle.Workflow.Workflow.Stages...),
		TerminalStages: append([]string{}, bundle.Workflow.Workflow.TerminalStages...),
		Transitions:    append([]WorkflowTransitionContract{}, bundle.Workflow.Workflow.Transitions...),
		Timers:         append([]WorkflowTimerContract{}, bundle.Workflow.Workflow.Timers...),
		Guards:         append([]GuardActionEntry{}, bundle.Hooks.Guards...),
		Actions:        append([]GuardActionEntry{}, bundle.Hooks.Actions...),
		GuardByID:      map[string]GuardActionEntry{},
		ActionByID:     map[string]GuardActionEntry{},
		FlowInitial:    map[string]string{},
		FlowStates:     map[string][]string{},
		FlowTerminal:   map[string][]string{},
		FlowNamespace:  map[string]string{},
		FlowPrefix:     map[string]string{},
		FlowRules:      map[string]string{},
		FlowInputs:     map[string][]string{},
		FlowOutputs:    map[string][]string{},
		FlowReads:      map[string][]string{},
		FlowWrites:     map[string][]string{},
		FlowAgents:     map[string][]FlowRequiredAgent{},
		WritePinOwners: map[string][]string{},
		NodeHandlers:   map[string]map[string]SystemNodeEventHandler{},
		EventOwners:    map[string][]string{},
		HandlerTransitionIndex: map[string]map[string]HandlerTransitionSemantic{},
	}
	for _, entry := range semantics.Guards {
		if id := strings.TrimSpace(entry.ID); id != "" {
			semantics.GuardByID[id] = entry
		}
	}
	for _, entry := range semantics.Actions {
		if id := strings.TrimSpace(entry.ID); id != "" {
			semantics.ActionByID[id] = entry
		}
	}
	for flowID, schema := range bundle.FlowSchemas {
		flowID = strings.TrimSpace(flowID)
		if flowID == "" {
			continue
		}
		semantics.FlowInitial[flowID] = strings.TrimSpace(schema.InitialState)
		semantics.FlowStates[flowID] = append([]string{}, schema.States...)
		semantics.FlowTerminal[flowID] = append([]string{}, schema.TerminalStates...)
		assignedNamespace := strings.TrimSpace(flowAssignedNamespace(bundle.Paths.Flows, flowID))
		if assignedNamespace == "" {
			assignedNamespace = strings.TrimSpace(schema.NamespacePrefix)
		}
		semantics.FlowNamespace[flowID] = assignedNamespace
		semantics.FlowPrefix[flowID] = strings.TrimSpace(schema.NamespacePrefix)
		semantics.FlowRules[flowID] = strings.TrimSpace(schema.NamespaceRule)
		semantics.FlowInputs[flowID] = append([]string{}, schema.Pins.Inputs.Events...)
		semantics.FlowOutputs[flowID] = append([]string{}, schema.Pins.Outputs.Events...)
		semantics.FlowReads[flowID] = append([]string{}, schema.Pins.Inputs.Reads...)
		semantics.FlowWrites[flowID] = append([]string{}, schema.Pins.Outputs.Writes...)
		semantics.FlowAgents[flowID] = append([]FlowRequiredAgent{}, schema.RequiredAgents...)
		for _, writePin := range schema.Pins.Outputs.Writes {
			writePin = strings.TrimSpace(writePin)
			if writePin == "" {
				continue
			}
			semantics.WritePinOwners[writePin] = appendIfMissingString(semantics.WritePinOwners[writePin], flowID)
		}
	}
	for nodeID, node := range bundle.Nodes {
		nodeID = strings.TrimSpace(nodeID)
		if nodeID == "" || len(node.EventHandlers) == 0 {
			continue
		}
		handlers := make(map[string]SystemNodeEventHandler, len(node.EventHandlers))
		source, _ := bundle.NodeContractSource(nodeID)
		for eventType, handler := range node.EventHandlers {
			eventType = strings.TrimSpace(eventType)
			if eventType == "" {
				continue
			}
			handlers[eventType] = handler
			semantics.EventOwners[eventType] = appendIfMissingString(semantics.EventOwners[eventType], nodeID)
			transition := HandlerTransitionSemantic{
				ID:               fmt.Sprintf("%s:%s", nodeID, eventType),
				NodeID:           nodeID,
				FlowID:           strings.TrimSpace(source.FlowID),
				EventType:        eventType,
				Action:           strings.TrimSpace(handler.Action),
				Guard:            handler.Guard,
				AdvancesTo:       strings.TrimSpace(handler.AdvancesTo),
				SetsGate:         handler.SetsGate,
				DataAccumulation: handler.DataAccumulation,
				Emits:            strings.TrimSpace(handler.Emits),
				Condition:        strings.TrimSpace(handler.Condition),
				CompletionRule:   strings.TrimSpace(handler.CompletionRule),
				OnComplete:       handler.OnComplete,
				Rules:            handler.Rules,
			}
			semantics.HandlerTransitions = append(semantics.HandlerTransitions, transition)
			if semantics.HandlerTransitionIndex[nodeID] == nil {
				semantics.HandlerTransitionIndex[nodeID] = map[string]HandlerTransitionSemantic{}
			}
			semantics.HandlerTransitionIndex[nodeID][eventType] = transition
		}
		semantics.NodeHandlers[nodeID] = handlers
	}
	if len(semantics.Stages) == 0 {
		semantics.Stages = deriveWorkflowStagesFromFlows(bundle.Paths.Flows, bundle.FlowSchemas)
	}
	if len(semantics.TerminalStages) == 0 {
		semantics.TerminalStages = deriveWorkflowTerminalStagesFromFlows(bundle.Paths.Flows, bundle.FlowSchemas)
	}
	bundle.Semantics = semantics
}

func deriveWorkflowStagesFromFlows(paths []FlowContractPaths, schemas map[string]FlowSchemaDocument) []WorkflowStageContract {
	if len(paths) == 0 || len(schemas) == 0 {
		return nil
	}
	out := make([]WorkflowStageContract, 0)
	seen := make(map[string]struct{})
	for _, flow := range paths {
		flowID := strings.TrimSpace(flow.ID)
		schema, ok := schemas[flowID]
		if !ok {
			continue
		}
		for _, state := range schema.States {
			state = strings.TrimSpace(state)
			if state == "" {
				continue
			}
			if _, exists := seen[state]; exists {
				continue
			}
			seen[state] = struct{}{}
			out = append(out, WorkflowStageContract{
				ID:    state,
				Phase: flowID,
			})
		}
	}
	return out
}

func deriveWorkflowTerminalStagesFromFlows(paths []FlowContractPaths, schemas map[string]FlowSchemaDocument) []string {
	if len(paths) == 0 || len(schemas) == 0 {
		return nil
	}
	out := make([]string, 0)
	seen := make(map[string]struct{})
	for _, flow := range paths {
		flowID := strings.TrimSpace(flow.ID)
		schema, ok := schemas[flowID]
		if !ok {
			continue
		}
		for _, state := range schema.TerminalStates {
			state = strings.TrimSpace(state)
			if state == "" {
				continue
			}
			if _, exists := seen[state]; exists {
				continue
			}
			seen[state] = struct{}{}
			out = append(out, state)
		}
	}
	return out
}

func flowAssignedNamespace(paths []FlowContractPaths, flowID string) string {
	flowID = strings.TrimSpace(flowID)
	if flowID == "" {
		return ""
	}
	for _, flow := range paths {
		if strings.TrimSpace(flow.ID) == flowID {
			return strings.TrimSpace(flow.Namespace)
		}
	}
	return ""
}

func appendIfMissingString(items []string, value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return items
	}
	for _, item := range items {
		if strings.TrimSpace(item) == value {
			return items
		}
	}
	return append(items, value)
}

func handlerPatternMatches(pattern, eventType string) bool {
	pattern = strings.TrimSpace(pattern)
	eventType = strings.TrimSpace(eventType)
	if pattern == "" || eventType == "" {
		return false
	}
	if pattern == eventType {
		return true
	}
	if !strings.Contains(pattern, "*") {
		return false
	}
	matched, err := path.Match(pattern, eventType)
	return err == nil && matched
}

func loadProjectContractView(paths ProjectPackagePaths, manifest ProjectPackageDocument) (ProjectContractView, error) {
	view := ProjectContractView{
		Paths:    paths,
		Manifest: manifest,
		Nodes:    map[string]SystemNodeContract{},
		Events:   map[string]EventCatalogEntry{},
		Agents:   map[string]AgentRegistryEntry{},
		Tools:    map[string]ToolSchemaEntry{},
		Policy:   map[string]any{},
	}
	if err := loadOptionalYAMLMap(paths.ProjectNodesFile, &view.Nodes); err != nil {
		return view, err
	}
	if err := loadOptionalYAMLMap(paths.ProjectEventsFile, &view.Events); err != nil {
		return view, err
	}
	if err := loadOptionalYAMLMap(paths.ProjectAgentsFile, &view.Agents); err != nil {
		return view, err
	}
	if err := loadOptionalYAMLMap(paths.ProjectToolsFile, &view.Tools); err != nil {
		return view, err
	}
	if err := loadOptionalYAMLMap(paths.ProjectPolicyFile, &view.Policy); err != nil {
		return view, err
	}
	return view, nil
}

func loadFlowContractView(paths FlowContractPaths, schema FlowSchemaDocument) (FlowContractView, error) {
	view := FlowContractView{
		Paths:  paths,
		Schema: schema,
		Nodes:  map[string]SystemNodeContract{},
		Events: map[string]EventCatalogEntry{},
		Agents: map[string]AgentRegistryEntry{},
		Tools:  map[string]ToolSchemaEntry{},
		Policy: map[string]any{},
	}
	if err := loadOptionalYAMLMap(paths.NodesFile, &view.Nodes); err != nil {
		return view, err
	}
	if err := loadOptionalYAMLMap(paths.EventsFile, &view.Events); err != nil {
		return view, err
	}
	if err := loadOptionalYAMLMap(paths.AgentsFile, &view.Agents); err != nil {
		return view, err
	}
	if err := loadOptionalYAMLMap(paths.ToolsFile, &view.Tools); err != nil {
		return view, err
	}
	if err := loadOptionalYAMLMap(paths.PolicyFile, &view.Policy); err != nil {
		return view, err
	}
	return view, nil
}

func populateMergedPackageViews(bundle *WorkflowContractBundle) error {
	for _, pkg := range bundle.PackageTree {
		view, ok := bundle.ProjectContracts[pkg.Key]
		if !ok {
			continue
		}
		if err := mergeNodeContracts(bundle, view.Nodes, ContractItemSource{PackageKey: pkg.Key, Layer: "project", File: view.Paths.ProjectNodesFile}); err != nil {
			return err
		}
		if err := mergeEventContracts(bundle, view.Events, ContractItemSource{PackageKey: pkg.Key, Layer: "project", File: view.Paths.ProjectEventsFile}); err != nil {
			return err
		}
		if err := mergeAgentContracts(bundle, view.Agents, ContractItemSource{PackageKey: pkg.Key, Layer: "project", File: view.Paths.ProjectAgentsFile}); err != nil {
			return err
		}
		if err := mergeToolContracts(bundle, view.Tools, ContractItemSource{PackageKey: pkg.Key, Layer: "project", File: view.Paths.ProjectToolsFile}); err != nil {
			return err
		}
		mergePolicyContracts(bundle, view.Policy, ContractItemSource{PackageKey: pkg.Key, Layer: "project", File: view.Paths.ProjectPolicyFile})
	}
	for _, flow := range bundle.Paths.Flows {
		view, ok := bundle.FlowContracts[flow.ID]
		if !ok {
			continue
		}
		sourcePrefix := ContractItemSource{PackageKey: flow.PackageKey, FlowID: flow.ID, Layer: "flow"}
		if err := mergeNodeContracts(bundle, view.Nodes, contractSourceWithFile(sourcePrefix, view.Paths.NodesFile)); err != nil {
			return err
		}
		if err := mergeEventContracts(bundle, view.Events, contractSourceWithFile(sourcePrefix, view.Paths.EventsFile)); err != nil {
			return err
		}
		if err := mergeAgentContracts(bundle, view.Agents, contractSourceWithFile(sourcePrefix, view.Paths.AgentsFile)); err != nil {
			return err
		}
		if err := mergeToolContracts(bundle, view.Tools, contractSourceWithFile(sourcePrefix, view.Paths.ToolsFile)); err != nil {
			return err
		}
		mergePolicyContracts(bundle, view.Policy, contractSourceWithFile(sourcePrefix, view.Paths.PolicyFile))
	}
	return nil
}

func mergeNodeContracts(bundle *WorkflowContractBundle, entries map[string]SystemNodeContract, source ContractItemSource) error {
	for id, entry := range entries {
		key := strings.TrimSpace(id)
		if key == "" {
			continue
		}
		if existing, ok := bundle.NodeSources[key]; ok {
			if reflect.DeepEqual(bundle.MergedNodes[key], entry) {
				continue
			}
			return fmt.Errorf("duplicate merged node id %q from %s and %s", key, existing.File, source.File)
		}
		bundle.MergedNodes[key] = entry
		bundle.NodeSources[key] = source
	}
	return nil
}

func mergeEventContracts(bundle *WorkflowContractBundle, entries map[string]EventCatalogEntry, source ContractItemSource) error {
	for id, entry := range entries {
		key := strings.TrimSpace(id)
		if key == "" {
			continue
		}
		if existing, ok := bundle.EventSources[key]; ok {
			if reflect.DeepEqual(bundle.MergedEvents[key], entry) {
				continue
			}
			return fmt.Errorf("duplicate merged event id %q from %s and %s", key, existing.File, source.File)
		}
		bundle.MergedEvents[key] = entry
		bundle.EventSources[key] = source
	}
	return nil
}

func mergeAgentContracts(bundle *WorkflowContractBundle, entries map[string]AgentRegistryEntry, source ContractItemSource) error {
	for id, entry := range entries {
		key := strings.TrimSpace(id)
		if key == "" {
			continue
		}
		if existing, ok := bundle.AgentSources[key]; ok {
			if reflect.DeepEqual(bundle.MergedAgents[key], entry) {
				continue
			}
			return fmt.Errorf("duplicate merged agent id %q from %s and %s", key, existing.File, source.File)
		}
		bundle.MergedAgents[key] = entry
		bundle.AgentSources[key] = source
	}
	return nil
}

func mergeToolContracts(bundle *WorkflowContractBundle, entries map[string]ToolSchemaEntry, source ContractItemSource) error {
	for id, entry := range entries {
		key := strings.TrimSpace(id)
		if key == "" {
			continue
		}
		if existing, ok := bundle.ToolSources[key]; ok {
			if reflect.DeepEqual(bundle.MergedTools[key], entry) {
				continue
			}
			return fmt.Errorf("duplicate merged tool id %q from %s and %s", key, existing.File, source.File)
		}
		bundle.MergedTools[key] = entry
		bundle.ToolSources[key] = source
	}
	return nil
}

func mergePolicyContracts(bundle *WorkflowContractBundle, entries map[string]any, source ContractItemSource) {
	for id, entry := range entries {
		key := strings.TrimSpace(id)
		if key == "" {
			continue
		}
		bundle.MergedPolicy[key] = entry
		bundle.PolicySources[key] = source
	}
}

func contractSourceWithFile(source ContractItemSource, file string) ContractItemSource {
	source.File = file
	return source
}

func (b *WorkflowContractBundle) TransitionIDsByOwner() map[string][]string {
	out := map[string][]string{}
	if b == nil {
		return out
	}
	for _, transition := range b.WorkflowTransitions() {
		owner := strings.TrimSpace(transition.Node)
		if owner == "" {
			continue
		}
		out[owner] = append(out[owner], strings.TrimSpace(transition.ID))
	}
	for owner := range out {
		sort.Strings(out[owner])
	}
	return out
}

func ContractFilesExist(repoRoot string) []string {
	paths := ResolveWorkflowContractPaths(repoRoot)
	files := []string{
		paths.WorkflowSchemaFile,
		paths.GuardRegistryFile,
		paths.SystemNodesFile,
		paths.EventCatalogFile,
		paths.AgentRegistryFile,
		paths.ToolSchemasFile,
		paths.PolicyFile,
		paths.PromptsDir,
		paths.PlatformSpecFile,
		paths.VerificationGatesFile,
		paths.ToolingLockFile,
		paths.DDLFile,
	}
	if paths.ProjectPackageFile != "" {
		files = append(files,
			paths.ProjectPackageFile,
			paths.ProjectNodesFile,
			paths.ProjectEventsFile,
			paths.ProjectAgentsFile,
		)
		for _, pkg := range paths.ProjectPackages {
			files = append(files,
				pkg.PackageFile,
				pkg.ProjectNodesFile,
				pkg.ProjectEventsFile,
				pkg.ProjectAgentsFile,
				pkg.ProjectToolsFile,
				pkg.ProjectPolicyFile,
			)
		}
		for _, flow := range paths.Flows {
			files = append(files, flow.SchemaFile, flow.NodesFile, flow.EventsFile, flow.AgentsFile)
		}
	}
	missing := make([]string, 0)
	for _, path := range files {
		if strings.TrimSpace(path) == "" {
			continue
		}
		if _, err := os.Stat(path); err != nil {
			missing = append(missing, path)
		}
	}
	sort.Strings(missing)
	return missing
}

func preferredContractPath(preferred, legacy string) string {
	if preferred != "" {
		if _, err := os.Stat(preferred); err == nil {
			return preferred
		}
	}
	return legacy
}

func preferredContractDir(preferred, legacy string) string {
	if preferred != "" {
		if stat, err := os.Stat(preferred); err == nil && stat.IsDir() {
			return preferred
		}
	}
	return legacy
}

func existingFile(path string) string {
	if path == "" {
		return ""
	}
	if stat, err := os.Stat(path); err == nil && !stat.IsDir() {
		return path
	}
	return ""
}

func existingDir(path string) string {
	if path == "" {
		return ""
	}
	if stat, err := os.Stat(path); err == nil && stat.IsDir() {
		return path
	}
	return ""
}

func (p ProjectPackageDocument) ChildPackages() []ProjectPackageRef {
	out := make([]ProjectPackageRef, 0, len(p.Packages)+len(p.Children)+len(p.Subpackages))
	out = append(out, p.Packages...)
	out = append(out, p.Children...)
	out = append(out, p.Subpackages...)
	return out
}

func (p ProjectPackageRef) ResolveLocation() string {
	for _, candidate := range []string{p.Path, p.Package, p.Dir} {
		if resolved := strings.TrimSpace(candidate); resolved != "" {
			return resolved
		}
	}
	return ""
}

func discoverProjectPackagePaths(packageFile, workflowDir string) []ProjectPackagePaths {
	rootFile := existingFile(packageFile)
	rootDir := filepath.Dir(rootFile)
	if strings.TrimSpace(rootFile) == "" || strings.TrimSpace(workflowDir) == "" {
		return nil
	}
	visited := map[string]bool{}
	var out []ProjectPackagePaths
	var walk func(packageFile, parentKey string, depth int)
	walk = func(packageFile, parentKey string, depth int) {
		packageFile = existingFile(packageFile)
		if packageFile == "" || visited[packageFile] {
			return
		}
		visited[packageFile] = true

		var manifest ProjectPackageDocument
		if err := loadYAMLFile(packageFile, &manifest); err != nil {
			return
		}

		packageDir := filepath.Dir(packageFile)
		key := "."
		if rel, err := filepath.Rel(rootDir, packageDir); err == nil && strings.TrimSpace(rel) != "" {
			key = filepath.Clean(rel)
		}
		pkg := ProjectPackagePaths{
			Key:               key,
			ParentKey:         parentKey,
			Depth:             depth,
			Dir:               packageDir,
			PackageFile:       packageFile,
			ProjectNodesFile:  existingFile(filepath.Join(packageDir, "nodes.yaml")),
			ProjectEventsFile: existingFile(filepath.Join(packageDir, "events.yaml")),
			ProjectAgentsFile: existingFile(filepath.Join(packageDir, "agents.yaml")),
			ProjectToolsFile:  existingFile(filepath.Join(packageDir, "tools.yaml")),
			ProjectPolicyFile: existingFile(filepath.Join(packageDir, "policy.yaml")),
			ProjectPromptsDir: existingDir(filepath.Join(packageDir, "prompts")),
		}
		if runtime := resolveRuntimeBridgePaths(packageDir, manifest); runtime.Dir != "" {
			pkg.RuntimeBridge = runtime
		} else if runtimeDir := existingDir(filepath.Join(packageDir, "runtime")); runtimeDir != "" {
			pkg.RuntimeBridge = RuntimeBridgePaths{
				Dir:        runtimeDir,
				NodesFile:  existingFile(filepath.Join(runtimeDir, "nodes.yaml")),
				EventsFile: existingFile(filepath.Join(runtimeDir, "events.yaml")),
				AgentsFile: existingFile(filepath.Join(runtimeDir, "agents.yaml")),
				ToolsFile:  existingFile(filepath.Join(runtimeDir, "tools.yaml")),
				PolicyFile: existingFile(filepath.Join(runtimeDir, "policy.yaml")),
			}
		}
		for _, flow := range manifest.Flows {
			flowDirName := strings.TrimSpace(flow.Flow)
			if flowDirName == "" {
				continue
			}
			dir := filepath.Join(packageDir, "flows", flowDirName)
			pkg.Flows = append(pkg.Flows, FlowContractPaths{
				ID:         strings.TrimSpace(flow.ID),
				Flow:       flowDirName,
				Namespace:  strings.TrimSpace(flow.Namespace),
				PackageKey: pkg.Key,
				PackageDir: packageDir,
				Dir:        dir,
				SchemaFile: existingFile(filepath.Join(dir, "schema.yaml")),
				NodesFile:  existingFile(filepath.Join(dir, "nodes.yaml")),
				EventsFile: existingFile(filepath.Join(dir, "events.yaml")),
				AgentsFile: existingFile(filepath.Join(dir, "agents.yaml")),
				ToolsFile:  existingFile(filepath.Join(dir, "tools.yaml")),
				PolicyFile: existingFile(filepath.Join(dir, "policy.yaml")),
				PromptsDir: existingDir(filepath.Join(dir, "prompts")),
			})
		}
		sort.Slice(pkg.Flows, func(i, j int) bool {
			return strings.Compare(pkg.Flows[i].ID, pkg.Flows[j].ID) < 0
		})
		out = append(out, pkg)

		for _, child := range manifest.ChildPackages() {
			location := child.ResolveLocation()
			if strings.TrimSpace(location) == "" {
				continue
			}
			childPath := filepath.Join(packageDir, location)
			if strings.HasSuffix(strings.ToLower(location), ".yaml") {
				walk(childPath, pkg.Key, depth+1)
				continue
			}
			walk(filepath.Join(childPath, "package.yaml"), pkg.Key, depth+1)
		}
	}
	walk(rootFile, "", 0)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Depth == out[j].Depth {
			return strings.Compare(out[i].Key, out[j].Key) < 0
		}
		return out[i].Depth < out[j].Depth
	})
	return out
}

func resolveRuntimeBridgePaths(packageDir string, manifest ProjectPackageDocument) RuntimeBridgePaths {
	resolve := func(rel string) string {
		rel = strings.TrimSpace(rel)
		if rel == "" {
			return ""
		}
		return existingFile(filepath.Join(packageDir, filepath.Clean(rel)))
	}
	runtime := RuntimeBridgePaths{
		NodesFile:  resolve(manifest.RuntimeContracts.Nodes),
		EventsFile: resolve(manifest.RuntimeContracts.Events),
		AgentsFile: resolve(manifest.RuntimeContracts.Agents),
		ToolsFile:  resolve(manifest.RuntimeContracts.Tools),
		PolicyFile: resolve(manifest.RuntimeContracts.Policy),
	}
	for _, candidate := range []string{
		runtime.NodesFile,
		runtime.EventsFile,
		runtime.AgentsFile,
		runtime.ToolsFile,
		runtime.PolicyFile,
	} {
		if candidate != "" {
			runtime.Dir = filepath.Dir(candidate)
			break
		}
	}
	return runtime
}

func validateDiscoveredPackageTree(pkgs []LoadedProjectPackage) error {
	seenPackages := map[string]struct{}{}
	seenFlows := map[string]string{}
	for _, pkg := range pkgs {
		if _, exists := seenPackages[pkg.Key]; exists {
			return fmt.Errorf("duplicate package key %q discovered in package tree", pkg.Key)
		}
		seenPackages[pkg.Key] = struct{}{}
		for _, flow := range pkg.Paths.Flows {
			flowID := strings.TrimSpace(flow.ID)
			if flowID == "" {
				continue
			}
			if existing, exists := seenFlows[flowID]; exists {
				return fmt.Errorf("duplicate flow id %q discovered in package tree (%s, %s)", flowID, existing, pkg.Key)
			}
			seenFlows[flowID] = pkg.Key
		}
	}
	return nil
}

func loadYAMLFile(path string, target any) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	if err := yaml.Unmarshal(raw, target); err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}
	return nil
}

func loadOptionalYAMLMap(path string, target any) error {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	return loadYAMLFile(path, target)
}
