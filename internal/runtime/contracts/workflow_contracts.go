package contracts

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

type EmpireContractPaths struct {
	ContractsRoot         string
	WorkflowDir           string
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
}

type WorkflowContractBundle struct {
	Paths    EmpireContractPaths
	Workflow WorkflowSchemaDocument
	Hooks    GuardActionRegistryDocument
	Nodes    map[string]SystemNodeContract
	Events   map[string]EventCatalogEntry
	Agents   map[string]AgentRegistryEntry
	Tools    map[string]ToolSchemaEntry
	Policy   map[string]any
	Platform PlatformSpecDocument
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
	} `yaml:"workflow"`
}

type WorkflowStageContract struct {
	ID          string `yaml:"id"`
	Phase       string `yaml:"phase"`
	Description string `yaml:"description"`
}

type WorkflowTransitionContract struct {
	ID                string   `yaml:"id"`
	From              any      `yaml:"from"`
	To                string   `yaml:"to"`
	Trigger           string   `yaml:"trigger"`
	Node              string   `yaml:"node"`
	Guards            []string `yaml:"guards"`
	Actions           []string `yaml:"actions"`
	AllowTerminalExit bool     `yaml:"allow_terminal_exit"`
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
	ID               string   `yaml:"id"`
	ExecutionType    string   `yaml:"execution_type"`
	Implementation   string   `yaml:"implementation"`
	SubscribesTo     []string `yaml:"subscribes_to"`
	Produces         []string `yaml:"produces"`
	OwnedTransitions []string `yaml:"owned_transitions"`
	StateTable       string   `yaml:"state_table"`
	IdempotencyTable string   `yaml:"idempotency_table"`
	Timers           []string `yaml:"timers"`
}

type EventCatalogEntry struct {
	Emitter           string   `yaml:"emitter"`
	EmitterType       string   `yaml:"emitter_type"`
	AlternateEmitters []string `yaml:"alternate_emitters"`
	Consumer          any      `yaml:"consumer"`
	ConsumerType      any      `yaml:"consumer_type"`
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

func ResolveEmpireContractPaths(repoRoot string) EmpireContractPaths {
	contractsRoot := filepath.Join(repoRoot, "contracts")
	workflowDir := filepath.Join(contractsRoot, "empire")
	return EmpireContractPaths{
		ContractsRoot:         contractsRoot,
		WorkflowDir:           workflowDir,
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
}

func LoadEmpireWorkflowContractBundle(repoRoot string) (*WorkflowContractBundle, error) {
	paths := ResolveEmpireContractPaths(repoRoot)
	bundle := &WorkflowContractBundle{Paths: paths}
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
	return bundle, nil
}

func (b *WorkflowContractBundle) TransitionIDsByOwner() map[string][]string {
	out := map[string][]string{}
	if b == nil {
		return out
	}
	for _, transition := range b.Workflow.Workflow.Transitions {
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

func EmpireContractFilesExist(repoRoot string) []string {
	paths := ResolveEmpireContractPaths(repoRoot)
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
	missing := make([]string, 0)
	for _, path := range files {
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
