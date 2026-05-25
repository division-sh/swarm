package llm

import (
	"context"
	"fmt"
	"strings"
)

type ProviderTransport string

const (
	ProviderTransportAPI ProviderTransport = "api"
	ProviderTransportCLI ProviderTransport = "cli"
)

type BudgetUsageAccounting string

const (
	BudgetUsageExact     BudgetUsageAccounting = "exact"
	BudgetUsageEstimated BudgetUsageAccounting = "estimated"
)

type ProviderContract struct {
	RuntimeMode string
	Provider    string
	Transport   ProviderTransport

	ToolSchema       ProviderToolSchemaContract
	SessionLifecycle ProviderSessionLifecycleContract
	Response         ProviderResponseContract
	NativeTools      ProviderNativeToolContract
	Persistence      ProviderPersistenceContract
	Budget           ProviderBudgetContract
}

type ProviderToolSchemaContract struct {
	ValidatesInputSchemas bool
	TranslatesTools       bool
	ReturnsToolResults    bool
}

type ProviderSessionLifecycleContract struct {
	StartsSessions            bool
	ContinuesSessions         bool
	SupportsConversationModes bool
	ProviderSessionIDStrategy string
	RotatesSessions           bool
	PreservesRetryLineage     bool
}

type ProviderResponseContract struct {
	NormalizesMessages     bool
	NormalizesToolCalls    bool
	PreservesRawResponse   bool
	NormalizesVisibleTools bool
	StreamingParser        string
}

type ProviderNativeToolContract struct {
	Capabilities                NativeToolCapabilities
	StrictProviderNativeSupport bool
	FallbackToolsAllowed        bool
	StartupVisibleSurfaceProbe  bool
}

type ProviderPersistenceContract struct {
	PersistsTurns                 bool
	PersistsConversationSnapshots bool
	PersistsTaskModeAudit         bool
}

type ProviderBudgetContract struct {
	UsageAccounting BudgetUsageAccounting
}

type ProviderContractProvider interface {
	ProviderContract() ProviderContract
}

type ConversationSnapshotPersister interface {
	PersistConversationSnapshot(ctx context.Context, s *Session) error
}

func ProviderContractForRuntime(runtime Runtime) (ProviderContract, bool) {
	provider, ok := runtime.(ProviderContractProvider)
	if !ok || provider == nil {
		return ProviderContract{}, false
	}
	return provider.ProviderContract(), true
}

func RequireProviderContract(runtimeMode string, runtime Runtime) (ProviderContract, error) {
	if runtime == nil {
		return ProviderContract{}, fmt.Errorf("llm runtime is required")
	}
	contract, ok := ProviderContractForRuntime(runtime)
	if !ok {
		return ProviderContract{}, fmt.Errorf("llm runtime %T does not expose provider contract", runtime)
	}
	if err := contract.Validate(); err != nil {
		return ProviderContract{}, err
	}
	if contract.Persistence.PersistsConversationSnapshots {
		if _, ok := runtime.(ConversationSnapshotPersister); !ok {
			return ProviderContract{}, fmt.Errorf("llm runtime %T declares conversation snapshot persistence but does not implement it", runtime)
		}
	}
	if contract.NativeTools.StartupVisibleSurfaceProbe {
		if _, ok := runtime.(StartupVisibleToolSurfaceProber); !ok {
			return ProviderContract{}, fmt.Errorf("llm runtime %T declares startup visible tool probe but does not implement it", runtime)
		}
	}
	if want := strings.TrimSpace(runtimeMode); want != "" && contract.RuntimeMode != want {
		return ProviderContract{}, fmt.Errorf("llm runtime mode %q exposes provider contract for %q", want, contract.RuntimeMode)
	}
	return contract, nil
}

func (c ProviderContract) Validate() error {
	if strings.TrimSpace(c.RuntimeMode) == "" {
		return fmt.Errorf("llm provider contract runtime mode is required")
	}
	if strings.TrimSpace(c.Provider) == "" {
		return fmt.Errorf("llm provider contract provider is required")
	}
	switch c.Transport {
	case ProviderTransportAPI, ProviderTransportCLI:
	default:
		return fmt.Errorf("llm provider contract %s has unsupported transport %q", c.RuntimeMode, c.Transport)
	}
	if !c.ToolSchema.ValidatesInputSchemas {
		return fmt.Errorf("llm provider contract %s must validate provider input schemas", c.RuntimeMode)
	}
	if !c.ToolSchema.TranslatesTools {
		return fmt.Errorf("llm provider contract %s must translate platform tools to provider tools", c.RuntimeMode)
	}
	if !c.ToolSchema.ReturnsToolResults {
		return fmt.Errorf("llm provider contract %s must return platform tool results to the provider", c.RuntimeMode)
	}
	if !c.SessionLifecycle.StartsSessions || !c.SessionLifecycle.ContinuesSessions {
		return fmt.Errorf("llm provider contract %s must own start and continue session behavior", c.RuntimeMode)
	}
	if !c.SessionLifecycle.SupportsConversationModes {
		return fmt.Errorf("llm provider contract %s must preserve conversation mode semantics", c.RuntimeMode)
	}
	if strings.TrimSpace(c.SessionLifecycle.ProviderSessionIDStrategy) == "" {
		return fmt.Errorf("llm provider contract %s must declare provider session id strategy", c.RuntimeMode)
	}
	if !c.SessionLifecycle.RotatesSessions {
		return fmt.Errorf("llm provider contract %s must account for session rotation", c.RuntimeMode)
	}
	if !c.SessionLifecycle.PreservesRetryLineage {
		return fmt.Errorf("llm provider contract %s must preserve retry lineage", c.RuntimeMode)
	}
	if !c.Response.NormalizesMessages || !c.Response.NormalizesToolCalls {
		return fmt.Errorf("llm provider contract %s must normalize provider messages and tool calls", c.RuntimeMode)
	}
	if !c.Response.PreservesRawResponse {
		return fmt.Errorf("llm provider contract %s must preserve raw provider response", c.RuntimeMode)
	}
	if strings.TrimSpace(c.Response.StreamingParser) == "" {
		return fmt.Errorf("llm provider contract %s must declare response parser strategy", c.RuntimeMode)
	}
	if c.NativeTools.StrictProviderNativeSupport && c.NativeTools.FallbackToolsAllowed {
		return fmt.Errorf("llm provider contract %s cannot allow fallback native tools in strict provider-native mode", c.RuntimeMode)
	}
	if !c.Persistence.PersistsTurns {
		return fmt.Errorf("llm provider contract %s must persist turns", c.RuntimeMode)
	}
	if !c.Persistence.PersistsConversationSnapshots {
		return fmt.Errorf("llm provider contract %s must persist conversation snapshots", c.RuntimeMode)
	}
	if !c.Persistence.PersistsTaskModeAudit {
		return fmt.Errorf("llm provider contract %s must preserve task-mode audit", c.RuntimeMode)
	}
	switch c.Budget.UsageAccounting {
	case BudgetUsageExact, BudgetUsageEstimated:
	default:
		return fmt.Errorf("llm provider contract %s must declare budget usage accounting", c.RuntimeMode)
	}
	return nil
}

func NativeToolCapabilitiesForRuntime(runtime Runtime) NativeToolCapabilities {
	contract, ok := ProviderContractForRuntime(runtime)
	if !ok {
		return NativeToolCapabilities{}
	}
	return contract.NativeTools.Capabilities
}

func RuntimeEnforcesProviderNativeTools(runtime Runtime) bool {
	contract, ok := ProviderContractForRuntime(runtime)
	return ok && contract.NativeTools.StrictProviderNativeSupport
}

func StartupVisibleToolSurfaceProberForRuntime(runtime Runtime) (StartupVisibleToolSurfaceProber, bool) {
	contract, ok := ProviderContractForRuntime(runtime)
	if !ok || !contract.NativeTools.StartupVisibleSurfaceProbe {
		return nil, false
	}
	prober, ok := runtime.(StartupVisibleToolSurfaceProber)
	return prober, ok && prober != nil
}

func PersistConversationSnapshotForRuntime(ctx context.Context, runtime Runtime, session *Session) error {
	if session == nil {
		return nil
	}
	contract, ok := ProviderContractForRuntime(runtime)
	if !ok || !contract.Persistence.PersistsConversationSnapshots {
		return nil
	}
	persister, ok := runtime.(ConversationSnapshotPersister)
	if !ok || persister == nil {
		return fmt.Errorf("llm runtime %T declares conversation snapshot persistence but does not implement it", runtime)
	}
	return persister.PersistConversationSnapshot(ctx, session)
}
