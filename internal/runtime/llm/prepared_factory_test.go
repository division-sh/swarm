package llm

import (
	"context"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/sessions"
)

func TestPreparedProviderFactoryHasNoBusinessAuthority(t *testing.T) {
	for _, backend := range []string{selection.BackendAnthropic, selection.BackendClaudeCLI, selection.BackendOpenAICompatible, selection.BackendOpenAIResponses, selection.BackendMock} {
		t.Run(backend, func(t *testing.T) {
			configuredBackend := backend
			if backend == selection.BackendMock {
				configuredBackend = selection.BackendClaudeCLI
			}
			cfg := &config.Config{LLM: config.LLMConfig{Backend: configuredBackend}}
			profile, err := selection.ResolveLiveBackend(configuredBackend)
			if err != nil {
				t.Fatal(err)
			}
			set, err := NewPreparedAgentRuntimeSet(profile, RuntimeFactory{Cfg: cfg})
			if err != nil {
				t.Fatal(err)
			}
			var client Runtime
			if backend == selection.BackendMock {
				var resolved AgentRuntimeResolution
				resolved, err = set.ResolveAgentRuntime(resolvedMockAgent("prepared-mock"))
				client = resolved.Runtime
			} else {
				client, err = set.defaultSlot.get()
			}
			if err != nil {
				t.Fatal(err)
			}
			var controller *effects.Controller
			var sessionStore sessions.Registry
			var live LiveSessionAcquirer
			switch r := client.(type) {
			case *AnthropicAPIRuntime:
				controller, sessionStore, live = r.completionController, r.sessions, r.liveSessions
			case *ClaudeCLIRuntime:
				controller, sessionStore, live = r.completionController, r.sessions, r.liveSessions
			case *OpenAICompatibleRuntime:
				controller, sessionStore, live = r.completionController, r.sessions, r.liveSessions
			case *OpenAIResponsesRuntime:
				controller, sessionStore, live = r.completionController, r.sessions, r.liveSessions
			case *MockRuntime:
				controller, sessionStore, live = r.completionController, r.sessions, r.liveSessions
			default:
				t.Fatalf("unclassified prepared provider %T", client)
			}
			if controller != nil || sessionStore != nil || live != nil {
				t.Fatal("preparation acquired business services")
			}
			if _, _, err := prepareCompletionContext(context.Background(), controller, cfg, &Session{}, ""); err == nil || !strings.Contains(err.Error(), "completion_execution_controller_missing") {
				t.Fatalf("prepared client reached completion admission: %v", err)
			}
			if _, err := (RuntimeFactory{Cfg: cfg}).Build(); err == nil || !strings.Contains(err.Error(), "completion execution controller is required") {
				t.Fatalf("normal construction guard was weakened: %v", err)
			}
		})
	}
}

func TestPreparedProviderFactoryRejectsMockConfiguredDefault(t *testing.T) {
	profile, err := selection.ResolveActiveBackend(selection.BackendMock)
	if err != nil {
		t.Fatal(err)
	}
	factory := RuntimeFactory{Cfg: &config.Config{LLM: config.LLMConfig{Backend: selection.BackendClaudeCLI}}}
	for _, prepared := range []bool{false, true} {
		var set *AgentRuntimeSet
		if prepared {
			set, err = NewPreparedAgentRuntimeSet(profile, factory)
		} else {
			set, err = NewAgentRuntimeSet(profile, factory, nil)
		}
		if set != nil || err == nil || !strings.Contains(err.Error(), "backend mock is retired as a public selector") {
			t.Fatalf("prepared=%t admitted configured mock default: %v", prepared, err)
		}
	}
}

func TestPreparedProviderFactoryRejectsExecutableDependencies(t *testing.T) {
	profile, err := selection.ResolveActiveBackend(selection.BackendClaudeCLI)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"config", "completion", "session", "live_session"} {
		t.Run(name, func(t *testing.T) {
			factory := RuntimeFactory{Cfg: &config.Config{LLM: config.LLMConfig{Backend: profile.ID}}}
			switch name {
			case "config":
				factory.Cfg = nil
			case "completion":
				factory.CompletionController = &effects.Controller{}
			case "session":
				factory.Sessions = sessions.NewInMemoryRegistry(0)
			case "live_session":
				factory.LiveSessions = NewTransientLiveSessionAcquirer(sessions.NewInMemoryRegistry(0))
			}
			if _, err := NewPreparedAgentRuntimeSet(profile, factory); err == nil {
				t.Fatal("prepared construction retained executable dependency")
			}
		})
	}
}
