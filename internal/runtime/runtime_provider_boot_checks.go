package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/mcp"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	"github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

// ManagedProviderBootChecks is process-local evidence, not a capability or a
// durable startup receipt. Its records can only be filled by actual checks.
type ManagedProviderBootChecks struct {
	config  string
	source  string
	image   string
	starts  map[string]json.RawMessage
	keys    map[string]llmselection.Profile
	targets map[string]struct{}
}

func ObserveManagedProviderBootChecks(ctx context.Context, cfg *config.Config, source semanticview.Source, options WorkflowContractValidationOptions, targets []ManagedProviderCheckTarget, discovered map[string]mcp.DiscoveredTool, inspection workspace.Resolver, docker *workspace.DockerManager, listenAddr string) (_ *ManagedProviderBootChecks, retErr error) {
	configHash, err := providerCheckHash(cfg)
	if err != nil {
		return nil, err
	}
	checks := &ManagedProviderBootChecks{config: configHash, starts: map[string]json.RawMessage{}, keys: map[string]llmselection.Profile{}, targets: map[string]struct{}{}}
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil || bundle.SourceArtifact == nil {
		return nil, fmt.Errorf("managed provider startup requires bundle-backed source")
	}
	checks.source = bundle.SourceArtifact.BundleHash()
	credentials := llm.NewProviderCredentialResolver(options.ProviderCredentials)
	var projection *sourceartifact.RuntimeProjection
	defer func() {
		if projection != nil {
			retErr = errors.Join(retErr, projection.Release())
		}
	}()
	for _, target := range targets {
		selection, err := llm.ValidateAgentExecutionDescriptor(options.LLMProfile, target.Actor)
		if err != nil {
			return nil, err
		}
		if selection.Profile.ID == llmselection.BackendMock {
			continue // Mock admission retains its native, non-provider observation.
		}
		captured, credential, err := credentials.Capture(ctx, selection.Profile)
		if err != nil {
			return nil, err
		}
		group, err := providerCheckHash([]string{selection.Profile.Provider, credential.Key, credential.Value})
		if err != nil {
			return nil, err
		}
		model, err := llmselection.ResolveModel(selection.ModelProfile, llmselection.ModelResolution{Model: target.Actor.Model, Models: cfg.LLM.Models})
		if err != nil {
			return nil, err
		}
		if selection.Profile.ID == llmselection.BackendClaudeCLI && projection == nil {
			if docker == nil {
				return nil, llm.ErrClaudeWorkspaceRequired
			}
			bundle, ok := semanticview.Bundle(source)
			if !ok || bundle == nil {
				return nil, fmt.Errorf("managed provider startup requires bundle-backed source")
			}
			projection, err = sourceartifact.MaterializeRuntimeProjection(bundle.SourceArtifact)
			if err != nil {
				return nil, err
			}
			checks.image, err = docker.InspectWorkspaceImage(ctx)
			if err != nil {
				return nil, err
			}
		}
		if selection.Profile.ID == llmselection.BackendClaudeCLI {
			contract, err := llm.ProviderContractForProfile(selection.Profile)
			if err != nil {
				return nil, err
			}
			native := tools.NativeToolAdmissionOptions{ProviderContract: contract, Credentials: options.Credentials, Workspaces: inspection}
			definitions, capabilities, err := tools.PrepareActorToolAdmission(ctx, source, target.Actor, discovered, native)
			if err != nil {
				return nil, err
			}
			planned, err := llm.CompileManagedCapabilityAdmission(target.Actor, contract, definitions, capabilities)
			if err != nil {
				return nil, err
			}
			// Ask the definition producer again; never reconstruct its delivered
			// catalogue from the already-compiled planned names or hashes.
			delivered, _, err := tools.PrepareActorToolAdmission(ctx, source, target.Actor, discovered, native)
			if err != nil {
				return nil, err
			}
			prompt, err := ManagedProviderStartupPrompt(target.Actor)
			if err != nil {
				return nil, err
			}
			response, err := observeManagedProviderBootTarget(ctx, cfg, captured, target, prompt, planned, delivered, docker, projection, checks.image, listenAddr)
			if err != nil {
				return nil, fmt.Errorf("claude cli startup probe failed for agent %s: %w", target.Actor.ID, err)
			}
			key, err := providerStartupCheckKey(cfg, credential, checks.source, target, prompt, planned)
			if err != nil {
				return nil, err
			}
			checks.starts[key], err = json.Marshal(response)
			if err != nil {
				return nil, err
			}
		}
		if _, answered := checks.keys[group]; !answered {
			err := func() (retErr error) {
				var resource *workspace.Target
				if selection.Profile.ID == llmselection.BackendClaudeCLI {
					resource, retErr = docker.PrepareClaudeStartupObservation(ctx, target.Actor, projection)
					if retErr != nil {
						return retErr
					}
					defer func() {
						cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
						defer cancel()
						retErr = errors.Join(retErr, resource.Release(cleanup))
					}()
					if resource.StartupObservationImage() != checks.image {
						return fmt.Errorf("managed provider startup inputs changed")
					}
				}
				return llm.ObserveProviderStartupAnswer(ctx, cfg, selection.Profile, model, captured, resource)
			}()
			if err != nil {
				return nil, err
			}
			checks.keys[group] = selection.Profile
		}
		key, err := providerCheckHash(target)
		if err != nil {
			return nil, err
		}
		checks.targets[key] = struct{}{}
	}
	return checks, ctx.Err()
}

func observeManagedProviderBootTarget(ctx context.Context, cfg *config.Config, credentials llm.ProviderCredentialResolver, target ManagedProviderCheckTarget, prompt string, planned []managedcapabilities.PlannedTool, delivered []llm.ToolDefinition, docker *workspace.DockerManager, projection *sourceartifact.RuntimeProjection, image, listenAddr string) (_ *llm.Response, retErr error) {
	catalogue, err := mcp.NewStartupCatalogue(planned, delivered)
	if err != nil {
		return nil, err
	}
	host, _, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
	if err != nil {
		return nil, err
	}
	server := &http.Server{Handler: catalogue}
	joined := make(chan error, 1)
	go func() { joined <- server.Serve(listener) }()
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		shutdownErr := server.Shutdown(cleanup)
		if shutdownErr != nil {
			shutdownErr = errors.Join(shutdownErr, server.Close())
		}
		retErr = errors.Join(retErr, shutdownErr)
		if err := <-joined; err != nil && !errors.Is(err, http.ErrServerClosed) {
			retErr = errors.Join(retErr, err)
		}
	}()
	hostURL, workspaceURL, err := toolgateway.ListenerEndpoints(listener.Addr())
	if err != nil {
		return nil, err
	}
	if _, err := catalogue.Observation(hostURL).Probe(ctx); err != nil {
		return nil, err
	}
	resource, err := docker.PrepareClaudeStartupObservation(ctx, target.Actor, projection)
	if err != nil {
		return nil, err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		retErr = errors.Join(retErr, resource.Release(cleanup))
	}()
	if resource.StartupObservationImage() != image {
		return nil, fmt.Errorf("managed provider startup inputs changed")
	}
	return llm.ObserveClaudeStartup(ctx, cfg, credentials, target.Actor, prompt, planned, resource, catalogue.Observation(workspaceURL))
}

func (c *ManagedProviderBootChecks) validateWorkspace(ctx context.Context, lifecycle workspace.Lifecycle) error {
	if c.image == "" {
		return nil
	}
	inspection, ok := lifecycle.(interface {
		InspectWorkspaceImage(context.Context) (string, error)
	})
	if !ok {
		return llm.ErrClaudeWorkspaceRequired
	}
	image, err := inspection.InspectWorkspaceImage(ctx)
	if err != nil || image != c.image {
		return errors.Join(fmt.Errorf("managed provider startup inputs changed"), err)
	}
	return nil
}

func (c *ManagedProviderBootChecks) ValidateConfiguration(ctx context.Context, cfg *config.Config, credentials llm.ProviderCredentialResolver) error {
	hash, err := providerCheckHash(cfg)
	if err != nil || c == nil || hash != c.config {
		return errors.Join(fmt.Errorf("managed provider startup inputs changed"), err)
	}
	for expected, profile := range c.keys {
		credential, err := credentials.Resolve(ctx, profile)
		if err != nil {
			return err
		}
		actual, err := providerCheckHash([]string{profile.Provider, credential.Key, credential.Value})
		if err != nil || actual != expected {
			return errors.Join(fmt.Errorf("managed provider startup inputs changed"), err)
		}
	}
	return ctx.Err()
}

func (c *ManagedProviderBootChecks) startupResponse(ctx context.Context, cfg *config.Config, credentials llm.ProviderCredentialResolver, target ManagedProviderCheckTarget, prompt string, planned []managedcapabilities.PlannedTool) (*llm.Response, error) {
	if err := c.ValidateConfiguration(ctx, cfg, credentials); err != nil {
		return nil, err
	}
	profile, err := cfg.LLMBackendProfile()
	if err != nil {
		return nil, err
	}
	selection, err := llm.ValidateAgentExecutionDescriptor(profile, target.Actor)
	if err != nil {
		return nil, err
	}
	credential, err := credentials.Resolve(ctx, selection.Profile)
	if err != nil {
		return nil, err
	}
	key, err := providerStartupCheckKey(cfg, credential, c.source, target, prompt, planned)
	if err != nil {
		return nil, err
	}
	value, found := c.starts[key]
	if !found {
		return nil, fmt.Errorf("managed provider startup inputs changed")
	}
	var response llm.Response
	if err := canonicaljson.DecodePreservingNumberLexemes(value, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func providerStartupCheckKey(cfg *config.Config, credential llm.ProviderCredential, source string, target ManagedProviderCheckTarget, prompt string, planned []managedcapabilities.PlannedTool) (string, error) {
	return providerCheckHash(struct {
		Source      string
		Config      *config.Config
		Target      ManagedProviderCheckTarget
		Prompt      string
		Tools       []managedcapabilities.PlannedTool
		Credential  llm.ProviderCredential
		Environment []string
	}{source, cfg, target, prompt, planned, credential, []string{
		os.Getenv("SWARM_CLAUDE_USE_MCP"), os.Getenv("SWARM_CLAUDE_PERMISSION_MODE"), os.Getenv("SWARM_CLAUDE_BYPASS_PERMISSIONS"),
	}})
}

func (c *ManagedProviderBootChecks) validateTargets(ctx context.Context, cfg *config.Config, credentials llm.ProviderCredentialResolver, candidates []managedProviderPreflightAgent) error {
	if err := c.ValidateConfiguration(ctx, cfg, credentials); err != nil {
		return err
	}
	profile, err := cfg.LLMBackendProfile()
	if err != nil {
		return err
	}
	for _, candidate := range candidates {
		selection, err := llm.ValidateAgentExecutionDescriptor(profile, candidate.config)
		if err != nil {
			return err
		}
		if selection.Profile.ID == llmselection.BackendMock {
			continue
		}
		key, err := providerCheckHash(ManagedProviderCheckTarget{Actor: candidate.config, Plan: candidate.plan})
		if err != nil {
			return err
		}
		if _, found := c.targets[key]; !found {
			return fmt.Errorf("managed provider startup inputs changed")
		}
		credential, err := credentials.Resolve(ctx, selection.Profile)
		if err != nil {
			return err
		}
		group, err := providerCheckHash([]string{selection.Profile.Provider, credential.Key, credential.Value})
		if err != nil {
			return err
		}
		if _, found := c.keys[group]; !found {
			return fmt.Errorf("managed provider startup inputs changed")
		}
	}
	return nil
}

func providerCheckHash(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}
