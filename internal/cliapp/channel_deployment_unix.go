//go:build linux || darwin

package cliapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	credentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/sessionprovider"
	"github.com/division-sh/swarm/internal/sessionprovider/authority"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/store/selected"
)

// One invocation owns one connect attempt. The durable operation owner holds
// registration; this composition retains only its current SDK possession.
type deploymentChannelClient struct {
	store           *selected.Owner
	service         *channelonboarding.Service
	identities      *operatorchannel.Service
	credentials     *credentials.SnapshotOwner
	process         *worklifetime.Process
	connection      *sessionprovider.RuntimeConnection
	directory       string
	principal       operatorchannel.Principal
	trigger         providertriggers.InboundAdmissionPlan
	expectedAccount string
}

func openDeploymentChannelClient(ctx context.Context, provider string, opts rootCommandOptions) (_ *deploymentChannelClient, err error) {
	root := opts.invocationRoot
	loaded, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: root.Path(), ExplicitPath: rootConfigPath(opts)})
	if err != nil {
		return nil, err
	}
	if !loaded.DeploymentConfigured() {
		return nil, errors.New("No deployment config here: run from the deployment directory or pass --config")
	}
	base, err := LoadConfiguredPlatformPackBase(root.Path(), loaded)
	if err != nil {
		return nil, err
	}
	projectPacks, err := packartifact.LoadProjectPackSet(root.Path())
	if err != nil {
		return nil, err
	}
	inventory, err := packartifact.NewEffectivePackInventory(base, projectPacks.Sources)
	if err != nil {
		return nil, err
	}
	specPath, err := EmbeddedPlatformSpecPath()
	if err != nil {
		return nil, err
	}
	spec, err := loadChannelPlatformSpecDocument(specPath)
	if err != nil {
		return nil, err
	}
	projection, err := packadmission.Admit(inventory, spec)
	if err != nil {
		return nil, err
	}
	var candidate channelonboarding.Candidate
	for _, plan := range projection.ChannelPlans {
		if plan.Provider() != provider {
			continue
		}
		if candidate.Provider != "" {
			return nil, fmt.Errorf("more than one admitted channel implements %s", provider)
		}
		identity, err := plan.InterfaceIdentity()
		if err != nil {
			return nil, err
		}
		profile, ok := plan.OnboardingProfile()
		if !ok {
			return nil, channelonboarding.ErrInvalidRequest
		}
		candidate = channelonboarding.Candidate{DeploymentRegistration: true, Provider: provider, Interface: identity,
			Plan: plan, Posture: channelonboarding.ActivationPosture(profile.ActivationPosture()),
			Ceremony: channelonboarding.CeremonyAccountLink, ProviderCredentialRole: profile.ProviderCredential()}
	}
	if candidate.Provider == "" || candidate.Validate() != nil {
		return nil, fmt.Errorf("no admitted deployment channel for %s", provider)
	}
	catalog, err := channelonboarding.NewCandidateCatalog([]channelonboarding.Candidate{candidate})
	if err != nil {
		return nil, err
	}
	directory, err := resolveCLISwarmDirFromConfig(root, opts.swarmDirResolutionOptions(), loaded.cli)
	if err != nil {
		return nil, err
	}
	selection, err := ResolveRuntimeStoreSelection(root.Path(), "", false, loaded)
	if err != nil {
		return nil, err
	}
	request, err := selectedAuthorityRequest(ctx, selection, loaded.Config)
	if err != nil {
		return nil, err
	}
	if selection.Backend == storebackend.BackendSQLite {
		if err := os.MkdirAll(filepath.Dir(selection.SQLitePath), 0o700); err != nil {
			return nil, err
		}
	}
	owner, err := selected.OpenRuntime(ctx, selected.RuntimeRequest{Selection: selection, PostgresDSN: request.PostgresDSN})
	if err != nil {
		return nil, err
	}
	c := &deploymentChannelClient{store: owner, directory: directory.Path, process: worklifetime.NewProcess()}
	defer func() {
		if err != nil {
			err = errors.Join(err, c.close(context.WithoutCancel(ctx)))
		}
	}()
	plans, err := store.GeneratePlatformTableDDLs(spec)
	if err != nil {
		return nil, err
	}
	schema, err := SchemaBootstrapRequest(spec, plans, nil)
	if err != nil {
		return nil, err
	}
	if err = owner.Schema().BootstrapSchema(ctx, schema); err != nil {
		return nil, err
	}
	credentialStore, err := CredentialFileStore()
	if err != nil {
		return nil, err
	}
	c.credentials, err = credentials.NewSnapshotOwner(credentialStore)
	if err != nil {
		return nil, err
	}
	proofs, err := operatorchannel.NewFileProofStore(directory.Path)
	if err != nil {
		return nil, err
	}
	c.principal, err = owner.OperatorChannels().EnsureOperatorPrincipal(ctx, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	c.identities, err = operatorchannel.NewService(owner.OperatorChannels(), proofs, c, []operatorchannel.InterfaceIdentity{candidate.Interface}, c.principal.ID)
	if err != nil {
		return nil, err
	}
	if _, err = c.identities.PreparePrincipal(ctx, time.Now().UTC()); err != nil {
		return nil, err
	}
	if provider == "whatsapp" {
		c.trigger, err = projection.ProviderTriggers.CompileAdmission(providertriggers.CompileAdmissionRequest{Provider: provider, Alias: provider})
		if err != nil {
			return nil, err
		}
	}
	writer, err := channelonboarding.NewCredentialWriter(credentialStore)
	if err != nil {
		return nil, err
	}
	c.service, err = channelonboarding.NewService(channelonboarding.ServiceOptions{Deployment: true, OperatorAccount: loaded.Config.Channels.HITL,
		Store: owner.ChannelOnboarding(), Operations: owner.DeploymentChannels(), Identities: c.identities, Credentials: writer, Sessions: c,
		Catalog: func() (*channelonboarding.CandidateCatalog, error) { return catalog, nil }})
	return c, err
}

func (c *deploymentChannelClient) call(ctx context.Context, method string, params map[string]any, target any) error {
	text := func(key string) string { value, _ := params[key].(string); return value }
	var result any
	var err error
	switch method {
	case "channel.onboarding_select", "channel.onboarding_start":
		save, _ := params["save_proof"].(bool)
		input := channelonboarding.StartInput{Verb: channelonboarding.Verb(text("verb")), SaveProof: save,
			Selection: channelonboarding.CandidateSelection{Provider: text("provider")}, ExpectedAccount: c.expectedAccount,
			IdempotencyKey: text("idempotency_key"), ProviderCredential: text("provider_credential"), ExpectedSelection: text("expected_selection")}
		if method == "channel.onboarding_select" {
			result, err = c.service.Select(ctx, input)
		} else {
			result, err = c.service.Start(ctx, input)
		}
	case "channel.onboarding_get":
		result, err = c.service.Get(ctx, text("operation_id"))
	case "channel.onboarding_retry":
		checkpoint, _ := params["checkpoint_only"].(bool)
		result, err = c.service.Retry(ctx, channelonboarding.RetryInput{OperationID: text("operation_id"), CheckpointOnly: checkpoint, ProviderCredential: text("provider_credential")})
	case "channel.confirm":
		revision, _ := params["expected_revision"].(int64)
		approve, _ := params["approve"].(bool)
		op, binding, failure := c.service.ConfirmIdentity(ctx, text("operation_id"), revision, approve, time.Now().UTC())
		result, err = channelOperationEnvelope{Operation: op, Binding: &binding}, failure
	default:
		return channelonboarding.ErrInvalidRequest
	}
	if err != nil {
		return err
	}
	raw, err := json.Marshal(result)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, target)
}

func (c *deploymentChannelClient) QualifySessionPlan(candidate channelonboarding.Candidate) error {
	return sessionprovider.QualifyBootstrapPlan(candidate.Plan)
}
func (c *deploymentChannelClient) BootstrapSession(ctx context.Context, op channelonboarding.Operation, candidate channelonboarding.Candidate) error {
	if c.connection != nil {
		return c.connection.CheckBootstrap(ctx)
	}
	var err error
	c.connection, err = sessionprovider.OpenDeploymentConnection(ctx, sessionprovider.DeploymentConnectionOptions{Directory: c.directory,
		OperationID: op.OperationID, Principal: c.principal, Process: c.process, Operations: c.store.DeploymentChannels(),
		Store: c.store.ChannelOnboarding(), Credentials: c.credentials, Plan: candidate.Plan, Trigger: c.trigger})
	if err != nil {
		return err
	}
	return c.connection.Connect(ctx)
}
func (c *deploymentChannelClient) ResumeSession(ctx context.Context, op channelonboarding.Operation, candidate channelonboarding.Candidate) error {
	if c.connection != nil {
		reusable, err := c.connection.CheckSessionReuse(ctx, op)
		if err != nil || reusable {
			return err
		}
		if err := c.connection.Close(ctx); err != nil {
			return err
		}
		c.connection = nil
	}
	return c.BootstrapSession(ctx, op, candidate)
}
func (c *deploymentChannelClient) AwaitSessionBootstrap(ctx context.Context, _ channelonboarding.Operation) error {
	return c.connection.CheckBootstrap(ctx)
}
func (c *deploymentChannelClient) CheckpointSessionPairing(ctx context.Context, op channelonboarding.Operation) (channelonboarding.Operation, bool, error) {
	return c.connection.CheckpointPairing(ctx, op.Revision)
}
func (c *deploymentChannelClient) ReadSessionPairing(ctx context.Context, _ channelonboarding.Operation, principal operatorchannel.Principal) (channelonboarding.PairingReadback, error) {
	if c.connection == nil {
		return channelonboarding.PairingReadback{Status: "not_started"}, nil
	}
	return c.connection.PairingReadback(ctx, principal)
}
func (c *deploymentChannelClient) ReadSessionConnection(ctx context.Context, op channelonboarding.Operation) (channelonboarding.SessionConnectionReadback, error) {
	view := channelonboarding.SessionConnectionReadback{State: "disconnected", Linked: op.AccountReference != "", AccountPresentation: op.AccountReference}
	if c.connection == nil || op.Phase == channelonboarding.PhaseFailed || op.Phase == channelonboarding.PhaseRetired {
		return view, nil
	}
	view.Reason = c.connection.DisconnectionReason()
	if op.SessionAccount.Validate() == nil {
		var err error
		view.AccountPresentation, view.AccountName, err = c.connection.AccountPresentation(ctx)
		if err != nil {
			return view, err
		}
	}
	if view.Reason == "" {
		view.State = "connecting"
		if view.Linked && c.connection.CheckBootstrap(ctx) == nil {
			view.State = "connected"
		}
	}
	return view, nil
}
func (c *deploymentChannelClient) CurrentValueMatchesSeal(ctx context.Context, value credentials.ValueEvidence) (bool, error) {
	return c.credentials.CurrentValueMatchesSeal(ctx, value)
}
func (c *deploymentChannelClient) AdmitSessionAccount(ctx context.Context, account operatorchannel.SessionAccountAdmission) (authority.Admission, error) {
	if c.connection == nil {
		return authority.Admission{}, &operatorchannel.SessionProviderUnavailableError{Provider: account.Provider}
	}
	return c.connection.AdmitSessionAccount(ctx, account)
}
func (c *deploymentChannelClient) close(ctx context.Context) error {
	var err error
	if c.connection != nil {
		err = c.connection.Close(ctx)
	}
	c.process.Retire()
	_, joined := c.process.Join(ctx)
	if joined != nil {
		return errors.Join(err, joined)
	}
	return errors.Join(err, c.store.CloseUnactivated())
}
