package cliapp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	credentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/store/selected"
)

// One invocation owns one connect attempt. The durable operation owner holds
// registration; this composition retains only its current SDK possession.
type deploymentChannelClient struct {
	mu              sync.Mutex
	store           *selected.Owner
	service         *channelonboarding.Service
	identities      *operatorchannel.Service
	process         *worklifetime.Process
	session         ChannelRegistrationSession
	expectedAccount string
	provider        string
	borrowed        bool
	remote          *cliAPIClient
	holder          *startupownership.Authority
}

type registrationHolder struct {
	store   *selected.Owner
	process *worklifetime.Process
}

func UseDeploymentChannelControl(ctx context.Context, root InvocationRoot, config, directory, provider string, owner *selected.Owner, process *worklifetime.Process, factory ChannelRegistrationFactory) (*deploymentChannelClient, error) {
	if owner == nil || process == nil {
		return nil, channelonboarding.ErrInvalidRequest
	}
	opts := defaultRootCommandOptions()
	opts.invocationRoot, opts.channelRegistration = root, factory
	opts.rootFlags = &rootCommandFlagState{configPath: config, configPathSet: true, swarmDir: directory, swarmDirSet: true}
	return openDeploymentChannelClient(ctx, provider, opts, registrationHolder{owner, process})
}

func openDeploymentChannelClient(ctx context.Context, provider string, opts rootCommandOptions, existing ...registrationHolder) (_ *deploymentChannelClient, err error) {
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
	directory, err := resolveCLISwarmDirFromConfig(root, opts.swarmDirResolutionOptions(), loaded.cli)
	if err != nil {
		return nil, err
	}
	c, err := openDeploymentChannelStore(ctx, provider, opts, loaded, existing...)
	if err != nil {
		return nil, err
	}
	if c.remote != nil {
		return c, nil
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, c.close(context.WithoutCancel(ctx)))
		}
	}()
	if !c.borrowed {
		plans, err := store.GeneratePlatformTableDDLs(spec)
		if err != nil {
			return nil, err
		}
		schema, err := SchemaBootstrapRequest(spec, plans, nil)
		if err != nil {
			return nil, err
		}
		if err := c.store.Schema().BootstrapSchema(ctx, schema); err != nil {
			return nil, err
		}
	}
	return c, c.initializeRegistration(ctx, directory.Path, loaded.Config.Channels.HITL, candidate, projection.ProviderTriggers, opts)
}

func openDeploymentChannelStore(ctx context.Context, provider string, opts rootCommandOptions, loaded RuntimeConfigLoadResult, existing ...registrationHolder) (*deploymentChannelClient, error) {
	root := opts.invocationRoot
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
	c := &deploymentChannelClient{process: worklifetime.NewProcess(), provider: provider}
	if len(existing) == 1 {
		c.store, c.process, c.borrowed = existing[0].store, existing[0].process, true
		return c, nil
	}
	_, statErr := os.Stat(selection.SQLitePath)
	if selection.Backend == storebackend.BackendSQLite && statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return nil, statErr
	}
	if selection.Backend != storebackend.BackendSQLite || statErr == nil {
		inspection, err := selected.OpenAuthorityInspection(ctx, request)
		if err != nil {
			return nil, err
		}
		observed, readErr := inspection.InspectAuthority(ctx)
		if err := errors.Join(readErr, inspection.Close()); err != nil {
			return nil, err
		}
		if observed.Possession == nil {
			return nil, channelonboarding.ErrConflict
		}
		if !observed.Possession.Available {
			if observed.Authority == nil {
				return nil, errors.New("deployment is busy; no authenticated serve holder is available")
			}
			remote, err := newCLIAPIClient(opts)
			if err != nil {
				return nil, err
			}
			return &deploymentChannelClient{remote: remote, holder: observed.Authority, provider: provider}, nil
		}
	}
	c.store, err = selected.OpenRuntime(ctx, selected.RuntimeRequest{Selection: selection, PostgresDSN: request.PostgresDSN})
	return c, err
}

func (c *deploymentChannelClient) initializeRegistration(ctx context.Context, directory string, hitl *channelonboarding.AccountReference, candidate channelonboarding.Candidate, triggers *providertriggers.CatalogSnapshot, opts rootCommandOptions) error {
	owner, provider := c.store, c.provider
	credentialStore, err := CredentialFileStore()
	if err != nil {
		return err
	}
	currentness, err := credentials.NewSnapshotOwner(credentialStore)
	if err != nil {
		return err
	}
	proofs, err := operatorchannel.NewFileProofStore(directory)
	if err != nil {
		return err
	}
	principal, err := owner.OperatorChannels().EnsureOperatorPrincipal(ctx, time.Now().UTC())
	if err != nil {
		return err
	}
	var trigger providertriggers.InboundAdmissionPlan
	switch provider {
	case "whatsapp":
		trigger, err = triggers.CompileAdmission(providertriggers.CompileAdmissionRequest{Provider: provider, Alias: provider})
	case "telegram":
		trigger, err = triggers.CompileRegistrationPolling(provider)
	}
	if err != nil {
		return err
	}
	if opts.channelRegistration == nil {
		return errors.New("channel registration is not installed in this executable")
	}
	c.session, err = opts.channelRegistration(ctx, ChannelRegistrationOptions{Directory: directory, Principal: principal,
		Process: c.process, Store: owner.ChannelOnboarding(), Operations: owner.DeploymentChannels(), Credentials: currentness,
		Identities: owner.OperatorChannels(), HTTP: opts.httpClient, Plan: candidate.Plan, Trigger: trigger})
	if err != nil {
		return err
	}
	c.identities, err = operatorchannel.NewService(owner.OperatorChannels(), proofs, c.session, []operatorchannel.InterfaceIdentity{candidate.Interface}, principal.ID)
	if err != nil {
		return err
	}
	if _, err = c.identities.PreparePrincipal(ctx, time.Now().UTC()); err != nil {
		return err
	}
	writer, err := channelonboarding.NewCredentialWriter(credentialStore)
	if err != nil {
		return err
	}
	c.service, err = channelonboarding.NewService(channelonboarding.ServiceOptions{Deployment: true, OperatorAccount: hitl,
		Store: owner.ChannelOnboarding(), Operations: owner.DeploymentChannels(), Identities: c.identities, Credentials: writer, Sessions: c.session,
		Catalog: func() (*channelonboarding.CandidateCatalog, error) {
			return channelonboarding.NewCandidateCatalog([]channelonboarding.Candidate{candidate})
		}})
	return err
}

func (c *deploymentChannelClient) call(ctx context.Context, method string, params map[string]any, target any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.remote != nil {
		params["scope"], params["holder"], params["provider"] = "deployment", c.holder, c.provider
		if method == "channel.onboarding_start" && c.expectedAccount != "" {
			params["account"] = c.expectedAccount
		}
		return c.remote.call(ctx, method, params, target)
	}
	text := func(key string) string { value, _ := params[key].(string); return value }
	var result any
	var err error
	switch method {
	case "channel.onboarding_select", "channel.onboarding_start":
		expected := c.expectedAccount
		if text("account") != "" {
			expected = text("account")
		}
		save, _ := params["save_proof"].(bool)
		input := channelonboarding.StartInput{Verb: channelonboarding.Verb(text("verb")), SaveProof: save,
			Selection: channelonboarding.CandidateSelection{Provider: text("provider")}, ExpectedAccount: expected,
			IdempotencyKey: text("idempotency_key"), ProviderCredential: text("provider_credential"), ExpectedSelection: text("expected_selection")}
		if method == "channel.onboarding_select" {
			result, err = c.service.Select(ctx, input)
		} else {
			result, err = c.service.Start(ctx, input)
		}
	case "channel.onboarding_get":
		current, readErr := c.service.Get(ctx, text("operation_id"))
		if readErr != nil {
			return readErr
		}
		if current.Operation.Provider == "telegram" && current.IdentityOperation != nil && current.IdentityOperation.State == operatorchannel.StateAwaitingClaim {
			if err := c.session.PollOperatorClaim(ctx, current.Operation); err != nil {
				return err
			}
		}
		result, err = c.service.Get(ctx, text("operation_id"))
	case "channel.onboarding_retry":
		if reverify, _ := params["reverify"].(bool); reverify {
			result, err = c.service.ReverifyDeployment(ctx, text("operation_id"))
			break
		}
		checkpoint, _ := params["checkpoint_only"].(bool)
		result, err = c.service.Retry(ctx, channelonboarding.RetryInput{OperationID: text("operation_id"), CheckpointOnly: checkpoint, ProviderCredential: text("provider_credential")})
	case "channel.confirm":
		revision, _ := params["expected_revision"].(int64)
		approve, _ := params["approve"].(bool)
		op, binding, failure := c.service.ConfirmIdentity(ctx, text("operation_id"), revision, approve, time.Now().UTC())
		result, err = channelOperationEnvelope{Operation: op, Binding: &binding}, failure
	case "channel.logout":
		revision, _ := params["expected_revision"].(int64)
		result, err = c.service.LogoutDeployment(ctx, text("operation_id"), revision)
	case "channel.list":
		rows, failure := c.service.DeploymentAccounts(ctx, c.provider)
		result, err = map[string]any{"accounts": rows}, failure
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

func (c *deploymentChannelClient) close(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.remote != nil {
		return nil
	}
	var err error
	if c.session != nil {
		err = c.session.Close(ctx)
	}
	if c.borrowed {
		return err
	}
	c.process.Retire()
	_, joined := c.process.Join(ctx)
	if joined != nil {
		return errors.Join(err, joined)
	}
	return errors.Join(err, c.store.CloseUnactivated())
}

func (c *deploymentChannelClient) Dispatch(ctx context.Context, method string, params map[string]any, target any) error {
	return c.call(ctx, method, params, target)
}
func (c *deploymentChannelClient) Close(ctx context.Context) error { return c.close(ctx) }

func (c *deploymentChannelClient) ConsumeWebhookClaim(ctx context.Context, account channelonboarding.AccountReference, credential func(channelonboarding.CredentialAdmission) bool, admit func() (providertriggers.Delivery, error), current func() bool) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.session.ConsumeWebhookClaim(ctx, account, credential, admit, current)
}

func (c *deploymentChannelClient) accounts(ctx context.Context) ([]channelonboarding.Result, error) {
	var result struct {
		Accounts []channelonboarding.Result `json:"accounts"`
	}
	err := c.call(ctx, "channel.list", map[string]any{}, &result)
	return result.Accounts, err
}

func (c *deploymentChannelClient) connectAccount(ctx context.Context, out, errOut io.Writer, opts channelConnectOptions) (begun channelOnboardingResult, proceed bool, err error) {
	accounts, err := c.accounts(ctx)
	if err != nil {
		return begun, false, err
	}
	if opts.account != "" {
		ref, err := channelonboarding.CanonicalAccount(c.provider, opts.account)
		if err != nil {
			return begun, false, err
		}
		c.expectedAccount = ref.Account
		operation, err := selectDeploymentAccount(accounts, ref.Account, opts.apiOptions.input, errOut, false)
		if err != nil {
			return begun, false, err
		}
		if err := c.call(ctx, "channel.onboarding_retry", map[string]any{"operation_id": operation.OperationID, "reverify": true}, &begun); err != nil {
			return begun, false, err
		}
		if begun.Operation.Phase == channelonboarding.PhaseRetired && begun.Operation.FailureCode == "native_account_revoked" {
			fmt.Fprintln(out, "Account was logged out from the phone. Link it again:")
			begun = channelOnboardingResult{}
		}
	} else if len(accounts) > 0 && !opts.yes {
		for _, account := range accounts {
			fmt.Fprintf(out, "Account: %s.\n", account.Operation.DeploymentAccount())
		}
		approved, err := confirmChannelAccount(opts.apiOptions.input, errOut, "Link another? [y/N] ")
		if err != nil || !approved {
			return begun, false, err
		}
	}
	return begun, true, nil
}

func runDeploymentChannelList(ctx context.Context, out, errOut io.Writer, opts rootCommandOptions, output cliOutputOptions) error {
	accounts := []channelonboarding.Result{}
	for _, provider := range []string{"telegram", "whatsapp"} {
		client, err := openDeploymentChannelClient(ctx, provider, opts)
		if err != nil {
			return returnCLIValidationError(errOut, err)
		}
		rows, err := client.accounts(ctx)
		err = errors.Join(err, client.close(context.WithoutCancel(ctx)))
		if err != nil {
			return returnCLIValidationError(errOut, err)
		}
		accounts = append(accounts, rows...)
	}
	return renderCLIOutput(out, errOut, output, map[string]any{"accounts": accounts}, func(w io.Writer) {
		for _, row := range accounts {
			state := "pending"
			if row.Operation.AccountConfirmed && row.Operation.Phase == channelonboarding.PhaseSucceeded {
				state = "registered"
			}
			fmt.Fprintf(w, "%s %s · %s\n", row.Operation.Provider, row.Operation.DeploymentAccount(), state)
		}
		if len(accounts) == 0 {
			fmt.Fprintln(w, "No registered accounts. Run swarm channel connect <provider>.")
		}
	}, func() ([]string, error) {
		values := make([]string, 0, len(accounts))
		for _, row := range accounts {
			values = append(values, row.Operation.DeploymentAccount())
		}
		return values, nil
	})
}

func runDeploymentChannelLogout(ctx context.Context, out, errOut io.Writer, provider, account string, opts rootCommandOptions, output cliOutputOptions) (resultErr error) {
	if err := output.validate(); err != nil {
		return returnCLIValidationError(errOut, err)
	}
	if account != "" {
		ref, err := channelonboarding.CanonicalAccount(provider, account)
		if err != nil {
			return returnCLIValidationError(errOut, err)
		}
		account = ref.Account
	}
	client, err := openDeploymentChannelClient(ctx, provider, opts)
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	defer func() { resultErr = errors.Join(resultErr, client.close(context.WithoutCancel(ctx))) }()
	rows, err := client.accounts(ctx)
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	if len(rows) == 0 && provider == "whatsapp" && account == "" {
		if err := client.close(context.WithoutCancel(ctx)); err != nil {
			return returnCLIValidationError(errOut, err)
		}
		return runChannelLogout(ctx, out, errOut, provider, "", opts, output)
	}
	selected, err := selectDeploymentAccount(rows, account, opts.input, errOut, !output.asJSON && !output.asYAML && !output.quiet && controlStdinIsTerminal(opts))
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	if selected.OperationID == "" {
		return nil
	}
	var result channelonboarding.Result
	if err := client.call(ctx, "channel.logout", map[string]any{"operation_id": selected.OperationID, "expected_revision": selected.Revision}, &result); err != nil {
		return returnCLIValidationError(errOut, err)
	}
	if !result.Operation.LogoutCompleted {
		return returnCLIValidationError(errOut, errors.New("logout has not completed"))
	}
	return renderCLIOutput(out, errOut, output, result, func(w io.Writer) {
		fmt.Fprintf(w, "Logged out %s account %s.\n", provider, selected.AccountReference)
	}, func() ([]string, error) { return []string{selected.AccountReference}, nil })
}

func selectDeploymentAccount(rows []channelonboarding.Result, account string, input io.Reader, out io.Writer, interactive bool) (channelonboarding.Operation, error) {
	var selected channelonboarding.Operation
	if account == "" && len(rows) > 1 {
		if !interactive {
			return selected, errors.New("more than one account matches; use swarm channel logout <provider> <account>")
		}
		for _, row := range rows {
			fmt.Fprintf(out, "%s %s\n", row.Operation.Provider, row.Operation.DeploymentAccount())
		}
		if input != nil {
			input = bufio.NewReader(input)
		}
		for _, row := range rows {
			approved, err := confirmChannelAccount(input, out, "Log out "+row.Operation.DeploymentAccount()+"? [y/N] ")
			if err != nil {
				return selected, err
			}
			if approved {
				account = row.Operation.DeploymentAccount()
				break
			}
		}
		if account == "" {
			return selected, nil
		}
	}
	for _, row := range rows {
		if account != "" && row.Operation.DeploymentAccount() != account {
			continue
		}
		if selected.OperationID != "" {
			return selected, errors.New("account registration is ambiguous")
		}
		selected = row.Operation
	}
	if selected.OperationID == "" {
		return selected, errors.New("account is not registered; run swarm channel connect <provider> to link it")
	}
	return selected, nil
}
