package cliapp

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cli/argcount"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/spf13/cobra"
)

type channelInterfaceResult = operatorchannel.InterfaceIdentity
type channelOperationResult = operatorchannel.Operation
type channelReadbackResult = channelonboarding.ConnectedChannelReadback

type channelListResult struct {
	PrincipalID string                  `json:"principal_id"`
	Channels    []channelReadbackResult `json:"channels"`
}

type channelOperationEnvelope struct {
	Operation channelOperationResult   `json:"operation"`
	Binding   *operatorchannel.Binding `json:"binding,omitempty"`
}

type channelConnectOptions struct {
	apiOptions       rootCommandOptions
	output           cliOutputOptions
	verb             string
	noSave           bool
	yes              bool
	bundle           string
	interfaceRef     string
	target           string
	idempotencyKey   string
	credentialStdin  bool
	resumeCredential string
}

type channelStatusOptions struct {
	apiOptions rootCommandOptions
	output     cliOutputOptions
	bundle     string
	target     string
}

type channelOnboardingResult = channelonboarding.Result

func newChannelCommand(opts rootCommandOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "channel",
		Short: "Connect and inspect verified operator channels.",
		Args:  argcount.ExactArgs(0),
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(
		newChannelLifecycleCommand(opts, "connect"),
		newChannelLifecycleCommand(opts, "reconnect"),
		newChannelLifecycleCommand(opts, "rebind"),
		newChannelResumeCommand(opts),
		newChannelListCommand(opts),
		newChannelStatusCommand(opts),
		newChannelUnbindCommand(opts),
		newChannelProofRevokeCommand(opts),
	)
	return cmd
}

func newChannelLifecycleCommand(opts rootCommandOptions, verb string) *cobra.Command {
	commandOpts := channelConnectOptions{apiOptions: opts, verb: verb}
	cmd := &cobra.Command{
		Use:   verb + " <provider>",
		Short: channelLifecycleShort(verb),
		Args:  argcount.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runChannelConnect(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), args[0], commandOpts)
		},
	}
	if verb != "reconnect" {
		cmd.Flags().BoolVar(&commandOpts.noSave, "no-save", false, "Do not save the verified account proof on this machine")
	}
	cmd.Flags().BoolVarP(&commandOpts.yes, "yes", "y", false, "Approve the authenticated claimant without an interactive prompt")
	cmd.Flags().StringVar(&commandOpts.bundle, "bundle", "", "Select the exact bundle hash")
	cmd.Flags().StringVar(&commandOpts.interfaceRef, "interface", "", "Select the exact pack-qualified channel interface")
	cmd.Flags().StringVar(&commandOpts.target, "target", "", "Select the exact provider activation target")
	cmd.Flags().StringVar(&commandOpts.idempotencyKey, "idempotency-key", "", "Optional idempotency key for safe retries (advanced)")
	_ = cmd.Flags().MarkHidden("idempotency-key")
	if verb == "reconnect" || verb == "rebind" {
		cmd.Flags().BoolVar(&commandOpts.credentialStdin, "credential-stdin", false, "Read an explicit replacement provider credential from hidden input or stdin")
	}
	bindCLIAPIConnectionFlagsWithClass(cmd, &commandOpts.apiOptions, cliAPICommandClassControl, "swarm channel "+verb)
	bindCLIOutputFlags(cmd, &commandOpts.output)
	return cmd
}

func newChannelResumeCommand(opts rootCommandOptions) *cobra.Command {
	commandOpts := channelConnectOptions{apiOptions: opts, verb: "resume"}
	cmd := &cobra.Command{
		Use:   "resume <operation-id>",
		Short: "Resume one exact durable channel onboarding operation.",
		Args:  argcount.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runChannelResume(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), args[0], commandOpts)
		},
	}
	cmd.Flags().BoolVarP(&commandOpts.yes, "yes", "y", false, "Approve the authenticated claimant without an interactive prompt")
	cmd.Flags().BoolVar(&commandOpts.credentialStdin, "credential-stdin", false, "Read an explicit replacement provider credential from hidden input or stdin")
	bindCLIAPIConnectionFlagsWithClass(cmd, &commandOpts.apiOptions, cliAPICommandClassControl, "swarm channel resume")
	bindCLIOutputFlags(cmd, &commandOpts.output)
	return cmd
}

func channelLifecycleShort(verb string) string {
	switch verb {
	case "reconnect":
		return "Repair a provider channel while preserving its verified identity."
	case "rebind":
		return "Replace a provider channel's verified identity and activation."
	default:
		return "Connect a provider channel end to end."
	}
}

func newChannelListCommand(opts rootCommandOptions) *cobra.Command {
	commandOpts := opts
	var output cliOutputOptions
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List operator channel bindings.",
		Args:  argcount.ExactArgs(0),
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runChannelList(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), commandOpts, output)
		},
	}
	bindCLIAPIConnectionFlagsWithClass(cmd, &commandOpts, cliAPICommandClassReadOnly, "swarm channel list")
	bindCLIOutputFlags(cmd, &output)
	return cmd
}

func newChannelStatusCommand(opts rootCommandOptions) *cobra.Command {
	commandOpts := channelStatusOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "status <interface>",
		Short: "Inspect one exact connected-channel activation.",
		Args:  argcount.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runChannelStatus(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), args[0], commandOpts)
		},
	}
	cmd.Flags().StringVar(&commandOpts.bundle, "bundle", "", "Select the exact bundle hash")
	cmd.Flags().StringVar(&commandOpts.target, "target", "", "Select the exact provider activation target")
	bindCLIAPIConnectionFlagsWithClass(cmd, &commandOpts.apiOptions, cliAPICommandClassReadOnly, "swarm channel status")
	bindCLIOutputFlags(cmd, &commandOpts.output)
	return cmd
}

func newChannelUnbindCommand(opts rootCommandOptions) *cobra.Command {
	commandOpts := opts
	var output cliOutputOptions
	var idempotencyKey string
	cmd := &cobra.Command{
		Use:   "unbind <interface>",
		Short: "Remove one local operator channel binding.",
		Args:  argcount.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runChannelUnbind(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), args[0], idempotencyKey, commandOpts, output)
		},
	}
	cmd.Flags().StringVar(&idempotencyKey, "idempotency-key", "", "Optional idempotency key for safe retries (advanced)")
	_ = cmd.Flags().MarkHidden("idempotency-key")
	bindCLIAPIConnectionFlagsWithClass(cmd, &commandOpts, cliAPICommandClassControl, "swarm channel unbind")
	bindCLIOutputFlags(cmd, &output)
	return cmd
}

func newChannelProofRevokeCommand(opts rootCommandOptions) *cobra.Command {
	commandOpts := opts
	cmd := &cobra.Command{
		Use:    "revoke-proof <interface>",
		Short:  "Revoke a machine-local verified account proof.",
		Hidden: true,
		Args:   argcount.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := newCLIAPIClient(commandOpts)
			if err != nil {
				return returnCLIAPIError(cmd.ErrOrStderr(), err, channelErrorClassifier())
			}
			list, err := fetchChannelList(cmd.Context(), client)
			if err != nil {
				return returnCLIAPIError(cmd.ErrOrStderr(), err, channelErrorClassifier())
			}
			row, err := selectChannelIdentityReadback(list.Channels, args[0])
			if err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			if row.Identity.ProofRevision < 1 {
				return returnCLIValidationError(cmd.ErrOrStderr(), fmt.Errorf("channel %s has no machine proof to revoke", args[0]))
			}
			var result struct {
				Proof struct {
					Revision int64 `json:"revision"`
				} `json:"proof"`
			}
			if err := client.call(cmd.Context(), "channel.proof_revoke", map[string]any{"interface": args[0], "expected_revision": row.Identity.ProofRevision}, &result); err != nil {
				return returnCLIAPIError(cmd.ErrOrStderr(), err, channelErrorClassifier())
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Revoked %s machine proof at revision %d.\n", row.Identity.Interface.ChannelPackID, result.Proof.Revision)
			return nil
		},
	}
	bindCLIAPIConnectionFlagsWithClass(cmd, &commandOpts, cliAPICommandClassControl, "swarm channel revoke-proof")
	return cmd
}

func runChannelConnect(ctx context.Context, out, errOut io.Writer, provider string, opts channelConnectOptions) error {
	if err := opts.output.validate(); err != nil {
		return returnCLIValidationError(errOut, err)
	}
	provider = strings.TrimSpace(provider)
	if provider == "" {
		return returnCLIValidationError(errOut, errors.New("channel provider is required"))
	}
	if (opts.verb == "connect" || opts.verb == "rebind") && !opts.yes && !controlStdinIsTerminal(opts.apiOptions) {
		return returnCLIValidationError(errOut, errors.New("channel claimant confirmation requires a terminal or --yes"))
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, channelErrorClassifier())
	}
	params := map[string]any{
		"provider": provider, "verb": opts.verb, "save_proof": !opts.noSave,
	}
	for key, value := range map[string]string{"bundle": opts.bundle, "interface": opts.interfaceRef, "target": opts.target} {
		if value = strings.TrimSpace(value); value != "" {
			params[key] = value
		}
	}
	if strings.TrimSpace(opts.idempotencyKey) != "" {
		params["idempotency_key"] = strings.TrimSpace(opts.idempotencyKey)
	}
	if opts.verb == "connect" || opts.credentialStdin {
		credential, readErr := readSecretValue(opts.apiOptions.input, errOut, false)
		if readErr != nil {
			return returnCLIValidationError(errOut, fmt.Errorf("read %s provider credential: %w", provider, readErr))
		}
		params["provider_credential"] = credential
		defer delete(params, "provider_credential")
	}
	var begun channelOnboardingResult
	if err := client.call(ctx, "channel.onboarding_start", params, &begun); err != nil {
		return returnCLIAPIError(errOut, err, channelErrorClassifier())
	}
	progressOut := out
	if opts.output.asJSON || opts.output.quiet {
		progressOut = io.Discard
	}
	result, err := completeChannelOnboarding(ctx, client, begun, opts, progressOut, errOut)
	if err != nil {
		return err
	}
	return renderCLIOutput(out, errOut, opts.output, result, func(w io.Writer) {
		fmt.Fprintf(w, "Connected %s channel READY at activation revision %d.\n", result.Operation.Provider, result.Operation.ActivationRevision)
	}, func() ([]string, error) {
		return []string{result.Operation.OperationID}, nil
	})
}

func runChannelResume(ctx context.Context, out, errOut io.Writer, operationID string, opts channelConnectOptions) error {
	if err := opts.output.validate(); err != nil {
		return returnCLIValidationError(errOut, err)
	}
	operationID = strings.TrimSpace(operationID)
	if operationID == "" {
		return returnCLIValidationError(errOut, errors.New("channel onboarding operation ID is required"))
	}
	if !opts.yes && !controlStdinIsTerminal(opts.apiOptions) {
		return returnCLIValidationError(errOut, errors.New("channel claimant confirmation requires a terminal or --yes"))
	}
	if opts.credentialStdin {
		credential, readErr := readSecretValue(opts.apiOptions.input, errOut, false)
		if readErr != nil {
			return returnCLIValidationError(errOut, fmt.Errorf("read replacement provider credential: %w", readErr))
		}
		opts.resumeCredential = credential
		defer func() { opts.resumeCredential = "" }()
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, channelErrorClassifier())
	}
	var current channelOnboardingResult
	if err := client.call(ctx, "channel.onboarding_get", map[string]any{"operation_id": operationID}, &current); err != nil {
		return returnCLIAPIError(errOut, err, channelErrorClassifier())
	}
	progressOut := out
	if opts.output.asJSON || opts.output.quiet {
		progressOut = io.Discard
	}
	result, err := completeChannelOnboarding(ctx, client, current, opts, progressOut, errOut)
	if err != nil {
		return err
	}
	return renderCLIOutput(out, errOut, opts.output, result, func(w io.Writer) {
		fmt.Fprintf(w, "Connected %s channel READY at activation revision %d.\n", result.Operation.Provider, result.Operation.ActivationRevision)
	}, func() ([]string, error) {
		return []string{result.Operation.OperationID}, nil
	})
}

func completeChannelOnboarding(ctx context.Context, client *cliAPIClient, result channelOnboardingResult, opts channelConnectOptions, progressOut, errOut io.Writer) (channelOnboardingResult, error) {
	identityAnnounced := false
	for {
		switch result.Operation.Phase {
		case "succeeded":
			if result.Readiness == nil || !result.Readiness.Ready {
				reason := "connected readiness evidence is unavailable"
				if result.Readiness != nil && strings.TrimSpace(string(result.Readiness.Reason)) != "" {
					reason = string(result.Readiness.Reason)
				}
				return channelOnboardingResult{}, returnCLIValidationError(errOut, fmt.Errorf("channel onboarding succeeded historically but the channel is not READY: %s", reason))
			}
			return result, nil
		case "failed", "retired":
			return channelOnboardingResult{}, returnCLIValidationError(errOut, fmt.Errorf("channel onboarding ended in %s: %s", result.Operation.Phase, result.Operation.FailureMessage))
		}
		if identity := result.IdentityOperation; identity != nil {
			now := time.Now()
			if opts.apiOptions.now != nil {
				now = opts.apiOptions.now()
			}
			overdue := !identity.ExpiresAt.IsZero() && !identity.ExpiresAt.After(now) &&
				(identity.State == "awaiting_claim" || identity.State == "awaiting_confirmation")
			if !overdue && identity.State == "awaiting_claim" {
				if !identityAnnounced {
					fmt.Fprintf(progressOut, "Send this exact code through the live %s channel before %s:\n\n  %s\n\nWaiting for an authenticated claimant...\n", result.Operation.Provider, identity.ExpiresAt.Local().Format(time.RFC3339), identity.Challenge)
					identityAnnounced = true
				}
				var err error
				result, err = waitForChannelOnboardingClaim(ctx, client, result, opts.apiOptions)
				if err != nil {
					return channelOnboardingResult{}, returnCLIAPIError(errOut, err, channelErrorClassifier())
				}
				continue
			}
			if !overdue && identity.State == "awaiting_confirmation" {
				fmt.Fprintf(progressOut, "Claimed by %s in a %s conversation.\n", identity.AccountPresentation, identity.ConversationScope)
				approve := opts.yes
				var err error
				if !approve {
					approve, err = confirmChannelClaimant(opts.apiOptions.input, errOut)
					if err != nil {
						return channelOnboardingResult{}, returnCLIValidationError(errOut, err)
					}
				}
				var confirmed channelOperationEnvelope
				if err := client.call(ctx, "channel.confirm", map[string]any{"operation_id": identity.OperationID, "expected_revision": identity.Revision, "approve": approve}, &confirmed); err != nil {
					return channelOnboardingResult{}, returnCLIAPIError(errOut, err, channelErrorClassifier())
				}
				if !approve {
					var settled channelOnboardingResult
					if err := client.call(ctx, "channel.onboarding_retry", map[string]any{"operation_id": result.Operation.OperationID}, &settled); err != nil {
						return channelOnboardingResult{}, returnCLIAPIError(errOut, err, channelErrorClassifier())
					}
					if settled.Operation.Phase != "failed" && settled.Operation.Phase != "retired" {
						return channelOnboardingResult{}, returnCLIValidationError(errOut, errors.New("channel claimant was rejected but onboarding responsibility did not settle"))
					}
					return channelOnboardingResult{}, returnCLIValidationError(errOut, errors.New("channel claimant was rejected; onboarding failed and its slot was released"))
				}
			}
		}
		params := map[string]any{"operation_id": result.Operation.OperationID}
		if opts.resumeCredential != "" {
			params["provider_credential"] = opts.resumeCredential
		}
		var next channelOnboardingResult
		if err := client.call(ctx, "channel.onboarding_retry", params, &next); err != nil {
			return channelOnboardingResult{}, returnCLIAPIError(errOut, err, channelErrorClassifier())
		}
		delete(params, "provider_credential")
		opts.resumeCredential = ""
		if next.Operation.Revision == result.Operation.Revision && next.Operation.Phase == result.Operation.Phase {
			return channelOnboardingResult{}, returnCLIValidationError(errOut, fmt.Errorf("channel onboarding is blocked in %s; retry after resolving the reported prerequisite", next.Operation.Phase))
		}
		result = next
	}
}

func waitForChannelOnboardingClaim(ctx context.Context, client *cliAPIClient, begun channelOnboardingResult, opts rootCommandOptions) (channelOnboardingResult, error) {
	wait := opts.channelConnectWait
	if wait <= 0 {
		wait = 2 * time.Minute
	}
	poll := opts.channelConnectPoll
	if poll <= 0 {
		poll = 500 * time.Millisecond
	}
	deadline := time.NewTimer(wait)
	defer deadline.Stop()
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	for {
		var current channelOnboardingResult
		if err := client.call(ctx, "channel.onboarding_get", map[string]any{"operation_id": begun.Operation.OperationID}, &current); err != nil {
			return channelOnboardingResult{}, err
		}
		if current.IdentityOperation == nil || current.IdentityOperation.State != "awaiting_claim" {
			return current, nil
		}
		select {
		case <-ctx.Done():
			return channelOnboardingResult{}, ctx.Err()
		case <-deadline.C:
			return channelOnboardingResult{}, fmt.Errorf("timed out waiting for channel claim; resume operation %s with `swarm channel resume %s`", begun.Operation.OperationID, begun.Operation.OperationID)
		case <-ticker.C:
		}
	}
}

func runChannelList(ctx context.Context, out, errOut io.Writer, opts rootCommandOptions, output cliOutputOptions) error {
	if err := output.validate(); err != nil {
		return returnCLIValidationError(errOut, err)
	}
	client, err := newCLIAPIClient(opts)
	if err != nil {
		return returnCLIAPIError(errOut, err, channelErrorClassifier())
	}
	result, err := fetchChannelList(ctx, client)
	if err != nil {
		return returnCLIAPIError(errOut, err, channelErrorClassifier())
	}
	return renderCLIOutput(out, errOut, output, result, func(w io.Writer) {
		writeChannelList(w, result)
	}, func() ([]string, error) {
		selectors := make([]string, 0, len(result.Channels))
		for _, row := range result.Channels {
			selectors = append(selectors, row.Identity.Interface.Selector)
		}
		return selectors, nil
	})
}

func runChannelStatus(ctx context.Context, out, errOut io.Writer, selector string, opts channelStatusOptions) error {
	if err := opts.output.validate(); err != nil {
		return returnCLIValidationError(errOut, err)
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, channelErrorClassifier())
	}
	result, err := fetchChannelList(ctx, client)
	if err != nil {
		return returnCLIAPIError(errOut, err, channelErrorClassifier())
	}
	row, err := selectChannelActivationReadback(result.Channels, selector, opts.bundle, opts.target)
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	return renderCLIOutput(out, errOut, opts.output, row, func(w io.Writer) {
		writeChannelList(w, channelListResult{PrincipalID: result.PrincipalID, Channels: []channelReadbackResult{row}})
	}, func() ([]string, error) {
		return []string{row.Activation.ActivationID}, nil
	})
}

func runChannelUnbind(ctx context.Context, out, errOut io.Writer, selector, idempotencyKey string, opts rootCommandOptions, output cliOutputOptions) error {
	if err := output.validate(); err != nil {
		return returnCLIValidationError(errOut, err)
	}
	client, err := newCLIAPIClient(opts)
	if err != nil {
		return returnCLIAPIError(errOut, err, channelErrorClassifier())
	}
	list, err := fetchChannelList(ctx, client)
	if err != nil {
		return returnCLIAPIError(errOut, err, channelErrorClassifier())
	}
	row, err := selectChannelIdentityReadback(list.Channels, selector)
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	if row.Identity.BindingRevision < 1 {
		return returnCLIValidationError(errOut, fmt.Errorf("channel %s has no local binding to unbind", selector))
	}
	params := map[string]any{"interface": row.Identity.Interface.Selector, "expected_revision": row.Identity.BindingRevision}
	if strings.TrimSpace(idempotencyKey) != "" {
		params["idempotency_key"] = strings.TrimSpace(idempotencyKey)
	}
	var result channelOperationEnvelope
	if err := client.call(ctx, "channel.unbind", params, &result); err != nil {
		return returnCLIAPIError(errOut, err, channelErrorClassifier())
	}
	human, quiet := channelOperationRenderers(result, row.Identity.Interface.ChannelPackID)
	return renderCLIOutput(out, errOut, output, result, human, quiet)
}

func channelOperationRenderers(result channelOperationEnvelope, packID string) (func(io.Writer), func() ([]string, error)) {
	human := func(w io.Writer) {
		switch result.Operation.State {
		case "rejected":
			fmt.Fprintln(w, "Claim rejected; no channel binding was created.")
		case "unbound":
			fmt.Fprintf(w, "Unbound %s at revision %d. The machine proof was not revoked.\n", packID, result.Operation.BindingRevision)
		default:
			fmt.Fprintf(w, "Connected %s to %s at binding revision %d.\n", packID, result.Operation.AccountPresentation, result.Operation.BindingRevision)
		}
	}
	quiet := func() ([]string, error) {
		return []string{result.Operation.OperationID}, nil
	}
	return human, quiet
}

func waitForChannelClaim(ctx context.Context, client *cliAPIClient, begun channelOperationResult, opts rootCommandOptions) (channelOperationResult, error) {
	wait := opts.channelConnectWait
	if wait <= 0 {
		wait = 2 * time.Minute
	}
	poll := opts.channelConnectPoll
	if poll <= 0 {
		poll = 500 * time.Millisecond
	}
	deadline := time.NewTimer(wait)
	defer deadline.Stop()
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	for {
		list, err := fetchChannelList(ctx, client)
		if err != nil {
			return channelOperationResult{}, err
		}
		for _, row := range list.Channels {
			if row.Identity.PendingOperation != nil && row.Identity.PendingOperation.OperationID == begun.OperationID {
				if row.Identity.PendingOperation.State != "awaiting_claim" {
					return *row.Identity.PendingOperation, nil
				}
			}
		}
		select {
		case <-ctx.Done():
			return channelOperationResult{}, ctx.Err()
		case <-deadline.C:
			return channelOperationResult{}, fmt.Errorf("timed out waiting for channel claim; the operation remains visible in `swarm channel list` until %s", begun.ExpiresAt.Local().Format(time.RFC3339))
		case <-ticker.C:
		}
	}
}

func confirmChannelClaimant(input io.Reader, out io.Writer) (bool, error) {
	if input == nil {
		input = strings.NewReader("")
	}
	fmt.Fprint(out, "Confirm this account? [y/N] ")
	line, err := bufio.NewReader(input).ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return false, fmt.Errorf("read claimant confirmation: %w", err)
	}
	answer := strings.ToLower(strings.TrimSpace(line))
	return answer == "y" || answer == "yes", nil
}

func fetchChannelList(ctx context.Context, client *cliAPIClient) (channelListResult, error) {
	var result channelListResult
	err := client.call(ctx, "channel.list", map[string]any{}, &result)
	return result, err
}

func selectChannelIdentityReadback(rows []channelReadbackResult, selector string) (channelReadbackResult, error) {
	selector = strings.TrimSpace(selector)
	matches := []channelReadbackResult{}
	for _, row := range rows {
		if selector == row.Identity.Interface.Selector {
			matches = append(matches, row)
		}
	}
	if len(matches) == 0 {
		return channelReadbackResult{}, fmt.Errorf("operator channel interface %q is not active", selector)
	}
	active := matches[:0]
	for _, row := range matches {
		if row.Identity.Status != "stale" {
			active = append(active, row)
		}
	}
	if len(active) == 1 {
		return active[0], nil
	}
	if len(active) > 1 {
		first := active[0].Identity
		for _, row := range active[1:] {
			if !sameChannelIdentityReadback(first, row.Identity) {
				return channelReadbackResult{}, fmt.Errorf("operator channel interface %q has contradictory retained identities", selector)
			}
		}
		return active[0], nil
	}
	if len(active) == 0 && len(matches) == 1 {
		return matches[0], nil
	}
	return channelReadbackResult{}, fmt.Errorf("operator channel interface %q is ambiguous", selector)
}

func sameChannelIdentityReadback(left, right operatorchannel.Readback) bool {
	return left.PrincipalID == right.PrincipalID && left.Interface.Normalized() == right.Interface.Normalized() &&
		left.Status == right.Status && left.BindingRevision == right.BindingRevision && left.ExternalAccountRef == right.ExternalAccountRef &&
		left.ConversationRef == right.ConversationRef && left.ConversationScope == right.ConversationScope &&
		left.AccountPresentation == right.AccountPresentation && left.Source == right.Source && left.ProofID == right.ProofID &&
		left.ProofRevision == right.ProofRevision && left.ProofStatus == right.ProofStatus
}

func selectChannelActivationReadback(rows []channelReadbackResult, selector, bundle, target string) (channelReadbackResult, error) {
	selector, bundle, target = strings.TrimSpace(selector), strings.TrimSpace(bundle), strings.TrimSpace(target)
	if (bundle == "") != (target == "") {
		return channelReadbackResult{}, errors.New("channel status requires --bundle and --target together")
	}
	matches := []channelReadbackResult{}
	for _, row := range rows {
		if row.Identity.Interface.Selector != selector || row.Activation == nil || row.Activation.Status != channelonboarding.ActivationCurrent {
			continue
		}
		if bundle != "" && (row.Activation.Coordinate.BundleHash != bundle || row.Activation.TargetSelector != target) {
			continue
		}
		matches = append(matches, row)
	}
	if len(matches) == 1 {
		return matches[0], nil
	}
	if len(matches) == 0 {
		return channelReadbackResult{}, fmt.Errorf("connected channel activation %q is not active", selector)
	}
	return channelReadbackResult{}, fmt.Errorf("connected channel activation %q is ambiguous; provide --bundle and --target from `swarm channel list`", selector)
}

func writeChannelList(out io.Writer, result channelListResult) {
	rows := make([][]string, 0, len(result.Channels))
	footers := []string{}
	for _, row := range result.Channels {
		account := row.Identity.AccountPresentation
		if account == "" {
			account = "-"
		}
		ready, reason := "-", row.Identity.Reason
		if row.Readiness != nil {
			ready = fmt.Sprint(row.Readiness.Ready)
			if row.Readiness.Reason != "" {
				reason = string(row.Readiness.Reason)
			}
		}
		if row.Recovery != nil {
			reason = string(row.Recovery.Reason)
			for _, command := range row.Recovery.Commands {
				footers = append(footers, fmt.Sprintf("channel %s: identity verified, activation lost with store - run %s", row.Recovery.Provider, command))
			}
		}
		bundle, target := "-", "-"
		if row.Activation != nil {
			bundle = row.Activation.Coordinate.BundleHash
			target = row.Activation.TargetSelector
		}
		rows = append(rows, []string{row.Identity.Interface.ChannelPackID, string(row.Identity.Status), ready, account, fmt.Sprintf("%d", row.Identity.BindingRevision), string(row.Identity.ConversationScope), reason, bundle, target, row.Identity.Interface.Selector})
		if row.Identity.PendingOperation != nil {
			footers = append(footers, fmt.Sprintf("%s pending %s: %s (expires %s)", row.Identity.Interface.ChannelPackID, row.Identity.PendingOperation.Kind, row.Identity.PendingOperation.State, row.Identity.PendingOperation.ExpiresAt.Local().Format(time.RFC3339)))
		}
	}
	writeCLITable(out, cliTable{
		Columns: []cliTableColumn{{Header: "PACK"}, {Header: "STATUS"}, {Header: "READY"}, {Header: "ACCOUNT"}, {Header: "REVISION"}, {Header: "SCOPE"}, {Header: "REASON"}, {Header: "BUNDLE"}, {Header: "TARGET"}, {Header: "SELECTOR", KeyColumn: true, IdentifierFamily: cliIdentifierFamilyOperatorChannel}},
		Rows:    rows, EmptyMessage: "No operator channels are active.", FooterLines: footers,
	})
}

func channelErrorClassifier() cliAPIErrorClassifier {
	return cliAPIErrorClassifier{
		notFoundCodes: []string{"CHANNEL_INTERFACE_NOT_FOUND", "CHANNEL_OPERATION_NOT_FOUND"},
		conflictCodes: []string{"CHANNEL_INTERFACE_AMBIGUOUS", "CHANNEL_BINDING_CONFLICT", "CHANNEL_REVISION_CONFLICT", "CHANNEL_OPERATION_TERMINAL", "CHANNEL_PROOF_UNAVAILABLE"},
	}
}
