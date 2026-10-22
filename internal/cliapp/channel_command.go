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
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/spf13/cobra"
)

type channelOperationResult = operatorchannel.Operation
type channelReadbackResult = channelonboarding.ConnectedChannelReadback

type channelListResult struct {
	PrincipalID string                  `json:"principal_id"`
	Channels    []channelReadbackResult `json:"channels"`
}

func (r channelListResult) linkedSessionOperation(provider string) (string, error) {
	operationID, best, matches := "", -1, 0
	for _, row := range r.Channels {
		if row.Operation == nil || row.Operation.Provider != provider || row.Connection == nil || !row.Connection.Linked ||
			row.Operation.Phase == channelonboarding.PhaseFailed || row.Operation.Phase == channelonboarding.PhaseRetired {
			continue
		}
		rank := 0
		if !row.Operation.Phase.Terminal() {
			rank = 1
		}
		if row.Connection.State == "connected" {
			rank = 2
		}
		if rank > best {
			operationID, best, matches = row.Operation.OperationID, rank, 1
		} else if rank == best && operationID != row.Operation.OperationID {
			matches++
		}
	}
	if matches > 1 {
		return "", errors.New("more than one WhatsApp connection is linked")
	}
	if operationID == "" {
		return "", errors.New("Not linked: run swarm channel connect whatsapp")
	}
	return operationID, nil
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
	source           string
	sourceSet        bool
	interfaceRef     string
	target           string
	idempotencyKey   string
	credentialStdin  bool
	resumeCredential string
	clientLanguage   string
	waitForPairing   bool
}

type channelStatusOptions struct {
	apiOptions rootCommandOptions
	output     cliOutputOptions
	source     string
	sourceSet  bool
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
		newChannelLogoutCommand(opts),
		newChannelProofRevokeCommand(opts),
	)
	return cmd
}

func newChannelLogoutCommand(opts rootCommandOptions) *cobra.Command {
	commandOpts := opts
	var output cliOutputOptions
	var idempotencyKey string
	cmd := &cobra.Command{
		Use:   "logout <provider>",
		Short: "Unlink one exact paired connection and delete its local pairing state.",
		Args:  argcount.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runChannelLogout(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), args[0], idempotencyKey, commandOpts, output)
		},
	}
	cmd.Flags().StringVar(&idempotencyKey, "idempotency-key", "", "Optional idempotency key for safe retries (advanced)")
	_ = cmd.Flags().MarkHidden("idempotency-key")
	bindCLIAPIConnectionFlagsWithClass(cmd, &commandOpts, cliAPICommandClassControl, "swarm channel logout")
	bindCLIOutputFlags(cmd, &output)
	return cmd
}

func runChannelLogout(ctx context.Context, out, errOut io.Writer, operationID, idempotencyKey string, opts rootCommandOptions, output cliOutputOptions) error {
	client, err := newCLIAPIClient(opts)
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	if operationID == "whatsapp" {
		list, err := fetchChannelList(ctx, client)
		if err != nil {
			return returnCLIAPIError(errOut, err, channelErrorClassifier())
		}
		operationID, err = list.linkedSessionOperation("whatsapp")
		if err != nil {
			return returnCLIValidationError(errOut, err)
		}
	}
	var retained channelOnboardingResult
	if err := client.call(ctx, "channel.onboarding_get", map[string]any{"operation_id": operationID}, &retained); err != nil {
		return returnCLIAPIError(errOut, err, channelErrorClassifier())
	}
	if retained.Operation.OperationID != operationID || retained.Operation.Revision < 1 {
		return returnCLIValidationError(errOut, errors.New("channel readback does not identify the requested operation"))
	}
	revision := retained.Operation.Revision
	if retained.Logout != nil {
		if retained.Logout.OperationID != operationID || retained.Logout.ExpectedRevision < 1 {
			return returnCLIValidationError(errOut, errors.New("channel logout readback contradicts the requested operation"))
		}
		revision = retained.Logout.ExpectedRevision
	}
	params := map[string]any{"operation_id": operationID, "expected_revision": revision}
	if strings.TrimSpace(idempotencyKey) != "" {
		params["idempotency_key"] = strings.TrimSpace(idempotencyKey)
	}
	var result channelonboarding.SessionLogoutReadback
	if err := client.call(ctx, "channel.logout", params, &result); err != nil {
		return returnCLIAPIError(errOut, err, channelErrorClassifier())
	}
	if err := result.Validate(); err != nil || result.OperationID != operationID || result.ExpectedRevision != revision {
		return returnCLIValidationError(errOut, errors.New("channel logout response contradicts the requested responsibility"))
	}
	if !output.asJSON && !output.asYAML && !output.quiet && result.Teardown.Phase != channelonboarding.TeardownSucceeded {
		return returnCLIValidationError(errOut, errors.New("logout has not completed; the account has not been reported unlinked"))
	}
	human := func(w io.Writer) {
		if result.Teardown.Phase == channelonboarding.TeardownSucceeded {
			fmt.Fprintln(w, "Logged out; removed from Linked Devices")
		} else {
			fmt.Fprintln(w, "Logout has not completed; the account has not been reported unlinked.")
		}
	}
	quiet := func() ([]string, error) { return []string{result.Teardown.TeardownID}, nil }
	return renderCLIOutput(out, errOut, output, result, human, quiet)
}

func newChannelLifecycleCommand(opts rootCommandOptions, verb string) *cobra.Command {
	commandOpts := channelConnectOptions{apiOptions: opts, verb: verb}
	cmd := &cobra.Command{
		Use:   verb + " <provider>",
		Short: channelLifecycleShort(verb),
		Args:  argcount.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			commandOpts.sourceSet = cmd.Flags().Changed("source")
			return runChannelConnect(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), args[0], commandOpts)
		},
	}
	if verb != "reconnect" {
		cmd.Flags().BoolVar(&commandOpts.noSave, "no-save", false, "Do not save the verified account proof on this machine")
	}
	cmd.Flags().BoolVarP(&commandOpts.yes, "yes", "y", false, "Approve the authenticated claimant without an interactive prompt")
	cmd.Flags().StringVar(&commandOpts.source, "source", "", "Select the exact source directory for this channel")
	cmd.Flags().StringVar(&commandOpts.interfaceRef, "interface", "", "Select the exact pack-qualified channel interface")
	cmd.Flags().StringVar(&commandOpts.target, "target", "", "Select the exact provider activation target")
	cmd.Flags().StringVar(&commandOpts.clientLanguage, "client-language", "", "Declare the Telegram client language to qualify (en or fr); labels remain English")
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
	cmd.Flags().StringVar(&commandOpts.clientLanguage, "client-language", "", "Declare or requalify this operation's client language without reinstalling its native entry")
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
			commandOpts.sourceSet = cmd.Flags().Changed("source")
			return runChannelStatus(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), args[0], commandOpts)
		},
	}
	cmd.Flags().StringVar(&commandOpts.source, "source", "", "Select the exact source directory; pair with --target")
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
	opts.waitForPairing = opts.verb == "connect" && !opts.output.asJSON && !opts.output.asYAML && !opts.output.quiet
	provider = strings.TrimSpace(provider)
	if provider == "" {
		return returnCLIValidationError(errOut, errors.New("channel provider is required"))
	}
	if (opts.verb == "connect" || opts.verb == "rebind") && !opts.yes && !controlStdinIsTerminal(opts.apiOptions) {
		return returnCLIValidationError(errOut, errors.New("channel account confirmation requires a terminal or --yes"))
	}
	bundleHash := ""
	if opts.sourceSet {
		sourceHash, err := admitCLIIdentitySource(opts.apiOptions.invocationRoot, opts.source)
		if err != nil {
			return returnCLIValidationError(errOut, err)
		}
		bundleHash = sourceHash
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, channelErrorClassifier())
	}
	params := map[string]any{
		"provider": provider, "verb": opts.verb, "save_proof": !opts.noSave,
	}
	if opts.clientLanguage != "" {
		params["client_language"] = opts.clientLanguage
	}
	for key, value := range map[string]string{"bundle": bundleHash, "interface": opts.interfaceRef, "target": opts.target} {
		if value = strings.TrimSpace(value); value != "" {
			params[key] = value
		}
	}
	if strings.TrimSpace(opts.idempotencyKey) != "" {
		params["idempotency_key"] = strings.TrimSpace(opts.idempotencyKey)
	}
	begun, err := startCLIChannelOnboarding(ctx, client, params, opts, errOut)
	if err != nil {
		return err
	}
	if opts.waitForPairing && opts.apiOptions.input != nil {
		opts.apiOptions.input = bufio.NewReader(opts.apiOptions.input)
	}
	if opts.waitForPairing && begun.Operation.Posture == channelonboarding.ActivationSessionConnection {
		fmt.Fprintf(out, "Connecting to %s...\n", begun.Operation.Provider)
	}
	progressOut := out
	if opts.output.asJSON || opts.output.quiet {
		progressOut = io.Discard
	}
	result, err := completeChannelOnboarding(ctx, client, begun, opts, progressOut, errOut)
	if err != nil {
		return err
	}
	return renderChannelOnboardingResult(out, errOut, opts.output, result)
}

func runChannelResume(ctx context.Context, out, errOut io.Writer, operationID string, opts channelConnectOptions) error {
	if err := opts.output.validate(); err != nil {
		return returnCLIValidationError(errOut, err)
	}
	operationID = strings.TrimSpace(operationID)
	if operationID == "" {
		return returnCLIValidationError(errOut, errors.New("channel onboarding operation ID is required"))
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, channelErrorClassifier())
	}
	var current channelOnboardingResult
	if err := client.call(ctx, "channel.onboarding_get", map[string]any{"operation_id": operationID}, &current); err != nil {
		return returnCLIAPIError(errOut, err, channelErrorClassifier())
	}
	if current.Operation.OperationID != operationID {
		return returnCLIValidationError(errOut, errors.New("channel readback does not identify the requested operation"))
	}
	if opts.clientLanguage != "" {
		var updated channelOnboardingResult
		if err := client.call(ctx, "channel.onboarding_retry", map[string]any{
			"operation_id": operationID, "client_language": opts.clientLanguage,
			"expected_locale_revision": current.Operation.ClientLocaleRevision,
		}, &updated); err != nil {
			return returnCLIAPIError(errOut, err, channelErrorClassifier())
		}
		current = updated
	}
	now := time.Now()
	if opts.apiOptions.now != nil {
		now = opts.apiOptions.now()
	}
	if channelClaimantConfirmationRequired(current, now) && !opts.yes && !controlStdinIsTerminal(opts.apiOptions) {
		return returnCLIValidationError(errOut, errors.New("channel claimant confirmation requires a terminal or --yes"))
	}
	if opts.credentialStdin {
		credential, readErr := readSelectedChannelCredential(current, opts, errOut)
		if readErr != nil {
			return readErr
		}
		opts.resumeCredential = credential
		defer func() { opts.resumeCredential = "" }()
	}
	progressOut := out
	if opts.output.asJSON || opts.output.quiet {
		progressOut = io.Discard
	}
	result, err := completeChannelOnboarding(ctx, client, current, opts, progressOut, errOut)
	if err != nil {
		return err
	}
	return renderChannelOnboardingResult(out, errOut, opts.output, result)
}

func writeChannelNativeQualification(out io.Writer, result channelOnboardingResult) {
	if result.Readiness == nil || result.Readiness.NativeInbox == nil {
		if result.Readiness == nil || result.Readiness.NativeInboxRequired {
			fmt.Fprintln(out, "Native inbox is not yet qualified; use swarm channel status for current evidence.")
		}
		return
	}
	qualification := result.Readiness.NativeInbox
	fmt.Fprintf(out, "Native inbox qualification: %s (client language %q, locale revision %d).\n",
		qualification.State, qualification.ClientLanguage, qualification.LocaleRevision)
	if qualification.Reason != "" {
		fmt.Fprintln(out, qualification.Reason)
	}
	if result.Operation.Provider == "telegram" && result.Operation.ClientLanguage == "" {
		fmt.Fprintf(out, "Declare the Telegram language used on this device: swarm channel resume %s --client-language <language>.\n", result.Operation.OperationID)
	}
}

func completeChannelOnboarding(ctx context.Context, client *cliAPIClient, result channelOnboardingResult, opts channelConnectOptions, progressOut, errOut io.Writer) (channelOnboardingResult, error) {
	sharedAudienceAnnounced := false
	acceptedAccount := ""
	advancePending := opts.verb == "resume"
	awaitLogin := false
	now := time.Now
	if opts.apiOptions.now != nil {
		now = opts.apiOptions.now
	}
	for {
		pending, err := validateCLIChannelProgress(result)
		if err != nil {
			return channelOnboardingResult{}, returnCLIValidationError(errOut, err)
		}
		if pending && !opts.waitForPairing && !advancePending && opts.resumeCredential == "" {
			return result, nil
		}
		waitingForConnection := result.Operation.Phase == channelonboarding.PhaseSucceeded && result.Connection != nil && result.Connection.Linked
		if opts.waitForPairing {
			if pending || waitingForConnection {
				result, err = waitForChannelOnboardingPairing(ctx, client, result, opts, awaitLogin, progressOut, errOut)
				if err != nil {
					return channelOnboardingResult{}, err
				}
			}
			var previewPending bool
			result, previewPending, err = confirmCLIChannelAccount(ctx, client, result, opts, progressOut, errOut, &acceptedAccount)
			if err != nil {
				return channelOnboardingResult{}, err
			}
			awaitLogin = previewPending
			if previewPending {
				continue
			}
		}
		advancePending = false
		switch result.Operation.Phase {
		case "succeeded", "failed", "retired":
			// The shared result renderer owns READY validation and terminal
			// diagnostics for connect and explicit machine continuations.
			return result, nil
		}
		if identity := result.IdentityOperation; identity != nil {
			observedAt := now()
			switch {
			case identity.State == "awaiting_claim" && (identity.ExpiresAt.IsZero() || identity.ExpiresAt.After(observedAt)):
				fmt.Fprintf(progressOut, "Send the code %s from your operator phone to this number before %s.\nWaiting for an authenticated claimant...\n", identity.Challenge, identity.ExpiresAt.Local().Format(time.RFC3339))
				var err error
				result, err = waitForChannelOnboardingClaim(ctx, client, result, opts.apiOptions)
				if err != nil {
					return channelOnboardingResult{}, returnCLIAPIError(errOut, err, channelErrorClassifier())
				}
				continue
			case channelClaimantConfirmationRequired(result, observedAt):
				err = confirmChannelOnboardingClaim(ctx, client, result, opts, progressOut, errOut, &sharedAudienceAnnounced)
			}
		}
		if err != nil {
			return channelOnboardingResult{}, err
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
		pending, pendingErr := validateCLIChannelContinuation(result, next, false)
		if pendingErr != nil {
			return channelOnboardingResult{}, returnCLIValidationError(errOut, pendingErr)
		}
		if pending && !opts.waitForPairing {
			return next, nil
		}
		result = next
	}
}

func waitForChannelOnboardingPairing(ctx context.Context, client *cliAPIClient, current channelOnboardingResult, opts channelConnectOptions, awaitLogin bool, out, errOut io.Writer) (channelOnboardingResult, error) {
	poll := opts.apiOptions.channelConnectPoll
	if poll <= 0 {
		poll = 500 * time.Millisecond
	}
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	lastQR := ""
	for {
		pending, err := validateCLIChannelProgress(current)
		if err != nil {
			return channelOnboardingResult{}, returnCLIValidationError(errOut, err)
		}
		switch {
		case current.Connection != nil && current.Connection.State == "disconnected" && current.Connection.Reason != "":
			return channelOnboardingResult{}, returnCLIValidationError(errOut, fmt.Errorf("WhatsApp disconnected: %s", cliSanitizeOneLineValue(current.Connection.Reason)))
		case !pending && (current.Operation.Phase != channelonboarding.PhaseSucceeded || current.Connection == nil || !current.Connection.Linked || current.Readiness != nil && current.Readiness.Ready):
			return current, nil
		case pending && current.Pairing.Code != "" && current.Pairing.Code != lastQR:
			if lastQR != "" {
				fmt.Fprintln(out, "QR expired, new QR:")
			}
			if err := renderChannelOnboardingResult(out, errOut, opts.output, current); err != nil {
				return channelOnboardingResult{}, err
			}
			lastQR = current.Pairing.Code
			awaitLogin = false
		}
		select {
		case <-ctx.Done():
			return channelOnboardingResult{}, returnCLIValidationError(errOut, errors.New("channel connection stopped"))
		case <-ticker.C:
		}
		if pending && current.Pairing.Status != channelonboarding.PairingStatusPreparing && current.Pairing.Code == "" && (!awaitLogin || current.Pairing.Connected) {
			break
		}
		var next channelOnboardingResult
		if err := client.call(ctx, "channel.onboarding_get", map[string]any{"operation_id": current.Operation.OperationID}, &next); err != nil {
			return channelOnboardingResult{}, returnCLIAPIError(errOut, err, channelErrorClassifier())
		}
		if next.Operation.OperationID != current.Operation.OperationID || next.Operation.Revision < current.Operation.Revision {
			return channelOnboardingResult{}, returnCLIValidationError(errOut, errors.New("channel progress changed the retained operation"))
		}
		current = next
	}
	return current, nil
}

func confirmCLIChannelAccount(ctx context.Context, client *cliAPIClient, current channelOnboardingResult, opts channelConnectOptions, out, errOut io.Writer, acceptedAccount *string) (channelOnboardingResult, bool, error) {
	// A pairing checkpoint preserves only a preview. Account approval is
	// required before continuing to activation or operator enrollment.
	pending, err := validateCLIChannelProgress(current)
	if err != nil {
		return channelOnboardingResult{}, false, returnCLIValidationError(errOut, err)
	}
	if !pending {
		switch {
		case current.Operation.Posture != channelonboarding.ActivationSessionConnection,
			current.Operation.Phase != channelonboarding.PhaseAwaitingExternalIdentity,
			current.Operation.ActivationRevision != 0, current.IdentityOperation != nil, current.Binding != nil:
			return current, false, nil
		case current.Connection == nil, !current.Connection.Linked, current.Connection.State != "connected",
			current.Readiness != nil && current.Readiness.Ready:
			return channelOnboardingResult{}, false, returnCLIValidationError(errOut, errors.New("the retained account preview has no connected, non-executable pairing"))
		}
	}
	if pending && current.Pairing.Paired && !current.Pairing.Connected {
		var next channelOnboardingResult
		if err := client.call(ctx, "channel.onboarding_retry", map[string]any{
			"operation_id": current.Operation.OperationID, "checkpoint_only": true,
		}, &next); err != nil {
			return channelOnboardingResult{}, false, returnCLIAPIError(errOut, err, channelErrorClassifier())
		}
		pending, err := validateCLIChannelContinuation(current, next, true)
		if err != nil {
			return channelOnboardingResult{}, false, returnCLIValidationError(errOut, err)
		}
		if pending {
			return next, true, nil
		}
		current = next
	}
	account, name := "", ""
	if current.Pairing != nil {
		account, name = current.Pairing.AccountPresentation, current.Pairing.AccountName
	} else if current.Connection != nil {
		account, name = current.Connection.AccountPresentation, current.Connection.AccountName
	}
	if account == "" {
		return channelOnboardingResult{}, false, returnCLIValidationError(errOut, errors.New("the linked account could not be identified; connection is not READY"))
	}
	if account == *acceptedAccount {
		return current, false, nil
	}
	label := cliSanitizeOneLineValue(account)
	if name != "" {
		label += " (" + cliSanitizeOneLineValue(name) + ")"
	}
	fmt.Fprintf(out, "Linked to %s\n", label)
	approve := opts.yes
	if !approve {
		approve, err = confirmChannelAccount(opts.apiOptions.input, errOut, "Is this the right account? [y/N] ")
		if err != nil {
			return channelOnboardingResult{}, false, returnCLIValidationError(errOut, err)
		}
	}
	if approve {
		*acceptedAccount = account
		return current, false, nil
	}
	paired := current
	if current.Operation.Phase == channelonboarding.PhaseActivatingProvider {
		if err := client.call(ctx, "channel.onboarding_retry", map[string]any{"operation_id": current.Operation.OperationID, "checkpoint_only": true}, &paired); err != nil {
			return channelOnboardingResult{}, false, returnCLIAPIError(errOut, err, channelErrorClassifier())
		}
	}
	if paired.Operation.OperationID != current.Operation.OperationID || paired.Operation.Phase != channelonboarding.PhaseAwaitingExternalIdentity ||
		paired.Connection == nil || !paired.Connection.Linked {
		return channelOnboardingResult{}, false, returnCLIValidationError(errOut, errors.New("the rejected account has no exact logout target"))
	}
	if err := runChannelLogout(ctx, out, errOut, current.Operation.OperationID, "", opts.apiOptions, opts.output); err != nil {
		return channelOnboardingResult{}, false, err
	}
	params := map[string]any{"provider": current.Operation.Provider, "verb": "connect", "save_proof": !opts.noSave,
		"bundle": current.Operation.Coordinate.BundleHash, "interface": current.Operation.Interface.Selector, "target": current.Operation.TargetSelector}
	next, err := startCLIChannelOnboarding(ctx, client, params, opts, errOut)
	return next, true, err
}

func confirmChannelOnboardingClaim(ctx context.Context, client *cliAPIClient, result channelOnboardingResult, opts channelConnectOptions, progressOut, errOut io.Writer, sharedAudienceAnnounced *bool) error {
	identity := result.IdentityOperation
	if result.Operation.Provider == "whatsapp" {
		fmt.Fprintf(progressOut, "Code received from %s (%s conversation).\n", cliSanitizeOneLineValue(identity.AccountPresentation), identity.ConversationScope)
	} else {
		fmt.Fprintf(progressOut, "Claimed by %s in a %s conversation.\n", identity.AccountPresentation, identity.ConversationScope)
	}
	if identity.ConversationScope == "shared" && !*sharedAudienceAnnounced {
		fmt.Fprintln(progressOut, "This conversation will receive future notices, decision cards, and their updates. Members of the conversation can see them.")
		*sharedAudienceAnnounced = true
	}
	approve := opts.yes
	var err error
	if !approve {
		approve, err = confirmChannelAccount(opts.apiOptions.input, errOut, "Make them the operator? [y/N] ")
		if err != nil {
			return returnCLIValidationError(errOut, err)
		}
	}
	var confirmed channelOperationEnvelope
	if err := client.call(ctx, "channel.confirm", map[string]any{"operation_id": identity.OperationID, "expected_revision": identity.Revision, "approve": approve}, &confirmed); err != nil {
		return returnCLIAPIError(errOut, err, channelErrorClassifier())
	}
	if !approve {
		var settled channelOnboardingResult
		if err := client.call(ctx, "channel.onboarding_retry", map[string]any{"operation_id": result.Operation.OperationID}, &settled); err != nil {
			return returnCLIAPIError(errOut, err, channelErrorClassifier())
		}
		if settled.Operation.Phase != "failed" && settled.Operation.Phase != "retired" {
			return returnCLIValidationError(errOut, errors.New("channel claimant was rejected but onboarding responsibility did not settle"))
		}
		return returnCLIValidationError(errOut, errors.New("channel claimant was rejected; onboarding failed and its slot was released"))
	}
	return nil
}

func channelClaimantConfirmationRequired(result channelOnboardingResult, now time.Time) bool {
	identity := result.IdentityOperation
	if result.Operation.Phase != "awaiting_operator_confirmation" || identity == nil || identity.State != "awaiting_confirmation" {
		return false
	}
	return identity.ExpiresAt.IsZero() || identity.ExpiresAt.After(now)
}

func waitForChannelOnboardingClaim(ctx context.Context, client *cliAPIClient, begun channelOnboardingResult, opts rootCommandOptions) (channelOnboardingResult, error) {
	continuation := ""
	if begun.Operation.Provider != "whatsapp" {
		continuation = fmt.Sprintf("; resume operation %s with `swarm channel resume %s`", begun.Operation.OperationID, begun.Operation.OperationID)
	}
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
			return channelOnboardingResult{}, fmt.Errorf("%w: stopped waiting for the operator's code%s", ctx.Err(), continuation)
		case <-deadline.C:
			return channelOnboardingResult{}, fmt.Errorf("timed out waiting for the operator's code%s", continuation)
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
	if opts.sourceSet != (strings.TrimSpace(opts.target) != "") {
		return returnCLIValidationError(errOut, errors.New("channel status requires --source and --target together"))
	}
	bundleHash := ""
	if opts.sourceSet {
		sourceHash, err := admitCLIIdentitySource(opts.apiOptions.invocationRoot, opts.source)
		if err != nil {
			return returnCLIValidationError(errOut, err)
		}
		bundleHash = sourceHash
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, channelErrorClassifier())
	}
	result, err := fetchChannelList(ctx, client)
	if err != nil {
		return returnCLIAPIError(errOut, err, channelErrorClassifier())
	}
	row, err := selectChannelActivationReadback(result.Channels, selector, bundleHash, opts.target)
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

func confirmChannelAccount(input io.Reader, out io.Writer, prompt string) (bool, error) {
	if input == nil {
		input = strings.NewReader("")
	}
	fmt.Fprint(out, prompt)
	reader, ok := input.(*bufio.Reader)
	if !ok {
		reader = bufio.NewReader(input)
	}
	line, err := reader.ReadString('\n')
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
		return channelReadbackResult{}, errors.New("channel status requires --source and --target together")
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
	return channelReadbackResult{}, fmt.Errorf("connected channel activation %q is ambiguous; provide --source with its matching source directory and --target from `swarm channel list`", selector)
}

func writeChannelList(out io.Writer, result channelListResult) {
	rows := make([][]string, 0, len(result.Channels))
	footers := []string{}
	wroteSession := false
	for _, row := range result.Channels {
		if row.Identity.Interface.ChannelPackID == "whatsapp" || row.Operation != nil && row.Operation.Provider == "whatsapp" {
			writeWhatsAppConnection(out, row.Connection, false)
			wroteSession = true
			continue
		}
		account := row.Identity.AccountPresentation
		if account == "" {
			account = "-"
		}
		ready, nativeInbox, reason := "-", "-", row.Identity.Reason
		ready, nativeInbox, reason, footers = channelListReadiness(row, ready, nativeInbox, reason, footers)
		if row.Recovery != nil {
			reason = string(row.Recovery.Reason)
			for _, command := range row.Recovery.Commands {
				if row.Operation != nil && !row.Operation.Phase.Terminal() {
					footers = append(footers, fmt.Sprintf("channel %s: onboarding %s (%s) - run %s", row.Recovery.Provider, row.Operation.Phase, row.Operation.OperationID, command))
				} else {
					footers = append(footers, fmt.Sprintf("channel %s: identity verified, activation lost with store - run %s", row.Recovery.Provider, command))
				}
			}
		}
		bundle, target := "-", "-"
		if row.Activation != nil {
			bundle = sourceartifact.ShortHashLabel(row.Activation.Coordinate.BundleHash)
			target = row.Activation.TargetSelector
		} else if row.Operation != nil {
			bundle = sourceartifact.ShortHashLabel(row.Operation.Coordinate.BundleHash)
			target = row.Operation.TargetSelector
		}
		if row.SourceLabel != "" {
			bundle = row.SourceLabel
		}
		rows = append(rows, []string{row.Identity.Interface.ChannelPackID, string(row.Identity.Status), ready, nativeInbox, account, fmt.Sprintf("%d", row.Identity.BindingRevision), string(row.Identity.ConversationScope), reason, bundle, target, row.Identity.Interface.Selector})
		if row.Identity.PendingOperation != nil {
			footers = append(footers, fmt.Sprintf("%s pending %s: %s (expires %s)", row.Identity.Interface.ChannelPackID, row.Identity.PendingOperation.Kind, row.Identity.PendingOperation.State, row.Identity.PendingOperation.ExpiresAt.Local().Format(time.RFC3339)))
		}
	}
	if wroteSession && len(rows) == 0 {
		return
	}
	writeCLITable(out, cliTable{
		Columns: []cliTableColumn{{Header: "PACK"}, {Header: "STATUS"}, {Header: "READY"}, {Header: "NATIVE INBOX"}, {Header: "ACCOUNT"}, {Header: "REVISION"}, {Header: "SCOPE"}, {Header: "REASON"}, {Header: "BUNDLE"}, {Header: "TARGET"}, {Header: "SELECTOR", KeyColumn: true, IdentifierFamily: cliIdentifierFamilyOperatorChannel}},
		Rows:    rows, EmptyMessage: "No operator channels are active.", FooterLines: footers,
	})
}

func channelListReadiness(row channelReadbackResult, ready, nativeInbox, reason string, footers []string) (string, string, string, []string) {
	if row.Readiness != nil {
		ready = fmt.Sprint(row.Readiness.Ready)
		if row.Readiness.NativeInbox != nil {
			qualification := row.Readiness.NativeInbox
			nativeInbox = string(qualification.State)
			if qualification.ClientLanguage != "" {
				nativeInbox += "/" + qualification.ClientLanguage
			}
			if qualification.Reason != "" {
				footers = append(footers, fmt.Sprintf("channel %s native inbox: %s", row.Identity.Interface.ChannelPackID, qualification.Reason))
			}
		}
		if row.Readiness.Reason != "" {
			reason = string(row.Readiness.Reason)
		}
	}
	return ready, nativeInbox, reason, footers
}

func channelErrorClassifier() cliAPIErrorClassifier {
	return cliAPIErrorClassifier{
		notFoundCodes: []string{"CHANNEL_INTERFACE_NOT_FOUND", "CHANNEL_OPERATION_NOT_FOUND"},
		conflictCodes: []string{"CHANNEL_INTERFACE_AMBIGUOUS", "CHANNEL_BINDING_CONFLICT", "CHANNEL_REVISION_CONFLICT", "CHANNEL_OPERATION_TERMINAL", "CHANNEL_PROOF_UNAVAILABLE"},
	}
}
