package cliapp

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/cli/argcount"
	"github.com/spf13/cobra"
)

type channelInterfaceResult struct {
	InterfaceRef        string `json:"interface_ref"`
	ChannelPackID       string `json:"channel_pack_id"`
	ChannelPackVersion  string `json:"channel_pack_version"`
	ChannelManifestHash string `json:"channel_manifest_hash"`
	SemanticGeneration  string `json:"semantic_generation"`
	Selector            string `json:"selector"`
}

type channelOperationResult struct {
	OperationID         string                 `json:"operation_id"`
	Kind                string                 `json:"kind"`
	Interface           channelInterfaceResult `json:"interface"`
	Challenge           string                 `json:"challenge"`
	State               string                 `json:"state"`
	Revision            int64                  `json:"revision"`
	BindingRevision     int64                  `json:"binding_revision"`
	AccountPresentation string                 `json:"account_presentation"`
	ConversationScope   string                 `json:"conversation_scope"`
	ExpiresAt           time.Time              `json:"expires_at"`
}

type channelReadbackResult struct {
	Interface           channelInterfaceResult  `json:"interface"`
	Status              string                  `json:"status"`
	Reason              string                  `json:"reason"`
	BindingRevision     int64                   `json:"binding_revision"`
	AccountPresentation string                  `json:"account_presentation"`
	ConversationScope   string                  `json:"conversation_scope"`
	ProofRevision       int64                   `json:"proof_revision"`
	PendingOperation    *channelOperationResult `json:"pending_operation"`
}

type channelListResult struct {
	PrincipalID string                  `json:"principal_id"`
	Channels    []channelReadbackResult `json:"channels"`
}

type channelOperationEnvelope struct {
	Operation channelOperationResult `json:"operation"`
}

type channelConnectOptions struct {
	apiOptions     rootCommandOptions
	output         cliOutputOptions
	noSave         bool
	replace        bool
	yes            bool
	idempotencyKey string
}

func newChannelCommand(opts rootCommandOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "channel",
		Short: "Connect and inspect verified operator channels.",
		Args:  argcount.ExactArgs(0),
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(newChannelConnectCommand(opts), newChannelListCommand(opts), newChannelUnbindCommand(opts), newChannelProofRevokeCommand(opts))
	return cmd
}

func newChannelConnectCommand(opts rootCommandOptions) *cobra.Command {
	commandOpts := channelConnectOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "connect <interface>",
		Short: "Verify and connect an operator channel account.",
		Args:  argcount.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runChannelConnect(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), args[0], commandOpts)
		},
	}
	cmd.Flags().BoolVar(&commandOpts.noSave, "no-save", false, "Do not save the verified account proof on this machine")
	cmd.Flags().BoolVar(&commandOpts.replace, "replace", false, "Replace the currently bound account after confirmation")
	cmd.Flags().BoolVarP(&commandOpts.yes, "yes", "y", false, "Approve the authenticated claimant without an interactive prompt")
	cmd.Flags().StringVar(&commandOpts.idempotencyKey, "idempotency-key", "", "Optional idempotency key for safe retries (advanced)")
	_ = cmd.Flags().MarkHidden("idempotency-key")
	bindCLIAPIConnectionFlagsWithClass(cmd, &commandOpts.apiOptions, cliAPICommandClassControl, "swarm channel connect")
	bindCLIOutputFlags(cmd, &commandOpts.output)
	return cmd
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
			row, err := selectChannelReadback(list.Channels, args[0])
			if err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			if row.ProofRevision < 1 {
				return returnCLIValidationError(cmd.ErrOrStderr(), fmt.Errorf("channel %s has no machine proof to revoke", args[0]))
			}
			var result struct {
				Proof struct {
					Revision int64 `json:"revision"`
				} `json:"proof"`
			}
			if err := client.call(cmd.Context(), "channel.proof_revoke", map[string]any{"interface": args[0], "expected_revision": row.ProofRevision}, &result); err != nil {
				return returnCLIAPIError(cmd.ErrOrStderr(), err, channelErrorClassifier())
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Revoked %s machine proof at revision %d.\n", row.Interface.ChannelPackID, result.Proof.Revision)
			return nil
		},
	}
	bindCLIAPIConnectionFlagsWithClass(cmd, &commandOpts, cliAPICommandClassControl, "swarm channel revoke-proof")
	return cmd
}

func runChannelConnect(ctx context.Context, out, errOut io.Writer, selector string, opts channelConnectOptions) error {
	if err := opts.output.validate(); err != nil {
		return returnCLIValidationError(errOut, err)
	}
	selector = strings.TrimSpace(selector)
	if selector == "" {
		return returnCLIValidationError(errOut, errors.New("channel interface is required"))
	}
	if !opts.yes && !controlStdinIsTerminal(opts.apiOptions) {
		return returnCLIValidationError(errOut, errors.New("channel claimant confirmation requires a terminal or --yes"))
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, channelErrorClassifier())
	}
	list, err := fetchChannelList(ctx, client)
	if err != nil {
		return returnCLIAPIError(errOut, err, channelErrorClassifier())
	}
	row, err := selectChannelReadback(list.Channels, selector)
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	method := "channel.connect"
	if row.Status == "current" || row.Status == "revoked" {
		method = "channel.reconnect"
		if opts.replace {
			method = "channel.rebind"
		}
	} else if opts.replace {
		return returnCLIValidationError(errOut, errors.New("--replace requires a current channel binding"))
	}
	params := map[string]any{
		"interface": row.Interface.Selector, "expected_revision": row.BindingRevision, "save_proof": !opts.noSave,
	}
	if strings.TrimSpace(opts.idempotencyKey) != "" {
		params["idempotency_key"] = strings.TrimSpace(opts.idempotencyKey)
	}
	var begun channelOperationEnvelope
	if err := client.call(ctx, method, params, &begun); err != nil {
		return returnCLIAPIError(errOut, err, channelErrorClassifier())
	}
	op := begun.Operation
	progressOut := out
	if opts.output.asJSON || opts.output.quiet {
		progressOut = io.Discard
	}
	fmt.Fprintf(progressOut, "Send this exact code through the live %s channel before %s:\n\n  %s\n\nWaiting for an authenticated claimant...\n", row.Interface.ChannelPackID, op.ExpiresAt.Local().Format(time.RFC3339), op.Challenge)
	claimed, err := waitForChannelClaim(ctx, client, op, opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, channelErrorClassifier())
	}
	if claimed.State == "bound" {
		result := channelOperationEnvelope{Operation: claimed}
		human, quiet := channelOperationRenderers(result, row.Interface.ChannelPackID)
		return renderCLIOutput(out, errOut, opts.output, result, human, quiet)
	}
	if claimed.State != "awaiting_confirmation" {
		return returnCLIValidationError(errOut, fmt.Errorf("channel operation ended in %s", claimed.State))
	}
	fmt.Fprintf(progressOut, "Claimed by %s in a %s conversation.\n", claimed.AccountPresentation, claimed.ConversationScope)
	approve := opts.yes
	if !approve {
		approve, err = confirmChannelClaimant(opts.apiOptions.input, errOut)
		if err != nil {
			return returnCLIValidationError(errOut, err)
		}
	}
	var confirmed channelOperationEnvelope
	if err := client.call(ctx, "channel.confirm", map[string]any{"operation_id": claimed.OperationID, "expected_revision": claimed.Revision, "approve": approve}, &confirmed); err != nil {
		return returnCLIAPIError(errOut, err, channelErrorClassifier())
	}
	human, quiet := channelOperationRenderers(confirmed, row.Interface.ChannelPackID)
	return renderCLIOutput(out, errOut, opts.output, confirmed, human, quiet)
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
			selectors = append(selectors, row.Interface.Selector)
		}
		return selectors, nil
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
	row, err := selectChannelReadback(list.Channels, selector)
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	if row.BindingRevision < 1 {
		return returnCLIValidationError(errOut, fmt.Errorf("channel %s has no local binding to unbind", selector))
	}
	params := map[string]any{"interface": row.Interface.Selector, "expected_revision": row.BindingRevision}
	if strings.TrimSpace(idempotencyKey) != "" {
		params["idempotency_key"] = strings.TrimSpace(idempotencyKey)
	}
	var result channelOperationEnvelope
	if err := client.call(ctx, "channel.unbind", params, &result); err != nil {
		return returnCLIAPIError(errOut, err, channelErrorClassifier())
	}
	human, quiet := channelOperationRenderers(result, row.Interface.ChannelPackID)
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
			if row.PendingOperation != nil && row.PendingOperation.OperationID == begun.OperationID {
				if row.PendingOperation.State != "awaiting_claim" {
					return *row.PendingOperation, nil
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

func selectChannelReadback(rows []channelReadbackResult, selector string) (channelReadbackResult, error) {
	selector = strings.TrimSpace(selector)
	matches := []channelReadbackResult{}
	for _, row := range rows {
		if selector == row.Interface.Selector {
			matches = append(matches, row)
		}
	}
	if len(matches) == 0 {
		return channelReadbackResult{}, fmt.Errorf("operator channel interface %q is not active", selector)
	}
	active := matches[:0]
	for _, row := range matches {
		if row.Status != "stale" {
			active = append(active, row)
		}
	}
	if len(active) == 1 {
		return active[0], nil
	}
	if len(active) == 0 && len(matches) == 1 {
		return matches[0], nil
	}
	return channelReadbackResult{}, fmt.Errorf("operator channel interface %q is ambiguous; use the exact selector shown by `swarm channel list`", selector)
}

func writeChannelList(out io.Writer, result channelListResult) {
	rows := make([][]string, 0, len(result.Channels))
	footers := []string{}
	for _, row := range result.Channels {
		account := row.AccountPresentation
		if account == "" {
			account = "-"
		}
		rows = append(rows, []string{row.Interface.ChannelPackID, row.Status, account, fmt.Sprintf("%d", row.BindingRevision), row.ConversationScope, row.Reason, row.Interface.Selector})
		if row.PendingOperation != nil {
			footers = append(footers, fmt.Sprintf("%s pending %s: %s (expires %s)", row.Interface.ChannelPackID, row.PendingOperation.Kind, row.PendingOperation.State, row.PendingOperation.ExpiresAt.Local().Format(time.RFC3339)))
		}
	}
	writeCLITable(out, cliTable{
		Columns: []cliTableColumn{{Header: "PACK"}, {Header: "STATUS"}, {Header: "ACCOUNT"}, {Header: "REVISION"}, {Header: "SCOPE"}, {Header: "REASON"}, {Header: "SELECTOR", KeyColumn: true, IdentifierFamily: cliIdentifierFamilyOperatorChannel}},
		Rows:    rows, EmptyMessage: "No operator channels are active.", FooterLines: footers,
	})
}

func channelErrorClassifier() cliAPIErrorClassifier {
	return cliAPIErrorClassifier{
		notFoundCodes: []string{"CHANNEL_INTERFACE_NOT_FOUND", "CHANNEL_OPERATION_NOT_FOUND"},
		conflictCodes: []string{"CHANNEL_INTERFACE_AMBIGUOUS", "CHANNEL_BINDING_CONFLICT", "CHANNEL_REVISION_CONFLICT", "CHANNEL_OPERATION_TERMINAL", "CHANNEL_PROOF_UNAVAILABLE"},
	}
}
