package cliapp

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/division-sh/swarm/internal/cli/argcount"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
)

type standingCommandOptions struct {
	apiOptions        rootCommandOptions
	action            string
	reason            string
	idempotencyKey    string
	idempotencyKeySet bool
}

type standingCommandResult struct {
	ServiceID      string `json:"service_id"`
	RunID          string `json:"run_id"`
	Generation     int64  `json:"generation"`
	EffectiveState string `json:"effective_state"`
	Transition     string `json:"transition"`
}

func newStandingCommand(opts rootCommandOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "standing",
		Short: "Control declaration-owned standing services.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(
		newStandingActionCommand(opts, "suspend"),
		newStandingActionCommand(opts, "resume"),
		newStandingActionCommand(opts, "reset"),
	)
	return cmd
}

func newStandingActionCommand(apiOptions rootCommandOptions, action string) *cobra.Command {
	opts := standingCommandOptions{apiOptions: apiOptions, action: action}
	cmd := &cobra.Command{
		Use:   action + " <service-id>",
		Short: standingActionDescription(action),
		Args:  argcount.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			opts.idempotencyKeySet = cmd.Flags().Changed("idempotency-key")
			return runStandingActionCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), args[0], opts)
		},
	}
	cmd.Flags().StringVar(&opts.reason, "reason", "", "Optional operator reason")
	cmd.Flags().StringVar(&opts.idempotencyKey, "idempotency-key", "", "Optional idempotency key for safe retries (advanced)")
	_ = cmd.Flags().MarkHidden("idempotency-key")
	bindCLIAPIConnectionFlagsWithClass(cmd, &opts.apiOptions, cliAPICommandClassControl, "swarm standing "+action)
	return cmd
}

func standingActionDescription(action string) string {
	switch action {
	case "suspend":
		return "Persistently suspend a standing service and quiesce its current work."
	case "resume":
		return "Resume a suspended standing service from its declaration."
	case "reset":
		return "Replace the current standing generation with a fresh generation."
	default:
		return "Control a standing service."
	}
}

func runStandingActionCommand(ctx context.Context, out, errOut io.Writer, rawServiceID string, opts standingCommandOptions) error {
	serviceID, err := uuid.Parse(strings.TrimSpace(rawServiceID))
	if err != nil {
		fmt.Fprintln(errOut, "service id must be a UUID")
		return commandExitError{code: controlCommandExitCodeValidation}
	}
	idempotencyKey, err := optionalNonEmptyFlag("--idempotency-key", opts.idempotencyKey, opts.idempotencyKeySet)
	if err != nil {
		writeCLIAPIError(errOut, err)
		return commandExitError{code: controlCommandExitCodeValidation}
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		writeCLIAPIError(errOut, err)
		return commandExitError{code: controlCommandErrorExitCode(err)}
	}
	params := map[string]any{"service_id": serviceID.String()}
	if reason := strings.TrimSpace(opts.reason); reason != "" {
		params["reason"] = reason
	}
	if idempotencyKey != "" {
		params["idempotency_key"] = idempotencyKey
	}
	var result standingCommandResult
	method := "standing." + opts.action
	if err := client.call(ctx, method, params, &result); err != nil {
		writeCLIAPIError(errOut, err)
		return commandExitError{code: controlCommandErrorExitCode(err)}
	}
	if result.ServiceID == "" || result.RunID == "" || result.Generation <= 0 || result.EffectiveState == "" || result.Transition == "" {
		writeCLIAPIError(errOut, fmt.Errorf("malformed %s result", method))
		return commandExitError{code: controlCommandExitCodeRuntimeError}
	}
	fmt.Fprintf(out, "standing service %s state=%s run=%s generation=%d transition=%s\n", result.ServiceID, result.EffectiveState, result.RunID, result.Generation, result.Transition)
	return nil
}
