package main

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"
)

const (
	agentRestartMethod         = "agent.restart"
	agentRestartUse            = "restart <agent-id>"
	agentRestartExitValidation = 2
	agentRestartExitRuntime    = 3
	agentRestartExitAuth       = 4
	agentRestartExitNotFound   = 5
	agentRestartExitConflict   = 6
)

type agentRestartCommandOptions struct {
	apiOptions     rootCommandOptions
	idempotencyKey string
}

type agentRestartResult struct {
	OK bool `json:"ok"`
}

func newAgentRestartCommand(opts rootCommandOptions) *cobra.Command {
	restartOpts := agentRestartCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   agentRestartUse,
		Short: "Restart a stuck or failed agent.",
		Args:  cliExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAgentRestartCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), args, restartOpts)
		},
	}
	setCLIArgDiscoveryHint(cmd, "List agent ids with `swarm agent list`.")
	cmd.Flags().StringVar(&restartOpts.idempotencyKey, "idempotency-key", "", "Optional idempotency key for safe retries (advanced)")
	_ = cmd.Flags().MarkHidden("idempotency-key")
	bindCLIAPIConnectionFlagsWithClass(cmd, &restartOpts.apiOptions, cliAPICommandClassMutating, "swarm agent restart")
	return cmd
}

func runAgentRestartCommand(ctx context.Context, out, errOut io.Writer, args []string, opts agentRestartCommandOptions) error {
	agentID, err := validateAgentRestartArgs(args)
	if err != nil {
		writeCLIAPIError(errOut, err)
		return commandExitError{code: agentRestartExitValidation}
	}

	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		writeCLIAPIError(errOut, err)
		return commandExitError{code: agentRestartErrorExitCode(err)}
	}

	var result agentRestartResult
	if err := client.call(ctx, agentRestartMethod, opts.params(agentID), &result); err != nil {
		writeCLIAPIError(errOut, err)
		return commandExitError{code: agentRestartErrorExitCode(err)}
	}
	if err := validateAgentRestartResult(result); err != nil {
		writeCLIAPIError(errOut, err)
		return commandExitError{code: agentRestartExitRuntime}
	}
	writeAgentRestartResult(out, agentID)
	return nil
}

func validateAgentRestartArgs(args []string) (string, error) {
	if len(args) != 1 {
		return "", newCLIArgCountDiagnosticFromUse("swarm agent restart", "restart", agentRestartUse, args, cliArgCountRule{exact: 1}, "List agent ids with `swarm agent list`.")
	}
	agentID := strings.TrimSpace(args[0])
	if agentID == "" {
		return "", fmt.Errorf("agent id is required")
	}
	return agentID, nil
}

func (opts agentRestartCommandOptions) params(agentID string) map[string]any {
	params := map[string]any{"agent_id": agentID}
	if key := strings.TrimSpace(opts.idempotencyKey); key != "" {
		params["idempotency_key"] = key
	}
	return params
}

func validateAgentRestartResult(result agentRestartResult) error {
	if !result.OK {
		return fmt.Errorf("malformed agent.restart result: ok must be true")
	}
	return nil
}

func writeAgentRestartResult(out io.Writer, agentID string) {
	if out == nil {
		return
	}
	fmt.Fprintf(out, "agent restart ok: agent_id=%s\n", agentID)
}

func agentRestartErrorExitCode(err error) int {
	return cliAPIErrorExitCode(err, cliAPIErrorClassifier{
		runtimeExit:   agentRestartExitRuntime,
		authExit:      agentRestartExitAuth,
		notFoundExit:  agentRestartExitNotFound,
		conflictExit:  agentRestartExitConflict,
		notFoundCodes: []string{"AGENT_NOT_FOUND"},
		conflictCodes: []string{"IDEMPOTENCY_CONFLICT"},
	})
}
