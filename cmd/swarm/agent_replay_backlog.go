package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/spf13/cobra"
)

const (
	agentReplayBacklogMethod         = "agent.replay_backlog"
	agentReplayBacklogExitValidation = 2
	agentReplayBacklogExitRuntime    = 3
	agentReplayBacklogExitAuth       = 4
	agentReplayBacklogExitNotFound   = 5
	agentReplayBacklogExitConflict   = 6
)

type agentReplayBacklogCommandOptions struct {
	apiOptions     rootCommandOptions
	idempotencyKey string
}

type agentReplayBacklogResult struct {
	OK            bool `json:"ok"`
	ReplayedCount *int `json:"replayed_count"`
}

func newAgentReplayBacklogCommand(opts rootCommandOptions) *cobra.Command {
	replayOpts := agentReplayBacklogCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "replay-backlog <agent-id>",
		Short: "Replay an agent backlog through v1 RPC.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAgentReplayBacklogCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), args, replayOpts)
		},
	}
	cmd.Flags().StringVar(&replayOpts.idempotencyKey, "idempotency-key", "", "Optional v1 API idempotency key")
	return cmd
}

func runAgentReplayBacklogCommand(ctx context.Context, out, errOut io.Writer, args []string, opts agentReplayBacklogCommandOptions) error {
	agentID, err := validateAgentReplayBacklogArgs(args)
	if err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: agentReplayBacklogExitValidation}
	}

	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: agentReplayBacklogErrorExitCode(err)}
	}

	var result agentReplayBacklogResult
	if err := client.call(ctx, agentReplayBacklogMethod, opts.params(agentID), &result); err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: agentReplayBacklogErrorExitCode(err)}
	}
	if err := validateAgentReplayBacklogResult(result); err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: agentReplayBacklogExitRuntime}
	}
	writeAgentReplayBacklogResult(out, agentID, result)
	return nil
}

func validateAgentReplayBacklogArgs(args []string) (string, error) {
	if len(args) != 1 {
		return "", fmt.Errorf("agent replay-backlog requires <agent-id>")
	}
	agentID := strings.TrimSpace(args[0])
	if agentID == "" {
		return "", fmt.Errorf("agent id is required")
	}
	return agentID, nil
}

func (opts agentReplayBacklogCommandOptions) params(agentID string) map[string]any {
	params := map[string]any{"agent_id": agentID}
	if key := strings.TrimSpace(opts.idempotencyKey); key != "" {
		params["idempotency_key"] = key
	}
	return params
}

func validateAgentReplayBacklogResult(result agentReplayBacklogResult) error {
	if !result.OK {
		return fmt.Errorf("malformed agent.replay_backlog result: ok must be true")
	}
	if result.ReplayedCount == nil {
		return fmt.Errorf("malformed agent.replay_backlog result: replayed_count is required")
	}
	if *result.ReplayedCount < 0 {
		return fmt.Errorf("malformed agent.replay_backlog result: replayed_count must be >= 0")
	}
	return nil
}

func writeAgentReplayBacklogResult(out io.Writer, agentID string, result agentReplayBacklogResult) {
	if out == nil {
		return
	}
	fmt.Fprintf(out, "agent replay-backlog ok: agent_id=%s replayed_count=%d\n", agentID, *result.ReplayedCount)
}

func agentReplayBacklogErrorExitCode(err error) int {
	if err == nil {
		return 0
	}
	if strings.Contains(err.Error(), "SWARM_API_TOKEN is required") {
		return agentReplayBacklogExitAuth
	}
	var httpErr *cliAPIHTTPError
	if errors.As(err, &httpErr) {
		if httpErr.statusCode == http.StatusUnauthorized || httpErr.statusCode == http.StatusForbidden {
			return agentReplayBacklogExitAuth
		}
		return agentReplayBacklogExitRuntime
	}
	var rpcErr *jsonRPCError
	if errors.As(err, &rpcErr) {
		switch applicationErrorCode(rpcErr.Data) {
		case "UNAUTHORIZED":
			return agentReplayBacklogExitAuth
		case "AGENT_NOT_FOUND":
			return agentReplayBacklogExitNotFound
		case "IDEMPOTENCY_CONFLICT":
			return agentReplayBacklogExitConflict
		default:
			return agentReplayBacklogExitRuntime
		}
	}
	return agentReplayBacklogExitRuntime
}
