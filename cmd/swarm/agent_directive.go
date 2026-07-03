package main

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"
)

const (
	agentDirectiveMethod         = "agent.send_directive"
	agentDirectiveEventType      = "platform.agent_directive"
	agentDirectiveExitValidation = 2
	agentDirectiveExitRuntime    = 3
	agentDirectiveExitAuth       = 4
	agentDirectiveExitNotFound   = 5
	agentDirectiveExitConflict   = 6
)

type agentDirectiveCommandOptions struct {
	apiOptions     rootCommandOptions
	runID          string
	idempotencyKey string
}

type agentDirectiveResult struct {
	OK                 bool   `json:"ok"`
	Response           string `json:"response,omitempty"`
	RunID              string `json:"run_id"`
	RunIDResolution    string `json:"run_id_resolution"`
	DirectiveEventID   string `json:"directive_event_id"`
	DirectiveEventType string `json:"directive_event_type"`
}

var agentDirectiveValidRunIDResolutions = map[string]struct{}{
	"specified":                    {},
	"inferred_from_active_session": {},
	"new_run_allocated":            {},
}

func newAgentDirectiveCommand(opts rootCommandOptions) *cobra.Command {
	directiveOpts := agentDirectiveCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "directive <agent-id> <message>",
		Short: "Send a directive message to a running agent.",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAgentDirectiveCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), args, directiveOpts)
		},
	}
	cmd.Flags().StringVar(&directiveOpts.runID, "run-id", "", "Optional explicit nonterminal run target")
	cmd.Flags().StringVar(&directiveOpts.idempotencyKey, "idempotency-key", "", "Optional idempotency key for safe retries (advanced)")
	_ = cmd.Flags().MarkHidden("idempotency-key")
	bindCLIAPIConnectionFlagsWithClass(cmd, &directiveOpts.apiOptions, cliAPICommandClassMutating, "swarm agent directive")
	return cmd
}

func runAgentDirectiveCommand(ctx context.Context, out, errOut io.Writer, args []string, opts agentDirectiveCommandOptions) error {
	agentID, directive, err := validateAgentDirectiveArgs(args)
	if err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: agentDirectiveExitValidation}
	}

	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: agentDirectiveErrorExitCode(err)}
	}

	var result agentDirectiveResult
	if err := client.call(ctx, agentDirectiveMethod, opts.params(agentID, directive), &result); err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: agentDirectiveErrorExitCode(err)}
	}
	if err := validateAgentDirectiveResult(result); err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: agentDirectiveExitRuntime}
	}
	writeAgentDirectiveResult(out, agentID, result)
	return nil
}

func validateAgentDirectiveArgs(args []string) (string, string, error) {
	if len(args) != 2 {
		return "", "", fmt.Errorf("agent directive requires <agent-id> and <message>")
	}
	agentID := strings.TrimSpace(args[0])
	if agentID == "" {
		return "", "", fmt.Errorf("agent id is required")
	}
	if strings.TrimSpace(args[1]) == "" {
		return "", "", fmt.Errorf("directive is required")
	}
	return agentID, args[1], nil
}

func (opts agentDirectiveCommandOptions) params(agentID, directive string) map[string]any {
	params := map[string]any{
		"agent_id":  agentID,
		"directive": directive,
	}
	if runID := strings.TrimSpace(opts.runID); runID != "" {
		params["run_id"] = runID
	}
	if key := strings.TrimSpace(opts.idempotencyKey); key != "" {
		params["idempotency_key"] = key
	}
	return params
}

func validateAgentDirectiveResult(result agentDirectiveResult) error {
	if !result.OK {
		return fmt.Errorf("malformed agent.send_directive result: ok must be true")
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "run_id", value: result.RunID},
		{name: "run_id_resolution", value: result.RunIDResolution},
		{name: "directive_event_id", value: result.DirectiveEventID},
		{name: "directive_event_type", value: result.DirectiveEventType},
	} {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("malformed agent.send_directive result: %s is required", field.name)
		}
	}
	if _, ok := agentDirectiveValidRunIDResolutions[strings.TrimSpace(result.RunIDResolution)]; !ok {
		return fmt.Errorf("malformed agent.send_directive result: run_id_resolution=%q is not valid", result.RunIDResolution)
	}
	if strings.TrimSpace(result.DirectiveEventType) != agentDirectiveEventType {
		return fmt.Errorf("malformed agent.send_directive result: directive_event_type=%q, want %q", result.DirectiveEventType, agentDirectiveEventType)
	}
	return nil
}

func writeAgentDirectiveResult(out io.Writer, agentID string, result agentDirectiveResult) {
	if out == nil {
		return
	}
	fmt.Fprintf(out, "agent directive ok: agent_id=%s run_id=%s run_id_resolution=%s directive_event_id=%s directive_event_type=%s\n",
		agentID,
		result.RunID,
		result.RunIDResolution,
		result.DirectiveEventID,
		result.DirectiveEventType,
	)
	if response := strings.TrimSpace(result.Response); response != "" {
		fmt.Fprintf(out, "response=%s\n", response)
	}
}

func agentDirectiveErrorExitCode(err error) int {
	return cliAPIErrorExitCode(err, cliAPIErrorClassifier{
		runtimeExit:   agentDirectiveExitRuntime,
		authExit:      agentDirectiveExitAuth,
		notFoundExit:  agentDirectiveExitNotFound,
		conflictExit:  agentDirectiveExitConflict,
		notFoundCodes: []string{"AGENT_NOT_FOUND", "RUN_NOT_FOUND"},
		conflictCodes: []string{
			"AGENT_NOT_RUNNING",
			"RUN_ALREADY_TERMINAL",
			"AMBIGUOUS_RUN_TARGET",
			"IDEMPOTENCY_CONFLICT",
		},
	})
}
