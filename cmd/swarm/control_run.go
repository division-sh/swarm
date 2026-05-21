package main

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"
)

const (
	controlCommandRunListMethod        = "run.list"
	controlCommandRunPauseMethod       = "run.pause"
	controlCommandRunContinueMethod    = "run.continue"
	controlCommandRunStopMethod        = "run.stop"
	controlCommandRuntimePauseMethod   = "runtime.pause"
	controlCommandRuntimeResumeMethod  = "runtime.resume"
	controlCommandStopAllPageLimit     = 500
	controlCommandStatusRunning        = "running"
	controlCommandStatusPaused         = "paused"
	controlCommandExitCodeValidation   = 2
	controlCommandExitCodeRuntimeError = 3
	controlCommandExitCodeAuth         = 4
	controlCommandExitCodeNotFound     = 5
	controlCommandExitCodeConflict     = 6
)

type controlRunCommandOptions struct {
	apiOptions rootCommandOptions
	action     string
	runMethod  string
	allMethod  string
	all        bool
}

type controlCommandOKResult struct {
	OK bool `json:"ok"`
}

type controlStopAllFailure struct {
	runID string
	err   error
}

func newControlPauseCommand(opts rootCommandOptions) *cobra.Command {
	return newControlRunCommand(controlRunCommandOptions{
		apiOptions: opts,
		action:     "pause",
		runMethod:  controlCommandRunPauseMethod,
		allMethod:  controlCommandRuntimePauseMethod,
	})
}

func newControlContinueCommand(opts rootCommandOptions) *cobra.Command {
	return newControlRunCommand(controlRunCommandOptions{
		apiOptions: opts,
		action:     "continue",
		runMethod:  controlCommandRunContinueMethod,
		allMethod:  controlCommandRuntimeResumeMethod,
	})
}

func newControlStopCommand(opts rootCommandOptions) *cobra.Command {
	return newControlRunCommand(controlRunCommandOptions{
		apiOptions: opts,
		action:     "stop",
		runMethod:  controlCommandRunStopMethod,
	})
}

func newControlRunCommand(opts controlRunCommandOptions) *cobra.Command {
	cmdOpts := opts
	cmd := &cobra.Command{
		Use:   opts.action + " [<run-id>] [--all]",
		Short: fmt.Sprintf("%s a run or the supported all-runs scope through v1 RPC.", controlCommandTitle(opts.action)),
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runOpts := cmdOpts
			return runControlRunCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), args, runOpts)
		},
	}
	cmd.Flags().BoolVar(&cmdOpts.all, "all", false, "Apply the supported all-runs scope for this action")
	bindCLIAPIConnectionFlags(cmd, &cmdOpts.apiOptions)
	return cmd
}

func controlCommandTitle(action string) string {
	if action == "" {
		return ""
	}
	return strings.ToUpper(action[:1]) + action[1:]
}

func runControlRunCommand(ctx context.Context, out, errOut io.Writer, args []string, opts controlRunCommandOptions) error {
	runID, err := validateControlRunTarget(args, opts.all)
	if err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: controlCommandExitCodeValidation}
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: controlCommandErrorExitCode(err)}
	}

	if opts.all {
		if opts.action == "stop" {
			return runControlStopAllCommand(ctx, out, errOut, client)
		}
		if opts.allMethod == "" {
			fmt.Fprintf(errOut, "unsupported all-runs control action %q\n", opts.action)
			return commandExitError{code: controlCommandExitCodeRuntimeError}
		}
		if err := callControlOK(ctx, client, opts.allMethod, map[string]any{}); err != nil {
			fmt.Fprintln(errOut, err)
			return commandExitError{code: controlCommandErrorExitCode(err)}
		}
		writeControlOK(out, opts.action, "runtime", "")
		return nil
	}

	if err := callControlOK(ctx, client, opts.runMethod, map[string]any{"run_id": runID}); err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: controlCommandErrorExitCode(err)}
	}
	writeControlOK(out, opts.action, "run", runID)
	return nil
}

func validateControlRunTarget(args []string, all bool) (string, error) {
	if all {
		if len(args) > 0 {
			return "", fmt.Errorf("--all cannot be combined with a run id")
		}
		return "", nil
	}
	if len(args) != 1 {
		return "", fmt.Errorf("pass a run id or --all")
	}
	runID := strings.TrimSpace(args[0])
	if runID == "" {
		return "", fmt.Errorf("run id is required")
	}
	return runID, nil
}

func callControlOK(ctx context.Context, client *cliAPIClient, method string, params map[string]any) error {
	var result controlCommandOKResult
	if err := client.call(ctx, method, params, &result); err != nil {
		return err
	}
	if !result.OK {
		return fmt.Errorf("malformed %s result: ok must be true", method)
	}
	return nil
}

func runControlStopAllCommand(ctx context.Context, out, errOut io.Writer, client *cliAPIClient) error {
	runIDs, err := listControlStopAllRunIDs(ctx, client)
	if err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: controlCommandErrorExitCode(err)}
	}
	failures := []controlStopAllFailure{}
	stopped := 0
	for _, runID := range runIDs {
		if err := callControlOK(ctx, client, controlCommandRunStopMethod, map[string]any{"run_id": runID}); err != nil {
			failures = append(failures, controlStopAllFailure{runID: runID, err: err})
			continue
		}
		stopped++
	}

	writeControlStopAllResult(out, len(runIDs), stopped, len(failures))
	if len(failures) > 0 {
		writeControlStopAllFailures(errOut, failures)
		return commandExitError{code: controlStopAllExitCode(failures)}
	}
	return nil
}

func listControlStopAllRunIDs(ctx context.Context, client *cliAPIClient) ([]string, error) {
	statuses := []string{controlCommandStatusRunning, controlCommandStatusPaused}
	seenRuns := map[string]struct{}{}
	runIDs := []string{}
	for _, status := range statuses {
		cursor := ""
		seenCursors := map[string]struct{}{}
		for {
			params := map[string]any{
				"status": status,
				"limit":  controlCommandStopAllPageLimit,
			}
			if cursor != "" {
				params["cursor"] = cursor
			}
			result, err := fetchDiagnosticRunList(ctx, client, params)
			if err != nil {
				return nil, err
			}
			for _, run := range result.Runs {
				if run.Status != status {
					return nil, fmt.Errorf("malformed run.list result: status filter %q returned run %s with status %q", status, run.RunID, run.Status)
				}
				runID := strings.TrimSpace(run.RunID)
				if _, ok := seenRuns[runID]; ok {
					continue
				}
				seenRuns[runID] = struct{}{}
				runIDs = append(runIDs, runID)
			}
			nextCursor := strings.TrimSpace(result.NextCursor)
			if nextCursor == "" {
				break
			}
			if _, ok := seenCursors[nextCursor]; ok {
				return nil, fmt.Errorf("malformed run.list result: repeated next_cursor %q for status %s", nextCursor, status)
			}
			seenCursors[nextCursor] = struct{}{}
			cursor = nextCursor
		}
	}
	return runIDs, nil
}

func writeControlOK(out io.Writer, action, scope, runID string) {
	if out == nil {
		return
	}
	fmt.Fprintf(out, "control %s ok: scope=%s", action, scope)
	if runID != "" {
		fmt.Fprintf(out, " run_id=%s", runID)
	}
	fmt.Fprintln(out)
}

func writeControlStopAllResult(out io.Writer, matched, stopped, failed int) {
	if out == nil {
		return
	}
	if failed == 0 {
		fmt.Fprintf(out, "control stop ok: scope=all matched=%d stopped=%d failed=0\n", matched, stopped)
		return
	}
	fmt.Fprintf(out, "control stop partial: scope=all matched=%d stopped=%d failed=%d\n", matched, stopped, failed)
}

func writeControlStopAllFailures(errOut io.Writer, failures []controlStopAllFailure) {
	if errOut == nil {
		return
	}
	for _, failure := range failures {
		fmt.Fprintf(errOut, "control stop failed: run_id=%s error=%v\n", failure.runID, failure.err)
	}
}

func controlStopAllExitCode(failures []controlStopAllFailure) int {
	code := controlCommandExitCodeRuntimeError
	for _, failure := range failures {
		switch controlCommandErrorExitCode(failure.err) {
		case controlCommandExitCodeAuth:
			return controlCommandExitCodeAuth
		case controlCommandExitCodeConflict:
			code = controlCommandExitCodeConflict
		}
	}
	return code
}

func controlCommandErrorExitCode(err error) int {
	return cliAPIErrorExitCode(err, cliAPIErrorClassifier{
		runtimeExit:   controlCommandExitCodeRuntimeError,
		authExit:      controlCommandExitCodeAuth,
		notFoundExit:  controlCommandExitCodeNotFound,
		conflictExit:  controlCommandExitCodeConflict,
		notFoundCodes: []string{"RUN_NOT_FOUND"},
		conflictCodes: []string{"IDEMPOTENCY_CONFLICT"},
	})
}
