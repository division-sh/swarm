package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"
)

const (
	runtimeNukeMethod        = "runtime.nuke"
	runtimeNukeStatusDryRun  = "dry_run"
	runtimeNukeStatusDone    = "completed"
	runtimeNukeStatusPartial = "partial_failure"
)

type runtimeNukeCommandOptions struct {
	apiOptions rootCommandOptions
	yes        bool
	dryRun     bool
}

type runtimeNukeResult struct {
	OK             bool                     `json:"ok"`
	Status         string                   `json:"status"`
	DryRun         bool                     `json:"dry_run"`
	OperationName  string                   `json:"operation_name"`
	Plan           runtimeNukePlanResult    `json:"plan"`
	Quiescence     runtimeNukeQuiescence    `json:"quiescence"`
	Cleanup        runtimeNukeCleanup       `json:"cleanup"`
	Containers     runtimeNukeContainers    `json:"containers"`
	PartialFailure bool                     `json:"partial_failure"`
	Errors         []runtimeNukeResultError `json:"errors,omitempty"`
}

type runtimeNukePlanResult struct {
	Plan runtimeNukePlan `json:"plan"`
}

type runtimeNukePlan struct {
	ActiveRuns       []runtimeNukeRunRef       `json:"active_runs"`
	ActiveDeliveries []runtimeNukeDeliveryRef  `json:"active_deliveries"`
	RunScopedTables  []runtimeNukeTableRef     `json:"run_scoped_tables"`
	EntityContainers []runtimeNukeContainerRef `json:"entity_containers"`
}

type runtimeNukeRunRef struct {
	RunID  string `json:"run_id"`
	Status string `json:"status"`
}

type runtimeNukeDeliveryRef struct {
	DeliveryID string `json:"delivery_id"`
	RunID      string `json:"run_id"`
	Status     string `json:"status"`
}

type runtimeNukeTableRef struct {
	Name   string `json:"name"`
	Action string `json:"action"`
}

type runtimeNukeQuiescence struct {
	Runs       []runtimeNukeQuiescedRun      `json:"runs"`
	Deliveries []runtimeNukeQuiescedDelivery `json:"deliveries"`
}

type runtimeNukeQuiescedRun struct {
	RunID   string `json:"run_id"`
	Status  string `json:"status"`
	Changed bool   `json:"changed"`
}

type runtimeNukeQuiescedDelivery struct {
	DeliveryID string `json:"delivery_id"`
	Status     string `json:"status"`
	Changed    bool   `json:"changed"`
}

type runtimeNukeCleanup struct {
	Tables []runtimeNukeCleanupTable `json:"tables"`
}

type runtimeNukeCleanupTable struct {
	Table       string `json:"table"`
	MatchedRows int64  `json:"matched_rows"`
	DeletedRows int64  `json:"deleted_rows"`
}

type runtimeNukeContainers struct {
	Selected       []runtimeNukeContainerRef     `json:"selected"`
	Preserved      []runtimeNukeContainerRef     `json:"preserved"`
	Missing        []runtimeNukeContainerRef     `json:"missing"`
	AlreadyStopped []runtimeNukeContainerRef     `json:"already_stopped"`
	Stopped        []runtimeNukeContainerRef     `json:"stopped"`
	Failed         []runtimeNukeContainerFailure `json:"failed"`
}

type runtimeNukeContainerRef struct {
	Name string `json:"name"`
	Kind string `json:"kind"`
}

type runtimeNukeContainerFailure struct {
	Container runtimeNukeContainerRef `json:"container"`
	Error     string                  `json:"error"`
}

type runtimeNukeResultError struct {
	Scope   string `json:"scope"`
	Message string `json:"message"`
}

func newControlNukeCommand(opts rootCommandOptions) *cobra.Command {
	nukeOpts := runtimeNukeCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "nuke",
		Short: "Destructively reset Swarm runtime state through v1 RPC.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runControlNukeCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), nukeOpts)
		},
	}
	cmd.Flags().BoolVarP(&nukeOpts.yes, "yes", "y", false, "Skip the destructive confirmation prompt")
	cmd.Flags().BoolVar(&nukeOpts.dryRun, "dry-run", false, "Preview the destructive reset without applying it")
	bindCLIAPIConnectionFlags(cmd, &nukeOpts.apiOptions)
	return cmd
}

func runControlNukeCommand(ctx context.Context, out, errOut io.Writer, opts runtimeNukeCommandOptions) error {
	if !opts.dryRun && !opts.yes {
		if !controlStdinIsTerminal(opts.apiOptions) {
			fmt.Fprintln(errOut, "ERROR: `swarm control nuke` is destructive; pass --yes for non-TTY invocations.")
			return commandExitError{code: 2}
		}
		confirmed, err := confirmRuntimeNuke(opts.apiOptions.input, errOut)
		if err != nil {
			fmt.Fprintf(errOut, "read confirmation: %v\n", err)
			return commandExitError{code: 2}
		}
		if !confirmed {
			fmt.Fprintln(errOut, "Aborted; no destruction performed.")
			return commandExitError{code: 2}
		}
	}

	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: runtimeNukeErrorExitCode(err)}
	}
	params := map[string]any{"dry_run": opts.dryRun}
	var result runtimeNukeResult
	if err := client.call(ctx, runtimeNukeMethod, params, &result); err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: runtimeNukeErrorExitCode(err)}
	}
	if err := validateRuntimeNukeResult(result); err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: 3}
	}
	writeRuntimeNukeResult(out, result)
	if result.PartialFailure {
		writeRuntimeNukeFailures(errOut, result)
		return commandExitError{code: 3}
	}
	return nil
}

func confirmRuntimeNuke(input io.Reader, errOut io.Writer) (bool, error) {
	if input == nil {
		input = strings.NewReader("")
	}
	fmt.Fprintln(errOut, "WARNING: `swarm control nuke` will destroy all Swarm-owned runtime state.")
	fmt.Fprint(errOut, "Continue? [y/N] ")
	line, err := bufio.NewReader(input).ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return false, err
	}
	answer := strings.ToLower(strings.TrimSpace(line))
	return answer == "y" || answer == "yes", nil
}

func validateRuntimeNukeResult(result runtimeNukeResult) error {
	if strings.TrimSpace(result.OperationName) == "" {
		return fmt.Errorf("malformed runtime.nuke result: operation_name is required")
	}
	switch result.Status {
	case runtimeNukeStatusDryRun:
		if !result.OK || !result.DryRun || result.PartialFailure {
			return fmt.Errorf("malformed runtime.nuke result: dry_run status must be ok=true dry_run=true partial_failure=false")
		}
	case runtimeNukeStatusDone:
		if !result.OK || result.DryRun || result.PartialFailure {
			return fmt.Errorf("malformed runtime.nuke result: completed status must be ok=true dry_run=false partial_failure=false")
		}
	case runtimeNukeStatusPartial:
		if result.OK || result.DryRun || !result.PartialFailure {
			return fmt.Errorf("malformed runtime.nuke result: partial_failure status must be ok=false dry_run=false partial_failure=true")
		}
	default:
		return fmt.Errorf("malformed runtime.nuke result: unsupported status %q", result.Status)
	}
	return nil
}

func writeRuntimeNukeResult(out io.Writer, result runtimeNukeResult) {
	if out == nil {
		return
	}
	counts := runtimeNukeCounts(result)
	switch result.Status {
	case runtimeNukeStatusDryRun:
		fmt.Fprintln(out, "runtime nuke dry-run: no destructive actions performed")
	case runtimeNukeStatusDone:
		if counts.empty() {
			fmt.Fprintln(out, "runtime state is already empty; nuke complete (no-op)")
		} else {
			fmt.Fprintln(out, "runtime nuke complete")
		}
	case runtimeNukeStatusPartial:
		fmt.Fprintln(out, "runtime nuke partial failure")
	}
	fmt.Fprintf(out, "operation=%s status=%s dry_run=%t partial_failure=%t\n", result.OperationName, result.Status, result.DryRun, result.PartialFailure)
	fmt.Fprintf(out, "active_runs=%d active_deliveries=%d run_scoped_tables=%d selected_containers=%d preserved_containers=%d stopped_containers=%d failed_containers=%d\n",
		counts.activeRuns, counts.activeDeliveries, counts.runScopedTables, counts.selectedContainers, counts.preservedContainers, counts.stoppedContainers, counts.failedContainers)
	if len(result.Cleanup.Tables) > 0 {
		fmt.Fprintf(out, "cleanup_matched_rows=%d cleanup_deleted_rows=%d\n", counts.cleanupMatchedRows, counts.cleanupDeletedRows)
	}
	writeContainerNames(out, "stopped", result.Containers.Stopped)
	writeContainerNames(out, "already_stopped", result.Containers.AlreadyStopped)
	writeContainerNames(out, "preserved", result.Containers.Preserved)
}

func writeRuntimeNukeFailures(errOut io.Writer, result runtimeNukeResult) {
	if errOut == nil {
		return
	}
	for _, failure := range result.Errors {
		fmt.Fprintf(errOut, "runtime.nuke failure: scope=%s message=%s\n", failure.Scope, failure.Message)
	}
	for _, failure := range result.Containers.Failed {
		fmt.Fprintf(errOut, "runtime.nuke container failure: container=%s error=%s\n", failure.Container.Name, failure.Error)
	}
}

func writeContainerNames(out io.Writer, label string, containers []runtimeNukeContainerRef) {
	if len(containers) == 0 {
		return
	}
	names := make([]string, 0, len(containers))
	for _, container := range containers {
		if name := strings.TrimSpace(container.Name); name != "" {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return
	}
	fmt.Fprintf(out, "%s_containers=%s\n", label, strings.Join(names, ","))
}

type runtimeNukeResultCounts struct {
	activeRuns          int
	activeDeliveries    int
	runScopedTables     int
	selectedContainers  int
	preservedContainers int
	stoppedContainers   int
	failedContainers    int
	cleanupMatchedRows  int64
	cleanupDeletedRows  int64
}

func (c runtimeNukeResultCounts) empty() bool {
	return c.activeRuns == 0 &&
		c.activeDeliveries == 0 &&
		c.selectedContainers == 0 &&
		c.stoppedContainers == 0 &&
		c.failedContainers == 0 &&
		c.cleanupMatchedRows == 0 &&
		c.cleanupDeletedRows == 0
}

func runtimeNukeCounts(result runtimeNukeResult) runtimeNukeResultCounts {
	counts := runtimeNukeResultCounts{
		activeRuns:          len(result.Plan.Plan.ActiveRuns),
		activeDeliveries:    len(result.Plan.Plan.ActiveDeliveries),
		runScopedTables:     len(result.Plan.Plan.RunScopedTables),
		selectedContainers:  len(result.Containers.Selected),
		preservedContainers: len(result.Containers.Preserved),
		stoppedContainers:   len(result.Containers.Stopped),
		failedContainers:    len(result.Containers.Failed),
	}
	if counts.activeRuns == 0 {
		counts.activeRuns = changedRunCount(result.Quiescence.Runs)
	}
	if counts.activeDeliveries == 0 {
		counts.activeDeliveries = changedDeliveryCount(result.Quiescence.Deliveries)
	}
	for _, table := range result.Cleanup.Tables {
		counts.cleanupMatchedRows += table.MatchedRows
		counts.cleanupDeletedRows += table.DeletedRows
	}
	return counts
}

func changedRunCount(runs []runtimeNukeQuiescedRun) int {
	count := 0
	for _, run := range runs {
		if run.Changed {
			count++
		}
	}
	return count
}

func changedDeliveryCount(deliveries []runtimeNukeQuiescedDelivery) int {
	count := 0
	for _, delivery := range deliveries {
		if delivery.Changed {
			count++
		}
	}
	return count
}

func runtimeNukeErrorExitCode(err error) int {
	return cliAPIErrorExitCode(err, cliAPIErrorClassifier{
		conflictCodes: []string{"RUNTIME_NUKE_IN_PROGRESS", "IDEMPOTENCY_CONFLICT"},
	})
}
