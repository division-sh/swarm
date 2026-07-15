package cliapp

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/division-sh/swarm/internal/cli/argcount"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
)

const (
	runForkMethod       = "run.fork"
	runForkCommandShape = "swarm run fork <source-run-id> [--bundle-hash <bundle_hash>] [--at-event <event-id>] [--confirm-source-freeze] [--idempotency-key <key>]"
)

type forkCommandOptions struct {
	apiOptions rootCommandOptions
	output     cliOutputOptions

	bundleHash          string
	atEvent             string
	confirmSourceFreeze bool
	idempotencyKey      string

	bundleHashSet     bool
	atEventSet        bool
	idempotencyKeySet bool
}

type runForkResult struct {
	Owner              string `json:"owner"`
	SourceRunID        string `json:"source_run_id"`
	SourceRunStatus    string `json:"source_run_status"`
	SourceFrozen       *bool  `json:"source_frozen"`
	ForkRunID          string `json:"fork_run_id"`
	ForkEventID        string `json:"fork_event_id"`
	ForkRunStatus      string `json:"fork_run_status"`
	BundleHash         string `json:"bundle_hash"`
	ExecutedEventCount int    `json:"executed_event_count"`
}

func newForkCommand(opts rootCommandOptions) *cobra.Command {
	forkOpts := forkCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:     "fork <source-run-id>",
		Short:   "Branch a run to replay it with changed contracts or policy.",
		Example: `  swarm run fork <source-run-id> --at-event <event-id>`,
		Long:    runForkCommandShape + "\n\nBranch a run to replay it with changed contracts or policy.",
		Args:    argcount.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			forkOpts.bundleHashSet = cmd.Flags().Changed("bundle-hash")
			forkOpts.atEventSet = cmd.Flags().Changed("at-event")
			forkOpts.idempotencyKeySet = cmd.Flags().Changed("idempotency-key")
			if err := forkOpts.output.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			return runForkCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), forkOpts, args[0])
		},
	}
	argcount.SetDiscoveryHint(cmd, "List run ids with `swarm run list`.")
	cmd.Flags().StringVar(&forkOpts.bundleHash, "bundle-hash", "", "Target bundle hash for run.fork selection")
	cmd.Flags().StringVar(&forkOpts.atEvent, "at-event", "", "Fork at this source event id")
	cmd.Flags().BoolVar(&forkOpts.confirmSourceFreeze, "confirm-source-freeze", false, "Authorize permanently freezing an active source unless source advancement preserves it")
	cmd.Flags().StringVar(&forkOpts.idempotencyKey, "idempotency-key", "", "Optional idempotency key for retry-safe fork creation")
	_ = cmd.Flags().MarkHidden("idempotency-key")
	bindCLIOutputFlags(cmd, &forkOpts.output)
	bindCLIAPIConnectionFlagsWithClass(cmd, &forkOpts.apiOptions, cliAPICommandClassMutating, "swarm run fork")
	return cmd
}

func runForkCommand(ctx context.Context, out, errOut io.Writer, opts forkCommandOptions, rawSourceRunID string) error {
	params, err := opts.params(rawSourceRunID)
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, runForkAPIErrorClassifier())
	}
	if !opts.confirmSourceFreeze {
		sourceRunID, _ := params["source_run_id"].(string)
		run, err := runCommandGet(ctx, client, sourceRunID)
		if err != nil {
			return returnCLIAPIError(errOut, err, runForkAPIErrorClassifier())
		}
		if runForkSourceStatusActive(run.Status) {
			if err := requireRunForkSourceFreezeConfirmation(opts.apiOptions, sourceRunID, errOut); err != nil {
				return err
			}
			params["confirm_source_freeze"] = true
		}
	}
	var result runForkResult
	if err := client.call(ctx, runForkMethod, params, &result); err != nil {
		return returnCLIAPIError(errOut, err, runForkAPIErrorClassifier())
	}
	if err := validateRunForkResult(result); err != nil {
		return returnCLIAPIError(errOut, err, runForkAPIErrorClassifier())
	}
	return renderCLIOutput(out, errOut, opts.output, result, func(w io.Writer) {
		writeRunForkHuman(w, result)
	}, func() ([]string, error) {
		return []string{result.ForkRunID}, nil
	})
}

func (opts forkCommandOptions) params(rawSourceRunID string) (map[string]any, error) {
	sourceRunID, err := validateRunForkUUIDValue("source run id", rawSourceRunID)
	if err != nil {
		return nil, err
	}
	params := map[string]any{"source_run_id": sourceRunID}
	if opts.confirmSourceFreeze {
		params["confirm_source_freeze"] = true
	}

	bundleHash, err := optionalNonEmptyFlag("--bundle-hash", opts.bundleHash, opts.bundleHashSet)
	if err != nil {
		return nil, err
	}
	if bundleHash != "" {
		if _, err := validateBundleHashArg("--bundle-hash", bundleHash); err != nil {
			return nil, err
		}
		params["bundle_hash"] = bundleHash
	}

	forkEventID, err := optionalNonEmptyFlag("--at-event", opts.atEvent, opts.atEventSet)
	if err != nil {
		return nil, err
	}
	if forkEventID != "" {
		parsed, err := validateRunForkUUIDValue("--at-event", forkEventID)
		if err != nil {
			return nil, err
		}
		params["fork_event_id"] = parsed
	}

	idempotencyKey, err := optionalNonEmptyFlag("--idempotency-key", opts.idempotencyKey, opts.idempotencyKeySet)
	if err != nil {
		return nil, err
	}
	if idempotencyKey != "" {
		params["idempotency_key"] = idempotencyKey
	}
	return params, nil
}

func runForkSourceStatusActive(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "running", "paused":
		return true
	default:
		return false
	}
}

func requireRunForkSourceFreezeConfirmation(opts rootCommandOptions, sourceRunID string, errOut io.Writer) error {
	if !controlStdinIsTerminal(opts) {
		return returnCLIValidationError(errOut, fmt.Errorf("run %s is active; pass --confirm-source-freeze for non-TTY invocations", sourceRunID))
	}
	fmt.Fprintf(errOut, "WARNING: this operation may permanently freeze run %s; source advancement instead preserves it as a live branch.\n", sourceRunID)
	fmt.Fprint(errOut, "Continue? [y/N] ")
	input := opts.input
	if input == nil {
		input = strings.NewReader("")
	}
	line, err := bufio.NewReader(input).ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return returnCLIValidationError(errOut, fmt.Errorf("read confirmation: %w", err))
	}
	answer := strings.ToLower(strings.TrimSpace(line))
	if answer != "y" && answer != "yes" {
		return returnCLIValidationError(errOut, fmt.Errorf("aborted; run fork was not started"))
	}
	return nil
}

func runForkAPIErrorClassifier() cliAPIErrorClassifier {
	return cliAPIErrorClassifier{
		notFoundCodes: []string{
			"RUN_NOT_FOUND",
			"EVENT_NOT_FOUND",
		},
		conflictCodes: []string{
			"IDEMPOTENCY_CONFLICT",
		},
	}
}

func validateRunForkUUIDValue(name, raw string) (string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", fmt.Errorf("%s is required", name)
	}
	parsed, err := uuid.Parse(value)
	if err != nil {
		return "", fmt.Errorf("%s must be a UUID", name)
	}
	return parsed.String(), nil
}

func validateRunForkResult(result runForkResult) error {
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "owner", value: result.Owner},
		{name: "source_run_id", value: result.SourceRunID},
		{name: "source_run_status", value: result.SourceRunStatus},
		{name: "fork_run_id", value: result.ForkRunID},
		{name: "fork_event_id", value: result.ForkEventID},
		{name: "fork_run_status", value: result.ForkRunStatus},
		{name: "bundle_hash", value: result.BundleHash},
	} {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("malformed run.fork result: %s is required", field.name)
		}
	}
	if result.ExecutedEventCount < 0 {
		return fmt.Errorf("malformed run.fork result: executed_event_count must be non-negative")
	}
	if result.SourceFrozen == nil {
		return fmt.Errorf("malformed run.fork result: source_frozen is required")
	}
	sourceStatus := strings.ToLower(strings.TrimSpace(result.SourceRunStatus))
	switch sourceStatus {
	case "running", "paused", "completed", "failed", "cancelled", "forked":
	default:
		return fmt.Errorf("malformed run.fork result: source_run_status %q is invalid", result.SourceRunStatus)
	}
	if *result.SourceFrozen != (sourceStatus == "forked") {
		return fmt.Errorf("malformed run.fork result: source_frozen=%t contradicts source_run_status %q", *result.SourceFrozen, result.SourceRunStatus)
	}
	if _, err := validateRunForkUUIDValue("source_run_id", result.SourceRunID); err != nil {
		return fmt.Errorf("malformed run.fork result: %w", err)
	}
	if _, err := validateRunForkUUIDValue("fork_run_id", result.ForkRunID); err != nil {
		return fmt.Errorf("malformed run.fork result: %w", err)
	}
	if _, err := validateRunForkUUIDValue("fork_event_id", result.ForkEventID); err != nil {
		return fmt.Errorf("malformed run.fork result: %w", err)
	}
	if _, err := validateBundleHashArg("bundle_hash", result.BundleHash); err != nil {
		return fmt.Errorf("malformed run.fork result: %w", err)
	}
	return nil
}

func writeRunForkHuman(w io.Writer, result runForkResult) {
	if w == nil {
		return
	}
	fmt.Fprintln(w, "Fork created")
	fmt.Fprintf(w, "source_run_id=%s fork_run_id=%s fork_event_id=%s\n", result.SourceRunID, result.ForkRunID, result.ForkEventID)
	fmt.Fprintf(w, "source_status=%s source_frozen=%t\n", formatCLIHumanCode(cliHumanCodeRunStatus, result.SourceRunStatus), *result.SourceFrozen)
	fmt.Fprintf(w, "status=%s bundle_hash=%s executed_event_count=%d\n", formatCLIHumanCode(cliHumanCodeRunStatus, result.ForkRunStatus), result.BundleHash, result.ExecutedEventCount)
	fmt.Fprintf(w, "owner=%s\n", result.Owner)
}
