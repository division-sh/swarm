package cliapp

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/division-sh/swarm/internal/cli/argcount"
	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
)

const (
	runForkMethod       = "run.fork"
	runForkCommandShape = "swarm run fork <source-run-id> [--source <directory>] [--at-start | --at-event <event-id>] [--pin <name@vN|ResourceVersionID>] [--allow-source-freeze] [--idempotency-key <key>]"
)

type forkCommandOptions struct {
	apiOptions rootCommandOptions
	output     cliOutputOptions

	source            string
	atEvent           string
	atStart           bool
	allowSourceFreeze bool
	idempotencyKey    string
	pins              []string

	sourceSet         bool
	atEventSet        bool
	atStartSet        bool
	idempotencyKeySet bool
}

type runForkResult struct {
	SourceLabel        string            `json:"source_label"`
	Owner              string            `json:"owner"`
	SourceRunID        string            `json:"source_run_id"`
	SourceRunStatus    string            `json:"source_run_status"`
	SourceFrozen       *bool             `json:"source_frozen"`
	ForkRunID          string            `json:"fork_run_id"`
	ForkPointKind      string            `json:"fork_point_kind"`
	ForkRevision       int64             `json:"fork_revision"`
	ForkEventID        string            `json:"fork_event_id,omitempty"`
	ForkRunStatus      string            `json:"fork_run_status"`
	BundleHash         string            `json:"bundle_hash"`
	ExecutedEventCount int               `json:"executed_event_count"`
	DataPins           []durabledata.Pin `json:"data_pins"`
}

func newForkCommand(opts rootCommandOptions) *cobra.Command {
	forkOpts := forkCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:     "fork <source-run-id>",
		Short:   "Branch a run to replay it with changed contracts or policy.",
		Example: "  swarm run fork <source-run-id> --at-start\n  swarm run fork <source-run-id> --at-event <event-id>",
		Long:    runForkCommandShape + "\n\nBranch a run to replay it with changed contracts or policy. --at-start selects the original committed start, before creating ingress. --at-start and --at-event are mutually exclusive; omitting both preserves the current latest-point selection.",
		Args:    argcount.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			forkOpts.sourceSet = cmd.Flags().Changed("source")
			forkOpts.atEventSet = cmd.Flags().Changed("at-event")
			forkOpts.atStartSet = cmd.Flags().Changed("at-start")
			forkOpts.idempotencyKeySet = cmd.Flags().Changed("idempotency-key")
			if err := forkOpts.output.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			return runForkCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), forkOpts, args[0])
		},
	}
	argcount.SetDiscoveryHint(cmd, "List run ids with `swarm run list`.")
	cmd.Flags().StringVar(&forkOpts.source, "source", "", "Select a target source directory already served into the selected store; defaults to the source run")
	cmd.Flags().StringVar(&forkOpts.atEvent, "at-event", "", "Fork at this source event id")
	cmd.Flags().BoolVar(&forkOpts.atStart, "at-start", false, "Fork at the original committed run start, before creating ingress; mutually exclusive with --at-event")
	cmd.Flags().StringArrayVar(&forkOpts.pins, "pin", nil, "Exact data version override: name@vN or name@ResourceVersionID (repeatable)")
	cmd.Flags().BoolVar(&forkOpts.allowSourceFreeze, "allow-source-freeze", false, "Allow permanent source freeze if it has not advanced beyond the fork point; a frozen source cannot resume. An advanced source stays independently live. Omit and decline the prompt to cancel")
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
	if len(opts.pins) > 0 {
		bundleHash, _ := params["bundle_hash"].(string)
		if bundleHash == "" {
			sourceRunID, _ := params["source_run_id"].(string)
			run, err := runCommandGet(ctx, client, sourceRunID)
			if err != nil {
				return returnCLIAPIError(errOut, err, runForkAPIErrorClassifier())
			}
			if !cliBundleHashPattern.MatchString(run.BundleHash) {
				return returnCLIValidationError(errOut, fmt.Errorf("source run has no exact source identity"))
			}
			bundleHash = run.BundleHash
		}
		declarations, err := listDataDeclarations(ctx, client, bundleHash)
		if err != nil {
			return returnCLIAPIError(errOut, err, dataAPIErrorClassifier())
		}
		overrides := make([]any, 0, len(opts.pins))
		seen := make(map[string]struct{}, len(opts.pins))
		for _, raw := range opts.pins {
			name, versionSelector, err := splitDataVersionSelector(raw, false)
			if err != nil {
				return returnCLIValidationError(errOut, fmt.Errorf("--pin: %w", err))
			}
			declaration, err := resolveDataDeclarationFromList(bundleHash, name, declarations)
			if err != nil {
				return returnCLIValidationError(errOut, err)
			}
			if _, duplicate := seen[declaration.Declaration.Key()]; duplicate {
				return returnCLIValidationError(errOut, fmt.Errorf("--pin repeats data declaration %s", dataDeclarationLabel(declaration)))
			}
			version, err := resolveDataVersion(ctx, client, declaration.Declaration, versionSelector, false)
			if err != nil {
				return returnCLIAPIError(errOut, err, dataAPIErrorClassifier())
			}
			seen[declaration.Declaration.Key()] = struct{}{}
			overrides = append(overrides, map[string]any{"declaration": declaration.Declaration, "version_id": version.VersionID})
		}
		params["bundle_hash"] = bundleHash
		params["data_pin_overrides"] = overrides
	}
	if !opts.allowSourceFreeze {
		sourceRunID, _ := params["source_run_id"].(string)
		run, err := runCommandGet(ctx, client, sourceRunID)
		if err != nil {
			return returnCLIAPIError(errOut, err, runForkAPIErrorClassifier())
		}
		if runForkSourceStatusActive(run.Status) {
			if err := requireRunForkSourceFreezeConfirmation(opts.apiOptions, sourceRunID, errOut); err != nil {
				return err
			}
			params["allow_source_freeze"] = true
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
	if (opts.atStartSet || opts.atStart) && (opts.atEventSet || opts.atEvent != "") {
		return nil, fmt.Errorf("--at-start and --at-event are mutually exclusive")
	}
	if opts.atStart {
		params["at_start"] = true
	}
	if opts.allowSourceFreeze {
		params["allow_source_freeze"] = true
	}

	if opts.sourceSet {
		sourceHash, err := admitCLIIdentitySource(opts.apiOptions.invocationRoot, opts.source)
		if err != nil {
			return nil, err
		}
		params["bundle_hash"] = sourceHash
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
		return returnCLIValidationError(errOut, fmt.Errorf("run %s is active; pass --allow-source-freeze to allow permanent source freeze if it has not advanced beyond the fork point. A frozen source cannot resume; an advanced source stays independently live. Without this permission, no fork is started", sourceRunID))
	}
	fmt.Fprintf(errOut, "WARNING: run %s will be permanently frozen if it has not advanced beyond the fork point. A frozen source cannot resume. An advanced source stays independently live.\n", sourceRunID)
	fmt.Fprint(errOut, "Allow source freeze and create the fork? [y/N] (No cancels without starting a fork) ")
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
		{name: "fork_point_kind", value: result.ForkPointKind},
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
	if result.DataPins == nil {
		return fmt.Errorf("malformed run.fork result: data_pins is required")
	}
	for index, pin := range result.DataPins {
		if pin.RunID != result.ForkRunID || pin.Declaration.Validate() != nil || pin.SchemaDigest.Validate() != nil || pin.VersionID.Validate() != nil {
			return fmt.Errorf("malformed run.fork result: data_pins[%d] is contradictory", index)
		}
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
	point := runfork.RunForkPoint{Kind: runfork.RunForkPointKind(result.ForkPointKind), Revision: result.ForkRevision, EventID: result.ForkEventID}
	if err := point.Validate(); err != nil {
		return fmt.Errorf("malformed run.fork result: fork point: %w", err)
	}
	if result.ForkEventID != "" {
		if _, err := validateRunForkUUIDValue("fork_event_id", result.ForkEventID); err != nil {
			return fmt.Errorf("malformed run.fork result: %w", err)
		}
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
	fmt.Fprintf(w, "source_run_id=%s fork_run_id=%s fork_point=%s@%d", result.SourceRunID, result.ForkRunID, result.ForkPointKind, result.ForkRevision)
	if result.ForkEventID != "" {
		fmt.Fprintf(w, " fork_event_id=%s", result.ForkEventID)
	}
	fmt.Fprintln(w)
	fmt.Fprintf(w, "source_status=%s source_frozen=%t\n", formatCLIHumanCode(cliHumanCodeRunStatus, result.SourceRunStatus), *result.SourceFrozen)
	fmt.Fprintf(w, "status=%s source=%s executed_event_count=%d\n", formatCLIHumanCode(cliHumanCodeRunStatus, result.ForkRunStatus), humanSourceIdentity(result.BundleHash, result.SourceLabel), result.ExecutedEventCount)
	fmt.Fprintf(w, "data_pins=%d\n", len(result.DataPins))
	fmt.Fprintf(w, "owner=%s\n", result.Owner)
}
