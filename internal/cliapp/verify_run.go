package cliapp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
	"github.com/google/uuid"
)

type verifyRunCommandResult struct {
	mutationlog.DriftReport
	Status string              `json:"status"`
	OK     bool                `json:"ok"`
	Reason string              `json:"reason,omitempty"`
	Errors []failures.Envelope `json:"errors"`
}

func runVerifyRunCommand(ctx context.Context, repo string, opts verifyCommandOptions, out, errOut io.Writer) int {
	result := verifyRunCommandResult{DriftReport: mutationlog.DriftReport{RunID: strings.TrimSpace(opts.runID), Rows: []mutationlog.DriftRow{}}, Status: "failed", Errors: []failures.Envelope{}}
	var cause error
	if err := opts.logging.validate(); err != nil {
		cause = failures.Wrap(failures.ClassSchemaInvalid, "invalid_logging_options", "verify", "run", nil, err)
	} else if _, err := uuid.Parse(result.RunID); err != nil {
		cause = failures.Wrap(failures.ClassSchemaInvalid, "invalid_run_id", "verify", "run", nil, err)
	} else if opts.portable {
		cause = failures.New(failures.ClassSchemaInvalid, "run_requires_store", "verify", "run", map[string]any{"reason": "--portable cannot be combined with --run"})
	} else {
		result.DriftReport, cause = inspectVerifyRun(ctx, repo, opts, result.RunID)
	}
	code := cliExitOK
	if cause != nil {
		var absent *storeselected.AbsentSQLiteStore
		if errors.As(cause, &absent) && failures.OnlyBranches(cause, func(err error) bool { _, ok := err.(*storeselected.AbsentSQLiteStore); return ok }) {
			result.Status = "not_run"
			result.Reason = "store admission: not evaluated \u2014 " + absent.Error()
		} else {
			failure, ok := failures.As(verifyRunFailure(result.RunID, cause))
			if !ok {
				return CLIExitRuntime
			}
			result.Errors = append(result.Errors, failure.Failure)
			code = CLIExitRuntime
			if failure.Failure.Class == failures.ClassSchemaInvalid {
				code = CLIExitValidation
			}
			if errors.Is(cause, context.Canceled) {
				code = cliExitInterrupted
			}
		}
	} else if len(result.Rows) != 0 {
		result.Status = "drift"
		code = CLIExitValidation
	} else {
		result.Status = "passed"
		result.OK = true
	}
	if result.Rows == nil {
		result.Rows = []mutationlog.DriftRow{}
	}
	if result.RunID == "" {
		result.RunID = strings.TrimSpace(opts.runID)
	}
	lines := func() ([]string, error) {
		var text strings.Builder
		if err := writeVerifyRunResult(&text, result); err != nil {
			return nil, err
		}
		return strings.Split(strings.TrimSuffix(text.String(), "\n"), "\n"), nil
	}
	var textError error
	if err := renderCLIOutput(out, errOut, opts.output, result, func(w io.Writer) {
		if w != nil {
			textError = writeVerifyRunResult(w, result)
		}
	}, lines); err != nil || textError != nil {
		return CLIExitValidation
	}
	return code
}

func inspectVerifyRun(ctx context.Context, repo string, opts verifyCommandOptions, runID string) (report mutationlog.DriftReport, err error) {
	report = mutationlog.DriftReport{RunID: runID, Rows: []mutationlog.DriftRow{}}
	request, schema, err := prepareVerifyRunInspection(ctx, repo, opts)
	if err != nil {
		return report, err
	}
	inspection, err := storeselected.OpenAdmissionInspection(ctx, request)
	if err != nil {
		return report, err
	}
	defer func() { err = errors.Join(err, inspection.Close()) }()
	_, err = inspection.Inspect(ctx, schema, func(snapshot *storeselected.AdmissionSnapshot) error {
		fresh, err := snapshot.RequiresSchemaPreparation()
		if err != nil {
			return err
		}
		if fresh {
			return failures.New(failures.ClassSchemaInvalid, "run_store_unprepared", "verify", "run", map[string]any{"run_id": runID})
		}
		report, err = snapshot.InspectRunMutationDrift(ctx, runID)
		return err
	})
	if err != nil {
		return mutationlog.DriftReport{RunID: runID, Rows: []mutationlog.DriftRow{}}, err
	}
	return report, nil
}

func prepareVerifyRunInspection(ctx context.Context, repo string, opts verifyCommandOptions) (storeselected.AuthorityRequest, store.SchemaBootstrapRequest, error) {
	cfg, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: repo, ExplicitPath: opts.configPath})
	if err != nil {
		return storeselected.AuthorityRequest{}, store.SchemaBootstrapRequest{}, err
	}
	paths, err := resolveCLISourcePlatformSpecPathsFromConfig(repo, CLISourcePlatformSpecPathOptions{SourceRoot: opts.sourceRoot, PlatformSpecPath: opts.platformSpecPath, ConfigPath: opts.configPath}, cfg.cli)
	if err != nil {
		return storeselected.AuthorityRequest{}, store.SchemaBootstrapRequest{}, err
	}
	base, err := LoadConfiguredPlatformPackBase(repo, cfg)
	if err != nil {
		return storeselected.AuthorityRequest{}, store.SchemaBootstrapRequest{}, err
	}
	_, bundle, err := NewSwarmWorkflowModuleWithPackBase(repo, paths.SourceRoot, paths.PlatformSpecPath, base)
	if err != nil {
		return storeselected.AuthorityRequest{}, store.SchemaBootstrapRequest{}, err
	}
	source, _, err := admitStructuralSource(cfg, bundle)
	if err != nil {
		return storeselected.AuthorityRequest{}, store.SchemaBootstrapRequest{}, err
	}
	return prepareVerifySelectedStoreInspection(ctx, repo, paths, cfg, opts.swarmDir, source)
}

func verifyRunFailure(runID string, cause error) error {
	attrs := map[string]any{"run_id": runID, "reason": cause.Error()}
	if errors.Is(cause, context.Canceled) {
		return failures.Wrap(failures.ClassDependencyUnavailable, "run_inspection_canceled", "verify", "run", attrs, cause)
	}
	if errors.Is(cause, context.DeadlineExceeded) {
		return failures.Wrap(failures.ClassTimeout, "run_inspection_timeout", "verify", "run", attrs, cause)
	}
	var missing *runlifecycle.RunNotFoundError
	if errors.As(cause, &missing) {
		return failures.Wrap(failures.ClassSchemaInvalid, "run_not_found", "verify", "run", attrs, cause)
	}
	var history *mutationlog.HistoryError
	if errors.As(cause, &history) {
		attrs["entity_id"], attrs["mutation_id"] = history.EntityID, history.MutationID
		return failures.Wrap(failures.ClassSchemaInvalid, history.Code, "verify", "run", attrs, cause)
	}
	if failure, ok := failures.As(cause); ok {
		return failure
	}
	return failures.Wrap(failures.ClassDependencyUnavailable, "run_inspection_failed", "verify", "run", attrs, cause)
}

func writeVerifyRunResult(w io.Writer, result verifyRunCommandResult) error {
	switch result.Status {
	case "passed":
		_, err := fmt.Fprintf(w, "%d entities checked, no drift\n", result.EntitiesChecked)
		return err
	case "not_run":
		_, err := fmt.Fprintf(w, "verify run not evaluated*\n%s\n", result.Reason)
		return err
	case "failed":
		for _, failure := range result.Errors {
			message := failure.Message
			if reason, ok := failure.Detail.Attributes["reason"].(string); ok {
				message = reason
			}
			if _, err := fmt.Fprintf(w, "verify run failed: %s: %s\n", failure.Detail.Code, message); err != nil {
				return err
			}
		}
		return nil
	}
	if _, err := fmt.Fprintf(w, "%d entities checked, %d mismatches\n", result.EntitiesChecked, len(result.Rows)); err != nil {
		return err
	}
	rows := make([][]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		path := ""
		if row.Path != nil {
			path = fmt.Sprintf("%q", *row.Path)
		}
		left, err := canonicaljson.MarshalPreservingNumberKinds(row.FoldedValue)
		if err != nil {
			return err
		}
		right, err := canonicaljson.MarshalPreservingNumberKinds(row.StoredValue)
		if err != nil {
			return err
		}
		rows = append(rows, []string{row.EntityID, row.Kind, string(row.Domain), path,
			fmt.Sprintf("%s:%s (present=%t)", row.FoldedType, left, row.FoldedPresent),
			fmt.Sprintf("%s:%s (present=%t)", row.StoredType, right, row.StoredPresent)})
	}
	var table strings.Builder
	// Evidence identifiers remain full; resource-ID display shortening does not
	// alter this mismatch report. The canonical renderer still owns the layout.
	writeCLITable(&table, cliTable{Columns: []cliTableColumn{
		{Header: "ENTITY"}, {Header: "KIND"}, {Header: "DOMAIN"},
		{Header: "PATH"}, {Header: "FOLDED"}, {Header: "STORED"},
	}, Rows: rows})
	_, err := io.WriteString(w, table.String())
	return err
}
