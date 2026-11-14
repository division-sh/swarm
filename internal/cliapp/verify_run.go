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
)

type verifyRunCommandResult struct {
	mutationlog.DriftReport
	Status string              `json:"status"`
	OK     bool                `json:"ok"`
	Reason string              `json:"reason,omitempty"`
	Errors []failures.Envelope `json:"errors"`
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
	if errors.As(cause, &missing) && failures.OnlyBranches(cause, func(err error) bool { return err == runlifecycle.ErrRunNotFound }) {
		return failures.Wrap(failures.ClassSchemaInvalid, "run_not_found", "verify", "run", attrs, cause)
	}
	var history *mutationlog.HistoryError
	if errors.As(cause, &history) && failures.OnlyBranches(cause, func(err error) bool { _, ok := err.(*mutationlog.HistoryError); return ok }) {
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
