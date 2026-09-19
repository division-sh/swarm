package cliapp

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/division-sh/swarm/internal/cli/argcount"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/spf13/cobra"
)

func newRunFanOutCommand(opts rootCommandOptions) *cobra.Command {
	group := &cobra.Command{Use: "fan-out", Short: "Inspect durable fan-out issuance."}
	query := fanoutobligation.ListQuery{}
	output := cliOutputOptions{}
	logging := cliLoggingOptions{}
	list := &cobra.Command{
		Use: "list <run-id>", Short: "Read one bounded page of fan-out intent diagnostics.", Args: argcount.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			query.RunID = args[0]
			if cmd.Flags().Changed("flow-path") && query.Filter.FlowPath == "" {
				return returnCLIValidationError(cmd.ErrOrStderr(), fmt.Errorf("--flow-path cannot be empty; use . for the selected root"))
			}
			if err := logging.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			if err := output.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			if cmd.Flags().Changed("limit") && query.Limit == 0 {
				return returnCLIValidationError(cmd.ErrOrStderr(), fmt.Errorf("--limit must be between 1 and 500"))
			}
			if err := query.Validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			client, err := newCLIAPIClient(opts)
			if err != nil {
				return returnCLIAPIError(cmd.ErrOrStderr(), err, diagnosticRunAPIErrorClassifier())
			}
			raw, err := json.Marshal(query)
			if err != nil {
				return err
			}
			var params map[string]any
			if err := json.Unmarshal(raw, &params); err != nil {
				return err
			}
			var page fanoutobligation.ListPage
			if err := client.call(cmd.Context(), "run.fan_out.list", params, &page); err != nil {
				return returnCLIAPIError(cmd.ErrOrStderr(), err, diagnosticRunAPIErrorClassifier())
			}
			if err := page.Validate(query); err != nil {
				return fmt.Errorf("malformed run.fan_out.list result: %w", err)
			}
			return renderCLIOutput(cmd.OutOrStdout(), cmd.ErrOrStderr(), output, page, func(w io.Writer) {
				fmt.Fprintf(w, "fan-out %s run_status=%s observed_at=%s\n", page.RunID, formatCLIHumanCode(cliHumanCodeRunStatus, page.RunStatus), page.ObservedAt.Format(time.RFC3339Nano))
				for _, row := range page.Intents {
					fmt.Fprintf(w, "%s durable_state=%s cursor=%d/%d owed=%d chunk=%d runtime=%s (%s)\n", row.Key.String(), row.DurableState, row.Cursor, row.Cardinality, row.Owed, row.NextChunkSize, row.Runtime.Availability, row.Runtime.Reason)
					if row.Runtime.Availability == "available" {
						fmt.Fprintf(w, "  runtime_observed_at=%s eligible=%t workers=%d active_workers=%d", row.Runtime.ObservedAt.Format(time.RFC3339Nano), *row.Runtime.Eligible, *row.Runtime.Workers, *row.Runtime.ActiveWorkers)
						if row.Runtime.LastCommitMS == nil {
							fmt.Fprintln(w, " last_commit_ms=unavailable")
						} else {
							fmt.Fprintf(w, " last_commit_ms=%g\n", *row.Runtime.LastCommitMS)
						}
					}
					if row.LeaseExpiresAt != nil {
						fmt.Fprintf(w, "  lease owner=%s generation=%d expires=%s\n", row.ClaimOwner, row.ClaimGeneration, row.LeaseExpiresAt.Format(time.RFC3339Nano))
					}
					if row.LastServedAt != nil {
						fmt.Fprintf(w, "  last_served_at=%s\n", row.LastServedAt.Format(time.RFC3339Nano))
					}
					if row.Retry != nil {
						fmt.Fprintf(w, "  retry_ready_at=%s cause=%s\n", row.Retry.ReadyAt.Format(time.RFC3339Nano), row.Retry.Failure.Detail.Code)
					}
					if row.Failure != nil {
						fmt.Fprintf(w, "  blocked=%s\n", row.Failure.Detail.Code)
					}
					if row.CancellationReason != "" {
						fmt.Fprintf(w, "  canceled=%s\n", row.CancellationReason)
					}
				}
				if page.NextCursor != "" {
					fmt.Fprintf(w, "next_cursor=%s\n", page.NextCursor)
				}
			}, func() ([]string, error) {
				keys := make([]string, len(page.Intents))
				for i, row := range page.Intents {
					keys[i] = row.Key.String()
				}
				return keys, nil
			})
		},
	}
	var status string
	argcount.SetDiscoveryHint(list, "List run ids with `swarm run list`.")
	list.PreRunE = func(cmd *cobra.Command, args []string) error {
		query.Filter.Status = fanoutobligation.Status(status)
		return nil
	}
	list.Flags().StringVar(&status, "status", "", "Exact durable status: open, blocked, closed, canceled")
	list.Flags().StringVar(&query.Filter.TriggeringDeliveryID, "triggering-delivery-id", "", "Exact triggering delivery UUID")
	list.Flags().StringVar(&query.Filter.FlowPath, "flow-path", "", "Exact declaration flow path")
	list.Flags().StringVar(&query.Filter.SemanticPath, "semantic-path", "", "Exact declaration semantic path")
	list.Flags().IntVar(&query.Limit, "limit", 0, "Page size, 1-500 (default 50)")
	list.Flags().StringVar(&query.Cursor, "cursor", "", "Opaque run/filter-bound continuation cursor")
	bindCLIOutputFlags(list, &output)
	bindCLILoggingFlags(list, &logging)
	bindCLIAPIConnectionFlags(list, &opts)
	group.AddCommand(list)
	return group
}
