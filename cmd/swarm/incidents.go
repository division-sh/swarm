package main

import (
	"context"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/spf13/cobra"
)

const runtimeIncidentsMethod = "runtime.incidents"

type runtimeIncidentCommandOptions struct {
	apiOptions rootCommandOptions

	sinceHours int
	component  string
	level      string
	mcpOnly    bool
	limit      int
	cursor     string

	sinceHoursSet bool
	mcpOnlySet    bool
	limitSet      bool
}

type runtimeIncidentListResult struct {
	Incidents  []runtimeIncident `json:"incidents"`
	NextCursor string            `json:"next_cursor,omitempty"`
}

type runtimeIncident struct {
	IncidentID    string   `json:"incident_id"`
	FirstSeen     string   `json:"first_seen"`
	LastSeen      string   `json:"last_seen"`
	Count         int      `json:"count"`
	Level         string   `json:"level"`
	Component     string   `json:"component"`
	ErrorCode     string   `json:"error_code,omitempty"`
	SampleMessage *string  `json:"sample_message"`
	SampleLogIDs  []string `json:"sample_log_ids"`
}

var runtimeIncidentValidLevels = map[string]struct{}{
	"debug": {},
	"info":  {},
	"warn":  {},
	"error": {},
}

var runtimeIncidentOpaqueIDPattern = regexp.MustCompile(`^[A-Za-z0-9_:.-]+$`)

func newIncidentsCommand(opts rootCommandOptions) *cobra.Command {
	incidentOpts := runtimeIncidentCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "incidents [filters]",
		Short: "List runtime incidents through /v1/rpc runtime.incidents.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			incidentOpts.sinceHoursSet = cmd.Flags().Changed("since-hours")
			incidentOpts.mcpOnlySet = cmd.Flags().Changed("mcp-only")
			incidentOpts.limitSet = cmd.Flags().Changed("limit")
			return runIncidentsCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), incidentOpts)
		},
	}
	cmd.Flags().IntVar(&incidentOpts.sinceHours, "since-hours", 0, "Look back this many hours, 1-720")
	cmd.Flags().StringVar(&incidentOpts.component, "component", "", "Filter by runtime component")
	cmd.Flags().StringVar(&incidentOpts.level, "level", "", "Filter by incident level: debug, info, warn, or error")
	cmd.Flags().BoolVar(&incidentOpts.mcpOnly, "mcp-only", false, "Only show incidents from MCP-prefixed components")
	cmd.Flags().IntVar(&incidentOpts.limit, "limit", 0, "Page size, 1-500")
	cmd.Flags().StringVar(&incidentOpts.cursor, "cursor", "", "Pagination cursor")
	bindCLIAPIConnectionFlags(cmd, &incidentOpts.apiOptions)
	return cmd
}

func runIncidentsCommand(ctx context.Context, out, errOut io.Writer, opts runtimeIncidentCommandOptions) error {
	params, err := opts.params()
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, runtimeIncidentErrorClassifier())
	}
	var result runtimeIncidentListResult
	if err := client.call(ctx, runtimeIncidentsMethod, params, &result); err != nil {
		return returnCLIAPIError(errOut, err, runtimeIncidentErrorClassifier())
	}
	if err := validateRuntimeIncidentListResult(result); err != nil {
		return returnCLIAPIError(errOut, err, runtimeIncidentErrorClassifier())
	}
	writeRuntimeIncidentListResult(out, result)
	return nil
}

func (opts runtimeIncidentCommandOptions) params() (map[string]any, error) {
	params := map[string]any{}
	if opts.sinceHoursSet {
		if opts.sinceHours < 1 || opts.sinceHours > 720 {
			return nil, fmt.Errorf("--since-hours must be between 1 and 720")
		}
		params["since_hours"] = opts.sinceHours
	}
	if component := strings.TrimSpace(opts.component); component != "" {
		params["component"] = component
	}
	if level := strings.ToLower(strings.TrimSpace(opts.level)); level != "" {
		if _, ok := runtimeIncidentValidLevels[level]; !ok {
			return nil, fmt.Errorf("--level must be one of debug, info, warn, error")
		}
		params["level"] = level
	}
	if opts.mcpOnlySet {
		params["mcp_only"] = opts.mcpOnly
	}
	if opts.limitSet {
		if opts.limit < 1 || opts.limit > 500 {
			return nil, fmt.Errorf("--limit must be between 1 and 500")
		}
		params["limit"] = opts.limit
	}
	if cursor := strings.TrimSpace(opts.cursor); cursor != "" {
		params["cursor"] = cursor
	}
	return params, nil
}

func validateRuntimeIncidentListResult(result runtimeIncidentListResult) error {
	if result.Incidents == nil {
		return fmt.Errorf("malformed runtime.incidents result: incidents is required")
	}
	for i, incident := range result.Incidents {
		if err := validateRuntimeIncident(fmt.Sprintf("runtime.incidents result: incidents[%d]", i), incident); err != nil {
			return err
		}
	}
	return nil
}

func validateRuntimeIncident(prefix string, incident runtimeIncident) error {
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "incident_id", value: incident.IncidentID},
		{name: "first_seen", value: incident.FirstSeen},
		{name: "last_seen", value: incident.LastSeen},
		{name: "level", value: incident.Level},
		{name: "component", value: incident.Component},
	} {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("malformed %s: %s is required", prefix, field.name)
		}
	}
	if err := validateRequiredTimestamp(prefix+".first_seen", incident.FirstSeen); err != nil {
		return err
	}
	if err := validateRequiredTimestamp(prefix+".last_seen", incident.LastSeen); err != nil {
		return err
	}
	if err := validateRuntimeIncidentOpaqueID(prefix+".incident_id", incident.IncidentID); err != nil {
		return err
	}
	if incident.Count < 1 {
		return fmt.Errorf("malformed %s: count must be at least 1", prefix)
	}
	if _, ok := runtimeIncidentValidLevels[strings.TrimSpace(incident.Level)]; !ok {
		return fmt.Errorf("malformed %s: level=%q is not a valid LogLevel", prefix, incident.Level)
	}
	if incident.SampleMessage == nil {
		return fmt.Errorf("malformed %s: sample_message is required", prefix)
	}
	if incident.SampleLogIDs == nil {
		return fmt.Errorf("malformed %s: sample_log_ids is required", prefix)
	}
	for i, logID := range incident.SampleLogIDs {
		if err := validateRuntimeIncidentOpaqueID(fmt.Sprintf("%s.sample_log_ids[%d]", prefix, i), logID); err != nil {
			return err
		}
	}
	return nil
}

func validateRuntimeIncidentOpaqueID(field, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("malformed result: %s must not be empty", field)
	}
	if len(value) > 256 {
		return fmt.Errorf("malformed result: %s must be at most 256 characters", field)
	}
	if !runtimeIncidentOpaqueIDPattern.MatchString(value) {
		return fmt.Errorf("malformed result: %s must match OpaqueId pattern", field)
	}
	return nil
}

func writeRuntimeIncidentListResult(out io.Writer, result runtimeIncidentListResult) {
	if out == nil {
		return
	}
	if len(result.Incidents) == 0 {
		fmt.Fprintln(out, "No runtime incidents match the filter.")
		return
	}
	fmt.Fprintln(out, "LAST SEEN\tLEVEL\tCOMPONENT\tERROR\tCOUNT\tINCIDENT ID\tSAMPLE")
	for _, incident := range result.Incidents {
		fmt.Fprintf(out, "%s\t%s\t%s\t%s\t%d\t%s\t%s\n",
			incident.LastSeen,
			incident.Level,
			incident.Component,
			runtimeIncidentDash(incident.ErrorCode),
			incident.Count,
			incident.IncidentID,
			runtimeIncidentSampleMessage(incident),
		)
	}
	if strings.TrimSpace(result.NextCursor) != "" {
		fmt.Fprintf(out, "next_cursor=%s\n", result.NextCursor)
	}
}

func runtimeIncidentErrorClassifier() cliAPIErrorClassifier {
	return cliAPIErrorClassifier{}
}

func runtimeIncidentDash(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "-"
	}
	return value
}

func runtimeIncidentSampleMessage(incident runtimeIncident) string {
	if incident.SampleMessage == nil {
		return ""
	}
	return *incident.SampleMessage
}
