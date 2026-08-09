package cliapp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/cli/argcount"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/spf13/cobra"
)

const (
	entityListMethod      = "entity.list"
	entityGetMethod       = "entity.get"
	entityAggregateMethod = "entity.aggregate"
)

type entityListCommandOptions struct {
	apiOptions rootCommandOptions
	output     cliOutputOptions

	runID        string
	flow         string
	entityType   string
	currentState string
	limit        int
	cursor       string

	runIDSet        bool
	flowSet         bool
	entityTypeSet   bool
	currentStateSet bool
	limitSet        bool
	cursorSet       bool
}

type entityViewCommandOptions struct {
	apiOptions rootCommandOptions
	output     cliOutputOptions

	runID    string
	runIDSet bool
}

type entityAggregateCommandOptions struct {
	apiOptions rootCommandOptions

	runID    string
	groupBy  string
	typeName string

	runIDSet   bool
	groupBySet bool
	typeSet    bool
}

type entityListResult struct {
	Entities   []entitySummary `json:"entities"`
	NextCursor string          `json:"next_cursor,omitempty"`
}

type entitySummary struct {
	EntityID     string `json:"entity_id"`
	RunID        string `json:"run_id"`
	FlowInstance string `json:"flow_instance"`
	EntityType   string `json:"entity_type"`
	CurrentState string `json:"current_state"`
	Slug         string `json:"slug,omitempty"`
	Name         string `json:"name,omitempty"`
	Revision     int    `json:"revision"`
	CreatedAt    string `json:"created_at"`
	UpdatedAt    string `json:"updated_at"`
}

type entityFull struct {
	Entity      entitySummary                  `json:"entity"`
	Fields      map[string]any                 `json:"fields"`
	Gates       map[string]bool                `json:"gates"`
	Accumulated map[string]any                 `json:"accumulated"`
	Loops       []loopruntime.PublicActivation `json:"loops,omitempty"`
}

type entityAggregateResult struct {
	Counts map[string]int `json:"counts"`
}

var (
	entityOpaqueIDPattern   = regexp.MustCompile(`^[A-Za-z0-9_:.-]+$`)
	entityFieldGroupPattern = regexp.MustCompile(`^fields\.[A-Za-z0-9_]+(\.[A-Za-z0-9_]+)*$`)
	entityGroupFields       = map[string]struct{}{
		"current_state":    {},
		"flow":             {},
		"flow_instance":    {},
		"workflow_name":    {},
		"workflow_version": {},
		"type":             {},
		"entity_type":      {},
		"slug":             {},
		"name":             {},
	}
)

func newEntitiesCommand(opts rootCommandOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "entities",
		Short: "List workflow entities.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(newEntitiesListCommand(opts))
	return cmd
}

func newEntityCommand(opts rootCommandOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "entity",
		Short: "List entities, or view and aggregate one entity's state.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(
		newEntitiesListCommand(opts),
		newEntityViewCommand(opts),
		newEntityAggregateCommand(opts),
	)
	return cmd
}

func newEntitiesListCommand(opts rootCommandOptions) *cobra.Command {
	listOpts := entityListCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List entities with filters.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := listOpts.output.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			listOpts.runIDSet = cmd.Flags().Changed("run-id")
			listOpts.flowSet = cmd.Flags().Changed("flow")
			listOpts.entityTypeSet = cmd.Flags().Changed("type")
			listOpts.currentStateSet = cmd.Flags().Changed("current-state")
			listOpts.limitSet = cmd.Flags().Changed("limit")
			listOpts.cursorSet = cmd.Flags().Changed("cursor")
			return runEntitiesListCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), listOpts)
		},
	}
	cmd.Flags().StringVar(&listOpts.runID, "run-id", "", "Filter by run id")
	cmd.Flags().StringVar(&listOpts.flow, "flow", "", "Filter by flow instance")
	cmd.Flags().StringVar(&listOpts.entityType, "type", "", "Filter by entity type")
	cmd.Flags().StringVar(&listOpts.currentState, "current-state", "", "Filter by current entity state")
	cmd.Flags().IntVar(&listOpts.limit, "limit", 0, "Optional page size, 1-500")
	cmd.Flags().StringVar(&listOpts.cursor, "cursor", "", "Pagination cursor")
	bindCLIAPIConnectionFlags(cmd, &listOpts.apiOptions)
	bindCLIOutputFlags(cmd, &listOpts.output)
	bindCLIOutputVerboseFlag(cmd, &listOpts.output)
	return cmd
}

func newEntityViewCommand(opts rootCommandOptions) *cobra.Command {
	viewOpts := entityViewCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "view <entity-id>",
		Short: "View one entity's state.",
		Args:  argcount.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := viewOpts.output.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			viewOpts.runIDSet = cmd.Flags().Changed("run-id")
			return runEntityViewCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), args[0], viewOpts)
		},
	}
	argcount.SetDiscoveryHint(cmd, "List entity ids with `swarm entity list`.")
	cmd.Flags().StringVar(&viewOpts.runID, "run-id", "", "Disambiguate entities reused across runs")
	bindCLIAPIConnectionFlags(cmd, &viewOpts.apiOptions)
	bindCLIOutputFlags(cmd, &viewOpts.output)
	bindCLIOutputVerboseFlag(cmd, &viewOpts.output)
	return cmd
}

func newEntityAggregateCommand(opts rootCommandOptions) *cobra.Command {
	aggregateOpts := entityAggregateCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "aggregate",
		Short: "Aggregate entity counts by field.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			aggregateOpts.runIDSet = cmd.Flags().Changed("run-id")
			aggregateOpts.groupBySet = cmd.Flags().Changed("group-by")
			aggregateOpts.typeSet = cmd.Flags().Changed("type")
			return runEntityAggregateCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), aggregateOpts)
		},
	}
	cmd.Flags().StringVar(&aggregateOpts.runID, "run-id", "", "Filter by run id")
	cmd.Flags().StringVar(&aggregateOpts.groupBy, "group-by", "", "Group by current_state, flow, flow_instance, workflow_name, workflow_version, type, entity_type, slug, name, or fields.<path>")
	cmd.Flags().StringVar(&aggregateOpts.typeName, "type", "", "Filter by entity type")
	bindCLIAPIConnectionFlags(cmd, &aggregateOpts.apiOptions)
	return cmd
}

func runEntitiesListCommand(ctx context.Context, out, errOut io.Writer, opts entityListCommandOptions) error {
	params, err := opts.params()
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, entityListAPIErrorClassifier())
	}
	var result entityListResult
	if err := client.call(ctx, entityListMethod, params, &result); err != nil {
		return returnCLIAPIError(errOut, err, entityListAPIErrorClassifier())
	}
	if err := validateEntityListResult(result); err != nil {
		return returnCLIAPIError(errOut, err, entityListAPIErrorClassifier())
	}
	return renderCLIOutput(out, errOut, opts.output, result, func(w io.Writer) {
		writeEntityListResult(w, result, entityListRenderOptions{verbose: opts.output.verbose, runScoped: opts.runIDSet})
	}, func() ([]string, error) {
		ids := make([]string, 0, len(result.Entities))
		for _, entity := range result.Entities {
			ids = append(ids, entity.EntityID)
		}
		return ids, nil
	})
}

func runEntityViewCommand(ctx context.Context, out, errOut io.Writer, entityID string, opts entityViewCommandOptions) error {
	entityID = strings.TrimSpace(entityID)
	if err := validateEntityOpaqueIDArg("entity id", entityID); err != nil {
		return returnCLIValidationError(errOut, err)
	}
	params := map[string]any{}
	runID := ""
	if opts.runIDSet {
		var err error
		runID, err = entityNonEmptyFlag("--run-id", opts.runID)
		if err != nil {
			return returnCLIValidationError(errOut, err)
		}
		if err := validateEntityOpaqueIDArg("--run-id", runID); err != nil {
			return returnCLIValidationError(errOut, err)
		}
		params["run_id"] = runID
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, entityViewAPIErrorClassifier())
	}
	params["entity_id"] = entityID
	var result entityFull
	if err := client.call(ctx, entityGetMethod, params, &result); err != nil {
		if runID == "" {
			return returnCLIAPIError(errOut, err, entityViewAPIErrorClassifier())
		}
		entityID, err = resolveCLIIdentifierAfterNotFound(ctx, client, cliIdentifierResolveRequest{
			Command: "swarm entity view", Selector: "arg:entity-id", Value: entityID,
			Scope: map[string]string{"run_id": runID},
		}, err, "ENTITY_NOT_FOUND")
		if err != nil {
			return returnCLIAPIError(errOut, err, entityViewAPIErrorClassifier())
		}
		params["entity_id"] = entityID
		if err := client.call(ctx, entityGetMethod, params, &result); err != nil {
			return returnCLIAPIError(errOut, err, entityViewAPIErrorClassifier())
		}
	}
	if err := validateEntityFullResult("entity.get result", result); err != nil {
		return returnCLIAPIError(errOut, err, entityViewAPIErrorClassifier())
	}
	return renderCLIOutput(out, errOut, opts.output, result, func(w io.Writer) {
		writeEntityFullResult(w, result, entityRenderOptions{verbose: opts.output.verbose})
	}, func() ([]string, error) {
		return []string{result.Entity.EntityID}, nil
	})
}

func runEntityAggregateCommand(ctx context.Context, out, errOut io.Writer, opts entityAggregateCommandOptions) error {
	params, err := opts.params()
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, entityAggregateAPIErrorClassifier())
	}
	var result entityAggregateResult
	if err := client.call(ctx, entityAggregateMethod, params, &result); err != nil {
		return returnCLIAPIError(errOut, err, entityAggregateAPIErrorClassifier())
	}
	if err := validateEntityAggregateResult(result); err != nil {
		return returnCLIAPIError(errOut, err, entityAggregateAPIErrorClassifier())
	}
	writeEntityAggregateResult(out, result)
	return nil
}

func (opts entityListCommandOptions) params() (map[string]any, error) {
	params := map[string]any{}
	if opts.runIDSet {
		runID, err := entityNonEmptyFlag("--run-id", opts.runID)
		if err != nil {
			return nil, err
		}
		if err := validateEntityOpaqueIDArg("--run-id", runID); err != nil {
			return nil, err
		}
		params["run_id"] = runID
	}
	if opts.flowSet {
		flow, err := entityNonEmptyFlag("--flow", opts.flow)
		if err != nil {
			return nil, err
		}
		params["flow"] = flow
	}
	if opts.entityTypeSet {
		entityType, err := entityNonEmptyFlag("--type", opts.entityType)
		if err != nil {
			return nil, err
		}
		params["type"] = entityType
	}
	if opts.currentStateSet {
		currentState, err := entityNonEmptyFlag("--current-state", opts.currentState)
		if err != nil {
			return nil, err
		}
		params["current_state"] = currentState
	}
	if opts.limitSet {
		if opts.limit < 1 || opts.limit > 500 {
			return nil, fmt.Errorf("--limit must be between 1 and 500")
		}
		params["limit"] = opts.limit
	}
	if opts.cursorSet {
		cursor, err := entityNonEmptyFlag("--cursor", opts.cursor)
		if err != nil {
			return nil, err
		}
		params["cursor"] = cursor
	}
	return params, nil
}

func (opts entityAggregateCommandOptions) params() (map[string]any, error) {
	params := map[string]any{}
	if opts.runIDSet {
		runID, err := entityNonEmptyFlag("--run-id", opts.runID)
		if err != nil {
			return nil, err
		}
		if err := validateEntityOpaqueIDArg("--run-id", runID); err != nil {
			return nil, err
		}
		params["run_id"] = runID
	}
	if opts.groupBySet {
		groupBy, err := entityNonEmptyFlag("--group-by", opts.groupBy)
		if err != nil {
			return nil, err
		}
		if err := validateEntityGroupBy(groupBy); err != nil {
			return nil, err
		}
		params["group_by"] = groupBy
	}
	if opts.typeSet {
		entityType, err := entityNonEmptyFlag("--type", opts.typeName)
		if err != nil {
			return nil, err
		}
		params["type"] = entityType
	}
	return params, nil
}

func entityNonEmptyFlag(name, value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("%s must not be empty", name)
	}
	return value, nil
}

func validateEntityOpaqueIDArg(name, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s is required", name)
	}
	if len(value) > 256 {
		return fmt.Errorf("%s must be at most 256 characters", name)
	}
	if !entityOpaqueIDPattern.MatchString(value) {
		return fmt.Errorf("%s must match OpaqueId pattern", name)
	}
	return nil
}

func validateEntityOpaqueIDField(field, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s is required", field)
	}
	if len(value) > 256 {
		return fmt.Errorf("%s must be at most 256 characters", field)
	}
	if !entityOpaqueIDPattern.MatchString(value) {
		return fmt.Errorf("%s must match OpaqueId pattern", field)
	}
	return nil
}

func validateEntityGroupBy(groupBy string) error {
	if _, ok := entityGroupFields[groupBy]; ok {
		return nil
	}
	if entityFieldGroupPattern.MatchString(groupBy) {
		return nil
	}
	return fmt.Errorf("--group-by must be one of current_state, flow, flow_instance, workflow_name, workflow_version, type, entity_type, slug, name, or fields.<path>")
}

func validateEntityListResult(result entityListResult) error {
	if result.Entities == nil {
		return fmt.Errorf("malformed entity.list result: entities is required")
	}
	for i, entity := range result.Entities {
		if err := validateEntitySummary(fmt.Sprintf("entity.list result: entities[%d]", i), entity); err != nil {
			return err
		}
	}
	return nil
}

func validateEntityFullResult(prefix string, result entityFull) error {
	if err := validateEntitySummary(prefix+".entity", result.Entity); err != nil {
		return err
	}
	if result.Fields == nil {
		return fmt.Errorf("malformed %s: fields is required", prefix)
	}
	if result.Gates == nil {
		return fmt.Errorf("malformed %s: gates is required", prefix)
	}
	if result.Accumulated == nil {
		return fmt.Errorf("malformed %s: accumulated is required", prefix)
	}
	return nil
}

func validateEntitySummary(prefix string, entity entitySummary) error {
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "entity_id", value: entity.EntityID},
		{name: "run_id", value: entity.RunID},
		{name: "flow_instance", value: entity.FlowInstance},
		{name: "entity_type", value: entity.EntityType},
		{name: "current_state", value: entity.CurrentState},
		{name: "created_at", value: entity.CreatedAt},
		{name: "updated_at", value: entity.UpdatedAt},
	} {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("malformed %s: %s is required", prefix, field.name)
		}
	}
	if err := validateEntityOpaqueIDField(prefix+".entity_id", entity.EntityID); err != nil {
		return fmt.Errorf("malformed %s: %w", prefix, err)
	}
	if err := validateEntityOpaqueIDField(prefix+".run_id", entity.RunID); err != nil {
		return fmt.Errorf("malformed %s: %w", prefix, err)
	}
	if entity.Revision < 0 {
		return fmt.Errorf("malformed %s: revision must be non-negative", prefix)
	}
	if err := validateRequiredTimestamp(prefix+".created_at", entity.CreatedAt); err != nil {
		return err
	}
	if err := validateRequiredTimestamp(prefix+".updated_at", entity.UpdatedAt); err != nil {
		return err
	}
	return nil
}

func validateEntityAggregateResult(result entityAggregateResult) error {
	if result.Counts == nil {
		return fmt.Errorf("malformed entity.aggregate result: counts is required")
	}
	for key, count := range result.Counts {
		if count < 0 {
			return fmt.Errorf("malformed entity.aggregate result: counts[%q] must be non-negative", key)
		}
	}
	return nil
}

type entityListRenderOptions struct {
	verbose   bool
	runScoped bool
}

func writeEntityListResult(out io.Writer, result entityListResult, opts entityListRenderOptions) {
	if out == nil {
		return
	}
	columns := []cliTableColumn{
		{Header: "ENTITY_ID", KeyColumn: true, IdentifierFamily: cliIdentifierFamilyEntity},
	}
	if !opts.runScoped {
		columns = append(columns, cliTableColumn{Header: "RUN", IdentifierFamily: cliIdentifierFamilyRun})
	}
	columns = append(columns,
		cliTableColumn{Header: "TYPE"},
		cliTableColumn{Header: "STATE"},
		cliTableColumn{Header: "FLOW", IdentifierFamily: cliIdentifierFamilyFlowInstance},
		cliTableColumn{Header: "UPDATED"},
	)
	rows := make([][]string, 0, len(result.Entities))
	for _, entity := range result.Entities {
		row := []string{entity.EntityID}
		if !opts.runScoped {
			row = append(row, entity.RunID)
		}
		row = append(row,
			entityDash(entityOneLine(entity.EntityType)),
			entityDash(entityOneLine(entity.CurrentState)),
			entityShortFlow(entityOneLine(entity.FlowInstance)),
			entityTimestampText(cliRelativeTimeNow(), entity.UpdatedAt, opts.verbose),
		)
		rows = append(rows, row)
	}
	footers := []string{}
	if strings.TrimSpace(result.NextCursor) != "" {
		footers = append(footers, fmt.Sprintf("next_cursor=%s", result.NextCursor))
	}
	writeCLITable(out, cliTable{
		Columns:      columns,
		Rows:         rows,
		EmptyMessage: "No entities match the current filters.",
		FooterLines:  footers,
	})
}

type entityRenderOptions struct {
	verbose bool
}

func writeEntityFullResult(out io.Writer, result entityFull, opts entityRenderOptions) {
	if out == nil {
		return
	}
	entity := result.Entity
	now := cliRelativeTimeNow()
	header := []cliLabeledDetailRow{
		{Label: "run", Value: entityOneLine(entity.RunID)},
		{Label: "flow", Value: entityOneLine(entity.FlowInstance)},
		{Label: "type", Value: entityDash(entityOneLine(entity.EntityType))},
		{Label: "state", Value: entityDash(entityOneLine(entity.CurrentState))},
		{Label: "revision", Value: fmt.Sprintf("%d", entity.Revision)},
		{Label: "created", Value: entityTimestampText(now, entity.CreatedAt, opts.verbose)},
		{Label: "updated", Value: entityTimestampText(now, entity.UpdatedAt, opts.verbose)},
	}
	if strings.TrimSpace(entity.Slug) != "" {
		header = append(header, cliLabeledDetailRow{Label: "slug", Value: entityOneLine(entity.Slug)})
	}
	if strings.TrimSpace(entity.Name) != "" {
		header = append(header, cliLabeledDetailRow{Label: "name", Value: entityOneLine(entity.Name)})
	}
	writeCLILabeledDetail(out, cliLabeledDetail{Title: "Entity " + entity.EntityID, Rows: header})

	writeEntityFieldSection(out, "Fields", result.Fields, entityContentField, true)
	writeEntityGatesSection(out, result.Gates)
	writeEntityAccumulatedSection(out, result.Accumulated, opts.verbose)
	if opts.verbose {
		writeEntityLoopSection(out, result.Loops)
		writeEntityFieldSection(out, "Bookkeeping", result.Fields, entityBookkeepingField, false)
	}
}

// entityTimestampText renders a humanized relative time; under --verbose it
// appends the absolute ISO value so operators can pin the exact moment.
func entityTimestampText(now time.Time, raw string, verbose bool) string {
	relative := formatCLIRelativeTime(now, raw)
	if !verbose {
		return relative
	}
	return relative + " (" + raw + ")"
}

// entityOneLine normalizes an unconstrained summary string (run id, flow,
// type, state, slug, name) so embedded line-breaking characters cannot break
// the aligned detail-row or table line discipline.
func entityOneLine(value string) string {
	return cliRenderOneLineValue(value)
}

// entityContentField reports whether a field key is workflow-declared content
// (default-visible) rather than platform-injected bookkeeping. The platform key
// set is owned by runtimecontracts.EntityFieldBookkeepingKeys, adjacent to the
// injection sites; a platform key forgotten there shows up in default output
// (fail-visible) instead of silently vanishing.
func entityContentField(key string) bool {
	return !runtimecontracts.IsEntityFieldBookkeepingKey(key)
}

func entityBookkeepingField(key string) bool {
	return runtimecontracts.IsEntityFieldBookkeepingKey(key)
}

func writeEntityFieldSection(out io.Writer, title string, fields map[string]any, include func(string) bool, stateEmpty bool) {
	rows := entityFieldRows(fields, include)
	if len(rows) == 0 {
		if stateEmpty {
			writeCLITitle(out, title+"  none")
		}
		return
	}
	writeCLILabeledDetail(out, cliLabeledDetail{Title: title, Rows: rows})
}

func entityFieldRows(fields map[string]any, include func(string) bool) []cliLabeledDetailRow {
	if include == nil {
		include = func(string) bool { return true }
	}
	keys := make([]string, 0, len(fields))
	for key := range fields {
		if include(key) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	rows := make([]cliLabeledDetailRow, 0, len(keys))
	for _, key := range keys {
		rows = append(rows, cliLabeledDetailRow{Label: cliRenderOneLineLabel(key), Value: entityFieldValue(fields[key])})
	}
	return rows
}

func entityFieldValue(value any) string {
	switch v := value.(type) {
	case nil:
		return "none"
	case bool:
		if v {
			return "true"
		}
		return "false"
	case string:
		rendered := cliRenderOneLineValue(v)
		if strings.TrimSpace(rendered) == "" {
			return "none"
		}
		return rendered
	case map[string]any:
		if len(v) == 0 {
			return "none"
		}
	case []any:
		if len(v) == 0 {
			return "none"
		}
	}
	return cliRenderOneLineValue(entityCompactJSON(value))
}

// writeEntityGatesSection renders EVERY declared gate with its value in
// default and --verbose; a false gate is not absence, it is the diagnostic an
// operator needs. "Gates  none" renders only when the gate map is empty.
func writeEntityGatesSection(out io.Writer, gates map[string]bool) {
	if len(gates) == 0 {
		writeCLITitle(out, "Gates  none")
		return
	}
	keys := make([]string, 0, len(gates))
	for key := range gates {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	rows := make([]cliLabeledDetailRow, 0, len(keys))
	for _, key := range keys {
		rows = append(rows, cliLabeledDetailRow{Label: cliRenderOneLineLabel(key), Value: fmt.Sprintf("%t", gates[key])})
	}
	writeCLILabeledDetail(out, cliLabeledDetail{Title: "Gates", Rows: rows})
}

// writeEntityAccumulatedSection states accumulated presence in default output
// (never silence) and renders the full map under --verbose.
func writeEntityAccumulatedSection(out io.Writer, accumulated map[string]any, verbose bool) {
	if verbose {
		writeEntityFieldSection(out, "Accumulated", accumulated, nil, true)
		return
	}
	if len(accumulated) == 0 {
		writeCLITitle(out, "Accumulated  none")
		return
	}
	writeCLITitle(out, fmt.Sprintf("Accumulated  %d keys (--verbose to expand)", len(accumulated)))
}

// writeEntityLoopSection renders bounded-loop activations as data under
// --verbose (strings as-is via loopruntime.PublicActivation; no lifecycle
// interpretation).
func writeEntityLoopSection(out io.Writer, loops []loopruntime.PublicActivation) {
	if len(loops) == 0 {
		return
	}
	rows := make([]cliLabeledDetailRow, 0, len(loops))
	for _, loop := range loops {
		summary := fmt.Sprintf("%s · attempt %d/%d · rev %s", loop.Status, loop.Attempt, loop.MaxAttempts, loop.RevisionID)
		if strings.TrimSpace(loop.CloseReason) != "" {
			summary += " · close_reason " + loop.CloseReason
		}
		summary += " · stage " + loop.CurrentStage
		rows = append(rows, cliLabeledDetailRow{Label: cliRenderOneLineLabel(loop.ID), Value: cliRenderOneLineValue(summary)})
	}
	writeCLILabeledDetail(out, cliLabeledDetail{Title: "Loops", Rows: rows})
}

var entityHashSegmentPattern = regexp.MustCompile(`^[0-9a-f]{40,}$`)

const entityHashSegmentMaxRunes = 12

// entityShortFlow truncates ONLY the hash-bearing flow-path segment (e.g. an
// embedded 64-hex bundle digest); instance-discriminating segments stay intact
// so two instances differing past a shared prefix still render as distinct,
// round-trippable cells.
func entityShortFlow(flow string) string {
	segments := strings.Split(strings.Trim(flow, "/"), "/")
	for i, segment := range segments {
		if entityHashSegmentPattern.MatchString(segment) && len(segment) > entityHashSegmentMaxRunes {
			segments[i] = segment[:entityHashSegmentMaxRunes] + "…"
		}
	}
	return strings.Join(segments, "/")
}

func writeEntityAggregateResult(out io.Writer, result entityAggregateResult) {
	if out == nil {
		return
	}
	keys := make([]string, 0, len(result.Counts))
	for key := range result.Counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	rows := make([][]string, 0, len(keys))
	for _, key := range keys {
		rows = append(rows, []string{entityDash(key), fmt.Sprintf("%d", result.Counts[key])})
	}
	writeCLITable(out, cliTable{
		Columns: []cliTableColumn{
			{Header: "GROUP"},
			{Header: "COUNT"},
		},
		Rows:         rows,
		EmptyMessage: "No entity aggregate rows match the current filters.",
	})
}

func entityListAPIErrorClassifier() cliAPIErrorClassifier {
	return cliAPIErrorClassifier{}
}

func entityViewAPIErrorClassifier() cliAPIErrorClassifier {
	return cliAPIErrorClassifier{notFoundCodes: []string{"ENTITY_NOT_FOUND"}}
}

func entityAggregateAPIErrorClassifier() cliAPIErrorClassifier {
	return cliAPIErrorClassifier{}
}

func entityCompactJSON(value any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return "{}"
	}
	return string(raw)
}

func entityDash(value string) string {
	if strings.TrimSpace(value) == "" {
		return "-"
	}
	return value
}
