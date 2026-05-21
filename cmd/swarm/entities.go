package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"

	"github.com/spf13/cobra"
)

const (
	entityListMethod      = "entity.list"
	entityGetMethod       = "entity.get"
	entityAggregateMethod = "entity.aggregate"
)

type entityListCommandOptions struct {
	apiOptions rootCommandOptions

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
	Entity      entitySummary   `json:"entity"`
	Fields      map[string]any  `json:"fields"`
	Gates       map[string]bool `json:"gates"`
	Accumulated map[string]any  `json:"accumulated"`
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
		Short: "List entities through v1 RPC.",
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
		Short: "View or aggregate entities through v1 RPC.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(
		newEntityViewCommand(opts),
		newEntityAggregateCommand(opts),
	)
	return cmd
}

func newEntitiesListCommand(opts rootCommandOptions) *cobra.Command {
	listOpts := entityListCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List entities through /v1/rpc entity.list.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
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
	return cmd
}

func newEntityViewCommand(opts rootCommandOptions) *cobra.Command {
	viewOpts := entityViewCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "view <entity-id>",
		Short: "View one entity through /v1/rpc entity.get.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			viewOpts.runIDSet = cmd.Flags().Changed("run-id")
			return runEntityViewCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), args[0], viewOpts)
		},
	}
	cmd.Flags().StringVar(&viewOpts.runID, "run-id", "", "Disambiguate entities reused across runs")
	bindCLIAPIConnectionFlags(cmd, &viewOpts.apiOptions)
	return cmd
}

func newEntityAggregateCommand(opts rootCommandOptions) *cobra.Command {
	aggregateOpts := entityAggregateCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "aggregate",
		Short: "Aggregate entity counts through /v1/rpc entity.aggregate.",
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
	writeEntityListResult(out, result)
	return nil
}

func runEntityViewCommand(ctx context.Context, out, errOut io.Writer, entityID string, opts entityViewCommandOptions) error {
	entityID = strings.TrimSpace(entityID)
	if err := validateEntityOpaqueIDArg("entity id", entityID); err != nil {
		return returnCLIValidationError(errOut, err)
	}
	params := map[string]any{"entity_id": entityID}
	if opts.runIDSet {
		runID, err := entityNonEmptyFlag("--run-id", opts.runID)
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
	var result entityFull
	if err := client.call(ctx, entityGetMethod, params, &result); err != nil {
		return returnCLIAPIError(errOut, err, entityViewAPIErrorClassifier())
	}
	if err := validateEntityFullResult("entity.get result", result); err != nil {
		return returnCLIAPIError(errOut, err, entityViewAPIErrorClassifier())
	}
	writeEntityFullResult(out, result)
	return nil
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

func writeEntityListResult(out io.Writer, result entityListResult) {
	if out == nil {
		return
	}
	if len(result.Entities) == 0 {
		fmt.Fprintln(out, "No entities match the filter.")
		return
	}
	fmt.Fprintln(out, "ENTITY_ID\tRUN_ID\tFLOW\tTYPE\tSTATE\tREVISION\tUPDATED\tSLUG\tNAME")
	for _, entity := range result.Entities {
		fmt.Fprintf(out, "%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s\n",
			entity.EntityID,
			entity.RunID,
			entity.FlowInstance,
			entity.EntityType,
			entity.CurrentState,
			entity.Revision,
			entity.UpdatedAt,
			entityDash(entity.Slug),
			entityDash(entity.Name),
		)
	}
	if strings.TrimSpace(result.NextCursor) != "" {
		fmt.Fprintf(out, "next_cursor=%s\n", result.NextCursor)
	}
}

func writeEntityFullResult(out io.Writer, result entityFull) {
	if out == nil {
		return
	}
	entity := result.Entity
	fmt.Fprintf(out, "Entity %s\n", entity.EntityID)
	fmt.Fprintf(out, "run_id=%s flow=%s type=%s state=%s revision=%d created_at=%s updated_at=%s\n",
		entity.RunID,
		entity.FlowInstance,
		entity.EntityType,
		entity.CurrentState,
		entity.Revision,
		entity.CreatedAt,
		entity.UpdatedAt,
	)
	fmt.Fprintf(out, "slug=%s name=%s\n", entityDash(entity.Slug), entityDash(entity.Name))
	fmt.Fprintf(out, "fields=%s\n", entityCompactJSON(result.Fields))
	fmt.Fprintf(out, "gates=%s\n", entityCompactJSON(result.Gates))
	fmt.Fprintf(out, "accumulated=%s\n", entityCompactJSON(result.Accumulated))
}

func writeEntityAggregateResult(out io.Writer, result entityAggregateResult) {
	if out == nil {
		return
	}
	if len(result.Counts) == 0 {
		fmt.Fprintln(out, "No entity aggregate rows match the filter.")
		return
	}
	keys := make([]string, 0, len(result.Counts))
	for key := range result.Counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	fmt.Fprintln(out, "GROUP\tCOUNT")
	for _, key := range keys {
		fmt.Fprintf(out, "%s\t%d\n", entityDash(key), result.Counts[key])
	}
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
