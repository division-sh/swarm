package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

const (
	bundleListMethod   = "bundle.list"
	bundleGetMethod    = "bundle.get"
	bundleAgentsMethod = "bundle.agents"
)

type bundleListCommandOptions struct {
	apiOptions rootCommandOptions
	output     cliOutputOptions

	limit  int
	cursor string

	limitSet  bool
	cursorSet bool
}

type bundleHashCommandOptions struct {
	apiOptions rootCommandOptions
	output     cliOutputOptions
}

type bundleListResult struct {
	Bundles    []bundleSummary `json:"bundles"`
	NextCursor string          `json:"next_cursor,omitempty"`
}

type bundleSummary struct {
	BundleHash    string         `json:"bundle_hash"`
	AgentCount    int            `json:"agent_count"`
	HasData       bool           `json:"has_data"`
	DataSizeBytes int64          `json:"data_size_bytes"`
	Metadata      map[string]any `json:"metadata"`
	IngestedAt    string         `json:"ingested_at"`
}

type bundleDetail struct {
	BundleHash    string         `json:"bundle_hash"`
	ContentYAML   string         `json:"content_yaml"`
	ParsedJSON    map[string]any `json:"parsed_json"`
	Metadata      map[string]any `json:"metadata"`
	AgentCount    int            `json:"agent_count"`
	HasData       bool           `json:"has_data"`
	DataSizeBytes int64          `json:"data_size_bytes"`
	IngestedAt    string         `json:"ingested_at"`
}

type bundleAgentsResult struct {
	Agents []bundleAgentDefinition `json:"agents"`
}

type bundleAgentDefinition struct {
	AgentID          string   `json:"agent_id"`
	FlowInstance     string   `json:"flow_instance,omitempty"`
	Role             string   `json:"role,omitempty"`
	Type             string   `json:"type,omitempty"`
	Model            string   `json:"model,omitempty"`
	LLMBackend       string   `json:"llm_backend,omitempty"`
	ConversationMode string   `json:"conversation_mode,omitempty"`
	SessionScope     string   `json:"session_scope,omitempty"`
	PromptPath       string   `json:"prompt_path,omitempty"`
	Subscriptions    []string `json:"subscriptions,omitempty"`
	Tools            []string `json:"tools,omitempty"`
}

func newBundleCommand(opts rootCommandOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bundle",
		Short: "Inspect persisted bundle catalog entries through v1 RPC.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(
		newBundleListCommand(opts),
		newBundleShowCommand(opts),
		newBundleAgentsCommand(opts),
	)
	return cmd
}

func newBundleListCommand(opts rootCommandOptions) *cobra.Command {
	listOpts := bundleListCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List persisted bundles through /v1/rpc bundle.list.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			listOpts.limitSet = cmd.Flags().Changed("limit")
			listOpts.cursorSet = cmd.Flags().Changed("cursor")
			if err := listOpts.output.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			return runBundleListCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), listOpts)
		},
	}
	cmd.Flags().IntVar(&listOpts.limit, "limit", 0, "Maximum number of bundles to return, from 1 to 500")
	cmd.Flags().StringVar(&listOpts.cursor, "cursor", "", "Opaque pagination cursor returned by bundle.list")
	bindCLIOutputFlags(cmd, &listOpts.output)
	bindCLIAPIConnectionFlags(cmd, &listOpts.apiOptions)
	return cmd
}

func newBundleShowCommand(opts rootCommandOptions) *cobra.Command {
	showOpts := bundleHashCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "show <bundle-hash>",
		Short: "Show one persisted bundle through /v1/rpc bundle.get.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := showOpts.output.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			return runBundleShowCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), showOpts, args[0])
		},
	}
	bindCLIOutputFlags(cmd, &showOpts.output)
	bindCLIAPIConnectionFlags(cmd, &showOpts.apiOptions)
	return cmd
}

func newBundleAgentsCommand(opts rootCommandOptions) *cobra.Command {
	agentsOpts := bundleHashCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "agents <bundle-hash>",
		Short: "List bundle agent definitions through /v1/rpc bundle.agents.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := agentsOpts.output.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			return runBundleAgentsCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), agentsOpts, args[0])
		},
	}
	bindCLIOutputFlags(cmd, &agentsOpts.output)
	bindCLIAPIConnectionFlags(cmd, &agentsOpts.apiOptions)
	return cmd
}

func runBundleListCommand(ctx context.Context, out, errOut io.Writer, opts bundleListCommandOptions) error {
	params, err := opts.params()
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, bundleAPIErrorClassifier())
	}
	var result bundleListResult
	if err := client.call(ctx, bundleListMethod, params, &result); err != nil {
		return returnCLIAPIError(errOut, err, bundleAPIErrorClassifier())
	}
	if err := validateBundleListResult(result); err != nil {
		return returnCLIAPIError(errOut, err, bundleAPIErrorClassifier())
	}
	return renderCLIOutput(out, errOut, opts.output, result, func(w io.Writer) {
		writeBundleListHuman(w, result)
	}, func() ([]string, error) {
		values := make([]string, 0, len(result.Bundles))
		for _, bundle := range result.Bundles {
			values = append(values, bundle.BundleHash)
		}
		return values, nil
	})
}

func runBundleShowCommand(ctx context.Context, out, errOut io.Writer, opts bundleHashCommandOptions, rawBundleHash string) error {
	bundleHash, err := validateBundleHashArg("bundle hash", rawBundleHash)
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, bundleAPIErrorClassifier())
	}
	var result bundleDetail
	if err := client.call(ctx, bundleGetMethod, map[string]any{"bundle_hash": bundleHash}, &result); err != nil {
		return returnCLIAPIError(errOut, err, bundleAPIErrorClassifier())
	}
	if err := validateBundleDetail(result, bundleHash); err != nil {
		return returnCLIAPIError(errOut, err, bundleAPIErrorClassifier())
	}
	return renderCLIOutput(out, errOut, opts.output, result, func(w io.Writer) {
		writeBundleDetailHuman(w, result)
	}, func() ([]string, error) {
		return []string{result.BundleHash}, nil
	})
}

func runBundleAgentsCommand(ctx context.Context, out, errOut io.Writer, opts bundleHashCommandOptions, rawBundleHash string) error {
	bundleHash, err := validateBundleHashArg("bundle hash", rawBundleHash)
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, bundleAPIErrorClassifier())
	}
	var result bundleAgentsResult
	if err := client.call(ctx, bundleAgentsMethod, map[string]any{"bundle_hash": bundleHash}, &result); err != nil {
		return returnCLIAPIError(errOut, err, bundleAPIErrorClassifier())
	}
	if err := validateBundleAgentsResult(result); err != nil {
		return returnCLIAPIError(errOut, err, bundleAPIErrorClassifier())
	}
	return renderCLIOutput(out, errOut, opts.output, result, func(w io.Writer) {
		writeBundleAgentsHuman(w, result)
	}, func() ([]string, error) {
		values := make([]string, 0, len(result.Agents))
		for _, agent := range result.Agents {
			values = append(values, agent.AgentID)
		}
		return values, nil
	})
}

func (opts bundleListCommandOptions) params() (map[string]any, error) {
	params := map[string]any{}
	if opts.limitSet {
		if opts.limit < 1 || opts.limit > 500 {
			return nil, fmt.Errorf("--limit must be between 1 and 500")
		}
		params["limit"] = opts.limit
	}
	cursor, err := optionalNonEmptyFlag("--cursor", opts.cursor, opts.cursorSet)
	if err != nil {
		return nil, err
	}
	if cursor != "" {
		params["cursor"] = cursor
	}
	return params, nil
}

func bundleAPIErrorClassifier() cliAPIErrorClassifier {
	return cliAPIErrorClassifier{notFoundCodes: []string{"BUNDLE_NOT_FOUND"}}
}

func validateBundleHashArg(name, raw string) (string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", fmt.Errorf("%s is required", name)
	}
	if !cliBundleHashPattern.MatchString(value) {
		return "", fmt.Errorf("%s must match bundle-v1:sha256:<64 lowercase hex>", name)
	}
	return value, nil
}

func validateBundleListResult(result bundleListResult) error {
	if result.Bundles == nil {
		return fmt.Errorf("malformed bundle.list result: bundles is required")
	}
	for i, bundle := range result.Bundles {
		if err := validateBundleSummary(fmt.Sprintf("bundles[%d]", i), bundle); err != nil {
			return err
		}
	}
	return nil
}

func validateBundleSummary(path string, summary bundleSummary) error {
	if _, err := validateBundleHashArg(path+".bundle_hash", summary.BundleHash); err != nil {
		return fmt.Errorf("malformed bundle summary: %w", err)
	}
	if summary.AgentCount < 0 {
		return fmt.Errorf("malformed bundle summary: %s.agent_count must be non-negative", path)
	}
	if summary.DataSizeBytes < 0 {
		return fmt.Errorf("malformed bundle summary: %s.data_size_bytes must be non-negative", path)
	}
	if summary.Metadata == nil {
		return fmt.Errorf("malformed bundle summary: %s.metadata is required", path)
	}
	if _, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(summary.IngestedAt)); err != nil {
		return fmt.Errorf("malformed bundle summary: %s.ingested_at must be RFC3339: %w", path, err)
	}
	return nil
}

func validateBundleDetail(result bundleDetail, expectedBundleHash string) error {
	if _, err := validateBundleHashArg("bundle_hash", result.BundleHash); err != nil {
		return fmt.Errorf("malformed bundle.get result: %w", err)
	}
	if result.BundleHash != expectedBundleHash {
		return fmt.Errorf("malformed bundle.get result: bundle_hash=%q, want %q", result.BundleHash, expectedBundleHash)
	}
	if strings.TrimSpace(result.ContentYAML) == "" {
		return fmt.Errorf("malformed bundle.get result: content_yaml is required")
	}
	if result.ParsedJSON == nil {
		return fmt.Errorf("malformed bundle.get result: parsed_json is required")
	}
	if err := validateBundleSummary("bundle", bundleSummary{
		BundleHash:    result.BundleHash,
		AgentCount:    result.AgentCount,
		HasData:       result.HasData,
		DataSizeBytes: result.DataSizeBytes,
		Metadata:      result.Metadata,
		IngestedAt:    result.IngestedAt,
	}); err != nil {
		return fmt.Errorf("malformed bundle.get result: %w", err)
	}
	return nil
}

func validateBundleAgentsResult(result bundleAgentsResult) error {
	if result.Agents == nil {
		return fmt.Errorf("malformed bundle.agents result: agents is required")
	}
	for i, agent := range result.Agents {
		if strings.TrimSpace(agent.AgentID) == "" {
			return fmt.Errorf("malformed bundle.agents result: agents[%d].agent_id is required", i)
		}
	}
	return nil
}

func writeBundleListHuman(w io.Writer, result bundleListResult) {
	if w == nil {
		return
	}
	if len(result.Bundles) == 0 {
		fmt.Fprintln(w, "No bundles found")
	} else {
		for _, bundle := range result.Bundles {
			fmt.Fprintf(w, "bundle %s agents=%d has_data=%t data_size_bytes=%d ingested_at=%s",
				bundle.BundleHash, bundle.AgentCount, bundle.HasData, bundle.DataSizeBytes, bundle.IngestedAt)
			if rendered := compactJSONValue(bundle.Metadata); rendered != "{}" {
				fmt.Fprintf(w, " metadata=%s", rendered)
			}
			fmt.Fprintln(w)
		}
	}
	if cursor := strings.TrimSpace(result.NextCursor); cursor != "" {
		fmt.Fprintf(w, "next_cursor=%s\n", cursor)
	}
}

func writeBundleDetailHuman(w io.Writer, result bundleDetail) {
	if w == nil {
		return
	}
	fmt.Fprintf(w, "Bundle %s\n", result.BundleHash)
	fmt.Fprintf(w, "agents=%d has_data=%t data_size_bytes=%d ingested_at=%s\n", result.AgentCount, result.HasData, result.DataSizeBytes, result.IngestedAt)
	if rendered := compactJSONValue(result.Metadata); rendered != "{}" {
		fmt.Fprintf(w, "metadata=%s\n", rendered)
	}
	fmt.Fprintf(w, "parsed_json=%s\n", compactJSONValue(result.ParsedJSON))
	fmt.Fprintln(w, "content_yaml:")
	fmt.Fprintln(w, strings.TrimRight(result.ContentYAML, "\n"))
}

func writeBundleAgentsHuman(w io.Writer, result bundleAgentsResult) {
	if w == nil {
		return
	}
	if len(result.Agents) == 0 {
		fmt.Fprintln(w, "No bundle agents found")
		return
	}
	for _, agent := range result.Agents {
		fields := []string{fmt.Sprintf("agent %s", agent.AgentID)}
		appendKV := func(key, value string) {
			if strings.TrimSpace(value) != "" {
				fields = append(fields, fmt.Sprintf("%s=%s", key, value))
			}
		}
		appendKV("flow_instance", agent.FlowInstance)
		appendKV("role", agent.Role)
		appendKV("type", agent.Type)
		appendKV("model", agent.Model)
		appendKV("llm_backend", agent.LLMBackend)
		appendKV("conversation_mode", agent.ConversationMode)
		appendKV("session_scope", agent.SessionScope)
		appendKV("prompt_path", agent.PromptPath)
		if len(agent.Subscriptions) > 0 {
			fields = append(fields, "subscriptions="+strings.Join(agent.Subscriptions, ","))
		}
		if len(agent.Tools) > 0 {
			fields = append(fields, "tools="+strings.Join(agent.Tools, ","))
		}
		fmt.Fprintln(w, strings.Join(fields, " "))
	}
}

func compactJSONValue(value any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return "{}"
	}
	return string(raw)
}
