package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	runtimecontracts "swarm/internal/runtime/contracts"
)

const (
	bundleListMethod     = "bundle.list"
	bundleGetMethod      = "bundle.get"
	bundleAgentsMethod   = "bundle.agents"
	bundleRegisterMethod = "bundle.register"
	bundleDeleteMethod   = "bundle.delete"
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

type bundleRegisterCommandOptions struct {
	apiOptions rootCommandOptions
	output     cliOutputOptions

	dataBlobPath      string
	contractsDir      string
	idempotencyKey    string
	dataBlobSet       bool
	contractsSet      bool
	idempotencyKeySet bool
	repoRoot          string
}

type bundleDeleteCommandOptions struct {
	apiOptions rootCommandOptions
	output     cliOutputOptions

	force          bool
	dryRun         bool
	idempotencyKey string

	forceSet          bool
	dryRunSet         bool
	idempotencyKeySet bool
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

type bundleRegistrationResult struct {
	BundleHash    string `json:"bundle_hash"`
	Registered    *bool  `json:"registered"`
	HasData       *bool  `json:"has_data"`
	DataSizeBytes *int64 `json:"data_size_bytes"`
}

type bundleDeleteResult struct {
	OK                  *bool                      `json:"ok"`
	Status              string                     `json:"status"`
	OperationName       string                     `json:"operation_name"`
	BundleHash          string                     `json:"bundle_hash"`
	Force               *bool                      `json:"force"`
	Deleted             *bool                      `json:"deleted"`
	DryRun              *bool                      `json:"dry_run"`
	ActiveRunsStopped   *int                       `json:"active_runs_stopped"`
	DeliveriesCancelled *int                       `json:"deliveries_cancelled"`
	ContainersStopped   *int                       `json:"containers_stopped"`
	PartialFailure      *bool                      `json:"partial_failure"`
	Plan                map[string]any             `json:"plan"`
	Cleanup             map[string]any             `json:"cleanup"`
	Containers          map[string]any             `json:"containers"`
	FinalMutation       map[string]any             `json:"final_mutation"`
	Errors              []bundleDeletePartialError `json:"errors,omitempty"`
}

type bundleDeletePartialError struct {
	Scope   string `json:"scope"`
	Message string `json:"message"`
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

func newBundleCommand(repoRoot string, opts rootCommandOptions) *cobra.Command {
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
		newBundleRegisterCommand(repoRoot, opts),
		newBundleDeleteCommand(opts),
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

func newBundleRegisterCommand(repoRoot string, opts rootCommandOptions) *cobra.Command {
	registerOpts := bundleRegisterCommandOptions{apiOptions: opts, repoRoot: repoRoot}
	cmd := &cobra.Command{
		Use:   "register <registration-envelope-yaml> | register --contracts <contracts-directory>",
		Short: "Register a bundle through /v1/rpc bundle.register.",
		Args:  cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			registerOpts.dataBlobSet = cmd.Flags().Changed("data-blob")
			registerOpts.contractsSet = cmd.Flags().Changed("contracts")
			registerOpts.idempotencyKeySet = cmd.Flags().Changed("idempotency-key")
			if err := registerOpts.output.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			return runBundleRegisterCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), registerOpts, args)
		},
	}
	cmd.Flags().StringVar(&registerOpts.dataBlobPath, "data-blob", "", "Path to a BundleRegisterDataBlobV1 JSON document")
	cmd.Flags().StringVar(&registerOpts.contractsDir, "contracts", "", "Package a local contracts directory into BundleRegistrationEnvelopeV1 before calling bundle.register")
	cmd.Flags().StringVar(&registerOpts.idempotencyKey, "idempotency-key", "", "Optional idempotency key for bundle.register")
	bindCLIOutputFlags(cmd, &registerOpts.output)
	bindCLIAPIConnectionFlags(cmd, &registerOpts.apiOptions)
	return cmd
}

func newBundleDeleteCommand(opts rootCommandOptions) *cobra.Command {
	deleteOpts := bundleDeleteCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "delete <bundle-hash>",
		Short: "Delete a persisted bundle through /v1/rpc bundle.delete.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			deleteOpts.forceSet = cmd.Flags().Changed("force")
			deleteOpts.dryRunSet = cmd.Flags().Changed("dry-run")
			deleteOpts.idempotencyKeySet = cmd.Flags().Changed("idempotency-key")
			if err := deleteOpts.output.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			return runBundleDeleteCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), deleteOpts, args[0])
		},
	}
	cmd.Flags().BoolVar(&deleteOpts.force, "force", false, "Force bundle deletion by quiescing affected active work before deleting")
	cmd.Flags().BoolVar(&deleteOpts.dryRun, "dry-run", false, "Plan bundle deletion without applying destructive changes")
	cmd.Flags().StringVar(&deleteOpts.idempotencyKey, "idempotency-key", "", "Optional idempotency key for bundle.delete")
	bindCLIOutputFlags(cmd, &deleteOpts.output)
	bindCLIAPIConnectionFlags(cmd, &deleteOpts.apiOptions)
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

func runBundleRegisterCommand(ctx context.Context, out, errOut io.Writer, opts bundleRegisterCommandOptions, args []string) error {
	params, err := opts.params(args)
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, bundleAPIErrorClassifier())
	}
	var result bundleRegistrationResult
	if err := client.call(ctx, bundleRegisterMethod, params, &result); err != nil {
		return returnCLIAPIError(errOut, err, bundleAPIErrorClassifier())
	}
	if err := validateBundleRegistrationResult(result); err != nil {
		return returnCLIAPIError(errOut, err, bundleAPIErrorClassifier())
	}
	return renderCLIOutput(out, errOut, opts.output, result, func(w io.Writer) {
		writeBundleRegistrationHuman(w, result)
	}, func() ([]string, error) {
		return []string{result.BundleHash}, nil
	})
}

func runBundleDeleteCommand(ctx context.Context, out, errOut io.Writer, opts bundleDeleteCommandOptions, rawBundleHash string) error {
	params, bundleHash, err := opts.params(rawBundleHash)
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, bundleDeleteAPIErrorClassifier())
	}
	var result bundleDeleteResult
	if err := client.call(ctx, bundleDeleteMethod, params, &result); err != nil {
		return returnCLIAPIError(errOut, err, bundleDeleteAPIErrorClassifier())
	}
	if err := validateBundleDeleteResult(result, bundleHash); err != nil {
		return returnCLIAPIError(errOut, err, bundleDeleteAPIErrorClassifier())
	}
	return renderCLIOutput(out, errOut, opts.output, result, func(w io.Writer) {
		writeBundleDeleteHuman(w, result)
	}, func() ([]string, error) {
		return []string{result.BundleHash}, nil
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

func (opts bundleRegisterCommandOptions) params(args []string) (map[string]any, error) {
	if opts.contractsSet {
		return opts.contractsDirectoryParams(args)
	}
	if len(args) != 1 {
		return nil, fmt.Errorf("register requires <registration-envelope-yaml> or --contracts <contracts-directory>")
	}
	return opts.preparedEnvelopeParams(args[0])
}

func (opts bundleRegisterCommandOptions) preparedEnvelopeParams(envelopePath string) (map[string]any, error) {
	contentYAML, err := readBundleRegisterTextFile("registration envelope", envelopePath)
	if err != nil {
		return nil, err
	}
	params := map[string]any{"content_yaml": contentYAML}
	if opts.dataBlobSet {
		dataBlob, err := readBundleRegisterDataBlob(opts.dataBlobPath)
		if err != nil {
			return nil, err
		}
		params["data_blob"] = dataBlob
	}
	if idempotencyKey, err := optionalNonEmptyFlag("--idempotency-key", opts.idempotencyKey, opts.idempotencyKeySet); err != nil {
		return nil, err
	} else if idempotencyKey != "" {
		params["idempotency_key"] = idempotencyKey
	}
	return params, nil
}

func (opts bundleRegisterCommandOptions) contractsDirectoryParams(args []string) (map[string]any, error) {
	if len(args) != 0 {
		return nil, fmt.Errorf("--contracts cannot be combined with a registration envelope argument")
	}
	if opts.dataBlobSet {
		return nil, fmt.Errorf("--data-blob cannot be used with --contracts")
	}
	contractsDir, err := optionalNonEmptyFlag("--contracts", opts.contractsDir, opts.contractsSet)
	if err != nil {
		return nil, err
	}
	repoRoot := strings.TrimSpace(opts.repoRoot)
	if repoRoot == "" {
		repoRoot = discoverRepoRoot()
	}
	if repoRoot == "" {
		cwd, err := os.Getwd()
		if err != nil {
			return nil, fmt.Errorf("resolve repo root: %w", err)
		}
		repoRoot = cwd
	}
	paths, err := resolveCLIContractPlatformSpecPaths(repoRoot, cliContractPlatformSpecPathOptions{
		ContractsPath: contractsDir,
	})
	if err != nil {
		return nil, err
	}
	upload, err := runtimecontracts.BuildBundleRegistrationDirectoryUpload(repoRoot, paths.ContractsPath, paths.PlatformSpecPath)
	if err != nil {
		return nil, fmt.Errorf("package contracts directory: %w", err)
	}
	params := map[string]any{"content_yaml": upload.ContentYAML}
	if upload.DataBlob != nil && len(upload.DataBlob.Entries) > 0 {
		params["data_blob"] = upload.DataBlob
	}
	if idempotencyKey, err := optionalNonEmptyFlag("--idempotency-key", opts.idempotencyKey, opts.idempotencyKeySet); err != nil {
		return nil, err
	} else if idempotencyKey != "" {
		params["idempotency_key"] = idempotencyKey
	}
	return params, nil
}

func (opts bundleDeleteCommandOptions) params(rawBundleHash string) (map[string]any, string, error) {
	bundleHash, err := validateBundleHashArg("bundle hash", rawBundleHash)
	if err != nil {
		return nil, "", err
	}
	params := map[string]any{"bundle_hash": bundleHash}
	if opts.forceSet {
		params["force"] = opts.force
	}
	if opts.dryRunSet {
		params["dry_run"] = opts.dryRun
	}
	if idempotencyKey, err := optionalNonEmptyFlag("--idempotency-key", opts.idempotencyKey, opts.idempotencyKeySet); err != nil {
		return nil, "", err
	} else if idempotencyKey != "" {
		params["idempotency_key"] = idempotencyKey
	}
	return params, bundleHash, nil
}

func bundleAPIErrorClassifier() cliAPIErrorClassifier {
	return cliAPIErrorClassifier{
		notFoundCodes: []string{"BUNDLE_NOT_FOUND"},
		conflictCodes: []string{"BUNDLE_REGISTER_CONFLICT", "IDEMPOTENCY_CONFLICT"},
	}
}

func bundleDeleteAPIErrorClassifier() cliAPIErrorClassifier {
	return cliAPIErrorClassifier{
		notFoundCodes: []string{"BUNDLE_NOT_FOUND"},
		conflictCodes: []string{"BUNDLE_HAS_ACTIVE_RUNS", "BUNDLE_DELETE_IN_PROGRESS", "IDEMPOTENCY_CONFLICT"},
	}
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

func validateBundleRegistrationResult(result bundleRegistrationResult) error {
	if _, err := validateBundleHashArg("bundle_hash", result.BundleHash); err != nil {
		return fmt.Errorf("malformed bundle.register result: %w", err)
	}
	if result.Registered == nil {
		return fmt.Errorf("malformed bundle.register result: registered is required")
	}
	if result.HasData == nil {
		return fmt.Errorf("malformed bundle.register result: has_data is required")
	}
	if result.DataSizeBytes == nil {
		return fmt.Errorf("malformed bundle.register result: data_size_bytes is required")
	}
	if *result.DataSizeBytes < 0 {
		return fmt.Errorf("malformed bundle.register result: data_size_bytes must be non-negative")
	}
	return nil
}

func validateBundleDeleteResult(result bundleDeleteResult, expectedBundleHash string) error {
	if result.OK == nil {
		return fmt.Errorf("malformed bundle.delete result: ok is required")
	}
	switch result.Status {
	case "dry_run", "completed", "partial_failure":
	default:
		return fmt.Errorf("malformed bundle.delete result: status must be dry_run, completed, or partial_failure")
	}
	if result.OperationName != bundleDeleteMethod {
		return fmt.Errorf("malformed bundle.delete result: operation_name must be %s", bundleDeleteMethod)
	}
	if _, err := validateBundleHashArg("bundle_hash", result.BundleHash); err != nil {
		return fmt.Errorf("malformed bundle.delete result: %w", err)
	}
	if result.BundleHash != expectedBundleHash {
		return fmt.Errorf("malformed bundle.delete result: bundle_hash=%q, want %q", result.BundleHash, expectedBundleHash)
	}
	if result.Force == nil {
		return fmt.Errorf("malformed bundle.delete result: force is required")
	}
	if result.Deleted == nil {
		return fmt.Errorf("malformed bundle.delete result: deleted is required")
	}
	if result.DryRun == nil {
		return fmt.Errorf("malformed bundle.delete result: dry_run is required")
	}
	if result.PartialFailure == nil {
		return fmt.Errorf("malformed bundle.delete result: partial_failure is required")
	}
	for name, value := range map[string]*int{
		"active_runs_stopped":  result.ActiveRunsStopped,
		"deliveries_cancelled": result.DeliveriesCancelled,
		"containers_stopped":   result.ContainersStopped,
	} {
		if value == nil {
			return fmt.Errorf("malformed bundle.delete result: %s is required", name)
		}
		if *value < 0 {
			return fmt.Errorf("malformed bundle.delete result: %s must be non-negative", name)
		}
	}
	for name, value := range map[string]map[string]any{
		"plan":           result.Plan,
		"cleanup":        result.Cleanup,
		"containers":     result.Containers,
		"final_mutation": result.FinalMutation,
	} {
		if value == nil {
			return fmt.Errorf("malformed bundle.delete result: %s is required", name)
		}
	}
	for i, item := range result.Errors {
		if strings.TrimSpace(item.Scope) == "" {
			return fmt.Errorf("malformed bundle.delete result: errors[%d].scope is required", i)
		}
		if strings.TrimSpace(item.Message) == "" {
			return fmt.Errorf("malformed bundle.delete result: errors[%d].message is required", i)
		}
	}
	return nil
}

func readBundleRegisterTextFile(label, path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", fmt.Errorf("%s path is required", label)
	}
	info, err := os.Stat(path)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", label, err)
	}
	if info.IsDir() {
		return "", fmt.Errorf("%s must be a file", label)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", label, err)
	}
	if strings.TrimSpace(string(raw)) == "" {
		return "", fmt.Errorf("%s must be non-empty", label)
	}
	return string(raw), nil
}

func readBundleRegisterDataBlob(path string) (map[string]any, error) {
	raw, err := readBundleRegisterTextFile("data blob", path)
	if err != nil {
		return nil, err
	}
	var dataBlob map[string]any
	decoder := json.NewDecoder(strings.NewReader(raw))
	if err := decoder.Decode(&dataBlob); err != nil {
		return nil, fmt.Errorf("--data-blob must contain one BundleRegisterDataBlobV1 JSON object: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("--data-blob must contain one BundleRegisterDataBlobV1 JSON object")
	}
	if dataBlob == nil {
		return nil, fmt.Errorf("--data-blob must contain one BundleRegisterDataBlobV1 JSON object")
	}
	return dataBlob, nil
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

func writeBundleRegistrationHuman(w io.Writer, result bundleRegistrationResult) {
	if w == nil {
		return
	}
	fmt.Fprintf(w, "bundle %s registered=%t has_data=%t data_size_bytes=%d\n",
		result.BundleHash, *result.Registered, *result.HasData, *result.DataSizeBytes)
}

func writeBundleDeleteHuman(w io.Writer, result bundleDeleteResult) {
	if w == nil {
		return
	}
	fmt.Fprintf(w, "bundle %s status=%s deleted=%t force=%t dry_run=%t active_runs_stopped=%d deliveries_cancelled=%d containers_stopped=%d partial_failure=%t\n",
		result.BundleHash, result.Status, *result.Deleted, *result.Force, *result.DryRun, *result.ActiveRunsStopped, *result.DeliveriesCancelled, *result.ContainersStopped, *result.PartialFailure)
	if len(result.Errors) > 0 {
		fmt.Fprintf(w, "errors=%s\n", compactJSONValue(result.Errors))
	}
}

func compactJSONValue(value any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return "{}"
	}
	return string(raw)
}
