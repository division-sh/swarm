package cliapp

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/cli/argcount"
	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
)

const (
	dataCheckMethod  = "data.check"
	dataImportMethod = "data.import"
	dataShowMethod   = "data.show"
	dataPruneMethod  = "data.prune"
)

type dataCommandOptions struct {
	apiOptions rootCommandOptions
	output     cliOutputOptions
	bundleHash string
}

type dataImportOptions struct {
	dataCommandOptions
	checkOnly          bool
	sourceInvocationID string
	expectedHead       string
}

type dataShowOptions struct {
	dataCommandOptions
	position uint64
	keyJSON  string
	format   string
}

type dataPruneOptions struct {
	dataCommandOptions
	pruneInvocationID string
	expectedHead      string
}

func newDataCommand(opts rootCommandOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "data",
		Short: "Import, inspect, and prune immutable named data versions.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(newDataImportCommand(opts), newDataShowCommand(opts), newDataPruneCommand(opts))
	return cmd
}

func newDataImportCommand(root rootCommandOptions) *cobra.Command {
	opts := dataImportOptions{dataCommandOptions: dataCommandOptions{apiOptions: root}}
	cmd := &cobra.Command{
		Use:   "import <qualified-or-unambiguous-name> <file.jsonl>",
		Short: "Validate or atomically import one complete JSONL data version.",
		Args:  argcount.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := opts.output.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			return runDataImportCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), opts, args[0], args[1])
		},
	}
	cmd.Flags().BoolVar(&opts.checkOnly, "check", false, "Validate and record the durable outcome without importing")
	cmd.Flags().StringVar(&opts.sourceInvocationID, "source-invocation-id", "", "Required permanent source operation UUID")
	cmd.Flags().StringVar(&opts.expectedHead, "expected-head", "", "Required expected head: absent or an exact ResourceVersionID")
	cmd.Flags().StringVar(&opts.bundleHash, "bundle-hash", "", "Exact selected bundle hash; defaults to the serving runtime's exact bundle")
	bindCLIOutputFlags(cmd, &opts.output)
	bindCLIAPIConnectionFlagsWithClass(cmd, &opts.apiOptions, cliAPICommandClassMutating, "swarm data import")
	return cmd
}

func runDataImportCommand(ctx context.Context, out, errOut io.Writer, opts dataImportOptions, selector, path string) error {
	id, err := canonicalCLIUUID("--source-invocation-id", opts.sourceInvocationID)
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	expected, err := parseCLIExpectedHead(opts.expectedHead)
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	input, err := readBoundedDataFile(opts.apiOptions.invocationRoot.Resolve(path))
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, dataAPIErrorClassifier())
	}
	bundleHash, err := selectedDataBundleIdentity(ctx, client, opts.bundleHash)
	if err != nil {
		return returnCLIAPIError(errOut, err, dataAPIErrorClassifier())
	}
	declaration, err := resolveDataDeclaration(ctx, client, bundleHash, selector)
	if err != nil {
		return returnCLIAPIError(errOut, err, dataAPIErrorClassifier())
	}
	method := dataImportMethod
	if opts.checkOnly {
		method = dataCheckMethod
	}
	params := map[string]any{
		"source_invocation_id": id,
		"bundle_hash":          bundleHash,
		"declaration":          declaration.Declaration,
		"expected_head":        expected,
		"input": map[string]any{
			"format":         "jsonl",
			"content_base64": base64.StdEncoding.EncodeToString(input),
		},
	}
	var result durabledata.SourceOperationResult
	if err := client.call(ctx, method, params, &result); err != nil {
		return returnCLIAPIError(errOut, err, dataAPIErrorClassifier())
	}
	return renderCLIOutput(out, errOut, opts.output, result, func(w io.Writer) {
		fmt.Fprintf(w, "%s %s\n", result.Operation, result.Outcome)
		fmt.Fprintf(w, "data=%s version=%s head=%s\n", dataDeclarationLabel(declaration), result.Candidate.VersionID, result.Head.After.VersionID)
		if result.Delta.State == "computed" && result.Delta.Summary != nil {
			fmt.Fprintf(w, "delta +%d -%d ~%d\n", result.Delta.Summary.Added, result.Delta.Summary.Removed, result.Delta.Summary.Changed)
		}
		if result.Defects.ItemCount > 0 {
			fmt.Fprintf(w, "defects=%d\n", result.Defects.ItemCount)
		}
	}, func() ([]string, error) {
		return []string{string(result.Candidate.VersionID)}, nil
	})
}

func newDataShowCommand(root rootCommandOptions) *cobra.Command {
	opts := dataShowOptions{dataCommandOptions: dataCommandOptions{apiOptions: root}}
	cmd := &cobra.Command{
		Use:   "show [data-selector]",
		Short: "List data declarations or read one exact immutable version.",
		Args:  argcount.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := opts.output.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			return runDataShowCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), opts, args)
		},
	}
	cmd.Flags().StringVar(&opts.bundleHash, "bundle-hash", "", "Exact selected bundle hash; defaults to the serving runtime's exact bundle")
	cmd.Flags().Uint64Var(&opts.position, "position", 0, "Read one exact 1-based row position from a keyless version")
	cmd.Flags().StringVar(&opts.keyJSON, "key", "", "Read one exact keyed row using a canonical JSON scalar")
	cmd.Flags().StringVar(&opts.format, "format", "summary", "Output format: summary or jsonl")
	bindCLIOutputFlags(cmd, &opts.output)
	bindCLIAPIConnectionFlags(cmd, &opts.apiOptions)
	return cmd
}

func runDataShowCommand(ctx context.Context, out, errOut io.Writer, opts dataShowOptions, args []string) error {
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, dataAPIErrorClassifier())
	}
	bundleHash, err := selectedDataBundleIdentity(ctx, client, opts.bundleHash)
	if err != nil {
		return returnCLIAPIError(errOut, err, dataAPIErrorClassifier())
	}
	if len(args) == 0 {
		if opts.position != 0 || strings.TrimSpace(opts.keyJSON) != "" || opts.format != "summary" {
			return returnCLIValidationError(errOut, fmt.Errorf("--position, --key, and --format require a data selector"))
		}
		declarations, err := listDataDeclarations(ctx, client, bundleHash)
		if err != nil {
			return returnCLIAPIError(errOut, err, dataAPIErrorClassifier())
		}
		return renderCLIOutput(out, errOut, opts.output, declarations, func(w io.Writer) {
			for _, declaration := range declarations {
				fmt.Fprintf(w, "%s head=%s versions=%d bytes=%d\n", dataDeclarationLabel(declaration), declaration.Head.VersionID, declaration.VersionCount, declaration.MaterializedBytes)
			}
		}, func() ([]string, error) {
			rows := make([]string, len(declarations))
			for i := range declarations {
				rows[i] = dataDeclarationLabel(declarations[i])
			}
			return rows, nil
		})
	}
	name, versionSelector, err := splitDataVersionSelector(args[0], true)
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	declaration, err := resolveDataDeclaration(ctx, client, bundleHash, name)
	if err != nil {
		return returnCLIAPIError(errOut, err, dataAPIErrorClassifier())
	}
	selector, err := dataVersionSelector(versionSelector, true)
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	if opts.format == "jsonl" {
		if opts.position != 0 || strings.TrimSpace(opts.keyJSON) != "" || opts.output.asJSON || opts.output.asYAML || opts.output.quiet {
			return returnCLIValidationError(errOut, fmt.Errorf("--format jsonl cannot be combined with --position, --key, --json, --yaml, or --quiet"))
		}
		return streamDataJSONL(ctx, client, out, declaration.Declaration, selector)
	}
	if opts.format != "summary" {
		return returnCLIValidationError(errOut, fmt.Errorf("--format must be summary or jsonl"))
	}
	if opts.position != 0 && strings.TrimSpace(opts.keyJSON) != "" {
		return returnCLIValidationError(errOut, fmt.Errorf("--position and --key are mutually exclusive"))
	}
	if opts.position != 0 || strings.TrimSpace(opts.keyJSON) != "" {
		rowSelector := map[string]any{"position": opts.position}
		if raw := strings.TrimSpace(opts.keyJSON); raw != "" {
			value, err := canonicaljson.Decode([]byte(raw))
			if err != nil {
				return returnCLIValidationError(errOut, fmt.Errorf("--key must be one canonical JSON scalar: %w", err))
			}
			key, err := durabledata.BusinessKeyFromValue(value)
			if err != nil {
				return returnCLIValidationError(errOut, fmt.Errorf("--key: %w", err))
			}
			projected, err := key.Value()
			if err != nil {
				return returnCLIValidationError(errOut, fmt.Errorf("--key: %w", err))
			}
			rowSelector = map[string]any{"key": projected}
		}
		var row durabledata.RowDTO
		err := client.call(ctx, dataShowMethod, map[string]any{"view": "row", "declaration": declaration.Declaration, "selector": selector, "row_selector": rowSelector}, &row)
		if err != nil {
			return returnCLIAPIError(errOut, err, dataAPIErrorClassifier())
		}
		return renderCLIOutput(out, errOut, opts.output, row, func(w io.Writer) {
			if row.Key != nil {
				fmt.Fprintf(w, "%s@%s key=%s ordinal=%d value=%v\n", dataDeclarationLabel(declaration), row.VersionID, *row.Key, row.Ordinal, row.Value)
				return
			}
			fmt.Fprintf(w, "%s@%s position=%d value=%v\n", dataDeclarationLabel(declaration), row.VersionID, row.Ordinal, row.Value)
		}, func() ([]string, error) {
			if row.Key != nil {
				return []string{string(*row.Key)}, nil
			}
			return []string{fmt.Sprint(row.Ordinal)}, nil
		})
	}
	var version durabledata.VersionSummary
	if err := client.call(ctx, dataShowMethod, map[string]any{"view": "version", "declaration": declaration.Declaration, "selector": selector}, &version); err != nil {
		return returnCLIAPIError(errOut, err, dataAPIErrorClassifier())
	}
	return renderCLIOutput(out, errOut, opts.output, version, func(w io.Writer) {
		fmt.Fprintf(w, "%s@%s alias=%s rows=%d payload=%s bytes=%d\n", dataDeclarationLabel(declaration), version.VersionID, version.Alias, version.Manifest.RowCount, version.PayloadState, version.MaterializedBytes)
	}, func() ([]string, error) { return []string{string(version.VersionID)}, nil })
}

func newDataPruneCommand(root rootCommandOptions) *cobra.Command {
	opts := dataPruneOptions{dataCommandOptions: dataCommandOptions{apiOptions: root}}
	cmd := &cobra.Command{
		Use:   "prune <data-selector>",
		Short: "Prune one exact unpinned non-current data payload.",
		Args:  argcount.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := opts.output.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			return runDataPruneCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), opts, args[0])
		},
	}
	cmd.Flags().StringVar(&opts.bundleHash, "bundle-hash", "", "Exact selected bundle hash; defaults to the serving runtime's exact bundle")
	cmd.Flags().StringVar(&opts.pruneInvocationID, "prune-invocation-id", "", "Required permanent prune operation UUID")
	cmd.Flags().StringVar(&opts.expectedHead, "expected-head", "", "Required expected head: absent or an exact ResourceVersionID")
	bindCLIOutputFlags(cmd, &opts.output)
	bindCLIAPIConnectionFlagsWithClass(cmd, &opts.apiOptions, cliAPICommandClassMutating, "swarm data prune")
	return cmd
}

func runDataPruneCommand(ctx context.Context, out, errOut io.Writer, opts dataPruneOptions, rawSelector string) error {
	id, err := canonicalCLIUUID("--prune-invocation-id", opts.pruneInvocationID)
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	expected, err := parseCLIExpectedHead(opts.expectedHead)
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	name, rawVersion, err := splitDataVersionSelector(rawSelector, false)
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, dataAPIErrorClassifier())
	}
	bundleHash, err := selectedDataBundleIdentity(ctx, client, opts.bundleHash)
	if err != nil {
		return returnCLIAPIError(errOut, err, dataAPIErrorClassifier())
	}
	declaration, err := resolveDataDeclaration(ctx, client, bundleHash, name)
	if err != nil {
		return returnCLIAPIError(errOut, err, dataAPIErrorClassifier())
	}
	version, err := resolveDataVersion(ctx, client, declaration.Declaration, rawVersion, true)
	if err != nil {
		return returnCLIAPIError(errOut, err, dataAPIErrorClassifier())
	}
	params := map[string]any{"prune_invocation_id": id, "declaration": declaration.Declaration, "version_id": version.VersionID, "expected_head": expected}
	var result durabledata.PruneOperationResult
	if err := client.call(ctx, dataPruneMethod, params, &result); err != nil {
		return returnCLIAPIError(errOut, err, dataAPIErrorClassifier())
	}
	return renderCLIOutput(out, errOut, opts.output, result, func(w io.Writer) {
		fmt.Fprintf(w, "%s %s@%s payload=%s->%s pins=%d\n", result.Outcome, dataDeclarationLabel(declaration), result.VersionID, result.PayloadBefore, result.PayloadAfter, result.PinCount)
	}, func() ([]string, error) { return []string{result.Outcome}, nil })
}

func selectedDataBundleIdentity(ctx context.Context, client *cliAPIClient, raw string) (string, error) {
	if value := strings.TrimSpace(raw); value != "" {
		if !cliBundleHashPattern.MatchString(value) {
			return "", fmt.Errorf("--bundle-hash must be bundle-v1:sha256:<64 lowercase hex>")
		}
		return value, nil
	}
	health, err := runCommandHealth(ctx, client)
	if err != nil {
		return "", err
	}
	value := strings.TrimSpace(health.Bundle.BundleHash)
	if !cliBundleHashPattern.MatchString(value) {
		return "", fmt.Errorf("health.check did not return one exact selected bundle hash")
	}
	return value, nil
}

func listDataDeclarations(ctx context.Context, client *cliAPIClient, bundleHash string) ([]durabledata.DeclarationSummary, error) {
	page := map[string]any{"limit": durabledata.MaxDataDeclarationsPerBundle, "byte_limit": durabledata.MaxPublicPageBytes}
	var all []durabledata.DeclarationSummary
	for {
		var result durabledata.PageResult[durabledata.DeclarationSummary]
		if err := client.call(ctx, dataShowMethod, map[string]any{"view": "declarations", "bundle_hash": bundleHash, "page": page}, &result); err != nil {
			return nil, err
		}
		if result.ItemCount != len(result.Items) || (result.Continuation.State != "end" && result.Continuation.State != "more") {
			return nil, fmt.Errorf("malformed data.show declarations page")
		}
		all = append(all, result.Items...)
		if result.Continuation.State == "end" {
			break
		}
		if result.Continuation.Cursor == "" {
			return nil, fmt.Errorf("malformed data.show declarations continuation")
		}
		page["cursor"] = result.Continuation.Cursor
	}
	if len(all) > durabledata.MaxDataDeclarationsPerBundle {
		return nil, fmt.Errorf("malformed data.show result exceeds declaration limit")
	}
	sort.Slice(all, func(i, j int) bool {
		return durabledata.CompareDeclarationRef(all[i].Declaration, all[j].Declaration) < 0
	})
	return all, nil
}

func resolveDataDeclaration(ctx context.Context, client *cliAPIClient, bundleHash, selector string) (durabledata.DeclarationSummary, error) {
	declarations, err := listDataDeclarations(ctx, client, bundleHash)
	if err != nil {
		return durabledata.DeclarationSummary{}, err
	}
	return resolveDataDeclarationFromList(bundleHash, selector, declarations)
}

func resolveDataDeclarationFromList(bundleHash, selector string, declarations []durabledata.DeclarationSummary) (durabledata.DeclarationSummary, error) {
	selector = strings.TrimSpace(selector)
	if selector == "" {
		return durabledata.DeclarationSummary{}, fmt.Errorf("data name must be non-empty")
	}
	var matches []durabledata.DeclarationSummary
	for _, declaration := range declarations {
		if selector == declaration.LocalName || selector == dataDeclarationLabel(declaration) {
			matches = append(matches, declaration)
		}
	}
	if len(matches) == 1 {
		return matches[0], nil
	}
	var candidates []string
	for _, declaration := range declarations {
		if declaration.LocalName == selector || strings.HasSuffix(selector, "/"+declaration.LocalName) || len(matches) > 1 {
			candidates = append(candidates, fmt.Sprintf("%s (%s)", dataDeclarationLabel(declaration), declaration.Declaration.EventName))
		}
	}
	sort.Strings(candidates)
	if len(matches) > 1 {
		return durabledata.DeclarationSummary{}, fmt.Errorf("data name %q is ambiguous; choose one exact candidate: %s", selector, strings.Join(candidates, ", "))
	}
	if len(candidates) == 0 {
		for _, declaration := range declarations {
			candidates = append(candidates, fmt.Sprintf("%s (%s)", dataDeclarationLabel(declaration), declaration.Declaration.EventName))
		}
	}
	return durabledata.DeclarationSummary{}, fmt.Errorf("data name %q is not declared in bundle %s; candidates: %s", selector, bundleHash, strings.Join(candidates, ", "))
}

func buildRunDataEnvelope(ctx context.Context, root InvocationRoot, client *cliAPIClient, bundleHash, runID string, imports, pins []string) (map[string]any, error) {
	if len(imports) == 0 && len(pins) == 0 {
		return nil, nil
	}
	declarations, err := listDataDeclarations(ctx, client, bundleHash)
	if err != nil {
		return nil, err
	}
	selected := make(map[string]string, len(imports)+len(pins))
	importItems := make([]any, 0, len(imports))
	for _, raw := range imports {
		name, path, ok := strings.Cut(raw, "=")
		if !ok || strings.TrimSpace(name) == "" || strings.TrimSpace(path) == "" {
			return nil, fmt.Errorf("--data must be name=file.jsonl")
		}
		declaration, err := resolveDataDeclarationFromList(bundleHash, name, declarations)
		if err != nil {
			return nil, err
		}
		key := declaration.Declaration.Key()
		if prior := selected[key]; prior != "" {
			return nil, fmt.Errorf("data declaration %s is selected by both %s and --data", dataDeclarationLabel(declaration), prior)
		}
		input, err := readBoundedDataFile(root.Resolve(path))
		if err != nil {
			return nil, err
		}
		selected[key] = "--data"
		childID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm.cli.fused-data.v1\x00"+runID+"\x00"+key)).String()
		importItems = append(importItems, map[string]any{
			"source_invocation_id": childID,
			"declaration":          declaration.Declaration,
			"expected_head":        declaration.Head,
			"input":                map[string]any{"format": "jsonl", "content_base64": base64.StdEncoding.EncodeToString(input)},
		})
	}
	pinItems := make([]any, 0, len(pins))
	for _, raw := range pins {
		name, versionSelector, err := splitDataVersionSelector(raw, false)
		if err != nil {
			return nil, fmt.Errorf("--pin: %w", err)
		}
		declaration, err := resolveDataDeclarationFromList(bundleHash, name, declarations)
		if err != nil {
			return nil, err
		}
		key := declaration.Declaration.Key()
		if prior := selected[key]; prior != "" {
			return nil, fmt.Errorf("data declaration %s is selected by both %s and --pin", dataDeclarationLabel(declaration), prior)
		}
		version, err := resolveDataVersion(ctx, client, declaration.Declaration, versionSelector, true)
		if err != nil {
			return nil, err
		}
		selected[key] = "--pin"
		pinItems = append(pinItems, map[string]any{"declaration": declaration.Declaration, "version_id": version.VersionID})
	}
	return map[string]any{"imports": importItems, "pins": pinItems}, nil
}

func dataDeclarationLabel(declaration durabledata.DeclarationSummary) string {
	if declaration.Declaration.PackageKey == "." {
		return "./" + declaration.LocalName
	}
	return strings.TrimSuffix(declaration.Declaration.PackageKey, "/") + "/" + declaration.LocalName
}

func parseCLIExpectedHead(raw string) (durabledata.ExpectedHead, error) {
	raw = strings.TrimSpace(raw)
	if raw == "absent" {
		return durabledata.AbsentHead(), nil
	}
	id := durabledata.VersionID(raw)
	if err := id.Validate(); err != nil {
		return durabledata.ExpectedHead{}, fmt.Errorf("--expected-head must be absent or an exact ResourceVersionID")
	}
	return durabledata.VersionHead(id), nil
}

func canonicalCLIUUID(flag, raw string) (string, error) {
	value := strings.TrimSpace(raw)
	parsed, err := uuid.Parse(value)
	if err != nil || parsed == uuid.Nil || parsed.String() != value {
		return "", fmt.Errorf("%s must be one canonical non-zero lowercase UUID", flag)
	}
	return value, nil
}

func readBoundedDataFile(path string) ([]byte, error) {
	file, err := os.Open(strings.TrimSpace(path))
	if err != nil {
		return nil, fmt.Errorf("open data file: %w", err)
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, durabledata.MaxDecodedImportBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read data file: %w", err)
	}
	if len(raw) > durabledata.MaxDecodedImportBytes {
		return nil, fmt.Errorf("data file exceeds the %d-byte decoded import limit", durabledata.MaxDecodedImportBytes)
	}
	return raw, nil
}

func splitDataVersionSelector(raw string, defaultHead bool) (string, string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", fmt.Errorf("data selector must be non-empty")
	}
	index := strings.LastIndex(raw, "@")
	if index < 0 {
		if defaultHead {
			return raw, "head", nil
		}
		return "", "", fmt.Errorf("data selector must include @head, @vN, or @ResourceVersionID")
	}
	if index == 0 || index == len(raw)-1 {
		return "", "", fmt.Errorf("data selector must have non-empty name and version")
	}
	return raw[:index], raw[index+1:], nil
}

func dataVersionSelector(raw string, allowHead bool) (map[string]any, error) {
	if raw == "head" {
		if !allowHead {
			return nil, fmt.Errorf("head is not allowed for this exact version selector")
		}
		return map[string]any{"kind": "head"}, nil
	}
	if strings.HasPrefix(raw, "v") {
		if _, err := durabledata.ParseVersionAlias(raw); err != nil {
			return nil, err
		}
		return map[string]any{"kind": "alias", "alias": raw}, nil
	}
	id := durabledata.VersionID(raw)
	if err := id.Validate(); err != nil {
		return nil, fmt.Errorf("version selector must be head, vN, or an exact ResourceVersionID")
	}
	return map[string]any{"kind": "version", "version_id": id}, nil
}

func resolveDataVersion(ctx context.Context, client *cliAPIClient, declaration durabledata.DeclarationRef, raw string, allowHead bool) (durabledata.VersionSummary, error) {
	selector, err := dataVersionSelector(raw, allowHead)
	if err != nil {
		return durabledata.VersionSummary{}, err
	}
	var result durabledata.VersionSummary
	if err := client.call(ctx, dataShowMethod, map[string]any{"view": "version", "declaration": declaration, "selector": selector}, &result); err != nil {
		return durabledata.VersionSummary{}, err
	}
	if result.Declaration != declaration || result.VersionID.Validate() != nil {
		return durabledata.VersionSummary{}, fmt.Errorf("malformed data.show version result")
	}
	return result, nil
}

func streamDataJSONL(ctx context.Context, client *cliAPIClient, out io.Writer, declaration durabledata.DeclarationRef, selector map[string]any) error {
	page := map[string]any{"limit": durabledata.MaxPublicPageItems, "byte_limit": durabledata.MaxPublicPageBytes}
	var expectedVersion durabledata.VersionID
	var expectedContent durabledata.ContentDigest
	expectedOrdinal := uint64(1)
	totalRows := 0
	contentHash := sha256.New()
	for {
		var chunk durabledata.ExportChunk
		if err := client.call(ctx, dataShowMethod, map[string]any{"view": "export_chunk", "declaration": declaration, "selector": selector, "page": page}, &chunk); err != nil {
			return err
		}
		raw, err := base64.StdEncoding.DecodeString(chunk.ChunkBase64)
		if err != nil || base64.StdEncoding.EncodeToString(raw) != chunk.ChunkBase64 || len(raw) != chunk.ChunkBytes {
			return fmt.Errorf("malformed data.show export chunk")
		}
		hash := sha256.Sum256(raw)
		emptyExport := expectedVersion == "" && chunk.TotalRows == 0 && chunk.RowCount == 0 && len(raw) == 0 &&
			chunk.FirstOrdinal == 0 && chunk.Continuation.State == "end" && chunk.Continuation.Cursor == ""
		validShape := emptyExport || (chunk.TotalRows > 0 && chunk.RowCount > 0 && chunk.FirstOrdinal == expectedOrdinal)
		if chunk.ChunkSHA256 != "sha256:"+hex.EncodeToString(hash[:]) || chunk.RowCount < 0 ||
			!validShape {
			return fmt.Errorf("contradictory data.show export chunk")
		}
		if expectedVersion == "" {
			expectedVersion, expectedContent = chunk.VersionID, chunk.ContentDigest
		} else if chunk.VersionID != expectedVersion || chunk.ContentDigest != expectedContent {
			return fmt.Errorf("data.show export changed version between pages")
		}
		if _, err := out.Write(raw); err != nil {
			return err
		}
		_, _ = contentHash.Write(raw)
		totalRows += chunk.RowCount
		expectedOrdinal += uint64(chunk.RowCount)
		if chunk.Continuation.State == "end" {
			if totalRows != chunk.TotalRows {
				return fmt.Errorf("data.show export row count contradiction")
			}
			return nil
		}
		if chunk.Continuation.State != "more" || chunk.Continuation.Cursor == "" {
			return fmt.Errorf("malformed data.show export continuation")
		}
		page["cursor"] = chunk.Continuation.Cursor
	}
}

func dataAPIErrorClassifier() cliAPIErrorClassifier {
	return cliAPIErrorClassifier{
		notFoundCodes: []string{"DATA_CONTRACT_NOT_FOUND", "DATA_DECLARATION_NOT_FOUND", "DATA_VERSION_NOT_FOUND", "DATA_OPERATION_NOT_FOUND"},
		conflictCodes: []string{"DATA_INVOCATION_CONFLICT", "DATA_HEAD_CONFLICT", "DATA_PIN_CONFLICT", "DATA_PAYLOAD_PRUNED"},
	}
}
