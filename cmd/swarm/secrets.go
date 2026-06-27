package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

type secretsListOptions struct {
	contractsPath    string
	platformSpecPath string
	asJSON           bool
	missing          bool
	present          bool
	source           string
}

type secretsCheckOptions struct {
	contractsPath    string
	platformSpecPath string
	asJSON           bool
}

type secretRecord struct {
	Key        string              `json:"key"`
	Source     string              `json:"source"`
	Writable   bool                `json:"writable"`
	Shadowed   bool                `json:"shadowed"`
	Present    bool                `json:"present"`
	UpdatedAt  string              `json:"updated_at"`
	RequiredBy []secretRequirement `json:"required_by"`
}

type secretRequirement struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
}

type secretsListResult struct {
	Secrets []secretRecord `json:"secrets"`
}

type secretsCheckResult struct {
	OK      bool           `json:"ok"`
	Missing []secretRecord `json:"missing"`
}

func newSecretsCommand(ctx context.Context, repo string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "secrets",
		Short: "Manage local Swarm secrets.",
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(
		newSecretsSetCommand(ctx, repo),
		newSecretsListCommand(ctx, repo),
		newSecretsCheckCommand(ctx, repo),
		newSecretsRemoveCommand(ctx, repo),
	)
	return cmd
}

func newSecretsSetCommand(ctx context.Context, repo string) *cobra.Command {
	var stdin bool
	cmd := &cobra.Command{
		Use:   "set <key>",
		Short: "Store a secret in the local file tier.",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 1 {
				return nil
			}
			if len(args) > 1 {
				return fmt.Errorf("secret values must be provided through hidden prompt or stdin, not argv")
			}
			return fmt.Errorf("secret key is required")
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := loadRepoDotEnv(assetCommandRepoRoot(repo)); err != nil {
				return returnSecretsRuntimeError(cmd.ErrOrStderr(), fmt.Errorf("load .env: %w", err))
			}
			value, err := readSecretValue(cmd.InOrStdin(), cmd.ErrOrStderr(), stdin)
			if err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			store, err := buildCredentialStore()
			if err != nil {
				return returnSecretsRuntimeError(cmd.ErrOrStderr(), fmt.Errorf("configure credential store: %w", err))
			}
			key := strings.TrimSpace(args[0])
			if err := store.Set(ctx, key, value); err != nil {
				return returnSecretsStoreError(cmd.ErrOrStderr(), err)
			}
			desc, err := runtimecredentials.Describe(ctx, store, nil, key)
			if err != nil {
				return returnSecretsRuntimeError(cmd.ErrOrStderr(), err)
			}
			record := secretRecordFromDescriptor(desc)
			fmt.Fprintf(cmd.OutOrStdout(), "secret set: key=%s source=%s writable=%s shadowed=%s present=%s\n",
				record.Key, dash(record.Source), yesNo(record.Writable), yesNo(record.Shadowed), yesNo(record.Present))
			return nil
		},
	}
	cmd.Flags().BoolVar(&stdin, "stdin", false, "Read the secret value from stdin")
	return cmd
}

func newSecretsListCommand(ctx context.Context, repo string) *cobra.Command {
	opts := secretsListOptions{}
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List local secret metadata without values.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.missing && opts.present {
				return returnCLIValidationError(cmd.ErrOrStderr(), fmt.Errorf("--missing and --present are mutually exclusive"))
			}
			opts.source = strings.TrimSpace(opts.source)
			if opts.source != "" && opts.source != runtimecredentials.SourceEnv && opts.source != runtimecredentials.SourceFile {
				return returnCLIValidationError(cmd.ErrOrStderr(), fmt.Errorf("--source must be env or file"))
			}
			if err := loadRepoDotEnv(assetCommandRepoRoot(repo)); err != nil {
				return returnSecretsRuntimeError(cmd.ErrOrStderr(), fmt.Errorf("load .env: %w", err))
			}
			store, err := buildCredentialStore()
			if err != nil {
				return returnSecretsRuntimeError(cmd.ErrOrStderr(), fmt.Errorf("configure credential store: %w", err))
			}
			source, err := loadSecretsSource(cmd, repo, opts.contractsPath, opts.platformSpecPath, opts.missing)
			if err != nil {
				return returnSecretsRuntimeError(cmd.ErrOrStderr(), err)
			}
			descriptors, err := runtimecredentials.ListDescriptors(ctx, store, source)
			if err != nil {
				return returnSecretsRuntimeError(cmd.ErrOrStderr(), err)
			}
			records := filterSecretRecords(secretRecordsFromDescriptors(descriptors), opts)
			result := secretsListResult{Secrets: records}
			if opts.asJSON {
				return encodeSecretsJSON(cmd.OutOrStdout(), result)
			}
			writeSecretsTable(cmd.OutOrStdout(), records)
			return nil
		},
	}
	cmd.Flags().StringVar(&opts.contractsPath, "contracts", opts.contractsPath, "Path to Swarm contract bundle root for required_by metadata")
	cmd.Flags().StringVar(&opts.platformSpecPath, "platform-spec", opts.platformSpecPath, "Path to platform spec yaml")
	cmd.Flags().BoolVar(&opts.asJSON, "json", false, "Render successful output as one JSON document")
	cmd.Flags().BoolVar(&opts.missing, "missing", false, "Show required secrets that are missing")
	cmd.Flags().BoolVar(&opts.present, "present", false, "Show present secrets")
	cmd.Flags().StringVar(&opts.source, "source", "", "Filter present secrets by effective source: env or file")
	return cmd
}

func newSecretsCheckCommand(ctx context.Context, repo string) *cobra.Command {
	opts := secretsCheckOptions{}
	cmd := &cobra.Command{
		Use:   "check",
		Short: "Validate required Swarm secrets are configured.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := loadRepoDotEnv(assetCommandRepoRoot(repo)); err != nil {
				return returnSecretsRuntimeError(cmd.ErrOrStderr(), fmt.Errorf("load .env: %w", err))
			}
			store, err := buildCredentialStore()
			if err != nil {
				return returnSecretsRuntimeError(cmd.ErrOrStderr(), fmt.Errorf("configure credential store: %w", err))
			}
			source, err := loadSecretsSource(cmd, repo, opts.contractsPath, opts.platformSpecPath, true)
			if err != nil {
				return returnSecretsRuntimeError(cmd.ErrOrStderr(), err)
			}
			missing, err := runtimecredentials.MissingRequired(ctx, store, source)
			if err != nil {
				return returnSecretsRuntimeError(cmd.ErrOrStderr(), err)
			}
			records := secretRecordsFromDescriptors(missing)
			result := secretsCheckResult{OK: len(records) == 0, Missing: records}
			if opts.asJSON {
				if err := encodeSecretsJSON(cmd.OutOrStdout(), result); err != nil {
					return err
				}
			} else if result.OK {
				fmt.Fprintln(cmd.OutOrStdout(), "all required secrets present")
			} else {
				fmt.Fprintln(cmd.OutOrStdout(), "missing required secrets:")
				writeSecretsTable(cmd.OutOrStdout(), records)
			}
			if !result.OK {
				return commandExitError{code: cliExitRuntime}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&opts.contractsPath, "contracts", opts.contractsPath, "Path to Swarm contract bundle root")
	cmd.Flags().StringVar(&opts.platformSpecPath, "platform-spec", opts.platformSpecPath, "Path to platform spec yaml")
	cmd.Flags().BoolVar(&opts.asJSON, "json", false, "Render successful output as one JSON document")
	return cmd
}

func newSecretsRemoveCommand(ctx context.Context, repo string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "rm <key>",
		Aliases: []string{"remove"},
		Short:   "Remove a secret from the local file tier.",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := loadRepoDotEnv(assetCommandRepoRoot(repo)); err != nil {
				return returnSecretsRuntimeError(cmd.ErrOrStderr(), fmt.Errorf("load .env: %w", err))
			}
			store, err := buildCredentialStore()
			if err != nil {
				return returnSecretsRuntimeError(cmd.ErrOrStderr(), fmt.Errorf("configure credential store: %w", err))
			}
			key := strings.TrimSpace(args[0])
			if err := store.Delete(ctx, key); err != nil {
				return returnSecretsStoreError(cmd.ErrOrStderr(), err)
			}
			desc, err := runtimecredentials.Describe(ctx, store, nil, key)
			if err != nil {
				return returnSecretsRuntimeError(cmd.ErrOrStderr(), err)
			}
			record := secretRecordFromDescriptor(desc)
			fmt.Fprintf(cmd.OutOrStdout(), "secret removed from file tier: key=%s source=%s writable=%s shadowed=%s present=%s\n",
				record.Key, dash(record.Source), yesNo(record.Writable), yesNo(record.Shadowed), yesNo(record.Present))
			return nil
		},
	}
	return cmd
}

func readSecretValue(in io.Reader, errOut io.Writer, forceStdin bool) (string, error) {
	if in == nil {
		in = bytes.NewReader(nil)
	}
	if !forceStdin {
		if file, ok := in.(interface {
			Fd() uintptr
		}); ok && term.IsTerminal(int(file.Fd())) {
			return readSecretValueFromTerminal(int(file.Fd()), errOut)
		}
	}
	raw, err := io.ReadAll(in)
	if err != nil {
		return "", fmt.Errorf("read secret from stdin: %w", err)
	}
	value := trimSecretInputTerminator(string(raw))
	if value == "" {
		return "", fmt.Errorf("secret value is required")
	}
	return value, nil
}

func readSecretValueFromTerminal(fd int, errOut io.Writer) (string, error) {
	if errOut != nil {
		fmt.Fprint(errOut, "Secret value: ")
	}
	first, err := term.ReadPassword(fd)
	if errOut != nil {
		fmt.Fprintln(errOut)
	}
	if err != nil {
		return "", fmt.Errorf("read secret: %w", err)
	}
	if errOut != nil {
		fmt.Fprint(errOut, "Confirm secret value: ")
	}
	second, err := term.ReadPassword(fd)
	if errOut != nil {
		fmt.Fprintln(errOut)
	}
	if err != nil {
		return "", fmt.Errorf("confirm secret: %w", err)
	}
	if !bytes.Equal(first, second) {
		return "", fmt.Errorf("secret values did not match")
	}
	value := string(first)
	if value == "" {
		return "", fmt.Errorf("secret value is required")
	}
	return value, nil
}

func trimSecretInputTerminator(value string) string {
	value = strings.TrimSuffix(value, "\n")
	value = strings.TrimSuffix(value, "\r")
	return value
}

func loadSecretsSource(cmd *cobra.Command, repo, contractsPath, platformSpecPath string, required bool) (semanticview.Source, error) {
	source, err := loadSecretsSourceRequired(repo, contractsPath, platformSpecPath)
	if err == nil {
		return source, nil
	}
	if required || cmd.Flags().Changed("contracts") || cmd.Flags().Changed("platform-spec") {
		return nil, err
	}
	return nil, nil
}

func loadSecretsSourceRequired(repo, contractsPath, platformSpecPath string) (semanticview.Source, error) {
	resolvedPaths, err := resolveCLIContractPlatformSpecPaths(assetCommandRepoRoot(repo), cliContractPlatformSpecPathOptions{
		ContractsPath:    contractsPath,
		PlatformSpecPath: platformSpecPath,
	})
	if err != nil {
		return nil, fmt.Errorf("resolve path config: %w", err)
	}
	contractsRoot, err := normalizeContractsRoot(resolvedPaths.ContractsPath)
	if err != nil {
		return nil, fmt.Errorf("resolve contracts: %w", err)
	}
	_, bundle, err := newSwarmWorkflowModule(assetCommandRepoRoot(repo), contractsRoot, resolvedPaths.PlatformSpecPath)
	if err != nil {
		return nil, fmt.Errorf("load Swarm contracts: %w", err)
	}
	return semanticview.Wrap(bundle), nil
}

func secretRecordsFromDescriptors(descriptors []runtimecredentials.Descriptor) []secretRecord {
	records := make([]secretRecord, 0, len(descriptors))
	for _, desc := range descriptors {
		records = append(records, secretRecordFromDescriptor(desc))
	}
	sort.Slice(records, func(i, j int) bool {
		return records[i].Key < records[j].Key
	})
	return records
}

func secretRecordFromDescriptor(desc runtimecredentials.Descriptor) secretRecord {
	record := secretRecord{
		Key:        desc.Key,
		Source:     desc.Source,
		Writable:   desc.Writable,
		Shadowed:   desc.Shadowed,
		Present:    desc.Present,
		RequiredBy: []secretRequirement{},
	}
	if desc.UpdatedAt != nil && !desc.UpdatedAt.IsZero() {
		record.UpdatedAt = desc.UpdatedAt.UTC().Format(time.RFC3339)
	}
	for _, ref := range desc.RequiredBy {
		record.RequiredBy = append(record.RequiredBy, secretRequirement{Kind: ref.Kind, Name: ref.Name})
	}
	return record
}

func filterSecretRecords(records []secretRecord, opts secretsListOptions) []secretRecord {
	out := make([]secretRecord, 0, len(records))
	for _, record := range records {
		if opts.missing && (record.Present || len(record.RequiredBy) == 0) {
			continue
		}
		if opts.present && !record.Present {
			continue
		}
		if opts.source != "" && record.Source != opts.source {
			continue
		}
		out = append(out, record)
	}
	return out
}

func writeSecretsTable(out io.Writer, records []secretRecord) {
	if out == nil {
		return
	}
	writer := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(writer, "KEY\tSOURCE\tWRITABLE\tSHADOWED\tPRESENT\tUPDATED_AT\tREQUIRED_BY")
	for _, record := range records {
		fmt.Fprintf(writer, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			record.Key,
			dash(record.Source),
			yesNo(record.Writable),
			yesNo(record.Shadowed),
			yesNo(record.Present),
			dash(record.UpdatedAt),
			dash(formatSecretRequirements(record.RequiredBy)),
		)
	}
	_ = writer.Flush()
}

func formatSecretRequirements(items []secretRequirement) string {
	if len(items) == 0 {
		return ""
	}
	parts := make([]string, 0, len(items))
	for _, item := range items {
		item.Kind = strings.TrimSpace(item.Kind)
		item.Name = strings.TrimSpace(item.Name)
		if item.Kind == "" || item.Name == "" {
			continue
		}
		parts = append(parts, item.Kind+":"+item.Name)
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

func encodeSecretsJSON(out io.Writer, value any) error {
	if out == nil {
		return nil
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func yesNo(value bool) string {
	if value {
		return "yes"
	}
	return "no"
}

func dash(value string) string {
	if strings.TrimSpace(value) == "" {
		return "-"
	}
	return value
}

func returnSecretsRuntimeError(errOut io.Writer, err error) error {
	if errOut != nil {
		fmt.Fprintln(errOut, err)
	}
	return commandExitError{code: cliExitRuntime}
}

func returnSecretsStoreError(errOut io.Writer, err error) error {
	if errors.Is(err, runtimecredentials.ErrStoreLocked) {
		if errOut != nil {
			fmt.Fprintln(errOut, err)
		}
		return commandExitError{code: cliExitConflict}
	}
	return returnSecretsRuntimeError(errOut, err)
}
