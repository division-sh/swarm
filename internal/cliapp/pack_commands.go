package cliapp

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/division-sh/swarm/internal/cli/argcount"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packartifact"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/spf13/cobra"
)

type packCommandOptions struct {
	repoRoot         string
	root             rootCommandOptions
	contractsPath    string
	platformSpecPath string
	output           cliOutputOptions
}

type packInventoryEntryReadback struct {
	ID           string                    `json:"id" yaml:"id"`
	Type         string                    `json:"type" yaml:"type"`
	Version      string                    `json:"version" yaml:"version"`
	ManifestHash string                    `json:"manifest_hash" yaml:"manifest_hash"`
	Source       string                    `json:"source" yaml:"source"`
	Directory    string                    `json:"directory" yaml:"directory"`
	Origin       packartifact.ImportOrigin `json:"origin,omitempty" yaml:"origin,omitempty"`
	Modified     bool                      `json:"modified" yaml:"modified"`
	ShadowsBase  bool                      `json:"shadows_base" yaml:"shadows_base"`
}

type packInventoryReadback struct {
	BaseMode        packartifact.SelectionMode   `json:"base_mode" yaml:"base_mode"`
	BaseDigest      string                       `json:"base_digest" yaml:"base_digest"`
	BaseDirectories []string                     `json:"base_directories,omitempty" yaml:"base_directories,omitempty"`
	EffectiveDigest string                       `json:"effective_digest" yaml:"effective_digest"`
	Packs           []packInventoryEntryReadback `json:"packs" yaml:"packs"`
}

type packImportReadback struct {
	ID      string `json:"id" yaml:"id"`
	Changed bool   `json:"changed" yaml:"changed"`
	Source  string `json:"source" yaml:"source"`
}

type packShowReadback struct {
	Pack         packInventoryEntryReadback `json:"pack" yaml:"pack"`
	EnvelopeYAML string                     `json:"envelope_yaml" yaml:"envelope_yaml"`
	ManifestYAML string                     `json:"manifest_yaml" yaml:"manifest_yaml"`
}

func newPacksCommand(ctx context.Context, repo string, root rootCommandOptions) *cobra.Command {
	cmd := &cobra.Command{Use: "packs", Short: "Inspect the selected platform and project pack inventory.", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error { return cmd.Help() }}
	cmd.AddCommand(newPacksListCommand(ctx, repo, root), newPacksShowCommand(ctx, repo, root))
	return cmd
}

func newPacksListCommand(_ context.Context, repo string, root rootCommandOptions) *cobra.Command {
	opts := packCommandOptions{repoRoot: repo, root: root}
	cmd := &cobra.Command{
		Use: "list", Short: "List the exact effective pack inventory.", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := opts.output.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			inventory, err := loadEffectivePackInventory(opts)
			if err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			readback := packInventoryReadbackFromInventory(inventory)
			return renderCLIOutput(cmd.OutOrStdout(), cmd.ErrOrStderr(), opts.output, readback, func(out io.Writer) {
				writePackInventory(out, readback)
			}, func() ([]string, error) {
				ids := make([]string, 0, len(readback.Packs))
				for _, pack := range readback.Packs {
					ids = append(ids, pack.ID)
				}
				return ids, nil
			})
		},
	}
	bindPackSourceFlags(cmd, &opts)
	bindCLIOutputFlags(cmd, &opts.output)
	return cmd
}

func newPacksShowCommand(_ context.Context, repo string, root rootCommandOptions) *cobra.Command {
	opts := packCommandOptions{repoRoot: repo, root: root}
	cmd := &cobra.Command{
		Use: "show <pack-id>", Short: "Show one selected pack and its provenance.", Args: argcount.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := opts.output.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			inventory, err := loadEffectivePackInventory(opts)
			if err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			id := strings.TrimSpace(args[0])
			entry, ok := inventory.Lookup(id)
			if ok {
				pack := packInventoryEntryReadbackFromEntry(entry)
				show := packShowReadback{Pack: pack, EnvelopeYAML: string(entry.EnvelopeBody()), ManifestYAML: string(entry.ManifestBody())}
				return renderCLIOutput(cmd.OutOrStdout(), cmd.ErrOrStderr(), opts.output, show, func(out io.Writer) {
					writePackEntry(out, pack)
					fmt.Fprintln(out, "pack.yaml:")
					fmt.Fprint(out, show.EnvelopeYAML)
					if !strings.HasSuffix(show.EnvelopeYAML, "\n") {
						fmt.Fprintln(out)
					}
					fmt.Fprintf(out, "%s:\n", packartifact.ManifestFileNameForType(entry.Type()))
					fmt.Fprint(out, show.ManifestYAML)
					if !strings.HasSuffix(show.ManifestYAML, "\n") {
						fmt.Fprintln(out)
					}
				}, func() ([]string, error) { return []string{pack.ID}, nil })
			}
			return returnCLIValidationError(cmd.ErrOrStderr(), fmt.Errorf("pack %q is not selected; list selected packs with `swarm packs list`", id))
		},
	}
	bindPackSourceFlags(cmd, &opts)
	bindCLIOutputFlags(cmd, &opts.output)
	return cmd
}

func newImportPackCommand(repo string, root rootCommandOptions) *cobra.Command {
	opts := packCommandOptions{repoRoot: repo, root: root}
	cmd := &cobra.Command{
		Use: "import <pack-id>", Short: "Import an embedded pack into the selected project.", Args: argcount.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := opts.output.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			paths, err := ResolveCLIContractPlatformSpecPaths(opts.repoRoot, CLIContractPlatformSpecPathOptions{ContractsPath: opts.contractsPath, PlatformSpecPath: opts.platformSpecPath, ConfigPath: rootConfigPath(opts.root)})
			if err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			contractsRoot, err := NormalizeContractsRoot(paths.ContractsPath)
			if err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			platformSpec, err := loadChannelPlatformSpecDocument(paths.PlatformSpecPath)
			if err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			embedded, err := packartifact.LoadEmbeddedPlatformPackInventory(strings.TrimSpace(platformSpec.Platform.Version))
			if err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			id := strings.TrimSpace(args[0])
			changed, err := packartifact.ImportEmbeddedPack(contractsRoot, id, embedded)
			if err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			result := packImportReadback{ID: id, Changed: changed, Source: packartifact.ProvenanceProject}
			return renderCLIOutput(cmd.OutOrStdout(), cmd.ErrOrStderr(), opts.output, result, func(out io.Writer) {
				state := "already imported"
				if changed {
					state = "imported"
				}
				fmt.Fprintf(out, "pack %s: %s source=project\n", id, state)
			}, func() ([]string, error) { return []string{id}, nil })
		},
	}
	bindPackSourceFlags(cmd, &opts)
	bindCLIOutputFlags(cmd, &opts.output)
	return cmd
}

func bindPackSourceFlags(cmd *cobra.Command, opts *packCommandOptions) {
	cmd.Flags().StringVar(&opts.contractsPath, "contracts", "", "Path to the project contracts root")
	cmd.Flags().StringVar(&opts.platformSpecPath, "platform-spec", "", retiredPlatformSpecFlagHelp)
}

func rootConfigPath(root rootCommandOptions) string {
	if root.rootFlags != nil && root.rootFlags.configPathSet {
		return root.rootFlags.configPath
	}
	return ""
}

func loadEffectivePackInventory(opts packCommandOptions) (*packartifact.EffectivePackInventory, error) {
	cfgResult, err := loadPackInventoryConfig(opts.repoRoot, rootConfigPath(opts.root))
	if err != nil {
		return nil, err
	}
	paths, err := resolvePackInventoryPaths(opts, cfgResult)
	if err != nil {
		return nil, err
	}
	base, err := LoadConfiguredPlatformPackBase(opts.repoRoot, cfgResult)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(paths.ContractsPath) == "" {
		inventory, err := packartifact.NewEffectivePackInventory(base, nil)
		if err != nil {
			return nil, err
		}
		platformSpec, err := loadChannelPlatformSpecDocument(paths.PlatformSpecPath)
		if err != nil {
			return nil, err
		}
		if _, err := packadmission.Admit(inventory, platformSpec); err != nil {
			return nil, err
		}
		return inventory, nil
	}
	contractsRoot, err := NormalizeContractsRoot(paths.ContractsPath)
	if err != nil {
		return nil, err
	}
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOptions(opts.repoRoot, contractsRoot, paths.PlatformSpecPath, runtimecontracts.WorkflowContractLoadOptions{
		PlatformPackBase: base, AdmitPackInventory: packadmission.AdmitInventory,
	})
	if err != nil {
		return nil, err
	}
	return bundle.PackInventory, nil
}

func resolvePackInventoryPaths(opts packCommandOptions, cfgResult RuntimeConfigLoadResult) (CLIContractPlatformSpecPaths, error) {
	return resolveCLIContractPlatformSpecPathsFromConfig(opts.repoRoot, CLIContractPlatformSpecPathOptions{
		ContractsPath: opts.contractsPath, PlatformSpecPath: opts.platformSpecPath,
	}, cfgResult.cli)
}

func loadPackInventoryConfig(repoRoot, explicitPath string) (RuntimeConfigLoadResult, error) {
	result, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: repoRoot, ExplicitPath: explicitPath})
	if err == nil {
		return result, nil
	}
	if result.Config == nil {
		return result, err
	}
	if _, authored := result.KeyOrigins["runtime.execution_posture"]; authored {
		return result, err
	}
	blockers := unifiedConfigBlockers(result.Diagnostics)
	_, postureErr := result.Config.ProcessExecutionPosture()
	if len(blockers) != 1 || postureErr == nil || blockers[0].Kind != unifiedConfigDiagnosticValidationFailed || blockers[0].Message != postureErr.Error() {
		return result, err
	}
	configCopy := *result.Config
	configCopy.Runtime.ExecutionPosture = executionposture.Live
	if validationErr := configCopy.Validate(); validationErr != nil {
		return result, validationErr
	}
	result.Config = &configCopy
	return result, nil
}

func packInventoryReadbackFromInventory(inventory *packartifact.EffectivePackInventory) packInventoryReadback {
	result := packInventoryReadback{}
	if inventory == nil {
		return result
	}
	result.BaseMode = inventory.BaseSelectionMode()
	result.BaseDigest = inventory.BaseDigest()
	result.BaseDirectories = inventory.BaseDirectories()
	result.EffectiveDigest = inventory.Digest()
	for _, entry := range inventory.Entries() {
		result.Packs = append(result.Packs, packInventoryEntryReadbackFromEntry(entry))
	}
	return result
}

func packInventoryEntryReadbackFromEntry(entry packartifact.Entry) packInventoryEntryReadback {
	return packInventoryEntryReadback{
		ID: entry.ID(), Type: entry.Type(), Version: entry.Version(), ManifestHash: entry.ManifestHash(),
		Source: entry.Source(), Directory: entry.Directory(), Origin: entry.Origin(), Modified: entry.Modified(), ShadowsBase: entry.ShadowsBase(),
	}
}

func writePackInventory(out io.Writer, inventory packInventoryReadback) {
	fmt.Fprintf(out, "pack inventory: base=%s base_digest=%s effective_digest=%s\n", inventory.BaseMode, inventory.BaseDigest, inventory.EffectiveDigest)
	if inventory.BaseMode == packartifact.SelectionDevelopmentOverride {
		fmt.Fprintf(out, "development override active: %d packs from %s (replacing the embedded inventory)\n", len(inventory.Packs), strings.Join(inventory.BaseDirectories, ", "))
	}
	for _, pack := range inventory.Packs {
		writePackEntry(out, pack)
	}
}

func writePackEntry(out io.Writer, pack packInventoryEntryReadback) {
	fmt.Fprintf(out, "%s type=%s version=%s source=%s manifest=%s", pack.ID, pack.Type, pack.Version, pack.Source, pack.ManifestHash)
	if pack.ShadowsBase {
		fmt.Fprint(out, " shadows_base=true")
	}
	if pack.Modified {
		fmt.Fprint(out, " modified=true")
	}
	if pack.Origin.Valid() {
		fmt.Fprintf(out, " origin=%s@%s:%s", pack.Origin.ID, pack.Origin.Version, pack.Origin.ManifestHash)
	}
	fmt.Fprintln(out)
}
