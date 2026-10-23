package cliapp

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/division-sh/swarm/internal/cli/argcount"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/packmodel"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/spf13/cobra"
)

type packCommandOptions struct {
	repoRoot   string
	root       rootCommandOptions
	sourceRoot string
	output     cliOutputOptions
}

type packInventoryEntryReadback struct {
	ID           string   `json:"id" yaml:"id"`
	Bodies       []string `json:"bodies" yaml:"bodies"`
	Version      string   `json:"version" yaml:"version"`
	ManifestHash string   `json:"manifest_hash" yaml:"manifest_hash"`
	Source       string   `json:"source" yaml:"source"`
	Directory    string   `json:"directory" yaml:"directory"`
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
	ManifestYAML string                     `json:"manifest_yaml" yaml:"manifest_yaml"`
	Bodies       map[string]string          `json:"bodies" yaml:"bodies"`
}

func newPacksCommand(ctx context.Context, invocationRoot InvocationRoot, root rootCommandOptions) *cobra.Command {
	cmd := &cobra.Command{Use: "packs", Short: "Inspect the selected platform and project pack inventory.", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error { return cmd.Help() }}
	cmd.AddCommand(newPacksListCommand(ctx, invocationRoot.Path(), root), newPacksShowCommand(ctx, invocationRoot.Path(), root))
	return cmd
}

func newPacksListCommand(_ context.Context, repo string, root rootCommandOptions) *cobra.Command {
	opts := packCommandOptions{repoRoot: repo, root: root}
	cmd := &cobra.Command{
		Use: "list [directory]", Short: "List the exact effective pack inventory.", Args: argcount.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 1 {
				opts.sourceRoot = args[0]
			}
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
		Use: "show <provider> [directory]", Short: "Show one selected provider directory.", Args: argcount.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 2 {
				opts.sourceRoot = args[1]
			}
			if err := opts.output.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			inventory, err := loadEffectivePackInventory(opts)
			if err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			id := args[0]
			entry, ok := inventory.Lookup(id)
			if ok {
				pack := packInventoryEntryReadbackFromEntry(entry)
				show := packShowReadback{Pack: pack, ManifestYAML: string(entry.ManifestBody()), Bodies: map[string]string{}}
				for _, role := range []packmodel.BodyRole{packmodel.BodyTrigger, packmodel.BodyConnector, packmodel.BodyChannel} {
					if body, present := entry.Provider().Body(role); present {
						show.Bodies[role.FileName()] = string(body.Bytes())
					}
				}
				return renderCLIOutput(cmd.OutOrStdout(), cmd.ErrOrStderr(), opts.output, show, func(out io.Writer) {
					writePackEntry(out, pack)
					fmt.Fprintln(out, "manifest.yaml:")
					fmt.Fprint(out, show.ManifestYAML)
					if !strings.HasSuffix(show.ManifestYAML, "\n") {
						fmt.Fprintln(out)
					}
					for _, name := range pack.Bodies {
						fmt.Fprintf(out, "%s:\n%s\n", name, show.Bodies[name])
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

func newImportPackCommand(invocationRoot InvocationRoot, root rootCommandOptions) *cobra.Command {
	opts := packCommandOptions{repoRoot: invocationRoot.Path(), root: root}
	cmd := &cobra.Command{
		Use: "import <provider> [directory]", Short: "Copy an embedded provider directory into the selected flow.", Args: argcount.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 2 {
				opts.sourceRoot = args[1]
			}
			if err := opts.output.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			cfgResult, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: opts.repoRoot, ExplicitPath: rootConfigPath(opts.root)})
			if err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			paths, err := resolvePackInventoryPaths(opts, cfgResult)
			if err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			sourceRoot, err := NormalizeSourceRoot(paths.SourceRoot)
			if err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			specPath, err := EmbeddedPlatformSpecPath()
			if err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			platformSpec, err := loadChannelPlatformSpecDocument(specPath)
			if err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			embedded, err := packartifact.LoadEmbeddedPlatformPackInventory(strings.TrimSpace(platformSpec.Platform.Version))
			if err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			id := args[0]
			changed, err := packartifact.ImportEmbeddedPack(sourceRoot, id, embedded)
			if err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			result := packImportReadback{ID: id, Changed: changed, Source: "local"}
			return renderCLIOutput(cmd.OutOrStdout(), cmd.ErrOrStderr(), opts.output, result, func(out io.Writer) {
				state := "already imported"
				if changed {
					state = "imported"
				}
				fmt.Fprintf(out, "provider %s: %s source=local\n", id, state)
			}, func() ([]string, error) { return []string{id}, nil })
		},
	}
	bindPackSourceFlags(cmd, &opts)
	bindCLIOutputFlags(cmd, &opts.output)
	return cmd
}

func bindPackSourceFlags(cmd *cobra.Command, opts *packCommandOptions) {
}

func rootConfigPath(root rootCommandOptions) string {
	if root.rootFlags != nil && root.rootFlags.configPathSet {
		return root.rootFlags.configPath
	}
	return ""
}

func loadEffectivePackInventory(opts packCommandOptions) (*packartifact.EffectivePackInventory, error) {
	cfgResult, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: opts.repoRoot, ExplicitPath: rootConfigPath(opts.root)})
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
	sourceRoot, err := NormalizeSourceRoot(paths.SourceRoot)
	if err != nil {
		return nil, err
	}
	specPath, err := EmbeddedPlatformSpecPath()
	if err != nil {
		return nil, err
	}
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOptions(opts.repoRoot, sourceRoot, specPath, runtimecontracts.WorkflowContractLoadOptions{
		PlatformPackBase: base, AdmitPackInventory: packadmission.AdmitInventory,
	})
	if err != nil {
		return nil, err
	}
	return bundle.PackInventory, nil
}

func resolvePackInventoryPaths(opts packCommandOptions, cfgResult RuntimeConfigLoadResult) (CLISourcePaths, error) {
	return resolveCLISourcePathsFromConfig(opts.repoRoot, CLISourcePathOptions{
		SourceRoot: opts.sourceRoot,
	}, cfgResult.cli)
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
	out := packInventoryEntryReadback{
		ID: entry.ID(), Version: entry.Version(), ManifestHash: entry.ManifestHash(),
		Source: entry.Source(), Directory: entry.Directory(),
	}
	for _, role := range []packmodel.BodyRole{packmodel.BodyTrigger, packmodel.BodyConnector, packmodel.BodyChannel} {
		if _, present := entry.Provider().Body(role); present {
			out.Bodies = append(out.Bodies, role.FileName())
		}
	}
	return out
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
	fmt.Fprintf(out, "%s bodies=%s version=%s source=%s manifest=%s", pack.ID, strings.Join(pack.Bodies, ","), pack.Version, pack.Source, pack.ManifestHash)
	fmt.Fprintln(out)
}
