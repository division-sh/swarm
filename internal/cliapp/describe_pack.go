package cliapp

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/division-sh/swarm/internal/cli/argcount"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packartifact"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/spf13/cobra"
)

type describePackOptions struct {
	packCommandOptions
	provider string
	tool     string
	consume  bool
}

type describePackContext struct {
	config     RuntimeConfigLoadResult
	projection packadmission.Projection
	bundle     *runtimecontracts.WorkflowContractBundle
}

func newDescribePackCommand(ctx context.Context, root InvocationRoot, rootOpts rootCommandOptions) *cobra.Command {
	opts := describePackOptions{packCommandOptions: packCommandOptions{repoRoot: root.Path(), root: rootOpts}}
	cmd := &cobra.Command{
		Use: "pack [provider] [directory]", Short: "Describe an admitted pack's events, tools and consumption blocks.",
		Args: argcount.MaximumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) > 0 {
				opts.provider = args[0]
			}
			if len(args) > 1 {
				opts.sourceRoot = args[1]
			}
			if err := validateDescribePackOptions(opts, cmd.Flags().Changed("tool")); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			loaded, err := loadDescribePackContext(opts.packCommandOptions, opts.provider != "")
			if err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			if opts.provider == "" {
				inventory := packInventoryReadbackFromInventory(loaded.projection.Inventory)
				return renderCLIOutput(cmd.OutOrStdout(), cmd.ErrOrStderr(), opts.output, inventory,
					func(out io.Writer) { writeDescribePackList(out, inventory) }, func() ([]string, error) {
						var ids []string
						for _, entry := range inventory.Packs {
							ids = append(ids, entry.ID)
						}
						return ids, nil
					})
			}
			description, err := buildPackDescription(ctx, loaded, opts.provider)
			if err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			if opts.tool != "" {
				description, err = selectPackDescriptionTool(description, opts.tool)
				if err != nil {
					return returnCLIValidationError(cmd.ErrOrStderr(), err)
				}
			}
			return renderCLIOutput(cmd.OutOrStdout(), cmd.ErrOrStderr(), opts.output, description,
				func(out io.Writer) { writePackDescription(out, description, opts) },
				func() ([]string, error) { return []string{description.ID}, nil })
		},
	}
	cmd.Flags().StringVar(&opts.tool, "tool", "", "Show one exact canonical connector tool")
	cmd.Flags().BoolVar(&opts.consume, "consume", false, "Render descriptor-derived consumption fragments")
	bindCLIOutputFlags(cmd, &opts.output)
	return cmd
}

func validateDescribePackOptions(opts describePackOptions, toolRequested bool) error {
	if err := opts.output.validate(); err != nil {
		return err
	}
	if (toolRequested || opts.consume) && opts.provider == "" {
		return fmt.Errorf("--tool and --consume require an exact provider; list providers with `swarm describe pack`")
	}
	if toolRequested && (opts.tool == "" || opts.tool != strings.TrimSpace(opts.tool)) {
		return fmt.Errorf("--tool requires an exact canonical tool ID")
	}
	if toolRequested && opts.consume {
		return fmt.Errorf("--tool and --consume are separate views; choose one")
	}
	return nil
}

func loadDescribePackContext(opts packCommandOptions, inspectProject bool) (describePackContext, error) {
	cfg, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: opts.repoRoot, ExplicitPath: rootConfigPath(opts.root)})
	if err != nil {
		return describePackContext{}, err
	}
	base, err := LoadConfiguredPlatformPackBase(opts.repoRoot, cfg)
	if err != nil {
		return describePackContext{}, err
	}
	loaded := describePackContext{config: cfg}
	if inspectProject && (opts.sourceRoot != "" || cfg.DeploymentConfigured()) {
		paths, err := resolvePackInventoryPaths(opts, cfg)
		if err != nil {
			return describePackContext{}, err
		}
		specPath, err := EmbeddedPlatformSpecPath()
		if err != nil {
			return describePackContext{}, err
		}
		_, schemaErr := os.Stat(filepath.Join(paths.SourceRoot, "schema.yaml"))
		if schemaErr != nil && !os.IsNotExist(schemaErr) {
			return describePackContext{}, schemaErr
		}
		if opts.sourceRoot != "" || schemaErr == nil {
			artifact, err := sourceartifact.AdmitDirectory(paths.SourceRoot)
			if err != nil {
				return describePackContext{}, err
			}
			rooted, err := artifact.RootedSource()
			if err != nil {
				return describePackContext{}, err
			}
			loaded.bundle, err = runtimecontracts.LoadWorkflowContractBundleFromRootedSource(opts.repoRoot, rooted, specPath,
				runtimecontracts.WorkflowContractLoadOptions{PlatformPackBase: base, AdmitPackInventory: packadmission.AdmitInventory})
			if err != nil {
				return describePackContext{}, err
			}
			loaded.projection, err = packadmission.FromBundle(loaded.bundle)
			return loaded, err
		}
	}
	// An installed-pack query requires neither an invented flow nor a secret read.
	inventory, err := packartifact.NewEffectivePackInventory(base)
	if err != nil {
		return describePackContext{}, err
	}
	specPath, err := EmbeddedPlatformSpecPath()
	if err != nil {
		return describePackContext{}, err
	}
	platform, err := loadChannelPlatformSpecDocument(specPath)
	if err != nil {
		return describePackContext{}, err
	}
	loaded.projection, err = packadmission.Admit(inventory, platform)
	return loaded, err
}
