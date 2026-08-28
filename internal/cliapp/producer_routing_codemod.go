package cliapp

import (
	"fmt"
	"io"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/spf13/cobra"
)

type migrateProducerRoutingOptions struct {
	contractsPath string
	output        cliOutputOptions
}

type migrateProducerRoutingResult struct {
	ContractsRoot string `json:"contracts_root"`
	Removed       int    `json:"removed"`
}

func newMigrateProducerRoutingCommand(root InvocationRoot) *cobra.Command {
	opts := migrateProducerRoutingOptions{}
	cmd := &cobra.Command{
		Use:   "migrate-producer-routing",
		Short: "Remove deterministic retired emit.broadcast: true declarations.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runMigrateProducerRoutingCommand(cmd.OutOrStdout(), cmd.ErrOrStderr(), root.Path(), opts)
		},
	}
	cmd.Flags().StringVar(&opts.contractsPath, "contracts", "", "Path to the Swarm contract bundle root")
	if err := cmd.MarkFlagRequired("contracts"); err != nil {
		panic(err)
	}
	bindCLIOutputFlags(cmd, &opts.output)
	return cmd
}

func runMigrateProducerRoutingCommand(out, errOut io.Writer, repo string, opts migrateProducerRoutingOptions) error {
	resolved := ResolvePath(repo, opts.contractsPath)
	contractsRoot, err := NormalizeContractsRoot(resolved)
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	removed, err := runtimecontracts.RewriteRetiredProducerBroadcasts(contractsRoot)
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	result := migrateProducerRoutingResult{ContractsRoot: contractsRoot, Removed: removed}
	return renderCLIOutput(out, errOut, opts.output, result, func(w io.Writer) {
		if removed == 0 {
			fmt.Fprintln(w, "no deterministic emit.broadcast: true declarations found")
			return
		}
		fmt.Fprintf(w, "migrated contract bundle: removed %d retired emit.broadcast: true declaration%s\n", removed, pluralSuffix(removed))
	}, func() ([]string, error) {
		return []string{fmt.Sprintf("%d", removed)}, nil
	})
}
