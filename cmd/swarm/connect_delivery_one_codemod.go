package main

import (
	"fmt"
	"io"
	"path/filepath"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/spf13/cobra"
)

type migrateConnectDeliveryOneOptions struct {
	contractsPath string
	output        cliOutputOptions
}

type migrateConnectDeliveryOneResult struct {
	PackageFile string `json:"package_file"`
	Removed     int    `json:"removed"`
}

func newMigrateConnectDeliveryOneCommand(repo string) *cobra.Command {
	opts := migrateConnectDeliveryOneOptions{}
	cmd := &cobra.Command{
		Use:   "migrate-connect-delivery-one",
		Short: "Remove retired connect.delivery: one declarations.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runMigrateConnectDeliveryOneCommand(
				cmd.OutOrStdout(),
				cmd.ErrOrStderr(),
				assetCommandRepoRoot(repo),
				opts,
			)
		},
	}
	cmd.Flags().StringVar(&opts.contractsPath, "contracts", "", "Path to the Swarm contract bundle root containing package.yaml")
	if err := cmd.MarkFlagRequired("contracts"); err != nil {
		panic(err)
	}
	bindCLIOutputFlags(cmd, &opts.output)
	return cmd
}

func runMigrateConnectDeliveryOneCommand(out, errOut io.Writer, repo string, opts migrateConnectDeliveryOneOptions) error {
	resolved := resolvePath(repo, opts.contractsPath)
	contractsRoot, err := normalizeContractsRoot(resolved)
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	packageFile := filepath.Join(contractsRoot, "package.yaml")
	removed, err := runtimecontracts.RewriteRetiredConnectDeliveryOne(packageFile)
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	result := migrateConnectDeliveryOneResult{PackageFile: packageFile, Removed: removed}
	return renderCLIOutput(out, errOut, opts.output, result, func(w io.Writer) {
		if removed == 0 {
			fmt.Fprintln(w, "no connect.delivery: one declarations found")
			return
		}
		fmt.Fprintf(w, "migrated package.yaml: removed %d retired connect.delivery: one declaration%s\n", removed, pluralSuffix(removed))
	}, func() ([]string, error) {
		return []string{fmt.Sprintf("%d", removed)}, nil
	})
}
