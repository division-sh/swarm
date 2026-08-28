package cliapp

import (
	"fmt"
	"io"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/spf13/cobra"
)

type mintElementIDsOptions struct {
	contractsPath string
	output        cliOutputOptions
}

type mintElementIDsResult struct {
	ContractsPath string `json:"contracts_path"`
	FilesChanged  int    `json:"files_changed"`
	IDsMinted     int    `json:"ids_minted"`
}

func newMintElementIDsCommand(root InvocationRoot) *cobra.Command {
	opts := mintElementIDsOptions{}
	cmd := &cobra.Command{
		Use:   "mint-element-ids",
		Short: "Mint stable IDs for adopted authored contract elements.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runMintElementIDsCommand(cmd.OutOrStdout(), cmd.ErrOrStderr(), root.Path(), opts)
		},
	}
	cmd.Flags().StringVar(&opts.contractsPath, "contracts", "", "Path to one contract bundle or a corpus tree")
	if err := cmd.MarkFlagRequired("contracts"); err != nil {
		panic(err)
	}
	bindCLIOutputFlags(cmd, &opts.output)
	return cmd
}

func runMintElementIDsCommand(out, errOut io.Writer, repo string, opts mintElementIDsOptions) error {
	root := ResolvePath(repo, opts.contractsPath)
	minted, err := runtimecontracts.MintHandlerRuleElementIDs(root)
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	result := mintElementIDsResult{ContractsPath: root, FilesChanged: minted.FilesChanged, IDsMinted: minted.IDsMinted}
	return renderCLIOutput(out, errOut, opts.output, result, func(w io.Writer) {
		fmt.Fprintf(w, "minted %d stable element ID%s across %d file%s\n", minted.IDsMinted, pluralSuffix(minted.IDsMinted), minted.FilesChanged, pluralSuffix(minted.FilesChanged))
	}, func() ([]string, error) {
		return []string{fmt.Sprintf("%d", minted.IDsMinted)}, nil
	})
}
