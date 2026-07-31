package cliapp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/spf13/cobra"
)

type migrateResolutionInstanceKeyOptions struct {
	contractsPath string
	output        cliOutputOptions
}

type migrateResolutionInstanceKeyResult struct {
	ContractsRoot string `json:"contracts_root"`
	Files         int    `json:"files"`
	Declarations  int    `json:"declarations"`
}

func newMigrateResolutionInstanceKeyCommand(repo string) *cobra.Command {
	opts := migrateResolutionInstanceKeyOptions{}
	cmd := &cobra.Command{
		Use:   "migrate-resolution-instance-key",
		Short: "Move retired resolution.instance_key authority to instance.by and carries.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runMigrateResolutionInstanceKeyCommand(cmd.OutOrStdout(), cmd.ErrOrStderr(), assetCommandRepoRoot(repo), opts)
		},
	}
	cmd.Flags().StringVar(&opts.contractsPath, "contracts", "", "Path to the Swarm contract bundle root")
	if err := cmd.MarkFlagRequired("contracts"); err != nil {
		panic(err)
	}
	bindCLIOutputFlags(cmd, &opts.output)
	return cmd
}

func runMigrateResolutionInstanceKeyCommand(out, errOut io.Writer, repo string, opts migrateResolutionInstanceKeyOptions) error {
	resolved := ResolvePath(repo, opts.contractsPath)
	contractsRoot, err := NormalizeContractsRoot(resolved)
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	result, err := runtimecontracts.RewriteRetiredResolutionInstanceKeys(contractsRoot, func(candidateRoot string) error {
		opts := defaultVerifyCommandOptions()
		opts.contractsPath = candidateRoot
		var stdout bytes.Buffer
		var stderr bytes.Buffer
		if code := runVerifyCommandWithOutput(context.Background(), repo, opts, &stdout, &stderr); code != 0 {
			detail := strings.TrimSpace(strings.Join([]string{stderr.String(), stdout.String()}, "\n"))
			if detail == "" {
				detail = fmt.Sprintf("swarm verify exited with code %d", code)
			}
			return fmt.Errorf("swarm verify rejected candidate: %s", detail)
		}
		return nil
	})
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	view := migrateResolutionInstanceKeyResult{
		ContractsRoot: contractsRoot,
		Files:         result.Files,
		Declarations:  result.Declarations,
	}
	return renderCLIOutput(out, errOut, opts.output, view, func(w io.Writer) {
		if result.Declarations == 0 {
			fmt.Fprintln(w, "no resolution.instance_key declarations found")
			return
		}
		fmt.Fprintf(w, "migrated %d resolution.instance_key declaration%s across %d schema file%s\n", result.Declarations, pluralSuffix(result.Declarations), result.Files, pluralSuffix(result.Files))
	}, func() ([]string, error) {
		return []string{fmt.Sprintf("%d", result.Declarations)}, nil
	})
}
