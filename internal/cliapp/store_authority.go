package cliapp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/division-sh/swarm/internal/config"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
	"github.com/spf13/cobra"
)

type storeAuthorityOptions struct {
	configPath string
	storeMode  string
}

type authorityInspectionStore interface {
	InspectAuthority(context.Context) (runtimestartupownership.AuthorityInspection, error)
	Close() error
}

func newStoreAuthorityCommand(ctx context.Context, root InvocationRoot) *cobra.Command {
	opts := storeAuthorityOptions{storeMode: storebackend.ActiveDefaultBackend().String()}
	cmd := &cobra.Command{Use: "store", Short: "Inspect the selected-store process lease.", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error { return cmd.Help() },
	}
	status := &cobra.Command{Use: "status", Short: "Show the selected store's process lease.", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			inspection, err := inspectSelectedStoreAuthority(ctx, root, cmd, opts)
			if err != nil {
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), authorityInspectionLine(inspection))
			return nil
		},
	}
	status.Flags().StringVar(&opts.configPath, "config", "", "Path to swarm.yaml config")
	status.Flags().StringVar(&opts.storeMode, "store", opts.storeMode, RuntimeStoreBackendHelp)
	cmd.AddCommand(status)
	return cmd
}

func inspectSelectedStoreAuthority(ctx context.Context, root InvocationRoot, cmd *cobra.Command, opts storeAuthorityOptions) (inspection runtimestartupownership.AuthorityInspection, resultErr error) {
	repo := root.Path()
	configPath, _, err := effectiveCommandConfigPath(cmd, opts.configPath, cmd.Flags().Changed("config"))
	if err != nil {
		return runtimestartupownership.AuthorityInspection{}, err
	}
	cfgResult, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: repo, ExplicitPath: configPath})
	if err != nil {
		return runtimestartupownership.AuthorityInspection{}, err
	}
	selection, err := resolveAuthorityStoreSelection(root, cmd, cfgResult, CLISourcePaths{}, opts.storeMode, cmd.Flags().Changed("store"))
	if err != nil {
		return runtimestartupownership.AuthorityInspection{}, err
	}
	if selection.Backend == storebackend.BackendSQLite {
		if _, statErr := os.Stat(selection.SQLitePath); errors.Is(statErr, os.ErrNotExist) {
			return runtimestartupownership.EmptyAuthorityInspection("sqlite_retained_owner")
		} else if statErr != nil {
			return runtimestartupownership.AuthorityInspection{}, fmt.Errorf("inspect selected SQLite store: %w", statErr)
		}
	}
	selected, err := constructAuthorityInspectionStore(ctx, selection, cfgResult.Config)
	if err != nil {
		return runtimestartupownership.AuthorityInspection{}, err
	}
	return inspectAuthorityAndClose(ctx, selected)
}

func inspectAuthorityAndClose(ctx context.Context, selected authorityInspectionStore) (inspection runtimestartupownership.AuthorityInspection, resultErr error) {
	if selected == nil {
		return runtimestartupownership.AuthorityInspection{}, errors.New("authority inspection store is required")
	}
	defer closeAuthorityResult(&resultErr, selected.Close)
	return selected.InspectAuthority(ctx)
}

func closeAuthorityResult(resultErr *error, closeStore func() error) {
	if resultErr == nil || closeStore == nil {
		return
	}
	if closeErr := closeStore(); closeErr != nil {
		*resultErr = errors.Join(*resultErr, closeErr)
	}
}

func authorityInspectionLine(inspection runtimestartupownership.AuthorityInspection) string {
	if inspection.Possession == nil {
		return "Project owner: selected store has not been created."
	}
	lease := "available"
	if !inspection.Possession.Available {
		lease = "held"
	}
	if inspection.Authority == nil {
		return fmt.Sprintf("Project owner: selected-store lease is %s; no process generation has been admitted.", lease)
	}
	return fmt.Sprintf("Project owner: selected-store lease is %s (last admitted generation %d, owner %s).",
		lease, inspection.Authority.AuthorityGeneration, inspection.Authority.OwnerID)
}

func constructAuthorityInspectionStore(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (authorityInspectionStore, error) {
	request, err := selectedAuthorityRequest(ctx, selection, cfg)
	if err != nil {
		return nil, err
	}
	return storeselected.OpenAuthorityInspection(ctx, request)
}

func resolveAuthorityStoreSelection(root InvocationRoot, cmd *cobra.Command, admitted RuntimeConfigLoadResult, paths CLISourcePaths, storeMode string, storeModeSet bool) (storebackend.Selection, error) {
	if blockers := unifiedConfigBlockers(admitted.Diagnostics); len(blockers) > 0 {
		return storebackend.Selection{}, unifiedConfigError{Diagnostics: blockers}
	}
	if admitted.Config == nil || !admitted.validated {
		return storebackend.Selection{}, fmt.Errorf("admitted runtime config is required")
	}
	repo := root.Path()
	swarmDirFlag, swarmDirFlagSet := rootSwarmDirFlag(cmd)
	swarmDir, err := resolveCLISwarmDirFromConfig(root, cliSwarmDirOptions{SwarmDir: swarmDirFlag, SwarmDirFlagSet: swarmDirFlagSet}, admitted.cli)
	if err != nil {
		return storebackend.Selection{}, err
	}
	project := resolveLocalRuntimeStateProject(repo, paths)
	defaultPath, defaultSource := localRuntimeSQLiteDefault(swarmDir, project)
	return resolveRuntimeStoreSelectionWithDefault(repo, storeMode, storeModeSet, admitted.Config, defaultPath, defaultSource)
}

func selectedAuthorityRequest(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (storeselected.AuthorityRequest, error) {
	request := storeselected.AuthorityRequest{Selection: selection}
	if selection.Backend != storebackend.BackendPostgres {
		return request, nil
	}
	if cfg == nil {
		return storeselected.AuthorityRequest{}, errors.New("runtime config is required")
	}
	var credentials runtimecredentials.Store
	if strings.TrimSpace(cfg.Database.PasswordSecretKey) != "" {
		fileStore, err := CredentialFileStore()
		if err != nil {
			return storeselected.AuthorityRequest{}, err
		}
		credentials = fileStore
	}
	password, err := store.ResolveDatabasePassword(ctx, cfg.Database, credentials)
	if err != nil {
		return storeselected.AuthorityRequest{}, err
	}
	request.PostgresDSN = store.DSNFromConfig(cfg.Database, password)
	return request, nil
}
