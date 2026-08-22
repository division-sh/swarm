package cliapp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/config"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	storeconstruction "github.com/division-sh/swarm/internal/store/construction"
	"github.com/division-sh/swarm/internal/versionmetadata"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
)

type storeAuthorityOptions struct {
	configPath string
	storeMode  string
	confirm    string
}

type authorityMaintenanceStore interface {
	runtimestartupownership.AuthorityMaintenanceStore
	store.SchemaBootstrapper
	Close() error
}

type authorityInspectionStore interface {
	InspectAuthority(context.Context) (runtimestartupownership.AuthorityInspection, error)
	Close() error
}

func newStoreAuthorityCommand(ctx context.Context, repo string) *cobra.Command {
	opts := storeAuthorityOptions{storeMode: storebackend.ActiveDefaultBackend().String()}
	cmd := &cobra.Command{
		Use:   "store",
		Short: "Inspect and repair selected-store project ownership.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	status := &cobra.Command{
		Use:   "status",
		Short: "Show who the selected store records as project owner.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			inspection, err := inspectSelectedStoreAuthority(ctx, repo, cmd, opts)
			if err != nil {
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), authorityInspectionLine(inspection))
			return nil
		},
	}
	repair := &cobra.Command{
		Use:   "repair-authority",
		Short: "Repair an inconsistent project ownership record after inspection.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			selected, closeStore, err := openAuthorityMaintenanceStore(ctx, repo, cmd, opts)
			if err != nil {
				return err
			}
			defer closeStore()
			inspection, err := selected.InspectAuthority(ctx)
			if err != nil {
				return err
			}
			if inspection.Status != runtimestartupownership.AuthorityInspectionCorrupt {
				fmt.Fprintln(cmd.OutOrStdout(), authorityInspectionLine(inspection))
				return fmt.Errorf("project ownership repair is not required")
			}
			fmt.Fprintln(cmd.OutOrStdout(), "The recorded project session is inconsistent.")
			fmt.Fprintln(cmd.OutOrStdout(), "Plan: replace only the broken session record with a clean stopped session.")
			fmt.Fprintln(cmd.OutOrStdout(), "Your flows, runs, and data are untouched.")
			if strings.TrimSpace(opts.confirm) == "" {
				fmt.Fprintf(cmd.OutOrStdout(), "Re-run with --confirm %s to apply this exact plan.\n", inspection.FindingsDigest)
				return commandExitError{code: CLIExitValidation}
			}
			if strings.TrimSpace(opts.confirm) != inspection.FindingsDigest {
				return fmt.Errorf("--confirm does not match the current findings; inspect again")
			}
			result, err := selected.RepairAuthority(ctx, runtimestartupownership.AuthorityRepairRequest{
				OperationID: uuid.NewString(), FindingsDigest: inspection.FindingsDigest, Confirmed: true,
			})
			if err != nil {
				return err
			}
			if err := result.Validate(); err != nil {
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), "✓ Project ownership repaired. Your flows, runs, and data were untouched.")
			return nil
		},
	}
	for _, child := range []*cobra.Command{status, repair} {
		child.Flags().StringVar(&opts.configPath, "config", "", "Path to swarm.yaml config")
		child.Flags().StringVar(&opts.storeMode, "store", opts.storeMode, RuntimeStoreBackendHelp)
	}
	repair.Flags().StringVar(&opts.confirm, "confirm", "", "Confirm the exact findings digest printed by inspection")
	cmd.AddCommand(status, repair)
	return cmd
}

func inspectSelectedStoreAuthority(ctx context.Context, repo string, cmd *cobra.Command, opts storeAuthorityOptions) (runtimestartupownership.AuthorityInspection, error) {
	configPath, _, err := effectiveCommandConfigPath(cmd, opts.configPath, cmd.Flags().Changed("config"))
	if err != nil {
		return runtimestartupownership.AuthorityInspection{}, err
	}
	cfgResult, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: repo, ExplicitPath: configPath})
	if err != nil {
		return runtimestartupownership.AuthorityInspection{}, err
	}
	paths, err := ResolveCLIContractPlatformSpecPaths(repo, CLIContractPlatformSpecPathOptions{ConfigPath: configPath})
	if err != nil {
		return runtimestartupownership.AuthorityInspection{}, err
	}
	selection, err := resolveAuthorityStoreSelection(repo, configPath, cmd, cfgResult.Config, paths, opts.storeMode, cmd.Flags().Changed("store"))
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
	defer selected.Close()
	return selected.InspectAuthority(ctx)
}

func authorityInspectionLine(inspection runtimestartupownership.AuthorityInspection) string {
	switch inspection.Status {
	case runtimestartupownership.AuthorityInspectionEmpty:
		return "Project owner: no previous session is recorded."
	case runtimestartupownership.AuthorityInspectionCorrupt:
		return "Project owner: unknown because the recorded session is inconsistent. Run `swarm store repair-authority` to inspect and repair it."
	case runtimestartupownership.AuthorityInspectionValid:
		if inspection.State == runtimestartupownership.StateActive {
			return fmt.Sprintf("Project owner: session %s is recorded as active; status does not infer whether its process is live.", strings.TrimSpace(inspection.OwnerID))
		}
		return "Project owner: no active session is recorded."
	default:
		return "Project owner: unavailable."
	}
}

func openAuthorityMaintenanceStore(ctx context.Context, repo string, cmd *cobra.Command, opts storeAuthorityOptions) (authorityMaintenanceStore, func(), error) {
	configPath, _, err := effectiveCommandConfigPath(cmd, opts.configPath, cmd.Flags().Changed("config"))
	if err != nil {
		return nil, func() {}, err
	}
	cfgResult, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: repo, ExplicitPath: configPath})
	if err != nil {
		return nil, func() {}, err
	}
	paths, err := ResolveCLIContractPlatformSpecPaths(repo, CLIContractPlatformSpecPathOptions{ConfigPath: configPath})
	if err != nil {
		return nil, func() {}, err
	}
	_, bundle, err := NewSwarmWorkflowModule(repo, paths.ContractsPath, paths.PlatformSpecPath)
	if err != nil {
		return nil, func() {}, fmt.Errorf("load selected-store schema source: %w", err)
	}
	plans, err := StateStoreSchemaPlans(bundle)
	if err != nil {
		return nil, func() {}, err
	}
	selection, err := resolveAuthorityStoreSelection(repo, configPath, cmd, cfgResult.Config, paths, opts.storeMode, cmd.Flags().Changed("store"))
	if err != nil {
		return nil, func() {}, err
	}
	selected, err := constructAuthorityMaintenanceStore(ctx, selection, cfgResult.Config)
	if err != nil {
		return nil, func() {}, err
	}
	closeStore := func() { _ = selected.Close() }
	metadata, err := versionmetadata.Resolve(InjectedBuildMetadata())
	if err != nil {
		closeStore()
		return nil, func() {}, err
	}
	err = selected.BootstrapSchema(ctx, store.SchemaBootstrapRequest{
		PlatformPlans: plans.Platform, StatePlans: plans.State,
		Origin: store.RuntimeStoreOrigin{
			SwarmVersion: metadata.BinaryVersion, PlatformVersion: strings.TrimSpace(bundle.Platform.Platform.Version), CreatedAt: time.Now().UTC(),
		},
	})
	if err != nil {
		closeStore()
		return nil, func() {}, err
	}
	return selected, closeStore, nil
}

func openAuthorityInspectionStore(ctx context.Context, repo string, cmd *cobra.Command, opts doctorOptions) (authorityInspectionStore, func(), error) {
	configPath, _, err := effectiveCommandConfigPath(cmd, opts.configPath, cmd.Flags().Changed("config"))
	if err != nil {
		return nil, func() {}, err
	}
	cfgResult, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: repo, ExplicitPath: configPath})
	if err != nil {
		return nil, func() {}, err
	}
	paths, err := ResolveCLIContractPlatformSpecPaths(repo, CLIContractPlatformSpecPathOptions{
		ContractsPath: opts.contractsPath, PlatformSpecPath: opts.platformSpecPath, ConfigPath: configPath,
	})
	if err != nil {
		return nil, func() {}, err
	}
	selection, err := resolveAuthorityStoreSelection(repo, configPath, cmd, cfgResult.Config, paths, storebackend.ActiveDefaultBackend().String(), false)
	if err != nil {
		return nil, func() {}, err
	}
	selected, err := constructAuthorityInspectionStore(ctx, selection, cfgResult.Config)
	if err != nil {
		return nil, func() {}, err
	}
	return selected, func() { _ = selected.Close() }, nil
}

func constructAuthorityInspectionStore(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (authorityInspectionStore, error) {
	if selection.Backend == storebackend.BackendSQLite {
		selected, err := storeconstruction.OpenSQLiteRuntimeReadOnly(selection.SQLitePath)
		return selected, err
	}
	return constructAuthorityMaintenanceStore(ctx, selection, cfg)
}

func resolveAuthorityStoreSelection(repo, configPath string, cmd *cobra.Command, cfg *config.Config, paths CLIContractPlatformSpecPaths, storeMode string, storeModeSet bool) (storebackend.Selection, error) {
	cliCfg, err := loadCLICommandConfigWithOptions(unifiedConfigLoadOptions{RepoRoot: repo, ExplicitPath: configPath})
	if err != nil {
		return storebackend.Selection{}, err
	}
	swarmDirFlag, swarmDirFlagSet := rootSwarmDirFlag(cmd)
	swarmDir, err := resolveCLISwarmDirFromConfig(cliSwarmDirOptions{SwarmDir: swarmDirFlag, SwarmDirFlagSet: swarmDirFlagSet}, cliCfg)
	if err != nil {
		return storebackend.Selection{}, err
	}
	project := resolveLocalRuntimeStateProject(repo, paths)
	defaultPath, defaultSource := localRuntimeSQLiteDefault(swarmDir, project)
	return resolveRuntimeStoreSelectionWithDefault(repo, storeMode, storeModeSet, cfg, defaultPath, defaultSource)
}

func constructAuthorityMaintenanceStore(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (authorityMaintenanceStore, error) {
	switch selection.Backend {
	case storebackend.BackendSQLite:
		selected, _, err := storeconstruction.OpenSQLiteRuntimeWithOwnershipBinding(selection.SQLitePath)
		return selected, err
	case storebackend.BackendPostgres:
		var credentials runtimecredentials.Store
		if strings.TrimSpace(cfg.Database.PasswordSecretKey) != "" {
			fileStore, err := CredentialFileStore()
			if err != nil {
				return nil, err
			}
			credentials = fileStore
		}
		password, err := store.ResolveDatabasePassword(ctx, cfg.Database, credentials)
		if err != nil {
			return nil, err
		}
		selected, _, err := storeconstruction.OpenPostgres(store.DSNFromConfig(cfg.Database, password))
		return selected, err
	default:
		return nil, fmt.Errorf("selected store backend is required")
	}
}
