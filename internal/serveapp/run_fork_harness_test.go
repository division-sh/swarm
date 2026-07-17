package serveapp

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimerunforkadmission "github.com/division-sh/swarm/internal/runtime/runforkadmission"
	runtimerunforkexecution "github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/google/uuid"
)

// runForkRuntimeOwnerHarness preserves internal runtime/store fork owner coverage for targeted tests.
// The public `swarm run fork <source-run-id> [--bundle-hash <bundle_hash>] [--at-event <event-id>] [--confirm-source-freeze] [--idempotency-key <key>]` command consumes /v1/rpc run.fork rather than this harness.
func runForkRuntimeOwnerHarness(ctx context.Context, repo string, args []string, out io.Writer) int {
	fs := flag.NewFlagSet("fork", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	configPath := fs.String("config", "", "Path to swarm.yaml config")
	backend := fs.String("backend", "", "LLM backend profile for local runtime startup")
	contractsPath := fs.String("contracts", "", "Path to selected Swarm contract bundle root for fork planning or selected-contract execution")
	platformSpecPath := fs.String("platform-spec", defaultPlatformSpecPath, "Path to platform spec yaml")
	storeMode := fs.String("store", storebackend.ActiveDefaultBackend().String(), cliapp.RuntimeStoreBackendHelp)
	runID := fs.String("run", "", "Source run ID to plan from")
	at := fs.String("at", "", "Fork point event UUID")
	dryRun := fs.Bool("dry-run", false, "Plan the fork without mutating runtime state")
	materializeOnly := fs.Bool("materialize-only", false, "Create fork run and materialize state snapshot without resuming execution")
	activate := fs.Bool("activate", false, "Activate an already materialized state-only fork")
	confirmSourceFreeze := fs.Bool("confirm-source-freeze", false, "Confirm that activation may permanently freeze an active source run")
	asJSON := fs.Bool("json", false, "Emit JSON")
	if err := fs.Parse(args); err != nil {
		if out != nil {
			fmt.Fprintf(out, "fork failed: %v\n", err)
		}
		return 2
	}
	storeModeSet := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "store" {
			storeModeSet = true
		}
	})
	modeCount := 0
	for _, enabled := range []bool{*dryRun, *materializeOnly, *activate} {
		if enabled {
			modeCount++
		}
	}
	if modeCount > 1 {
		if out != nil {
			fmt.Fprintln(out, "fork failed: --dry-run, --materialize-only, and --activate are mutually exclusive")
		}
		return 2
	}
	selectedContractsRequested := strings.TrimSpace(*contractsPath) != ""
	if modeCount == 0 && !selectedContractsRequested {
		if out != nil {
			fmt.Fprintln(out, "fork failed: mutating fork execution without --contracts is not implemented; use --dry-run, --materialize-only, --activate, or --contracts")
		}
		return 2
	}
	if *activate && strings.TrimSpace(*at) != "" {
		if out != nil {
			fmt.Fprintln(out, "fork failed: --activate targets --run <fork_run_id> and does not accept --at")
		}
		return 2
	}
	cfgResult, err := cliapp.LoadRuntimeConfigWithOptions(cliapp.RuntimeConfigLoadOptions{
		RepoRoot:        repo,
		ExplicitPath:    *configPath,
		BackendOverride: *backend,
	})
	if err != nil {
		if out != nil {
			fmt.Fprintf(out, "fork failed: load config: %v\n", err)
		}
		return 1
	}
	cfg := cfgResult.Config
	storeSelection, err := cliapp.ResolveRuntimeStoreSelection(repo, *storeMode, storeModeSet, cfg)
	if err != nil {
		if out != nil {
			fmt.Fprintf(out, "fork failed: resolve store backend: %v\n", err)
		}
		return 1
	}
	stores, err := buildStores(ctx, storeSelection, cfg)
	if err != nil {
		if out != nil {
			fmt.Fprintf(out, "fork failed: init stores: %v\n", err)
		}
		return 1
	}
	storeFacade := stores.facade()
	defer storeFacade.close()
	resolvedPlatformSpecPath := cliapp.ResolvePath(repo, *platformSpecPath)
	if strings.TrimSpace(*platformSpecPath) == defaultPlatformSpecPath {
		if _, statErr := os.Stat(resolvedPlatformSpecPath); statErr != nil {
			resolvedPlatformSpecPath = cliapp.ResolvePath(cliapp.RepoRoot(), defaultPlatformSpecPath)
		}
	}
	platformSpec, err := loadServePlatformSpecDocument(resolvedPlatformSpecPath)
	if err != nil {
		if out != nil {
			fmt.Fprintf(out, "fork failed: load platform spec: %v\n", err)
		}
		return 1
	}
	platformPlans, err := store.GeneratePlatformTableDDLs(platformSpec)
	if err != nil {
		if out != nil {
			fmt.Fprintf(out, "fork failed: generate platform schema: %v\n", err)
		}
		return 1
	}
	bootstrapRequest, err := schemaBootstrapRequest(platformSpec, platformPlans, nil)
	if err != nil {
		if out != nil {
			fmt.Fprintf(out, "fork failed: prepare platform schema bootstrap: %v\n", err)
		}
		return 1
	}
	if err := ensureServeSchemaTables(ctx, stores, bootstrapRequest); err != nil {
		if out != nil {
			fmt.Fprintf(out, "fork failed: bootstrap platform schema: %v\n", err)
		}
		return 1
	}
	ctx = runForkRuntimeOwnerContext(ctx)
	runForkOwner, ok := storeFacade.runForkRuntimeOwner()
	if !ok {
		if out != nil {
			fmt.Fprintln(out, "fork failed: postgres store required")
		}
		return 1
	}
	if *activate {
		result, err := runForkOwner.activate(ctx, runtimerunforkexecution.SelectedContractActivationGateRequest{
			ForkRunID:           strings.TrimSpace(*runID),
			ConfirmSourceFreeze: *confirmSourceFreeze,
			SourceLoader: runtimerunforkexecution.ContractBundleSourceLoader{
				RepoRoot:         repo,
				PlatformSpecPath: resolvedPlatformSpecPath,
			},
		})
		if err != nil {
			if out != nil {
				fmt.Fprintf(out, "fork failed: %v\n", err)
			}
			return 1
		}
		if *asJSON {
			enc := json.NewEncoder(out)
			enc.SetIndent("", "  ")
			if err := enc.Encode(result); err != nil {
				fmt.Fprintf(out, "fork failed: encode json: %v\n", err)
				return 1
			}
			return 0
		}
		printRunForkActivation(out, result.RunForkActivation)
		return 0
	}
	if *materializeOnly {
		var contractSelection *store.RunForkContractSelection
		if contracts := strings.TrimSpace(*contractsPath); contracts != "" {
			contractsRoot, err := cliapp.NormalizeContractsRoot(cliapp.ResolveContractsPath(repo, contracts))
			if err != nil {
				writeForkContractLoadError(out, "fork failed: resolve contracts", err)
				return cliapp.CLIExitValidation
			}
			_, bundle, err := cliapp.NewSwarmWorkflowModule(repo, contractsRoot, cliapp.ResolvePath(repo, *platformSpecPath))
			if err != nil {
				writeForkContractLoadError(out, "fork failed: load selected contracts", err)
				return cliapp.CLIExitValidation
			}
			source := semanticview.Wrap(bundle)
			selection := runtimerunforkadmission.SelectedContractSelection(source, contractsRoot)
			contractSelection = &selection
			bundleHash, err := runtimecontracts.BundleHash(bundle)
			if err != nil {
				writeForkContractLoadError(out, "fork failed: hash selected contracts", err)
				return cliapp.CLIExitValidation
			}
			result, err := runForkOwner.materialize(ctx, store.RunForkMaterializeRequest{
				SourceRunID:       strings.TrimSpace(*runID),
				At:                strings.TrimSpace(*at),
				ContractSelection: contractSelection,
				BundleHash:        bundleHash,
				BundleSource:      storerunlifecycle.BundleSourceEphemeral,
			})
			if err != nil {
				if out != nil {
					fmt.Fprintf(out, "fork failed: %v\n", err)
				}
				return 1
			}
			if *asJSON {
				enc := json.NewEncoder(out)
				enc.SetIndent("", "  ")
				if err := enc.Encode(result); err != nil {
					fmt.Fprintf(out, "fork failed: encode json: %v\n", err)
					return 1
				}
				return 0
			}
			printRunForkMaterialization(out, result)
			return 0
		}
		result, err := runForkOwner.materialize(ctx, store.RunForkMaterializeRequest{
			SourceRunID:       strings.TrimSpace(*runID),
			At:                strings.TrimSpace(*at),
			ContractSelection: contractSelection,
		})
		if err != nil {
			if out != nil {
				fmt.Fprintf(out, "fork failed: %v\n", err)
			}
			return 1
		}
		if *asJSON {
			enc := json.NewEncoder(out)
			enc.SetIndent("", "  ")
			if err := enc.Encode(result); err != nil {
				fmt.Fprintf(out, "fork failed: encode json: %v\n", err)
				return 1
			}
			return 0
		}
		printRunForkMaterialization(out, result)
		return 0
	}
	if modeCount == 0 && selectedContractsRequested {
		contractsRoot, err := cliapp.NormalizeContractsRoot(cliapp.ResolveContractsPath(repo, *contractsPath))
		if err != nil {
			writeForkContractLoadError(out, "fork failed: resolve contracts", err)
			return cliapp.CLIExitValidation
		}
		_, bundle, err := cliapp.NewSwarmWorkflowModule(repo, contractsRoot, cliapp.ResolvePath(repo, *platformSpecPath))
		if err != nil {
			writeForkContractLoadError(out, "fork failed: load selected contracts", err)
			return cliapp.CLIExitValidation
		}
		source := semanticview.Wrap(bundle)
		credentialStore, err := cliapp.BuildCredentialStore()
		if err != nil {
			if out != nil {
				fmt.Fprintf(out, "fork failed: configure credentials: %v\n", err)
			}
			return 1
		}
		managedCredentialStore, err := cliapp.BuildManagedCredentialStore()
		if err != nil {
			if out != nil {
				fmt.Fprintf(out, "fork failed: configure managed credentials: %v\n", err)
			}
			return 1
		}
		providerCredentialStore, err := cliapp.BuildProviderCredentialStore()
		if err != nil {
			if out != nil {
				fmt.Fprintf(out, "fork failed: configure provider credentials: %v\n", err)
			}
			return 1
		}
		swarmDir, err := cliapp.ResolveServeContextRegistrationSwarmDir(cliapp.DefaultServeOptions())
		if err != nil {
			if out != nil {
				fmt.Fprintf(out, "fork failed: resolve Swarm directory: %v\n", err)
			}
			return 1
		}
		localState, err := cliapp.ResolveLocalRuntimeState(cliapp.LocalRuntimeStateOptions{
			RepoRoot:                repo,
			ResolvedPaths:           cliapp.CLIContractPlatformSpecPaths{ContractsPath: contractsRoot},
			SwarmDir:                swarmDir,
			Config:                  cfg,
			CreateDefaultDataSource: true,
		})
		if err != nil {
			if out != nil {
				fmt.Fprintf(out, "fork failed: resolve workspace data source: %v\n", err)
			}
			return 1
		}
		mountSources := localState.MountSources
		workspaceBackendPreference, err := cliapp.ResolveWorkspaceBackend("", false, cfg)
		if err != nil {
			if out != nil {
				fmt.Fprintf(out, "fork failed: resolve workspace backend: %v\n", err)
			}
			return 1
		}
		workspaceBackend, err := cliapp.DecideWorkspaceBackend(workspaceBackendPreference, cfg, source)
		if err != nil {
			if out != nil {
				fmt.Fprintf(out, "fork failed: resolve workspace backend: %v\n", err)
			}
			return 1
		}
		workspaces, err := cliapp.ConfiguredWorkspaceLifecycleForBackend(storeFacade.workspaceDB(), cfg, contractsRoot, source, mountSources, workspaceBackend)
		if err != nil {
			if out != nil {
				fmt.Fprintf(out, "fork failed: configure workspaces: %v\n", err)
			}
			return 1
		}
		result, err := runForkOwner.execute(ctx, runtimerunforkexecution.SelectedContractExecutionRequest{
			SourceRunID: strings.TrimSpace(*runID),
			At:          strings.TrimSpace(*at),
			SourceLoader: runtimerunforkexecution.ContractBundleSourceLoader{
				RepoRoot:         repo,
				PlatformSpecPath: resolvedPlatformSpecPath,
			},
			ContractSelection: runtimerunforkadmission.SelectedContractSelection(source, contractsRoot),
			AgentRuntime: runtimerunforkexecution.SelectedContractAgentRuntimeOptions{
				Config:              cfg,
				EntityStore:         stores.ToolEntityStore,
				HumanTaskStore:      stores.HumanTaskStore,
				SessionRegistry:     stores.SessionRegistry,
				ConversationStore:   stores.ConversationStore,
				ScheduleStore:       stores.ScheduleStore,
				MailboxStore:        stores.MailboxStore,
				Workspace:           workspaces,
				Credentials:         credentialStore,
				ManagedCredentials:  managedCredentialStore,
				ProviderCredentials: providerCredentialStore,
			},
		})
		if err != nil {
			if out != nil {
				fmt.Fprintf(out, "fork failed: %v\n", err)
			}
			return 1
		}
		if *asJSON {
			enc := json.NewEncoder(out)
			enc.SetIndent("", "  ")
			if err := enc.Encode(result); err != nil {
				fmt.Fprintf(out, "fork failed: encode json: %v\n", err)
				return 1
			}
			return 0
		}
		printSelectedContractExecution(out, result)
		return 0
	}
	plan, err := runForkOwner.plan(ctx, store.RunForkPlanRequest{
		SourceRunID: strings.TrimSpace(*runID),
		At:          strings.TrimSpace(*at),
	})
	if err != nil {
		if out != nil {
			fmt.Fprintf(out, "fork failed: %v\n", err)
		}
		return 1
	}
	if contracts := strings.TrimSpace(*contractsPath); contracts != "" {
		replayAdmission := store.RunForkSelectedContractReplayResumeAdmission(plan)
		if len(plan.ReplayResumeAdmission.Dispositions) > 0 || len(plan.ReplayResumeAdmission.UnsupportedBlockers) > 0 {
			plan.UnsupportedBlockers = replayAdmission.UnsupportedBlockers
			plan.UnsupportedBlockerCount = len(replayAdmission.UnsupportedBlockers)
		}
		plan.ReplayResumeAdmission = replayAdmission
		plan.ExecutionReady = replayAdmission.StateOnlyExecutionReady || replayAdmission.DeliveryEventReplayReady

		contractsRoot, err := cliapp.NormalizeContractsRoot(cliapp.ResolveContractsPath(repo, contracts))
		if err != nil {
			writeForkContractLoadError(out, "fork failed: resolve contracts", err)
			return cliapp.CLIExitValidation
		}
		_, bundle, err := cliapp.NewSwarmWorkflowModule(repo, contractsRoot, cliapp.ResolvePath(repo, *platformSpecPath))
		if err != nil {
			writeForkContractLoadError(out, "fork failed: load selected contracts", err)
			return cliapp.CLIExitValidation
		}
		source := semanticview.Wrap(bundle)
		admission, err := runtimerunforkadmission.AdmitContractFrontier(runtimerunforkadmission.ContractFrontierRequest{
			Plan:              plan,
			Source:            source,
			ContractSelection: runtimerunforkadmission.SelectedContractSelection(source, contractsRoot),
		})
		if err != nil {
			if out != nil {
				fmt.Fprintf(out, "fork failed: admit selected contract frontier: %v\n", err)
			}
			return 1
		}
		routeAdmission, err := runtimerunforkadmission.AdmitSelectedContractRouteHistory(runtimerunforkadmission.SelectedContractRouteHistoryRequest{
			Plan:              plan,
			Source:            source,
			ContractSelection: runtimerunforkadmission.SelectedContractSelection(source, contractsRoot),
			FrontierAdmission: admission,
		})
		if err != nil {
			if out != nil {
				fmt.Fprintf(out, "fork failed: admit selected contract routes: %v\n", err)
			}
			return 1
		}
		routeTopology, err := runtimerunforkexecution.BuildSelectedContractRouteTopology(runtimerunforkexecution.SelectedContractRouteTopologyRequest{
			Admission:      admission,
			RouteAdmission: routeAdmission,
		})
		if err != nil {
			if out != nil {
				fmt.Fprintf(out, "fork failed: classify selected contract route topology: %v\n", err)
			}
			return 1
		}
		executionModel, err := runtimerunforkexecution.BuildSelectedContractExecutionModel(runtimerunforkexecution.SelectedContractExecutionModelRequest{
			Admission:      admission,
			RouteAdmission: routeAdmission,
			RouteTopology:  routeTopology,
		})
		if err != nil {
			if out != nil {
				fmt.Fprintf(out, "fork failed: model selected contract execution: %v\n", err)
			}
			return 1
		}
		readiness, err := runtimerunforkexecution.BuildSelectedContractReadinessClassifier(runtimerunforkexecution.SelectedContractReadinessClassifierRequest{
			Plan:                      plan,
			ContractFrontierAdmission: admission,
			SelectedContractExecution: executionModel,
		})
		if err != nil {
			if out != nil {
				fmt.Fprintf(out, "fork failed: classify selected contract readiness: %v\n", err)
			}
			return 1
		}
		plan.ContractFrontierAdmission = &admission
		plan.SelectedContractExecution = &executionModel
		plan.SelectedContractReadiness = &readiness
	}
	if *asJSON {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		if err := enc.Encode(plan); err != nil {
			fmt.Fprintf(out, "fork failed: encode json: %v\n", err)
			return 1
		}
		return 0
	}
	printRunForkPlan(out, plan)
	return 0
}

func runForkRuntimeOwnerContext(ctx context.Context) context.Context {
	runtimeInstanceID := uuid.NewString()
	ctx = runtimecorrelation.WithRuntimeInstanceID(ctx, runtimeInstanceID)
	return runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.RuntimeScope(runtimeInstanceID))
}

func writeForkContractLoadError(out io.Writer, prefix string, err error) {
	if out == nil || err == nil {
		return
	}
	if _, ok := runtimecontracts.AsLoaderDiagnostic(err); ok {
		fmt.Fprintln(out, err)
		return
	}
	fmt.Fprintf(out, "%s: %v\n", strings.TrimSpace(prefix), err)
}

func printRunForkActivation(w io.Writer, result store.RunForkActivation) {
	if w == nil {
		return
	}
	fmt.Fprintf(w, "Fork activated for source run %s\n", result.SourceRunID)
	fmt.Fprintf(w, "Fork Run: %s status=%s\n", result.ForkRunID, result.ForkRunStatus)
	fmt.Fprintf(w, "Source Run: %s status=%s\n", result.SourceRunID, result.SourceRunStatus)
	fmt.Fprintf(w, "Fork Point: %s (%s) at %s\n", result.ForkPoint.EventName, result.ForkPoint.EventID, result.ForkPoint.Timestamp.Format(time.RFC3339Nano))
	fmt.Fprintf(w, "Summary: activated=%t source_frozen=%t replay_resume_blocked=%t materialized_entities=%d\n",
		result.Activated,
		result.SourceFrozen,
		result.ReplayResumeBlocked,
		result.MaterializedEntityCount,
	)
	if result.DeliveryEventReplay != nil {
		fmt.Fprintf(w, "Delivery/Event Replay: events=%d deliveries=%d owner=%s\n",
			result.DeliveryEventReplay.ReplayedEventCount,
			result.DeliveryEventReplay.ReplayedDeliveryCount,
			result.DeliveryEventReplay.Owner,
		)
	}
	if result.SelectedContractBinding != nil {
		fmt.Fprintf(w, "Selected Contract Binding: owner=%s contracts=%s workflow=%s@%s\n",
			result.SelectedContractBinding.Owner,
			result.SelectedContractBinding.ContractSelection.ContractsRoot,
			result.SelectedContractBinding.ContractSelection.WorkflowName,
			result.SelectedContractBinding.ContractSelection.WorkflowVersion,
		)
	}
}

func printSelectedContractExecution(w io.Writer, result runtimerunforkexecution.SelectedContractExecutionResult) {
	if w == nil {
		return
	}
	fmt.Fprintf(w, "Selected-contract fork executed for source run %s\n", result.Materialization.SourceRunID)
	fmt.Fprintf(w, "Fork Run: %s status=%s\n", result.Materialization.ForkRunID, result.Activation.ForkRunStatus)
	fmt.Fprintf(w, "Source Run: %s status=%s\n", result.Activation.SourceRunID, result.Activation.SourceRunStatus)
	fmt.Fprintf(w, "Owner: %s\n", result.Owner)
	fmt.Fprintf(w, "Summary: executed_events=%d materialized_entities=%d source_frozen=%t\n",
		result.ExecutedEventCount,
		result.Materialization.MaterializedEntityCount,
		result.Activation.SourceFrozen,
	)
	if result.Activation.BranchDivergence != nil {
		fmt.Fprintf(w, "Selected Contract Branch: owner=%s policy=%s source_frozen=%t source_status=%s facts=%s\n",
			result.Activation.BranchDivergence.Owner,
			result.Activation.BranchDivergence.Policy,
			result.Activation.BranchDivergence.SourceFrozen,
			result.Activation.BranchDivergence.SourceRunStatusAfterActivation,
			strings.Join(result.Activation.BranchDivergence.SourceAdvancedFacts, ","),
		)
	}
	if len(result.ForkEvents) > 0 {
		fmt.Fprintln(w, "Fork Events:")
		for _, event := range result.ForkEvents {
			fmt.Fprintf(w, "  %s -> %s %s\n", event.SourceEventID, event.ForkEventID, event.EventName)
		}
	}
}

func printRunForkMaterialization(w io.Writer, result store.RunForkMaterialization) {
	if w == nil {
		return
	}
	fmt.Fprintf(w, "Fork materialized for run %s\n", result.SourceRunID)
	fmt.Fprintf(w, "Fork Run: %s status=%s\n", result.ForkRunID, result.ForkRunStatus)
	fmt.Fprintf(w, "Fork Point: %s (%s) at %s\n", result.ForkPoint.EventName, result.ForkPoint.EventID, result.ForkPoint.Timestamp.Format(time.RFC3339Nano))
	fmt.Fprintf(w, "Summary: materialized_entities=%d delivery_resume_blocked=%t source_status_unchanged=%t\n",
		result.MaterializedEntityCount,
		result.DeliveryResumeBlocked,
		result.SourceRunStatusUnchanged,
	)
	if result.SelectedContractBinding != nil {
		fmt.Fprintf(w, "Selected Contract Binding: owner=%s contracts=%s workflow=%s@%s\n",
			result.SelectedContractBinding.Owner,
			result.SelectedContractBinding.ContractSelection.ContractsRoot,
			result.SelectedContractBinding.ContractSelection.WorkflowName,
			result.SelectedContractBinding.ContractSelection.WorkflowVersion,
		)
	}
}

func printRunForkPlan(w io.Writer, plan store.RunForkPlan) {
	if w == nil {
		return
	}
	fmt.Fprintf(w, "Fork plan for run %s\n", plan.SourceRunID)
	if strings.TrimSpace(plan.SourceRunStatus) != "" {
		fmt.Fprintf(w, "Source Status: %s\n", plan.SourceRunStatus)
	}
	fmt.Fprintf(w, "Fork Point: %s (%s) at %s\n", plan.ForkPoint.EventName, plan.ForkPoint.EventID, plan.ForkPoint.Timestamp.Format(time.RFC3339Nano))
	fmt.Fprintf(w, "Summary: events_at_fork=%d reconstructed_entities=%d pending_work=%d unsupported_blockers=%d execution_ready=%t\n",
		plan.EventCountAtFork,
		plan.ReconstructedEntityCount,
		plan.PendingWorkCount,
		plan.UnsupportedBlockerCount,
		plan.ExecutionReady,
	)
	if len(plan.UnsupportedBlockers) > 0 {
		fmt.Fprintln(w, "Unsupported Blockers:")
		for _, blocker := range plan.UnsupportedBlockers {
			fmt.Fprintf(w, "  %s: %s\n", blocker.Code, blocker.Message)
		}
	}
	if len(plan.PendingWork) > 0 {
		fmt.Fprintln(w, "Pending Work:")
		for _, item := range plan.PendingWork {
			fmt.Fprintf(w, "  %s %s subscriber=%s/%s status=%s class=%s\n",
				item.EventID,
				item.EventName,
				item.SubscriberType,
				item.SubscriberID,
				item.Status,
				item.Classification,
			)
		}
	}
	if plan.ContractFrontierAdmission != nil {
		admission := plan.ContractFrontierAdmission
		fmt.Fprintf(w, "Contract Frontier Admission: owner=%s non_mutating=%t frontier_events=%d historical_execution_supported=%t\n",
			admission.Owner,
			admission.NonMutating,
			admission.FrontierEventCount,
			admission.HistoricalExecutionSupported,
		)
	}
	if plan.SelectedContractExecution != nil {
		model := plan.SelectedContractExecution
		fmt.Fprintf(w, "Selected Contract Execution Model: owner=%s non_mutating=%t execution_supported=%t future_owner=%s\n",
			model.Owner,
			model.NonMutating,
			model.ExecutionSupported,
			model.FutureExecutionOwner,
		)
	}
	if plan.SelectedContractReadiness != nil {
		readiness := plan.SelectedContractReadiness
		fmt.Fprintf(w, "Selected Contract Readiness: owner=%s non_mutating=%t facts=%d future_owner=%s\n",
			readiness.Owner,
			readiness.NonMutating,
			len(readiness.FactMatrix),
			readiness.FutureExecutionOwner,
		)
	}
}

func asString(v any) string {
	switch typed := v.(type) {
	case string:
		return typed
	case []byte:
		return string(typed)
	default:
		return ""
	}
}
