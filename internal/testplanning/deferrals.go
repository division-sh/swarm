package testplanning

import "strings"

// These are finite non-credit roots, not an inventory of all tests. Any newly
// selected root defaults to a required PASS unless it has an explicit reason.
var rootDeferralReasons = map[string]map[string]string{
	"internal/packartifact": {
		"TestProjectPackImportSubprocessHelper": "subprocess entry point, not a standalone proof",
	},
	"internal/releasee2e": {
		"TestWorkspaceMCPCompiledDockerConformance":                  "real Docker proof required separately by the workspace-image CI job; no skip credit",
		"TestWorkspaceMCPCompiledDockerDoctorAndLiveBootRefusal":     "real Docker proof required separately by the workspace-image CI job; no skip credit",
		"TestClaudeCLIPositiveSelectorAgainstInstalledWorkspace":     "separately provisioned live selector proof",
		"TestClaudeCLIPaidAgenticLifecycleFromReleaseBinaryDefaults": "separately provisioned paid agentic proof",
		"TestCommandLiveServeAndRestartParity":                       "separately provisioned live command proof",
		"TestCommandLiveUneditedScaffoldReadiness":                   "separately provisioned live scaffold proof",
	},
	"internal/runtime/cataloge2e": {
		"TestTier11Probe": "opt-in diagnostic duplicate, not catalog completion credit",
	},
	"internal/runtime/llm": {
		"TestClaudeCLIFirstTurnFakeDockerHelper": "subprocess entry point, not a standalone proof",
	},
	"internal/runtime/runforkexecution": {
		"TestSelectedWorkspacePreparationRealDockerBothStores": "opt-in real Docker proof",
		"TestSelectedForkCrashProcessHelper":                   "subprocess entry point, not a standalone proof",
	},
	"internal/runtime/testfixtures/canonicalrouting": {
		"TestCanonicalRoutingTrackedSplitsRemainOpen": "separately provisioned remote tracker check",
	},
	"internal/runtime/workspace": {
		"TestWorkerRealDockerIdentityReuseAndCancellationJoin":           "real Docker proof required separately by the workspace-image CI job; no skip credit",
		"TestWorkerRealDockerHTTPDeadlineJoinsGatewayRequest":            "real Docker proof required separately by the workspace-image CI job; no skip credit",
		"TestWorkerRealDockerLostClientJoinsGatewayRequest":              "real Docker proof required separately by the workspace-image CI job; no skip credit",
		"TestWorkerRealDockerLostClientAfterToolCommitDoesNotReplay":     "real Docker proof required separately by the workspace-image CI job; no skip credit",
		"TestWorkerRealDockerCallDeadlineKeepsCommittedOutcomeUncertain": "real Docker proof required separately by the workspace-image CI job; no skip credit",
		"TestWorkerRealDockerLostClientRetainsCleanupFailure":            "real Docker proof required separately by the workspace-image CI job; no skip credit",
		"TestWorkerRealDockerLostClientPreservesSiblingAndSource":        "real Docker proof required separately by the workspace-image CI job; no skip credit",
		"TestClaudeStateDockerRetentionAndRefusal":                       "opt-in real Docker proof",
		"TestClaudeStateDockerSharedWorkspaceIsolation":                  "opt-in real Docker proof",
		"TestResetContainerIntentRealDocker":                             "opt-in real Docker proof",
	},
	"internal/serveapp": {
		"TestMockNormalRealDockerEmissionBothStores":                    "opt-in real Docker retained H emission proof; both-store execution is qualified separately",
		"TestChannelOnboardingCrashServeProcessHelper":                  "subprocess entry point, not a standalone proof",
		"TestLifecycleDiagnosticServeProcessHelper":                     "subprocess entry point, not a standalone proof",
		"TestMailboxCompletionServeProcessHelper":                       "subprocess entry point, not a standalone proof",
		"TestOwnedMockLifecycleProcessEntry":                            "subprocess entry point, not a standalone proof",
		"TestOwnedNumericTimerInspection":                               "numeric lifecycle parent-owned subprocess entry point, not a standalone proof",
		"TestResetCrashServeProcessHelper":                              "subprocess entry point, not a standalone proof",
		"TestClaudeAttemptProofProcessHelper":                           "subprocess entry point, not a standalone proof",
		"TestResetStartupProjectionDisposalRealDocker":                  "opt-in real Docker proof",
		"TestServePostgresLossAndRestartFromDurableState":               "separately provisioned disposable PostgreSQL server-loss proof",
		"TestServePostgresLostCommitResponseRecoversDurablePublication": "separately provisioned disposable PostgreSQL server-loss proof",
		"TestServePostgresRemotePossessionOutlivesLocalLoss":            "separately provisioned disposable PostgreSQL server-loss proof",
		"TestServePostgresSilentMonitorWithdrawsReadinessAndJoins":      "separately provisioned disposable PostgreSQL server-loss proof",
		"TestShopifyLocalProviderToolSmoke":                             "opt-in Shopify provider-tool proof",
		"TestTypeformManualLiveHTTPSWebhookSmoke":                       "opt-in Typeform live webhook proof",
	},
	"internal/testpostgres": {
		"TestManagerCreateBeforeMetadataCrashHelper":           "subprocess entry point, not a standalone proof",
		"TestManagerTemplateCloneProcessHelper":                "subprocess entry point, not a standalone proof",
		"TestSwarmTestDockerRunnersQueueBeforeSecondProvision": "unwrapped Docker runner topology only",
	},
}

func deferredRootReason(unit ProofUnit, root TestRoot, build BuildContext) (reason string, profileReplacement bool) {
	if build.GOOS == "windows" {
		switch root.Package + "\x00" + root.Name {
		case "github.com/division-sh/swarm/cmd/swarm-test-changed\x00TestFullSuiteFallbackExecutesCanonicalWrapper":
			return "Unix-only shell probe", false
		case "github.com/division-sh/swarm/internal/testpostgres\x00TestRunAndServiceLeasesAttachExactAuthorityToChild":
			return "Unix-only process authority proof", false
		}
	}
	if root.Package == "github.com/division-sh/swarm/internal/releasee2e" {
		switch root.Name {
		case "TestGoldenAgentWorkloadSQLiteSmoke":
			if unit.WorkloadProfile == ProfileFull {
				return "full continuous workload supersedes PR SQLite smoke", true
			}
		case "TestCompiledProcessFullLifecycleSQLiteSmoke":
			if TierRank(unit.WorkloadProfile) >= TierRank(ProfileLifecycle) {
				return "complete J1-J5 workload supersedes J1 smoke", true
			}
		case "TestGoldenAgentWorkloadRestartAndForcedKillOnBothBackends",
			"TestCompiledProcessFullLifecycleJourneysSQLitePostgres":
			if unit.WorkloadProfile == ProfileCore {
				return "full continuous workload is replaced by PR N=2/J1 smoke", true
			}
		case "TestGoldenAgentWorkloadBurstConcurrencyOnBothBackendsIteration1", "TestGoldenAgentWorkloadBurstConcurrencyOnBothBackendsIteration2":
			if unit.WorkloadProfile != ProfileFull {
				return "N=10 race bursts require full tier", true
			}
		}
	}
	const module = "github.com/division-sh/swarm/"
	if !strings.HasPrefix(root.Package, module) {
		return "", false
	}
	return rootDeferralReasons[strings.TrimPrefix(root.Package, module)][root.Name], false
}
