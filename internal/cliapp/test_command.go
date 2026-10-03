package cliapp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cli/argcount"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/runtime"
	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	runtimeeventschema "github.com/division-sh/swarm/internal/runtime/eventschema"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/scenarioderivation"
	"github.com/division-sh/swarm/internal/runtime/scenariodocument"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
)

const (
	scenarioTestSetupEntitiesMethod = "test.setup_entities"

	scenarioTestExitValidation = 2
	scenarioTestExitRuntime    = 3
	scenarioTestExitAuth       = 4
	scenarioTestExitNotFound   = 5
	scenarioTestExitRejected   = 6

	defaultScenarioTestTimeout = 30 * time.Second
	defaultScenarioTestPoll    = 250 * time.Millisecond
)

type scenarioTestCommandOptions struct {
	apiOptions   rootCommandOptions
	contracts    string
	timeout      time.Duration
	pollInterval time.Duration
	derive       string
	input        string
	allInputs    bool
}

type scenarioTestFile struct {
	Path     string
	FlowID   string
	Raw      []byte
	Document *scenariodocument.Document
}

type preparedScenario struct {
	file      scenarioTestFile
	document  scenarioDocument
	evaluator *scenarioExpressionEvaluator
	execution *scenarioexecution.Selector
}

type scenarioDocument = scenariodocument.CLI
type scenarioSetup = scenariodocument.Setup
type scenarioSetupEntity = scenariodocument.SetupEntity
type scenarioStep = scenariodocument.Step

type generatedInputFixturePlan struct {
	flowID          string
	pinName         string
	eventKey        string
	schemaDigest    string
	identity        string
	canonicalSchema json.RawMessage
	payload         json.RawMessage
}

type scenarioExpect = scenariodocument.Expect
type scenarioEventExpect = scenariodocument.EventExpect
type scenarioEntityExpect = scenariodocument.EntityExpect
type scenarioInvalid = scenariodocument.Invalid
type scenarioInvalidCase = scenariodocument.InvalidCase

type scenarioRunState struct {
	RunID         string
	LastEventID   string
	SetupEntities map[string]scenarioSetupEntityBinding
}

type scenarioSetupEntityBinding struct {
	Alias        string
	EntityID     string
	FlowInstance string
	EntityType   string
	CurrentState string
}

type testSetupEntitiesResult struct {
	RunID    string                         `json:"run_id"`
	Entities []testSetupEntityBindingResult `json:"entities"`
}

type testSetupEntityBindingResult struct {
	Alias        string `json:"alias"`
	EntityID     string `json:"entity_id"`
	FlowInstance string `json:"flow_instance,omitempty"`
	EntityType   string `json:"entity_type"`
	CurrentState string `json:"current_state"`
}

type scenarioTestValidationError struct {
	err error
}

func (e scenarioTestValidationError) Error() string {
	if e.err == nil {
		return ""
	}
	return e.err.Error()
}

func (e scenarioTestValidationError) Unwrap() error {
	return e.err
}

type scenarioRunner struct {
	client                  *cliAPIClient
	bundle                  *runtimecontracts.WorkflowContractBundle
	source                  semanticview.Source
	bundleHash              string
	sourceRoot              string
	timeout                 time.Duration
	pollInterval            time.Duration
	out                     io.Writer
	effectiveSourceIdentity scenarioexecution.EffectiveSourceIdentity
	scenarioExecution       *scenarioexecution.Selector
}

type scenarioExpressionEvaluator = scenariodocument.Evaluator

func newTestCommand(root InvocationRoot, opts rootCommandOptions) *cobra.Command {
	testOpts := scenarioTestCommandOptions{
		apiOptions:   opts,
		timeout:      defaultScenarioTestTimeout,
		pollInterval: defaultScenarioTestPoll,
	}
	cmd := &cobra.Command{
		Use:   "test [directory] [scenario-label]",
		Short: "Run deterministic scenario tests through public read owners.",
		Args:  argcount.RangeArgs(0, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if cliAPIConnectionFlagsChanged(cmd) {
				return returnScenarioTestValidationError(cmd.ErrOrStderr(), fmt.Errorf("swarm test owns a fresh private session; --api-server, --context and --api-token-file are not supported"))
			}
			var scenarios []string
			if len(args) > 0 {
				testOpts.contracts = args[0]
			}
			if len(args) > 1 {
				scenarios = args[1:]
			}
			return runScenarioTestCommand(cmd.Context(), root.Path(), cmd.OutOrStdout(), cmd.ErrOrStderr(), scenarios, testOpts)
		},
	}
	cmd.Flags().DurationVar(&testOpts.timeout, "timeout", defaultScenarioTestTimeout, "Safety deadline for test quiescence")
	cmd.Flags().DurationVar(&testOpts.pollInterval, "poll-interval", defaultScenarioTestPoll, "Canonical readback polling interval while waiting for quiescence")
	cmd.Flags().StringVar(&testOpts.derive, "derive", "", "Derive and run a scenario for an exact flow")
	cmd.Flags().StringVar(&testOpts.input, "input", "", "Exact public input pin for derived mode")
	cmd.Flags().BoolVar(&testOpts.allInputs, "all-inputs", false, "Derive one scenario for every public input of the selected flow")
	bindCLIAPIConnectionFlagsWithClass(cmd, &testOpts.apiOptions, cliAPICommandClassMutating, "swarm test")
	return cmd
}

func runScenarioTestCommand(ctx context.Context, RepoRoot string, out, errOut io.Writer, args []string, opts scenarioTestCommandOptions) error {
	if opts.timeout <= 0 {
		return returnScenarioTestValidationError(errOut, fmt.Errorf("--timeout must be positive"))
	}
	if opts.pollInterval <= 0 {
		return returnScenarioTestValidationError(errOut, fmt.Errorf("--poll-interval must be positive"))
	}
	configPath := ""
	if opts.apiOptions.rootFlags != nil && opts.apiOptions.rootFlags.configPathSet {
		configPath = opts.apiOptions.rootFlags.configPath
	}
	configResult, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: RepoRoot, ExplicitPath: configPath})
	if err != nil {
		return returnScenarioTestValidationError(errOut, fmt.Errorf("load runtime config: %w", err))
	}
	sourceRoot, platformSpec, err := resolveScenarioTestSources(RepoRoot, opts.contracts, configResult.cli)
	if err != nil {
		return returnScenarioTestValidationError(errOut, err)
	}
	deriveFlow := strings.Trim(strings.TrimSpace(opts.derive), "/")
	if deriveFlow == "" && (strings.TrimSpace(opts.input) != "" || opts.allInputs) {
		return returnScenarioTestValidationError(errOut, fmt.Errorf("--input and --all-inputs require --derive"))
	}
	if deriveFlow != "" && len(args) > 0 {
		return returnScenarioTestValidationError(errOut, fmt.Errorf("--derive cannot be combined with scenario-file arguments"))
	}
	platformPackBase, err := LoadConfiguredPlatformPackBase(RepoRoot, configResult)
	if err != nil {
		return returnScenarioTestValidationError(errOut, fmt.Errorf("load platform pack base: %w", err))
	}
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOptions(RepoRoot, sourceRoot, platformSpec, runtimecontracts.WorkflowContractLoadOptions{
		PlatformPackBase: platformPackBase, AdmitPackInventory: packadmission.AdmitInventory,
	})
	if err != nil {
		return returnScenarioTestValidationError(errOut, fmt.Errorf("load contract bundle: %w", err))
	}
	var files []scenarioTestFile
	if deriveFlow == "" {
		files, err = discoverScenarioTestFiles(bundle, args)
		if err != nil {
			return returnScenarioTestValidationError(errOut, err)
		}
		if len(files) == 0 {
			return returnScenarioTestValidationError(errOut, fmt.Errorf("no scenario files found in an admitted tests/ resource branch"))
		}
	}
	if deriveFlow == "" {
		files, err = prepareScenarioTestFiles(files)
		if err != nil {
			return returnScenarioTestValidationError(errOut, err)
		}
	}
	metadata, err := packadmission.FromBundle(bundle)
	if err != nil {
		return returnScenarioTestValidationError(errOut, fmt.Errorf("admit pack metadata: %w", err))
	}
	bundleHash, err := runtimecontracts.BundleHash(bundle)
	if err != nil {
		return returnScenarioTestValidationError(errOut, err)
	}
	sourceFact, err := runtimecorrelation.NewSourceArtifactFact(bundleHash)
	if err != nil {
		return returnScenarioTestValidationError(errOut, err)
	}
	projection, err := runtime.AdmitEffectiveSourceProjection(runtime.EffectiveSourceProjectionRequest{
		Source: semanticview.Wrap(bundle), SourceArtifactFact: sourceFact,
		ProviderTriggerCatalog: metadata.ProviderTriggers, ChannelPlans: metadata.ChannelPlans,
	})
	if err != nil {
		return returnScenarioTestValidationError(errOut, fmt.Errorf("admit effective source: %w", err))
	}
	source := projection.Source()
	validation := runtime.StructuralWorkflowContractValidationOptions()
	validation.AllowHarnessInputs, validation.AllowHarnessOutputs = true, true
	validation.ModelAliases = configResult.Config.LLM.Models
	validation.ProviderTriggerCatalog, validation.ChannelPlans = metadata.ProviderTriggers, metadata.ChannelPlans
	if _, err := runtime.ValidateWorkflowContractSurface(ctx, source, validation); err != nil {
		return returnScenarioTestValidationError(errOut, err)
	}
	profile, err := configResult.Config.LLMBackendProfile()
	if err != nil {
		return returnScenarioTestValidationError(errOut, err)
	}
	if _, err := runtimebootverify.PrepareSourceBootEffectContext(source, profile, executionposture.MockOnly); err != nil {
		return returnScenarioTestValidationError(errOut, fmt.Errorf("test completeness: %w", err))
	}
	var plans []scenarioderivation.Plan
	if deriveFlow != "" {
		plans, err = scenarioderivation.Compile(source, projection.Identity(), scenarioderivation.Request{
			FlowID: deriveFlow, Input: opts.input, AllInputs: opts.allInputs,
		})
		if err != nil {
			return returnScenarioTestValidationError(errOut, err)
		}
	}
	runner := scenarioRunner{
		bundle: bundle, source: source, bundleHash: bundleHash,
		sourceRoot: sourceRoot, timeout: opts.timeout, pollInterval: opts.pollInterval,
		out: out, effectiveSourceIdentity: projection.Identity(),
	}
	prepared := make([]preparedScenario, 0, len(files))
	for _, file := range files {
		scenario, err := runner.prepareScenario(file)
		if err != nil {
			return returnScenarioTestValidationError(errOut, err)
		}
		prepared = append(prepared, scenario)
	}
	if opts.apiOptions.runTest == nil {
		return returnScenarioTestValidationError(errOut, fmt.Errorf("private test session runner is unavailable"))
	}
	err = opts.apiOptions.runTest(ctx, TestSessionRequest{
		Bundle: bundle, SourceRoot: sourceRoot, PlatformSpecPath: platformSpec,
		PlatformPackBase: platformPackBase, LiveBackend: profile.ID, ModelAliases: configResult.Config.LLM.Models,
	}, func(sessionCtx context.Context, endpoint TestSessionEndpoint) error {
		rpcEndpoint, err := cliAPIRPCEndpointFromServer(endpoint.APIServer, "private test session")
		if err != nil {
			return err
		}
		if !cliAPIRPCEndpointAllowsDefaultToken(rpcEndpoint) || endpoint.Token == "" {
			return fmt.Errorf("private test session requires a numeric loopback endpoint and explicit token")
		}
		client := &cliAPIClient{endpoint: rpcEndpoint, token: endpoint.Token, httpClient: opts.apiOptions.httpClient}
		if client.httpClient == nil {
			client.httpClient = http.DefaultClient
		}
		if _, err := scenarioTestSourceArtifactFact(sessionCtx, client, bundleHash); err != nil {
			return err
		}
		runner.client = client
		for _, plan := range plans {
			if err := runner.runDerivedPlan(sessionCtx, plan); err != nil {
				return err
			}
		}
		for _, scenario := range prepared {
			if err := runner.runPreparedScenario(sessionCtx, scenario); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		writeCLIAPIError(errOut, err)
		return commandExitError{code: scenarioTestAPIErrorExitCode(err)}
	}
	fmt.Fprintf(out, "swarm test ok: scenarios=%d\n", len(files)+len(plans))
	return nil
}

func scenarioTestSourceArtifactFact(ctx context.Context, client *cliAPIClient, bundleHash string) (runtimecorrelation.SourceArtifactFact, error) {
	if client == nil {
		return runtimecorrelation.SourceArtifactFact{}, fmt.Errorf("runtime API client is required for scenario source identity")
	}
	var identity apiv1.RuntimeIdentityResult
	if err := client.call(ctx, "runtime.identity", map[string]any{}, &identity); err != nil {
		return runtimecorrelation.SourceArtifactFact{}, err
	}
	var matched runtimecorrelation.SourceArtifactFact
	matchedSet := false
	seen := make(map[string]struct{}, len(identity.SourceArtifacts))
	available := make([]string, 0, len(identity.SourceArtifacts))
	for i := range identity.SourceArtifacts {
		candidate := identity.SourceArtifacts[i]
		fact, err := runtimecorrelation.DecodeSourceArtifactFact(candidate.BundleHash)
		if err != nil {
			return runtimecorrelation.SourceArtifactFact{}, fmt.Errorf("target runtime returned invalid source fact at source_artifacts[%d]: %w", i, err)
		}
		canonicalHash := fact.BundleHash()
		if _, duplicate := seen[canonicalHash]; duplicate {
			return runtimecorrelation.SourceArtifactFact{}, fmt.Errorf("runtime identity returned duplicate source facts for source %s", humanSourceIdentity(canonicalHash, ""))
		}
		seen[canonicalHash] = struct{}{}
		available = append(available, canonicalHash)
		if canonicalHash != bundleHash {
			continue
		}
		matched = fact
		matchedSet = true
	}
	if !matchedSet {
		sort.Strings(available)
		labels := make([]string, len(available))
		for i, hash := range available {
			labels[i] = humanSourceIdentity(hash, "")
		}
		return runtimecorrelation.SourceArtifactFact{}, fmt.Errorf("target runtime does not serve source %s (available: %s); serve its matching source directory first", humanSourceIdentity(bundleHash, ""), strings.Join(labels, ", "))
	}
	return matched, nil
}

func resolveScenarioTestSources(RepoRoot, sourceArgument string, cfg cliCommandConfig) (string, string, error) {
	var err error
	RepoRoot, err = requireInvocationRootPath(RepoRoot)
	if err != nil {
		return "", "", err
	}
	sourceRoot, err := ResolveSourceRoot(RepoRoot, sourceArgument)
	if err != nil {
		return "", "", err
	}
	platformSpec := strings.TrimSpace(cfg.Paths.PlatformSpecPath)
	if platformSpec == "" {
		platformSpec, err = EmbeddedPlatformSpecPath()
		if err != nil {
			return "", "", fmt.Errorf("resolve embedded platform spec: %w", err)
		}
	}
	return sourceRoot, ResolvePath(RepoRoot, platformSpec), nil
}

func discoverScenarioTestFiles(bundle *runtimecontracts.WorkflowContractBundle, args []string) ([]scenarioTestFile, error) {
	if bundle == nil || bundle.SourceArtifact == nil {
		return nil, fmt.Errorf("scenario discovery requires an admitted source artifact")
	}
	byLabel, err := scenarioTestFilesByLabel(bundle.SourceArtifact)
	if err != nil {
		return nil, err
	}
	if len(args) > 0 {
		out := make([]scenarioTestFile, 0, len(args))
		for _, arg := range args {
			label, err := normalizeScenarioLabel(arg)
			if err != nil {
				return nil, err
			}
			file, ok := byLabel[label]
			if !ok {
				return nil, fmt.Errorf("scenario file %q is not an admitted YAML member of a tests/ resource branch", arg)
			}
			out = append(out, file)
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
		return out, nil
	}
	out := make([]scenarioTestFile, 0, len(byLabel))
	for _, file := range byLabel {
		document, found, err := scenariodocument.Discover(file.Raw, file.Path)
		if err != nil {
			return nil, err
		}
		if found {
			file.Document = &document
			out = append(out, file)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out, nil
}

func scenarioTestFilesByLabel(artifact *sourceartifact.AdmittedSourceArtifact) (map[string]scenarioTestFile, error) {
	root := artifact.Root()
	if root == nil {
		return nil, fmt.Errorf("scenario discovery requires an admitted flow tree")
	}
	byLabel := map[string]scenarioTestFile{}
	var visit func(*sourceartifact.FlowNode) error
	visit = func(flow *sourceartifact.FlowNode) error {
		for _, label := range flow.Resources("tests") {
			ext := strings.ToLower(path.Ext(label))
			if ext != ".yaml" && ext != ".yml" {
				continue
			}
			entry, ok := artifact.Entry(label)
			if !ok {
				return fmt.Errorf("scenario resource %q is missing from its admitted source artifact", label)
			}
			byLabel[label] = scenarioTestFile{Path: label, FlowID: flow.Path(), Raw: entry.Bytes()}
		}
		for _, child := range flow.Children() {
			if err := visit(child); err != nil {
				return err
			}
		}
		return nil
	}
	if err := visit(root); err != nil {
		return nil, err
	}
	return byLabel, nil
}

func normalizeScenarioLabel(raw string) (string, error) {
	if filepath.IsAbs(raw) || strings.Contains(raw, "\\") {
		return "", fmt.Errorf("scenario file %q must be a selected-root-relative artifact label", raw)
	}
	label := path.Clean(strings.TrimSpace(raw))
	if label == "." || label == ".." || strings.HasPrefix(label, "../") || label != strings.TrimSpace(raw) {
		return "", fmt.Errorf("scenario file %q must be one normalized selected-root-relative artifact label", raw)
	}
	return label, nil
}

func (r scenarioRunner) prepareScenario(file scenarioTestFile) (preparedScenario, error) {
	doc, err := scenarioDocumentForFile(file)
	if err != nil {
		return preparedScenario{}, fmt.Errorf("%s: %w", file.Path, err)
	}
	seed, err := r.scenarioEvaluatorSeed(file, doc)
	if err != nil {
		return preparedScenario{}, err
	}
	evaluator, err := newScenarioExpressionEvaluator(seed, doc.Vars)
	if err != nil {
		return preparedScenario{}, fmt.Errorf("%s: %w", file.Path, err)
	}
	if doc.Derive != nil {
		file, doc, r.scenarioExecution, err = r.prepareAuthoredDerivedScenario(file, doc, evaluator)
		if err != nil {
			return preparedScenario{}, fmt.Errorf("%s: %w", file.Path, err)
		}
	}
	if doc.Invalid != nil {
		if err := r.runInvalidVariants(file, doc, evaluator); err != nil {
			return preparedScenario{}, err
		}
	}
	doc, err = scenariodocument.PrepareCLI(doc, evaluator, func(label string) (semanticvalue.Value, error) { return r.loadFixturePayload(file.Path, label) })
	if err != nil {
		return preparedScenario{}, fmt.Errorf("%s: %w", file.Path, err)
	}
	if err := r.validatePreparedScenario(file, doc, evaluator); err != nil {
		return preparedScenario{}, err
	}
	return preparedScenario{file: file, document: doc, evaluator: evaluator, execution: r.scenarioExecution}, nil
}

func scenarioDocumentForFile(file scenarioTestFile) (scenarioDocument, error) {
	if file.Document != nil {
		return file.Document.Projection()
	}
	if len(file.Raw) == 0 {
		return scenarioDocument{}, fmt.Errorf("admitted scenario bytes are missing")
	}
	admitted, err := scenariodocument.Admit(file.Raw, file.Path)
	if err != nil {
		return scenarioDocument{}, err
	}
	return admitted.Projection()
}

func (r scenarioRunner) prepareAuthoredDerivedScenario(file scenarioTestFile, doc scenarioDocument, evaluator *scenarioExpressionEvaluator) (scenarioTestFile, scenarioDocument, *scenarioexecution.Selector, error) {
	declaration, err := scenarioderivation.MaterializeDeclaration(*doc.Derive, evaluator)
	if err != nil {
		return file, doc, nil, err
	}
	plans, err := scenarioderivation.Compile(r.source, r.effectiveSourceIdentity, scenarioderivation.Request{
		FlowID: declaration.FlowID, Input: declaration.Input, Set: declaration.Set,
		ProfileID: declaration.Name, Responses: declaration.ConnectorResponses,
	})
	if err != nil {
		return file, doc, nil, err
	}
	payload, err := materializeScenarioSemanticPayload(plans[0].Payload)
	if err != nil {
		return file, doc, nil, err
	}
	data, err := scenariodocument.Materialize(payload)
	if err != nil {
		return file, doc, nil, err
	}
	doc.Steps = []scenarioStep{{Action: "publish", PublishEvent: plans[0].EventKey, Payload: data}}
	file.FlowID = plans[0].FlowID
	selector, err := scenarioexecution.NewSelector(plans[0].Profile)
	if err != nil {
		return file, doc, nil, err
	}
	addDerivedGenericOracle(&doc.Expect, plans[0].EventKey)
	return file, doc, &selector, nil
}

func (r scenarioRunner) validatePreparedScenario(file scenarioTestFile, doc scenarioDocument, evaluator *scenarioExpressionEvaluator) error {
	for _, entity := range doc.Setup.Entities {
		if _, err := r.evaluateScenarioSetupEntity(file, evaluator, entity); err != nil {
			return fmt.Errorf("%s: setup: %w", file.Path, err)
		}
	}
	for i, step := range doc.Steps {
		if err := r.validatePreparedStep(file, evaluator, step); err != nil {
			return fmt.Errorf("%s: step %d: %w", file.Path, i+1, err)
		}
	}
	for _, expectation := range doc.Expect.Entities {
		if _, err := evaluateScenarioEntityDetail(expectation, evaluator); err != nil {
			return fmt.Errorf("%s: expect.entities: %w", file.Path, err)
		}
	}
	return nil
}

func (r scenarioRunner) validatePreparedStep(file scenarioTestFile, evaluator *scenarioExpressionEvaluator, step scenarioStep) error {
	if step.Action == "publish" {
		_, _, err := r.buildPublishPayload(file, evaluator, step)
		return err
	}
	if _, _, err := evaluateScenarioCardMatch(evaluator, "", step.Match); err != nil {
		return err
	}
	if step.Action == "mailbox.defer" {
		until, err := evaluator.Evaluate(step.Until)
		if err != nil {
			return err
		}
		if _, err := time.Parse(time.RFC3339, optionalScenarioString(until)); err != nil {
			return fmt.Errorf("mailbox.defer until must be RFC3339: %w", err)
		}
	}
	return nil
}

func (r scenarioRunner) runPreparedScenario(ctx context.Context, prepared preparedScenario) error {
	file, doc, evaluator := prepared.file, prepared.document, prepared.evaluator
	r.scenarioExecution = prepared.execution
	state := &scenarioRunState{SetupEntities: map[string]scenarioSetupEntityBinding{}}
	if len(doc.Setup.Entities) > 0 {
		if err := r.runScenarioSetup(ctx, file, evaluator, state, doc.Setup); err != nil {
			return fmt.Errorf("%s: setup: %w", file.Path, err)
		}
	}
	for i, step := range doc.Steps {
		if err := r.runScenarioStep(ctx, file, evaluator, state, step); err != nil {
			return fmt.Errorf("%s: step %d: %w", file.Path, i+1, err)
		}
		if state.RunID != "" {
			if err := r.waitForQuiescence(ctx, state.RunID); err != nil {
				return fmt.Errorf("%s: step %d: %w", file.Path, i+1, err)
			}
		}
	}
	if state.RunID != "" {
		if err := r.waitForQuiescence(ctx, state.RunID); err != nil {
			return fmt.Errorf("%s: %w", file.Path, err)
		}
		if !doc.Expect.Empty() {
			if err := r.evaluateExpectations(ctx, state, evaluator, doc.Expect); err != nil {
				return fmt.Errorf("%s: %w", file.Path, err)
			}
		}
	}
	fmt.Fprintf(r.out, "scenario ok: %s\n", file.Path)
	return nil
}

func prepareScenarioTestFiles(files []scenarioTestFile) ([]scenarioTestFile, error) {
	prepared := append([]scenarioTestFile(nil), files...)
	for i := range prepared {
		if len(prepared[i].Raw) == 0 {
			return nil, fmt.Errorf("%s: admitted scenario bytes are missing", prepared[i].Path)
		}
		doc, err := scenariodocument.Admit(prepared[i].Raw, prepared[i].Path)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", prepared[i].Path, err)
		}
		prepared[i].Document = &doc
	}
	return prepared, nil
}

func (r scenarioRunner) runDerivedPlan(ctx context.Context, plan scenarioderivation.Plan) error {
	payload, err := materializeScenarioSemanticPayload(plan.Payload)
	if err != nil {
		return scenarioTestValidationError{err: fmt.Errorf("materialize derived payload: %w", err)}
	}
	selector, err := scenarioexecution.NewSelector(plan.Profile)
	if err != nil {
		return scenarioTestValidationError{err: fmt.Errorf("select scenario execution profile: %w", err)}
	}
	r.scenarioExecution = &selector
	defer func() { r.scenarioExecution = nil }()
	file := scenarioTestFile{Path: path.Join("tests", "derived-"+scenarioSHA40(plan.FlowID+"\x00"+plan.PinName)+".yaml"), FlowID: plan.FlowID}
	data, err := scenariodocument.Materialize(payload)
	if err != nil {
		return scenarioTestValidationError{err: err}
	}
	doc := scenarioDocument{
		Name:  "derived:" + plan.FlowID + "/" + plan.PinName,
		Steps: []scenarioStep{{Action: "publish", PublishEvent: plan.EventKey, Payload: data}},
	}
	addDerivedGenericOracle(&doc.Expect, plan.EventKey)
	seed := strings.Join([]string{scenarioderivation.PlanVersion, r.effectiveSourceIdentity.Digest(), plan.FlowID, plan.PinName}, "\x00")
	evaluator, err := newScenarioExpressionEvaluator(seed, nil)
	if err != nil {
		return scenarioTestValidationError{err: err}
	}
	state := &scenarioRunState{SetupEntities: map[string]scenarioSetupEntityBinding{}}
	if err := r.runScenarioStep(ctx, file, evaluator, state, doc.Steps[0]); err != nil {
		return fmt.Errorf("derived %s/%s: %w", plan.FlowID, plan.PinName, err)
	}
	if err := r.waitForQuiescence(ctx, state.RunID); err != nil {
		return fmt.Errorf("derived %s/%s: %w", plan.FlowID, plan.PinName, err)
	}
	if err := r.evaluateExpectations(ctx, state, evaluator, doc.Expect); err != nil {
		return fmt.Errorf("derived %s/%s: %w", plan.FlowID, plan.PinName, err)
	}
	fmt.Fprintf(r.out, "scenario ok: derived:%s/%s\n", plan.FlowID, plan.PinName)
	return nil
}

func addDerivedGenericOracle(expect *scenarioExpect, eventKey string) {
	if expect == nil {
		return
	}
	for _, existing := range expect.Events.Include {
		if existing == eventKey {
			goto deadLetters
		}
	}
	expect.Events.Include = append(expect.Events.Include, eventKey)
deadLetters:
	if expect.NoDeadLetters == nil {
		value := true
		expect.NoDeadLetters = &value
	}
}

func parseScenarioDocument(raw []byte) (scenarioDocument, error) {
	document, err := scenariodocument.Admit(raw, "")
	if err != nil {
		return scenarioDocument{}, err
	}
	return document.Projection()
}

func (r scenarioRunner) scenarioEvaluatorSeed(file scenarioTestFile, doc scenarioDocument) (string, error) {
	label, err := normalizeScenarioLabel(file.Path)
	if err != nil {
		return "", fmt.Errorf("derive scenario identity: %w", err)
	}
	return scenariodocument.Seed(label, doc.Name, doc.Seed), nil
}

func newScenarioExpressionEvaluator(seed string, rawVars map[string]any) (*scenarioExpressionEvaluator, error) {
	return scenariodocument.NewEvaluator(seed, rawVars)
}

func optionalScenarioString(value any) string {
	text, _ := value.(string)
	return strings.TrimSpace(text)
}

func scenarioSHA40(value string) string { return scenariodocument.SHA40(value) }

func scenarioUUID(seed, label string) string { return scenariodocument.UUID(seed, label) }

func scenarioSetupEntityID(seed, runID string, entity evaluatedScenarioSetupEntity) string {
	flowID := strings.TrimSpace(entity.FlowID)
	if flowID == "" || flowID == "." {
		return runtimeflowidentity.EntityID(runID)
	}
	return scenarioUUID(seed, "setup.entity."+entity.Alias)
}

func (r scenarioRunner) runInvalidVariants(file scenarioTestFile, doc scenarioDocument, evaluator *scenarioExpressionEvaluator) error {
	baseStep, err := invalidBasePublishStep(doc.Invalid.Base)
	if err != nil {
		return fmt.Errorf("%s: invalid.base: %w", file.Path, err)
	}
	base, _, err := r.buildPublishPayload(file, evaluator, baseStep)
	if err != nil {
		return fmt.Errorf("%s: invalid.base must be valid: %w", file.Path, err)
	}
	for _, item := range doc.Invalid.Cases {
		payload := cloneAnyMap(base)
		if err := scenariodocument.ValidateSetPaths(item.Set); err != nil {
			return fmt.Errorf("%s: invalid case %s: %w", file.Path, item.Name, err)
		}
		keys := make([]string, 0, len(item.Set))
		for key := range item.Set {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			value, err := evaluator.Evaluate(item.Set[key])
			if err != nil {
				return fmt.Errorf("%s: invalid case %s: %w", file.Path, item.Name, err)
			}
			if err := scenariodocument.SetPath(payload, key, value); err != nil {
				return err
			}
		}
		data, err := scenariodocument.Materialize(payload)
		if err != nil {
			return fmt.Errorf("%s: invalid case %s: %w", file.Path, item.Name, err)
		}
		step := baseStep
		step.Payload = data
		_, _, err = r.buildPublishPayload(file, evaluator, step)
		if err == nil {
			return fmt.Errorf("%s: invalid case %s unexpectedly passed pre-mutation validation", file.Path, item.Name)
		}
		var violation *runtimeeventschema.Violation
		if !errors.As(err, &violation) {
			return fmt.Errorf("%s: invalid case %s did not produce a payload-schema violation: %w", file.Path, item.Name, err)
		}
	}
	return nil
}

func invalidBasePublishStep(base map[string]any) (scenarioStep, error) {
	raw, ok := base["publish"]
	if !ok {
		return scenarioStep{}, fmt.Errorf("publish is required")
	}
	eventName, ok := raw.(string)
	if !ok || strings.TrimSpace(eventName) == "" {
		return scenarioStep{}, fmt.Errorf("publish must be non-empty text")
	}
	payload := base["payload"]
	if text, ok := payload.(string); ok && text == "generate" {
		return scenarioStep{}, fmt.Errorf("generated invalid-case bases are not supported; author an explicit payload before applying invalid.set overrides")
	}
	return scenarioStep{Action: "publish", PublishEvent: eventName, Payload: payload}, nil
}

func (r scenarioRunner) runScenarioSetup(ctx context.Context, file scenarioTestFile, evaluator *scenarioExpressionEvaluator, state *scenarioRunState, setup scenarioSetup) error {
	if state.RunID != "" {
		return scenarioTestValidationError{err: fmt.Errorf("setup requires an empty run context")}
	}
	runID := scenarioUUID(evaluator.Seed(), "setup.run")
	params := map[string]any{
		"bundle_hash":     r.bundleHash,
		"run_id":          runID,
		"idempotency_key": scenarioSHA40(evaluator.Seed() + "\x00setup.entities"),
	}
	if selector := r.scenarioExecutionParams(); selector != nil {
		params["scenario_execution"] = selector
	}
	entities := make([]any, 0, len(setup.Entities))
	for _, entity := range setup.Entities {
		evaluated, err := r.evaluateScenarioSetupEntity(file, evaluator, entity)
		if err != nil {
			return scenarioTestValidationError{err: err}
		}
		entityID := scenarioSetupEntityID(evaluator.Seed(), runID, evaluated)
		flowInstance := evaluated.FlowID
		if flowInstance == "." {
			// Setup persistence owns the root runtime coordinate (the run ID), not the authored dot path.
			flowInstance = ""
		}
		entities = append(entities, map[string]any{
			"alias":         evaluated.Alias,
			"entity_id":     entityID,
			"flow_instance": flowInstance,
			"entity_type":   evaluated.EntityType,
			"current_state": evaluated.CurrentState,
			"fields":        evaluated.Fields,
			"gates":         evaluated.Gates,
		})
	}
	params["entities"] = entities
	var result testSetupEntitiesResult
	if err := r.client.call(ctx, scenarioTestSetupEntitiesMethod, params, &result); err != nil {
		return err
	}
	if err := validateTestSetupEntitiesResult(result, runID); err != nil {
		return err
	}
	state.RunID = strings.TrimSpace(result.RunID)
	state.SetupEntities = map[string]scenarioSetupEntityBinding{}
	for _, entity := range result.Entities {
		alias := strings.TrimSpace(entity.Alias)
		state.SetupEntities[alias] = scenarioSetupEntityBinding{
			Alias:        alias,
			EntityID:     strings.TrimSpace(entity.EntityID),
			FlowInstance: strings.Trim(strings.TrimSpace(entity.FlowInstance), "/"),
			EntityType:   strings.TrimSpace(entity.EntityType),
			CurrentState: strings.TrimSpace(entity.CurrentState),
		}
	}
	return nil
}

type evaluatedScenarioSetupEntity struct {
	Alias        string
	FlowID       string
	EntityType   string
	CurrentState string
	Fields       map[string]any
	Gates        map[string]bool
}

func (r scenarioRunner) evaluateScenarioSetupEntity(file scenarioTestFile, evaluator *scenarioExpressionEvaluator, entity scenarioSetupEntity) (evaluatedScenarioSetupEntity, error) {
	flowID := strings.Trim(strings.TrimSpace(file.FlowID), "/")
	if entity.Flow != nil {
		value, err := evaluator.Evaluate(entity.Flow)
		if err != nil {
			return evaluatedScenarioSetupEntity{}, fmt.Errorf("setup.entities[%s].flow: %w", entity.Alias, err)
		}
		flowID = strings.Trim(optionalScenarioString(value), "/")
	}
	primary, err := r.bundle.ResolveTestSetupPrimaryEntity(flowID, entity.EntityType)
	if err != nil {
		return evaluatedScenarioSetupEntity{}, fmt.Errorf("setup.entities[%s].flow: %w", entity.Alias, err)
	}
	if primary.EntityType != entity.EntityType {
		return evaluatedScenarioSetupEntity{}, fmt.Errorf("setup.entities[%s].type = %q, want declared entity type %q for flow %s", entity.Alias, entity.EntityType, primary.EntityType, scenarioFlowLabel(flowID))
	}
	stageFlowID := flowID
	if stageFlowID == "" {
		stageFlowID = "."
	}
	graph, found := r.bundle.WorkflowStageTopology(stageFlowID)
	if !found || graph.FlowID != stageFlowID || !graph.ValidStageCatalog() {
		return evaluatedScenarioSetupEntity{}, fmt.Errorf("setup.entities[%s].flow %q has no selected compiled stage catalog", entity.Alias, stageFlowID)
	}
	currentState := ""
	if entity.StateSet {
		value, err := evaluator.Evaluate(entity.CurrentState)
		if err != nil {
			return evaluatedScenarioSetupEntity{}, fmt.Errorf("setup.entities[%s].current_state: %w", entity.Alias, err)
		}
		currentState = optionalScenarioString(value)
	} else {
		initial, err := graph.InitialStoredStage()
		if err == nil {
			currentState = initial.ID()
		}
	}
	if currentState == "" {
		return evaluatedScenarioSetupEntity{}, fmt.Errorf("setup.entities[%s].current_state is required because flow %s has no initial state", entity.Alias, scenarioFlowLabel(flowID))
	}
	if _, err := graph.ResolveStoredStage(currentState); err != nil {
		return evaluatedScenarioSetupEntity{}, fmt.Errorf("setup.entities[%s].current_state %q is not declared for flow %s", entity.Alias, currentState, scenarioFlowLabel(flowID))
	}
	fields, err := r.evaluateScenarioSetupFields(evaluator, primary, entity)
	if err != nil {
		return evaluatedScenarioSetupEntity{}, err
	}
	gates, err := r.evaluateScenarioSetupGates(evaluator, flowID, entity)
	if err != nil {
		return evaluatedScenarioSetupEntity{}, err
	}
	return evaluatedScenarioSetupEntity{
		Alias:        entity.Alias,
		FlowID:       flowID,
		EntityType:   entity.EntityType,
		CurrentState: currentState,
		Fields:       fields,
		Gates:        gates,
	}, nil
}

func (r scenarioRunner) evaluateScenarioSetupFields(evaluator *scenarioExpressionEvaluator, primary runtimecontracts.PrimaryEntityContract, entity scenarioSetupEntity) (map[string]any, error) {
	if !entity.FieldsSet {
		return map[string]any{}, nil
	}
	value, err := evaluator.Evaluate(entity.Fields)
	if err != nil {
		return nil, fmt.Errorf("setup.entities[%s].fields: %w", entity.Alias, err)
	}
	fields, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("setup.entities[%s].fields must evaluate to a mapping", entity.Alias)
	}
	contract := entityruntime.Contract{
		FlowID:     strings.Trim(strings.TrimSpace(primary.FlowID), "/"),
		EntityType: strings.TrimSpace(primary.EntityType),
		Entity:     primary.Contract,
		Types:      primary.Types,
	}
	for field, fieldValue := range fields {
		normalized, err := entityruntime.NormalizeFieldValue(contract, field, fieldValue)
		if err != nil {
			return nil, fmt.Errorf("setup.entities[%s].fields.%s: %w", entity.Alias, field, err)
		}
		fields[field] = normalized
	}
	return fields, nil
}

func (r scenarioRunner) evaluateScenarioSetupGates(evaluator *scenarioExpressionEvaluator, flowID string, entity scenarioSetupEntity) (map[string]bool, error) {
	if !entity.GatesSet {
		return map[string]bool{}, nil
	}
	value, err := evaluator.Evaluate(entity.Gates)
	if err != nil {
		return nil, fmt.Errorf("setup.entities[%s].gates: %w", entity.Alias, err)
	}
	raw, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("setup.entities[%s].gates must evaluate to a mapping", entity.Alias)
	}
	declared := r.declaredScenarioGateNames(flowID)
	out := make(map[string]bool, len(raw))
	for gate, rawValue := range raw {
		boolValue, ok := rawValue.(bool)
		if !ok {
			return nil, fmt.Errorf("setup.entities[%s].gates.%s must evaluate to boolean", entity.Alias, gate)
		}
		if _, ok := declared[gate]; !ok {
			return nil, fmt.Errorf("setup.entities[%s].gates.%s is not declared for flow %s", entity.Alias, gate, scenarioFlowLabel(flowID))
		}
		out[gate] = boolValue
	}
	return out, nil
}

func (r scenarioRunner) declaredScenarioGateNames(flowID string) map[string]struct{} {
	flowID = strings.Trim(strings.TrimSpace(flowID), "/")
	out := map[string]struct{}{}
	for _, record := range r.bundle.ScopedNodeRecords() {
		node, err := record.Identity()
		if err != nil || strings.Trim(node.FlowPath(), "/") != flowID {
			continue
		}
		for _, gate := range record.Entry.GateState.Gates {
			if name := strings.TrimSpace(gate.Name); name != "" {
				out[name] = struct{}{}
			}
		}
	}
	for _, transition := range r.bundle.DerivedHandlerTransitions() {
		if strings.Trim(transition.Node.FlowPath(), "/") != flowID {
			continue
		}
		if transition.SetsGate != nil {
			if name := strings.TrimSpace(transition.SetsGate.Name); name != "" {
				out[name] = struct{}{}
			}
		}
		for _, gate := range transition.ClearGates {
			gate = strings.TrimSpace(gate)
			if gate != "" && gate != "*" {
				out[gate] = struct{}{}
			}
		}
	}
	return out
}

func scenarioFlowLabel(flowID string) string {
	if strings.TrimSpace(flowID) == "" {
		return "<root>"
	}
	return strings.Trim(strings.TrimSpace(flowID), "/")
}

func validateTestSetupEntitiesResult(result testSetupEntitiesResult, wantRunID string) error {
	result.RunID = strings.TrimSpace(result.RunID)
	if result.RunID == "" {
		return fmt.Errorf("malformed test.setup_entities result: run_id is required")
	}
	if wantRunID != "" && result.RunID != wantRunID {
		return fmt.Errorf("malformed test.setup_entities result: run_id = %q, want %q", result.RunID, wantRunID)
	}
	if len(result.Entities) == 0 {
		return fmt.Errorf("malformed test.setup_entities result: entities is required")
	}
	aliases := map[string]struct{}{}
	for i, entity := range result.Entities {
		if strings.TrimSpace(entity.Alias) == "" {
			return fmt.Errorf("malformed test.setup_entities result: entities[%d].alias is required", i)
		}
		if _, ok := aliases[entity.Alias]; ok {
			return fmt.Errorf("malformed test.setup_entities result: entities[%d].alias %q is repeated", i, entity.Alias)
		}
		aliases[entity.Alias] = struct{}{}
		for _, field := range []struct {
			name  string
			value string
		}{
			{name: "entity_id", value: entity.EntityID},
			{name: "entity_type", value: entity.EntityType},
			{name: "current_state", value: entity.CurrentState},
		} {
			if strings.TrimSpace(field.value) == "" {
				return fmt.Errorf("malformed test.setup_entities result: entities[%d].%s is required", i, field.name)
			}
		}
		if _, err := uuid.Parse(entity.EntityID); err != nil {
			return fmt.Errorf("malformed test.setup_entities result: entities[%d].entity_id must be UUID", i)
		}
	}
	return nil
}

func (r scenarioRunner) runScenarioStep(ctx context.Context, file scenarioTestFile, evaluator *scenarioExpressionEvaluator, state *scenarioRunState, step scenarioStep) error {
	switch step.Action {
	case "publish":
		return r.runPublishStep(ctx, file, evaluator, state, step)
	case "mailbox.decide", "mailbox.defer":
		return r.runMailboxStep(ctx, evaluator, state, step)
	default:
		return fmt.Errorf("unsupported action %q", step.Action)
	}
}

func (r scenarioRunner) runPublishStep(ctx context.Context, file scenarioTestFile, evaluator *scenarioExpressionEvaluator, state *scenarioRunState, step scenarioStep) error {
	payload, resolvedEvent, err := r.buildPublishPayload(file, evaluator, step)
	if err != nil {
		return scenarioTestValidationError{err: err}
	}
	eventName := r.scopedEventName(file.FlowID, resolvedEvent)
	params := map[string]any{
		"event_name":  eventName,
		"payload":     payload,
		"bundle_hash": r.bundleHash,
	}
	if selector := r.scenarioExecutionParams(); selector != nil {
		params["scenario_execution"] = selector
	}
	if state.RunID != "" {
		params["run_id"] = state.RunID
	}
	if state.RunID != "" && step.SourceEventID == nil && state.LastEventID != "" {
		params["source_event_id"] = state.LastEventID
	}
	for _, field := range []struct {
		name  string
		value any
	}{
		{name: "idempotency_key", value: step.IdempotencyKey},
		{name: "emitter", value: step.Emitter},
		{name: "source_event_id", value: step.SourceEventID},
	} {
		value, err := evaluator.Evaluate(field.value)
		if err != nil {
			return fmt.Errorf("%s: %w", field.name, err)
		}
		if text := optionalScenarioString(value); text != "" {
			params[field.name] = text
		}
	}
	targetFlow, err := evaluator.Evaluate(step.TargetFlowInstance)
	if err != nil {
		return fmt.Errorf("target_flow_instance: %w", err)
	}
	targetEntity, err := evaluator.Evaluate(step.TargetEntityID)
	if err != nil {
		return fmt.Errorf("target_entity_id: %w", err)
	}
	targetFlowText := optionalScenarioString(targetFlow)
	targetEntityText := optionalScenarioString(targetEntity)
	if step.Target != nil {
		targetAliasValue, err := evaluator.Evaluate(step.Target)
		if err != nil {
			return fmt.Errorf("target: %w", err)
		}
		targetAlias := optionalScenarioString(targetAliasValue)
		if targetAlias == "" {
			return fmt.Errorf("target must evaluate to a non-empty setup alias")
		}
		if state.RunID == "" {
			return fmt.Errorf("target alias requires an existing run context")
		}
		binding, ok := state.SetupEntities[targetAlias]
		if !ok {
			return fmt.Errorf("target alias %q is not declared in setup.entities", targetAlias)
		}
		if strings.TrimSpace(binding.FlowInstance) == "" || binding.FlowInstance == state.RunID {
			return fmt.Errorf("target alias %q resolves to root entity; event.publish target requires a flow-scoped setup entity", targetAlias)
		}
		targetFlowText = binding.FlowInstance
		targetEntityText = binding.EntityID
	}
	if targetFlowText != "" || targetEntityText != "" {
		if state.RunID == "" {
			return fmt.Errorf("target route requires an existing run context")
		}
		if targetFlowText == "" || targetEntityText == "" {
			return fmt.Errorf("target route requires both target_flow_instance and target_entity_id")
		}
		params["target"] = map[string]any{
			"flow_instance": strings.Trim(targetFlowText, "/"),
			"entity_id":     targetEntityText,
		}
	}
	var result eventPublishResult
	if err := r.client.call(ctx, eventPublishMethod, params, &result); err != nil {
		return err
	}
	if err := validateEventPublishResult(result); err != nil {
		return err
	}
	state.RunID = result.RunID
	state.LastEventID = result.EventID
	return nil
}

func (r scenarioRunner) scenarioExecutionParams() map[string]any {
	if r.scenarioExecution == nil {
		return nil
	}
	return map[string]any{
		"profile_id":              r.scenarioExecution.ProfileID,
		"profile_digest":          r.scenarioExecution.ProfileDigest,
		"effective_source_digest": r.scenarioExecution.EffectiveSourceDigest,
	}
}

func (r scenarioRunner) scopedEventName(flowID, eventName string) string {
	eventName = strings.TrimSpace(eventName)
	flowID = strings.Trim(strings.TrimSpace(flowID), "/")
	if flowID == "" || flowID == "." || eventName == "" || strings.Contains(eventName, "/") {
		return eventName
	}
	resolved := r.bundle.ResolveFlowEventReference(flowID, eventName)
	if strings.Contains(resolved, "/") {
		return resolved
	}
	return flowID + "/" + eventName
}

func (r scenarioRunner) buildPublishPayload(file scenarioTestFile, evaluator *scenarioExpressionEvaluator, step scenarioStep) (map[string]any, string, error) {
	if step.GeneratePayload {
		plan, err := r.compileGeneratedInputFixture(file, evaluator, step.PublishEvent)
		if err != nil {
			return nil, "", err
		}
		payload, err := plan.materializePayload()
		if err != nil {
			return nil, "", err
		}
		return payload, plan.eventKey, nil
	}
	payload, err := r.buildPayloadFromSpec(file, evaluator, step.Payload)
	if err != nil {
		return nil, "", err
	}
	resolution, err := r.resolvePublishSchema(file, step.PublishEvent, "")
	if err != nil {
		return nil, "", err
	}
	if err := runtimeeventschema.ValidatePayloadAgainstSchema(resolution.Schema.Schema, payload); err != nil {
		return nil, "", err
	}
	return payload, resolution.EventKey, nil
}

func (r scenarioRunner) compileGeneratedInputFixture(file scenarioTestFile, evaluator *scenarioExpressionEvaluator, publishIdentity string) (generatedInputFixturePlan, error) {
	if r.bundle == nil {
		return generatedInputFixturePlan{}, fmt.Errorf("generated payload requires a loaded contract bundle")
	}
	if evaluator == nil || strings.TrimSpace(evaluator.Seed()) == "" {
		return generatedInputFixturePlan{}, fmt.Errorf("generated payload requires a recorded scenario identity")
	}

	if r.source == nil {
		return generatedInputFixturePlan{}, fmt.Errorf("generated payload requires the effective semantic source")
	}
	census := semanticview.BuildAuthoredEventEndpointCensus(r.source)
	association := census.ResolveDeclaredInputEndpoint(file.FlowID, publishIdentity)
	endpoint, ok := association.Endpoint()
	if !ok {
		return generatedInputFixturePlan{}, publishInputEndpointError(census, association)
	}
	resolution, err := r.resolvePublishSchema(file, endpoint.Event.EventKey(), endpoint.PinName)
	if err != nil {
		return generatedInputFixturePlan{}, err
	}

	canonicalSchema := runtimeeventschema.CanonicalAcceptanceSchema(resolution.Schema.Schema)
	canonicalSchemaJSON, err := canonicaljson.Bytes(canonicalSchema)
	if err != nil {
		return generatedInputFixturePlan{}, fmt.Errorf(
			"generated payload for flow %q input pin %q event %q: canonicalize schema: %w",
			scenarioFlowLabel(file.FlowID), endpoint.PinName, resolution.EventKey, err,
		)
	}
	schemaSum := sha256.Sum256(canonicalSchemaJSON)
	schemaDigest := "sha256:" + hex.EncodeToString(schemaSum[:])
	identity := strings.Join([]string{
		"generated-input-fixture-v1",
		evaluator.Seed(),
		strings.Trim(strings.TrimSpace(file.FlowID), "/"),
		strings.TrimSpace(endpoint.PinName),
		strings.TrimSpace(resolution.EventKey),
		schemaDigest,
	}, "\x00")
	generated, err := runtimeeventschema.InhabitDeterministically(resolution.Schema.Schema, runtimeeventschema.InhabitationContext{Identity: identity})
	if err != nil {
		return generatedInputFixturePlan{}, fmt.Errorf(
			"generated payload for flow %q input pin %q event %q: %w",
			scenarioFlowLabel(file.FlowID), endpoint.PinName, resolution.EventKey, err,
		)
	}
	payload, ok := generated.(map[string]any)
	if !ok {
		return generatedInputFixturePlan{}, fmt.Errorf(
			"generated payload for flow %q input pin %q event %q: input event schema must produce an object, got %T",
			scenarioFlowLabel(file.FlowID), endpoint.PinName, resolution.EventKey, generated,
		)
	}
	if err := runtimeeventschema.ValidatePayloadAgainstSchema(resolution.Schema.Schema, payload); err != nil {
		return generatedInputFixturePlan{}, fmt.Errorf(
			"generated payload for flow %q input pin %q event %q failed canonical post-validation: %w",
			scenarioFlowLabel(file.FlowID), endpoint.PinName, resolution.EventKey, err,
		)
	}
	payloadJSON, err := canonicaljson.Bytes(payload)
	if err != nil {
		return generatedInputFixturePlan{}, fmt.Errorf(
			"generated payload for flow %q input pin %q event %q: canonicalize payload: %w",
			scenarioFlowLabel(file.FlowID), endpoint.PinName, resolution.EventKey, err,
		)
	}
	return generatedInputFixturePlan{
		flowID:          strings.Trim(strings.TrimSpace(file.FlowID), "/"),
		pinName:         strings.TrimSpace(endpoint.PinName),
		eventKey:        strings.TrimSpace(resolution.EventKey),
		schemaDigest:    schemaDigest,
		identity:        identity,
		canonicalSchema: append(json.RawMessage(nil), canonicalSchemaJSON...),
		payload:         append(json.RawMessage(nil), payloadJSON...),
	}, nil
}

func (r scenarioRunner) resolvePublishSchema(file scenarioTestFile, eventIdentity, pinName string) (semanticview.EventSchemaResolution, error) {
	if r.source == nil {
		return semanticview.EventSchemaResolution{}, fmt.Errorf("publish payload requires the effective semantic source")
	}
	flowID := strings.TrimSpace(file.FlowID)
	eventIdentity = strings.TrimSpace(eventIdentity)
	if split := strings.LastIndex(eventIdentity, "/"); split > 0 && split+1 < len(eventIdentity) {
		candidateFlowID := strings.TrimSpace(eventIdentity[:split])
		if _, ok := r.source.FlowScopeByID(candidateFlowID); ok {
			flowID = candidateFlowID
			eventIdentity = strings.TrimSpace(eventIdentity[split+1:])
		}
	}
	if strings.TrimSpace(pinName) == "" {
		association := semanticview.BuildAuthoredEventEndpointCensus(r.source).ResolveDeclaredInputEndpoint(flowID, eventIdentity)
		if endpoint, ok := association.Endpoint(); ok && strings.TrimSpace(eventIdentity) == strings.TrimSpace(endpoint.PinName) {
			eventIdentity = endpoint.Event.EventKey()
		}
	}
	canonicalEventKey := strings.TrimSpace(r.source.ResolveFlowEventReference(flowID, eventIdentity))
	if canonicalEventKey == "" {
		return semanticview.EventSchemaResolution{}, fmt.Errorf(
			"publish payload for flow %q event %q has no canonical event identity",
			scenarioFlowLabel(flowID), strings.TrimSpace(eventIdentity),
		)
	}
	resolution := semanticview.ResolveEventSchema(r.source, flowID, eventIdentity)
	if !resolution.HasSchema {
		context := fmt.Sprintf("event %q", strings.TrimSpace(eventIdentity))
		if strings.TrimSpace(pinName) != "" {
			context = fmt.Sprintf("input pin %q event %q", strings.TrimSpace(pinName), strings.TrimSpace(eventIdentity))
		}
		return semanticview.EventSchemaResolution{}, fmt.Errorf("publish payload for flow %q %s has no resolved event schema; declare the event payload", scenarioFlowLabel(flowID), context)
	}
	if err := resolution.UnresolvedTypeError(); err != nil {
		return semanticview.EventSchemaResolution{}, fmt.Errorf("publish payload for flow %q event %q: %w", scenarioFlowLabel(flowID), resolution.EventKey, err)
	}
	// Schema resolution may select a wildcard declaration. Publication still
	// carries the concrete event identity resolved from the effective source.
	resolution.EventKey = canonicalEventKey
	return resolution, nil
}

func (p generatedInputFixturePlan) materializePayload() (map[string]any, error) {
	if len(p.payload) == 0 {
		return nil, fmt.Errorf("generated input fixture plan has no admitted payload")
	}
	payload, err := materializeScenarioSemanticPayload(p.payload)
	if err != nil {
		return nil, fmt.Errorf("materialize generated input fixture plan: %w", err)
	}
	if payload == nil {
		return nil, fmt.Errorf("generated input fixture plan payload must be an object")
	}
	return payload, nil
}

func materializeScenarioSemanticPayload(raw []byte) (map[string]any, error) {
	admitted, err := canonicaljson.Decode(raw)
	if err != nil {
		return nil, err
	}
	projected, err := workflowexpr.ProjectSemanticValue(admitted)
	if err != nil {
		return nil, err
	}
	payload, ok := projected.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("scenario payload must be an object")
	}
	return payload, nil
}

func publishInputEndpointError(census semanticview.AuthoredEventEndpointCensus, association semanticview.EndpointAssociationResult) error {
	pins := association.Candidates
	if len(pins) == 0 {
		for _, endpoint := range census.InputPins() {
			if strings.TrimSpace(endpoint.FlowID) == strings.TrimSpace(association.FlowID) {
				pins = append(pins, endpoint)
			}
		}
	}
	labels := make([]string, 0, len(pins))
	for _, endpoint := range pins {
		label := strings.TrimSpace(endpoint.PinName)
		if eventKey := strings.TrimSpace(endpoint.Event.EventKey()); eventKey != "" {
			label += " (" + eventKey + ")"
		}
		labels = append(labels, label)
	}
	sort.Strings(labels)
	flow := scenarioFlowLabel(association.FlowID)
	if association.Status == semanticview.EndpointAssociationAmbiguous {
		return fmt.Errorf(
			"publish identity %q in flow %q is ambiguous; matching input pins: %s; publish an exact pin name",
			association.Identity, flow, strings.Join(labels, ", "),
		)
	}
	available := "none"
	if len(labels) > 0 {
		available = strings.Join(labels, ", ")
	}
	return fmt.Errorf(
		"publish identity %q in flow %q does not resolve to a declared input pin; available input pins: %s",
		association.Identity, flow, available,
	)
}

func (r scenarioRunner) buildPayloadFromSpec(file scenarioTestFile, evaluator *scenarioExpressionEvaluator, spec any) (map[string]any, error) {
	data, err := scenariodocument.PreparePayload(spec, evaluator, func(label string) (semanticvalue.Value, error) {
		return r.loadFixturePayload(file.Path, label)
	})
	if err != nil {
		return nil, err
	}
	value, err := data.Interface()
	if err != nil {
		return nil, err
	}
	return value.(map[string]any), nil
}

func (r scenarioRunner) loadFixturePayload(scenarioLabel, rawLabel string) (semanticvalue.Value, error) {
	if r.bundle == nil || r.bundle.SourceArtifact == nil {
		return semanticvalue.Value{}, fmt.Errorf("payload.from requires an admitted source artifact")
	}
	rawLabel = strings.TrimSpace(rawLabel)
	if rawLabel == "" || path.IsAbs(rawLabel) || strings.Contains(rawLabel, "\\") {
		return semanticvalue.Value{}, fmt.Errorf("payload.from %q must be a scenario-relative artifact label", rawLabel)
	}
	label := path.Clean(path.Join(path.Dir(scenarioLabel), rawLabel))
	if label == "." || label == ".." || strings.HasPrefix(label, "../") {
		return semanticvalue.Value{}, fmt.Errorf("payload.from %s escapes the admitted source artifact", rawLabel)
	}
	entry, ok := r.bundle.SourceArtifact.Entry(label)
	if !ok {
		return semanticvalue.Value{}, fmt.Errorf("payload.from %s does not name an admitted source member", rawLabel)
	}
	return scenariodocument.AdmitFixture(entry.Bytes(), label)
}

func cloneAnyMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = cloneAny(value)
	}
	return out
}

func cloneAny(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneAnyMap(typed)
	case []any:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, cloneAny(item))
		}
		return out
	default:
		return typed
	}
}

func (r scenarioRunner) runMailboxStep(ctx context.Context, evaluator *scenarioExpressionEvaluator, state *scenarioRunState, step scenarioStep) error {
	if state.RunID == "" {
		return fmt.Errorf("%s requires an existing run context", step.Action)
	}
	params := map[string]any{}
	if key, err := evaluator.Evaluate(step.IdempotencyKey); err != nil {
		return fmt.Errorf("idempotency_key: %w", err)
	} else if text := optionalScenarioString(key); text != "" {
		params["idempotency_key"] = text
	}
	switch step.Action {
	case "mailbox.decide":
		verdict, err := evaluator.Evaluate(step.Verdict)
		if err != nil {
			return scenarioTestValidationError{err: fmt.Errorf("verdict: %w", err)}
		}
		if text := optionalScenarioString(verdict); text != "" {
			params["verdict"] = text
		} else {
			return scenarioTestValidationError{err: fmt.Errorf("mailbox.decide verdict is required")}
		}
		fields, err := evaluator.Evaluate(step.Fields)
		if err != nil {
			return scenarioTestValidationError{err: fmt.Errorf("fields: %w", err)}
		}
		if fields != nil {
			m, ok := fields.(map[string]any)
			if !ok {
				return scenarioTestValidationError{err: fmt.Errorf("mailbox.decide fields must be an object")}
			}
			params["fields"] = m
		}
	case "mailbox.defer":
		until, err := evaluator.Evaluate(step.Until)
		if err != nil {
			return scenarioTestValidationError{err: fmt.Errorf("until: %w", err)}
		}
		text := optionalScenarioString(until)
		if text == "" {
			return scenarioTestValidationError{err: fmt.Errorf("mailbox.defer until is required")}
		}
		if _, err := time.Parse(time.RFC3339, text); err != nil {
			return scenarioTestValidationError{err: fmt.Errorf("mailbox.defer until must be RFC3339: %w", err)}
		}
		params["until"] = text
	}
	cardID, contentHash, err := r.findDecisionCard(ctx, evaluator, state.RunID, step.Match)
	if err != nil {
		return err
	}
	params["card_id"] = cardID
	if step.Action == "mailbox.decide" {
		params["observed_content_hash"] = contentHash
	}
	var result mailboxMutationResult
	if err := r.client.call(ctx, step.Action, params, &result); err != nil {
		return err
	}
	if !result.OK || result.CardID != cardID || result.ChangeID <= 0 {
		return fmt.Errorf("malformed %s result", step.Action)
	}
	return nil
}

func (r scenarioRunner) findDecisionCard(ctx context.Context, evaluator *scenarioExpressionEvaluator, runID string, match map[string]any) (string, string, error) {
	params, evaluatedMatch, err := evaluateScenarioCardMatch(evaluator, runID, match)
	if err != nil {
		return "", "", err
	}
	matches, err := r.collectScenarioCardMatches(ctx, params, evaluatedMatch)
	if err != nil {
		return "", "", err
	}
	if len(matches) != 1 {
		return "", "", fmt.Errorf("decision-card match for run %s returned %d items, want exactly one", runID, len(matches))
	}
	var detail mailboxDetailProjection
	if err := r.client.call(ctx, "mailbox.get", map[string]any{"mailbox_id": matches[0].CardID}, &detail); err != nil {
		return "", "", err
	}
	if err := validateMailboxDetailResult(detail); err != nil {
		return "", "", err
	}
	return matches[0].CardID, detail.DecisionCard.CardContentHash, nil
}

func evaluateScenarioCardMatch(evaluator *scenarioExpressionEvaluator, runID string, match map[string]any) (map[string]any, map[string]string, error) {
	params := map[string]any{
		"status": "pending",
		"run_id": runID,
		"limit":  200,
	}
	evaluatedMatch := make(map[string]string, len(match))
	for key, value := range match {
		evaluated, err := evaluator.Evaluate(value)
		if err != nil {
			return nil, nil, fmt.Errorf("match.%s: %w", key, err)
		}
		text, ok := evaluated.(string)
		if !ok {
			return nil, nil, fmt.Errorf("match.%s must resolve to text", key)
		}
		text = strings.TrimSpace(text)
		if text == "" {
			continue
		}
		evaluatedMatch[key] = text
		switch key {
		case "entity_id":
			params["entity_id"] = text
		case "anchor_kind":
			params["anchor_kind"] = text
		case "card_id", "decision", "stage", "flow_instance", "requester_agent_id", "category", "scope", "request_event_id", "activity_id":
		default:
			return nil, nil, scenarioTestValidationError{err: fmt.Errorf("unsupported decision-card match field %q", key)}
		}
	}
	if _, err := admitScenarioCardMatch(evaluatedMatch); err != nil {
		return nil, nil, err
	}
	return params, evaluatedMatch, nil
}

func admitScenarioCardMatch(evaluatedMatch map[string]string) (string, error) {
	anchorKind := evaluatedMatch["anchor_kind"]
	if !decisioncard.IsRegisteredAnchorKind(anchorKind) {
		return "", scenarioTestValidationError{err: fmt.Errorf("decision-card match.anchor_kind is required and must be one of: %s", decisioncard.RegisteredAnchorKindDescription())}
	}
	for key := range evaluatedMatch {
		switch anchorKind {
		case string(decisioncard.AnchorKindStageGate):
			if key == "requester_agent_id" || key == "category" || key == "scope" || key == "request_event_id" || key == "activity_id" {
				return "", scenarioTestValidationError{err: fmt.Errorf("decision-card match.%s is not valid for anchor_kind stage_gate", key)}
			}
		case string(decisioncard.AnchorKindHumanTask):
			if key == "decision" || key == "stage" || key == "request_event_id" || key == "activity_id" {
				return "", scenarioTestValidationError{err: fmt.Errorf("decision-card match.%s is not valid for anchor_kind human_task", key)}
			}
		case string(decisioncard.AnchorKindProposedEffect):
			if key == "stage" || key == "requester_agent_id" || key == "category" {
				return "", scenarioTestValidationError{err: fmt.Errorf("decision-card match.%s is not valid for anchor_kind proposed_effect", key)}
			}
		}
	}
	return anchorKind, nil
}

func (r scenarioRunner) collectScenarioCardMatches(ctx context.Context, params map[string]any, evaluatedMatch map[string]string) ([]mailboxDecisionCardSummary, error) {
	anchorKind := evaluatedMatch["anchor_kind"]
	matches := make([]mailboxDecisionCardSummary, 0)
	seen := make(map[string]struct{})
	// Uniqueness is a property of the complete public match set, not a page.
	for {
		var result mailboxListResult
		if err := r.client.call(ctx, "mailbox.list", params, &result); err != nil {
			return nil, err
		}
		if err := validateMailboxListResult(result); err != nil {
			return nil, err
		}
		for _, item := range result.Items {
			if item.Kind != "decision_card" || item.DecisionCard == nil {
				continue
			}
			card := *item.DecisionCard
			if !scenarioCardMatches(card, anchorKind, evaluatedMatch) {
				continue
			}
			matches = append(matches, card)
		}
		if result.NextCursor == "" {
			break
		}
		if _, exists := seen[result.NextCursor]; exists {
			return nil, fmt.Errorf("malformed mailbox.list result: repeated next_cursor %q", result.NextCursor)
		}
		seen[result.NextCursor] = struct{}{}
		params["cursor"] = result.NextCursor
	}
	return matches, nil
}

func scenarioCardMatches(card mailboxDecisionCardSummary, anchorKind string, evaluatedMatch map[string]string) bool {
	if card.AnchorKind != anchorKind {
		return false
	}
	if expected := evaluatedMatch["card_id"]; expected != "" && card.CardID != expected {
		return false
	}
	if expected := evaluatedMatch["entity_id"]; expected != "" && card.Scope.EntityID != expected {
		return false
	}
	if expected := evaluatedMatch["flow_instance"]; expected != "" && card.Scope.FlowInstance != expected {
		return false
	}
	if expected := evaluatedMatch["decision"]; expected != "" && card.Decision != expected {
		return false
	}
	if expected := evaluatedMatch["stage"]; expected != "" && card.Anchor.Stage != expected {
		return false
	}
	if expected := evaluatedMatch["requester_agent_id"]; expected != "" && card.Anchor.RequesterAgentID != expected {
		return false
	}
	if expected := evaluatedMatch["category"]; expected != "" && card.Category != expected {
		return false
	}
	if expected := evaluatedMatch["request_event_id"]; expected != "" && card.Anchor.RequestEventID != expected {
		return false
	}
	if expected := evaluatedMatch["activity_id"]; expected != "" && card.Anchor.ActivityID != expected {
		return false
	}
	if expected := evaluatedMatch["scope"]; expected != "" && card.Scope.Kind != expected {
		return false
	}
	return true
}

func (r scenarioRunner) waitForQuiescence(ctx context.Context, runID string) error {
	deadline := time.Now().Add(r.timeout)
	for {
		quiescence, err := r.readTestQuiescence(ctx, runID)
		if err != nil {
			return err
		}
		if BoolPointerValue(quiescence.Ready) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("test quiescence deadline reached for run %s with active_deliveries=%d unsettled_pipeline_events=%d due_timers=%d active_session_leases=%d",
				runID,
				IntPointerValue(quiescence.ActiveDeliveries),
				IntPointerValue(quiescence.UnsettledPipelineEvents),
				IntPointerValue(quiescence.DueTimers),
				IntPointerValue(quiescence.ActiveSessionLeases),
			)
		}
		timer := time.NewTimer(r.pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (r scenarioRunner) readTestQuiescence(ctx context.Context, runID string) (diagnosticRunTestQuiescence, error) {
	params := map[string]any{"run_id": runID}
	var result DiagnosticRunDiagnosisResult
	if err := r.client.call(ctx, "run.diagnose", params, &result); err != nil {
		return diagnosticRunTestQuiescence{}, err
	}
	if err := validateDiagnosticRunDiagnosis(result); err != nil {
		return diagnosticRunTestQuiescence{}, err
	}
	return *result.TestQuiescence, nil
}

func (r scenarioRunner) evaluateExpectations(ctx context.Context, state *scenarioRunState, evaluator *scenarioExpressionEvaluator, expect scenarioExpect) error {
	runID := state.RunID
	if err := r.waitForEventExpectations(ctx, runID, expect.Events); err != nil {
		return err
	}
	if expect.NoDeadLetters != nil && *expect.NoDeadLetters {
		if err := r.assertNoDeadLetters(ctx, runID); err != nil {
			return err
		}
	}
	for _, entity := range expect.Entities {
		if err := r.assertEntityExpectation(ctx, state, evaluator, entity); err != nil {
			return err
		}
	}
	return nil
}

func (r scenarioRunner) waitForEventExpectations(ctx context.Context, runID string, expect scenarioEventExpect) error {
	deadline := time.Now().Add(r.timeout)
	for {
		rows, err := r.fetchRunTraceRows(ctx, runID)
		if err != nil {
			return err
		}
		names := uniqueScenarioTraceEventNames(rows)
		if err := assertScenarioEventExpectations(names, expect); err == nil {
			return nil
		} else if time.Now().After(deadline) {
			return err
		}

		timer := time.NewTimer(r.pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func uniqueScenarioTraceEventNames(rows []diagnosticRunTraceRow) []string {
	names := make([]string, 0, len(rows))
	seen := map[string]struct{}{}
	for _, row := range rows {
		eventID := strings.TrimSpace(row.EventID)
		if eventID == "" {
			eventID = strings.TrimSpace(row.EventName)
		}
		if _, ok := seen[eventID]; ok {
			continue
		}
		seen[eventID] = struct{}{}
		names = append(names, row.EventName)
	}
	return names
}

func (r scenarioRunner) fetchRunTraceRows(ctx context.Context, runID string) ([]diagnosticRunTraceRow, error) {
	params := map[string]any{"run_id": runID, "limit": 500}
	var out []diagnosticRunTraceRow
	seen := map[string]struct{}{}
	for {
		var result diagnosticRunTraceResult
		if err := r.client.call(ctx, "run.trace", params, &result); err != nil {
			return nil, err
		}
		if err := validateDiagnosticRunTraceResult(result); err != nil {
			return nil, err
		}
		out = append(out, result.Trace...)
		cursor := strings.TrimSpace(result.NextCursor)
		if cursor == "" {
			return out, nil
		}
		if _, ok := seen[cursor]; ok {
			return nil, fmt.Errorf("malformed run.trace result: repeated next_cursor %q", cursor)
		}
		seen[cursor] = struct{}{}
		params["cursor"] = cursor
	}
}

func assertScenarioEventExpectations(actual []string, expect scenarioEventExpect) error {
	if len(expect.Include) > 0 {
		for _, want := range expect.Include {
			if !scenarioStringSliceContains(actual, want) {
				return fmt.Errorf("expected event %s was not observed in %v", want, actual)
			}
		}
	}
	if expect.Exact != nil {
		got := append([]string(nil), actual...)
		want := append([]string(nil), expect.Exact...)
		sort.Strings(got)
		sort.Strings(want)
		if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
			return fmt.Errorf("event exact expectation mismatch: got %v want %v", actual, expect.Exact)
		}
	}
	if len(expect.Ordered) > 0 {
		pos := 0
		for _, eventName := range actual {
			if eventName == expect.Ordered[pos] {
				pos++
				if pos == len(expect.Ordered) {
					return nil
				}
			}
		}
		return fmt.Errorf("ordered event expectation %v was not observed in %v", expect.Ordered, actual)
	}
	return nil
}

func (r scenarioRunner) assertNoDeadLetters(ctx context.Context, runID string) error {
	params := map[string]any{
		"filter": map[string]any{
			"run_id": runID,
		},
		"limit": 500,
	}
	seen := map[string]struct{}{}
	for {
		var result eventListResult
		if err := r.client.call(ctx, eventObservationMethodList, params, &result); err != nil {
			return err
		}
		if err := validateEventListResult(result); err != nil {
			return err
		}
		for _, event := range result.Events {
			if strings.TrimSpace(event.EventName) == "platform.dead_letter" || len(event.DeadLetters) > 0 {
				evidence := "platform.dead_letter"
				if len(event.DeadLetters) > 0 {
					evidence = scenarioFailureEvidence(event.DeadLetters[0].Failure)
				}
				return fmt.Errorf("expected no dead letters for run %s, found event %s (%s)", runID, event.EventID, evidence)
			}
			for _, delivery := range event.Deliveries {
				if strings.TrimSpace(delivery.Status) == "dead_letter" || len(delivery.DeadLetters) > 0 {
					evidence := strings.TrimSpace(delivery.ReasonCode)
					if delivery.Failure != nil {
						evidence = scenarioFailureEvidence(*delivery.Failure)
					} else if len(delivery.DeadLetters) > 0 {
						evidence = scenarioFailureEvidence(delivery.DeadLetters[0].Failure)
					}
					return fmt.Errorf("expected no dead letters for run %s, found delivery %s on event %s (%s)", runID, delivery.DeliveryID, event.EventID, evidence)
				}
			}
		}
		cursor := strings.TrimSpace(result.NextCursor)
		if cursor == "" {
			return nil
		}
		if _, ok := seen[cursor]; ok {
			return fmt.Errorf("malformed event.list result: repeated next_cursor %q", cursor)
		}
		seen[cursor] = struct{}{}
		params["cursor"] = cursor
	}
}

func scenarioFailureEvidence(failure runtimefailures.Envelope) string {
	parts := []string{strings.TrimSpace(failure.Detail.Code + ": " + failure.Message)}
	if component := strings.TrimSpace(failure.Component); component != "" {
		parts = append(parts, "component="+component)
	}
	if operation := strings.TrimSpace(failure.Operation); operation != "" {
		parts = append(parts, "operation="+operation)
	}
	if len(failure.Detail.Attributes) > 0 {
		parts = append(parts, fmt.Sprintf("attributes=%v", failure.Detail.Attributes))
	}
	return strings.Join(parts, " ")
}

func (r scenarioRunner) assertEntityExpectation(ctx context.Context, state *scenarioRunState, evaluator *scenarioExpressionEvaluator, expect scenarioEntityExpect) error {
	runID := state.RunID
	if expect.Ref != "" {
		binding, ok := state.SetupEntities[expect.Ref]
		if !ok {
			return fmt.Errorf("expect.entities ref %q is not declared in setup.entities", expect.Ref)
		}
		if expect.EntityType != "" && expect.EntityType != binding.EntityType {
			return fmt.Errorf("expect.entities ref %q type = %q, want %q", expect.Ref, binding.EntityType, expect.EntityType)
		}
		return r.assertEntityDetailExpectation(ctx, runID, binding.EntityID, binding.EntityType, evaluator, expect)
	}
	params := map[string]any{
		"run_id": runID,
		"type":   expect.EntityType,
		"limit":  500,
	}
	entities := []entitySummary{}
	seen := map[string]struct{}{}
	for {
		var result entityListResult
		if err := r.client.call(ctx, entityListMethod, params, &result); err != nil {
			return err
		}
		if err := validateEntityListResult(result); err != nil {
			return err
		}
		entities = append(entities, result.Entities...)
		cursor := strings.TrimSpace(result.NextCursor)
		if cursor == "" {
			break
		}
		if _, ok := seen[cursor]; ok {
			return fmt.Errorf("malformed entity.list result: repeated next_cursor %q", cursor)
		}
		seen[cursor] = struct{}{}
		params["cursor"] = cursor
	}
	count := len(entities)
	if expect.Count != nil && count != *expect.Count {
		return fmt.Errorf("entity expectation for type %s got count %d, want %d", expect.EntityType, count, *expect.Count)
	}
	if !expect.HasDetailAssertion() {
		return nil
	}
	if count != 1 {
		return fmt.Errorf("entity detail expectation for type %s returned %d entities, want exactly one", expect.EntityType, count)
	}
	return r.assertEntityDetailExpectation(ctx, runID, entities[0].EntityID, expect.EntityType, evaluator, expect)
}

func (r scenarioRunner) assertEntityDetailExpectation(ctx context.Context, runID string, entityID string, entityType string, evaluator *scenarioExpressionEvaluator, expect scenarioEntityExpect) error {
	detail, err := evaluateScenarioEntityDetail(expect, evaluator)
	if err != nil {
		return err
	}
	var full entityFull
	if err := r.client.call(ctx, entityGetMethod, map[string]any{"entity_id": entityID, "run_id": runID}, &full); err != nil {
		return err
	}
	if err := validateEntityFullResult("entity.get result", full); err != nil {
		return err
	}
	if full.Entity.EntityID != entityID {
		return fmt.Errorf("malformed entity.get result: entity.entity_id = %q, want %q", full.Entity.EntityID, entityID)
	}
	if full.Entity.RunID != runID {
		return fmt.Errorf("malformed entity.get result: entity.run_id = %q, want %q", full.Entity.RunID, runID)
	}
	if full.Entity.EntityType != entityType {
		return fmt.Errorf("malformed entity.get result: entity.entity_type = %q, want %q", full.Entity.EntityType, entityType)
	}
	if detail.State != nil && full.Entity.CurrentState != *detail.State {
		return fmt.Errorf("entity %s current_state = %q, want %q", entityID, full.Entity.CurrentState, *detail.State)
	}
	if detail.FieldsSet {
		if err := assertScenarioJSONEqual(fmt.Sprintf("entity %s fields", entityID), full.Fields, detail.Fields); err != nil {
			return err
		}
	}
	if detail.GatesSet {
		if err := assertScenarioJSONEqual(fmt.Sprintf("entity %s gates", entityID), full.Gates, detail.Gates); err != nil {
			return err
		}
	}
	return nil
}

type evaluatedScenarioEntityDetail struct {
	State     *string
	Fields    map[string]any
	FieldsSet bool
	Gates     map[string]bool
	GatesSet  bool
}

func evaluateScenarioEntityDetail(e scenarioEntityExpect, evaluator *scenarioExpressionEvaluator) (evaluatedScenarioEntityDetail, error) {
	var out evaluatedScenarioEntityDetail
	if evaluator == nil {
		return out, fmt.Errorf("scenario expression evaluator is required for entity detail assertions")
	}
	if e.StateSet {
		value, err := evaluator.Evaluate(e.CurrentState)
		if err != nil {
			return out, fmt.Errorf("expect.entities.current_state: %w", err)
		}
		state, ok := value.(string)
		if !ok || strings.TrimSpace(state) == "" {
			return out, fmt.Errorf("expect.entities.current_state must evaluate to a non-empty string")
		}
		state = strings.TrimSpace(state)
		out.State = &state
	}
	if e.FieldsSet {
		value, err := evaluator.Evaluate(e.Fields)
		if err != nil {
			return out, fmt.Errorf("expect.entities.fields: %w", err)
		}
		fields, ok := value.(map[string]any)
		if !ok {
			return out, fmt.Errorf("expect.entities.fields must evaluate to a mapping")
		}
		out.Fields = fields
		out.FieldsSet = true
	}
	if e.GatesSet {
		value, err := evaluator.Evaluate(e.Gates)
		if err != nil {
			return out, fmt.Errorf("expect.entities.gates: %w", err)
		}
		gates, ok := value.(map[string]any)
		if !ok {
			return out, fmt.Errorf("expect.entities.gates must evaluate to a mapping")
		}
		out.Gates = map[string]bool{}
		for gate, raw := range gates {
			value, ok := raw.(bool)
			if !ok {
				return out, fmt.Errorf("expect.entities.gates.%s must evaluate to boolean", gate)
			}
			out.Gates[gate] = value
		}
		out.GatesSet = true
	}
	return out, nil
}

func assertScenarioJSONEqual(label string, got, want any) error {
	gotJSON, err := scenarioCanonicalJSON(got)
	if err != nil {
		return fmt.Errorf("%s actual value is not JSON encodable: %w", label, err)
	}
	wantJSON, err := scenarioCanonicalJSON(want)
	if err != nil {
		return fmt.Errorf("%s expected value is not JSON encodable: %w", label, err)
	}
	if gotJSON != wantJSON {
		return fmt.Errorf("%s mismatch: got %s want %s", label, gotJSON, wantJSON)
	}
	return nil
}

func scenarioCanonicalJSON(value any) (string, error) {
	out, err := canonicaljson.Bytes(value)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

func scenarioStringSliceContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func returnScenarioTestValidationError(errOut io.Writer, err error) error {
	writeCLIAPIError(errOut, err)
	return commandExitError{code: scenarioTestExitValidation}
}

func scenarioTestAPIErrorExitCode(err error) int {
	var validation scenarioTestValidationError
	if errors.As(err, &validation) {
		return scenarioTestExitValidation
	}
	return cliAPIErrorExitCode(err, cliAPIErrorClassifier{
		runtimeExit:  scenarioTestExitRuntime,
		authExit:     scenarioTestExitAuth,
		notFoundExit: scenarioTestExitNotFound,
		conflictExit: scenarioTestExitRejected,
		notFoundCodes: []string{
			"RUN_NOT_FOUND",
			"EVENT_NOT_FOUND",
			"MAILBOX_NOT_FOUND",
		},
		conflictCodes: []string{
			"BUNDLE_MISMATCH",
			"BUNDLE_SCOPE_REQUIRED",
			"BUNDLE_UNAVAILABLE",
			"BUNDLE_DATA_INTEGRITY_ERROR",
			"UNSUPPORTED_BUNDLE_HASH",
			"EVENT_NOT_DECLARED",
			"EVENT_PUBLISH_FAILED",
			"PAYLOAD_VALIDATION_FAILED",
			"RUN_ALREADY_TERMINAL",
			"IDEMPOTENCY_CONFLICT",
			"MAILBOX_ALREADY_DECIDED",
			"MAILBOX_CARD_SUPERSEDED",
			"MAILBOX_STALE_CARD",
		},
	})
}
