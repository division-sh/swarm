package main

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	runtimeeventschema "github.com/division-sh/swarm/internal/runtime/eventschema"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
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
	platformSpec string
	timeout      time.Duration
	pollInterval time.Duration
}

type scenarioTestFile struct {
	Path   string
	FlowID string
}

type scenarioDocument struct {
	Name    string
	Seed    string
	Vars    map[string]any
	Setup   scenarioSetup
	Steps   []scenarioStep
	Expect  scenarioExpect
	Invalid *scenarioInvalid
}

type scenarioSetup struct {
	Entities []scenarioSetupEntity
}

type scenarioSetupEntity struct {
	Alias        string
	EntityType   string
	Flow         any
	CurrentState any
	StateSet     bool
	Fields       map[string]any
	FieldsSet    bool
	Gates        map[string]any
	GatesSet     bool
}

type scenarioStep struct {
	Action             string
	PublishEvent       string
	Payload            any
	Match              map[string]any
	Reason             any
	Until              any
	IdempotencyKey     any
	Emitter            any
	SourceEventID      any
	Target             any
	TargetFlowInstance any
	TargetEntityID     any
}

type scenarioExpect struct {
	Events        scenarioEventExpect
	NoDeadLetters *bool
	Entities      []scenarioEntityExpect
}

type scenarioEventExpect struct {
	Include []string
	Exact   []string
	Ordered []string
}

type scenarioEntityExpect struct {
	Ref          string
	EntityType   string
	Count        *int
	CurrentState any
	StateSet     bool
	Fields       map[string]any
	FieldsSet    bool
	Gates        map[string]any
	GatesSet     bool
}

type scenarioInvalid struct {
	Base  map[string]any
	Cases []scenarioInvalidCase
}

type scenarioInvalidCase struct {
	Name   string
	Set    map[string]any
	Expect string
}

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
	client       *cliAPIClient
	bundle       *runtimecontracts.WorkflowContractBundle
	bundleHash   string
	contractsDir string
	timeout      time.Duration
	pollInterval time.Duration
	out          io.Writer
}

type scenarioExpressionEvaluator struct {
	env  *cel.Env
	seed string
	vars map[string]any
}

func newTestCommand(repoRoot string, opts rootCommandOptions) *cobra.Command {
	testOpts := scenarioTestCommandOptions{
		apiOptions:   opts,
		timeout:      defaultScenarioTestTimeout,
		pollInterval: defaultScenarioTestPoll,
	}
	cmd := &cobra.Command{
		Use:   "test [scenario-file ...]",
		Short: "Run deterministic scenario tests through public read owners.",
		Args:  cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runScenarioTestCommand(cmd.Context(), repoRoot, cmd.OutOrStdout(), cmd.ErrOrStderr(), args, testOpts)
		},
	}
	cmd.Flags().StringVar(&testOpts.contracts, "contracts", "", "Contract package root containing scenario tests")
	cmd.Flags().StringVar(&testOpts.platformSpec, "platform-spec", "", "platform-spec.yaml path used to load the contract bundle")
	cmd.Flags().DurationVar(&testOpts.timeout, "timeout", defaultScenarioTestTimeout, "Safety deadline for test quiescence")
	cmd.Flags().DurationVar(&testOpts.pollInterval, "poll-interval", defaultScenarioTestPoll, "Canonical readback polling interval while waiting for quiescence")
	bindCLIAPIConnectionFlagsWithClass(cmd, &testOpts.apiOptions, cliAPICommandClassMutating, "swarm test")
	return cmd
}

func runScenarioTestCommand(ctx context.Context, repoRoot string, out, errOut io.Writer, args []string, opts scenarioTestCommandOptions) error {
	if opts.timeout <= 0 {
		return returnScenarioTestValidationError(errOut, fmt.Errorf("--timeout must be positive"))
	}
	if opts.pollInterval <= 0 {
		return returnScenarioTestValidationError(errOut, fmt.Errorf("--poll-interval must be positive"))
	}
	contractsDir, platformSpec, err := resolveScenarioTestSources(repoRoot, opts.contracts, opts.platformSpec)
	if err != nil {
		return returnScenarioTestValidationError(errOut, err)
	}
	files, err := discoverScenarioTestFiles(contractsDir, args)
	if err != nil {
		return returnScenarioTestValidationError(errOut, err)
	}
	if len(files) == 0 {
		return returnScenarioTestValidationError(errOut, fmt.Errorf("no scenario files found under contracts/tests or contracts/flows/<flow>/tests"))
	}
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, contractsDir, platformSpec)
	if err != nil {
		return returnScenarioTestValidationError(errOut, fmt.Errorf("load contract bundle: %w", err))
	}
	bundleHash, err := runtimecontracts.BundleHash(bundle)
	if err != nil {
		return returnScenarioTestValidationError(errOut, fmt.Errorf("compute bundle_hash: %w", err))
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: scenarioTestAPIErrorExitCode(err)}
	}
	runner := scenarioRunner{
		client:       client,
		bundle:       bundle,
		bundleHash:   bundleHash,
		contractsDir: contractsDir,
		timeout:      opts.timeout,
		pollInterval: opts.pollInterval,
		out:          out,
	}
	for _, file := range files {
		if err := runner.runScenarioFile(ctx, file); err != nil {
			fmt.Fprintln(errOut, err)
			return commandExitError{code: scenarioTestAPIErrorExitCode(err)}
		}
	}
	fmt.Fprintf(out, "swarm test ok: scenarios=%d\n", len(files))
	return nil
}

func resolveScenarioTestSources(repoRoot, contractsFlag, platformSpecFlag string) (string, string, error) {
	repoRoot = strings.TrimSpace(repoRoot)
	if repoRoot == "" {
		repoRoot = "."
	}
	cfg, err := loadCLIAPIConfigFile()
	if err != nil {
		return "", "", err
	}
	contractsDir := strings.TrimSpace(contractsFlag)
	if contractsDir == "" {
		contractsDir = strings.TrimSpace(cfg.ContractsPath)
	}
	if contractsDir == "" {
		contractsDir = filepath.Join(repoRoot, "contracts")
		if _, err := os.Stat(filepath.Join(contractsDir, "package.yaml")); err != nil {
			contractsDir = repoRoot
		}
	}
	contractsDir, err = absFrom(repoRoot, contractsDir)
	if err != nil {
		return "", "", err
	}
	platformSpec := strings.TrimSpace(platformSpecFlag)
	if platformSpec == "" {
		platformSpec = strings.TrimSpace(cfg.PlatformSpecPath)
	}
	if platformSpec == "" {
		platformSpec = runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	}
	platformSpec, err = absFrom(repoRoot, platformSpec)
	if err != nil {
		return "", "", err
	}
	return contractsDir, platformSpec, nil
}

func absFrom(base, path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", fmt.Errorf("path is required")
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(base, path)
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	return abs, nil
}

func discoverScenarioTestFiles(contractsDir string, args []string) ([]scenarioTestFile, error) {
	contractsDir, err := filepath.Abs(contractsDir)
	if err != nil {
		return nil, err
	}
	if len(args) > 0 {
		out := make([]scenarioTestFile, 0, len(args))
		for _, arg := range args {
			path, err := absFrom(contractsDir, arg)
			if err != nil {
				return nil, err
			}
			file, ok := classifyScenarioTestFile(contractsDir, path)
			if !ok {
				return nil, fmt.Errorf("scenario file %s is outside supported roots contracts/tests and contracts/flows/<flow>/tests", path)
			}
			out = append(out, file)
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
		return out, nil
	}
	var out []scenarioTestFile
	for _, root := range []string{
		filepath.Join(contractsDir, "tests"),
		filepath.Join(contractsDir, "flows"),
	} {
		if _, err := os.Stat(root); err != nil {
			continue
		}
		if err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			if ext := strings.ToLower(filepath.Ext(path)); ext != ".yaml" && ext != ".yml" {
				return nil
			}
			if file, ok := classifyScenarioTestFile(contractsDir, path); ok {
				if autoDiscoveredScenarioCandidate(path) {
					out = append(out, file)
				}
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out, nil
}

func classifyScenarioTestFile(contractsDir, path string) (scenarioTestFile, bool) {
	absContracts, err := filepath.Abs(contractsDir)
	if err != nil {
		return scenarioTestFile{}, false
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return scenarioTestFile{}, false
	}
	rel, err := filepath.Rel(absContracts, absPath)
	if err != nil || rel == "." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == ".." {
		return scenarioTestFile{}, false
	}
	parts := splitPath(rel)
	if len(parts) >= 2 && parts[0] == "tests" {
		return scenarioTestFile{Path: absPath}, true
	}
	if len(parts) >= 4 && parts[0] == "flows" {
		for i := 2; i < len(parts); i++ {
			if parts[i] == "tests" {
				return scenarioTestFile{Path: absPath, FlowID: strings.Join(parts[1:i], "/")}, true
			}
		}
	}
	return scenarioTestFile{}, false
}

func autoDiscoveredScenarioCandidate(path string) bool {
	raw, err := os.ReadFile(path)
	if err != nil {
		return true
	}
	var root yaml.Node
	if err := yaml.Unmarshal(raw, &root); err != nil || len(root.Content) == 0 || root.Content[0].Kind != yaml.MappingNode {
		return true
	}
	top := mappingNode(root.Content[0])
	return top["version"] != nil || top["steps"] != nil || top["invalid"] != nil
}

func splitPath(path string) []string {
	raw := strings.Split(filepath.ToSlash(path), "/")
	out := raw[:0]
	for _, part := range raw {
		if part != "" && part != "." {
			out = append(out, part)
		}
	}
	return out
}

func (r scenarioRunner) runScenarioFile(ctx context.Context, file scenarioTestFile) error {
	raw, err := os.ReadFile(file.Path)
	if err != nil {
		return scenarioTestValidationError{err: fmt.Errorf("%s: read scenario: %w", file.Path, err)}
	}
	doc, err := parseScenarioDocument(raw)
	if err != nil {
		return scenarioTestValidationError{err: fmt.Errorf("%s: %w", file.Path, err)}
	}
	seed, err := r.scenarioEvaluatorSeed(file, doc)
	if err != nil {
		return scenarioTestValidationError{err: fmt.Errorf("%s: %w", file.Path, err)}
	}
	evaluator, err := newScenarioExpressionEvaluator(seed, doc.Vars)
	if err != nil {
		return scenarioTestValidationError{err: fmt.Errorf("%s: %w", file.Path, err)}
	}
	if doc.Invalid != nil {
		if err := r.runInvalidVariants(file, doc, evaluator); err != nil {
			return scenarioTestValidationError{err: err}
		}
	}
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
		if !doc.Expect.empty() {
			if err := r.evaluateExpectations(ctx, state, evaluator, doc.Expect); err != nil {
				return fmt.Errorf("%s: %w", file.Path, err)
			}
		}
	}
	fmt.Fprintf(r.out, "scenario ok: %s\n", file.Path)
	return nil
}

func parseScenarioDocument(raw []byte) (scenarioDocument, error) {
	var root yaml.Node
	if err := yaml.Unmarshal(raw, &root); err != nil {
		return scenarioDocument{}, fmt.Errorf("parse YAML: %w", err)
	}
	if len(root.Content) == 0 || root.Content[0].Kind != yaml.MappingNode {
		return scenarioDocument{}, fmt.Errorf("scenario document must be a YAML mapping")
	}
	top := mappingNode(root.Content[0])
	for key := range top {
		switch key {
		case "version", "name", "seed", "vars", "setup", "steps", "expect", "invalid":
		default:
			return scenarioDocument{}, fmt.Errorf("unsupported top-level field %q", key)
		}
	}
	if node := top["version"]; node != nil {
		version := strings.TrimSpace(fmt.Sprint(yamlNodeValue(node)))
		if version != "" && version != "1" && version != "v1" {
			return scenarioDocument{}, fmt.Errorf("unsupported scenario version %q", version)
		}
	}
	doc := scenarioDocument{Vars: map[string]any{}}
	if node := top["name"]; node != nil {
		doc.Name = strings.TrimSpace(fmt.Sprint(yamlNodeValue(node)))
	}
	if node := top["seed"]; node != nil {
		doc.Seed = strings.TrimSpace(fmt.Sprint(yamlNodeValue(node)))
	}
	if node := top["vars"]; node != nil {
		vars, ok := yamlNodeValue(node).(map[string]any)
		if !ok {
			return scenarioDocument{}, fmt.Errorf("vars must be a mapping")
		}
		doc.Vars = vars
	}
	if node := top["setup"]; node != nil {
		setup, err := parseScenarioSetup(node)
		if err != nil {
			return scenarioDocument{}, err
		}
		doc.Setup = setup
	}
	stepsNode := top["steps"]
	if stepsNode == nil {
		return scenarioDocument{}, fmt.Errorf("steps is required")
	}
	if stepsNode.Kind != yaml.SequenceNode || len(stepsNode.Content) == 0 {
		return scenarioDocument{}, fmt.Errorf("steps must be a non-empty list")
	}
	for _, node := range stepsNode.Content {
		step, err := parseScenarioStep(node)
		if err != nil {
			return scenarioDocument{}, err
		}
		doc.Steps = append(doc.Steps, step)
	}
	if node := top["expect"]; node != nil {
		expect, err := parseScenarioExpect(node)
		if err != nil {
			return scenarioDocument{}, err
		}
		doc.Expect = expect
	}
	if node := top["invalid"]; node != nil {
		invalid, err := parseScenarioInvalid(node)
		if err != nil {
			return scenarioDocument{}, err
		}
		doc.Invalid = &invalid
	}
	return doc, nil
}

func parseScenarioSetup(node *yaml.Node) (scenarioSetup, error) {
	if node.Kind != yaml.MappingNode {
		return scenarioSetup{}, fmt.Errorf("setup must be a mapping")
	}
	m := yamlNodeValue(node).(map[string]any)
	for key := range m {
		switch key {
		case "entities":
		default:
			return scenarioSetup{}, fmt.Errorf("unsupported setup field %q", key)
		}
	}
	rawEntities, ok := m["entities"].([]any)
	if !ok || len(rawEntities) == 0 {
		return scenarioSetup{}, fmt.Errorf("setup.entities must be a non-empty list")
	}
	seen := map[string]struct{}{}
	out := scenarioSetup{Entities: make([]scenarioSetupEntity, 0, len(rawEntities))}
	for i, raw := range rawEntities {
		item, err := parseScenarioSetupEntity(raw, i)
		if err != nil {
			return scenarioSetup{}, err
		}
		if _, ok := seen[item.Alias]; ok {
			return scenarioSetup{}, fmt.Errorf("setup.entities[%d].as %q is duplicated", i, item.Alias)
		}
		seen[item.Alias] = struct{}{}
		out.Entities = append(out.Entities, item)
	}
	return out, nil
}

func parseScenarioSetupEntity(raw any, i int) (scenarioSetupEntity, error) {
	m, ok := raw.(map[string]any)
	if !ok {
		return scenarioSetupEntity{}, fmt.Errorf("setup.entities[%d] must be a mapping", i)
	}
	var item scenarioSetupEntity
	for key, value := range m {
		switch key {
		case "as":
			item.Alias = strings.TrimSpace(fmt.Sprint(value))
		case "type":
			item.EntityType = strings.TrimSpace(fmt.Sprint(value))
		case "flow":
			item.Flow = cloneAny(value)
		case "current_state":
			if text, ok := value.(string); ok && strings.TrimSpace(text) == "" {
				return scenarioSetupEntity{}, fmt.Errorf("setup.entities[%d].current_state must be non-empty", i)
			}
			item.CurrentState = cloneAny(value)
			item.StateSet = true
		case "fields":
			fields, ok := value.(map[string]any)
			if !ok {
				return scenarioSetupEntity{}, fmt.Errorf("setup.entities[%d].fields must be a mapping", i)
			}
			item.Fields = cloneAnyMap(fields)
			item.FieldsSet = true
		case "gates":
			gates, ok := value.(map[string]any)
			if !ok {
				return scenarioSetupEntity{}, fmt.Errorf("setup.entities[%d].gates must be a mapping", i)
			}
			item.Gates = cloneAnyMap(gates)
			item.GatesSet = true
		default:
			return scenarioSetupEntity{}, fmt.Errorf("unsupported setup.entities[%d] field %q", i, key)
		}
	}
	if item.Alias == "" {
		return scenarioSetupEntity{}, fmt.Errorf("setup.entities[%d].as is required", i)
	}
	if item.EntityType == "" {
		return scenarioSetupEntity{}, fmt.Errorf("setup.entities[%d].type is required", i)
	}
	return item, nil
}

func (r scenarioRunner) scenarioEvaluatorSeed(file scenarioTestFile, doc scenarioDocument) (string, error) {
	rel, err := filepath.Rel(r.contractsDir, file.Path)
	if err != nil {
		return "", fmt.Errorf("derive scenario identity: %w", err)
	}
	rel = filepath.ToSlash(filepath.Clean(rel))
	if rel == "." || strings.HasPrefix(rel, "../") || rel == ".." {
		return "", fmt.Errorf("scenario file %s is outside contract package root", file.Path)
	}
	parts := []string{
		"scenario-v1",
		"path=" + rel,
		"name=" + strings.TrimSpace(doc.Name),
		"seed=" + strings.TrimSpace(doc.Seed),
	}
	return strings.Join(parts, "\x00"), nil
}

func parseScenarioStep(node *yaml.Node) (scenarioStep, error) {
	if node.Kind != yaml.MappingNode {
		return scenarioStep{}, fmt.Errorf("scenario step must be a mapping")
	}
	m := yamlNodeValue(node).(map[string]any)
	if rawPublish, ok := m["publish"]; ok {
		eventName := strings.TrimSpace(fmt.Sprint(rawPublish))
		if eventName == "" {
			return scenarioStep{}, fmt.Errorf("publish step requires non-empty event name")
		}
		for key := range m {
			switch key {
			case "publish", "payload", "idempotency_key", "emitter", "source_event_id", "target", "target_flow_instance", "target_entity_id":
			default:
				return scenarioStep{}, fmt.Errorf("unsupported publish step field %q", key)
			}
		}
		if _, ok := m["target"]; ok {
			if _, hasFlow := m["target_flow_instance"]; hasFlow {
				return scenarioStep{}, fmt.Errorf("publish step target cannot be combined with target_flow_instance")
			}
			if _, hasEntity := m["target_entity_id"]; hasEntity {
				return scenarioStep{}, fmt.Errorf("publish step target cannot be combined with target_entity_id")
			}
		}
		return scenarioStep{
			Action:             "publish",
			PublishEvent:       eventName,
			Payload:            m["payload"],
			IdempotencyKey:     m["idempotency_key"],
			Emitter:            m["emitter"],
			SourceEventID:      m["source_event_id"],
			Target:             m["target"],
			TargetFlowInstance: m["target_flow_instance"],
			TargetEntityID:     m["target_entity_id"],
		}, nil
	}
	if len(m) != 1 {
		return scenarioStep{}, fmt.Errorf("scenario step must contain publish or one mailbox action")
	}
	for key, value := range m {
		action := normalizeScenarioMailboxAction(key)
		if action == "" {
			return scenarioStep{}, fmt.Errorf("unsupported scenario action %q", key)
		}
		cfg, ok := value.(map[string]any)
		if !ok {
			return scenarioStep{}, fmt.Errorf("%s step must be a mapping", key)
		}
		for cfgKey := range cfg {
			switch cfgKey {
			case "match", "payload", "reason", "until", "idempotency_key":
			default:
				return scenarioStep{}, fmt.Errorf("unsupported %s step field %q", key, cfgKey)
			}
		}
		match, ok := cfg["match"].(map[string]any)
		if !ok {
			match = map[string]any{}
		}
		return scenarioStep{
			Action:         action,
			Match:          match,
			Payload:        cfg["payload"],
			Reason:         cfg["reason"],
			Until:          cfg["until"],
			IdempotencyKey: cfg["idempotency_key"],
		}, nil
	}
	return scenarioStep{}, fmt.Errorf("scenario step is empty")
}

func normalizeScenarioMailboxAction(value string) string {
	switch strings.TrimSpace(value) {
	case "mailbox.approve", "approve_mailbox":
		return "mailbox.approve"
	case "mailbox.reject", "reject_mailbox":
		return "mailbox.reject"
	case "mailbox.defer", "defer_mailbox":
		return "mailbox.defer"
	default:
		return ""
	}
}

func parseScenarioExpect(node *yaml.Node) (scenarioExpect, error) {
	if node.Kind != yaml.MappingNode {
		return scenarioExpect{}, fmt.Errorf("expect must be a mapping")
	}
	m := yamlNodeValue(node).(map[string]any)
	var expect scenarioExpect
	for key, value := range m {
		switch key {
		case "events":
			events, err := parseScenarioEventExpect(value)
			if err != nil {
				return scenarioExpect{}, err
			}
			expect.Events = events
		case "no_dead_letters":
			b, ok := value.(bool)
			if !ok {
				return scenarioExpect{}, fmt.Errorf("expect.no_dead_letters must be boolean")
			}
			expect.NoDeadLetters = &b
		case "entities":
			entities, err := parseScenarioEntityExpect(value)
			if err != nil {
				return scenarioExpect{}, err
			}
			expect.Entities = entities
		default:
			return scenarioExpect{}, fmt.Errorf("unsupported expect field %q", key)
		}
	}
	return expect, nil
}

func parseScenarioEventExpect(value any) (scenarioEventExpect, error) {
	if list, ok := value.([]any); ok {
		values, err := stringListFromAny("expect.events", list)
		return scenarioEventExpect{Include: values}, err
	}
	m, ok := value.(map[string]any)
	if !ok {
		return scenarioEventExpect{}, fmt.Errorf("expect.events must be a list or mapping")
	}
	var out scenarioEventExpect
	for key, raw := range m {
		list, ok := raw.([]any)
		if !ok {
			return scenarioEventExpect{}, fmt.Errorf("expect.events.%s must be a list", key)
		}
		values, err := stringListFromAny("expect.events."+key, list)
		if err != nil {
			return scenarioEventExpect{}, err
		}
		switch key {
		case "include":
			out.Include = values
		case "exact":
			out.Exact = values
		case "ordered":
			out.Ordered = values
		default:
			return scenarioEventExpect{}, fmt.Errorf("unsupported expect.events field %q", key)
		}
	}
	return out, nil
}

func parseScenarioEntityExpect(value any) ([]scenarioEntityExpect, error) {
	list, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("expect.entities must be a list")
	}
	out := make([]scenarioEntityExpect, 0, len(list))
	for i, raw := range list {
		m, ok := raw.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("expect.entities[%d] must be a mapping", i)
		}
		var item scenarioEntityExpect
		for key, value := range m {
			switch key {
			case "ref":
				item.Ref = strings.TrimSpace(fmt.Sprint(value))
			case "type":
				item.EntityType = strings.TrimSpace(fmt.Sprint(value))
			case "count":
				count, ok := intFromAny(value)
				if !ok || count < 0 {
					return nil, fmt.Errorf("expect.entities[%d].count must be a non-negative integer", i)
				}
				item.Count = &count
			case "current_state":
				if text, ok := value.(string); ok && strings.TrimSpace(text) == "" {
					return nil, fmt.Errorf("expect.entities[%d].current_state must be non-empty", i)
				}
				item.CurrentState = cloneAny(value)
				item.StateSet = true
			case "fields":
				fields, ok := value.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("expect.entities[%d].fields must be a mapping", i)
				}
				item.Fields = cloneAnyMap(fields)
				item.FieldsSet = true
			case "gates":
				gates, ok := value.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("expect.entities[%d].gates must be a mapping", i)
				}
				item.Gates = cloneAnyMap(gates)
				item.GatesSet = true
			default:
				return nil, fmt.Errorf("unsupported expect.entities[%d] field %q", i, key)
			}
		}
		if item.EntityType == "" {
			if item.Ref == "" {
				return nil, fmt.Errorf("expect.entities[%d].type is required", i)
			}
		}
		detail := item.hasDetailAssertion()
		if item.Ref != "" && item.Count != nil {
			return nil, fmt.Errorf("expect.entities[%d].count cannot be combined with ref", i)
		}
		if item.Count != nil && detail {
			return nil, fmt.Errorf("expect.entities[%d].count cannot be combined with current_state, fields, or gates", i)
		}
		if item.Count == nil && !detail {
			return nil, fmt.Errorf("expect.entities[%d] requires count, current_state, fields, or gates", i)
		}
		out = append(out, item)
	}
	return out, nil
}

func (e scenarioExpect) empty() bool {
	return len(e.Events.Include) == 0 && len(e.Events.Exact) == 0 && len(e.Events.Ordered) == 0 && e.NoDeadLetters == nil && len(e.Entities) == 0
}

func (e scenarioEntityExpect) hasDetailAssertion() bool {
	return e.StateSet || e.FieldsSet || e.GatesSet
}

func parseScenarioInvalid(node *yaml.Node) (scenarioInvalid, error) {
	if node.Kind != yaml.MappingNode {
		return scenarioInvalid{}, fmt.Errorf("invalid must be a mapping")
	}
	m := yamlNodeValue(node).(map[string]any)
	for key := range m {
		switch key {
		case "base", "cases":
		default:
			return scenarioInvalid{}, fmt.Errorf("unsupported invalid field %q", key)
		}
	}
	base, ok := m["base"].(map[string]any)
	if !ok || len(base) == 0 {
		return scenarioInvalid{}, fmt.Errorf("invalid.base must be a mapping")
	}
	casesRaw, ok := m["cases"].([]any)
	if !ok || len(casesRaw) == 0 {
		return scenarioInvalid{}, fmt.Errorf("invalid.cases must be a non-empty list")
	}
	out := scenarioInvalid{Base: base}
	for i, raw := range casesRaw {
		m, ok := raw.(map[string]any)
		if !ok {
			return scenarioInvalid{}, fmt.Errorf("invalid.cases[%d] must be a mapping", i)
		}
		item := scenarioInvalidCase{Expect: "reject"}
		for key, value := range m {
			switch key {
			case "name":
				item.Name = strings.TrimSpace(fmt.Sprint(value))
			case "set":
				set, ok := value.(map[string]any)
				if !ok {
					return scenarioInvalid{}, fmt.Errorf("invalid.cases[%d].set must be a mapping", i)
				}
				item.Set = set
			case "expect":
				item.Expect = strings.TrimSpace(fmt.Sprint(value))
			default:
				return scenarioInvalid{}, fmt.Errorf("unsupported invalid.cases[%d] field %q", i, key)
			}
		}
		if item.Name == "" {
			item.Name = fmt.Sprintf("case-%d", i+1)
		}
		if item.Expect != "reject" && item.Expect != "fail_closed" {
			return scenarioInvalid{}, fmt.Errorf("invalid.cases[%d].expect must be reject or fail_closed", i)
		}
		out.Cases = append(out.Cases, item)
	}
	return out, nil
}

func stringListFromAny(field string, list []any) ([]string, error) {
	out := make([]string, 0, len(list))
	for i, raw := range list {
		value := strings.TrimSpace(fmt.Sprint(raw))
		if value == "" {
			return nil, fmt.Errorf("%s[%d] must be non-empty", field, i)
		}
		out = append(out, value)
	}
	return out, nil
}

func intFromAny(value any) (int, bool) {
	switch typed := value.(type) {
	case int:
		return typed, true
	case int64:
		return int(typed), int64(int(typed)) == typed
	case float64:
		i := int(typed)
		return i, typed == float64(i)
	default:
		return 0, false
	}
}

func mappingNode(node *yaml.Node) map[string]*yaml.Node {
	out := map[string]*yaml.Node{}
	for i := 0; i+1 < len(node.Content); i += 2 {
		out[strings.TrimSpace(node.Content[i].Value)] = node.Content[i+1]
	}
	return out
}

func yamlNodeValue(node *yaml.Node) any {
	switch node.Kind {
	case yaml.MappingNode:
		out := map[string]any{}
		for i := 0; i+1 < len(node.Content); i += 2 {
			out[strings.TrimSpace(node.Content[i].Value)] = yamlNodeValue(node.Content[i+1])
		}
		return out
	case yaml.SequenceNode:
		out := make([]any, 0, len(node.Content))
		for _, item := range node.Content {
			out = append(out, yamlNodeValue(item))
		}
		return out
	case yaml.ScalarNode:
		var value any
		if err := node.Decode(&value); err == nil {
			return value
		}
		return node.Value
	default:
		return nil
	}
}

func newScenarioExpressionEvaluator(seed string, rawVars map[string]any) (*scenarioExpressionEvaluator, error) {
	e := &scenarioExpressionEvaluator{seed: seed, vars: map[string]any{}}
	env, err := cel.NewEnv(
		cel.Variable("vars", cel.DynType),
		cel.Function("scenario.sha40",
			cel.Overload("scenario_sha40_string", []*cel.Type{cel.StringType}, cel.StringType,
				cel.UnaryBinding(func(value ref.Val) ref.Val {
					return types.String(scenarioSHA40(fmt.Sprint(value.Value())))
				}),
			),
		),
		cel.Function("scenario.uuid",
			cel.Overload("scenario_uuid_string", []*cel.Type{cel.StringType}, cel.StringType,
				cel.UnaryBinding(func(value ref.Val) ref.Val {
					return types.String(scenarioUUID(seed, fmt.Sprint(value.Value())))
				}),
			),
		),
	)
	if err != nil {
		return nil, err
	}
	e.env = env
	keys := make([]string, 0, len(rawVars))
	for key := range rawVars {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value, err := e.evalValue(rawVars[key])
		if err != nil {
			return nil, fmt.Errorf("vars.%s: %w", key, err)
		}
		e.vars[key] = value
	}
	return e, nil
}

func (e *scenarioExpressionEvaluator) evalValue(value any) (any, error) {
	switch typed := value.(type) {
	case string:
		if strings.HasPrefix(typed, "${") && strings.HasSuffix(typed, "}") && len(typed) >= 3 {
			return e.evalExpression(strings.TrimSpace(typed[2 : len(typed)-1]))
		}
		return typed, nil
	case []any:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			value, err := e.evalValue(item)
			if err != nil {
				return nil, err
			}
			out = append(out, value)
		}
		return out, nil
	case map[string]any:
		out := map[string]any{}
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			value, err := e.evalValue(typed[key])
			if err != nil {
				return nil, fmt.Errorf("%s: %w", key, err)
			}
			out[key] = value
		}
		return out, nil
	default:
		return typed, nil
	}
}

func (e *scenarioExpressionEvaluator) evalExpression(expression string) (any, error) {
	if expression == "" {
		return nil, fmt.Errorf("CEL expression is empty")
	}
	ast, issues := e.env.Compile(expression)
	if issues != nil && issues.Err() != nil {
		return nil, issues.Err()
	}
	program, err := e.env.Program(ast)
	if err != nil {
		return nil, err
	}
	out, _, err := program.Eval(map[string]any{"vars": e.vars})
	if err != nil {
		return nil, err
	}
	return scenarioCELValue(out), nil
}

func scenarioCELValue(value ref.Val) any {
	switch typed := value.(type) {
	case types.String:
		return string(typed)
	case types.Bool:
		return bool(typed)
	case types.Int:
		return int64(typed)
	case types.Uint:
		return uint64(typed)
	case types.Double:
		return float64(typed)
	default:
		return value.Value()
	}
}

func optionalScenarioString(value any) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(value))
}

func scenarioSHA40(value string) string {
	sum := sha1.Sum([]byte(value))
	return hex.EncodeToString(sum[:])
}

func scenarioUUID(seed, label string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(seed+"\x00"+label)).String()
}

func scenarioSetupEntityID(seed, runID string, entity evaluatedScenarioSetupEntity) string {
	if strings.TrimSpace(entity.FlowID) == "" {
		return runtimeflowidentity.EntityID(runID)
	}
	return scenarioUUID(seed, "setup.entity."+entity.Alias)
}

func (r scenarioRunner) runInvalidVariants(file scenarioTestFile, doc scenarioDocument, evaluator *scenarioExpressionEvaluator) error {
	baseStep, err := invalidBasePublishStep(doc.Invalid.Base)
	if err != nil {
		return fmt.Errorf("%s: invalid.base: %w", file.Path, err)
	}
	for _, item := range doc.Invalid.Cases {
		step := baseStep
		payloadSpec, _ := step.Payload.(map[string]any)
		payloadSpec = cloneAnyMap(payloadSpec)
		if payloadSpec == nil {
			payloadSpec = map[string]any{}
		}
		if len(item.Set) > 0 {
			payloadSet, _ := payloadSpec["set"].(map[string]any)
			if payloadSet == nil {
				payloadSet = map[string]any{}
			}
			for key, value := range item.Set {
				payloadSet[key] = value
			}
			payloadSpec["set"] = payloadSet
		}
		step.Payload = payloadSpec
		if _, err := r.buildPublishPayload(file, evaluator, step); err == nil {
			return fmt.Errorf("%s: invalid case %s unexpectedly passed pre-mutation validation", file.Path, item.Name)
		}
	}
	return nil
}

func invalidBasePublishStep(base map[string]any) (scenarioStep, error) {
	raw, ok := base["publish"]
	if !ok {
		return scenarioStep{}, fmt.Errorf("publish is required")
	}
	eventName := strings.TrimSpace(fmt.Sprint(raw))
	if eventName == "" {
		return scenarioStep{}, fmt.Errorf("publish must be non-empty")
	}
	return scenarioStep{Action: "publish", PublishEvent: eventName, Payload: base["payload"]}, nil
}

func (r scenarioRunner) runScenarioSetup(ctx context.Context, file scenarioTestFile, evaluator *scenarioExpressionEvaluator, state *scenarioRunState, setup scenarioSetup) error {
	if state.RunID != "" {
		return scenarioTestValidationError{err: fmt.Errorf("setup requires an empty run context")}
	}
	runID := scenarioUUID(evaluator.seed, "setup.run")
	params := map[string]any{
		"bundle_hash":     r.bundleHash,
		"run_id":          runID,
		"idempotency_key": scenarioSHA40(evaluator.seed + "\x00setup.entities"),
	}
	entities := make([]any, 0, len(setup.Entities))
	for _, entity := range setup.Entities {
		evaluated, err := r.evaluateScenarioSetupEntity(file, evaluator, entity)
		if err != nil {
			return scenarioTestValidationError{err: err}
		}
		entityID := scenarioSetupEntityID(evaluator.seed, runID, evaluated)
		entities = append(entities, map[string]any{
			"alias":         evaluated.Alias,
			"entity_id":     entityID,
			"flow_instance": evaluated.FlowID,
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
		value, err := evaluator.evalValue(entity.Flow)
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
	currentState := ""
	if entity.StateSet {
		value, err := evaluator.evalValue(entity.CurrentState)
		if err != nil {
			return evaluatedScenarioSetupEntity{}, fmt.Errorf("setup.entities[%s].current_state: %w", entity.Alias, err)
		}
		currentState = optionalScenarioString(value)
	} else {
		currentState = strings.TrimSpace(r.bundle.FlowInitialStage(flowID))
	}
	if currentState == "" {
		return evaluatedScenarioSetupEntity{}, fmt.Errorf("setup.entities[%s].current_state is required because flow %s has no initial state", entity.Alias, scenarioFlowLabel(flowID))
	}
	if !scenarioStringSliceContains(r.bundle.FlowStates(flowID), currentState) {
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
	value, err := evaluator.evalValue(entity.Fields)
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
	value, err := evaluator.evalValue(entity.Gates)
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
	for nodeID, node := range r.bundle.Nodes {
		source, ok := r.bundle.NodeContractSource(nodeID)
		if !ok || strings.Trim(strings.TrimSpace(source.FlowID), "/") != flowID {
			continue
		}
		for _, gate := range node.GateState.Gates {
			if name := strings.TrimSpace(gate.Name); name != "" {
				out[name] = struct{}{}
			}
		}
	}
	for _, transition := range r.bundle.DerivedHandlerTransitions() {
		if strings.Trim(strings.TrimSpace(transition.FlowID), "/") != flowID {
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
	case "mailbox.approve", "mailbox.reject", "mailbox.defer":
		return r.runMailboxStep(ctx, evaluator, state, step)
	default:
		return fmt.Errorf("unsupported action %q", step.Action)
	}
}

func (r scenarioRunner) runPublishStep(ctx context.Context, file scenarioTestFile, evaluator *scenarioExpressionEvaluator, state *scenarioRunState, step scenarioStep) error {
	payload, err := r.buildPublishPayload(file, evaluator, step)
	if err != nil {
		return scenarioTestValidationError{err: err}
	}
	eventName := r.scopedEventName(file.FlowID, step.PublishEvent)
	params := map[string]any{
		"event_name":  eventName,
		"payload":     payload,
		"bundle_hash": r.bundleHash,
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
		value, err := evaluator.evalValue(field.value)
		if err != nil {
			return fmt.Errorf("%s: %w", field.name, err)
		}
		if text := optionalScenarioString(value); text != "" {
			params[field.name] = text
		}
	}
	targetFlow, err := evaluator.evalValue(step.TargetFlowInstance)
	if err != nil {
		return fmt.Errorf("target_flow_instance: %w", err)
	}
	targetEntity, err := evaluator.evalValue(step.TargetEntityID)
	if err != nil {
		return fmt.Errorf("target_entity_id: %w", err)
	}
	targetFlowText := optionalScenarioString(targetFlow)
	targetEntityText := optionalScenarioString(targetEntity)
	if step.Target != nil {
		targetAliasValue, err := evaluator.evalValue(step.Target)
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
		if strings.TrimSpace(binding.FlowInstance) == "" {
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

func (r scenarioRunner) scopedEventName(flowID, eventName string) string {
	eventName = strings.TrimSpace(eventName)
	flowID = strings.Trim(strings.TrimSpace(flowID), "/")
	if flowID == "" || eventName == "" || strings.Contains(eventName, "/") {
		return eventName
	}
	return r.bundle.ResolveFlowEventReference(flowID, eventName)
}

func (r scenarioRunner) buildPublishPayload(file scenarioTestFile, evaluator *scenarioExpressionEvaluator, step scenarioStep) (map[string]any, error) {
	payload, err := r.buildPayloadFromSpec(file, evaluator, step.Payload)
	if err != nil {
		return nil, err
	}
	schema, _, ok := runtimecontracts.EventSchemaForFlowEvent(r.bundle, file.FlowID, step.PublishEvent)
	if !ok {
		return nil, fmt.Errorf("event %s has no target schema in contract bundle", step.PublishEvent)
	}
	if err := runtimeeventschema.ValidatePayloadAgainstSchema(schema.Schema, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func (r scenarioRunner) buildPayloadFromSpec(file scenarioTestFile, evaluator *scenarioExpressionEvaluator, spec any) (map[string]any, error) {
	if spec == nil {
		return nil, fmt.Errorf("payload is required")
	}
	spec, err := evaluator.evalValue(spec)
	if err != nil {
		return nil, err
	}
	m, ok := spec.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("payload must be an object")
	}
	var payload map[string]any
	if rawFrom, ok := m["from"]; ok {
		fixturePath := strings.TrimSpace(fmt.Sprint(rawFrom))
		if fixturePath == "" {
			return nil, fmt.Errorf("payload.from must be non-empty")
		}
		payload, err = r.loadFixturePayload(file.Path, fixturePath)
		if err != nil {
			return nil, err
		}
	} else {
		payload = cloneAnyMap(m)
	}
	if rawSet, ok := m["set"]; ok {
		set, ok := rawSet.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("payload.set must be a mapping")
		}
		for path, value := range set {
			value, err := evaluator.evalValue(value)
			if err != nil {
				return nil, fmt.Errorf("payload.set.%s: %w", path, err)
			}
			if err := setPathValue(payload, strings.TrimPrefix(path, "payload."), value); err != nil {
				return nil, err
			}
		}
	}
	delete(payload, "from")
	delete(payload, "set")
	return payload, nil
}

func (r scenarioRunner) loadFixturePayload(scenarioPath, rawPath string) (map[string]any, error) {
	path := rawPath
	if !filepath.IsAbs(path) {
		path = filepath.Join(filepath.Dir(scenarioPath), path)
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if !pathWithin(r.contractsDir, abs) {
		return nil, fmt.Errorf("payload.from %s escapes contract package root", rawPath)
	}
	realContractsDir, err := filepath.EvalSymlinks(r.contractsDir)
	if err != nil {
		return nil, fmt.Errorf("resolve contract package root: %w", err)
	}
	realPath, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return nil, fmt.Errorf("resolve payload.from %s: %w", rawPath, err)
	}
	if !pathWithin(realContractsDir, realPath) {
		return nil, fmt.Errorf("payload.from %s escapes contract package root", rawPath)
	}
	raw, err := os.ReadFile(realPath)
	if err != nil {
		return nil, fmt.Errorf("read payload.from %s: %w", rawPath, err)
	}
	var node yaml.Node
	if err := yaml.Unmarshal(raw, &node); err != nil {
		return nil, fmt.Errorf("parse payload.from %s: %w", rawPath, err)
	}
	if len(node.Content) == 0 {
		return nil, fmt.Errorf("payload.from %s is empty", rawPath)
	}
	payload, ok := yamlNodeValue(node.Content[0]).(map[string]any)
	if !ok {
		return nil, fmt.Errorf("payload.from %s must contain an object", rawPath)
	}
	return payload, nil
}

func pathWithin(base, path string) bool {
	base, err := filepath.Abs(base)
	if err != nil {
		return false
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return false
	}
	rel, err := filepath.Rel(base, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func setPathValue(root map[string]any, rawPath string, value any) error {
	parts := strings.Split(strings.TrimSpace(rawPath), ".")
	if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" {
		return fmt.Errorf("payload.set path is required")
	}
	cursor := root
	for i, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return fmt.Errorf("payload.set path %q contains an empty segment", rawPath)
		}
		if i == len(parts)-1 {
			if value == nil {
				delete(cursor, part)
			} else {
				cursor[part] = value
			}
			return nil
		}
		next, _ := cursor[part].(map[string]any)
		if next == nil {
			next = map[string]any{}
			cursor[part] = next
		}
		cursor = next
	}
	return nil
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
	if key, err := evaluator.evalValue(step.IdempotencyKey); err != nil {
		return fmt.Errorf("idempotency_key: %w", err)
	} else if text := optionalScenarioString(key); text != "" {
		params["idempotency_key"] = text
	}
	switch step.Action {
	case "mailbox.approve":
		payload, err := evaluator.evalValue(step.Payload)
		if err != nil {
			return scenarioTestValidationError{err: fmt.Errorf("payload: %w", err)}
		}
		if payload != nil {
			m, ok := payload.(map[string]any)
			if !ok {
				return scenarioTestValidationError{err: fmt.Errorf("mailbox.approve payload must be an object")}
			}
			params["decision_payload"] = m
		}
	case "mailbox.reject":
		reason, err := evaluator.evalValue(step.Reason)
		if err != nil {
			return scenarioTestValidationError{err: fmt.Errorf("reason: %w", err)}
		}
		text := optionalScenarioString(reason)
		if text == "" {
			return scenarioTestValidationError{err: fmt.Errorf("mailbox.reject reason is required")}
		}
		params["reason"] = text
	case "mailbox.defer":
		until, err := evaluator.evalValue(step.Until)
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
	mailboxID, err := r.findMailboxID(ctx, evaluator, state.RunID, step.Match)
	if err != nil {
		return err
	}
	params["mailbox_id"] = mailboxID
	var result mailboxDecisionResult
	if err := r.client.call(ctx, step.Action, params, &result); err != nil {
		return err
	}
	if err := validateMailboxDecisionResult(strings.TrimPrefix(step.Action, "mailbox."), result); err != nil {
		return err
	}
	if result.DownstreamEventID != "" {
		state.LastEventID = result.DownstreamEventID
	}
	return nil
}

func (r scenarioRunner) findMailboxID(ctx context.Context, evaluator *scenarioExpressionEvaluator, runID string, match map[string]any) (string, error) {
	params := map[string]any{
		"status": "pending",
		"run_id": runID,
		"limit":  200,
	}
	for key, value := range match {
		evaluated, err := evaluator.evalValue(value)
		if err != nil {
			return "", fmt.Errorf("match.%s: %w", key, err)
		}
		text := strings.TrimSpace(fmt.Sprint(evaluated))
		if text == "" {
			continue
		}
		switch key {
		case "type":
			params["type"] = text
		case "priority":
			params["priority"] = text
		case "entity_id":
			params["entity_id"] = text
		default:
			return "", fmt.Errorf("unsupported mailbox match field %q", key)
		}
	}
	var result mailboxListResult
	if err := r.client.call(ctx, "mailbox.list", params, &result); err != nil {
		return "", err
	}
	if err := validateMailboxListResult(result); err != nil {
		return "", err
	}
	if len(result.Items) != 1 {
		return "", fmt.Errorf("mailbox match for run %s returned %d items, want exactly one", runID, len(result.Items))
	}
	return result.Items[0].MailboxID, nil
}

func (r scenarioRunner) waitForQuiescence(ctx context.Context, runID string) error {
	deadline := time.Now().Add(r.timeout)
	for {
		quiescence, err := r.readTestQuiescence(ctx, runID)
		if err != nil {
			return err
		}
		if boolPointerValue(quiescence.Ready) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("test quiescence deadline reached for run %s with active_deliveries=%d unsettled_pipeline_events=%d due_timers=%d active_session_leases=%d",
				runID,
				intPointerValue(quiescence.ActiveDeliveries),
				intPointerValue(quiescence.UnsettledPipelineEvents),
				intPointerValue(quiescence.DueTimers),
				intPointerValue(quiescence.ActiveSessionLeases),
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
	var result diagnosticRunDiagnosisResult
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
	rows, err := r.fetchRunTraceRows(ctx, runID)
	if err != nil {
		return err
	}
	names := uniqueScenarioTraceEventNames(rows)
	if err := assertScenarioEventExpectations(names, expect.Events); err != nil {
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
				return fmt.Errorf("expected event %s was not observed", want)
			}
		}
	}
	if len(expect.Exact) > 0 {
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
				return fmt.Errorf("expected no dead letters for run %s, found event %s", runID, event.EventID)
			}
			for _, delivery := range event.Deliveries {
				if strings.TrimSpace(delivery.Status) == "dead_letter" || len(delivery.DeadLetters) > 0 {
					return fmt.Errorf("expected no dead letters for run %s, found delivery %s on event %s", runID, delivery.DeliveryID, event.EventID)
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
	if !expect.hasDetailAssertion() {
		return nil
	}
	if count != 1 {
		return fmt.Errorf("entity detail expectation for type %s returned %d entities, want exactly one", expect.EntityType, count)
	}
	return r.assertEntityDetailExpectation(ctx, runID, entities[0].EntityID, expect.EntityType, evaluator, expect)
}

func (r scenarioRunner) assertEntityDetailExpectation(ctx context.Context, runID string, entityID string, entityType string, evaluator *scenarioExpressionEvaluator, expect scenarioEntityExpect) error {
	detail, err := expect.evaluatedDetail(evaluator)
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

func (e scenarioEntityExpect) evaluatedDetail(evaluator *scenarioExpressionEvaluator) (evaluatedScenarioEntityDetail, error) {
	var out evaluatedScenarioEntityDetail
	if evaluator == nil {
		return out, fmt.Errorf("scenario expression evaluator is required for entity detail assertions")
	}
	if e.StateSet {
		value, err := evaluator.evalValue(e.CurrentState)
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
		value, err := evaluator.evalValue(e.Fields)
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
		value, err := evaluator.evalValue(e.Gates)
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
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	var normalized any
	if err := json.Unmarshal(raw, &normalized); err != nil {
		return "", err
	}
	out, err := json.Marshal(normalized)
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
	fmt.Fprintln(errOut, err)
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
			"MAILBOX_APPROVAL_EVENT_UNCONFIGURED",
		},
	})
}
