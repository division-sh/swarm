package runtimepersistence

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type handoffRetryContract struct {
	path, symbol, transaction string
	resultReturn              string
	prefix                    []string
}

func handoffRetryContracts() []handoffRetryContract {
	return []handoffRetryContract{
		{path: "llmpersistence/sqlite_sessions.go", symbol: "LLMSQLiteOwner.acquireSQLiteLiveSession", transaction: "s.runRuntimeMutationOutcome"},
		{path: "llmpersistence/sqlite_sessions.go", symbol: "LLMSQLiteOwner.Release", transaction: "s.runRuntimeMutationOutcome"},
		{path: "llmpersistence/sqlite_sessions.go", symbol: "LLMSQLiteOwner.Rotate", transaction: "s.runRuntimeMutationOutcome"},
		{path: "llmpersistence/sqlite_sessions.go", symbol: "LLMSQLiteOwner.AdoptSessionID", transaction: "s.runRuntimeMutationOutcome"},
		{path: "llmpersistence/sqlite_sessions.go", symbol: "LLMSQLiteOwner.ResetAll", transaction: "s.runRuntimeMutationOutcome", prefix: []string{"summary = runtimesessions.ResetSummary{}"}},
		{path: "decisionpersistence/human_task_cards.go", symbol: "DecisionSQLiteOwner.CompleteHumanTaskOutcome", transaction: "s.runDecisionCardMutationOutcome"},
		{path: "decisionpersistence/proposed_effect_cards.go", symbol: "DecisionSQLiteOwner.CompleteProposedEffectRoute", transaction: "s.runDecisionCardMutationOutcome"},
		{path: "decisionpersistence/proposed_effect_cards.go", symbol: "DecisionSQLiteOwner.SupersedeProposedEffectsForLoopGenerations", transaction: "s.runDecisionCardMutationOutcome"},
		{path: "decisionpersistence/decision_cards.go", symbol: "DecisionSQLiteOwner.SupersedeDecisionCardsForStage", transaction: "s.runDecisionCardMutationOutcome"},
		{path: "eventpersistence/event_commit.go", symbol: "commitPublication", transaction: "run"},
		{path: "eventpersistence/event_commit.go", symbol: "EventSQLiteOwner.CommitAPIEventPublication", transaction: "s.runPrivateAuthorActivityMutationOutcome"},
		{path: "eventpersistence/sqlite_inbound_publication.go", symbol: "EventSQLiteOwner.CommitInboundPublication", transaction: "s.runPrivateAuthorActivityMutationOutcome"},
		{path: "delivery/lifecycle.go", symbol: "DeliverySQLiteOwner.SettleSuccess", transaction: "sqliteDeliveryMutationOutcome", resultReturn: "runtimedelivery.Snapshot{}, err"},
		{path: "delivery/lifecycle.go", symbol: "DeliverySQLiteOwner.SettleFailure", transaction: "sqliteDeliveryMutationOutcome", resultReturn: "runtimedelivery.Snapshot{}, err"},
		{path: "effectpersistence/completion_settlement.go", symbol: "EffectSQLiteOwner.SettleCompletion", transaction: "s.runPrivateAuthorActivityMutationOutcome"},
		{path: "effectpersistence/runtime_external_effects.go", symbol: "EffectSQLiteOwner.ReconcileExternalEffectAttempts", transaction: "s.runPrivateAuthorActivityMutationOutcome"},
		{path: "effectpersistence/runtime_external_effects.go", symbol: "EffectSQLiteOwner.SettleExternalAttempt", transaction: "s.runPrivateAuthorActivityMutationOutcome"},
		{path: "runlifecycle/run_control_sqlite.go", symbol: "RunLifecycleSQLiteOwner.runControlTransition", transaction: "mutate"},
		{path: "runlifecycle/run_lifecycle_candidates.go", symbol: "RunLifecycleSQLiteOwner.RequestCompletionCandidate", transaction: "s.runRuntimeMutationOutcome"},
		{path: "runlifecycle/run_lifecycle_mutation_adapter.go", symbol: "runSQLiteLifecycleOperation", transaction: "store.runPrivateAuthorActivityMutationOutcome"},
		{path: "pipelinepersistence/owner_operations.go", symbol: "sqlitePipelineObligationStore.MarkDecisionProcessed", transaction: "s.runRuntimeMutation"},
		{path: "pipelinepersistence/owner_operations.go", symbol: "sqlitePipelineObligationStore.Settle", transaction: "s.runRuntimeMutation"},
		{path: "pipelinepersistence/standing_service.go", symbol: "newSQLiteStandingServiceAdapter", transaction: "store.runPrivateAuthorActivityMutationOutcome"},
		{path: "pipelinepersistence/publication_group.go", symbol: "publicationGroup.Settle", transaction: "g.sqlite.runRuntimeMutationOutcome"},
		{path: "pipelinepersistence/workflow_engine_mutation_commit.go", symbol: "commitWorkflowEngineMutation", transaction: "run"},
		{path: "pipelinepersistence/workflow_timer_activation.go", symbol: "commitWorkflowTimerReconciliation", transaction: "run"},
		{path: "pipelinepersistence/generic_schedule_occurrence_commit.go", symbol: "commitGenericScheduleOccurrence", transaction: "run"},
		{path: "pipelinepersistence/workflow_timer_occurrence_commit.go", symbol: "commitWorkflowTimerOccurrence", transaction: "run"},
		{path: "pipelinepersistence/workflow_decision_route_commit.go", symbol: "commitProposedEffectRoute", transaction: "run"},
		{path: "pipelinepersistence/fan_out_owner.go", symbol: "commitFanOutChunk", transaction: "run", prefix: []string{
			"result = runtimepipeline.CommittedFanOutChunk{Publications: make([]runtimeengine.CommittedDurablePublication, 0, len(command.Outcomes))}",
			"operationComplete = false",
		}},
		{path: "runforkpersistence/run_fork_activation.go", symbol: "RunForkSQLiteOwner.ActivateRunFork", transaction: "s.backend.RunTransactionOutcome"},
		{path: "runforkpersistence/run_fork_selected_contract_activation_owner.go", symbol: "activateRunForkForSelectedContractExecution", transaction: "port.runMutation"},
	}
}

// These are concrete nonreplaying PostgreSQL siblings, not a path/name-based
// exemption. A newly introduced owner must be classified explicitly.
func handoffNonretryOwners() map[string]bool {
	rows := map[string][]string{
		"llmpersistence/postgres_sessions.go":            {"LLMPostgresOwner.acquirePostgresLiveSession", "LLMPostgresOwner.Release", "LLMPostgresOwner.Rotate", "LLMPostgresOwner.AdoptSessionID", "LLMPostgresOwner.ResetAll"},
		"decisionpersistence/decision_cards.go":          {"DecisionPostgresOwner.SupersedeDecisionCardsForStage"},
		"decisionpersistence/human_task_cards.go":        {"DecisionPostgresOwner.CompleteHumanTaskOutcome"},
		"decisionpersistence/proposed_effect_cards.go":   {"DecisionPostgresOwner.CompleteProposedEffectRoute", "DecisionPostgresOwner.SupersedeProposedEffectsForLoopGenerations"},
		"eventpersistence/event_commit.go":               {"EventPostgresOwner.CommitAPIEventPublication"},
		"eventpersistence/inbound_publication.go":        {"EventPostgresOwner.CommitInboundPublication"},
		"delivery/lifecycle.go":                          {"DeliveryPostgresOwner.SettleSuccess", "DeliveryPostgresOwner.SettleFailure"},
		"effectpersistence/completion_settlement.go":     {"EffectPostgresOwner.SettleCompletion"},
		"effectpersistence/runtime_external_effects.go":  {"EffectPostgresOwner.ReconcileExternalEffectAttempts", "EffectPostgresOwner.SettleExternalAttempt"},
		"runlifecycle/run_control.go":                    {"RunLifecyclePostgresOwner.runControlTransition"},
		"runlifecycle/run_lifecycle_candidates.go":       {"RunLifecyclePostgresOwner.RequestCompletionCandidate"},
		"runlifecycle/run_lifecycle_mutation_adapter.go": {"runPostgresLifecycleOperation"},
		"pipelinepersistence/owner_operations.go":        {"postgresPipelineObligationStore.MarkDecisionProcessed", "postgresPipelineObligationStore.Settle"},
		"pipelinepersistence/standing_service.go":        {"newPostgresStandingServiceAdapter"},
		"runforkpersistence/run_fork_activation.go":      {"RunForkPostgresOwner.ActivateRunFork"},
	}
	out := map[string]bool{}
	for path, symbols := range rows {
		for _, symbol := range symbols {
			out[path+":"+symbol] = true
		}
	}
	return out
}

func handoffFunction(source, symbol string) (*ast.FuncDecl, error) {
	functions, err := revisionGuardFunctions(source)
	if err != nil {
		return nil, err
	}
	fn := functions[symbol]
	if fn == nil {
		return nil, fmt.Errorf("missing handoff owner %s", symbol)
	}
	return fn, nil
}

func validateHandoffRetryEntry(source string, c handoffRetryContract) error {
	fn, err := handoffFunction(source, c.symbol)
	if err != nil {
		return err
	}
	var callbacks []*ast.FuncLit
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || revisionGuardNode(call.Fun) != c.transaction {
			return true
		}
		for _, arg := range call.Args {
			if callback, ok := arg.(*ast.FuncLit); ok {
				callbacks = append(callbacks, callback)
			}
			if id, ok := arg.(*ast.Ident); ok && id.Obj != nil {
				if assign, ok := id.Obj.Decl.(*ast.AssignStmt); ok {
					for _, rhs := range assign.Rhs {
						if callback, ok := rhs.(*ast.FuncLit); ok {
							callbacks = append(callbacks, callback)
						}
					}
				}
			}
		}
		return true
	})
	if len(callbacks) != 1 {
		return fmt.Errorf("%s: expected one exact %s callback, got %d", c.symbol, c.transaction, len(callbacks))
	}
	returns := c.resultReturn
	if returns == "" {
		returns = "err"
	}
	wantSource := "package p; func wanted() { if err := handoff.ResetAttempt(); err != nil { return " + returns + " };" + strings.Join(c.prefix, ";") + " }"
	want, err := handoffFunction(wantSource, "wanted")
	if err != nil {
		return err
	}
	actual := callbacks[0].Body.List
	if len(actual) < len(want.Body.List) {
		return fmt.Errorf("%s: missing attempt reset prefix", c.symbol)
	}
	for i, statement := range want.Body.List {
		if revisionGuardNode(actual[i]) != revisionGuardNode(statement) {
			return fmt.Errorf("%s: attempt prefix statement %d differs: %s", c.symbol, i, revisionGuardNode(actual[i]))
		}
	}
	resets := 0
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok && revisionGuardNode(call.Fun) == "handoff.ResetAttempt" {
			resets++
		}
		return true
	})
	if resets != 1 {
		return fmt.Errorf("%s: reset must occur once per outer callback, got %d", c.symbol, resets)
	}
	return nil
}

func handoffReservationOwner(fn *ast.FuncDecl) bool {
	generic := false
	for _, field := range fn.Type.Params.List {
		factory, ok := field.Type.(*ast.FuncType)
		if !ok || factory.Results == nil || len(factory.Results.List) == 0 || revisionGuardNode(factory.Results.List[0].Type) != "*runLifecycleCandidateHandoffReservation" {
			continue
		}
		for _, name := range field.Names {
			if name.Name == "reserve" {
				generic = true
			}
		}
	}
	found := false
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		expression := call.Fun
		if indexed, ok := expression.(*ast.IndexExpr); ok {
			expression = indexed.X
		}
		name := revisionGuardNode(expression)
		base := name[strings.LastIndex(name, ".")+1:]
		switch base {
		case "ReserveCandidateHandoff", "reserveRunLifecycleCandidateHandoff", "WithCandidateHandoffOutcome", "WithCandidateHandoffOutcomeResult", "withRunLifecycleCandidateHandoffOutcome", "withRunLifecycleCandidateHandoffResult":
			found = true
		case "reserve":
			if generic && name == "reserve" {
				found = true
			}
		}
		return true
	})
	return found
}

func validateHandoffOwnerCensus(sources map[string]string, contracts []handoffRetryContract, nonretry map[string]bool) error {
	expected := map[string]bool{}
	for _, c := range contracts {
		key := c.path + ":" + c.symbol
		if expected[key] || nonretry[key] {
			return fmt.Errorf("duplicate handoff classification: %s", key)
		}
		expected[key] = true
	}
	for key := range nonretry {
		expected[key] = true
	}
	// Forwarders must remain in their exact files; their operation callers are
	// the concrete census rows. They do not own a replayable callback.
	forwarders := map[string]bool{
		"decisionpersistence/helpers.go:reserveRunLifecycleCandidateHandoff":     true,
		"decisionpersistence/helpers.go:withRunLifecycleCandidateHandoffOutcome": true,
		"eventpersistence/owner.go:reserveRunLifecycleCandidateHandoff":          true,
		"eventpersistence/owner.go:withRunLifecycleCandidateHandoffResult":       true,
		"pipelinepersistence/owner.go:reserveRunLifecycleCandidateHandoff":       true,
		"runforkpersistence/owner.go:reserveRunLifecycleCandidateHandoff":        true,
		"runlifecycle/candidate_aliases.go:ReserveCandidateHandoff":              true,
		"runlifecycle/candidate_aliases.go:WithCandidateHandoffOutcomeResult":    true,
	}
	seen := map[string]bool{}
	for path, source := range sources {
		file, err := parser.ParseFile(token.NewFileSet(), path, source, 0)
		if err != nil {
			return err
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || !handoffReservationOwner(fn) {
				continue
			}
			symbol := fn.Name.Name
			if fn.Recv != nil {
				symbol = strings.TrimPrefix(revisionGuardNode(fn.Recv.List[0].Type), "*") + "." + symbol
			}
			key := path + ":" + symbol
			if forwarders[key] {
				continue
			}
			if !expected[key] {
				return fmt.Errorf("unclassified handoff reservation owner: %s", key)
			}
			seen[key] = true
		}
	}
	for key := range expected {
		if !seen[key] {
			return fmt.Errorf("classified handoff owner disappeared: %s", key)
		}
	}
	return nil
}

func TestCandidateHandoffRetryCallbackCensus(t *testing.T) {
	root := filepath.Join(repoRootForRuntimeWriterGuard(t), "internal/store/internal/backend")
	sources := map[string]string{}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		sources[filepath.ToSlash(rel)] = string(body)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	contracts := handoffRetryContracts()
	if len(contracts) != 32 {
		t.Fatalf("retry callback census=%d want32", len(contracts))
	}
	for _, c := range contracts {
		t.Run(c.symbol, func(t *testing.T) {
			if err := validateHandoffRetryEntry(sources[c.path], c); err != nil {
				t.Fatal(err)
			}
		})
	}
	if err := validateHandoffOwnerCensus(sources, contracts, handoffNonretryOwners()); err != nil {
		t.Fatal(err)
	}
}

func TestCandidateHandoffRetryCallbackGuardHostileControls(t *testing.T) {
	reset := "if err := handoff.ResetAttempt(); err != nil { return err };"
	valid := "package p; func write() { handoff, _ := ReserveCandidateHandoff(ctx); run(ctx, func() error { " + reset + " mutate(); return nil }) }"
	c := handoffRetryContract{path: "owner.go", symbol: "write", transaction: "run"}
	for _, row := range []struct {
		name, source string
		invalid      bool
	}{
		{"valid", valid, false},
		{"missing", strings.Replace(valid, reset, "", 1), true},
		{"late", strings.Replace(valid, reset, "mutate();"+reset, 1), true},
		{"ignored_error", strings.Replace(valid, reset, "_ = handoff.ResetAttempt();", 1), true},
		{"swallowed_error", strings.Replace(valid, "return err", "return nil", 1), true},
		{"wrong_owner", strings.Replace(valid, "handoff.ResetAttempt", "other.ResetAttempt", 1), true},
		{"conditional", strings.Replace(valid, reset, "if enabled {"+reset+"};", 1), true},
		{"nested_callback", strings.Replace(valid, reset, "defer func(){"+reset+"}();", 1), true},
		{"outside_callback", strings.Replace(strings.Replace(valid, reset, "", 1), "run(ctx", reset+"run(ctx", 1), true},
		{"per_candidate_second_reset", strings.Replace(valid, "mutate();", "mutate();"+reset, 1), true},
		{"wrong_transaction", strings.Replace(valid, "run(ctx", "otherRun(ctx", 1), true},
		{"duplicate_callback", strings.Replace(valid, "mutate();", "run(ctx, func() error {"+reset+"return nil});", 1), true},
	} {
		t.Run(row.name, func(t *testing.T) {
			if err := validateHandoffRetryEntry(row.source, c); (err != nil) != row.invalid {
				t.Fatalf("error=%v invalid=%v", err, row.invalid)
			}
		})
	}
	t.Run("named_callback", func(t *testing.T) {
		source := "package p; func write() { operation := func() error {" + reset + "return nil}; run(ctx, operation) }"
		if err := validateHandoffRetryEntry(source, c); err != nil {
			t.Fatal(err)
		}
		if err := validateHandoffRetryEntry(strings.Replace(source, reset, "", 1), c); err == nil {
			t.Fatal("missing named callback reset accepted")
		}
	})
	t.Run("fanout_attempt_outputs", func(t *testing.T) {
		fanout := handoffRetryContracts()[29]
		fanout.symbol = "write"
		source := strings.Replace(valid, "mutate();", strings.Join(fanout.prefix, ";")+";mutate();", 1)
		if err := validateHandoffRetryEntry(source, fanout); err != nil {
			t.Fatal(err)
		}
		for _, prefix := range fanout.prefix {
			if err := validateHandoffRetryEntry(strings.Replace(source, prefix+";", "", 1), fanout); err == nil {
				t.Fatalf("omitted %s accepted", prefix)
			}
		}
	})
	t.Run("new_or_omitted_owner", func(t *testing.T) {
		sources := map[string]string{"owner.go": valid}
		if err := validateHandoffOwnerCensus(sources, []handoffRetryContract{c}, nil); err != nil {
			t.Fatal(err)
		}
		if err := validateHandoffOwnerCensus(sources, nil, nil); err == nil {
			t.Fatal("unclassified owner accepted")
		}
		sources["owner.go"] += "; func newWriter(){ h, _ := ReserveCandidateHandoff(ctx); _ = h }"
		if err := validateHandoffOwnerCensus(sources, []handoffRetryContract{c}, nil); err == nil {
			t.Fatal("new owner accepted")
		}
	})
	t.Run("dialect_and_duplicate", func(t *testing.T) {
		if err := validateHandoffOwnerCensus(map[string]string{"owner.go": valid}, []handoffRetryContract{c, c}, nil); err == nil {
			t.Fatal("duplicate row accepted")
		}
		if err := validateHandoffOwnerCensus(map[string]string{"postgres.go": valid}, []handoffRetryContract{c}, nil); err == nil {
			t.Fatal("foreign file concealed missing owner")
		}
	})
}
