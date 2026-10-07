package pipeline

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

type writerBoundaryViolation struct {
	code string
	pos  token.Pos
}

var rawInstanceWriteSQL = regexp.MustCompile(`(?i)\b(?:update|insert\s+into|delete\s+from)\s+(?:entity_state|flow_instances)\b`)

func writerBoundaryCall(call *ast.CallExpr, name string) bool {
	switch fn := call.Fun.(type) {
	case *ast.Ident:
		return fn.Name == name
	case *ast.SelectorExpr:
		return fn.Sel.Name == name
	}
	return false
}

func writerBoundaryIdent(expr ast.Expr, binding *ast.Ident) bool {
	name, ok := expr.(*ast.Ident)
	return ok && binding != nil && name.Obj != nil && name.Obj == binding.Obj
}

func writerBoundaryField(expr ast.Expr, binding *ast.Ident, field string) bool {
	selector, ok := expr.(*ast.SelectorExpr)
	return ok && selector.Sel.Name == field && writerBoundaryIdent(selector.X, binding)
}

func writerBoundaryReceiver(fn *ast.FuncDecl, name string) bool {
	if fn.Recv == nil || len(fn.Recv.List) != 1 {
		return false
	}
	typ := fn.Recv.List[0].Type
	if pointer, ok := typ.(*ast.StarExpr); ok {
		typ = pointer.X
	}
	identifier, ok := typ.(*ast.Ident)
	return ok && identifier.Name == name
}

func writerBoundaryAssignment(fn *ast.FuncDecl, value func(ast.Expr) bool) *ast.Ident {
	var binding *ast.Ident
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if _, nested := node.(*ast.FuncLit); nested {
			return false
		}
		assignment, ok := node.(*ast.AssignStmt)
		if !ok || assignment.Tok != token.DEFINE || len(assignment.Rhs) != 1 || len(assignment.Lhs) == 0 || !value(assignment.Rhs[0]) {
			return true
		}
		binding, _ = assignment.Lhs[0].(*ast.Ident)
		return false
	})
	return binding
}

func writerBoundaryApplicationCall(expr ast.Expr, method string) bool {
	call, ok := expr.(*ast.CallExpr)
	if !ok {
		return false
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != method {
		return false
	}
	receiver, ok := selector.X.(*ast.Ident)
	return ok && receiver.Name == "application"
}

func writerBoundaryRejects(body *ast.BlockStmt, cause *ast.Ident) bool {
	for _, statement := range body.List {
		ret, ok := statement.(*ast.ReturnStmt)
		if !ok || len(ret.Results) != 2 {
			continue
		}
		failure := ret.Results[1]
		if cause != nil {
			return writerBoundaryIdent(failure, cause)
		}
		if name, ok := failure.(*ast.Ident); !ok || name.Name != "nil" {
			return true
		}
	}
	return false
}

func writerBoundaryApplicationCheck(branch *ast.IfStmt, method string) bool {
	if !writerBoundaryRejects(branch.Body, nil) {
		return false
	}
	if method == "previewOnly" {
		return writerBoundaryApplicationCall(branch.Cond, method)
	}
	if method == "identity" {
		condition, ok := branch.Cond.(*ast.BinaryExpr)
		if !ok || condition.Op != token.LOR {
			return false
		}
		route, routeOK := condition.X.(*ast.BinaryExpr)
		entity, entityOK := condition.Y.(*ast.BinaryExpr)
		return routeOK && entityOK && route.Op == token.NEQ && entity.Op == token.NEQ &&
			writerBoundaryApplicationCall(route.Y, "Route") && writerBoundaryApplicationCall(entity.Y, "EntityID")
	}
	assignment, ok := branch.Init.(*ast.AssignStmt)
	if !ok || len(assignment.Lhs) != 1 || len(assignment.Rhs) != 1 || !writerBoundaryApplicationCall(assignment.Rhs[0], "Validate") {
		return false
	}
	cause, ok := assignment.Lhs[0].(*ast.Ident)
	if !ok {
		return false
	}
	condition, ok := branch.Cond.(*ast.BinaryExpr)
	if !ok || condition.Op != token.NEQ || !writerBoundaryIdent(condition.X, cause) {
		return false
	}
	nilValue, ok := condition.Y.(*ast.Ident)
	return ok && nilValue.Name == "nil" && writerBoundaryRejects(branch.Body, cause)
}

func writerBoundaryR1Preparation(fn *ast.FuncDecl) []writerBoundaryViolation {
	var violations []writerBoundaryViolation
	var mutation *ast.Ident
	for _, parameter := range fn.Type.Params.List {
		for _, name := range parameter.Names {
			if name.Name == "engineMutation" {
				mutation = name
			}
		}
	}
	evaluated := writerBoundaryAssignment(fn, func(expr ast.Expr) bool {
		call, ok := expr.(*ast.CallExpr)
		return ok && writerBoundaryCall(call, "evaluatedWorkflowInstance") && len(call.Args) == 3 && writerBoundaryField(call.Args[2], mutation, "EvaluatedState")
	})
	current := writerBoundaryAssignment(fn, func(expr ast.Expr) bool {
		call, ok := expr.(*ast.CallExpr)
		return ok && writerBoundaryCall(call, "cloneWorkflowInstanceForEngineMutation") && len(call.Args) == 1 && writerBoundaryField(call.Args[0], evaluated, "instance")
	})
	if evaluated == nil || current == nil {
		violations = append(violations, writerBoundaryViolation{"r1_projection", fn.Pos()})
	}
	stateFence, revisionFence := false, false
	applicationChecks := map[string]bool{"Validate": false, "Route": false, "EntityID": false, "previewOnly": false}
	applicationRejections := map[string]bool{"Validate": false, "identity": false, "previewOnly": false}
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if branch, ok := node.(*ast.IfStmt); ok {
			for check := range applicationRejections {
				applicationRejections[check] = applicationRejections[check] || writerBoundaryApplicationCheck(branch, check)
			}
		}
		if assignment, ok := node.(*ast.AssignStmt); ok && len(assignment.Lhs) == 1 && len(assignment.Rhs) == 1 {
			if name, ok := assignment.Lhs[0].(*ast.Ident); ok {
				switch name.Name {
				case "expectedRevision":
					revisionFence = writerBoundaryField(assignment.Rhs[0], current, "Revision")
					if !revisionFence {
						violations = append(violations, writerBoundaryViolation{"r1_revision", assignment.Pos()})
					}
				case "expectedState":
					call, ok := assignment.Rhs[0].(*ast.CallExpr)
					stateFence = ok && writerBoundaryCall(call, "TrimSpace") && len(call.Args) == 1 && writerBoundaryField(call.Args[0], current, "CurrentState")
				}
			}
		}
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		for _, forbidden := range []string{"Load", "LoadState", "LoadTargetPersistence", "LoadWorkflowTargetPersistence", "LoadWorkflowInstance", "loadCurrentDeliveryTargetState", "ensureFlowOwnsEntity", "QueryRowContext", "QueryContext"} {
			if writerBoundaryCall(call, forbidden) {
				violations = append(violations, writerBoundaryViolation{"prepare_reread", call.Pos()})
			}
		}
		if selector, ok := call.Fun.(*ast.SelectorExpr); ok {
			if receiver, ok := selector.X.(*ast.Ident); ok && receiver.Name == "application" {
				if _, required := applicationChecks[selector.Sel.Name]; required {
					applicationChecks[selector.Sel.Name] = true
				}
			}
		}
		return true
	})
	if !revisionFence {
		violations = append(violations, writerBoundaryViolation{"r1_revision", fn.Pos()})
	}
	if !stateFence {
		violations = append(violations, writerBoundaryViolation{"r1_state", fn.Pos()})
	}
	for check, present := range applicationChecks {
		if !present {
			violations = append(violations, writerBoundaryViolation{"application_" + check, fn.Pos()})
		}
	}
	for check, present := range applicationRejections {
		if !present {
			violations = append(violations, writerBoundaryViolation{"application_reject_" + check, fn.Pos()})
		}
	}
	return violations
}

// This is a finite lexical guard, not a general control-flow or interprocedural proof.
func writerBoundaryEntityFence(fn *ast.FuncDecl) []writerBoundaryViolation {
	var violations []writerBoundaryViolation
	var unlock *ast.Ident
	var lockedAt token.Pos
	for _, statement := range fn.Body.List {
		assignment, ok := statement.(*ast.AssignStmt)
		if !ok || assignment.Tok != token.DEFINE || len(assignment.Lhs) != 1 || len(assignment.Rhs) != 1 {
			continue
		}
		call, ok := assignment.Rhs[0].(*ast.CallExpr)
		if !ok || !writerBoundaryCall(call, "lockWorkflowEntity") {
			continue
		}
		unlock, _ = assignment.Lhs[0].(*ast.Ident)
		lockedAt = call.Pos()
	}
	if unlock == nil {
		return []writerBoundaryViolation{{"entity_lock", fn.Pos()}}
	}
	releases := map[*ast.Object]bool{unlock.Obj: true}
	for _, statement := range fn.Body.List {
		assignment, ok := statement.(*ast.AssignStmt)
		if !ok || len(assignment.Lhs) != 1 || len(assignment.Rhs) != 1 {
			continue
		}
		closure, ok := assignment.Rhs[0].(*ast.FuncLit)
		if !ok {
			continue
		}
		ast.Inspect(closure.Body, func(node ast.Node) bool {
			if call, ok := node.(*ast.CallExpr); ok && writerBoundaryIdent(call.Fun, unlock) {
				if name, ok := assignment.Lhs[0].(*ast.Ident); ok {
					releases[name.Obj] = true
				}
			}
			return true
		})
	}
	var reads, commits, released, finalizers []token.Pos
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		switch node.(type) {
		case *ast.FuncLit, *ast.DeferStmt:
			return false
		}
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		if name, ok := call.Fun.(*ast.Ident); ok && releases[name.Obj] {
			released = append(released, call.Pos())
		}
		for _, reader := range []string{"AuthorizeAcceptedEvent", "Load", "LoadState", "LoadTargetPersistence"} {
			if writerBoundaryCall(call, reader) {
				reads = append(reads, call.Pos())
			}
		}
		if writerBoundaryCall(call, "CommitWorkflowEngineMutation") {
			commits = append(commits, call.Pos())
		}
		for _, finalizer := range []string{"FinalizeEnginePublications", "finalizeWorkflowLifecycleMutation", "DispatchPostCommit", "Commit"} {
			if writerBoundaryCall(call, finalizer) {
				finalizers = append(finalizers, call.Pos())
			}
		}
		return true
	})
	if len(reads) == 0 || len(commits) != 1 {
		return []writerBoundaryViolation{{"fence_inventory", fn.Pos()}}
	}
	for _, read := range reads {
		if read < lockedAt {
			violations = append(violations, writerBoundaryViolation{"read_before_lock", read})
		}
	}
	commit := commits[0]
	if commit < lockedAt {
		violations = append(violations, writerBoundaryViolation{"commit_before_lock", commit})
	}
	postCommitRelease := token.NoPos
	for _, release := range released {
		if release < commit {
			violations = append(violations, writerBoundaryViolation{"unlock_before_commit", release})
		} else if postCommitRelease == token.NoPos || release < postCommitRelease {
			postCommitRelease = release
		}
	}
	if postCommitRelease == token.NoPos {
		violations = append(violations, writerBoundaryViolation{"postcommit_unlock", commit})
	}
	for _, finalizer := range finalizers {
		if postCommitRelease == token.NoPos || finalizer < postCommitRelease {
			violations = append(violations, writerBoundaryViolation{"finalize_before_unlock", finalizer})
		}
	}
	return violations
}

func writerBoundaryRejectsOrdinaryPostCommit(fn *ast.FuncDecl) bool {
	if len(fn.Recv.List[0].Names) != 1 {
		return false
	}
	receiver := fn.Recv.List[0].Names[0]
	for _, statement := range fn.Body.List {
		branch, ok := statement.(*ast.IfStmt)
		if !ok {
			continue
		}
		assignment, ok := branch.Init.(*ast.AssignStmt)
		if !ok || len(assignment.Lhs) != 1 || len(assignment.Rhs) != 1 {
			continue
		}
		deactivation, ok := assignment.Lhs[0].(*ast.Ident)
		field, fieldOK := assignment.Rhs[0].(*ast.SelectorExpr)
		if !ok || !fieldOK || field.Sel.Name != "FlowDeactivation" || !writerBoundaryField(field.X, receiver, "PostCommit") {
			continue
		}
		condition, ok := branch.Cond.(*ast.BinaryExpr)
		if !ok || condition.Op != token.NEQ || !writerBoundaryIdent(condition.X, deactivation) {
			continue
		}
		if nilValue, ok := condition.Y.(*ast.Ident); !ok || nilValue.Name != "nil" {
			continue
		}
		// Rejection must precede identity validation and cover every non-terminated
		// status. A final stage label is not operational retirement authority.
		if len(branch.Body.List) == 0 {
			continue
		}
		rejection, ok := branch.Body.List[0].(*ast.IfStmt)
		if !ok || rejection.Init != nil {
			continue
		}
		statusCheck, ok := rejection.Cond.(*ast.BinaryExpr)
		if !ok || statusCheck.Op != token.NEQ {
			continue
		}
		status, ok := statusCheck.X.(*ast.SelectorExpr)
		if !ok || status.Sel.Name != "Status" || !writerBoundaryField(status.X, receiver, "State") {
			continue
		}
		terminated, ok := statusCheck.Y.(*ast.BasicLit)
		if !ok || terminated.Kind != token.STRING {
			continue
		}
		if value, err := strconv.Unquote(terminated.Value); err != nil || value != "terminated" {
			continue
		}
		if len(rejection.Body.List) == 1 {
			if ret, ok := rejection.Body.List[0].(*ast.ReturnStmt); ok && len(ret.Results) == 1 {
				if failure, ok := ret.Results[0].(*ast.CallExpr); ok && writerBoundaryCall(failure, "Errorf") {
					return true
				}
			}
		}
	}
	return false
}

func writerBoundaryFileViolations(path string, file *ast.File) []writerBoundaryViolation {
	var violations []writerBoundaryViolation
	entityStore := strings.Contains(filepath.ToSlash(path), "/backend/entityruntime/")
	ast.Inspect(file, func(node ast.Node) bool {
		if name, ok := node.(*ast.Ident); ok {
			switch name.Name {
			case "isTerminalFlowState", "prepareTerminalFlowInstanceDeactivation", "finishCommittedFlowDeactivation",
				"commitPreparedFlowDeactivation", "abortPreparedFlowDeactivation", "committedEngineFlowDeactivation",
				"CommittedFlowDeactivation", "FinalizeFlowDeactivation", "FlowInstanceDeactivationPreparer",
				"InstanceDeactivationPreparer", "instanceDeactivationPreparer", "notifyTestWorkflowTerminalCommitted":
				violations = append(violations, writerBoundaryViolation{"ordinary_flow_deactivation", name.Pos()})
			}
		}
		return true
	})
	for _, declaration := range file.Decls {
		switch decl := declaration.(type) {
		case *ast.FuncDecl:
			// Explicit termination retains its typed post-commit evidence. Ordinary
			// mutations cannot restore the producer under a different helper name.
			explicitTermination := filepath.ToSlash(path) == "internal/runtime/pipeline/workflow_gate_terminal.go" &&
				decl.Name.Name == "commitWorkflowTerminationAttempt" && writerBoundaryReceiver(decl, "PipelineCoordinator")
			if !explicitTermination {
				ast.Inspect(decl.Body, func(node ast.Node) bool {
					switch n := node.(type) {
					case *ast.KeyValueExpr:
						if name, ok := n.Key.(*ast.Ident); ok && name.Name == "FlowDeactivation" {
							violations = append(violations, writerBoundaryViolation{"ordinary_flow_deactivation", n.Pos()})
						}
					case *ast.AssignStmt:
						for _, target := range n.Lhs {
							if field, ok := target.(*ast.SelectorExpr); ok && field.Sel.Name == "FlowDeactivation" {
								violations = append(violations, writerBoundaryViolation{"ordinary_flow_deactivation", field.Pos()})
							}
						}
					case *ast.CompositeLit:
						var name string
						switch typ := n.Type.(type) {
						case *ast.Ident:
							name = typ.Name
						case *ast.SelectorExpr:
							name = typ.Sel.Name
						}
						if name == "WorkflowEngineFlowDeactivation" {
							violations = append(violations, writerBoundaryViolation{"ordinary_flow_deactivation", n.Pos()})
						}
					}
					return true
				})
			}
			if decl.Name.Name == "SaveEntityField" || decl.Name.Name == "savePostgresEntityField" || decl.Name.Name == "saveSQLiteEntityField" {
				violations = append(violations, writerBoundaryViolation{"raw_save_authority", decl.Pos()})
			}
			if decl.Name.Name == "MarkTerminated" && writerBoundaryReceiver(decl, "workflowInstanceStore") {
				violations = append(violations, writerBoundaryViolation{"raw_termination_authority", decl.Pos()})
			}
			if decl.Name.Name == "prepareMutation" {
				violations = append(violations, writerBoundaryR1Preparation(decl)...)
			}
			if decl.Name.Name == "Validate" && writerBoundaryReceiver(decl, "WorkflowEngineMutationCommand") && !writerBoundaryRejectsOrdinaryPostCommit(decl) {
				violations = append(violations, writerBoundaryViolation{"ordinary_postcommit_authority", decl.Pos()})
			}
			if decl.Name.Name == "handleWorkflowStageTimerFire" || decl.Name.Name == "routeWorkflowGateDecisionAttempt" || decl.Name.Name == "commitWorkflowTerminationAttempt" {
				violations = append(violations, writerBoundaryEntityFence(decl)...)
			}
		case *ast.GenDecl:
			for _, specification := range decl.Specs {
				typ, ok := specification.(*ast.TypeSpec)
				if !ok {
					continue
				}
				if typ.Name.Name == "CommittedEngineMutation" && filepath.ToSlash(path) == "internal/runtime/engine/interfaces.go" {
					if fields, ok := typ.Type.(*ast.StructType); ok {
						for _, field := range fields.Fields.List {
							for _, name := range field.Names {
								if name.Name == "FlowDeactivation" {
									violations = append(violations, writerBoundaryViolation{"ordinary_flow_deactivation", name.Pos()})
								}
							}
						}
					}
				}
				if typ.Name.Name != "EntityPersistence" {
					continue
				}
				if methods, ok := typ.Type.(*ast.InterfaceType); ok {
					for _, method := range methods.Methods.List {
						if len(method.Names) != 1 || (method.Names[0].Name != "LoadEntityState" && method.Names[0].Name != "QueryEntityStates") {
							violations = append(violations, writerBoundaryViolation{"raw_save_port", method.Pos()})
						}
					}
				}
			}
		}
	}
	if entityStore {
		ast.Inspect(file, func(node ast.Node) bool {
			if literal, ok := node.(*ast.BasicLit); ok && literal.Kind == token.STRING {
				text, err := strconv.Unquote(literal.Value)
				if err == nil && rawInstanceWriteSQL.MatchString(text) {
					violations = append(violations, writerBoundaryViolation{"raw_instance_sql", literal.Pos()})
				}
			}
			return true
		})
	}
	return violations
}

func writerBoundarySources(t *testing.T) map[string]string {
	t.Helper()
	root := filepath.Clean(filepath.Join("..", "..", ".."))
	paths := []string{
		"internal/runtime/engine/executor.go",
		"internal/runtime/engine/interfaces.go",
		"internal/runtime/pipeline/coordinator.go",
		"internal/runtime/pipeline/engine_adapter.go",
		"internal/runtime/pipeline/engine_mutation_commit.go",
		"internal/runtime/pipeline/lifecycle_probe.go",
		"internal/runtime/pipeline/workflow_instance_activation.go",
		"internal/runtime/pipeline/workflow_timer_lifecycle.go",
		"internal/runtime/pipeline/workflow_gate_decision.go",
		"internal/runtime/pipeline/workflow_gate_terminal.go",
		"internal/runtime/pipeline/workflow_instance_store.go",
		"internal/runtime/pipeline/persistence_ports.go",
		"internal/runtime/runforkexecution/execution.go",
		"internal/runtime/runforkexecution/runtime_container.go",
		"internal/runtime/runtime.go",
		"internal/runtime/tools/persistence.go",
		"internal/store/internal/runtimepersistence/facade_forwarders_generated.go",
	}
	entityPaths, err := filepath.Glob(filepath.Join(root, "internal/store/internal/backend/entityruntime/*.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range entityPaths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			t.Fatal(err)
		}
		paths = append(paths, relative)
	}
	sources := map[string]string{}
	for _, path := range paths {
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		sources[filepath.ToSlash(path)] = string(raw)
	}
	return sources
}

func TestIssue2564LiveWriterBoundaryGuard(t *testing.T) {
	checked := map[string]bool{}
	for path, source := range writerBoundarySources(t) {
		set := token.NewFileSet()
		file, err := parser.ParseFile(set, path, source, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok {
				checked[fn.Name.Name] = true
				if fn.Name.Name == "Validate" && writerBoundaryReceiver(fn, "WorkflowEngineMutationCommand") {
					checked["WorkflowEngineMutationCommand.Validate"] = true
				}
			}
		}
		for _, violation := range writerBoundaryFileViolations(path, file) {
			t.Errorf("%s: writer boundary %s", set.Position(violation.pos), violation.code)
		}
	}
	for _, required := range []string{"prepareMutation", "handleWorkflowStageTimerFire", "routeWorkflowGateDecisionAttempt", "commitWorkflowTerminationAttempt", "WorkflowEngineMutationCommand.Validate"} {
		if !checked[required] {
			t.Errorf("writer boundary consumer %s is missing", required)
		}
	}
}

func TestIssue2564LiveWriterBoundaryGuardRejectsRestoredBypasses(t *testing.T) {
	sources := writerBoundarySources(t)
	for _, tc := range []struct {
		name, path, from, to, code string
	}{
		{"raw_save_sql", "internal/store/internal/backend/entityruntime/persistence.go", "", `
func (s *EntityPostgresOwner) restoredSave(ctx context.Context) error {
 _, err := s.backend.ExecContext(ctx, "UPDATE entity_state SET revision=revision+1 WHERE entity_id=$1")
 return err
}
`, "raw_instance_sql"},
		{"raw_save_forwarder", "internal/store/internal/runtimepersistence/facade_forwarders_generated.go", "", `
func (s *PostgresStore) SaveEntityField(ctx context.Context) error { return s.entityOwner().SaveEntityField(ctx) }
`, "raw_save_authority"},
		{"raw_mark_terminated", "internal/runtime/pipeline/workflow_instance_store.go", "", `
func (s *workflowInstanceStore) MarkTerminated(ctx context.Context) error {
 _, err := s.engineMutations.CommitWorkflowEngineMutation(ctx, WorkflowEngineMutationCommand{})
 return err
}
`, "raw_termination_authority"},
		{"raw_save_port", "internal/runtime/tools/persistence.go", "type EntityPersistence interface {", "type EntityPersistence interface {\n SaveEntityField(ctx context.Context) error", "raw_save_port"},
		{"r2_revision", "internal/runtime/pipeline/engine_adapter.go", "expectedRevision := current.Revision", "r2, _, err := r.LoadState(ctx, address)\n if err != nil { return preparedWorkflowEngineState{}, err }\n expectedRevision := r2.Revision", "r1_revision"},
		{"prepare_reread", "internal/runtime/pipeline/engine_adapter.go", "expectedRevision := current.Revision", "r.LoadState(ctx, address)\n expectedRevision := current.Revision", "prepare_reread"},
		{"substitute_r1", "internal/runtime/pipeline/engine_adapter.go", "evaluatedWorkflowInstance(semanticSource, address, engineMutation.EvaluatedState)", "evaluatedWorkflowInstance(semanticSource, address, runtimeengine.StateSnapshot{})", "r1_projection"},
		{"validation_ignored", "internal/runtime/pipeline/engine_adapter.go", "if err := application.Validate(); err != nil {\n\t\t\treturn preparedWorkflowEngineState{}, err", "if err := application.Validate(); false {\n return preparedWorkflowEngineState{}, err", "application_reject_Validate"},
		{"identity_check_ignored", "internal/runtime/pipeline/engine_adapter.go", "if flowOwner.Route != application.Route() || entityID.String() != application.EntityID()", "if false && (flowOwner.Route != application.Route() || entityID.String() != application.EntityID())", "application_reject_identity"},
		{"preview_check_ignored", "internal/runtime/pipeline/engine_adapter.go", "if application.previewOnly() {\n\t\t\treturn preparedWorkflowEngineState{},", "if false && application.previewOnly() {\n return preparedWorkflowEngineState{},", "application_reject_previewOnly"},
		{"unlocked_timer", "internal/runtime/pipeline/workflow_timer_lifecycle.go", "unlock := pc.lockWorkflowEntity(evt.RoutingSource().Route().EntityID)", "unlock := func() {}", "entity_lock"},
		{"early_timer_unlock", "internal/runtime/pipeline/workflow_timer_lifecycle.go", "committed, err := pc.workflowStore.engineMutations.CommitWorkflowEngineMutation(ctx, WorkflowEngineMutationCommand{", "release()\n committed, err := pc.workflowStore.engineMutations.CommitWorkflowEngineMutation(ctx, WorkflowEngineMutationCommand{", "unlock_before_commit"},
		{"timer_finalizer_locked", "internal/runtime/pipeline/workflow_timer_lifecycle.go", "release()\n\tif !committed.Committed", "if !committed.Committed", "postcommit_unlock"},
		{"unlocked_gate", "internal/runtime/pipeline/workflow_gate_decision.go", "unlock := pc.lockWorkflowEntity(anchor.EntityID)", "unlock := func() {}", "entity_lock"},
		{"gate_read_before_lock", "internal/runtime/pipeline/workflow_gate_decision.go", "unlock := pc.lockWorkflowEntity(anchor.EntityID)", "pc.workflowStore.Load(ctx, flowIdentity)\n unlock := pc.lockWorkflowEntity(anchor.EntityID)", "read_before_lock"},
		{"gate_finalizer_locked", "internal/runtime/pipeline/workflow_gate_decision.go", "unlock()\n\tunlock = nil\n\tif planner", "unlock = nil\n if planner", "postcommit_unlock"},
		{"unlocked_termination", "internal/runtime/pipeline/workflow_gate_terminal.go", "unlock := pc.lockWorkflowEntity(entityID.String())", "unlock := func() {}", "entity_lock"},
		{"ordinary_engine_retirement", "internal/runtime/pipeline/engine_adapter.go", "ProposedEffects: proposedEffects, Publications: publications,", "ProposedEffects: proposedEffects, Publications: publications,\n PostCommit: WorkflowEnginePostCommitPlan{FlowDeactivation: &WorkflowEngineFlowDeactivation{Identity: state.Identity, EntityID: mutation.Address.EntityID.String(), NextState: state.CurrentState}},", "ordinary_flow_deactivation"},
		{"ordinary_timer_retirement", "internal/runtime/pipeline/workflow_timer_lifecycle.go", "State: state, Lifecycle: lifecycle.Commit,", "State: state, Lifecycle: lifecycle.Commit,\n PostCommit: WorkflowEnginePostCommitPlan{FlowDeactivation: &WorkflowEngineFlowDeactivation{Identity: flowIdentity, EntityID: entityID, NextState: nextStage}},", "ordinary_flow_deactivation"},
		{"ordinary_handoff_restored", "internal/runtime/pipeline/engine_adapter.go", "result.Committed = true", "result.Committed = true\n result.FlowDeactivation = committedEngineFlowDeactivation{owner: o, terminal: terminal}", "ordinary_flow_deactivation"},
		{"ordinary_finalizer_restored", "internal/runtime/pipeline/engine_adapter.go", "result.Committed = true", "result.Committed = true\n resultErr = errors.Join(resultErr, o.finishCommittedFlowDeactivation(ctx, terminal))", "ordinary_flow_deactivation"},
		{"renamed_retirement_producer", "internal/runtime/pipeline/engine_adapter.go", "", `
func restoredOrdinaryRetirement(command *WorkflowEngineMutationCommand) {
 command.PostCommit.FlowDeactivation = &WorkflowEngineFlowDeactivation{}
}
`, "ordinary_flow_deactivation"},
		{"ordinary_preparer_injection", "internal/runtime/pipeline/coordinator.go", "type PipelineCoordinatorOptions struct {", "type PipelineCoordinatorOptions struct {\n InstanceDeactivationPreparer FlowInstanceDeactivationPreparer", "ordinary_flow_deactivation"},
		{"normal_runtime_preparer_injection", "internal/runtime/runtime.go", "runtimepipeline.NewPipelineCoordinatorWithOptions(rt.Bus, runtimepipeline.PipelineCoordinatorOptions{", "runtimepipeline.NewPipelineCoordinatorWithOptions(rt.Bus, runtimepipeline.PipelineCoordinatorOptions{\n InstanceDeactivationPreparer: managerRef.PrepareFlowInstanceDeactivation,", "ordinary_flow_deactivation"},
		{"selected_runtime_preparer_injection", "internal/runtime/runforkexecution/execution.go", "return runtimepipeline.PipelineCoordinatorOptions{", "return runtimepipeline.PipelineCoordinatorOptions{\n InstanceDeactivationPreparer: agentRuntime.AgentManagerOptions.InstanceDeactivationPreparer,", "ordinary_flow_deactivation"},
		{"selected_container_preparer_injection", "internal/runtime/runforkexecution/runtime_container.go", "pipeline := newSelectedContractPipeline(bus, c.ports, req.LoadedSource, req.AgentRuntime.Options)", "req.AgentRuntime.Options.AgentManagerOptions.InstanceDeactivationPreparer = lifecycleManager.PrepareFlowInstanceDeactivation\n pipeline := newSelectedContractPipeline(bus, c.ports, req.LoadedSource, req.AgentRuntime.Options)", "ordinary_flow_deactivation"},
		{"engine_result_callback_restored", "internal/runtime/engine/interfaces.go", "type CommittedEngineMutation struct {", "type CommittedEngineMutation struct {\n FlowDeactivation func(context.Context) error", "ordinary_flow_deactivation"},
		{"engine_callback_consumer_restored", "internal/runtime/engine/executor.go", "postCommitErr = errors.Join(postCommitErr, err)", "postCommitErr = errors.Join(postCommitErr, err)\n if flowDeactivation != nil { postCommitErr = errors.Join(postCommitErr, flowDeactivation.FinalizeFlowDeactivation(ctx)) }", "ordinary_flow_deactivation"},
		{"ordinary_postcommit_check_deleted", "internal/runtime/pipeline/engine_mutation_commit.go", "\t\tif c.State.Status != \"terminated\" {\n\t\t\treturn fmt.Errorf(\"stage mutation cannot declare operational flow retirement\")\n\t\t}\n", "", "ordinary_postcommit_authority"},
		{"ordinary_postcommit_check_ignored", "internal/runtime/pipeline/engine_mutation_commit.go", "if c.State.Status != \"terminated\" {", "if false {", "ordinary_postcommit_authority"},
		{"ordinary_postcommit_stage_label", "internal/runtime/pipeline/engine_mutation_commit.go", "if c.State.Status != \"terminated\" {", "if c.State.CurrentState != \"terminated\" {", "ordinary_postcommit_authority"},
		{"ordinary_postcommit_rejection_swallowed", "internal/runtime/pipeline/engine_mutation_commit.go", "return fmt.Errorf(\"stage mutation cannot declare operational flow retirement\")", "return nil", "ordinary_postcommit_authority"},
		{"ordinary_postcommit_branch_ignored", "internal/runtime/pipeline/engine_mutation_commit.go", "if deactivation := c.PostCommit.FlowDeactivation; deactivation != nil {", "if deactivation := c.PostCommit.FlowDeactivation; false {", "ordinary_postcommit_authority"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source, ok := sources[tc.path]
			if !ok {
				t.Fatalf("guard source missing: %s", tc.path)
			}
			if tc.from == "" {
				source += tc.to
			} else {
				if strings.Count(source, tc.from) != 1 {
					t.Fatalf("negative mutation must hit exactly once: %s", tc.from)
				}
				source = strings.Replace(source, tc.from, tc.to, 1)
			}
			file, err := parser.ParseFile(token.NewFileSet(), tc.path, source, 0)
			if err != nil {
				t.Fatalf("negative mutation must remain parseable: %v", err)
			}
			violations := writerBoundaryFileViolations(tc.path, file)
			for _, violation := range violations {
				if violation.code == tc.code {
					return
				}
			}
			t.Fatalf("restored bypass escaped %s guard: %v", tc.code, violations)
		})
	}
}

func TestIssue2564WriterBoundaryAllowsCanonicalTerminationOnly(t *testing.T) {
	path := "internal/runtime/pipeline/persistence_ports.go"
	file, err := parser.ParseFile(token.NewFileSet(), path, writerBoundarySources(t)[path], 0)
	if err != nil {
		t.Fatal(err)
	}
	canonical := false
	for _, declaration := range file.Decls {
		if fn, ok := declaration.(*ast.FuncDecl); ok && fn.Name.Name == "MarkTerminated" && writerBoundaryReceiver(fn, "PipelineCoordinator") {
			canonical = true
		}
	}
	if !canonical {
		t.Fatal("canonical PipelineCoordinator.MarkTerminated is missing")
	}
	for _, violation := range writerBoundaryFileViolations(path, file) {
		if violation.code == "raw_termination_authority" {
			t.Fatal("canonical coordinator termination was confused with the retired raw store writer")
		}
	}
	path = "internal/runtime/pipeline/workflow_gate_terminal.go"
	set := token.NewFileSet()
	file, err = parser.ParseFile(set, path, writerBoundarySources(t)[path], 0)
	if err != nil {
		t.Fatal(err)
	}
	deactivation := false
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "commitWorkflowTerminationAttempt" || !writerBoundaryReceiver(fn, "PipelineCoordinator") {
			continue
		}
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			assignment, ok := node.(*ast.AssignStmt)
			if !ok || len(assignment.Lhs) != 1 || len(assignment.Rhs) != 1 {
				return true
			}
			field, ok := assignment.Lhs[0].(*ast.SelectorExpr)
			if !ok || field.Sel.Name != "FlowDeactivation" {
				return true
			}
			address, ok := assignment.Rhs[0].(*ast.UnaryExpr)
			if !ok || address.Op != token.AND {
				return true
			}
			if producer, ok := address.X.(*ast.CompositeLit); ok {
				name, ok := producer.Type.(*ast.Ident)
				deactivation = deactivation || (ok && name.Name == "WorkflowEngineFlowDeactivation")
			}
			return true
		})
	}
	if !deactivation {
		t.Fatal("explicit workflow termination lost its typed post-commit deactivation evidence")
	}
	for _, violation := range writerBoundaryFileViolations(path, file) {
		t.Errorf("%s: explicit termination writer boundary %s", set.Position(violation.pos), violation.code)
	}
}

func TestDeliveryTargetPreparationR1GuardRejectsValidationBypass(t *testing.T) {
	path := "internal/runtime/pipeline/engine_adapter.go"
	source := writerBoundarySources(t)[path]
	for _, method := range []string{"Validate", "Route", "EntityID", "previewOnly"} {
		t.Run(method, func(t *testing.T) {
			from := "application." + method + "()"
			changed := strings.ReplaceAll(source, from, "other."+method+"()")
			if changed == source {
				t.Fatal("negative application mutation missed")
			}
			file, err := parser.ParseFile(token.NewFileSet(), path, changed, 0)
			if err != nil {
				t.Fatal(err)
			}
			for _, violation := range writerBoundaryFileViolations(path, file) {
				if violation.code == "application_"+method {
					return
				}
			}
			t.Fatalf("application %s bypass escaped the R1 preparation guard", method)
		})
	}
}
