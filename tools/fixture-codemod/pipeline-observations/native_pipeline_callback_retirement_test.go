package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
)

func pipelineCallbackProtocolRewrite(name, source string) (string, error) {
	var replacements [][2]string
	switch name {
	case "TestSQLiteWorkflowInstanceStore_runPipelineMutationUsesRuntimeMutationRunner":
		replacements = [][2]string{
			{"var postCommitActions int32", "var mutationCallbacks int32"},
			{"err := store.runPipelineMutation(ctx, func(txctx context.Context) error {", "err := store.runPipelineMutation(ctx, func(txctx context.Context) error {\n\t\tatomic.AddInt32(&mutationCallbacks, 1)"},
			{"\t\tif !QueuePipelinePostCommitAction(txctx, func(context.Context) {\n\t\t\tatomic.AddInt32(&postCommitActions, 1)\n\t\t}) {\n\t\t\treturn errors.New(\"queue pipeline post-commit action\")\n\t\t}\n", ""},
			{"atomic.LoadInt32(&postCommitActions)", "atomic.LoadInt32(&mutationCallbacks)"},
			{"post-commit actions = %d, want 1", "mutation callbacks = %d, want 1"},
		}
	case "RunRuntimeMutationContextAcknowledged":
		replacements = [][2]string{
			{"\tpostCommit := make([]OwnerAction, 0, 4)\n\trollbackActions := make([]OwnerAction, 0, 4)\n", ""},
			{"txctx = withPipelinePostCommitActions(WithPipelineSQLTxContext(txctx, tx), &postCommit)\n\t\t\ttxctx = withPipelineRollbackActions(txctx, &rollbackActions)", "txctx = WithPipelineSQLTxContext(txctx, tx)"},
			{"\t\tflushPipelineRollbackActions(rollbackActions)\n", ""},
			{"\tflushPipelinePostCommitActions(postCommit)\n", ""},
		}
	default:
		return "", fmt.Errorf("unreviewed pipeline callback consumer %s", name)
	}
	for _, replacement := range replacements {
		if strings.Count(source, replacement[0]) != 1 {
			return "", fmt.Errorf("pipeline callback shape changed: %s", name)
		}
		source = strings.Replace(source, replacement[0], replacement[1], 1)
	}
	return canonicalFunction(source)
}

func TestPipelineCallbackProtocolRecipesPreserveMutationAndReceiptBoundaries(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-pipeline-callback-protocol-retirement" {
			continue
		}
		matched++
		want, err := pipelineCallbackProtocolRewrite(row.Function, row.Before)
		got, afterErr := canonicalFunction(row.After)
		if err != nil || afterErr != nil || got != want {
			t.Fatalf("pipeline mutation/acknowledgment contract changed: %s", row.Function)
		}
		if !row.Removed {
			t.Fatal("superseded raw callback capability was not retired")
		}
		root := filepath.Join("..", "..", "..")
		if files, changes, err := prepareFiles(root, []recipe{row}); err != nil || len(files) != 0 || len(changes) != 0 {
			t.Fatalf("retired protocol reappeared: %s/%v", row.Function, err)
		}
		if _, err := pipelineCallbackProtocolRewrite("foreign", row.Before); err == nil {
			t.Fatal("unreviewed pipeline callback consumer accepted")
		}
	}
	if matched != 2 {
		t.Fatalf("pipeline callback recipes=%d,want2", matched)
	}
}

func pipelineCallbackProtocolEscapes(source []byte) ([]string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "pipeline.go", source, parser.AllErrors)
	if err != nil {
		return nil, err
	}
	retired := map[string]bool{}
	for _, name := range strings.Fields("OwnerAction testSQLConnContextKey WithPipelineSQLConnContext PipelineSQLConnFromContext testPostCommitActionsKey testRollbackActionsKey withPipelinePostCommitActions WithPipelinePostCommitActions withPipelineRollbackActions WithPipelineRollbackActions queuePipelinePostCommitAction QueuePipelinePostCommitAction queuePipelineRollbackAction QueuePipelineRollbackAction flushPipelinePostCommitActions FlushPipelinePostCommitActions flushPipelineRollbackActions FlushPipelineRollbackActions") {
		retired[name] = true
	}
	for _, name := range strings.Fields("recordingRuntimeMutationRunner testSQLTxContextKey WithPipelineSQLTxContext PipelineSQLTxFromContext newSQLiteWorkflowInstanceStoreForTest newPostgresWorkflowInstanceStoreForTest newDurablePipelineCoordinatorForTest runPipelineMutation runInPipelineTransaction runInPipelineTransactionAcknowledged RunRuntimeMutationContext RunRuntimeMutationContextAcknowledged registerPipelineFixtureEntityStateDB pipelineFixtureEntityStateDB") {
		retired[name] = true
	}
	var escapes []string
	ast.Inspect(file, func(node ast.Node) bool {
		if identifier, ok := node.(*ast.Ident); ok && retired[identifier.Name] {
			escapes = append(escapes, identifier.Name)
		}
		return true
	})
	return escapes, nil
}

func TestPipelineCallbackProtocolCapabilityIsRetired(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	if err := checkoutsource.WalkDir(root, filepath.Join(root, "internal", "runtime", "pipeline"), func(path string, entry fs.DirEntry, failure error) error {
		if failure != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return failure
		}
		source, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		escapes, err := pipelineCallbackProtocolEscapes(source)
		if len(escapes) != 0 {
			t.Errorf("retired pipeline callback protocol remains: %s/%v", path, escapes)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"OwnerAction", "QueuePipelinePostCommitAction", "QueuePipelineRollbackAction", "FlushPipelinePostCommitActions", "WithPipelineSQLConnContext", "recordingRuntimeMutationRunner", "PipelineSQLTxFromContext", "newSQLiteWorkflowInstanceStoreForTest", "RunRuntimeMutationContextAcknowledged"} {
		if escapes, err := pipelineCallbackProtocolEscapes([]byte("package pipeline\nfunc probe(){" + name + "()}")); err != nil || len(escapes) != 1 {
			t.Fatalf("retired protocol mutant escaped: %s/%v", name, err)
		}
	}
	if _, err := pipelineCallbackProtocolEscapes([]byte("package pipeline\nfunc")); err == nil {
		t.Fatal("incomplete protocol source was accepted")
	}
}
