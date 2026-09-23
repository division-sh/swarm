package eventpersistence

import (
	"context"
	"database/sql"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	runtimelifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

func TestComposedEventWritersRequireMutationAttempt(t *testing.T) {
	ctx := context.Background()
	checks := map[string]func() error{
		"publication": func() error {
			_, err := commitPublicationTx(ctx, nil, nil, runtimebus.PublicationCommand{})
			return err
		},
		"fan-out publication": func() error {
			_, err := commitFanOutPublicationTx(ctx, nil, nil, runtimebus.PublicationCommand{}, fanoutobligation.OrdinalEmission{})
			return err
		},
		"directive": func() error {
			_, err := (&EventPostgresOwner{}).CommitDirectiveEventTx(ctx, nil, events.AdmittedEvent{})
			return err
		},
	}
	for name, check := range checks {
		t.Run(name, func(t *testing.T) {
			if err := check(); err == nil || !strings.Contains(err.Error(), "attempt") {
				t.Fatalf("missing mutation attempt was not rejected: %v", err)
			}
		})
	}
}

type standaloneCompletionProbe struct {
	standalone bool
	err        error
	read       bool
}

func (p *standaloneCompletionProbe) IsStandaloneRuntimePlatformEventTx(context.Context, *sql.Tx, string) (bool, error) {
	p.read = true
	return p.standalone, p.err
}

func (*standaloneCompletionProbe) WriteCompletionCandidateTx(context.Context, *sql.Tx, string, *time.Time) (runtimelifecycle.CandidateRequestResult, error) {
	panic("completion writer must not run before standalone classification")
}

func TestStandalonePublicationCompletionCapabilityFailsClosed(t *testing.T) {
	ctx := context.Background()
	for _, owner := range []standaloneCompletionCapability{
		(*EventPostgresOwner)(nil).standaloneCompletionOwner(),
		(&EventPostgresOwner{}).standaloneCompletionOwner(),
		(*EventSQLiteOwner)(nil).standaloneCompletionOwner(),
		(&EventSQLiteOwner{}).standaloneCompletionOwner(),
	} {
		if err := requestStandalonePublicationCompletion(ctx, nil, nil, owner, "event", "run"); err == nil || !strings.Contains(err.Error(), "capability") {
			t.Fatalf("missing standalone completion capability error = %v", err)
		}
	}
	nonStandalone := &standaloneCompletionProbe{}
	if err := requestStandalonePublicationCompletion(ctx, nil, nil, nonStandalone, "event", "run"); err != nil || !nonStandalone.read {
		t.Fatalf("non-standalone decision: read=%t error=%v", nonStandalone.read, err)
	}
	refusal := errors.New("standalone lookup failed")
	failing := &standaloneCompletionProbe{err: refusal}
	if err := requestStandalonePublicationCompletion(ctx, nil, nil, failing, "event", "run"); !errors.Is(err, refusal) {
		t.Fatalf("standalone lookup error = %v, want %v", err, refusal)
	}
}

func TestPublicationCompletionDecisionHasNoConcreteStoreSwitch(t *testing.T) {
	source, err := os.ReadFile("event_commit.go")
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), "event_commit.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != "commitValidatedPublicationSQL" {
			continue
		}
		capabilityUsed := false
		ast.Inspect(function.Body, func(node ast.Node) bool {
			switch node := node.(type) {
			case *ast.TypeAssertExpr:
				if name, ok := node.X.(*ast.Ident); ok && name.Name == "store" {
					t.Error("publication decision must not assert a concrete store")
				}
			case *ast.CallExpr:
				if name, ok := node.Fun.(*ast.Ident); ok && name.Name == "requestStandalonePublicationCompletion" {
					capabilityUsed = true
				}
			}
			return true
		})
		if !capabilityUsed {
			t.Fatal("publication decision bypasses required standalone completion capability")
		}
		return
	}
	t.Fatal("publication decision owner is missing")
}
