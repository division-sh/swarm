package bootverify

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestCompositionSelectedSingletonInputDoesNotRequireAnotherInitializer(t *testing.T) {
	bundle := loadFixtureBundle(t, filepath.Join("examples", "routing", "notify-all-children"))
	source := semanticview.Wrap(bundle)
	node, err := semanticview.ResolveExecutableNodeDeclaration(source, "portfolio", "portfolio-coordinator")
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range []string{"portfolio.account.register.requested", "portfolio.notify.requested"} {
		handler := semanticview.ResolveExecutableNodeSubscriptionHandler(source, node, event)
		if !handler.Matched || handler.Handler.CreateEntity {
			t.Fatalf("fixture must retain noninitializing handler %s", event)
		}
		if err := pipeline.ValidateExecutionHandlerDeclaration(source, node, handler.Handler); err != nil {
			t.Fatalf("ordinary handler %s must not require construction authority: %v", event, err)
		}
	}
	report := Run(context.Background(), source, Options{})
	if reportContains(report.Errors(), "flow_boundary_create_entity_validation", "") {
		t.Fatalf("composition-selected existing or optional ownership must not demand initialization: %#v", report.Errors())
	}
}
