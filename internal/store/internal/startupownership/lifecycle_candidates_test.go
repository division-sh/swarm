package startupownership

import (
	"context"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/store/internal/runhandoff"
)

func TestStartupLifecycleCandidatesBoundOnce(t *testing.T) {
	pg, sqlite := &StartupPostgresOwner{}, &StartupSQLiteOwner{}
	for _, test := range []struct {
		name string
		bind func(*runhandoff.CandidateCoordinator) error
		read func() *runhandoff.CandidateCoordinator
	}{
		{"postgres", pg.BindLifecycleCandidates, func() *runhandoff.CandidateCoordinator { return pg.lifecycleCandidates }},
		{"sqlite", sqlite.BindLifecycleCandidates, func() *runhandoff.CandidateCoordinator { return sqlite.lifecycleCandidates }},
		{"nil_postgres", (*StartupPostgresOwner)(nil).BindLifecycleCandidates, nil},
		{"nil_sqlite", (*StartupSQLiteOwner)(nil).BindLifecycleCandidates, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.bind(nil); err == nil {
				t.Fatal("nil coordinator accepted")
			}
			candidates := runhandoff.NewCandidateCoordinator()
			if test.read == nil {
				if err := test.bind(candidates); err == nil {
					t.Fatal("nil owner accepted a coordinator")
				}
				return
			}
			if err := test.bind(candidates); err != nil {
				t.Fatal(err)
			}
			for _, again := range []*runhandoff.CandidateCoordinator{candidates, runhandoff.NewCandidateCoordinator()} {
				if err := test.bind(again); err == nil {
					t.Fatal("duplicate coordinator accepted")
				}
			}
			if test.read() != candidates {
				t.Fatal("refused binding replaced the original coordinator")
			}
		})
	}
}

func TestStartupTakeoverMissingCandidatesRefusesBeforeNativeEntry(t *testing.T) {
	for _, test := range []struct {
		name   string
		commit func(context.Context, manager.AgentLifecycleTransition) (manager.AgentLifecycleTransitionResult, error)
	}{
		{"postgres", (&postgresSession{owner: &StartupPostgresOwner{}}).CommitAgentLifecycleTransition},
		{"sqlite", (&sqliteSession{owner: &StartupSQLiteOwner{}}).CommitAgentLifecycleTransition},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := test.commit(context.Background(), manager.AgentLifecycleTransition{OperationKind: "process_takeover"})
			if err == nil || !reflect.DeepEqual(result, manager.AgentLifecycleTransitionResult{}) {
				t.Fatalf("missing coordinator exposed mutation result: result=%+v error=%v", result, err)
			}
		})
	}
}

func TestStartupLifecycleCandidatesOnlyForTakeover(t *testing.T) {
	candidates := runhandoff.NewCandidateCoordinator()
	for _, bound := range []*runhandoff.CandidateCoordinator{nil, candidates} {
		for _, kind := range []string{"", "register", "restart", "source_set_rebind", "self_release", "teardown"} {
			selected, err := lifecycleMutationCandidates(kind, bound)
			if err != nil || selected != nil {
				t.Fatalf("non-takeover %q changed coordinator behavior: selected=%p error=%v", kind, selected, err)
			}
		}
	}
	selected, err := lifecycleMutationCandidates("process_takeover", candidates)
	if err != nil || selected != candidates {
		t.Fatalf("takeover did not retain the original coordinator: selected=%p error=%v", selected, err)
	}
}
