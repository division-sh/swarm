package startupownership

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/google/uuid"
)

type activityRecoveryRoleProbe struct {
	pipeline.ActivityAttemptJournal
	bound []runtimeownership.GrantEvidence
}

func (p *activityRecoveryRoleProbe) BindActivityGrant(grant runtimeownership.GrantEvidence) (pipeline.ActivityAttemptJournal, error) {
	p.bound = append(p.bound, grant)
	return p, nil
}

func (p *activityRecoveryRoleProbe) ReconcileDisplacedHTTPActivitiesTx(context.Context, *mutationprotocol.Attempt, DisplacedHTTPActivityCut) (pipeline.ActivityRecoverySummary, error) {
	panic("unit binding control must not enter native recovery")
}

func TestStartupActivityJournalRequiresOneDualRole(t *testing.T) {
	pg, sqlite := &StartupPostgresOwner{}, &StartupSQLiteOwner{}
	for _, test := range []struct {
		name string
		bind func(displacedHTTPActivityJournal) error
		read func() displacedHTTPActivityJournal
	}{
		{"postgres", pg.BindActivityJournalRole, func() displacedHTTPActivityJournal { return pg.activityJournal }},
		{"sqlite", sqlite.BindActivityJournalRole, func() displacedHTTPActivityJournal { return sqlite.activityJournal }},
		{"nil_postgres", (*StartupPostgresOwner)(nil).BindActivityJournalRole, nil},
		{"nil_sqlite", (*StartupSQLiteOwner)(nil).BindActivityJournalRole, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			var missing *activityRecoveryRoleProbe
			for _, input := range []displacedHTTPActivityJournal{nil, missing} {
				if test.bind(input) == nil {
					t.Fatal("missing activity role accepted")
				}
			}
			role := &activityRecoveryRoleProbe{}
			if test.read == nil {
				if test.bind(role) == nil {
					t.Fatal("nil owner accepted an activity role")
				}
				return
			}
			if err := test.bind(role); err != nil {
				t.Fatal(err)
			}
			for _, duplicate := range []displacedHTTPActivityJournal{role, &activityRecoveryRoleProbe{}} {
				if test.bind(duplicate) == nil || test.read() != role {
					t.Fatal("duplicate binding changed the original activity owner")
				}
			}
		})
	}
}

func TestRetainedActivityJournalBinderCapturesExactSession(t *testing.T) {
	authority := activityRecoveryTestAuthority()
	grant := runtimeownership.GrantEvidence{
		GrantID: uuid.NewString(), ProcessAuthorityID: authority.AuthorityID,
		ProcessOwnerID: authority.OwnerID, ProcessBootID: authority.BootID,
		BundleHash:        "bundle-v2:sha256:" + strings.Repeat("a", 64),
		RuntimeInstanceID: authority.RuntimeInstanceID, RuntimeGeneration: 1,
		SourceSetRevision: "source-set-v1:sha256:" + strings.Repeat("b", 64),
		StateVersion:      3, State: runtimeownership.GrantAdmitted,
	}
	if err := grant.Validate(); err != nil {
		t.Fatal(err)
	}
	role := &activityRecoveryRoleProbe{}
	binder, err := newRetainedActivityJournalBinder(authority, role)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := binder.BindActivityGrant(grant)
	if err != nil || journal != role || !reflect.DeepEqual(role.bound, []runtimeownership.GrantEvidence{grant}) {
		t.Fatalf("captured binder did not preserve the admitted role: %v", err)
	}
	for _, scenario := range []string{"foreign_lease", "foreign_owner", "foreign_boot", "prepared", "retired", "missing_evidence"} {
		t.Run(scenario, func(t *testing.T) {
			changed := grant
			switch scenario {
			case "foreign_lease":
				changed.ProcessAuthorityID = uuid.NewString()
			case "foreign_owner":
				changed.ProcessOwnerID = "another-process"
			case "foreign_boot":
				changed.ProcessBootID = uuid.NewString()
			case "prepared":
				changed.State = runtimeownership.GrantPrepared
			case "retired":
				changed.State = runtimeownership.GrantRetired
			case "missing_evidence":
				changed = runtimeownership.GrantEvidence{}
			}
			if journal, err := binder.BindActivityGrant(changed); err == nil || journal != nil || len(role.bound) != 1 {
				t.Fatal("refused process evidence reached the original activity binder")
			}
		})
	}
}

func TestStartupActivityJournalMissingRoleRefusesBeforeNativeEntry(t *testing.T) {
	authority := activityRecoveryTestAuthority()
	for _, test := range []struct {
		name   string
		binder func() (runtimeownership.ActivityJournalGrantBinder, error)
	}{
		{"postgres", (&postgresSession{owner: &StartupPostgresOwner{}, authority: authority}).ActivityJournalBinder},
		{"sqlite", (&sqliteSession{owner: &StartupSQLiteOwner{}, authority: authority}).ActivityJournalBinder},
	} {
		t.Run(test.name, func(t *testing.T) {
			if journal, err := test.binder(); err == nil || journal != nil {
				t.Fatal("unbound session supplied a journal")
			}
		})
	}
}

func activityRecoveryTestAuthority() runtimeownership.Authority {
	return runtimeownership.AuthorityFromLease(processbinding.LeaseStamp{LeaseIdentity: processbinding.LeaseIdentity{
		LeaseID: uuid.NewString(), OwnerID: "retained-activity-owner", BootID: uuid.NewString(),
		RuntimeInstanceID: uuid.NewString(), Backend: processbinding.SQLiteLeaseBackend,
	}, Generation: 2})
}
