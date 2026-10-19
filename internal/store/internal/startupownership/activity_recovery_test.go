package startupownership

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/activityjournal"
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

func TestStartupActivityJournalRequiresOneDualRole(t *testing.T) {
	pg, sqlite := &StartupPostgresOwner{}, &StartupSQLiteOwner{}
	for _, invalid := range []runtimeownership.ActivityJournalGrantBinder{
		(*activityjournal.ActivityPostgresOwner)(nil),
		(*activityjournal.ActivitySQLiteOwner)(nil),
		&activityRecoveryRoleProbe{},
	} {
		if pg.BindActivityJournalRole(invalid) == nil || sqlite.BindActivityJournalRole(invalid) == nil || pg.activityJournal != nil || sqlite.activityJournal != nil {
			t.Fatal("missing or non-native role changed the original native owner")
		}
	}
	if pg.BindActivityJournalRole(&activityjournal.ActivitySQLiteOwner{}) == nil || sqlite.BindActivityJournalRole(&activityjournal.ActivityPostgresOwner{}) == nil || pg.activityJournal != nil || sqlite.activityJournal != nil {
		t.Fatal("other-store role changed the original native owner")
	}
	for _, test := range []struct {
		name string
		bind func(bool) error
		read func() bool
	}{
		{"postgres", func(missing bool) error {
			if missing {
				return pg.BindActivityJournalRole(nil)
			}
			return pg.BindActivityJournalRole(&activityjournal.ActivityPostgresOwner{})
		}, func() bool { return pg.activityJournal != nil }},
		{"sqlite", func(missing bool) error {
			if missing {
				return sqlite.BindActivityJournalRole(nil)
			}
			return sqlite.BindActivityJournalRole(&activityjournal.ActivitySQLiteOwner{})
		}, func() bool { return sqlite.activityJournal != nil }},
		{"nil_postgres", func(missing bool) error {
			var role *activityjournal.ActivityPostgresOwner
			if !missing {
				role = &activityjournal.ActivityPostgresOwner{}
			}
			return (*StartupPostgresOwner)(nil).BindActivityJournalRole(role)
		}, nil},
		{"nil_sqlite", func(missing bool) error {
			var role *activityjournal.ActivitySQLiteOwner
			if !missing {
				role = &activityjournal.ActivitySQLiteOwner{}
			}
			return (*StartupSQLiteOwner)(nil).BindActivityJournalRole(role)
		}, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			for range 2 {
				if test.bind(true) == nil {
					t.Fatal("missing activity role accepted")
				}
			}
			if test.read == nil {
				if test.bind(false) == nil {
					t.Fatal("nil owner accepted an activity role")
				}
				return
			}
			if err := test.bind(false); err != nil {
				t.Fatal(err)
			}
			originalPG, originalSQLite := pg.activityJournal, sqlite.activityJournal
			for range 2 {
				if test.bind(false) == nil || !test.read() || pg.activityJournal != originalPG || sqlite.activityJournal != originalSQLite {
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

func TestStartupActivityRecoveryMissingRoleRefusesBeforeNativeEntry(t *testing.T) {
	authority := activityRecoveryTestAuthority()
	for _, test := range []struct {
		name    string
		recover func(context.Context) (pipeline.ActivityRecoverySummary, error)
		binder  func() (runtimeownership.ActivityJournalGrantBinder, error)
	}{
		{"postgres", (&postgresSession{owner: &StartupPostgresOwner{}, authority: authority}).ReconcileDisplacedHTTPActivities, (&postgresSession{owner: &StartupPostgresOwner{}, authority: authority}).ActivityJournalBinder},
		{"sqlite", (&sqliteSession{owner: &StartupSQLiteOwner{}, authority: authority}).ReconcileDisplacedHTTPActivities, (&sqliteSession{owner: &StartupSQLiteOwner{}, authority: authority}).ActivityJournalBinder},
	} {
		t.Run(test.name, func(t *testing.T) {
			if journal, err := test.binder(); err == nil || journal != nil {
				t.Fatal("unbound session supplied a journal")
			}
			result, err := test.recover(context.Background())
			if err == nil || !reflect.DeepEqual(result, pipeline.ActivityRecoverySummary{}) {
				t.Fatal("unbound session entered native recovery or returned a result")
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
