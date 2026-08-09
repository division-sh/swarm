package runtimepersistence

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeingress "github.com/division-sh/swarm/internal/runtime/ingress"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	storeapiidempotency "github.com/division-sh/swarm/internal/store/internal/apiidempotency"
	storeactivityjournal "github.com/division-sh/swarm/internal/store/internal/backend/activityjournal"
	storeactivityresult "github.com/division-sh/swarm/internal/store/internal/backend/activityresult"
	storeagent "github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
	storedecision "github.com/division-sh/swarm/internal/store/internal/backend/decisionpersistence"
	storedelivery "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	storeeffect "github.com/division-sh/swarm/internal/store/internal/backend/effectpersistence"
	storeentity "github.com/division-sh/swarm/internal/store/internal/backend/entityruntime"
	storeevent "github.com/division-sh/swarm/internal/store/internal/backend/eventpersistence"
	storegenericschedule "github.com/division-sh/swarm/internal/store/internal/backend/genericschedule"
	storellm "github.com/division-sh/swarm/internal/store/internal/backend/llmpersistence"
	storemanagedcapability "github.com/division-sh/swarm/internal/store/internal/backend/managedcapability"
	storepipeline "github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	storereplycontext "github.com/division-sh/swarm/internal/store/internal/backend/replycontext"
	storerunfork "github.com/division-sh/swarm/internal/store/internal/backend/runforkpersistence"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	storetimerobligation "github.com/division-sh/swarm/internal/store/internal/backend/timerobligation"
	storebudgetspend "github.com/division-sh/swarm/internal/store/internal/budgetspend"
	storebundlecatalog "github.com/division-sh/swarm/internal/store/internal/bundlecatalog"
	storeingress "github.com/division-sh/swarm/internal/store/internal/ingresspersistence"
	storemailbox "github.com/division-sh/swarm/internal/store/internal/mailboxpersistence"
	storeoperatorsurface "github.com/division-sh/swarm/internal/store/internal/operatorsurface"
	storerunbundle "github.com/division-sh/swarm/internal/store/internal/runbundle"
	storerunhandoff "github.com/division-sh/swarm/internal/store/internal/runhandoff"
	storeworkflowentityquery "github.com/division-sh/swarm/internal/store/internal/workflowentityquery"
	storeworkflowroute "github.com/division-sh/swarm/internal/store/internal/workflowroute"
	storeworkspace "github.com/division-sh/swarm/internal/store/internal/workspace"
)

// SQLiteRuntimeStore is the file-backed SQLite implementation of the selected
// runtime persistence contracts. SQLite is a local-dev backend: process-local
// mutexes provide startup/session serialization while persisted rows remain the
// canonical state consumed by the runtime.
type SQLiteRuntimeStore struct {
	agentSQLiteOwner             *storeagent.AgentSQLiteOwner
	activitySQLiteOwner          *storeactivityjournal.ActivitySQLiteOwner
	activityResultSQLiteOwner    *storeactivityresult.ActivityResultSQLiteOwner
	sQLiteOwner                  *storeapiidempotency.SQLiteOwner
	budgetSQLiteOwner            *storebudgetspend.BudgetSQLiteOwner
	sQLite                       *storebundlecatalog.SQLite
	decisionSQLiteOwner          *storedecision.DecisionSQLiteOwner
	deliverySQLiteOwner          *storedelivery.DeliverySQLiteOwner
	entitySQLiteOwner            *storeentity.EntitySQLiteOwner
	effectSQLiteOwner            *storeeffect.EffectSQLiteOwner
	eventSQLiteOwner             *storeevent.EventSQLiteOwner
	runtimeIngressSQLiteOwner    *storeingress.RuntimeIngressSQLiteOwner
	lLMSQLiteOwner               *storellm.LLMSQLiteOwner
	managedCapabilitySQLiteOwner *storemanagedcapability.ManagedCapabilitySQLiteOwner
	mailboxSQLiteOwner           *storemailbox.MailboxSQLiteOwner
	operatorSQLite               *storeoperatorsurface.OperatorSQLite
	pipelineSQLiteOwner          *storepipeline.PipelineSQLiteOwner
	replySQLiteOwner             *storereplycontext.ReplySQLiteOwner
	runForkSQLiteOwner           *storerunfork.RunForkSQLiteOwner
	runLifecycleSQLiteOwner      *storerunlifecycle.RunLifecycleSQLiteOwner
	timerObligationSQLiteReader  *storetimerobligation.SQLiteReader
	genericScheduleSQLiteOwner   *storegenericschedule.SQLiteOwner

	schema                *SQLiteSchemaStore
	backend               *sqlitebackend.Backend
	runBundles            *storerunbundle.SQLite
	workflowEntityQueries *storeworkflowentityquery.SQLite
	workflowRoutes        *storeworkflowroute.SQLite
	workspaceLookups      *storeworkspace.SQLite

	nowFn                  func() time.Time
	runLifecycleCandidates *storerunhandoff.CandidateCoordinator
}

var _ SchemaBootstrapper = (*SQLiteRuntimeStore)(nil)
var _ runtimebus.EventStore = (*SQLiteRuntimeStore)(nil)
var _ runtimebus.ActiveFlowInstanceDescriptorLister = (*SQLiteRuntimeStore)(nil)
var _ runtimebus.RunLifecycleReadPersistence = (*SQLiteRuntimeStore)(nil)
var _ runtimemanager.ManagerPersistence = (*SQLiteRuntimeStore)(nil)
var _ runtimetools.MailboxPersistence = (*SQLiteRuntimeStore)(nil)
var _ runtimeingress.Store = (*SQLiteRuntimeStore)(nil)
var _ runtimeruncontrol.Store = (*SQLiteRuntimeStore)(nil)

func (s *SQLiteRuntimeStore) Path() string {
	if s == nil || s.schema == nil {
		return ""
	}
	return s.schema.Path()
}

func (s *SQLiteRuntimeStore) Close() error {
	if s == nil || s.schema == nil {
		return nil
	}
	return s.schema.Close()
}

func (s *SQLiteRuntimeStore) Ping(ctx context.Context) error {
	if s == nil || s.schema == nil {
		return fmt.Errorf("sqlite runtime store is required")
	}
	return s.schema.Ping(ctx)
}

func (s *SQLiteRuntimeStore) BootstrapSchema(ctx context.Context, request SchemaBootstrapRequest) error {
	if s == nil || s.schema == nil {
		return fmt.Errorf("sqlite runtime store is required")
	}
	return s.schema.BootstrapSchema(ctx, request)
}

func (s *SQLiteRuntimeStore) SetSessionLockTTL(ttl time.Duration) {
	if s == nil {
		return
	}
	if ttl <= 0 {
		ttl = 120 * time.Second
	}
	s.lLMSQLiteOwner.SetSessionLockTTL(ttl)
}

func (s *SQLiteRuntimeStore) now() time.Time {
	if s == nil || s.nowFn == nil {
		return time.Now().UTC()
	}
	return s.nowFn().UTC()
}

func (s *SQLiteRuntimeStore) SetNowFnForTest(nowFn func() time.Time) {
	if s == nil {
		return
	}
	if nowFn == nil {
		nowFn = time.Now
	}
	s.nowFn = nowFn
	if s.genericScheduleSQLiteOwner != nil {
		s.genericScheduleSQLiteOwner.SetNowFnForTest(nowFn)
	}
}

func (s *SQLiteRuntimeStore) ListEventDeliveryRecipients(ctx context.Context, eventID string) ([]string, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil, errors.New("event_id is required for authoritative delivery recipient readback")
	}
	snapshots, err := s.DeliverySnapshotsForEvent(ctx, eventID)
	if err != nil {
		return nil, fmt.Errorf("query sqlite delivery recipients: %w", err)
	}
	out := make([]string, 0)
	for _, snapshot := range snapshots {
		if snapshot.Route.Recipient.IsAgent() {
			out = append(out, strings.TrimSpace(snapshot.SubscriberID))
		}
	}
	sort.Strings(out)
	return out, nil
}

func sqliteNullString(raw string) any {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	return raw
}

func sqliteNullUUID(raw string) any {
	raw = nullUUIDString(raw)
	if raw == "" {
		return nil
	}
	return raw
}

func sqliteTimeValue(raw any) (time.Time, bool, error) {
	switch v := raw.(type) {
	case nil:
		return time.Time{}, false, nil
	case time.Time:
		if v.IsZero() {
			return time.Time{}, false, nil
		}
		return v.UTC(), true, nil
	case string:
		return parseSQLiteTimeString(v)
	case []byte:
		return parseSQLiteTimeString(string(v))
	default:
		return time.Time{}, false, fmt.Errorf("unsupported sqlite time value %T", raw)
	}
}

func parseSQLiteTimeString(raw string) (time.Time, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false, nil
	}
	formats := []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999 -0700 MST",
		"2006-01-02 15:04:05.999999 -0700 MST",
		"2006-01-02 15:04:05 -0700 MST",
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05-07:00",
		"2006-01-02 15:04:05Z07:00",
		"2006-01-02 15:04:05",
	}
	var lastErr error
	for _, layout := range formats {
		parsed, err := time.Parse(layout, raw)
		if err == nil {
			return parsed.UTC(), true, nil
		}
		lastErr = err
	}
	return time.Time{}, false, fmt.Errorf("parse sqlite time %q: %w", raw, lastErr)
}
