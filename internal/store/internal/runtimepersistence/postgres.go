package runtimepersistence

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/config"
	storeadmin "github.com/division-sh/swarm/internal/store/internal/adminpersistence"
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
	storeoperatorchannel "github.com/division-sh/swarm/internal/store/internal/backend/operatorchannel"
	storepipeline "github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	storereplycontext "github.com/division-sh/swarm/internal/store/internal/backend/replycontext"
	storerunfork "github.com/division-sh/swarm/internal/store/internal/backend/runforkpersistence"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
	storetimerobligation "github.com/division-sh/swarm/internal/store/internal/backend/timerobligation"
	storebudgetspend "github.com/division-sh/swarm/internal/store/internal/budgetspend"
	storebundlecatalog "github.com/division-sh/swarm/internal/store/internal/bundlecatalog"
	storeingress "github.com/division-sh/swarm/internal/store/internal/ingresspersistence"
	storemailbox "github.com/division-sh/swarm/internal/store/internal/mailboxpersistence"
	storeoperatorsurface "github.com/division-sh/swarm/internal/store/internal/operatorsurface"
	storepreservation "github.com/division-sh/swarm/internal/store/internal/preservationpersistence"
	storeroutingrules "github.com/division-sh/swarm/internal/store/internal/routingrules"
	storerunbundle "github.com/division-sh/swarm/internal/store/internal/runbundle"
	storerunhandoff "github.com/division-sh/swarm/internal/store/internal/runhandoff"
	storeschema "github.com/division-sh/swarm/internal/store/internal/schemastore"
	storestartupownership "github.com/division-sh/swarm/internal/store/internal/startupownership"
	storeworkflowentityquery "github.com/division-sh/swarm/internal/store/internal/workflowentityquery"
	storeworkflowroute "github.com/division-sh/swarm/internal/store/internal/workflowroute"
	storeworkspace "github.com/division-sh/swarm/internal/store/internal/workspace"
)

type PostgresStore struct {
	agentPostgresOwner             *storeagent.AgentPostgresOwner
	bundleDeletePostgresOwner      *storeadmin.BundleDeletePostgresOwner
	destructiveResetPostgresOwner  *storeadmin.DestructiveResetPostgresOwner
	activityPostgresOwner          *storeactivityjournal.ActivityPostgresOwner
	activityResultPostgresOwner    *storeactivityresult.ActivityResultPostgresOwner
	postgresOwner                  *storeapiidempotency.PostgresOwner
	budgetPostgresOwner            *storebudgetspend.BudgetPostgresOwner
	postgres                       *storebundlecatalog.Postgres
	decisionPostgresOwner          *storedecision.DecisionPostgresOwner
	deliveryPostgresOwner          *storedelivery.DeliveryPostgresOwner
	entityPostgresOwner            *storeentity.EntityPostgresOwner
	effectPostgresOwner            *storeeffect.EffectPostgresOwner
	eventPostgresOwner             *storeevent.EventPostgresOwner
	runtimeIngressPostgresOwner    *storeingress.RuntimeIngressPostgresOwner
	lLMPostgresOwner               *storellm.LLMPostgresOwner
	managedCapabilityPostgresOwner *storemanagedcapability.ManagedCapabilityPostgresOwner
	mailboxPostgresOwner           *storemailbox.MailboxPostgresOwner
	operatorRunPostgres            *storeoperatorsurface.RunPostgres
	operatorEntityPostgres         *storeoperatorsurface.EntityPostgres
	operatorAgentPostgres          *storeoperatorsurface.AgentPostgres
	operatorConversationPostgres   *storeoperatorsurface.ConversationPostgres
	operatorObservabilityPostgres  *storeoperatorsurface.ObservabilityPostgres
	pipelinePostgresOwner          *storepipeline.PipelinePostgresOwner
	preservationPostgresOwner      *storepreservation.PreservationPostgresOwner
	replyPostgresOwner             *storereplycontext.ReplyPostgresOwner
	runForkPostgresOwner           *storerunfork.RunForkPostgresOwner
	routingPostgresOwner           *storeroutingrules.RoutingPostgresOwner
	runLifecyclePostgresOwner      *storerunlifecycle.RunLifecyclePostgresOwner
	startupPostgresOwner           *storestartupownership.StartupPostgresOwner
	timerObligationPostgresReader  *storetimerobligation.PostgresReader
	genericSchedulePostgresOwner   *storegenericschedule.PostgresOwner
	operatorChannelPostgresOwner   *storeoperatorchannel.PostgresOwner

	backend               *postgresbackend.Backend
	runBundles            *storerunbundle.Postgres
	workflowEntityQueries *storeworkflowentityquery.Postgres
	workflowRoutes        *storeworkflowroute.Postgres
	workspaceLookups      *storeworkspace.Postgres
	schemaOwner           *storeschema.Postgres

	runLifecycleCandidates *storerunhandoff.CandidateCoordinator
}

type EventPayloadValidator func(ctx context.Context, eventType string, payload []byte) error

func DSNFromConfig(cfg config.DatabaseConfig, password string) string {
	host := strings.TrimSpace(cfg.Host)
	if host == "" {
		host = "127.0.0.1"
	}
	port := cfg.Port
	if port == 0 {
		port = 5432
	}
	name := strings.TrimSpace(cfg.Name)
	if name == "" {
		name = "swarm"
	}
	sslMode := strings.TrimSpace(cfg.SSLMode)
	if sslMode == "" {
		sslMode = "disable"
	}
	user := strings.TrimSpace(cfg.User)
	if user == "" {
		user = "postgres"
	}
	parts := []string{
		postgresKeywordParam("host", host),
		fmt.Sprintf("port=%d", port),
		postgresKeywordParam("dbname", name),
		postgresKeywordParam("sslmode", sslMode),
		postgresKeywordParam("user", user),
	}
	if password != "" {
		parts = append(parts, postgresKeywordParam("password", password))
	}
	return strings.Join(parts, " ")
}

func postgresKeywordParam(key, value string) string {
	if value == "" {
		return key + "="
	}
	return fmt.Sprintf("%s='%s'", key, escapePostgresKeywordValue(value))
}

func escapePostgresKeywordValue(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	return strings.ReplaceAll(value, `'`, `\'`)
}

func (s *PostgresStore) SetSessionLockTTL(ttl time.Duration) {
	if s == nil {
		return
	}
	if ttl <= 0 {
		ttl = 120 * time.Second
	}
	s.lLMPostgresOwner.SetSessionLockTTL(ttl)
}

func (s *PostgresStore) Ping(ctx context.Context) error {
	if s == nil || !s.backend.Valid() {
		return fmt.Errorf("postgres store is required")
	}
	return s.backend.Ping(ctx)
}

func (s *PostgresStore) Close() error {
	if s == nil || !s.backend.Valid() {
		return nil
	}
	return s.backend.Close()
}
