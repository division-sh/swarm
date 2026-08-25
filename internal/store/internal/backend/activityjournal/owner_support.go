package activityjournal

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	authoractivityadapter "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity/readadapter"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	storerunstate "github.com/division-sh/swarm/internal/store/internal/backend/runstate"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
)

type ActivityPostgresOwner struct {
	backend     *postgresbackend.Backend
	schemaGuard func() error
	catalogMu   sync.Mutex
	catalog     *runtimeauthoractivity.EventCatalogRegistry
}

type ActivitySQLiteOwner struct {
	backend     *sqlitebackend.Backend
	schemaGuard func() error
	catalogMu   sync.Mutex
	catalog     *runtimeauthoractivity.EventCatalogRegistry
}

func (s *ActivityPostgresOwner) ListAuthorActivity(ctx context.Context, opts runtimeauthoractivity.ListOptions) (runtimeauthoractivity.ListResult, error) {
	if s == nil || s.backend == nil {
		return runtimeauthoractivity.ListResult{}, fmt.Errorf("postgres author-activity owner is required")
	}
	return authoractivityadapter.List(ctx, s.backend, authoractivityadapter.DialectPostgres, opts)
}

func (s *ActivitySQLiteOwner) ListAuthorActivity(ctx context.Context, opts runtimeauthoractivity.ListOptions) (runtimeauthoractivity.ListResult, error) {
	if s == nil || s.backend == nil {
		return runtimeauthoractivity.ListResult{}, fmt.Errorf("sqlite author-activity owner is required")
	}
	return authoractivityadapter.List(ctx, s.backend, authoractivityadapter.DialectSQLite, opts)
}

func (s *ActivityPostgresOwner) HeadAuthorActivity(ctx context.Context) (int64, error) {
	if s == nil || s.backend == nil {
		return 0, fmt.Errorf("postgres author-activity owner is required")
	}
	return authoractivityadapter.Head(ctx, s.backend)
}

func (s *ActivitySQLiteOwner) HeadAuthorActivity(ctx context.Context) (int64, error) {
	if s == nil || s.backend == nil {
		return 0, fmt.Errorf("sqlite author-activity owner is required")
	}
	return authoractivityadapter.Head(ctx, s.backend)
}

func (s *ActivityPostgresOwner) RegisterAuthorActivityEventCatalog(scope runtimeauthoractivity.Scope, descriptors []runtimeauthoractivity.EventDescriptor) (*runtimeauthoractivity.EventCatalogLease, error) {
	return s.authorActivityCatalog().Register(scope, descriptors)
}

func (s *ActivitySQLiteOwner) RegisterAuthorActivityEventCatalog(scope runtimeauthoractivity.Scope, descriptors []runtimeauthoractivity.EventDescriptor) (*runtimeauthoractivity.EventCatalogLease, error) {
	return s.authorActivityCatalog().Register(scope, descriptors)
}

func (s *ActivityPostgresOwner) ResolveAuthorActivityEventDescriptor(scope runtimeauthoractivity.Scope, name string) (runtimeauthoractivity.EventDescriptor, bool) {
	return s.authorActivityCatalog().Resolve(scope, name)
}

func (s *ActivitySQLiteOwner) ResolveAuthorActivityEventDescriptor(scope runtimeauthoractivity.Scope, name string) (runtimeauthoractivity.EventDescriptor, bool) {
	return s.authorActivityCatalog().Resolve(scope, name)
}

func (s *ActivityPostgresOwner) AuthorActivityEventCatalogRegistered(scope runtimeauthoractivity.Scope) bool {
	return s.authorActivityCatalog().HasScope(scope)
}

func (s *ActivitySQLiteOwner) AuthorActivityEventCatalogRegistered(scope runtimeauthoractivity.Scope) bool {
	return s.authorActivityCatalog().HasScope(scope)
}

func (s *ActivityPostgresOwner) authorActivityCatalog() *runtimeauthoractivity.EventCatalogRegistry {
	s.catalogMu.Lock()
	defer s.catalogMu.Unlock()
	if s.catalog == nil {
		s.catalog = runtimeauthoractivity.NewEventCatalogRegistry()
	}
	return s.catalog
}

func (s *ActivitySQLiteOwner) authorActivityCatalog() *runtimeauthoractivity.EventCatalogRegistry {
	s.catalogMu.Lock()
	defer s.catalogMu.Unlock()
	if s.catalog == nil {
		s.catalog = runtimeauthoractivity.NewEventCatalogRegistry()
	}
	return s.catalog
}

func NewPostgres(backend *postgresbackend.Backend, schemaGuard func() error) (*ActivityPostgresOwner, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("activity journal postgres backend is required")
	}
	return &ActivityPostgresOwner{backend: backend, schemaGuard: schemaGuard}, nil
}

func NewSQLite(backend *sqlitebackend.Backend, schemaGuard func() error) (*ActivitySQLiteOwner, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("activity journal sqlite backend is required")
	}
	return &ActivitySQLiteOwner{backend: backend, schemaGuard: schemaGuard}, nil
}

func (s *ActivityPostgresOwner) runPrivateAuthorActivityMutation(ctx context.Context, operation func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
	if s == nil || s.schemaGuard == nil {
		return fmt.Errorf("activity journal postgres owner is required")
	}
	if err := s.schemaGuard(); err != nil {
		return err
	}
	return s.backend.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		story, err := privateauthoractivity.Begin(txctx, tx, privateauthoractivity.DialectPostgres)
		if err != nil {
			return err
		}
		if err := operation(txctx, tx, story); err != nil {
			return err
		}
		return story.Finalize(txctx)
	})
}

func (s *ActivitySQLiteOwner) runPrivateAuthorActivityMutation(ctx context.Context, label string, operation func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
	if s == nil || s.schemaGuard == nil {
		return fmt.Errorf("activity journal sqlite owner is required")
	}
	if err := s.schemaGuard(); err != nil {
		return err
	}
	return s.backend.RunTransaction(ctx, label, func(txctx context.Context, tx *sql.Tx) error {
		story, err := privateauthoractivity.Begin(txctx, tx, privateauthoractivity.DialectSQLite)
		if err != nil {
			return err
		}
		if err := operation(txctx, tx, story); err != nil {
			return err
		}
		return story.Finalize(txctx)
	})
}

func requirePostgresRunActive(ctx context.Context, tx *sql.Tx, runID string) error {
	return storerunstate.RequirePostgresActiveTx(ctx, tx, runID)
}

func requireSQLiteRunActive(ctx context.Context, tx *sql.Tx, runID string) error {
	return storerunstate.RequireSQLiteActiveTx(ctx, tx, runID)
}
