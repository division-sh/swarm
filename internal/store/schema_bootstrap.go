package store

import (
	"context"
	"fmt"
	"strings"
	"time"
)

type SchemaDialect string

const (
	SchemaDialectPostgres SchemaDialect = "postgres"
	SchemaDialectSQLite   SchemaDialect = "sqlite"
)

const RuntimeStoreMetadataTable = "runtime_store_metadata"

type RuntimeStoreOrigin struct {
	SwarmVersion    string
	PlatformVersion string
	CreatedAt       time.Time
}

func (o RuntimeStoreOrigin) validate() error {
	if strings.TrimSpace(o.SwarmVersion) == "" {
		return fmt.Errorf("running Swarm version is required for schema bootstrap")
	}
	if strings.TrimSpace(o.PlatformVersion) == "" {
		return fmt.Errorf("running platform version is required for schema bootstrap")
	}
	if o.CreatedAt.IsZero() {
		return fmt.Errorf("store creation time is required for schema bootstrap")
	}
	return nil
}

type SchemaBootstrapRequest struct {
	PlatformPlans []SchemaTableDDL
	StatePlans    []SchemaTableDDL
	Origin        RuntimeStoreOrigin
}

func (r SchemaBootstrapRequest) validate() error {
	if len(r.PlatformPlans) == 0 {
		return fmt.Errorf("platform schema plans are required")
	}
	if err := r.Origin.validate(); err != nil {
		return err
	}
	for _, plan := range r.PlatformPlans {
		if strings.TrimSpace(plan.SchemaKind) != "platform_spec" {
			return fmt.Errorf("platform schema request contains %s table %s", strings.TrimSpace(plan.SchemaKind), strings.TrimSpace(plan.TableName))
		}
	}
	for _, plan := range r.StatePlans {
		if strings.TrimSpace(plan.SchemaKind) != "state_schema" {
			return fmt.Errorf("generated state schema request contains %s table %s", strings.TrimSpace(plan.SchemaKind), strings.TrimSpace(plan.TableName))
		}
	}
	return nil
}

type SchemaBootstrapper interface {
	BootstrapSchema(context.Context, SchemaBootstrapRequest) error
	ResolveSchemaCapabilities(context.Context) (StoreSchemaCapabilities, error)
}
