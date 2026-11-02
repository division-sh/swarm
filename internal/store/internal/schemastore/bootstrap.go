package schemastore

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

func (o RuntimeStoreOrigin) canonical() RuntimeStoreOrigin {
	if !o.CreatedAt.IsZero() {
		o.CreatedAt = o.CreatedAt.UTC().Round(time.Microsecond)
	}
	return o
}

func (o RuntimeStoreOrigin) Canonical() RuntimeStoreOrigin {
	return o.canonical()
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

func (o RuntimeStoreOrigin) validateStored() error {
	if strings.TrimSpace(o.SwarmVersion) == "" {
		return fmt.Errorf("stored Swarm version is required")
	}
	if strings.TrimSpace(o.PlatformVersion) == "" {
		return fmt.Errorf("stored platform version is required")
	}
	if o.CreatedAt.IsZero() {
		return fmt.Errorf("stored creation time must be non-zero")
	}
	return nil
}

type SchemaBootstrapRequest struct {
	PlatformPlans   []SchemaTableDDL
	Origin          RuntimeStoreOrigin
	deploymentPlans []SchemaTableDDL
}

func (r SchemaBootstrapRequest) canonical() SchemaBootstrapRequest {
	r.Origin = r.Origin.canonical()
	// Registration is additive deployment state. Its absence never makes an
	// otherwise unchanged runtime schema incompatible; existing tables still
	// undergo the complete platform comparison before any DDL is executed.
	platform := make([]SchemaTableDDL, 0, len(r.PlatformPlans))
	for _, plan := range r.PlatformPlans {
		if plan.TableName == "deployment_channel_operations" {
			r.deploymentPlans = append(r.deploymentPlans, plan)
		} else {
			platform = append(platform, plan)
		}
	}
	r.PlatformPlans = platform
	return r
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
	if len(r.deploymentPlans) > 1 {
		return fmt.Errorf("duplicate deployment registration schema")
	}
	for _, plan := range r.deploymentPlans {
		if plan.SchemaKind != "platform_spec" || plan.TableName != "deployment_channel_operations" {
			return fmt.Errorf("invalid deployment registration schema")
		}
	}
	return nil
}

type SchemaBootstrapper interface {
	BootstrapSchema(context.Context, SchemaBootstrapRequest) error
}
