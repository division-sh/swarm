package main

import (
	"fmt"
	"io"
	"strings"
)

const doctorSchemaInventoryOwner = "platform-spec.yaml#cli_specification.command_catalog.doctor.schema_inventory"

type doctorSchemaInventory struct {
	Owner       string                    `json:"owner"`
	TableCount  int                       `json:"table_count"`
	ColumnCount int                       `json:"column_count"`
	Tables      []serveSchemaTableSummary `json:"tables"`
}

func buildDoctorSchemaInventory(repo string, paths cliContractPlatformSpecPaths) (doctorSchemaInventory, error) {
	_, bundle, err := newSwarmWorkflowModule(repo, paths.ContractsPath, paths.PlatformSpecPath)
	if err != nil {
		return doctorSchemaInventory{}, fmt.Errorf("load contract schema source: %w", err)
	}
	plans, err := stateStoreSchemaPlans(bundle)
	if err != nil {
		return doctorSchemaInventory{}, err
	}
	summary := newServeSchemaPlanSummary(plans.all())
	return doctorSchemaInventory{
		Owner:       doctorSchemaInventoryOwner,
		TableCount:  summary.tableCount,
		ColumnCount: summary.columnCount,
		Tables:      append([]serveSchemaTableSummary(nil), summary.tables...),
	}, nil
}

func writeDoctorSchemaInventoryText(out io.Writer, inventory *doctorSchemaInventory) {
	if out == nil || inventory == nil {
		return
	}
	fmt.Fprintf(out, "schema inventory: %d tables · %d columns\n", inventory.TableCount, inventory.ColumnCount)
	for _, table := range inventory.Tables {
		fmt.Fprintf(out, "  %s · %d columns\n", strings.TrimSpace(table.Name), table.ColumnCount)
	}
}
