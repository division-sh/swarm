package serveapp

import (
	"context"
	"fmt"
	"testing"

	"github.com/division-sh/swarm/internal/store/storetest"
)

func requireProviderAliasStandingRun(t *testing.T, rt servedWorkspaceProofRuntime, flow string) string {
	t.Helper()
	rows, err := rt.Standing.ListStandingServiceStatuses(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var runs []string
	for _, row := range rows {
		if row.BundleHash == rt.BundleHash && row.FlowPath == flow {
			runs = append(runs, row.RunID)
		}
	}
	if len(runs) != 1 || runs[0] == "" {
		t.Fatalf("exact standing source %s/%s has runs=%v, want one", rt.BundleHash, flow, runs)
	}
	return runs[0]
}

type providerAliasActionStorage struct {
	InterfaceKey, State, Disposition string
	DispositionPresent               bool
}

func providerAliasActionIntentStorage(table storetest.SelectedForkStorageTableSnapshot, publicationID string) (providerAliasActionStorage, error) {
	rows, err := workspaceProofPhysicalRows(table)
	if err != nil {
		return providerAliasActionStorage{}, err
	}
	var exact []providerAliasActionStorage
	for _, row := range rows {
		id, err := workspaceProofPhysicalText(row, "publication_id")
		if err != nil {
			return providerAliasActionStorage{}, err
		}
		if id != publicationID {
			continue
		}
		var out providerAliasActionStorage
		if out.InterfaceKey, err = workspaceProofPhysicalText(row, "interface_key"); err != nil {
			return providerAliasActionStorage{}, err
		}
		if out.State, err = workspaceProofPhysicalText(row, "state"); err != nil {
			return providerAliasActionStorage{}, err
		}
		if string(row["interface_key"].Value) == "null" || string(row["state"].Value) == "null" {
			return providerAliasActionStorage{}, fmt.Errorf("action intent has NULL interface or state")
		}
		if out.Disposition, err = workspaceProofPhysicalText(row, "disposition"); err != nil {
			return providerAliasActionStorage{}, err
		}
		out.DispositionPresent = string(row["disposition"].Value) != "null"
		exact = append(exact, out)
	}
	if len(exact) != 1 {
		return providerAliasActionStorage{}, fmt.Errorf("exact action intent count=%d, want one", len(exact))
	}
	return exact[0], nil
}

type providerAliasHeaderStorage struct {
	EntityID, EntityType, Template, Mode string
	EntityTypePresent                    bool
}

func providerAliasConstructedHeaderStorage(table storetest.SelectedForkStorageTableSnapshot, runID, instance string) (providerAliasHeaderStorage, error) {
	rows, err := workspaceProofPhysicalRows(table)
	if err != nil {
		return providerAliasHeaderStorage{}, err
	}
	var exact []providerAliasHeaderStorage
	for _, row := range rows {
		run, err := workspaceProofPhysicalText(row, "run_id")
		if err != nil {
			return providerAliasHeaderStorage{}, err
		}
		path, err := workspaceProofPhysicalText(row, "instance_path")
		if err != nil {
			return providerAliasHeaderStorage{}, err
		}
		if run != runID || path != instance {
			continue
		}
		var out providerAliasHeaderStorage
		for column, target := range map[string]*string{
			"entity_id": &out.EntityID, "entity_type": &out.EntityType, "flow_template": &out.Template, "mode": &out.Mode,
		} {
			if *target, err = workspaceProofPhysicalText(row, column); err != nil {
				return providerAliasHeaderStorage{}, err
			}
			if column != "entity_type" && string(row[column].Value) == "null" {
				return providerAliasHeaderStorage{}, fmt.Errorf("constructed header has NULL %s", column)
			}
		}
		out.EntityTypePresent = string(row["entity_type"].Value) != "null"
		exact = append(exact, out)
	}
	if len(exact) != 1 {
		return providerAliasHeaderStorage{}, fmt.Errorf("exact constructed header count=%d, want one", len(exact))
	}
	return exact[0], nil
}

func providerAliasFieldRowCounts(table storetest.SelectedForkStorageTableSnapshot, runID, instance, entityID, entityType string) (int, int, error) {
	rows, err := workspaceProofPhysicalRows(table)
	if err != nil {
		return 0, 0, err
	}
	all, exact := 0, 0
	for _, row := range rows {
		run, err := workspaceProofPhysicalText(row, "run_id")
		if err != nil {
			return 0, 0, err
		}
		path, err := workspaceProofPhysicalText(row, "flow_instance")
		if err != nil {
			return 0, 0, err
		}
		if run != runID || path != instance {
			continue
		}
		all++
		entity, err := workspaceProofPhysicalText(row, "entity_id")
		if err != nil {
			return 0, 0, err
		}
		typeName, err := workspaceProofPhysicalText(row, "entity_type")
		if err != nil {
			return 0, 0, err
		}
		if entity == entityID && typeName == entityType {
			exact++
		}
	}
	return all, exact, nil
}

func providerAliasReplayCompletionCount(table storetest.SelectedForkStorageTableSnapshot, key string) (int, error) {
	rows, err := workspaceProofPhysicalRows(table)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, row := range rows {
		method, err := workspaceProofPhysicalText(row, "method")
		if err != nil {
			return 0, err
		}
		storedKey, err := workspaceProofPhysicalText(row, "idempotency_key")
		if err != nil {
			return 0, err
		}
		if method == "event.replay" && storedKey == key {
			count++
		}
	}
	return count, nil
}

func TestProviderAliasPhysicalTupleProofPreservesScopeNullsAndMultiplicity(t *testing.T) {
	actionColumns := []string{"publication_id", "interface_key", "state", "disposition"}
	actions := storetest.SelectedForkStorageTableSnapshot{
		Columns: actionColumns,
		Rows:    []string{workspaceProofPhysicalRow(t, actionColumns, "foreign", "other", "settled", "applied"), workspaceProofPhysicalRow(t, actionColumns, "exact", "bound-interface", "pending", nil)},
	}
	got, err := providerAliasActionIntentStorage(actions, "exact")
	if err != nil || got.InterfaceKey != "bound-interface" || got.State != "pending" || got.DispositionPresent {
		t.Fatalf("exact publication or nullable disposition changed: %+v %v", got, err)
	}
	actions.Rows[1] = workspaceProofPhysicalRow(t, actionColumns, "exact", "bound-interface", "settled", "rejected")
	if got, err := providerAliasActionIntentStorage(actions, "exact"); err != nil || !got.DispositionPresent || got.Disposition != "rejected" {
		t.Fatalf("settled disposition lost evidence: %+v %v", got, err)
	}
	actions.Rows = append(actions.Rows, actions.Rows[1])
	if got, err := providerAliasActionIntentStorage(actions, "exact"); err == nil || got != (providerAliasActionStorage{}) {
		t.Fatalf("duplicate publication leaked partial evidence: %+v %v", got, err)
	}
	headerColumns := []string{"run_id", "instance_path", "entity_id", "entity_type", "flow_template", "mode"}
	headers := storetest.SelectedForkStorageTableSnapshot{
		Columns: headerColumns,
		Rows:    []string{workspaceProofPhysicalRow(t, headerColumns, "exact-run", "node", "entity", nil, "node", "static")},
	}
	if got, err := providerAliasConstructedHeaderStorage(headers, "exact-run", "node"); err != nil || got.EntityTypePresent || got.EntityID != "entity" || got.Template != "node" || got.Mode != "static" {
		t.Fatalf("physical fieldless header changed: %+v %v", got, err)
	}
	headers.Rows[0] = workspaceProofPhysicalRow(t, headerColumns, "exact-run", "node", "entity", "", "node", "static")
	if got, err := providerAliasConstructedHeaderStorage(headers, "exact-run", "node"); err != nil || !got.EntityTypePresent || got.EntityType != "" {
		t.Fatalf("NULL type was conflated with present empty type: %+v %v", got, err)
	}
	fieldColumns := []string{"run_id", "flow_instance", "entity_id", "entity_type"}
	fields := storetest.SelectedForkStorageTableSnapshot{
		Columns: fieldColumns,
		Rows: []string{
			workspaceProofPhysicalRow(t, fieldColumns, "exact-run", "node", "entity", "receipt"),
			workspaceProofPhysicalRow(t, fieldColumns, "exact-run", "node", "orphan", "other"),
			workspaceProofPhysicalRow(t, fieldColumns, "foreign", "node", "entity", "receipt"),
			workspaceProofPhysicalRow(t, fieldColumns, "exact-run", "other", "entity", "receipt"),
		},
	}
	if all, exact, err := providerAliasFieldRowCounts(fields, "exact-run", "node", "entity", "receipt"); err != nil || all != 2 || exact != 1 {
		t.Fatalf("orphan rows or exact companion scope hidden: all=%d exact=%d err=%v", all, exact, err)
	}
	apiColumns := []string{"method", "idempotency_key", "actor"}
	api := storetest.SelectedForkStorageTableSnapshot{
		Columns: apiColumns,
		Rows: []string{
			workspaceProofPhysicalRow(t, apiColumns, "event.replay", "key", "a"),
			workspaceProofPhysicalRow(t, apiColumns, "event.replay", "key", "b"),
			workspaceProofPhysicalRow(t, apiColumns, "event.replay", "other", "a"),
			workspaceProofPhysicalRow(t, apiColumns, "agent.replay", "key", "a"),
		},
	}
	if count, err := providerAliasReplayCompletionCount(api, "key"); err != nil || count != 2 {
		t.Fatalf("API method/key/actor multiplicity changed: %d %v", count, err)
	}
	for _, raw := range []string{workspaceProofPhysicalRow(t, actionColumns, "exact", nil, "pending", nil), workspaceProofPhysicalRow(t, actionColumns, "exact", "bound-interface", nil, nil), `{`} {
		actions.Rows = []string{raw}
		if got, err := providerAliasActionIntentStorage(actions, "exact"); err == nil || got != (providerAliasActionStorage{}) {
			t.Fatalf("malformed action intent became successful evidence: %+v %v", got, err)
		}
	}
	// Ensure the physical parser keeps nested field text, not a re-encoded value.
	encoded := workspaceProofPhysicalRow(t, []string{"fields"}, `{"decimal":7.0,"nested":[null,true]}`)
	rows, err := workspaceProofPhysicalRows(storetest.SelectedForkStorageTableSnapshot{Columns: []string{"fields"}, Rows: []string{encoded}})
	if err != nil {
		t.Fatal(err)
	}
	if text, err := workspaceProofPhysicalText(rows[0], "fields"); err != nil || text != `{"decimal":7.0,"nested":[null,true]}` {
		t.Fatalf("stored field JSON bytes normalized: %s %v", text, err)
	}
}
