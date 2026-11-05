package cliapp

import (
	"context"
	"fmt"
	"io"
	"time"
)

type diagnosticTraceInstance struct {
	runID, entityID, instance string
	life                      int64
	startedAt                 time.Time
}

func readDiagnosticTraceInstance(ctx context.Context, client *cliAPIClient, params map[string]any) (*diagnosticTraceInstance, error) {
	filter, _ := params["filter"].(map[string]any)
	entities, _ := filter["entity_id"].([]string)
	if len(entities) != 1 {
		return nil, nil
	}
	runID, _ := params["run_id"].(string)
	request := map[string]any{"run_id": runID, "entity_id": entities[0]}
	if iter, present := filter["iter"]; present {
		request["iter"] = iter
	}
	var result entityFull
	if err := client.call(ctx, entityGetMethod, request, &result); err != nil {
		return nil, err
	}
	if err := validateEntityFullResult("run.trace entity.get result", result); err != nil {
		return nil, err
	}
	entity := result.Entity
	if entity.RunID != runID || entity.EntityID != entities[0] || entity.Life == nil || *entity.Life <= 0 {
		return nil, fmt.Errorf("run.trace presentation requires its exact run, entity and positive instance life")
	}
	if iter, present := filter["iter"]; present && iter != *entity.Life {
		return nil, fmt.Errorf("run.trace entity.get returned a different selected instance life")
	}
	started, err := entityLifeStartedAt(result)
	if err != nil {
		return nil, err
	}
	return &diagnosticTraceInstance{runID: runID, entityID: entity.EntityID, instance: entity.FlowInstance,
		life: *entity.Life, startedAt: started}, nil
}

func validateDiagnosticTraceInstance(ctx context.Context, client *cliAPIClient, params map[string]any, before *diagnosticTraceInstance) error {
	filter, _ := params["filter"].(map[string]any)
	if _, explicit := filter["iter"]; before == nil || explicit {
		return nil
	}
	after, err := readDiagnosticTraceInstance(ctx, client, params)
	if err != nil {
		return err
	}
	if after == nil || before.runID != after.runID || before.entityID != after.entityID || before.instance != after.instance ||
		before.life != after.life || !before.startedAt.Equal(after.startedAt) {
		return fmt.Errorf("run.trace instance life or start date changed during the finite read")
	}
	return nil
}

func writeDiagnosticTraceInstance(out io.Writer, instance *diagnosticTraceInstance) {
	if out == nil || instance == nil {
		return
	}
	fmt.Fprintf(out, "instance %s life %d", cliSanitizeOneLineValue(instance.instance), instance.life)
	if instance.life > 1 {
		fmt.Fprintf(out, " started over on %s", instance.startedAt.UTC().Format(time.RFC3339Nano))
	}
	fmt.Fprintln(out)
}
