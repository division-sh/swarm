package testpostgres

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

type removalDocker struct {
	base   dockerExecutor
	remove func(context.Context, ...string) ([]byte, error)
}

func (d removalDocker) CombinedOutput(ctx context.Context, args ...string) ([]byte, error) {
	if len(args) > 0 && args[0] == "rm" {
		return d.remove(ctx, args...)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return d.base.CombinedOutput(ctx, args...)
}

func TestServiceCloseRechecksTimedOutRemoval(t *testing.T) {
	for _, outcome := range []string{"absent", "remaining", "inspection_failure"} {
		t.Run(outcome, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				registry, record := testRegistryRecord(t, ServiceReady)
				attachContainerIdentity(t, &record, "container-id")
				if err := registry.putRecord(record); err != nil {
					t.Fatal(err)
				}
				fake := dockerWithContainers(t, testDockerInspect(record.ContainerID, record.LeaseID, record.RunnerID))
				inspectionFailure := errors.New("injected absence inspection failure")
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
				defer cancel()
				registry.docker = removalDocker{base: fake, remove: func(ctx context.Context, args ...string) ([]byte, error) {
					<-ctx.Done()
					switch outcome {
					case "absent":
						if _, err := fake.CombinedOutput(context.Background(), args...); err != nil {
							t.Fatal(err)
						}
					case "inspection_failure":
						fake.outputs["inspect "+record.ContainerID] = nil
						fake.errors["inspect "+record.ContainerID] = inspectionFailure
					}
					return []byte("injected slow removal"), errors.New("signal: killed")
				}}
				service := &Service{registry: registry, record: record}
				err := service.Close(ctx)
				if outcome == "absent" {
					if err != nil {
						t.Fatalf("confirmed completed removal: %v", err)
					}
					if _, err := registry.record(record.LeaseID); !os.IsNotExist(err) {
						t.Fatalf("confirmed removal retained registry row: %v", err)
					}
					assertServiceAuthorityAbsent(t, registry, record)
					return
				}
				if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "signal: killed") {
					t.Fatalf("removal failure lost its cause: %v", err)
				}
				if outcome == "remaining" && !strings.Contains(err.Error(), "still exists") {
					t.Fatalf("remaining container not reported: %v", err)
				}
				if outcome == "inspection_failure" && !errors.Is(err, inspectionFailure) {
					t.Fatalf("independent inspection failure lost: %v", err)
				}
				if _, err := registry.record(record.LeaseID); err != nil {
					t.Fatalf("unconfirmed removal lost reconciliation evidence: %v", err)
				}
			})
		})
	}
}

func TestServiceClosePreservesNonTimeoutRemovalFailure(t *testing.T) {
	registry, record := testRegistryRecord(t, ServiceReady)
	attachContainerIdentity(t, &record, "container-id")
	if err := registry.putRecord(record); err != nil {
		t.Fatal(err)
	}
	fake := dockerWithContainers(t, testDockerInspect(record.ContainerID, record.LeaseID, record.RunnerID))
	removeFailure := errors.New("injected removal failure")
	registry.docker = removalDocker{base: fake, remove: func(context.Context, ...string) ([]byte, error) {
		return []byte("daemon unavailable"), removeFailure
	}}
	err := (&Service{registry: registry, record: record}).Close(context.Background())
	if !errors.Is(err, removeFailure) || !strings.Contains(err.Error(), "daemon unavailable") {
		t.Fatalf("removal failure lost: %v", err)
	}
	if _, err := registry.record(record.LeaseID); err != nil {
		t.Fatalf("failed removal lost reconciliation evidence: %v", err)
	}
}

func TestServiceCloseRechecksTimedOutAbsenceVerification(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		registry, record := testRegistryRecord(t, ServiceReady)
		attachContainerIdentity(t, &record, "container-id")
		if err := registry.putRecord(record); err != nil {
			t.Fatal(err)
		}
		fake := dockerWithContainers(t, testDockerInspect(record.ContainerID, record.LeaseID, record.RunnerID))
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		registry.docker = removalDocker{base: fake, remove: func(ctx context.Context, args ...string) ([]byte, error) {
			<-ctx.Done()
			return fake.CombinedOutput(context.Background(), args...)
		}}
		if err := (&Service{registry: registry, record: record}).Close(ctx); err != nil {
			t.Fatalf("confirmed removal after the original verification context expired: %v", err)
		}
		if _, err := registry.record(record.LeaseID); !os.IsNotExist(err) {
			t.Fatalf("confirmed removal retained registry row: %v", err)
		}
		assertServiceAuthorityAbsent(t, registry, record)
	})
}
