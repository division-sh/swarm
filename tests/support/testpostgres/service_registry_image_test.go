package testpostgres

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestServiceRegistryProvisionAdmitsLocalPostgresImage(t *testing.T) {
	const list = "image ls --no-trunc --format {{.Repository}}:{{.Tag}} {{.ID}} postgres:16"
	const inspect = "image inspect --format {{.Id}} postgres:16"
	const pull = "pull postgres:16"
	imageID := "sha256:" + strings.Repeat("a", 64)
	local := "postgres:16 " + imageID
	for _, test := range []struct {
		name       string
		local      string
		listErr    error
		pullErr    error
		inspectErr error
		imageID    string
		wantPulls  int
		wantErr    string
	}{
		{name: "local_image_with_registry_offline", local: local, pullErr: errors.New("registry unavailable"), imageID: imageID},
		{name: "missing_image_pulled_once", imageID: imageID, wantPulls: 1},
		{name: "other_tag", local: "postgres:17 " + imageID, wantErr: "unexpected local image listing"},
		{name: "other_repository", local: "other/postgres:16 " + imageID, wantErr: "unexpected local image listing"},
		{name: "tag_prefix", local: "postgres:16-alpine " + imageID, wantErr: "unexpected local image listing"},
		{name: "malformed_listing", local: "not an image listing", wantErr: "unexpected local image listing"},
		{name: "ambiguous_listing", local: local + "\n" + local, wantErr: "unexpected local image listing"},
		{name: "daemon_failure_is_not_absence", listErr: errors.New("daemon unavailable"), wantErr: "daemon unavailable"},
		{name: "permission_failure_is_not_absence", listErr: errors.New("permission denied"), wantErr: "permission denied"},
		{name: "expired_listing_is_not_absence", listErr: context.DeadlineExceeded, wantErr: "context deadline exceeded"},
		{name: "missing_image_pull_failure", pullErr: errors.New("registry unavailable"), wantPulls: 1, wantErr: "registry unavailable"},
		{name: "missing_after_successful_pull", inspectErr: errors.New("No such image"), wantPulls: 1, wantErr: "No such image"},
		{name: "local_image_inspection_failure", local: local, inspectErr: errors.New("inspect unavailable"), wantErr: "inspect unavailable"},
		{name: "local_image_empty_identity", local: local, wantErr: "empty image ID"},
		{name: "pulled_image_empty_identity", wantPulls: 1, wantErr: "empty image ID"},
		{name: "malformed_identity", wantPulls: 1, imageID: "not-an-image-id", wantErr: "malformed image ID"},
		{name: "ambiguous_identity", local: local, imageID: imageID + "\n" + imageID, wantErr: "malformed image ID"},
		{name: "tag_changed_during_admission", local: local, imageID: "sha256:" + strings.Repeat("b", 64), wantErr: "image identity changed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry, _ := testRegistryRecord(t, ServicePrepared)
			docker := registry.docker.(*fakeDocker)
			docker.outputs[list] = []byte(test.local)
			docker.errors[list] = test.listErr
			docker.errors[pull] = test.pullErr
			docker.outputs["info --format {{.ID}}"] = []byte("daemon")
			if test.local != "" {
				docker.outputs[inspect] = []byte(test.imageID)
				docker.errors[inspect] = test.inspectErr
			} else {
				docker.errors[inspect] = errors.New("image inspected before pull")
				docker.afterCall = func(call string) {
					if call == pull && test.pullErr == nil {
						docker.outputs[inspect] = []byte(test.imageID)
						docker.errors[inspect] = test.inspectErr
					}
				}
			}
			admitted := errors.New("stop before service record publication")
			var prepared *ServiceRecord
			registry.beforeRegistrySave = func(doc registryDocument) error {
				for _, record := range doc.Services {
					if record.State == ServicePrepared {
						prepared = &record
						return admitted
					}
				}
				return nil
			}
			service, err := registry.Provision(context.Background(), "must-not-launch")
			if service != nil {
				t.Fatal("image control launched a service")
			}
			if test.wantErr == "" {
				if !errors.Is(err, admitted) || prepared == nil || prepared.ImageID != test.imageID || prepared.SpecHash != serviceSpecHash(test.imageID) {
					t.Fatalf("image was not admitted with exact identity: prepared=%+v err=%v calls=%v", prepared, err, docker.calls)
				}
			} else if err == nil || !strings.Contains(err.Error(), test.wantErr) || prepared != nil {
				t.Fatalf("image failure did not refuse before publication: prepared=%+v err=%v calls=%v", prepared, err, docker.calls)
			}
			pulls := 0
			for _, call := range docker.calls {
				if call == pull {
					pulls++
				}
			}
			if pulls != test.wantPulls {
				t.Fatalf("pull count=%d, want %d; calls=%v", pulls, test.wantPulls, docker.calls)
			}
			if doc, err := registry.registrySnapshot(); err != nil || len(doc.Services) != 0 {
				t.Fatalf("image control left service authority: %+v %v", doc, err)
			}
			assertNoNewImageServiceAuthority(t, registry, "")
		})
	}
}

type imageDockerFunc func(context.Context, ...string) ([]byte, error)

func (f imageDockerFunc) CombinedOutput(ctx context.Context, args ...string) ([]byte, error) {
	return f(ctx, args...)
}

func TestServiceRegistryImageCancellationPublishesNoAuthority(t *testing.T) {
	for _, cut := range []string{"before_query", "within_query", "after_query", "within_pull", "after_pull", "within_inspect", "after_inspect"} {
		t.Run(cut, func(t *testing.T) {
			registry, original := testRegistryRecord(t, ServicePrepared)
			fake := registry.docker.(*fakeDocker)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if cut == "before_query" {
				cancel()
			}
			reached := cut == "before_query"
			registry.docker = imageDockerFunc(func(got context.Context, args ...string) ([]byte, error) {
				if got != ctx {
					t.Fatal("image admission replaced the provisioning context")
				}
				if err := got.Err(); err != nil {
					return nil, err
				}
				phase := ""
				if args[0] == "pull" {
					phase = "pull"
				} else if len(args) > 1 && args[0] == "image" {
					phase = map[string]string{"ls": "query", "inspect": "inspect"}[args[1]]
				}
				if cut == "within_"+phase || cut == "after_"+phase {
					reached = true
					cancel()
					if strings.HasPrefix(cut, "within_") {
						return nil, got.Err()
					}
				}
				if phase == "inspect" {
					return []byte("sha256:" + strings.Repeat("a", 64)), nil
				}
				return fake.CombinedOutput(got, args...)
			})
			service, err := registry.Provision(ctx, "must-not-launch")
			if !reached || service != nil || err == nil || !strings.Contains(err.Error(), "context canceled") {
				t.Fatalf("cancellation did not refuse: reached=%t service=%v err=%v", reached, service, err)
			}
			assertNoNewImageServiceAuthority(t, registry, original.LeaseID)
		})
	}
}

func TestServiceRegistryConcurrentColdImagesKeepExactServiceIdentity(t *testing.T) {
	registry, _ := testRegistryRecord(t, ServicePrepared)
	fake := registry.docker.(*fakeDocker)
	imageID := "sha256:" + strings.Repeat("a", 64)
	fake.outputs["info --format {{.ID}}"] = []byte("daemon")
	fake.outputs["image inspect --format {{.Id}} postgres:16"] = []byte(imageID)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	queries, release := make(chan struct{}, 2), make(chan struct{})
	var mu sync.Mutex
	registry.docker = imageDockerFunc(func(ctx context.Context, args ...string) ([]byte, error) {
		if len(args) > 1 && args[0] == "image" && args[1] == "ls" {
			queries <- struct{}{}
			select {
			case <-release:
				return nil, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		mu.Lock()
		defer mu.Unlock()
		return fake.CombinedOutput(ctx, args...)
	})
	admitted := errors.New("stop before service record publication")
	var prepared []ServiceRecord
	registry.beforeRegistrySave = func(doc registryDocument) error {
		for _, record := range doc.Services {
			if record.State == ServicePrepared {
				mu.Lock()
				prepared = append(prepared, record)
				mu.Unlock()
				return admitted
			}
		}
		return nil
	}
	results := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := registry.Provision(ctx, "must-not-launch")
			results <- err
		}()
	}
	for range 2 {
		select {
		case <-queries:
		case <-ctx.Done():
			t.Fatal("cold callers did not both reach the controlled image cut")
		}
	}
	close(release)
	for range 2 {
		if err := <-results; !errors.Is(err, admitted) {
			t.Fatalf("concurrent image admission: %v", err)
		}
	}
	if len(prepared) != 2 || prepared[0].RunnerID == prepared[1].RunnerID || prepared[0].LeaseID == prepared[1].LeaseID || prepared[0].OwnerID != prepared[1].OwnerID {
		t.Fatalf("cold callers crossed their service identities: %+v", prepared)
	}
	for _, record := range prepared {
		if record.ImageID != imageID || record.DaemonID != "daemon" || record.SpecHash != serviceSpecHash(imageID) || record.CIDFile != filepath.Join(registry.StateRoot, "handoff", record.LeaseID+".cid") {
			t.Fatalf("cold caller changed its captured image/service coordinates: %+v", record)
		}
		if err := validateServiceRecord(record.LeaseID, record); err != nil {
			t.Fatal(err)
		}
	}
	pulls := 0
	for _, call := range fake.calls {
		if call == "pull postgres:16" {
			pulls++
		}
	}
	if pulls != 2 {
		t.Fatalf("cold callers pulled %d times, want one per attempt", pulls)
	}
	assertNoNewImageServiceAuthority(t, registry, "")
}

func assertNoNewImageServiceAuthority(t *testing.T, registry *ServiceRegistry, original string) {
	t.Helper()
	doc, err := registry.registrySnapshot()
	if err != nil {
		t.Fatal(err)
	}
	for id := range doc.Services {
		if id != original {
			t.Fatalf("unexpected service authority %s", id)
		}
	}
	for _, directory := range []string{"leases", "creators", "handoff"} {
		entries, err := os.ReadDir(filepath.Join(registry.StateRoot, directory))
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if original == "" || directory != "leases" || entry.Name() != original+".lock" {
				t.Fatalf("unexpected %s authority %s", directory, entry.Name())
			}
		}
	}
}
