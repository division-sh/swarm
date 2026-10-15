package testpostgres

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestServiceRegistryProvisionAdmitsLocalPostgresImage(t *testing.T) {
	const list = "image ls --quiet --no-trunc postgres:16"
	const inspect = "image inspect --format {{.Id}} postgres:16"
	const pull = "pull postgres:16"
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
		{name: "local_image_with_registry_offline", local: "image", pullErr: errors.New("registry unavailable"), imageID: "image"},
		{name: "missing_image_pulled_once", imageID: "image", wantPulls: 1},
		{name: "daemon_failure_is_not_absence", listErr: errors.New("daemon unavailable"), wantErr: "daemon unavailable"},
		{name: "permission_failure_is_not_absence", listErr: errors.New("permission denied"), wantErr: "permission denied"},
		{name: "expired_listing_is_not_absence", listErr: context.DeadlineExceeded, wantErr: "context deadline exceeded"},
		{name: "missing_image_pull_failure", pullErr: errors.New("registry unavailable"), wantPulls: 1, wantErr: "registry unavailable"},
		{name: "missing_after_successful_pull", inspectErr: errors.New("No such image"), wantPulls: 1, wantErr: "No such image"},
		{name: "local_image_inspection_failure", local: "image", inspectErr: errors.New("inspect unavailable"), wantErr: "inspect unavailable"},
		{name: "local_image_empty_identity", local: "image", wantErr: "empty image ID"},
		{name: "pulled_image_empty_identity", wantPulls: 1, wantErr: "empty image ID"},
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
		})
	}
}
