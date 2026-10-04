package serveapp

import (
	"context"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestReviewer2558TriggerOnlyRotationRequiresAdmission(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			credentialPath := filepath.Join(t.TempDir(), "credentials.json")
			t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
			file, err := credentials.NewFileStore(credentialPath)
			if err != nil {
				t.Fatal(err)
			}
			if err := file.Set(context.Background(), "webhook_signing.telegram", "original-secret"); err != nil {
				t.Fatal(err)
			}
			root := writeStandingTelegramServeFixture(t, "http://127.0.0.1:1")
			disableChannelOnboardingBusinessConsumers(t, root)
			opts := cliapp.ServeOptions{SourceRoot: root, PlatformSpecPath: defaultPlatformSpecPath,
				APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", SelfCheck: true, Verbose: true,
				WorkspaceBackend: "host", WorkspaceBackendSet: true, StoreMode: backend, StoreModeSet: true}
			var manager *runtime.RuntimeContextManager
			opts.TestRuntimeContextsReadyHook = func(m *runtime.RuntimeContextManager) { manager = m }
			if backend == "sqlite" {
				opts.ConfigPath = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, backend, filepath.Join(t.TempDir(), "store.sqlite"), channelOnboardingHostWorkspaceFields())
			} else {
				dsn, _, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				opts.ConfigPath = writeChannelOnboardingPostgresRuntimeConfig(t, dsn)
			}
			process := startServeRuntimeTestProcess(t, opts)
			process.waitForReadyLine()
			t.Cleanup(func() { process.stop() })
			endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString())
			post := func(id, secret string) int {
				t.Helper()
				body := `{"update_id":` + id + `,"message":{"message_id":` + id + `,"from":{"id":42},"chat":{"id":42,"type":"private"},"text":"probe"}}`
				req, err := http.NewRequest(http.MethodPost, endpoint+"/webhooks/chat/telegram", strings.NewReader(body))
				if err != nil {
					t.Fatal(err)
				}
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("X-Telegram-Bot-Api-Secret-Token", secret)
				response, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				io.Copy(io.Discard, response.Body)
				response.Body.Close()
				return response.StatusCode
			}
			if code := post("80001", "original-secret"); code != http.StatusAccepted {
				t.Fatalf("original admission HTTP %d", code)
			}
			if err := file.Set(context.Background(), "webhook_signing.telegram", "unadmitted-replacement"); err != nil {
				t.Fatal(err)
			}
			if code := post("80002", "unadmitted-replacement"); code == http.StatusAccepted {
				t.Error("trigger-only ingress accepted replacement signing authority without restart or explicit admission")
			}
			owner, err := credentials.NewSnapshotOwner(file)
			if err != nil {
				t.Fatal(err)
			}
			subjects, err := manager.EvaluatedCapabilitySubjects(context.Background(), owner)
			if err == nil {
				for _, subject := range subjects {
					if subject.Kind == packs.SubjectProviderTrigger && subject.Applicability == "effective" && subject.Status == packs.StatusReady {
						t.Error("capability readback reports READY for unadmitted replacement signing authority")
					}
				}
			}
		})
	}
}
