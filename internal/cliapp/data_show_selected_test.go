package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestDataShowCLIReadFamilyAcrossSelectedStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
			var selected interface {
				apiv1.DurableDataStore
				EnsureSourceArtifactWithData(context.Context, *sourceartifact.AdmittedSourceArtifact, durabledata.Catalog) (sourceartifact.EnsureResult, error)
			}
			if backend == "sqlite" {
				selected = storetest.StartSQLiteRuntimeStore(t)
			} else {
				_, db, _ := testutil.StartPostgres(t)
				selected = storetest.AdmitPostgresRuntimeStore(t, db)
			}
			ctx := context.Background()
			artifact := sourceartifactfixture.New("schema.yaml", []byte("name: data-show-cli\n"))
			ref, err := durabledata.ParseDeclarationRef(".", "records.loaded")
			if err != nil {
				t.Fatal(err)
			}
			schema := map[string]any{"type": "object", "additionalProperties": false, "required": []string{"slug", "body"}, "properties": map[string]any{"slug": map[string]any{"type": "string"}, "body": map[string]any{"type": "string"}}}
			compiled, defects := durabledata.CompileJSONL(ref, schema, "slug", nil)
			if len(defects) != 0 {
				t.Fatal(defects)
			}
			shape := durabledata.ImportShape{BundleHash: artifact.BundleHash(), Declaration: ref, SchemaDigest: compiled.Manifest.SchemaDigest, BusinessKey: "slug", Fields: []durabledata.ImportShapeField{{Name: "body", Required: true, Text: true}, {Name: "slug", Required: true, Text: true}}}
			catalog := durabledata.Catalog{BundleHash: artifact.BundleHash(), Declarations: []durabledata.Declaration{{Name: ref.EventName, Ref: ref, BusinessKey: "slug", SchemaDigest: compiled.Manifest.SchemaDigest, CanonicalSchema: compiled.CanonicalSchema}}, ImportShapes: []durabledata.ImportShape{shape}}
			if _, err := selected.EnsureSourceArtifactWithData(ctx, artifact, catalog); err != nil {
				t.Fatal(err)
			}
			var input strings.Builder
			for i := 0; i < 1001; i++ {
				raw, _ := json.Marshal(map[string]any{"slug": fmt.Sprintf("row-%04d", i), "body": "text"})
				input.Write(raw)
				input.WriteByte('\n')
			}
			result, err := selected.ExecuteDataSourceOperation(ctx, durabledata.SourceCommand{Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: artifact.BundleHash(), Declaration: ref, ExpectedHead: durabledata.AbsentHead(), InputFormat: "jsonl", Input: []byte(input.String())})
			if err != nil || result.Outcome != "accepted" {
				t.Fatalf("import=%#v/%v", result, err)
			}
			work := worklifetime.NewProcess()
			handler, err := apiv1.NewHandler(apiv1.Options{PlatformSpecPath: ResolvePath(RepoRoot(), defaultPlatformSpecPath), AuthTokens: []string{"test-token"}, ProcessWorkOwner: work, Handlers: apiv1.OperatorDataHandlers(apiv1.DataHandlerOptions{Store: selected})})
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(handler)
			defer server.Close()
			defer func() {
				work.Retire()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := work.Wait(ctx); err != nil {
					t.Error(err)
				}
			}()
			root := testRootCommandOptions(server)
			root.invocationRoot = mustInvocationRootForTest(t.TempDir())
			for _, mode := range []string{"text", "json", "quiet"} {
				for _, operand := range []string{"inventory", "./records.loaded@head", "./records.loaded@v1", "./records.loaded@" + string(result.Candidate.VersionID), "row"} {
					t.Run(mode+"/"+operand, func(t *testing.T) {
						args := []string{"--bundle-hash", artifact.BundleHash()}
						if mode != "text" {
							args = append(args, "--"+mode)
						}
						if operand == "row" {
							args = append(args, "./records.loaded@v1", "--key", `"row-0001"`)
						} else if operand != "inventory" {
							args = append(args, operand)
						}
						cmd := newDataShowCommand(root)
						var out, errOut bytes.Buffer
						cmd.SetOut(&out)
						cmd.SetErr(&errOut)
						cmd.SetArgs(args)
						if err := cmd.ExecuteContext(ctx); err != nil {
							t.Fatalf("CLI %v: %v/%s", args, err, errOut.String())
						}
						if out.Len() == 0 || errOut.Len() != 0 {
							t.Fatalf("CLI output=%s/%s", out.String(), errOut.String())
						}
						if operand != "inventory" && operand != "row" && !strings.Contains(out.String(), string(result.Candidate.VersionID)) {
							t.Fatalf("version selection lost: %s", out.String())
						}
						if operand == "row" && !strings.Contains(out.String(), "row-0001") {
							t.Fatalf("row selection lost: %s", out.String())
						}
					})
				}
			}
			t.Run("unsupported_yaml", func(t *testing.T) {
				cmd := newDataShowCommand(root)
				cmd.SetOut(&bytes.Buffer{})
				cmd.SetErr(&bytes.Buffer{})
				cmd.SetArgs([]string{"--yaml"})
				if err := cmd.ExecuteContext(ctx); err == nil || !strings.Contains(err.Error(), "unknown flag: --yaml") {
					t.Fatalf("unsupported output mode was admitted: %v", err)
				}
			})
			t.Run("jsonl_multi_page", func(t *testing.T) {
				cmd := newDataShowCommand(root)
				var out bytes.Buffer
				cmd.SetOut(&out)
				cmd.SetArgs([]string{"--bundle-hash", artifact.BundleHash(), "./records.loaded@v1", "--format", "jsonl"})
				if err := cmd.ExecuteContext(ctx); err != nil {
					t.Fatal(err)
				}
				stored, _, err := selected.ResolveDataVersionPayload(ctx, ref, durabledata.VersionSelector{Kind: "head"})
				if err != nil || stored.Manifest.RowCount != 1001 {
					t.Fatalf("selected summary=%#v/%v", stored, err)
				}
				canonical, defects := durabledata.CompileJSONL(ref, schema, "slug", []byte(input.String()))
				if len(defects) != 0 || !bytes.Equal(out.Bytes(), canonical.CanonicalJSONL) {
					t.Fatalf("multi-page CLI export differs: bytes=%d/%d", out.Len(), len(canonical.CanonicalJSONL))
				}
			})
			client, err := newCLIAPIClientForTest(t, root)
			if err != nil {
				t.Fatal(err)
			}
			declaration, err := resolveDataDeclaration(ctx, client, artifact.BundleHash(), "./records.loaded")
			if err != nil {
				t.Fatal(err)
			}
			loadedShape, err := loadFileImportShape(ctx, client, artifact.BundleHash(), declaration)
			if err != nil || loadedShape.BusinessKey != "slug" || len(loadedShape.Fields) != 2 || !loadedShape.Fields["body"].Text || loadedShape.Fields["body"].Optional || !loadedShape.Fields["slug"].Text || loadedShape.Fields["slug"].Optional {
				t.Fatalf("CLI compiled shape=%#v/%v", loadedShape, err)
			}
			t.Run("jsonl_empty", func(t *testing.T) {
				empty, err := selected.ExecuteDataSourceOperation(ctx, durabledata.SourceCommand{Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: artifact.BundleHash(), Declaration: ref, ExpectedHead: durabledata.VersionHead(result.Candidate.VersionID), InputFormat: "jsonl"})
				if err != nil || empty.Outcome != "accepted" {
					t.Fatalf("empty import=%#v/%v", empty, err)
				}
				cmd := newDataShowCommand(root)
				var out bytes.Buffer
				cmd.SetOut(&out)
				cmd.SetArgs([]string{"--bundle-hash", artifact.BundleHash(), "./records.loaded@head", "--format", "jsonl"})
				if err := cmd.ExecuteContext(ctx); err != nil || out.Len() != 0 {
					t.Fatalf("canonical empty CLI export=%q/%v", out.String(), err)
				}
			})
		})
	}
}
