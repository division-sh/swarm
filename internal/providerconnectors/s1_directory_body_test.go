package providerconnectors

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/packmodel"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

func s1ConnectorDirectoryMembers() map[string][]byte {
	return map[string][]byte{
		"manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\ncapabilities: {can: {call_provider_actions: [send_message]}, cannot: [bypass journal]}\n"),
		"connector.yaml": []byte(`tools:
  send_message:
    category: provider_connector
    handler_type: http
    effect_class: non_idempotent_write
    description: Send a message using the provider protocol
    credentials: [bot_token]
    http:
      method: POST
      url: 'https://api.example/bot{{credentials.bot_token}}/sendMessage?text={{input.text}}'
      headers: {X-Token: 'Bearer {{credentials.bot_token}}'}
      body:
        text: '{{input.text}}'
        authorization: '{{credentials.bot_token}}'
        nested: [{credential: '{{credentials.bot_token}}'}, 7, false, null]
        literal: 'credentials.bot_token and telegram.send_message stay literal'
    response_success: {kind: http_status_2xx}
`),
	}
}

func s1CapturedConnectorBody(t testing.TB, key string, members map[string][]byte) packartifact.ProviderBody {
	t.Helper()
	provider, err := packartifact.AdmitProviderMembers(key, members)
	if err != nil {
		t.Fatal(err)
	}
	body, present := provider.Body(packmodel.BodyConnector)
	if !present {
		t.Fatal("admitted provider lost connector role")
	}
	return body
}

func TestS1ConnectorBodyQualifiesToolsRolesAndCompiledTemplatesTogether(t *testing.T) {
	members := s1ConnectorDirectoryMembers()
	var firstHash string
	for _, key := range []string{"team-bot", "team_bot", "9-team"} {
		t.Run(key, func(t *testing.T) {
			body := s1CapturedConnectorBody(t, key, members)
			manifest, err := AdmitBody(body)
			if err != nil {
				t.Fatal(err)
			}
			tool, present := manifest.Tools[key+".send_message"]
			if manifest.Provider != key || len(manifest.Tools) != 1 || !present || len(tool.Credentials()) != 1 || tool.Credentials()[0] != key+".bot_token" {
				t.Fatalf("directory did not own complete tool/role identity: %+v", manifest)
			}
			if firstHash != "" && body.Hash() != firstHash {
				t.Fatal("directory qualification changed exact body hash")
			}
			firstHash = body.Hash()
			if !bytes.Equal(body.Bytes(), members["connector.yaml"]) || manifest.SourceValue().Location().File != key+"/connector.yaml" {
				t.Fatal("connector lost exact source bytes or lexical provenance")
			}
			execution, _ := tool.HTTPExecution()
			prepared, err := execution.Prepare(map[string]any{"text": "hello / world"}, map[string]any{key + ".bot_token": "fixture/token"})
			if err != nil || prepared.URL() != "https://api.example/botfixture%2Ftoken/sendMessage?text=hello%20%2F%20world" || prepared.Headers().Get("X-Token") != "Bearer fixture/token" {
				t.Fatalf("compiled request did not select qualified role: request=%+v err=%v", prepared, err)
			}
			var payload map[string]any
			if err := json.Unmarshal(prepared.Body(), &payload); err != nil {
				t.Fatal(err)
			}
			if payload["authorization"] != "fixture/token" || payload["text"] != "hello / world" || payload["literal"] != "credentials.bot_token and telegram.send_message stay literal" {
				t.Fatalf("template qualification rewrote payload values: %+v", payload)
			}
			nested := payload["nested"].([]any)
			if nested[0].(map[string]any)["credential"] != "fixture/token" || nested[1] != float64(7) || nested[2] != false || nested[3] != nil {
				t.Fatalf("recursive BODY template changed semantic values: %+v", nested)
			}
			for _, wrong := range []map[string]any{{"bot_token": "local"}, {"foreign.bot_token": "foreign"}, {key: map[string]any{"bot_token": "nested"}}} {
				if _, err := execution.Prepare(map[string]any{"text": "hello"}, wrong); err == nil {
					t.Fatal("missing exact qualified role accepted another key")
				}
			}
			value, err := tool.CanonicalValue()
			if err != nil {
				t.Fatal(err)
			}
			canonical, err := json.Marshal(value)
			if err != nil || !bytes.Contains(canonical, []byte("credentials."+key+".bot_token")) || !bytes.Contains(canonical, []byte(key+".bot_token")) {
				t.Fatalf("machine identity omitted qualified credential binding: %s err=%v", canonical, err)
			}
			copy := value["http"].(map[string]any)
			copy["url"] = "https://foreign.example"
			second, err := execution.Prepare(map[string]any{"text": "again"}, map[string]any{key + ".bot_token": "fixture/token"})
			if err != nil || !strings.HasPrefix(second.URL(), "https://api.example/") {
				t.Fatal("mutable canonical readback changed compiled HTTP owner")
			}
			if err := tool.Validate(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestS1ConnectorBodyRefusesQualifiedNamesRolesAndForeignTemplates(t *testing.T) {
	for _, cell := range []struct{ name, old, replacement string }{
		{"qualified tool", "  send_message:", "  team-bot.send_message:"},
		{"foreign tool", "  send_message:", "  foreign.send_message:"},
		{"path tool", "  send_message:", "  child/send_message:"},
		{"qualified role", "credentials: [bot_token]", "credentials: [team-bot.bot_token]"},
		{"whitespace role", "credentials: [bot_token]", "credentials: [' bot_token ']"},
		{"duplicate role", "credentials: [bot_token]", "credentials: [bot_token, bot_token]"},
		{"foreign template", "{{credentials.bot_token}}", "{{credentials.foreign.bot_token}}"},
		{"qualified template", "{{credentials.bot_token}}", "{{credentials.team-bot.bot_token}}"},
		{"undeclared template", "{{credentials.bot_token}}", "{{credentials.other}}"},
		{"credential object", "{{credentials.bot_token}}", "{{credentials}}"},
		{"credential array", "{{credentials.bot_token}}", "{{credentials.bot_token[0]}}"},
	} {
		t.Run(cell.name, func(t *testing.T) {
			members := s1ConnectorDirectoryMembers()
			members["connector.yaml"] = bytes.ReplaceAll(members["connector.yaml"], []byte(cell.old), []byte(cell.replacement))
			body := s1CapturedConnectorBody(t, "team-bot", members)
			if _, err := AdmitBody(body); err == nil || !strings.Contains(err.Error(), "team-bot/connector.yaml:") {
				t.Fatalf("invalid local-name or template acquired a connector: %v", err)
			}
		})
	}
	if _, err := AdmitBody(packartifact.ProviderBody{}); err == nil {
		t.Fatal("zero BODY became a connector")
	}
}

func TestS1ConnectorBodyManagedRolePreservesGrantPolicy(t *testing.T) {
	for _, key := range []string{"team-bot", "team_bot"} {
		members := s1ConnectorDirectoryMembers()
		members["connector.yaml"] = []byte(`tools:
  send_message:
    category: provider_connector
    handler_type: http
    effect_class: non_idempotent_write
    managed_credential:
      key: oauth
      header: X-Provider-Token
      scopes: [message.write]
      grant_type: client_credentials
      token_request: {client_auth: basic, body: form}
    http: {method: POST, url: 'https://api.example/send', body: {text: '{{input.text}}'}}
    response_success: {kind: http_status_2xx}
`)
		body := s1CapturedConnectorBody(t, key, members)
		manifest, err := AdmitBody(body)
		if err != nil {
			t.Fatal(err)
		}
		tool := manifest.Tools[key+".send_message"]
		managed, present := tool.ManagedCredentialExecution()
		if !present || managed.Key() != key+".oauth" || managed.Header() != "X-Provider-Token" || managed.GrantType() != "client_credentials" || strings.Join(managed.Scopes(), ",") != "message.write" || managed.TokenRequest().ClientAuth != "basic" || len(tool.Credentials()) != 0 {
			t.Fatalf("namespace projection changed managed authority: %+v", managed.Readback())
		}
		for _, invalid := range []string{"' oauth '", "team-bot.oauth", "foreign.oauth"} {
			bad := s1ConnectorDirectoryMembers()
			bad["connector.yaml"] = bytes.ReplaceAll(members["connector.yaml"], []byte("key: oauth"), []byte("key: "+invalid))
			if _, err := AdmitBody(s1CapturedConnectorBody(t, key, bad)); err == nil {
				t.Fatalf("invalid managed role %q was repaired", invalid)
			}
		}
	}
}

func TestS1ConnectorBodyLogicalReadbackAndRegistryKeepSeparateKeys(t *testing.T) {
	members := map[string][]byte{"schema.yaml": []byte("description: root\n")}
	for _, key := range []string{"team-bot", "team_bot"} {
		for label, body := range s1ConnectorDirectoryMembers() {
			members[key+"/"+label] = body
		}
	}
	artifact, err := sourceartifact.AdmitMembers(members)
	if err != nil {
		t.Fatal(err)
	}
	retained, err := sourceartifact.DecodeLogical(artifact.LogicalBlob())
	if err != nil {
		t.Fatal(err)
	}
	var loaded []LoadedPack
	for _, key := range []string{"team-bot", "team_bot"} {
		provider, err := packartifact.ProviderFromArtifact(retained, key)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := provider.Body(packmodel.BodyConnector)
		manifest, err := AdmitBody(body)
		if err != nil {
			t.Fatal(err)
		}
		pack := s1ConnectorPack(key)
		pack.Manifest = manifest
		pack.ManifestBody = body.Bytes()
		pack.Envelope.ManifestHash = body.Hash()
		loaded = append(loaded, pack)
	}
	registry, err := NewPackRegistry(loaded...)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"team-bot", "team_bot"} {
		pack, present := registry.Lookup(key, key+".send_message")
		if !present || pack.Manifest.Tools[key+".send_message"].Credentials()[0] != key+".bot_token" {
			t.Fatal("retained registry lost exact directory credential identity")
		}
	}
	subjects, err := CapabilitySubjects(context.Background(), semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}), CapabilityOptions{Registry: registry, IncludeInstalled: true})
	if err != nil || len(subjects) != 2 || subjects[0].Provider == subjects[1].Provider {
		t.Fatalf("installed readback collapsed provider copies: %+v err=%v", subjects, err)
	}
}

func TestS1ConnectorBodyGenerationReferencesDeclaredShortTools(t *testing.T) {
	for _, key := range []string{"team-bot", "team_bot"} {
		members := s1ConnectorDirectoryMembers()
		members["connector.yaml"] = append(members["connector.yaml"], []byte(`generation:
  schema_version: '1'
  generator_version: swarm-openapi-gen/v1
  source: {path: catalog/source.json, openapi_version: 3.0.3, sha256: 'sha256:`+strings.Repeat("a", 64)+`'}
  profile: {path: catalog/profile.yaml, schema_version: '1', sha256: 'sha256:`+strings.Repeat("b", 64)+`'}
  operations:
    - operation_id: messages/send
      tool_id: send_message
      permissions: [{id: messages:write, note: Message write permission}]
      response_success: {kind: http_status_2xx}
      fixture_id: messages/send
      fixture_status: passing
      review_status: approved
`)...)
		body := s1CapturedConnectorBody(t, key, members)
		manifest, err := AdmitBody(body)
		if err != nil {
			t.Fatal(err)
		}
		if manifest.Generation == nil || len(manifest.Generation.Operations) != 1 || manifest.Generation.Operations[0].ToolID != key+".send_message" {
			t.Fatalf("generation operation escaped directory tool owner: %+v", manifest.Generation)
		}
		pack := s1ConnectorPack(key)
		pack.Manifest = manifest
		pack.Envelope.ManifestHash = body.Hash()
		if _, present := GenerationSurfaceForPackTool(pack, key+".send_message"); !present {
			t.Fatal("compiled generation surface lost exact qualified tool")
		}
		for _, toolID := range []string{key + ".send_message", "foreign.send_message", "' send_message '", "absent"} {
			bad := s1ConnectorDirectoryMembers()
			bad["connector.yaml"] = bytes.Replace(members["connector.yaml"], []byte("tool_id: send_message"), []byte("tool_id: "+toolID), 1)
			if _, err := AdmitBody(s1CapturedConnectorBody(t, key, bad)); err == nil || !strings.Contains(err.Error(), "declared short tool name") {
				t.Fatalf("generation reference %q gained another identity: %v", toolID, err)
			}
		}
	}
}

func TestS1ConnectorBodyAliasesAndHeaderRemainSourceOwned(t *testing.T) {
	members := s1ConnectorDirectoryMembers()
	members["connector.yaml"] = bytes.Replace(members["connector.yaml"], []byte("send_message:\n"), []byte("send_message: &operation\n"), 1)
	members["connector.yaml"] = append(members["connector.yaml"], []byte("  send_copy: *operation\n")...)
	body := s1CapturedConnectorBody(t, "team-bot", members)
	manifest, err := AdmitBody(body)
	if err != nil || len(manifest.Tools) != 2 {
		t.Fatalf("aliased tool declaration lost its admitted role: %+v err=%v", manifest, err)
	}
	for _, name := range []string{"team-bot.send_message", "team-bot.send_copy"} {
		tool := manifest.Tools[name]
		if len(tool.Credentials()) != 1 || tool.Credentials()[0] != "team-bot.bot_token" {
			t.Fatalf("alias was qualified twice or lost credential: %s %+v", name, tool.Credentials())
		}
		execution, _ := tool.HTTPExecution()
		if _, err := execution.Prepare(map[string]any{"text": "hello"}, map[string]any{"team-bot.bot_token": "fixture"}); err != nil {
			t.Fatal(err)
		}
	}
	for _, provider := range []string{"team-bot", "foreign", "null"} {
		bad := s1ConnectorDirectoryMembers()
		bad["connector.yaml"] = append(bad["connector.yaml"], []byte("provider: "+provider+"\n")...)
		if _, err := packartifact.AdmitProviderMembers("team-bot", bad); err == nil || !strings.Contains(err.Error(), "directory is the provider name") {
			t.Fatalf("authored provider header bypassed shared ownership: %v", err)
		}
	}
}
