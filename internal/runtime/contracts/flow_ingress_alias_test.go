package contracts

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/sourceartifact"
)

func TestA9RootedAliasCompilation(t *testing.T) {
	root := t.TempDir()
	for _, flow := range []string{".", "shop", "shop/leaf"} {
		path := filepath.Join(root, flow)
		if err := os.MkdirAll(path, 0700); err != nil {
			t.Fatal(err)
		}
		body := "name: shop\ningress:\n  providers: [{provider: partner}]\n"
		if err := os.WriteFile(filepath.Join(path, "schema.yaml"), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	repo := repoRootForContractsTest(t)
	disk, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	retained, err := sourceartifact.DecodeLogical(disk.SourceArtifact.LogicalBlob())
	if err != nil {
		t.Fatal(err)
	}
	rebuilt, err := LoadWorkflowContractBundleFromArtifact(repo, retained, DefaultPlatformSpecFile(repo), WorkflowContractLoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, bundle := range []*WorkflowContractBundle{disk, rebuilt} {
		for flow, want := range map[string]string{".": "shop", "shop": "shop.shop", "shop/leaf": "shop.shop.leaf"} {
			got, present := bundle.FlowIngressAlias(flow)
			if !present || got != want {
				t.Fatalf("flow %q alias=%q present=%t, want %q", flow, got, present, want)
			}
			fact, present := bundle.EffectiveProvenance().Lookup(`schemas["` + flow + `"].ingress.alias`)
			if !present || fact.Origin != EffectiveValueOriginDerived || fact.RuleID != "flow.rooted_ingress_alias" || len(fact.InputPaths) != 2 {
				t.Fatalf("derived alias provenance = %+v present=%t", fact, present)
			}
		}
	}
	if !reflect.DeepEqual(disk.EffectiveProvenance().Entries(), rebuilt.EffectiveProvenance().Entries()) {
		t.Fatal("retained source changed alias provenance")
	}
	disk.RootSchema.Name = "changed"
	if got, _ := disk.FlowIngressAlias("shop"); got != "shop.shop" {
		t.Fatal("raw schema mutation changed the compiled alias")
	}
	if err := CompileWorkflowSemantics(disk); err != nil {
		t.Fatal(err)
	}
	if got, _ := disk.FlowIngressAlias("shop"); got != "changed.shop" {
		t.Fatalf("explicit recompilation did not consume the new root declaration: %q", got)
	}
}

func TestA9AliasPresenceAdmission(t *testing.T) {
	for _, alias := range []string{"null", "''", "false", "{}", "[]", "' chat'", "'chat '", "chat/support", "chat%2Fsupport", ".chat", "-chat", "'caf\u00e9'"} {
		t.Run(alias, func(t *testing.T) {
			_, err := loadSchemaFragment(t, "name: shop\ningress:\n  alias: "+alias+"\n  providers: [{provider: partner}]\n")
			if err == nil {
				t.Fatal("invalid explicit alias became a default")
			}
		})
	}
	for _, name := range []string{"", "shop.v2", "' shop'", "shop/branch"} {
		t.Run("default/"+name, func(t *testing.T) {
			_, err := loadSchemaFragment(t, "name: "+name+"\ningress:\n  providers: [{provider: partner}]\n")
			if err == nil {
				t.Fatal("invalid root name manufactured a default alias")
			}
		})
	}
	if _, err := loadSchemaFragment(t, "ingress:\n  providers: [{provider: partner}]\n"); err == nil || !strings.Contains(err.Error(), "root's flow name") {
		t.Fatalf("missing root name manufactured a default alias: %v", err)
	}
	if explicit, err := loadSchemaFragment(t, "ingress:\n  alias: independent\n  providers: [{provider: partner}]\n"); err != nil {
		t.Fatal(err)
	} else if alias, _ := explicit.FlowIngressAlias("."); alias != "independent" {
		t.Fatalf("explicit alias requires an unrelated root name: %q", alias)
	}
	bundle, err := loadSchemaFragment(t, "name: shop.v2\ningress:\n  alias: independent\n  providers: [{provider: partner}]\n")
	if err != nil {
		t.Fatal(err)
	}
	if alias, _ := bundle.FlowIngressAlias("."); alias != "independent" {
		t.Fatalf("override changed: %q", alias)
	}
	for _, row := range bundle.EffectiveProvenance().Entries() {
		if strings.HasSuffix(row.Path, ".ingress.alias") && row.Provenance.Origin != EffectiveValueOriginAuthored {
			t.Fatalf("override lost authored provenance: %+v", row)
		}
	}
}
