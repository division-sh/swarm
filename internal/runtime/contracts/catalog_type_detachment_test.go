package contracts

import (
	"reflect"
	"testing"
)

func TestCatalogTypeDetachmentFlowOverrideEnum(t *testing.T) {
	root := t.TempDir()
	for name, body := range map[string]string{
		"manifest.yaml":       "version: 1.0.0\nplatform_version: '*'\n",
		"child/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
		"schema.yaml":         "stages: {done: {final: true}}\n",
		"types.yaml":          "Choice: ['alpha', 'beta']\n",
		"child/schema.yaml":   "stages: {done: {final: true}}\n",
		"child/types.yaml":    "Choice: ['child', 'other']\n",
	} {
		writeFixtureFile(t, root+"/"+name, body)
	}
	repo := repoRootForContractsTest(t)
	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	view := bundle.ResolvedTypeCatalogForFlow("child")
	view.Enums["Choice"].Values[0] = "changed"
	if got := bundle.ResolvedTypeCatalogForFlow("child").Enums["Choice"].Values; !reflect.DeepEqual(got, []string{"child", "other"}) {
		t.Fatalf("incoming flow enum aliased retained declaration: %v", got)
	}
	if got := bundle.RootTypeCatalog().Enums["Choice"].Values; !reflect.DeepEqual(got, []string{"alpha", "beta"}) {
		t.Fatalf("child override changed root enum: %v", got)
	}
}
