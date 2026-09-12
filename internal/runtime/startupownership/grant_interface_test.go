package startupownership

import (
	"context"
	"reflect"
	"testing"
)

func TestOnlyLiveGrantExposesSourceSetPlan(t *testing.T) {
	common := reflect.TypeFor[GenerationGrant]()
	if _, present := common.MethodByName("SourceSetPlan"); present {
		t.Fatal("common execution grant exposes live source-set authority")
	}
	if _, present := reflect.TypeFor[*generationGrant]().MethodByName("SourceSetPlan"); present {
		t.Fatal("shared grant implementation exposes live source-set authority")
	}
	if _, present := reflect.TypeFor[LiveGenerationGrant]().MethodByName("SourceSetPlan"); !present {
		t.Fatal("normal runtime grant lost strict live source-set authority")
	}
	capability, _, plan := testCapability(t)
	process, err := capability.Evidence()
	if err != nil {
		t.Fatal(err)
	}
	grant, err := capability.IssueGenerationGrant(context.Background(), GrantRequest{
		BundleHash: startupBundleHashA, RuntimeInstanceID: process.RuntimeInstanceID,
		RuntimeGeneration: 1, SourceSetRevision: plan.Revision,
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := grant.SourceSetPlan(context.Background())
	if err != nil || got.Revision != plan.Revision {
		t.Fatalf("live source-set proof = %v/%v", got, err)
	}
}
