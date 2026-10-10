package main

import (
	"strings"
	"testing"
)

func TestManagerUnitConstructorDoesNotCreateAnImplicitRawDeliveryStore(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-manager-explicit-delivery-configuration")
	cut := "\tif opts.DeliveryStore == nil {\n\t\topts.DeliveryStore = newManagerDeliveryTestStore(t)\n\t}\n"
	want, err := canonicalFunction(strings.Replace(row.Before, cut, "", 1))
	got, afterErr := canonicalFunction(row.After)
	actual, actualErr := canonicalFunction(selectedCausalObservationBody(t, row.File, row.Function))
	if err != nil || afterErr != nil || actualErr != nil || !strings.Contains(row.Before, cut) || want != got || actual != got {
		t.Fatal("manager unit constructor changed beyond removal of implicit SQL persistence")
	}
	for _, preserved := range []string{"stores[0]", "opts.DeliveryStore = deliveryStore", "NewAgentManagerWithOptions(bus, factory, opts, stores...)", "ShutdownWithOptions(ShutdownOptions{Grace: 5 * time.Second})"} {
		mutant, mutantErr := canonicalFunction(strings.Replace(row.After, preserved, "unreviewed", 1))
		if !strings.Contains(row.After, preserved) || mutantErr == nil && mutant == want {
			t.Fatalf("lost explicit owner or joined lifetime accepted: %s", preserved)
		}
	}
	if strings.Contains(row.After, "newManagerDeliveryTestStore") {
		t.Fatal("raw-backed persistence fallback survives")
	}
}
