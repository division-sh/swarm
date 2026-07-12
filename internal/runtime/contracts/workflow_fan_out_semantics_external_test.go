package contracts_test

import (
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepaths "github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/notifyallchildren"
)

func TestFanOutEffectiveSemantics_DerivesScalarIdentityAndBoundOnce(t *testing.T) {
	bundle := notifyallchildren.LoadBundle(t, notifyallchildren.Options{})
	handler, ok := bundle.NodeEventHandler("portfolio-coordinator", notifyallchildren.OwnerTriggerEvent)
	if !ok || handler.FanOut == nil {
		t.Fatal("canonical notify handler fan_out missing")
	}

	effective, err := bundle.ResolveFanOutEffectiveSemantics(notifyallchildren.OwnerFlowID, notifyallchildren.OwnerTriggerEvent, *handler.FanOut)
	if err != nil {
		t.Fatalf("ResolveFanOutEffectiveSemantics: %v", err)
	}
	if effective.ItemType.Kind != runtimecontracts.CatalogTypeText || effective.Identity != "account_id" || !effective.IdentityDerived {
		t.Fatalf("effective scalar identity = kind:%s identity:%q derived:%v, want text/account_id/true", effective.ItemType.Kind, effective.Identity, effective.IdentityDerived)
	}
	if effective.AuthoredMaxItems != 100 || effective.MaxItems != 100 || !effective.MaxItemsSet {
		t.Fatalf("effective bound = authored:%d effective:%d set:%v, want 100/100/true", effective.AuthoredMaxItems, effective.MaxItems, effective.MaxItemsSet)
	}

	explicit := *handler.FanOut
	explicit.Identity = "account_id"
	effective, err = bundle.ResolveFanOutEffectiveSemantics(notifyallchildren.OwnerFlowID, notifyallchildren.OwnerTriggerEvent, explicit)
	if err != nil {
		t.Fatalf("ResolveFanOutEffectiveSemantics explicit identity: %v", err)
	}
	if effective.Identity != "account_id" || effective.IdentityDerived {
		t.Fatalf("explicit identity = %q derived:%v, want account_id/false", effective.Identity, effective.IdentityDerived)
	}
}

func TestFanOutEffectiveSemantics_RequiresIdentityForObjectItems(t *testing.T) {
	_, err := notifyallchildren.LoadBundleResult(t, notifyallchildren.Options{ObjectMembership: true})
	if err == nil || !strings.Contains(err.Error(), "fan_out.identity is required") {
		t.Fatalf("bundle load error = %v, want object identity requirement", err)
	}
}

func TestFanOutEffectiveSemantics_RejectsUndeclaredPayloadSourceWithExplicitIdentity(t *testing.T) {
	_, err := notifyallchildren.LoadBundleResult(t, notifyallchildren.Options{UndeclaredPayloadMembership: true})
	if err == nil || !strings.Contains(err.Error(), "references undeclared payload field account_ids") {
		t.Fatalf("bundle load error = %v, want undeclared payload collection rejection", err)
	}
}

func TestFanOutEffectiveSemantics_RejectsMissingEventContractWithExplicitIdentity(t *testing.T) {
	bundle := notifyallchildren.LoadBundle(t, notifyallchildren.Options{})
	handler, ok := bundle.NodeEventHandler("portfolio-coordinator", notifyallchildren.OwnerTriggerEvent)
	if !ok || handler.FanOut == nil {
		t.Fatal("canonical notify handler fan_out missing")
	}
	spec := *handler.FanOut
	spec.ItemsFrom = "payload.account_ids"
	spec.ItemsPath = runtimepaths.Parse(spec.ItemsFrom)
	spec.Identity = "account_id"

	_, err := bundle.ResolveFanOutEffectiveSemantics(notifyallchildren.OwnerFlowID, "portfolio.undeclared", spec)
	if err == nil || !strings.Contains(err.Error(), "references undeclared payload field account_ids") || !strings.Contains(err.Error(), "portfolio.undeclared") {
		t.Fatalf("ResolveFanOutEffectiveSemantics error = %v, want missing event-contract rejection", err)
	}
}
