package packartifact

import (
	"strings"
	"testing"
)

func TestPackSelectionReceiptBindsBaseAndEffectiveInventory(t *testing.T) {
	base, err := LoadPlatformPackInventoryFS(
		testInventoryFS(t, "provider.demo", TypeTrigger, "provider-triggers/demo", ProvenancePlatform, []byte("provider: demo\n")),
		InventoryManifestFileName,
		testPlatformVersion,
		SelectionEmbedded,
	)
	if err != nil {
		t.Fatal(err)
	}
	effective, err := NewEffectivePackInventory(base, nil)
	if err != nil {
		t.Fatal(err)
	}
	body, err := effective.SelectionReceiptBody()
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := ParsePackSelectionReceipt(body)
	if err != nil {
		t.Fatal(err)
	}
	if !receipt.Matches(base, effective) {
		t.Fatalf("receipt %#v does not match its exact base/effective inventory", receipt)
	}

	tampered := strings.Replace(string(body), effective.Digest(), "sha256:"+strings.Repeat("0", 64), 1)
	tamperedReceipt, err := ParsePackSelectionReceipt([]byte(tampered))
	if err != nil {
		t.Fatal(err)
	}
	if tamperedReceipt.Matches(base, effective) {
		t.Fatal("receipt with a competing effective digest matched")
	}

	missingEffective := strings.Replace(string(body), "effective_digest: "+effective.Digest()+"\n", "", 1)
	if _, err := ParsePackSelectionReceipt([]byte(missingEffective)); err == nil || !strings.Contains(err.Error(), "effective_digest is invalid") {
		t.Fatalf("missing effective digest error = %v", err)
	}
}

func TestPlatformPackBaseGenerationOwnerCommitsExactSameProcessGenerations(t *testing.T) {
	first, err := LoadPlatformPackInventoryFS(
		testInventoryFS(t, "provider.demo", TypeTrigger, "provider-triggers/demo", ProvenancePlatform, []byte("provider: demo\nrevision: one\n")),
		InventoryManifestFileName,
		testPlatformVersion,
		SelectionEmbedded,
	)
	if err != nil {
		t.Fatal(err)
	}
	second, err := LoadPlatformPackInventoryFS(
		testInventoryFS(t, "provider.demo", TypeTrigger, "provider-triggers/demo", ProvenancePlatform, []byte("provider: demo\nrevision: two\n")),
		InventoryManifestFileName,
		testPlatformVersion,
		SelectionDevelopmentOverride,
	)
	if err != nil {
		t.Fatal(err)
	}
	firstEffective, err := NewEffectivePackInventory(first, nil)
	if err != nil {
		t.Fatal(err)
	}
	secondEffective, err := NewEffectivePackInventory(second, nil)
	if err != nil {
		t.Fatal(err)
	}

	owner, err := NewPlatformPackBaseGenerationOwner(first)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := owner.PrepareSelection(second)
	if err != nil {
		t.Fatal(err)
	}
	current, err := owner.CurrentPlatformPackBase()
	if err != nil || current.Digest() != first.Digest() {
		t.Fatalf("prepared selection changed current base: current=%v err=%v", current, err)
	}
	if _, err := owner.ResolvePlatformPackBase(secondEffective.SelectionReceipt()); err == nil || !strings.Contains(err.Error(), "not retained by this process") {
		t.Fatalf("uncommitted generation resolution error = %v", err)
	}

	prepared.Commit()
	current, err = owner.CurrentPlatformPackBase()
	if err != nil || current.Digest() != second.Digest() {
		t.Fatalf("committed current base = %v err=%v, want %s", current, err, second.Digest())
	}
	resolvedFirst, err := owner.ResolvePlatformPackBase(firstEffective.SelectionReceipt())
	if err != nil || resolvedFirst.Digest() != first.Digest() {
		t.Fatalf("predecessor generation = %v err=%v", resolvedFirst, err)
	}
	resolvedSecond, err := owner.ResolvePlatformPackBase(secondEffective.SelectionReceipt())
	if err != nil || resolvedSecond.Digest() != second.Digest() {
		t.Fatalf("successor generation = %v err=%v", resolvedSecond, err)
	}
}
