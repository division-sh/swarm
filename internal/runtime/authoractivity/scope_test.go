package authoractivity

import (
	"context"
	"strings"
	"testing"
)

func TestBundleScopeForSource(t *testing.T) {
	runtimeID := "11111111-1111-1111-1111-111111111111"
	bundleA := "bundle-v1:sha256:" + strings.Repeat("a", 64)
	bundleB := "bundle-v1:sha256:" + strings.Repeat("b", 64)
	tests := []struct {
		name       string
		ctx        context.Context
		bundleHash string
		want       Scope
		wantErr    string
	}{
		{name: "promotes source-owned bundle", ctx: WithScope(context.Background(), RuntimeScope(runtimeID)), bundleHash: bundleA, want: BundleScope(runtimeID, bundleA)},
		{name: "retains matching exact bundle", ctx: WithScope(context.Background(), BundleScope(runtimeID, bundleA)), bundleHash: bundleA, want: BundleScope(runtimeID, bundleA)},
		{name: "retains exact bundle without duplicate source fact", ctx: WithScope(context.Background(), BundleScope(runtimeID, bundleA)), want: BundleScope(runtimeID, bundleA)},
		{name: "rejects conflicting source fact", ctx: WithScope(context.Background(), BundleScope(runtimeID, bundleA)), bundleHash: bundleB, wantErr: "conflicts"},
		{name: "rejects missing source fact from runtime scope", ctx: WithScope(context.Background(), RuntimeScope(runtimeID)), wantErr: "source-owned bundle_hash"},
		{name: "rejects global scope", ctx: WithScope(context.Background(), Scope{Kind: ScopeGlobal}), bundleHash: bundleA, wantErr: "cannot derive"},
		{name: "rejects missing scope", ctx: context.Background(), bundleHash: bundleA, wantErr: "exact runtime scope"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := BundleScopeForSource(tt.ctx, tt.bundleHash)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("BundleScopeForSource: %v", err)
			}
			if got != tt.want {
				t.Fatalf("scope = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestBundleScopeForTarget(t *testing.T) {
	runtimeID := "11111111-1111-1111-1111-111111111111"
	bundleA := "bundle-v1:sha256:" + strings.Repeat("a", 64)
	bundleB := "bundle-v1:sha256:" + strings.Repeat("b", 64)
	for _, tt := range []struct {
		name    string
		ctx     context.Context
		target  string
		want    Scope
		wantErr string
	}{
		{name: "promotes runtime target", ctx: WithScope(context.Background(), RuntimeScope(runtimeID)), target: bundleB, want: BundleScope(runtimeID, bundleB)},
		{name: "replaces caller bundle with exact target", ctx: WithScope(context.Background(), BundleScope(runtimeID, bundleA)), target: bundleB, want: BundleScope(runtimeID, bundleB)},
		{name: "rejects missing target", ctx: WithScope(context.Background(), RuntimeScope(runtimeID)), wantErr: "target-owned bundle_hash"},
		{name: "rejects global scope", ctx: WithScope(context.Background(), Scope{Kind: ScopeGlobal}), target: bundleB, wantErr: "cannot derive"},
		{name: "rejects missing scope", ctx: context.Background(), target: bundleB, wantErr: "exact runtime scope"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := BundleScopeForTarget(tt.ctx, tt.target)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("BundleScopeForTarget: %v", err)
			}
			if got != tt.want {
				t.Fatalf("scope = %#v, want %#v", got, tt.want)
			}
		})
	}
}
