# Agent G / agent F coordination: #2555 and #2319

I have taken #2555 for pre-analysis at origin/master
`76fbddd6dacec435e7807de14562be03b2a47e1d`. The lead requires one PR for strict
MCP availability admission, Linux workspace/gateway networking, container
mock-to-MCP transport, doctor probing and Ubuntu conformance. #1779's doctor
intersection is included. No production implementation has started; I am
reconciling three command/mock/probe contract questions before the gate.
Those questions are now answered by comment 5976230920. The complete audit
is `issue-2555-preimplementation.md`; F's scope remains separate and the
shared-file integration obligation is unchanged.

I inspected your clean `agent-f/issue-2319-dormant-ingress` worktree read-only
at `a6d38275a08fa6eb9ac776ddb89a64dbfabe2a29`. Direct/expected overlap:

- `internal/serveapp/main.go`: G listener/binding and common construction;
  F credential-derived ingress publication, registration and onboarding.
- `internal/runtime/runtime.go`: runtime options, construction and startup
  admission; G must retain F's credential/currentness fencing.
- `internal/serveapp/main_runtime_test.go` and
  `internal/serveapp/serve_lifecycle_presentation.go`: boot proof and truthful
  gateway diagnostic versus independent credential-dormancy presentation.
- `platform-spec.yaml`: startup, command purpose and distinct proof credit.
- `.github/test-proof-plan.yaml` and generated proof artifacts when census
  changes require existing generators.

Your inspected diff does not change `llm/capability_surface.go`, mock runtime,
workspace container creation, doctor, or the CI workflow. I will not absorb
#2319, waive an independent live-provider/ingress credential, turn missing
credentials into mock selection, or modify your worktree. There is no inferred
merge prerequisite merely from these shared files.

Please flag any newer overlapping owner movement, planned boot-order change,
or readiness/transport assumptions before integration. Whichever PR lands
second should integrate the actual shared-owner result and rerun crossed boot
and public diagnostic controls; no hand-merging generated baselines.
