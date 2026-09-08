# Issue #2321 Implementation Progress

Agent: agent-g. Status: implementation in progress, not review-ready.

This is a work checkpoint, not a Post-Implementation Proof Audit or a first-slice
closure claim. The approved 29-manifestation class and all T/L/H obligations remain
binding in `issue-2321-preimplementation.md` and `issue-2321-spec-delta.yaml`.

## Authority And Integration

- Independent gate cycle 2 approved:
  https://github.com/division-sh/swarm/issues/2321#issuecomment-5584592517.
- Integrated `origin/master@bdc82dfba`; audit-only commits rebased cleanly on branch
  `agent-g/2321-command-execution` before implementation. The approved historical
  audit remains an audit, not a claim that the old master is the implementation base.
- Preserved current run-scoped actor/source identities, terminal retirement, fresh
  activity lineage and static receiver readiness. No runtime startup, fork, timer,
  topology or persistence authority changes are included in this checkpoint.
- Docs watchlist approval `swarm-docs@efac8cd` is consumed. No new tracker or
  POTENTIAL_ISSUES entry. #2319 remains open and F-owned; #1801/#2284 remain separate.
- Unrelated #2008 WIP in the original Agent G worktree remains untouched.

## Implemented So Far

Structural reader separation in the existing owners:

- `packadmission.FromBundle` and `runtime.AdmitEffectiveSourceProjection` provide
  the admitted source to verify, describe, describe --graph and describe routes.
  No reader calls the credential-observing pack runtime loader.
- `runtime.ValidateWorkflowContractSurface` and `bootverify` receive an explicit
  structural validation purpose. Credential/managed-credential access, MCP
  discovery and concrete availability are not structural observations. Declared
  tools, permissions, workspace classes, fields, references and supplied doubles
  continue through existing validators.
- Model declaration/reference validation remains in `llm/selection`; live
  `ResolveModel` adds concrete selected-provider fulfillment. A configured alias
  without a target for the selected live provider is structurally valid but still
  fails live resolution. Undefined aliases remain invalid.
- Common validation severity options remain shared. Warning escalation is
  preserved, and later structural failures enter the canonical diagnostic report
  without replacing existing warning severity with a fabricated hard finding.
- Structural output explicitly reports `validation_scope: structural` and
  `live_readiness: not_evaluated`. Removed workspace selection output and verify
  operational capability subjects; kept inventory and harness-independence truth.
  Routing output retains routing-topology/v1 and its stable IDs.
- Removed the unused production `VerifyBundle` facade and workspace diagnostic
  wrapper. Execution-specific fixtures call the existing runtime owner through a
  test-only helper; they are not public structural-command proof.
- Promoted matching structural rules into authoritative `platform-spec.yaml`.
  Remaining approved command/composition rules are NOT claimed implemented yet.

## Executed Evidence

Focused commands passed:

```sh
go test ./internal/cliapp -run '^TestRunVerifyCommand_' -count=1
go test ./internal/cliapp -run '^TestStructuralReadersRetainPostBootInvalidity$' -count=1
go test ./internal/cliapp ./internal/runtime/bootverify -run '^(TestStructural|TestDescribeCommandJSONRendersRunScopedTemplateInstanceIdentity)' -count=1
go test ./internal/cliapp ./internal/serveapp -run '^(TestExecutionValidation|TestBuiltinToolParityInvariant_SupportedSurfacesShareRuntimeToolTruth_V2|TestStructuralReadersShareAdmittedSource)' -count=1
go test ./internal/apispec -count=1
```

Additional focused selector/model, effect-reachability, route-equivalence, provider
capability and harness-boundary checks passed during development. Newly introduced
tests pin the complete bootcheck/supplemental-check census, poison credential
interfaces and count MCP requests, preserve invalid workspace declarations, and
verify canonical post-boot errors remain visible across structural readers.

Built the actual `cmd/swarm` binary and executed verify, describe, describe --graph
and describe routes on the unchanged generated webhook scaffold from its invocation
root, with PATH pointing to a nonexistent directory and both credential-file paths
pointing to unreadable-as-documents directories. All four returned exit 0; no false
provider-event/conversation-reference hard invalidities or retired deployment
fields were emitted. Test-quarantined environment variables were unset. This is
structural compiled-command proof only, NOT T, L or H execution credit.

The first broader reader run found two failures: an obsolete expected empty JSON
response, and a real dropped warning-escalation option. Both were repaired; the
targeted regression tests now pass. A subsequent invalid-source test initially
stopped at warning escalation before reaching its intended later strict-schema
failure; its fixture now explicitly selects nonfatal warnings and strict schemas.

The broader reader-package rerun passed at this checkpoint after waiting for the
shared swarm-test admission slot:

```sh
go run ./cmd/swarm-test -- ./internal/cliapp ./internal/runtime/bootverify ./internal/runtime/llm/selection ./internal/apispec -count=1
```

Results: cliapp 43.110s; bootverify 21.417s; selection 0.002s; apispec 2.508s.
Authoritative spec YAML parsing and `git diff --check` also passed. This is not the
whole suite or the required final integrated T/L/H acceptance run.

## Outstanding In The Same Implementation

1. Command-owned live/mock selection, descriptor creation before persistence and
   read-only consumption on restore/fork; remove all mock-presence waivers and
   retired configuration selectors/producers.
2. Injected TestSessionRunner and shared serve/test runtime construction, pre-resource
   test admission, private endpoint/state ownership and joined cleanup; no connected
   test fallback or public retained/mock-serve selector.
3. Recovery default and presence-aware explicit-false behavior; example/archetype
   config removal and mutation/census guards; remaining authoritative spec sections.
4. T public mock test, L genuine live serve/dev/restart, and H internal retained
   mock lifecycle proof migration and execution, including both stores and all
   existing exact identity/event/delivery/card/timer assertions.
5. Full swarm-test run, exhaustive final-head proof audit, final normal PR and review.

Live prerequisite observation: Docker responds, but this host currently has neither
a Claude executable on PATH nor the Swarm workspace image. Only Go and PostgreSQL
images were present. No credential values were read or logged, and no paid live turn
has been attempted. A usable live environment must be established for L proof;
internal mock results cannot substitute. #2319 separately blocks combined
credential-absent webhook acceptance, not the implementation above.

No new semantic contradiction or approved stop condition has been discovered.
