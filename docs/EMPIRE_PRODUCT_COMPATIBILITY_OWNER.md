# Empire Product Compatibility Owner

## Purpose

The Empire Product Compatibility Owner is responsible for ensuring that the Empire product works correctly on the generic Swarm platform, without allowing Empire-specific semantics, assumptions, or implementation pressure to contaminate the generic runtime.

This role exists to protect both goals at the same time:
- Empire must run successfully on Swarm.
- Swarm must remain generic, spec-governed, and product-neutral.

## Mandate

The Empire Product Compatibility Owner shall:
- ensure Empire compatibility with the generic Swarm platform
- detect and track compatibility failures
- translate Empire failures into generic, spec-grounded platform work
- prevent Empire from becoming a de facto semantic owner of Swarm

The role does not own generic platform semantics. The role owns compatibility verification, failure classification, and contamination control at the Empire/Swarm boundary.

## Core Responsibilities

### 1. Compatibility Verification

- Run Empire compatibility checks at the appropriate level:
  - targeted verify
  - targeted smoke checks
  - full product runs when justified
- Maintain confidence that Empire continues to work on current Swarm `master`
- Decide when a platform change requires Empire-shaped validation

### 2. Failure Intake and Classification

- Review Empire failures and classify each one as one of:
  - platform bug
  - spec gap or contradiction
  - Empire contract bug
  - missing generic platform capability
  - Empire-specific request
- Ensure each failure is classified before work is assigned

### 3. Sanitized Platform Issue Creation

- Convert valid Empire-exposed platform problems into Swarm issues
- Ensure each issue is:
  - generic in wording
  - linked to exact governing spec sections
  - scoped to a canonical owner seam
  - free of unnecessary product-specific detail
- Avoid passing raw product context to engineers unless strictly necessary

### 4. Contamination Control

- Reject product-shaped reasoning in generic platform work
- Prevent “Empire needs this” from being accepted as sufficient justification for generic runtime changes
- Require every generic fix to be defendable in:
  - platform-spec terms
  - generic runtime terms
  - generic test/conformance terms

### 5. Compatibility Signoff

- Provide compatibility signoff for high-risk platform changes that touch:
  - loader / contract tree
  - boot verification
  - runtime execution semantics
  - sessions / scope
  - delivery / retry / dead-letter semantics
  - diagnostics / operator surfaces
- State clearly whether Empire verification is:
  - not needed
  - needed as targeted verify/smoke
  - needed as full run

## Authority

The Empire Product Compatibility Owner may:
- require Empire verification for sensitive platform changes
- reject issue framing that is product-shaped rather than generic/spec-grounded
- require a compatibility bug to be captured before it is forgotten
- require escalation when an Empire failure reveals a spec gap or contradiction

The role may not:
- invent new platform semantics from product pressure
- bypass the platform spec
- approve Empire-specific logic in generic runtime without explicit architectural approval
- treat Empire behavior as authoritative when it conflicts with the spec

## Operating Principles

- Empire is the proving ground, not the semantic owner.
- The spec is authoritative; Empire is evidence, not truth.
- Generic fixes must be expressed generically.
- If a fix cannot be described without product-specific language, it likely does not belong in generic runtime.
- Compatibility findings must not remain chat-only; they must be captured in tracked work.

## Required Outputs

The Empire Product Compatibility Owner is expected to produce:
- sanitized compatibility bug reports
- spec-linked platform issues
- compatibility validation decisions for high-risk changes
- explicit classification of Empire failures
- periodic compatibility status reports when needed

## Operating Playbook

### Verification Ladder

- Choose the cheapest check that can falsify the risky assumption.
- Current verification entrypoints:
  - targeted contract verify:
    - `go run ./cmd/swarm verify --contracts /Users/youmew/swarm/empire/contracts --platform-spec docs/specs/swarm-platform/platform/contracts/platform-spec.yaml`
  - full Empire run:
    - `swarm --contract .`
- Do not default to the full run when targeted verify is sufficient to catch the class of failure.

### Required Per-Failure Record

For every worthwhile Empire-exposed failure, record:
- classification:
  - platform bug
  - spec gap or contradiction
  - Empire contract bug
  - missing generic platform capability
  - Empire-specific request
- exact spec references
- generic seam / canonical owner
- issue action:
  - new issue created
  - existing issue updated
  - already captured by issue `#...`
  - Empire-only contract bug, not Swarm issue
- cheapest validation check that should catch this class next time

### Issue And Watchlist Policy

- If the failure is a generic platform problem and not already tracked:
  - create a sanitized Swarm issue
- If the failure is already covered:
  - cite the covering issue explicitly
- If the failure reveals a recurring class of semantic break:
  - update [`docs/watchlists/semantic-correctness.md`](/Users/youmew/dev/swarm/docs/watchlists/semantic-correctness.md)
- Do not maintain duplicate backlog state in docs once the GitHub issue exists.

### Test Improvement Rule

- Every full-run compatibility failure should be evaluated as a testing gap until proven otherwise.
- Default follow-through:
  - add or tighten the cheapest deterministic check that would have caught it
  - prefer `bootverify`, conformance, fixture-based runtime tests, and mock/CLI-test backends before live-LLM runs
- Full Empire runs should validate residual uncertainty, not serve as the first detector for routine verifier/runtime bugs.

### Signoff Format

For high-risk platform changes, compatibility signoff should state:
- whether Empire validation is:
  - not needed
  - needed as targeted verify/smoke
  - needed as full run
- why that level is the cheapest sufficient falsifier
- which generic seam is at risk
- which existing issue/watchlist class is relevant, if any

## Success Criteria

The role is successful when:
- Empire runs reliably on Swarm
- Empire failures are translated into clean generic platform work
- generic runtime remains free of Empire-specific semantics
- spec drift is caught early
- engineers can fix platform issues without broad product exposure

## Role Boundary Statement

The Empire Product Compatibility Owner ensures Empire works on Swarm by translating product failures into generic, spec-governed platform work while actively preventing product-specific contamination of the generic codebase.
