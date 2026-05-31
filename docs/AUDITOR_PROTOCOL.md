# Auditor Protocol

This document defines the long-lived audit workflow for the repository.

Use it for dedicated audit issues such as:

- architecture
- spec compliance
- persistence / recovery / migration
- conformance / invariants
- construction / dependency wiring
- operational side effects / verified completion
- maintenance
- read-model / observability
- type quality / string abuse / loose schema
- API sharpness / interface ambiguity / error model

The goal is to make audits repeatable, comparable over time, and useful for regression tracking.

## Core Model

- Audit issues are long-lived and remain open across multiple audit runs.
- Each audit run produces a report as an issue comment.
- The comment is the canonical artifact for that run.
- Findings should be mapped to one of:
  - already covered
  - new issue needed
  - watchpoint
  - fixed since last run
- Auditors do not silently rewrite the queue. They report and classify.

## Auditor Rules

- Do not implement fixes during the audit.
- Do not close the audit issue after one pass.
- Prefer concrete code references over general impressions.
- Focus on real semantic, architectural, or maintenance risk.
- Ignore minor style nits unless they are symptoms of a larger concern.
- Treat any listed "suggested starting points" as bootstrap anchors, not scope limits.
- Follow the concern across the codebase wherever the evidence leads.
- Treat authoritative sources as the source of truth:
  - runtime behavior in code
  - authoritative specs
  - current persisted/read-model contracts
- If a concern is already covered by an open issue or watchlist item, say so explicitly instead of duplicating it.
- If behavior appears correct but fragile, report it as residual risk rather than forcing a finding.
- Do not stop at the first local seam if the semantic path crosses other modules or boundaries.
- Do not report a purely local finding when the real risk depends on a wider path you have not checked.

## Standard Audit Procedure

1. Read the audit issue title and body carefully.
2. Restate the audit scope in one or two sentences before starting.
3. Inspect the current code on the live branch / current mainline target.
4. Inspect the most relevant docs/specs for that audit concern.
5. Use suggested starting points only to orient; expand outward across the whole concern.
6. Trace the highest-risk seams end-to-end across relevant boundaries.
7. Run focused tests where they materially validate the audit concern.
8. Record what you actually checked, what you did not check, and why.
9. Produce a report comment on the audit issue.
10. Include a final score from `0/100`.

When relevant, auditors should try to trace the full path across boundaries such as:

- spec / contract
- boot / validation
- runtime
- store / persistence
- recovery / replay
- read-model / operator surface

Anti-pattern to avoid:

- treating the "suggested starting points" list as the audit perimeter
- reporting findings that only prove a local inconsistency without checking the larger semantic path

## Standard Coverage Checklist

Every audit run should explicitly consider whether the concern crosses these boundaries:

- docs / spec / contract
- boot / validation
- runtime execution
- store / persistence
- recovery / replay
- API / read-model / operator surface
- tests / conformance coverage
- transport / config / environment, when relevant

Auditors do not need to force all of these into every finding.
They do need to disclose which ones they actually checked for the run.

## Required Report Structure

Every audit run comment should use this structure:

```text
Audit Run: <auditor_name>
Date: <YYYY-MM-DD>
Scope: <short scope summary>

Coverage

- Checked:
  - <boundary or subsystem 1>
  - <boundary or subsystem 2>
- Not Checked:
  - <boundary or subsystem not checked> — <why not checked>
- Known non-goals for this run:
  - <optional explicit non-goal>

Findings

1. <severity>: <finding title>
   - Evidence:
   - Impact:
   - Recommendation:
   - Classification: already covered | new issue needed | watchpoint | fixed since last run

2. ...

Residual Risk

- <residual risk 1>
- <residual risk 2>

Score

<N>/100
```

## Scoring Guidance

The final score is a coarse health score for the audited concern at the time of the run.

- `90-100`
  - strong shape
  - no major architectural or semantic gaps in the audited concern
  - only residual risks / minor follow-ups

- `75-89`
  - generally healthy
  - some meaningful gaps or drift, but not systemic failure

- `60-74`
  - mixed
  - multiple significant concerns or coverage gaps

- `40-59`
  - weak
  - architecture or compliance problems are still materially affecting confidence

- `0-39`
  - poor
  - the audited concern is still structurally unreliable

Score conservatively. It is better for the score to be slightly harsh than falsely reassuring.

## Finding Severity

Use these severities:

- `high`
  - correctness, semantic-owner, persistence, recovery, contract, or operational trust failure

- `medium`
  - meaningful architectural drift, dangerous ambiguity, incomplete coverage of a critical seam

- `low`
  - worthwhile cleanup, fragility, or limited residual smell that is not currently blocking correctness

## Classification Guidance

Use exactly one classification per finding:

- `already covered`
  - the issue/watchlist already tracks it well enough

- `new issue needed`
  - a concrete new implementation issue should be opened

- `watchpoint`
  - important to keep visible, but not yet ready or important enough for a new issue

- `fixed since last run`
  - this concern existed before and is now materially resolved

## Generic Auditor System Prompt

Use this as the generic auditor prompt template.
Replace the placeholders before use.

```text
You are a dedicated auditor for this repository.

Your job is not to implement fixes. Your job is to inspect the codebase for the concern tracked by audit issue #{ISSUE_NUMBER} and produce a disciplined audit report as an issue comment.

Audit issue:
- #{ISSUE_NUMBER}
- {ISSUE_TITLE}

Audit concern:
- {AUDIT_SCOPE_SUMMARY}

Required behavior:
- inspect the current code and the most relevant docs/specs for this concern
- use any suggested starting points only as bootstrap anchors, not as scope boundaries
- follow the concern across the codebase wherever the evidence leads
- trace cross-boundary paths when the real risk depends on more than one seam
- focus on real architectural, semantic, maintenance, or compliance risks
- ignore minor style nits unless they indicate a larger problem
- do not close the audit issue
- do not implement fixes
- classify each finding as:
  - already covered
  - new issue needed
  - watchpoint
  - fixed since last run

Output target:
- produce your result as a comment on issue #{ISSUE_NUMBER}

Your comment must use this exact structure:

Audit Run: {AUDITOR_NAME}
Date: {CURRENT_DATE}
Scope: {SHORT_SCOPE_SUMMARY}

Coverage

- Checked:
  - <boundary or subsystem 1>
  - <boundary or subsystem 2>
- Not Checked:
  - <boundary or subsystem not checked> — <why not checked>
- Known non-goals for this run:
  - <optional explicit non-goal>

Findings

1. <severity>: <finding title>
   - Evidence:
   - Impact:
   - Recommendation:
   - Classification: already covered | new issue needed | watchpoint | fixed since last run

2. ...

Residual Risk

- <residual risk 1>
- <residual risk 2>

Score

<N>/100

Scoring rules:
- 90-100 = strong shape, only residual risk
- 75-89 = generally healthy, some meaningful gaps
- 60-74 = mixed, multiple significant concerns
- 40-59 = weak, material confidence problems
- 0-39 = structurally unreliable

Prefer concrete file references and defensible reasoning.
If a concern is already tracked elsewhere, say so explicitly instead of duplicating it.
Make coverage explicit so the report shows what was actually audited and what remains uninspected.
```

## Minimal Reusable Invocation Prompt

If you want a shorter reusable auditor launch prompt, use this:

```text
Audit issue #{ISSUE_NUMBER}: {ISSUE_TITLE}

Run a dedicated audit for this concern.
Do not implement fixes.
Inspect the current code and the most relevant docs/specs.
Treat any suggested starting points as bootstrap anchors, not scope boundaries.
Explicitly disclose what boundaries you checked and what you did not check.
Post the result as an issue comment using the required Auditor Protocol format from:
`docs/AUDITOR_PROTOCOL.md`

Auditor name: {AUDITOR_NAME}
Date: {CURRENT_DATE}
```

## Notes

- Re-run audits periodically. These are not one-time activities.
- Multiple auditors may comment on the same audit issue in different runs.
- Different auditors can legitimately disagree on severity or score. That is useful signal, not a problem.
- Audit issues are part of the regression-control system and should be retained for future passes.
