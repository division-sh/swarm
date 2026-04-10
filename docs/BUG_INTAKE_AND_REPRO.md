# Bug Intake And Repro

## Purpose

Bug reports start from a symptom, not from a confirmed failure class.

The first job is to identify:

- what actually failed
- whether it reproduces on current head
- which exact execution path failed
- which hypotheses are still plausible

Only after that should a bug become implementation-ready.

## Default Rule

A bug report is a symptom-intake issue by default.

Default status:

- `status:needs-scope`

A bug issue should not move to implementation just because the symptom sounds plausible.

## Required Workflow

1. Open the bug as symptom intake.
2. Run a dedicated repro / identification pass.
3. Record the repro artifact on the issue.
4. Decide whether the issue is:
   - a live classified bug
   - a stale / already-fixed report
   - an ambiguous report that needs more repro
   - an audit / broader investigation candidate
   - an environment-specific failure
5. Only then open or gate implementation work.

Direct implementation without a repro artifact is an exception.

That exception is only honest when:

- the exact current-head failure path is already pinned
- the production owner chain is already clear
- the issue is obviously local rather than classification-heavy

## Repro Artifact

Every non-trivial bug needs one explicit repro / identification artifact before implementation.

That artifact should record:

- observed symptom
- exact environment / trigger context
- current-head reproducibility result:
  - reproduced
  - partially reproduced
  - not reproduced
  - reproduced only on a different path
- exact observed path
- exact reproduced path
- whether observed path and reproduced path are the same
- candidate hypotheses
- hypothesis disposition:
  - supported
  - falsified
  - unresolved
- working classification
- confidence level
- next action:
  - implementation issue
  - broader audit
  - stale closure
  - more repro needed

## Repro Protocol

The repro stream should move from highest-fidelity plausible path to lower-fidelity fallback only when necessary.

Required order:

1. identify the best current candidate for the observed live path
2. attempt repro on that path first
3. if exact-path repro is not yet possible, step down one level and record the downgrade explicitly
4. stop lowering fidelity once path identity becomes too weak to support closure claims

Recommended fidelity ladder:

- `L1` live supported entry surface
  - real endpoint / helper / operator flow / runtime mode that the report most likely used
- `L2` integration path with real runtime wiring
  - same owner chain with real routing, but inside a focused harness
- `L3` in-process owner-path harness
  - direct exercise of the likely canonical owner chain
- `L4` narrow unit seam
  - local helper / synthetic seam only

Hard rule:

- lower-fidelity repro can falsify a closure story or narrow the hypothesis set
- lower-fidelity repro cannot, by itself, close a higher-fidelity bug report unless path equivalence is made explicit and defensible

Every repro pass should state:

- target path
- actual exercised path
- fidelity level
- result
- next best repro step if still unresolved

## Path Identity Rule

For bug work, three paths matter:

- observed path
- reproduced path
- closure path

They must be compared explicitly.

If they are not the same, implementation cannot silently proceed as if they were.

## Allowed Outcomes

### 1. Live Classified Bug

Use when:

- the bug reproduces on current head
- the execution path is identified
- the working failure class is credible

Action:

- move to implementation gate
- switch issue to `status:ready` when appropriate

### 2. Stale / Already Fixed

Use when:

- the reported symptom does not reproduce on current head
- there is credible evidence the underlying behavior already changed elsewhere

Action:

- repair the issue record explicitly
- do not pretend a proof-only PR is an implementation fix

### 3. Ambiguous / More Repro Needed

Use when:

- the symptom is real enough to care about
- but current-head path identity or class identity is not yet clear

Action:

- keep the issue out of implementation
- continue repro / classification work

### 4. Broader Audit / Investigation

Use when:

- multiple plausible classes remain live
- or the symptom points at a broader ownership problem rather than one actionable child

Action:

- open or move to an audit / investigation issue

## Hard Rules

### No Implementation Gate Without Classification

Do not gate a bug issue for implementation until the repro artifact exists.

### No Proof-Only Closure For Live Bug Issues

A test-only or proof-only PR may not close a bug issue unless the issue was explicitly repaired first to say:

- the bug is stale
- the bug was already fixed elsewhere
- or the issue was misclassified as a live bug

If production behavior does not change, the PR is not an implementation fix.

### Stop If Current-Head Non-Repro Is Discovered Mid-Implementation

If implementation work discovers that the bug does not reproduce on current head:

- stop coding
- repair the issue and gate record
- decide whether the issue is stale, misclassified, or on the wrong path

Do not keep the old issue story and force closure through a fake fix PR.

## Review Expectations

Reviewers should explicitly ask:

- is this still a live bug on current head?
- what exact path was observed?
- what exact path was reproved?
- is the PR changing production behavior or only adding proof?

If the PR only adds proof, reviewers must require issue-state repair before accepting closure.

## Label Guidance

Use the existing label model:

- `status:needs-scope` for bug intake / repro-needed issues
- `status:ready` only after repro / classification is complete
- `scope:split` when symptom intake obviously hides multiple possible classes

## Minimal Repro Template

```text
Bug Repro / Identification

Issue:
- #<NNN>

Observed symptom:
- <what was reported>

Current-head reproducibility:
- <reproduced | partially reproduced | not reproduced | reproduced on different path>

Observed path:
- <reported path>

Reproduced path:
- <actual exercised path>

Path identity:
- <same | different | still unclear>

Candidate hypotheses:
- H1: <...>
- H2: <...>
- H3: <...>

Hypothesis disposition:
- H1: <supported | falsified | unresolved>
- H2: <supported | falsified | unresolved>
- H3: <supported | falsified | unresolved>

Working classification:
- <...>

Confidence:
- <high | medium | low>

Next action:
- <implementation issue | broader audit | stale closure | more repro needed>
```
