# DIAGNOSTIC ONLY: original onboarding retry cause

Disposable branch adaptation for reviewer-g's [6251490013 gate](https://github.com/division-sh/swarm/pull/2614#issuecomment-6251490013).
The landing PR remains frozen at `f2e0d031a06d3f8e5a6ec487c8f90c3e212860d8`.
Do not merge this branch. No dispatch is authorized by committing this packet.

## Proposed route

After explicit route/head approval, dispatch the existing `ci.yml` workflow on
`agent-g/diagnostic-2614-original-cause`, passing its full signed commit as
`expected_sha`. Only workflow_dispatch is enabled. The route refuses any other
branch, SHA, event, base ancestry or run attempt; attempt 2 is refused, not a retry.
Both jobs have read-only contents/actions permissions. It has no Required-summary,
timing-budget, preflight, selection-status or trusted-master publisher. The names
and artifacts are DIAGNOSTIC ONLY, never qualifying evidence for #2614.

The setup job retains ubuntu-latest, setup-go from go.mod, production compilation,
the full canonical owner-generated plan and compile-only release products (10m).
The worker retains ubuntu-latest and the canonical 18m timeout. It runs the original
`run-proof-batch.sh` exactly once with BATCH_ID=serveapp-runtime. The owner-derived
frozen selection digest covers the entire unit and batch: all selected roots,
required children, deferrals, packages, backend/environment, count, run/skip, Go
timeout and batch membership. The canonical batch has no extra race flag; neither
this route nor the capture adds one. The original runner still owns PostgreSQL
provisioning, the slot and cleanup. No load, sleeps, deadline or retry changes.
The full plan is retained, not rewritten to pretend this isolated unit is FULL.
Diagnostic caches have separate publication keys; ordinary caches are read-only.

## Evidence and privacy

Artifacts are attempt-qualified: diagnostic-2614-plan-RUN-ATTEMPT,
diagnostic-2614-products-RUN-ATTEMPT and
diagnostic-2614-RUN-ATTEMPT-serveapp-runtime. Evidence uploads run on failure too.
Missing identity, receipt, terminal population, wrapper exit or environment is
INCOMPLETE. Setup failure cannot claim an observed worker result.

The evidence artifact retains full plan/matrix, source/run/attempt/tree identity,
untouched typed canonical command evidence, every terminal test/package record,
capture validation/provenance, allowlisted Go/runner/Docker metadata, timestamps,
wrapper exit and cleanup-error flags, redacted original-cause trace and hashes.
The seven capture files are byte-identical to the locally validated overlay.
Raw stdout/stderr/test Output stays private on the ephemeral runner and is NOT
uploaded: these other roots may print pairing/session content. Only its hashes,
terminal records and the bounded type/sentinel/class/SQLSTATE trace leave it.
The target leaf trace includes ceremony, stage and request ordinal; other errors
without that correlation stay explicitly not_observed, never credited to rebind.
No error prose, QR/session material or arbitrary error values are serialized.

The wrapper result is not a cleanup-success oracle: it records only return and
whether the original runner reported a cleanup error. A timeout/missing return
is unobserved. A green diagnostic is negative attribution, not a fix or merge gate.

## Cost / observability

Prior hosted setup was 5m05s and this batch 7m44s. Estimate 13-16 Linux runner
minutes total, bounded by the existing 10m + 18m job limits. One worker, no matrix,
no new parallelism or GitHub capacity. No other local runtime reproduction is due.

Permanent logging is not part of this diagnostic/landing tree proposal. After the
observation, the same bounded capture may be reviewed at the method's fallback
boundary as an observability-only delta, with public behavior and joined errors
unchanged. No new cause code or retryable classification is proposed.

## Failure-site capture continuation

[6252394166](https://github.com/division-sh/swarm/pull/2614#issuecomment-6252394166)
authorizes offline preparation of this continuation, not its dispatch. The first
observation was negative for the PostgreSQL rebind and red in the SQLite
PublicLogout root; it did not establish which assertion/cause recurred.

The original seven-file Retry capture stays unchanged. Test-only additions give
the explicit-logout journey finite phase markers in its existing failure cleanup,
plus fixed labels at failing pairing/restart/logout checks. Shared Inbox checks
cover paging/notices, labels, card correspondence, delivery/receipt reads, missing
exact receipt, text equality and message/operation equality. RPC helper hooks are
scoped only to this journey and shared Inbox call; they record the exact RPC
assertion site and finite method, never response Data, output or error prose.
Scope restoration is observation bookkeeping, not a new service cleanup action.
The error_class field classifies only an already-present Go error argument;
none does not claim a structured RPC error was absent. RPC error presence is a
separate boolean, with no public response Data or speculative cause code.
No queries, settlement waits, retries, deadlines or original assertions are added
or relaxed. Receipt-missing flags are the last existing in-loop observations, not
an extra readback. No resend or uncertainty text is accepted as equivalent.

The unchanged workflow invokes the unchanged batch. The collector additionally
uploads failure-sites.jsonl, site-attribution.json, the exact site allowlist and
the retained offline sites-validation.txt receipt. Its explicit allowlist
retains only known leaf/phase/site/method, boolean flags and finite error classes.
Unknown/malformed labels produce payload-free rejection records. Unexpected
fields and prose are never uploaded. A phase-only marker is not an assertion site.
Any failed leaf without a site, or rejected capture, has causal_evidence=INCOMPLETE
even if transport evidence=OBSERVED. This never turns a diagnostic into proof.
The prior_failure framework boolean prevents a later cleanup failure from
crediting an earlier unsited failure as located; it observes no product state.

Replay the offline-only formatter/transport controls from the repository root:

```sh
PLAN_PATH=/path/to/current/owner-generated/proof-plan.json \
  ruby .github/diagnostics/2614/offline-sites.rb
```

The controls extract the actual standard-library-only Go capture helper into a
host-cache standalone harness, run it with the race detector, and transport its
synthetic records through the actual collector. They start no SDK, runtime, store,
PostgreSQL service or test runner, and add no test roots. Controls include Inbox
and logout sites, separate text/operation/missing/read-error labels, phase-only
and absent-site failures, unknown/malformed labels, wrong flags, secret-bearing
payloads, and a negative attempt. The normal source/attempt identity and original
Retry privacy controls remain required. A further hosted observation waits for
review of the exact signed delta; no rerun is authorized by these controls.
