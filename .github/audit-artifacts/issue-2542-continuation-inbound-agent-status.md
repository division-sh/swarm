# Cohort 98: exact paused agent-delivery status

The three GitHub/Slack/Stripe paused-ingress consumers now supply the original
selected coordinator to their shared status witness. The existing closed delivery
read owner projects only status for exact event/agent-role/subscriber keys. It
does not borrow a run join, delivery eligibility, newest row, context inference or
status normalization. Native absence remains ErrNoRows, not empty success.

One finite helper recipe pins the original PostgreSQL SELECT; the equivalent
SQLite query enables independent native controls. Existing caller recipes are
updated and normalize only the exact selected input, preserving every pending,
pause/resume, marker, payload, receipt and channel assertion. All three known
callers consume the owner; the recursive consumption probe found no other helper
or SQLite duplicate status interpreter.

Focused both-store native controls prove real in-progress/delivered values,
original read/no-writer evidence, event/agent exclusion, cancellation and missing/
closed-owner refusal. The actual three PostgreSQL pause/resume journeys run under
race. No new getter, callback, SQL allowance, semantic classifier, runtime/spec
behavior, framework or compatibility path. Existing #2542 approval/raw_sql_policy
govern; remaining marker/payload/setup and wider authority debt stays tracked.
