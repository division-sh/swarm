# Implementation Stop / New Runtime Defect

Historical reproduction record. Superseded by [bounded readback approval](https://github.com/division-sh/swarm/issues/2498#issuecomment-5901655130), the recorded production amendment in the main audit, and the candidate repair. The new active stop is `issue-2498-paused-restart-stop.md`; the former frozen/no-production-change statements below describe the earlier investigation, not the current head.

Agent-g, #2498, 2026-09-30. The [approved gate](https://github.com/division-sh/swarm/issues/2498#issuecomment-5901252923) was consumed and the classifier/oracle addendum was [recorded before implementation](https://github.com/division-sh/swarm/issues/2498#issuecomment-5901330932). Audit-only published head: `7d0ebd6e7`; production baseline: `origin/master@21bff28c8`. Fixture/probe WIP remains local in `/home/youmew/dev/swarm/worktrees/agent-g-2498` for independent reproduction.

## Confirmed Blocker

The migrated numeric fixture passes the supported verifier with its exact 100-row checksum preserved. The real compiled CLI creates a feed-only run against the real internal retained lifecycle listener on SQLite and PostgreSQL. Public `run.pause` acknowledges the paused run. The next required `run.fan_out.list` observation fails with JSON-RPC `-32603` on both stores, before crash/restart.

This is a public readback defect, not evidence that pause or recovery itself is broken. M05/M09/M12 cannot receive their required feed/checkpoint credit. No settled-restart substitution or private/raw-SQL checkpoint is proposed.

Root-cause reproduction: `fanoutobligation.Intent.ReadbackAt` (`read_surface.go:228`) unconditionally reads `Request.PlanRef.BundleHash`. A valid deployment feed deliberately has no handler PlanRef; its bundle belongs to `Request.Deployment.BundleHash`. The emitted bundle is empty, so `ListPage.Validate` rejects `invalid fan-out row 0`. New `TestDeploymentFanOutReadbackPreservesOriginBundle` fails for open and closed feeds in all three repetitions; existing handler readback and deployment closed-union controls pass all three repetitions.

Two sibling gaps are statically identified, not yet execution-qualified: `compareReadbackKeys` omits `DeploymentFeedID` while the selected-store cursor/order includes it; the authoritative OpenRPC `FanOutIntentKey` requires handler-only delivery/declaration fields and forbids the deployment key. A one-line bundle correction alone cannot establish the complete public deployment-readback contract.

## Gate Request

The test-only implementation is frozen at its explicit stop condition. Please rule on absorbing the bounded existing-owner public deployment-readback correction or tracking it as a prerequisite. No production repair has been made and no new issue, queue, recovery mechanism, endpoint, compatibility path, or framework is proposed.

The repair census must include the shared readback/page/key policy, both `Pipeline*Owner.ListFanOutIntents` adapters, `ObserveFanOutRuntimePage`, apiv1 `runFanOutListHandler`, authoritative API/OpenRPC key/readback/order contract, and CLI consumers. Preserve handler-origin behavior and qualify both origins, multi-feed ordering/cursors, paused/open/closed states and both stores. This proposal is not self-issued permission to code.

Reproducers:

```sh
go test ./internal/releasee2e -run '^TestGoldenNumericDataScatterParkRestartBothStores$' -count=1 -v -timeout=5m
go test ./internal/runtime/fanoutobligation -run '^TestDeploymentFanOutReadbackPreservesOriginBundle$' -count=3 -v
go test ./internal/runtime/fanoutobligation -run '^Test(FanOutReadbackStateAndUnavailableMetrics|DeploymentOriginClosedUnionAndZeroRow)$' -count=3
```

The first test requires existing host PostgreSQL test authority. The corpus gate remains approved for its existing test boundary; this newly discovered runtime repair is unapproved. #2407 remains open. No full suite, U1-U5 qualification, interrupted-recovery success, PR or closure proof is claimed. No production runtime files, old #2008 WIP, external handover, provider or Telegram state were changed. The public checkpoint probe and failing owner control are investigation evidence, not a review-ready suite.
