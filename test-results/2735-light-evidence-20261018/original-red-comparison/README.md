# #2735 catalog-required-verify RED classification, 2026-10-18

Source under test: 0e78f146d8a0afb141628520c3e7c4f32033c339.
Pinned master control: 909222b50cf88232d81af10b6fcc46ab78916fbe.
Original pre-rebase source: 55adb583b9de0572f69e6e4adb23776ac0851ed1.
Hosted run: https://github.com/division-sh/swarm/actions/runs/39148692608/job/117738853229

One invocation per source, through existing swarm-test admission:

    go run ./cmd/swarm-test -- ./internal/cliapp -run '^TestSwarmTestCatalogCompanionsAreProtocolOnly$' -count=1

Candidate: FAIL, package 3.330s, missing required instances/attributions.
Master: PASS, package 3.198s.

The scenario response helper is byte-identical on all three sources:
internal/cliapp/test_command_test.go blob 6c9f1b141aaaefe8af32cd78aa633c5daed7f240.
The validator is byte-identical before/after rebase:
internal/cliapp/diagnostics.go blob 60c15a5f4711dbf6d4feb124013792a69c655511.
Master does not require the two new arrays. Candidate does, matching its spec
and production response. The mock helper never supplies them. The failure
occurs in CLI response validation; no operational-source/store memo is called.

The complete hosted job log and failing-case list are retained. Eight failing
roots all consume that one helper. There are ten direct consumer roots in the
source; two negative-input roots never reach diagnosis and retain their exact
pre-lookup refusals. Proposed repair is fixture plus existing malformed-response
negative reaches only; no source repair has been applied or pushed.

Evidence hashes:
- candidate log: 690438eba11cfb8ca87f651ab9e9098e5d0c0d283730a56931a2449da622b33f
- master log: cedc1f2963dd87e1bfcfeb63f096bca1ccc66cf148af3e749aff8c6764461967
- hosted log: f70f12fcf3747830f96c3444db8d3071b5bfb05158baffd55a695c93f2c6d9e1
- hosted failing-case list: 8f2f7eb71dc3e4eb11674e7df794b2dad4977ba1cd984ee72e9e600563ccd64c
