# Reassemble test2json chunks per test before parsing the existing owner's
# receipts. This sums observation counters, never reconstructs domain history.
def totals($fields):
  reduce .[] as $row ({};
    reduce $fields[] as $field (. ;
      .[$field] = (.[$field] // 0) + $row[$field]));

[.[] | select(.Action == "output" and .Test != null)]
| group_by([.Package, .Test])
| map({test: .[0].Test, text: (map(.Output) | join(""))})
| [ .[] as $test
    | $test.text | split("\n")[]
    | select(test("Q6_(NATIVE|REGISTRY)_COPY_RECEIPT"))
    | capture("Q6_(?<workload>NATIVE|REGISTRY)_COPY_RECEIPT (?:phase=(?<phase>[^ ]+) )?counts=(?<counts>\\{.*\\})")
    | {workload: .workload, backend: ($test.test | split("/")[1]),
       phase: (.phase // "run.start-through-completed"), counts: (.counts | fromjson)} ]
| group_by([.workload, .backend])
| map({workload: .[0].workload, backend: .[0].backend, windows: length,
       transactions: (map(.counts) | totals([
         "BeginAttempts", "Begun", "ReadCommits", "WriteCommits", "Failed",
         "CommitAttempts", "CommitFailures", "RollbackAttempts", "CleanupFailures"])),
       finalizer_calls: (map(.counts.Revision) | totals([
         "Finalizations", "ExecCalls", "QueryCalls", "QueryRowCalls"])),
       json_copies: (map(.counts.JSONCopies) as $copies
         | reduce ["WorkflowHeader", "EntityMetadata"][] as $family ({};
             .[$family] = ($copies | map(.[$family]) | totals([
               "Calls", "Copies", "SubmittedBytes", "SucceededCalls", "FailedCalls",
               "SucceededBytes", "FailedBytes", "CommittedBytes", "UncommittedBytes",
               "IndeterminateBytes", "RollbackAttemptedBytes", "UnfinishedBytes"]))))})
| if length != 1 then error("one single-backend, single-execution receipt required")
  elif .[0].windows != (if .[0].workload == "NATIVE" then 207 else 1 end)
  then error("missing or duplicated observation windows")
  else .[0] end
