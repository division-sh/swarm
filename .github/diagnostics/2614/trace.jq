def deadlock_cycle:
  if type != "object" then {state:"cycle_unparsed"}
  elif .state == "cycle_unparsed" then {state:"cycle_unparsed"}
  elif .state == "cycle_observed_owners_unmapped" and
      (.edges|type)=="array" and (.edges|length)>=2 and (.edges|length)<=8 and
      all(.edges[];
        type=="object" and
        (.waiter|type)=="string" and (.waiter|test("^p[1-8]$")) and
        (.blocker|type)=="string" and (.blocker|test("^p[1-8]$")) and
        .waiter!=.blocker and .lock=="transactionid" and
        (.mode=="ShareLock" or .mode=="ExclusiveLock") and
        (.object|type)=="string" and (.object|test("^x[1-8]$")))
    then {state,edges:[.edges[]|{waiter,blocker,lock,mode,object}]}
  else {state:"cycle_unparsed"} end;
def metadata:
  {request_ordinal, truncated, nodes: [.nodes[] |
    . as $node |
    {index,parent,depth,type,sentinels,class,sqlstate,sqlstate_invalid} +
    (if .sqlstate=="40P01" and has("deadlock_cycle") then
      {deadlock_cycle:($node.deadlock_cycle|deadlock_cycle)} else {} end) |
    with_entries(select(.value != null))]};
def only_match(pattern):
  [capture(pattern)] | if length==1 then .[0] else error("malformed diagnostic trace") end;
[inputs | (try fromjson catch null) | select(.Output? != null) | . as $event |
if (.Output | contains("ORIGINAL_CAUSE leaf=")) then
  (.Output | only_match("ORIGINAL_CAUSE leaf=(?<leaf>[^ ]+) ceremony=(?<ceremony>[^ ]+) stage=(?<stage>[^ ]+) metadata=(?<data>\\{.*\\})")) as $trace |
  {time:$event.Time, package:$event.Package, test:$event.Test, leaf:$trace.leaf,
    ceremony:$trace.ceremony, stage:$trace.stage, cause:($trace.data | fromjson | metadata)}
elif (.Output | contains("ONBOARDING_ORIGINAL_CAUSE ")) then
  (.Output | only_match("ONBOARDING_ORIGINAL_CAUSE (?<data>\\{.*\\})")) as $trace |
  {time:$event.Time, package:$event.Package, test:$event.Test,
    ceremony:"not_observed", stage:"not_observed", cause:($trace.data | fromjson | metadata)}
else empty end] | .[]
