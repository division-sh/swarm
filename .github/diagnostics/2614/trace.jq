def metadata:
  {request_ordinal, truncated, nodes: [.nodes[] |
    {index,parent,depth,type,sentinels,class,sqlstate,sqlstate_invalid} |
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
