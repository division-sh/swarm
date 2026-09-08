import json


def handle(input):
    if len(input["tool_results"] or []) > 1:
        return {"text": "Child read reported."}
    if not input["tool_results"]:
        tool = [tool for tool in input["tools"] if tool["name"] == "read_flow_data"][0]
        own_id = tool["schema"]["properties"]["static_id"]["enum"][0]
        return {"calls": [{"name": "read_flow_data", "arguments": {
            "kind": "static_file", "static_id": own_id,
        }}]}
    result = json.loads(input["tool_results"][-1]["content"])["tool_result"][0]
    assert result["ok"], result
    value = result["result"]
    return {"calls": [{"name": "emit_child_completed", "arguments": {
        "static_id": value["static_id"], "content": value["content"],
    }}]}
