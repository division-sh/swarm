import json


def handle(input):
    results = []
    for message in input["tool_results"] or []:
        results.extend(json.loads(message["content"])["tool_result"])
    if results and results[-1]["name"].startswith("emit_"):
        return {"text": "Read reported."}
    tool = [tool for tool in input["tools"] if tool["name"] == "read_flow_data"][0]
    own_id = tool["schema"]["properties"]["static_id"]["enum"][0]
    if not results:
        event = json.loads(input["messages"][-1]["content"])["event"]
        foreign = event["payload"].get("foreign_id", "")
        return {"calls": [{"name": "read_flow_data", "arguments": {
            "kind": "static_file", "static_id": foreign or own_id,
        }}]}
    last = results[-1]
    assert last["ok"], last
    value = last["result"]
    return {"calls": [{"name": "emit_read_completed", "arguments": {
        "static_id": value["static_id"], "content": value["content"],
    }}]}
