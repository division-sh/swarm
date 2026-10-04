import json


def handle(input):
    results = input["tool_results"] or []
    if not results:
        tool = [tool for tool in input["tools"] if tool["name"] == "read_flow_data"][0]
        static_id = tool["schema"]["properties"]["static_id"]["enum"][0]
        return {"calls": [{"name": "read_flow_data", "arguments": {
            "kind": "static_file", "static_id": static_id,
        }}], "usage": {"input_tokens": 1, "output_tokens": 1}}
    result = json.loads(results[-1]["content"])["tool_result"][-1]
    assert result["ok"], result
    return {"calls": [{"name": "emit_work_completed", "arguments": {
        "marker": result["result"]["content"].strip(),
    }}], "usage": {"input_tokens": 1, "output_tokens": 1}}
