import json


def handle(input):
    if input.get("tool_results"):
        return {
            "text": "Candidate completed.",
            "usage": {"input_tokens": 8, "output_tokens": 3},
        }
    payload = json.loads(input["messages"][-1]["content"])["event"]["payload"]
    candidate_id = payload["candidate_id"]
    return {
        "calls": [{
            "name": "emit_candidate_analyzed",
            "arguments": {
                "batch_id": payload["batch_id"],
                "candidate_id": candidate_id,
                "summary": "analysis-" + candidate_id,
            },
        }],
        "usage": {"input_tokens": 8, "output_tokens": 3},
    }
