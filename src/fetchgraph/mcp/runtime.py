"""MCP tool dispatch and session-scoped evidence snapshots."""

import collections
import json
import time
import uuid
from importlib.resources import files

from jsonschema import Draft202012Validator

from fetchgraph.retrieval.models import EvidenceBundle
from fetchgraph.retrieval.engine import RetrievalEngine, Failure, dump

TOOLS = json.loads(files("fetchgraph.mcp").joinpath("tools.json").read_text())["tools"]
__all__ = ["Runtime", "Failure", "dump", "TOOLS"]


class Runtime(RetrievalEngine):
    def __init__(self, config):
        super().__init__(config)
        self.snapshots = collections.OrderedDict()

    async def call(self, name, args):
        tool = next(t for t in TOOLS if t["name"] == name)
        try:
            Draft202012Validator(tool["inputSchema"]).validate(args)
            if any(type(args[k]) is not int for k in ("offset", "length") if k in args):
                raise ValueError()
        except Exception:
            raise Failure("INVALID_ARGUMENT") from None
        if name == "fetchgraph_catalog":
            return {
                "sources": [
                    {
                        "id": key,
                        **{
                            k: value[k]
                            for k in (
                                "description",
                                "kind",
                                "inference_profiles",
                                "default_profile",
                            )
                        },
                    }
                    for key, value in sorted(self.config["sources"].items())
                ]
            }
        if name == "fetchgraph_read_evidence":
            for created, evidence in self.snapshots.values():
                if time.monotonic() - created < 600 and args["ref"] in evidence:
                    saved = evidence[args["ref"]]
                    offset = args.get("offset", 0)
                    length = args.get("length", 2048)
                    total = len(saved["text"])
                    if offset > total:
                        raise Failure("INVALID_ARGUMENT")
                    end = min(total, offset + length)
                    return {
                        **saved,
                        "ref": args["ref"],
                        "text": saved["text"][offset:end],
                        "total_chars": total,
                        "next_offset": end if end < total else None,
                    }
            raise Failure("EVIDENCE_GONE")
        outcome = await self.execute(
            args["question"],
            source_id=args["source"],
            inference_profile=args.get("inference_profile"),
            synthesize=name == "fetchgraph_ask",
        )
        if isinstance(outcome, EvidenceBundle):
            self.publish(outcome)
            return outcome.model_dump(mode="json")
        bundle, answer = outcome
        Draft202012Validator(tool["outputSchema"]).validate(answer)
        self.publish(bundle)
        return answer

    def publish(self, bundle):
        if not bundle.evidence:
            return
        self.snapshots[uuid.uuid4().hex] = (
            time.monotonic(),
            {
                e.ref: {
                    "source": e.source,
                    "locator": e.locator,
                    "text": e.text,
                    "selectors": e.provenance.selectors,
                }
                for e in bundle.evidence
            },
        )
        while len(self.snapshots) > 64:
            self.snapshots.popitem(last=False)
