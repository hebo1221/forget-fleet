"""Second key for paging: Muse Spark independently verifies every alert Glimmer raised.

Runs as a separate pass over the replay trace (in production this is an async queue behind the always-on loop).
The verifier sees the alert, the raw events in its receipt, and the agent memories the alert cites.
An alert pages a human only if the verifier agrees."""
import json, re, sys, concurrent.futures as cf
sys.path.insert(0, ".")
from fleetmem.client import SparkClient
from fleetmem.agent import VERIFY_TOOL

SYS = """You are an independent verifier for an always-on fleet agent. Check every factual claim in the alert (counts, dates,
causes, recommendations' premises) against the raw events and the agent memories provided. [e#] are raw events, [m#] are agent memories.
Treat a memory as support only if its text and its own events back the claim. Be strict about counting: follow-up reports of one
continuous outage are not separate occurrences. supported=true only if every claim is backed. List problems. Always call verdict."""


def main(path):
    t = json.load(open(path))
    E = {e["id"]: e for e in t["stream"]}
    jobs = []
    for di, d in enumerate(t["days"]):
        led = {m["id"]: m for m in d["ledger"]}
        for ai, a in enumerate(d["alerts"]):
            mids = sorted(set(re.findall(r"\bm\d+\b", a["text"])) | set(a.get("memories", [])), key=lambda x: int(x[1:]))
            mems = [{"id": m, "text": led[m]["text"], "events": led[m]["events"]} for m in mids if m in led]
            jobs.append((di, ai, a["text"], [E[x] for x in a["receipt"]], mems))

    def run(job):
        di, ai, text, evs, mems = job
        s = SparkClient()
        for attempt in range(3):
            try:
                v = s.call_tool([{"role": "system", "content": SYS}, {"role": "user", "content": json.dumps({
            "alert": text,
            "raw_events": [{"id": e["id"], "time": e["ts"], "pole": e["pole"], "type": e["type"], "what": e["summary"]} for e in evs],
            "agent_memories": mems}, ensure_ascii=False)}], VERIFY_TOOL, purpose="verify", max_tokens=8000)
                if "supported" in v:
                    break
            except Exception as ex:
                v = {"error": str(ex)[:160]}
        return di, ai, {"by": s.model, **v, "cost_usd": round(s.total_cost_usd, 5)}

    with cf.ThreadPoolExecutor(8) as ex:
        for di, ai, v in ex.map(run, jobs):
            t["days"][di]["alerts"][ai]["verify"] = v
            a = t["days"][di]["alerts"][ai]
            print(t["days"][di]["day"], a["pole"], "supported" if v.get("supported") else "REJECTED", (v.get("problems") or [""])[0][:120])
    json.dump(t, open(path, "w"), ensure_ascii=False, indent=1)


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "runs/replay_v3.json")
