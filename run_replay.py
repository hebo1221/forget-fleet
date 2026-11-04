"""Replay four weeks of a real 39-pole fleet through the always-on agent.

    python3 run_replay.py            # Glimmer local (GLIMMER_BASE_URL), Muse Spark for verify/escalation
Writes runs/replay_v2.json (per-day memory ops, alerts, escalations, ledger snapshots, call stats)."""
import json, os, sys, time
SRC_TRACE = os.environ.get("SRC_TRACE", "runs/replay_v2.json")
OUT_TRACE = os.environ.get("OUT_TRACE", "runs/replay_v2.json")
OUT_LEDGER = os.environ.get("OUT_LEDGER", "runs/v2.ledger.json")
from collections import defaultdict
from fleetmem.agent import FleetAgent
from fleetmem.client import SparkClient, glimmer_client
from fleetmem.fleet_ledger import FleetLedger

def main():
    stream = json.load(open("data/stream.json"))
    days = defaultdict(list)
    for e in stream:
        days[e["day"]].append(e)
    resume = "--resume" in sys.argv
    before = next((a.split("=", 1)[1] for a in sys.argv if a.startswith("--resume-before=")), None)
    ledger = FleetLedger(OUT_LEDGER, load=resume and not before)
    g = glimmer_client("low")
    try:
        s = SparkClient()
    except Exception:
        s = None
    cap = float([a for a in sys.argv[1:] if not a.startswith("--")][0]) if [a for a in sys.argv[1:] if not a.startswith("--")] else 3.0
    agent = FleetAgent(ledger, g, s, spend_cap_usd=cap)
    trace = {"started_at": time.time(), "brain": g.model, "verifier": s.model if s else None, "days": []}
    done = set()
    if resume:
        old = json.load(open(SRC_TRACE))
        trace = {k: v for k, v in old.items() if k != "stream"}
        if before:
            trace["days"] = [d for d in trace["days"] if d["day"] < before]
            from fleetmem.fleet_ledger import Memory
            snap = trace["days"][-1]["ledger"]
            ledger.items = {m["id"]: Memory(id=m["id"], pole=m["pole"], kind=m["kind"], text=m["text"], status=m["status"],
                                            events=m["events"], first_ts=m["first"], last_ts=m["last"], created_day=m["first"][:10],
                                            supersedes=m.get("supersedes"), superseded_by=m.get("superseded_by"),
                                            resolved_by=m.get("resolved_by")) for m in snap}
        for d in trace["days"]:
            done.add(d["day"])
            for x in d["events"]:
                agent.events[x] = next(e for e in stream if e["id"] == x)
            for o in d["ops"]:
                if o.get("op") == "explain":
                    agent.explained.update(o.get("events", []))
            for a in d["alerts"]:
                agent.alerted[a["pole"]] = len(a["occurrence_days"])
        print("resumed after", max(done), "with", len(ledger.items), "memories", flush=True)
    for day in sorted(days):
        if day in done:
            continue
        t0 = time.time(); n0g, n0s = len(g.calls), len(s.calls) if s else 0
        r = agent.ingest_day(day, days[day])
        r["events"] = [e["id"] for e in days[day]]
        r["ledger"] = [{"id": m.id, "pole": m.pole, "kind": m.kind, "text": m.text, "status": m.status, "events": m.events,
                        "first": m.first_ts, "last": m.last_ts, "supersedes": m.supersedes, "superseded_by": m.superseded_by,
                        "resolved_by": m.resolved_by} for m in ledger.items.values()]
        r["calls"] = {"glimmer": [c.__dict__ for c in g.calls[n0g:]], "spark": [c.__dict__ | {"cost_usd": c.cost_usd} for c in (s.calls[n0s:] if s else [])]}
        r["seconds"] = round(time.time() - t0, 1)
        trace["days"].append(r)
        json.dump(trace | {"stream": stream}, open(OUT_TRACE, "w"), ensure_ascii=False, indent=1)
        print(f"{day}: events={len(days[day])} ops={len(r['ops'])} alerts={len(r['alerts'])} esc={'Y' if r['escalation'] else '-'} "
              f"{r['seconds']}s spark=${s.total_cost_usd if s else 0:.3f}", flush=True)
    trace["finished_at"] = time.time()
    trace["glimmer_calls"] = len(g.calls); trace["spark_calls"] = len(s.calls) if s else 0
    trace["spark_cost_usd"] = round(s.total_cost_usd, 4) if s else 0
    json.dump(trace | {"stream": stream}, open(OUT_TRACE, "w"), ensure_ascii=False, indent=1)
    print("done", trace["glimmer_calls"], trace["spark_calls"], trace["spark_cost_usd"])

if __name__ == "__main__":
    main()
