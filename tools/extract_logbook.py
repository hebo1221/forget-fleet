"""One-off: turn our ops logbook (team notes) into a structured, anonymised event timeline.
Uses Muse Spark (Meta Model API). Input excludes plate text; output excludes names and plates."""
import json, re, sys, concurrent.futures as cf
sys.path.insert(0, '.')
from fleetmem.client import SparkClient

TOOL = {"type": "function", "function": {"name": "events", "description": "Operational fleet events", "parameters": {"type": "object", "properties": {"events": {"type": "array", "items": {"type": "object", "properties": {
    "ts_kst": {"type": "string", "description": "YYYY-MM-DD HH:MM in KST; best estimate from the note"},
    "pole": {"type": "string", "description": "Bay/pole id like A-23 or B-15, or 'fleet' or 'server'"},
    "type": {"type": "string", "enum": ["reboot", "offline", "engine_restart", "deploy", "rollback", "hold", "disk", "labeler_lag", "thermal", "false_occupancy", "missed_plate", "billing_case", "power", "network", "field_visit", "config", "other"]},
    "summary": {"type": "string", "description": "<= 22 words, English, plain facts, NO person names, NO plate numbers, NO company or place names"},
    "resolution": {"type": "string", "description": "What fixed it or the conclusion, if stated; else empty"}},
    "required": ["ts_kst", "pole", "type", "summary"]}}}, "required": ["events"]}}}

SYS = """You convert an operations logbook for a fleet of smart-parking poles into structured events.
Keep only things an always-on fleet operations agent would observe or be told: device reboots, offline periods, engine restarts,
software or firmware deployments and rollbacks, holds, disk pressure, labeler lag, thermal problems, false occupancy on empty bays,
missed or wrong plate reads at a bay (never the plate text), billing-affecting cases, power or network faults, field visits, config changes.
Skip research, model training, evaluations, meetings, approvals and paperwork. One event per distinct occurrence; keep exact times when given.
Never output person names, plate numbers, company names or place names. Always respond by calling the events tool."""

def run(chunk_id, sections):
    c = SparkClient()
    text = "\n\n".join(f"[{d}]\n{s}" for d, s in sections)
    args = c.call_tool([{"role": "system", "content": SYS}, {"role": "user", "content": text}], TOOL, purpose=f"logbook{chunk_id}", max_tokens=32000)
    evs = args.get("events", [])
    if isinstance(evs, str):
        try: evs = json.loads(evs)
        except Exception: evs = []
    evs = [e for e in evs if isinstance(e, dict)]
    json.dump(evs, open(f"data/logbook_chunk{chunk_id}.json", "w"), ensure_ascii=False, indent=1)
    return chunk_id, evs, c.total_cost_usd

if __name__ == "__main__":
    secs = json.load(open("data/logbook_sections.json"))
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 6
    chunks = [[] for _ in range(n)]
    tot = sum(len(s) for _, s in secs); acc = 0
    for d, s in secs:
        chunks[min(n - 1, int(acc / tot * n))].append((d, s)); acc += len(s)
    allev, spend = [], 0.0
    with cf.ThreadPoolExecutor(n) as ex:
        for cid, evs, cost in ex.map(lambda a: run(*a), list(enumerate(chunks))):
            print(f"chunk {cid}: {len(evs)} events, ${cost:.3f}", flush=True)
            allev += evs; spend += cost
    plate = re.compile(r"[0-9]{2,3}[가-힣][0-9]{4}")
    allev = [e for e in allev if not plate.search(json.dumps(e, ensure_ascii=False))]
    allev.sort(key=lambda e: e.get("ts_kst", ""))
    json.dump(allev, open("data/logbook_events.json", "w"), ensure_ascii=False, indent=1)
    print("total events", len(allev), f"spend ${spend:.3f}")
