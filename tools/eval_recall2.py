"""Second memory arm: Glimmer selects relevant memories from the full memory index, then answers from those memories
plus the raw events they cite (receipts). Same questions, same grader as eval_qa.py. Adds arm "memory_recall" to runs/eval_qa.json."""
import json, sys, time, concurrent.futures as cf
sys.path.insert(0, ".")
from fleetmem.client import SparkClient, glimmer_client
from eval_qa import QUESTIONS, ANSWER_SYS, GRADE_SYS, GRADE_TOOL, ev_line

SEL_TOOL = {"type": "function", "function": {"name": "select", "description": "Pick the memories needed to answer",
    "parameters": {"type": "object", "properties": {"ids": {"type": "array", "items": {"type": "string"}}}, "required": ["ids"]}}}
SEL_SYS = """You are the recall step of a fleet agent's memory. From the memory index, pick every memory needed to answer the
operator's question (up to 15). Include memories about the same poles, the same kind of problem, fixes, and fleet-wide events
that could explain it. Always call select."""

def main():
    tr = json.load(open("runs/replay_v3.json")); E = {e["id"]: e for e in tr["stream"]}
    led = tr["days"][-1]["ledger"]; L = {m["id"]: m for m in led}
    index = "\n".join(f"[{m['id']}] {m['pole']} {m['status']} seen {len(m['events'])}x {m['first'][:10]}..{m['last'][:10]}: {m['text']}" for m in led)
    full_log = "\n".join(ev_line(e) for e in tr["stream"])
    data = json.load(open("runs/eval_qa.json")); rows = data["rows"]
    g = glimmer_client("low")
    for i, q in enumerate(QUESTIONS):
        n0 = len(g.calls); t0 = time.time()
        sel = g.call_tool([{"role": "system", "content": SEL_SYS}, {"role": "user", "content": f"Memory index:\n{index}\n\nQuestion: {q}"}], SEL_TOOL, purpose="recall select", max_tokens=3000)
        ids = [x for x in sel.get("ids", []) if x in L][:15]
        cited = sorted({x for mid in ids for x in L[mid]["events"]}, key=lambda x: int(x[1:]))
        ctx = "Memories recalled by Forget:\n" + "\n".join(f"[{mid}] {L[mid]['pole']} ({L[mid]['status']}, seen {len(L[mid]['events'])}x): {L[mid]['text']}" for mid in ids) \
              + "\n\nRaw events those memories cite (receipts):\n" + "\n".join(ev_line(E[x]) for x in cited if x in E)
        ans = g.text([{"role": "system", "content": ANSWER_SYS}, {"role": "user", "content": f"{ctx}\n\nQuestion: {q}"}], purpose="qa recall", max_tokens=3000)
        ptoks = sum(c.prompt_tokens for c in g.calls[n0:])
        rows[i]["memory_recall"] = {"answer": ans, "prompt_tokens": ptoks, "latency_s": round(time.time() - t0, 1), "selected": ids}
        print("answered", i, ptoks, rows[i]["memory_recall"]["latency_s"], flush=True)
        json.dump(data, open("runs/eval_qa.json", "w"), ensure_ascii=False, indent=1)
    def grade(i):
        r = rows[i]; s = SparkClient(); gr = {}
        for _ in range(4):
            try:
                gr = s.call_tool([{"role": "system", "content": GRADE_SYS}, {"role": "user", "content":
                    f"Raw event log:\n{full_log}\n\nQuestion: {r['q']}\n\nAnswer to grade:\n{r['memory_recall']['answer']}"}], GRADE_TOOL, purpose="grade", max_tokens=8000)
                if "score" in gr: break
            except Exception as ex:
                gr = {"error": str(ex)[:120]}
        return i, gr, s.total_cost_usd
    spend = 0.0
    with cf.ThreadPoolExecutor(8) as ex:
        for i, gr, cost in ex.map(grade, range(len(rows))):
            rows[i]["memory_recall"].update(gr); spend += cost
    arms = ("memory_recall", "memory", "full_log", "none")
    data["summary"] = {a: {"score": sum(r[a].get("score", 0) for r in rows), "max": 2 * len(rows),
                           "avg_prompt_tokens": round(sum(r[a]["prompt_tokens"] for r in rows) / len(rows)),
                           "avg_latency_s": round(sum(r[a]["latency_s"] for r in rows) / len(rows), 1)} for a in arms}
    data["spark_cost_usd"] = round(data.get("spark_cost_usd", 0) + spend, 4)
    json.dump(data, open("runs/eval_qa.json", "w"), ensure_ascii=False, indent=1)
    print(json.dumps(data["summary"], indent=1))
    for r in rows: print(f"- {r['q'][:60]} | recall {r['memory_recall'].get('score')} mem {r['memory'].get('score')} full {r['full_log'].get('score')}")

if __name__ == "__main__":
    main()
