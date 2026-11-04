"""Grade saved eval answers with Muse Spark (blind to arm), with retries. Reads/writes runs/eval_qa.json."""
import json, sys, concurrent.futures as cf
sys.path.insert(0, ".")
from fleetmem.client import SparkClient
from eval_qa import GRADE_SYS, GRADE_TOOL, ev_line

def main():
    data = json.load(open("runs/eval_qa.json")); rows = data["rows"]
    stream = json.load(open("runs/replay_v3.json"))["stream"]
    full_log = "\n".join(ev_line(e) for e in stream)
    def grade(job):
        i, arm = job
        r = rows[i]; s = SparkClient(); gr = {}
        for attempt in range(4):
            try:
                gr = s.call_tool([{"role": "system", "content": GRADE_SYS}, {"role": "user", "content":
                    f"Raw event log:\n{full_log}\n\nQuestion: {r['q']}\n\nAnswer to grade:\n{r[arm]['answer']}"}], GRADE_TOOL, purpose="grade", max_tokens=8000)
                if "score" in gr: break
            except Exception as ex:
                gr = {"error": str(ex)[:120]}
        return i, arm, gr, s.total_cost_usd
    spend = 0.0
    with cf.ThreadPoolExecutor(8) as ex:
        for i, arm, gr, cost in ex.map(grade, [(i, a) for i in range(len(rows)) for a in ("memory", "full_log", "none")]):
            rows[i][arm].update(gr); spend += cost
    summ = {a: {"score": sum(r[a].get("score", 0) for r in rows), "max": 2 * len(rows), "graded": sum("score" in r[a] for r in rows),
                "avg_prompt_tokens": round(sum(r[a]["prompt_tokens"] for r in rows) / len(rows)),
                "avg_latency_s": round(sum(r[a]["latency_s"] for r in rows) / len(rows), 1)} for a in ("memory", "full_log", "none")}
    json.dump({"rows": rows, "summary": summ, "spark_cost_usd": round(spend, 4)}, open("runs/eval_qa.json", "w"), ensure_ascii=False, indent=1)
    print(json.dumps(summ, indent=1), "spark $", round(spend, 3))
    for r in rows:
        print(f"- {r['q'][:70]} | mem {r['memory'].get('score')} full {r['full_log'].get('score')} none {r['none'].get('score')}")

if __name__ == "__main__":
    main()
