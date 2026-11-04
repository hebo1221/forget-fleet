"""Operator questions that need weeks of memory: Forget memory vs full raw log in context (both on local Muse Glimmer).

Arms (same model, same questions):
  memory   : Glimmer answers from the memories Forget kept, plus the raw events those memories cite (the receipts)
  full_log : Glimmer answers with all 4 weeks of raw events pasted into its context
  none     : Glimmer answers with no history at all (floor)
Grading: Muse Spark judges each answer against the complete raw event log (1M context), blind to the arm.
"""
import json, re, sys, time
from fleetmem.client import SparkClient, glimmer_client

QUESTIONS = [
    "How many times has P-23 rebooted since Oct 9, on which dates, and what is the most likely cause?",
    "Which poles stopped reporting to the archive during these four weeks, and since when?",
    "Have P-38 or P-39 shown parked-car (occupied) readings on an empty bay at night before? When?",
    "What went wrong the last times we rolled out new software to the whole fleet, and what did we learn?",
    "Has the archive server's disk filled up before? What did we do about it?",
    "Which pole had the most holds, and what usually cleared them?",
    "When did labeler lag peak, and what brought it down?",
    "Which poles had power or network problems, and on which days?",
    "What changed on P-10 around Oct 15-16?",
    "Is there an open device-health issue I should send a technician for this week? Give the pole and why.",
    "P-33, P-34 and P-35 have been silent for 12 days. Should I send someone, and why?",
]

ANSWER_SYS = """You are the operations agent for a fleet of 39 smart-parking poles. Answer the operator's question using only the history provided.
Give dates and counts when you can, cite event ids like [e12], and say plainly if the history does not contain the answer. At most 80 words."""

GRADE_TOOL = {"type": "function", "function": {"name": "grade", "description": "Grade an answer against the raw log",
    "parameters": {"type": "object", "properties": {"score": {"type": "integer", "enum": [0, 1, 2]},
        "reason": {"type": "string"}}, "required": ["score", "reason"]}}}
GRADE_SYS = """You grade an operations agent's answer against the complete raw event log of the fleet.
score 2 = correct and complete on the facts that matter (counts, dates, poles, causes); 1 = partly correct or missing key facts;
0 = wrong, invented, or says it doesn't know when the log has the answer. Hallucinated facts score 0. Always call grade."""


def ev_line(e):
    return f"[{e['id']}] {e['ts']} {e['pole']} {e['type']}: {e['summary']}" + (f" (resolution: {e['resolution']})" if e.get("resolution") else "")


def retrieve(question, ledger, k=14):
    poles = set(re.findall(r"P-\d\d", question))
    words = {w for w in re.findall(r"[a-z]{4,}", question.lower())} - {"which", "have", "what", "when", "since", "these", "four", "weeks", "pole", "poles", "fleet"}
    def score(m):
        s = 3 * (m["pole"] in poles) + sum(w in m["text"].lower() for w in words)
        return s + 0.01 * len(m["events"])
    ms = sorted([m for m in ledger if m["status"] != "superseded"], key=score, reverse=True)
    return [m for m in ms if score(m) >= 1][:k] or ms[:6]


def main():
    import concurrent.futures as cf
    tr = json.load(open("runs/replay_v3.json"))
    stream = tr["stream"]; E = {e["id"]: e for e in stream}
    ledger = tr["days"][-1]["ledger"]
    full_log = "\n".join(ev_line(e) for e in stream)
    g = glimmer_client("low"); s = SparkClient()
    out = []
    for q in QUESTIONS:  # Glimmer has one slot: answer sequentially
        row = {"q": q}
        mems = retrieve(q, ledger)
        cited = sorted({x for m in mems for x in m["events"]}, key=lambda x: int(x[1:]))
        mem_ctx = "Memories kept by Forget:\n" + "\n".join(f"[{m['id']}] {m['pole']} ({m['status']}, seen {len(m['events'])}x, {m['first'][:10]}..{m['last'][:10]}): {m['text']}" for m in mems) \
                  + "\n\nRaw events those memories cite:\n" + "\n".join(ev_line(E[x]) for x in cited if x in E)
        for arm, ctx in (("memory", mem_ctx), ("full_log", "Full raw event log (Oct 9 - Nov 4):\n" + full_log), ("none", "(no history available)")):
            n0 = len(g.calls); t0 = time.time()
            ans = g.text([{"role": "system", "content": ANSWER_SYS}, {"role": "user", "content": f"{ctx}\n\nQuestion: {q}"}], purpose=f"qa {arm}", max_tokens=3000)
            c = g.calls[n0]
            row[arm] = {"answer": ans, "prompt_tokens": c.prompt_tokens, "latency_s": round(time.time() - t0, 1)}
        out.append(row)
        print("answered:", q[:60], {a: (row[a]["prompt_tokens"], row[a]["latency_s"]) for a in ("memory", "full_log", "none")}, flush=True)
        json.dump({"rows": out}, open("runs/eval_qa.json", "w"), ensure_ascii=False, indent=1)

    def grade(job):
        i, arm = job
        r = out[i]
        sc = SparkClient()
        gr = sc.call_tool([{"role": "system", "content": GRADE_SYS}, {"role": "user", "content":
            f"Raw event log:\n{full_log}\n\nQuestion: {r['q']}\n\nAnswer to grade:\n{r[arm]['answer']}"}], GRADE_TOOL, purpose="grade", max_tokens=3000)
        return i, arm, gr, sc.total_cost_usd
    spend = 0.0
    with cf.ThreadPoolExecutor(8) as ex:
        for i, arm, gr, cost in ex.map(grade, [(i, a) for i in range(len(out)) for a in ("memory", "full_log", "none")]):
            out[i][arm].update(gr); spend += cost
    summ = {a: {"score": sum(r[a].get("score", 0) for r in out), "max": 2 * len(out),
                "avg_prompt_tokens": round(sum(r[a]["prompt_tokens"] for r in out) / len(out)),
                "avg_latency_s": round(sum(r[a]["latency_s"] for r in out) / len(out), 1)} for a in ("memory", "full_log", "none")}
    json.dump({"rows": out, "summary": summ, "spark_cost_usd": round(spend, 4)}, open("runs/eval_qa.json", "w"), ensure_ascii=False, indent=1)
    print(json.dumps(summ, indent=1), "spark $", round(spend, 3))


if __name__ == "__main__":
    main()
