"""Always-on fleet agent with weeks-long memory.

Brain: Muse Glimmer 30B running locally (llama.cpp on a DGX Spark).
Escalation and independent checks: Muse Spark on Meta Model API.

Every day the agent reads the fleet's events, and Glimmer decides what is worth
carrying for weeks (WRITE). When an open memory accumulates repeated failures,
Glimmer writes an operator alert that cites the exact events (RECALL + receipt).
A deterministic check confirms every cited event exists and belongs to that
pole, and Muse Spark independently verifies the alert's claims against the raw
events (VERIFY). Fleet-wide anomalies on a single day escalate to Muse Spark.
"""
from __future__ import annotations

import datetime
import json
import re
from collections import Counter, defaultdict


def _days_before(day: str, n: int) -> str:
    return (datetime.date.fromisoformat(day) - datetime.timedelta(days=n)).isoformat()

from .client import SparkClient
from .fleet_ledger import FleetLedger

# Device-health failures count towards "has this happened before?" alerts.
FAILURE_TYPES = {"reboot", "offline", "power", "network", "thermal", "telemetry_gap", "false_occupancy"}

WRITE_TOOL = {"type": "function", "function": {"name": "memory_ops", "description": "Memory operations for today",
    "parameters": {"type": "object", "properties": {"ops": {"type": "array", "items": {"type": "object", "properties": {
        "op": {"type": "string", "enum": ["remember", "link", "explain", "resolve", "supersede"]},
        "pole": {"type": "string"},
        "kind": {"type": "string", "enum": ["incident", "pattern", "fix", "belief"]},
        "text": {"type": "string", "description": "One self-contained sentence an operator would want to read weeks later."},
        "target_id": {"type": "string", "description": "Existing memory id for link / resolve / supersede"},
        "events": {"type": "array", "items": {"type": "string"}, "description": "Ids of today's events that support this op"},
        "why": {"type": "string"}},
        "required": ["op", "events", "why"]}}}, "required": ["ops"]}}}

WRITE_SYS = """You are the long-term memory of an always-on operations agent that watches a fleet of 39 smart-parking poles 24/7.
Every day you get today's events and the open memories for the poles involved. Decide memory operations so that weeks later
an operator can ask "has this happened before?" and get an answer with receipts.
- link: an event is another occurrence of an open memory (same pole, same kind of problem). Prefer link over remember.
- resolve: an event shows the problem was fixed or its cause confirmed. Give the fix in text.
- supersede: something we believed about a pole or the fleet is no longer true. Give the new belief in text.
- explain: an event is expected because of something already remembered (for example planned maintenance or removal work, or a planned
  rollout restart). Give the memory that explains it as target_id. Explained events never raise alerts.
- remember: a new problem, change or fact worth tracking for weeks. Before remembering, check the open memories for the same pole:
  if one already covers this problem, link instead.
- Every device-health event (reboot, offline, power, network, telemetry_gap, thermal, false_occupancy) on a pole must be covered by
  one op: link it to that pole's open problem, explain it, or remember it as a new problem.
- Ignore routine noise (normal digests) unless it changes what we know.
- Cite only event ids that appear in today's events. Never invent events.
Always respond by calling memory_ops."""

ALERT_SYS = """You are an always-on fleet operations agent. Write a short alert for the on-call operator about a recurring problem.
Use ONLY the memory and events given. State how many times it happened and over how many days, list the dates,
mention any earlier suspected cause or fix, and recommend one concrete next action. Cite events as [e#] and the memory as [m#].
At most 70 words. Plain English."""

PRECHECK_TOOL = {"type": "function", "function": {"name": "precheck", "description": "Is this recurring failure already explained?",
    "parameters": {"type": "object", "properties": {"explained": {"type": "boolean"}, "memory_id": {"type": "string"},
        "why": {"type": "string"}}, "required": ["explained", "why"]}}}

PRECHECK_SYS = """Before paging a human about a recurring failure on one pole, check the agent's fleet-wide memories.
If a remembered planned activity (maintenance, removal, construction, a planned rollout restart) clearly explains these events,
answer explained=true with that memory_id. If not, or if you are unsure, answer explained=false. Always call precheck."""

VERIFY_TOOL = {"type": "function", "function": {"name": "verdict", "description": "Verify an alert against raw events",
    "parameters": {"type": "object", "properties": {
        "supported": {"type": "boolean"}, "problems": {"type": "array", "items": {"type": "string"}}},
        "required": ["supported", "problems"]}}}

VERIFY_SYS = """You are an independent verifier. Check every factual claim in the alert (counts, dates, causes) against the raw events.
supported=true only if every claim is backed by the events. List any unsupported or wrong claims. Always call verdict."""

FLEET_SYS = """You are the escalation analyst for an always-on fleet agent. Many poles reported the same kind of event on the same day.
Using the events and the fleet memories given, say in at most 60 words whether this looks like one shared cause
(for example a server-side deploy or restart) rather than independent pole faults, and what the operator should check.
Cite events as [e#]."""


class FleetAgent:
    def __init__(self, ledger: FleetLedger, glimmer: SparkClient, spark: SparkClient | None, spend_cap_usd: float = 3.0):
        self.ledger = ledger
        self.glimmer = glimmer
        self.spark = spark
        self.cap = spend_cap_usd
        self.events: dict[str, dict] = {}
        self.alerted: dict[str, int] = {}
        self.explained: set[str] = set()
        self.suppressed: list[dict] = []

    # -------------------------------------------------------------- WRITE
    def ingest_day(self, day: str, events: list[dict], batch: int = 10) -> dict:
        for e in events:
            self.events[e["id"]] = e
        ordered = sorted(events, key=lambda e: e["ts"])
        applied = []
        for k in range(0, len(ordered), batch):
            applied += self._write(day, ordered[k:k + batch])
        return self._finish_day(day, events, applied)

    def _write(self, day: str, events: list[dict]) -> list[dict]:
        poles = {e["pole"] for e in events}
        mems = self.ledger.open_for(poles)
        payload = {
            "today": day,
            "open_memories": [{"id": m.id, "pole": m.pole, "kind": m.kind, "text": m.text, "times_seen": len(m.events),
                               "first": m.first_ts[:10], "last": m.last_ts[:10]} for m in mems],
            "events": [{"id": e["id"], "time": e["ts"][11:16], "pole": e["pole"], "type": e["type"], "what": e["summary"],
                        **({"resolution": e["resolution"]} if e.get("resolution") else {})} for e in events],
        }
        args = self.glimmer.call_tool([{"role": "system", "content": WRITE_SYS},
                                       {"role": "user", "content": json.dumps(payload, ensure_ascii=False)}],
                                      WRITE_TOOL, purpose=f"write {day}", max_tokens=6000)
        applied = []
        valid = {e["id"] for e in events}
        for op in args.get("ops", []):
            evs = [x for x in op.get("events", []) if x in valid]
            if not evs:
                applied.append({"op": "rejected", "why": "no valid event ids", **op})
                continue
            ts = max(self.events[x]["ts"] for x in evs)
            pole = op.get("pole") or self.events[evs[0]]["pole"]
            kind, text, why, tid = op.get("kind", "incident"), op.get("text", ""), op.get("why", ""), op.get("target_id", "")
            m = None
            if op["op"] == "explain" and tid:
                m = self.ledger.link(tid, evs, ts, day, None, why)
                if m is not None:
                    self.explained.update(evs)
            elif op["op"] == "link" and tid:
                m = self.ledger.link(tid, evs, ts, day, text or None, why)
            elif op["op"] == "resolve" and tid:
                m = self.ledger.resolve(tid, text or "resolved", evs, ts, day, why)
            elif op["op"] == "supersede" and tid and text:
                m = self.ledger.supersede(tid, text, evs, ts, day, why)
            if m is None and text:
                m = self.ledger.remember(pole, kind, text, evs, ts, day, why)
                op = {**op, "op": "remember"}
            if m is not None:
                applied.append({"op": op["op"], "id": m.id, "target": tid, "pole": m.pole, "text": m.text, "events": evs, "why": why})
        return applied

    def _finish_day(self, day: str, events: list[dict], applied: list[dict]) -> dict:
        # Safety net: the model decides the semantics, code guarantees no device-health event is silently dropped.
        covered = {x for o in applied for x in o.get("events", [])}
        for e in events:
            if e["type"] in FAILURE_TYPES and e["pole"] not in ("fleet", "server") and e["id"] not in covered:
                cands = [m for m in self.ledger.items.values() if m.status == "open" and m.pole == e["pole"]
                         and any(self.events.get(x, {}).get("type") in FAILURE_TYPES for x in m.events)]
                if cands:
                    m = max(cands, key=lambda m: m.last_ts)
                    self.ledger.link(m.id, [e["id"]], e["ts"], day, None, "auto: device-health event on a pole with an open problem")
                    applied.append({"op": "link", "id": m.id, "target": m.id, "pole": m.pole, "text": m.text, "events": [e["id"]], "why": "auto-tracked", "auto": True})
                else:
                    m = self.ledger.remember(e["pole"], "incident", f"{e['pole']}: {e['type'].replace('_', ' ')} - {e['summary']}", [e["id"]], e["ts"], day, "auto-tracked")
                    applied.append({"op": "remember", "id": m.id, "pole": m.pole, "text": m.text, "events": [e["id"]], "why": "auto-tracked", "auto": True})
        self.ledger.save()
        n0 = len(self.suppressed)
        alerts = self._alerts(day)
        return {"day": day, "ops": applied, "alerts": alerts, "suppressed": self.suppressed[n0:],
                "escalation": self._fleet_escalation(day, events)}

    # -------------------------------------------------------------- RECALL -> alert with receipt
    def _alerts(self, day: str) -> list[dict]:
        out = []
        by_pole = defaultdict(list)
        for m in self.ledger.items.values():
            if m.status != "open" or m.pole in ("fleet", "server"):
                continue
            for x in m.events:
                e = self.events.get(x)
                if e and e["type"] in FAILURE_TYPES and x not in self.explained:
                    by_pole[m.pole].append((e, m))
        for pole, pairs in by_pole.items():
            evs = sorted({e["id"]: e for e, _ in pairs}.values(), key=lambda e: e["ts"])
            evs = [e for e in evs if e["day"] >= _days_before(day, 30)]
            days = sorted({e["day"] for e in evs})
            if len(days) < 3 or day not in days or (self.alerted.get(pole) and len(days) < self.alerted[pole] + 2):
                continue
            self.alerted[pole] = len(days)
            fleet_mems = [m for m in self.ledger.items.values() if m.pole in ("fleet", "server") and m.last_ts[:10] >= _days_before(day, 30)]
            pc = self.glimmer.call_tool([{"role": "system", "content": PRECHECK_SYS}, {"role": "user", "content": json.dumps({
                "pole": pole, "events": [{"id": e["id"], "time": e["ts"], "type": e["type"], "what": e["summary"]} for e in evs],
                "fleet_memories": [{"id": m.id, "status": m.status, "text": m.text, "since": m.first_ts} for m in fleet_mems][-30:]},
                ensure_ascii=False)}], PRECHECK_TOOL, purpose=f"precheck {pole}", max_tokens=3000)
            if pc.get("explained") and pc.get("memory_id") in self.ledger.items:
                self.explained.update(e["id"] for e in evs)
                self.suppressed.append({"pole": pole, "day": day, "by": pc["memory_id"], "why": pc.get("why", ""),
                                        "events": [e["id"] for e in evs], "occurrence_days": days})
                continue
            today_mem = [m for e, m in pairs if e["day"] == day][-1]
            mem_ids = sorted({m.id for _, m in pairs}, key=lambda x: int(x[1:]))
            text = self.glimmer.text([{"role": "system", "content": ALERT_SYS}, {"role": "user", "content": json.dumps({
                "pole": pole, "today": day,
                "memories": [{"id": mid, "text": self.ledger.items[mid].text, "since": self.ledger.items[mid].first_ts} for mid in mem_ids],
                "events": [{"id": e["id"], "time": e["ts"], "type": e["type"], "what": e["summary"],
                            **({"resolution": e["resolution"]} if e.get("resolution") else {})} for e in evs]},
                ensure_ascii=False)}], purpose=f"alert {pole}", max_tokens=3000)
            cited = sorted(set(re.findall(r"\be\d+\b", text)), key=lambda x: int(x[1:]))
            det_ok = bool(cited) and all(c in self.events and self.events[c]["pole"] == pole for c in cited)
            out.append({"memory": today_mem.id, "memories": mem_ids, "pole": pole, "day": day, "text": text,
                        "receipt": [e["id"] for e in evs], "occurrence_days": days, "cited": cited,
                        "receipt_check": det_ok, "verify": self._verify(text, evs)})
        return out

    def _verify(self, text: str, evs: list[dict]) -> dict:
        if not self.spark or self.spark.total_cost_usd > self.cap:
            return {"by": "skipped (cap)"}
        try:
            v = self.spark.call_tool([{"role": "system", "content": VERIFY_SYS}, {"role": "user", "content": json.dumps(
                {"alert": text, "raw_events": [{"id": e["id"], "time": e["ts"], "pole": e["pole"], "type": e["type"], "what": e["summary"]} for e in evs]},
                ensure_ascii=False)}], VERIFY_TOOL, purpose="verify", max_tokens=4000)
            return {"by": self.spark.model, **v}
        except Exception as ex:  # never block the always-on loop on the cloud
            return {"by": "error", "problems": [str(ex)[:120]]}

    # -------------------------------------------------------------- escalation
    def _fleet_escalation(self, day: str, events: list[dict]) -> dict | None:
        c = Counter(e["type"] for e in events if e["pole"] not in ("fleet", "server") and e["type"] in FAILURE_TYPES)
        if not c:
            return None
        typ, n = c.most_common(1)[0]
        poles = {e["pole"] for e in events if e["type"] == typ}
        if len(poles) < 5 or not self.spark or self.spark.total_cost_usd > self.cap:
            return None
        evs = [e for e in events if e["type"] == typ] + [e for e in events if e["pole"] in ("fleet", "server")]
        try:
            txt = self.spark.text([{"role": "system", "content": FLEET_SYS}, {"role": "user", "content": json.dumps({
                "day": day, "events": [{"id": e["id"], "time": e["ts"][11:16], "pole": e["pole"], "type": e["type"], "what": e["summary"]} for e in evs],
                "fleet_memories": [{"id": m.id, "text": m.text} for m in self.ledger.open_for({"fleet", "server"}, 8)]},
                ensure_ascii=False)}], purpose=f"escalate {day}", max_tokens=4000)
        except Exception as ex:
            return {"error": str(ex)[:120]}
        return {"day": day, "type": typ, "poles": sorted(poles), "by": self.spark.model, "text": txt, "events": [e["id"] for e in evs]}
