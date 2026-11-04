"""Append-only fleet memory with receipts.

A memory is something the agent decided is worth carrying for weeks: an
incident, a recurring pattern, a fix, or a belief about the fleet. Every memory
keeps the ids of the raw events that support it, so any claim the agent makes
can be traced back to timestamps (the receipt). Memories are never silently
rewritten: a changed belief supersedes the old one, a fix resolves an incident.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path

KINDS = ("incident", "pattern", "fix", "belief")


@dataclass
class Memory:
    id: str
    pole: str
    kind: str
    text: str
    status: str                  # open | resolved | superseded
    events: list[str]            # supporting raw event ids (the receipt)
    first_ts: str
    last_ts: str
    created_day: str
    supersedes: str | None = None
    superseded_by: str | None = None
    resolved_by: str | None = None
    history: list[dict] = field(default_factory=list)


class FleetLedger:
    def __init__(self, path: str | Path, load: bool = False):
        self.path = Path(path)
        self.items: dict[str, Memory] = {}
        self.log: list[dict] = []
        if load and self.path.is_file():
            raw = json.loads(self.path.read_text())
            self.items = {d["id"]: Memory(**d) for d in raw.get("items", [])}
            self.log = raw.get("log", [])

    def _id(self) -> str:
        return f"m{len(self.items) + 1}"

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps({"items": [asdict(m) for m in self.items.values()], "log": self.log},
                                        ensure_ascii=False, indent=1))

    def open_for(self, poles: set[str], limit: int = 24) -> list[Memory]:
        live = [m for m in self.items.values() if m.status == "open" and (m.pole in poles or m.pole in ("fleet", "server"))]
        live.sort(key=lambda m: m.last_ts, reverse=True)
        return live[:limit]

    def remember(self, pole, kind, text, events, ts, day, why="") -> Memory:
        m = Memory(id=self._id(), pole=pole, kind=kind if kind in KINDS else "incident", text=text, status="open",
                   events=list(dict.fromkeys(events)), first_ts=ts, last_ts=ts, created_day=day)
        self.items[m.id] = m
        self.log.append({"day": day, "op": "remember", "id": m.id, "pole": pole, "text": text, "events": m.events, "why": why})
        return m

    def link(self, mid, events, ts, day, text=None, why="") -> Memory | None:
        m = self.items.get(mid)
        if not m or m.status != "open":
            return None
        new = [e for e in events if e not in m.events]
        m.events += new
        m.last_ts = max(m.last_ts, ts)
        if text:
            m.history.append({"day": day, "old_text": m.text})
            m.text = text
        self.log.append({"day": day, "op": "link", "id": mid, "pole": m.pole, "events": new, "text": m.text, "why": why})
        return m

    def resolve(self, mid, fix_text, events, ts, day, why="") -> Memory | None:
        m = self.items.get(mid)
        if not m or m.status != "open":
            return None
        fix = self.remember(m.pole, "fix", fix_text, events, ts, day, why)
        m.status = "resolved"
        m.resolved_by = fix.id
        self.log.append({"day": day, "op": "resolve", "id": mid, "pole": m.pole, "by": fix.id, "why": why})
        return fix

    def supersede(self, mid, text, events, ts, day, why="") -> Memory | None:
        old = self.items.get(mid)
        if not old or old.status != "open":
            return None
        new = self.remember(old.pole, old.kind, text, old.events + list(events), ts, day, why)
        new.first_ts = old.first_ts
        new.supersedes = mid
        old.status = "superseded"
        old.superseded_by = new.id
        self.log.append({"day": day, "op": "supersede", "id": mid, "by": new.id, "pole": old.pole, "why": why})
        return new
