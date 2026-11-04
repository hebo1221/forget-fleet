"""Append-only memory ledger with receipts.

Nothing is ever silently overwritten. A correction creates a new item and
marks the old one `superseded`; a forget request marks the item `forgotten`
and scrubs its text from every place the model can read it. Each item keeps
its provenance: which session, which turn, and the exact quote it came from.
"""
from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

KINDS = ("constraint", "decision", "preference", "fact")


@dataclass
class Item:
    id: str
    text: str
    kind: str
    status: str  # active | superseded | forgotten
    session: str
    turn: int
    quote: str
    created_at: float
    supersedes: str | None = None
    superseded_by: str | None = None
    forgotten_at: float | None = None
    history: list[dict] = field(default_factory=list)


class Ledger:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.items: dict[str, Item] = {}
        self.events: list[dict] = []
        if self.path.exists():
            raw = json.loads(self.path.read_text())
            self.items = {d["id"]: Item(**d) for d in raw.get("items", [])}
            self.events = raw.get("events", [])

    # ---------------------------------------------------------------
    def _next_id(self) -> str:
        return f"m{len(self.items) + 1}"

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps({
            "items": [asdict(i) for i in self.items.values()],
            "events": self.events,
        }, ensure_ascii=False, indent=2))

    def _event(self, op: str, **kw) -> dict:
        ev = {"op": op, "at": time.time(), **kw}
        self.events.append(ev)
        return ev

    # ---------------------------------------------------------------
    def active(self) -> list[Item]:
        return [i for i in self.items.values() if i.status == "active"]

    def add(self, text: str, kind: str, session: str, turn: int, quote: str, why: str = "") -> Item:
        it = Item(id=self._next_id(), text=text, kind=kind if kind in KINDS else "fact",
                  status="active", session=session, turn=turn, quote=quote, created_at=time.time())
        self.items[it.id] = it
        self._event("remember", item=it.id, text=text, session=session, turn=turn, why=why)
        return it

    def supersede(self, old_id: str, text: str, kind: str, session: str, turn: int, quote: str, why: str = "") -> Item | None:
        old = self.items.get(old_id)
        if not old or old.status != "active":
            return None
        new = Item(id=self._next_id(), text=text, kind=kind if kind in KINDS else old.kind,
                   status="active", session=session, turn=turn, quote=quote,
                   created_at=time.time(), supersedes=old_id)
        self.items[new.id] = new
        old.status = "superseded"
        old.superseded_by = new.id
        self._event("correct", item=new.id, replaces=old_id, old_text=old.text, text=text,
                    session=session, turn=turn, why=why)
        return new

    def forget(self, item_id: str, session: str, turn: int, why: str = "") -> Item | None:
        it = self.items.get(item_id)
        if not it or it.status == "forgotten":
            return None
        it.status = "forgotten"
        it.forgotten_at = time.time()
        # Scrub content: the model must never be able to read it again.
        it.history.append({"scrubbed_at": it.forgotten_at, "kind": it.kind})
        it.text = "[forgotten at user's request]"
        it.quote = ""
        for ev in self.events:
            if ev.get("item") == item_id or ev.get("replaces") == item_id:
                for k in ("text", "old_text", "why"):
                    if k in ev:
                        ev[k] = "[forgotten]"
        # The forget request itself names the thing being forgotten, so its
        # wording is not stored either: only the fact that a forget happened.
        self._event("forget", item=item_id, session=session, turn=turn, why="user asked to forget")
        return it

    def leaks(self, needle: str) -> list[str]:
        """Return places where a forgotten string still appears (should be empty)."""
        blob = json.dumps({"items": [asdict(i) for i in self.items.values()], "events": self.events},
                          ensure_ascii=False).lower()
        return [needle] if needle.lower() in blob else []
