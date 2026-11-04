"""Minimal Meta Model API client (stdlib only).

Talks to the OpenAI-compatible Chat Completions endpoint of Meta Model API
(https://api.meta.ai/v1) and keeps a per-call usage log so every memory
decision can be traced back to the Muse Spark call that made it.
"""
from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_BASE_URL = "https://api.meta.ai/v1"
DEFAULT_MODEL = "muse-spark-1.3"

# Public list prices for muse-spark-1.3 (USD per 1M tokens), used only for
# the cost line shown in receipts. Reasoning tokens are billed as output.
PRICE_IN = 1.25
PRICE_OUT = 4.25


def load_env_file(path: str | os.PathLike | None) -> None:
    """Load KEY=VALUE lines into os.environ (without overriding)."""
    if not path:
        return
    p = Path(path).expanduser()
    if not p.exists():
        return
    for line in p.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())


@dataclass
class CallRecord:
    purpose: str
    model: str
    prompt_tokens: int
    completion_tokens: int
    reasoning_tokens: int
    latency_s: float
    price_in: float = PRICE_IN
    price_out: float = PRICE_OUT
    gen_tps: float = 0.0

    @property
    def cost_usd(self) -> float:
        return (self.prompt_tokens * self.price_in + self.completion_tokens * self.price_out) / 1e6


@dataclass
class SparkClient:
    api_key: str | None = None
    base_url: str = ""
    model: str = ""
    timeout_s: float = 300.0
    price_in: float = PRICE_IN
    price_out: float = PRICE_OUT
    extra_body: dict = field(default_factory=dict)
    local: bool = False
    calls: list[CallRecord] = field(default_factory=list)

    def __post_init__(self) -> None:
        load_env_file(os.environ.get("MODEL_API_ENV_FILE", ".env"))
        self.api_key = self.api_key or os.environ.get("MODEL_API_KEY")
        self.base_url = (self.base_url or os.environ.get("MODEL_API_BASE_URL") or DEFAULT_BASE_URL).rstrip("/")
        self.model = self.model or os.environ.get("MODEL_API_MODEL") or DEFAULT_MODEL
        if not self.api_key and not self.local:
            raise RuntimeError("MODEL_API_KEY is not set (see .env.example)")

    # ------------------------------------------------------------------
    def chat(self, messages: list[dict], *, purpose: str, tools: list[dict] | None = None,
             tool_choice: dict | str | None = None, max_tokens: int = 4000) -> dict:
        body: dict = {"model": self.model, "messages": messages, "max_tokens": max_tokens, **self.extra_body}
        if tools:
            body["tools"] = tools
        if tool_choice:
            body["tool_choice"] = tool_choice
        req = urllib.request.Request(
            f"{self.base_url}/chat/completions",
            data=json.dumps(body).encode(),
            headers={"Authorization": f"Bearer {self.api_key or 'local'}", "Content-Type": "application/json"},
            method="POST",
        )
        t0 = time.time()
        for attempt in range(4):
            try:
                with urllib.request.urlopen(req, timeout=self.timeout_s) as r:
                    data = json.loads(r.read())
                break
            except urllib.error.HTTPError as e:
                body_txt = e.read().decode()[:400]
                # 429 / 503 overload responses are rejected before generation, so a retry is safe.
                if e.code in (429, 503) and attempt < 3:
                    time.sleep(3 * (attempt + 1))
                    continue
                raise RuntimeError(f"Meta Model API {e.code}: {body_txt}") from None
        dt = time.time() - t0
        u = data.get("usage") or {}
        tm = data.get("timings") or {}
        self.calls.append(CallRecord(
            purpose=purpose,
            model=data.get("model", self.model),
            prompt_tokens=u.get("prompt_tokens", 0),
            completion_tokens=u.get("completion_tokens", 0),
            reasoning_tokens=(u.get("completion_tokens_details") or {}).get("reasoning_tokens", 0),
            latency_s=round(dt, 2),
            price_in=self.price_in, price_out=self.price_out,
            gen_tps=round(tm.get("predicted_per_second", 0.0), 1),
        ))
        return data["choices"][0]["message"]

    def call_tool(self, messages: list[dict], tool: dict, *, purpose: str, max_tokens: int = 6000) -> dict:
        """Force a single function call and return its parsed arguments."""
        name = tool["function"]["name"]
        msg = self.chat(messages, purpose=purpose, tools=[tool],
                        tool_choice="auto",
                        max_tokens=max_tokens)
        for tc in msg.get("tool_calls") or []:
            if tc.get("function", {}).get("name") == name:
                return json.loads(tc["function"]["arguments"] or "{}")
        # Fallback: some responses put JSON in content.
        content = (msg.get("content") or "").strip()
        if content.startswith("```"):
            content = content.strip("`").split("\n", 1)[-1]
        return json.loads(content)

    def text(self, messages: list[dict], *, purpose: str, max_tokens: int = 6000) -> str:
        return (self.chat(messages, purpose=purpose, max_tokens=max_tokens).get("content") or "").strip()

    @property
    def total_cost_usd(self) -> float:
        return sum(c.cost_usd for c in self.calls)


def glimmer_client(reasoning: str = "low") -> SparkClient:
    """Muse Glimmer 30B served locally (llama.cpp, OpenAI-compatible). No API key, no per-token cost."""
    return SparkClient(api_key="", timeout_s=900.0, base_url=os.environ.get("GLIMMER_BASE_URL", "http://127.0.0.1:30111/v1"),
                       model=os.environ.get("GLIMMER_MODEL", "muse-glimmer-30b"), local=True,
                       price_in=0.0, price_out=0.0,
                       extra_body={"chat_template_kwargs": {"reasoning_strength": reasoning}})
