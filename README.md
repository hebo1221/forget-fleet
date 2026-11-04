# Forget Fleet

**Weeks-long memory for an always-on Muse Glimmer agent.**

> Monitoring tells you what's broken now. Memory tells you it's the third time.

We run a 39-pole smart-parking deployment in Korea. Every pole has cameras, a plate reader and an engine. It runs 24/7, and it fails slowly: a reboot here, a hold there, a rollout that only half lands. The signal is spread across weeks, but an agent's context only covers hours.

Forget Fleet is an always-on operations agent built on **Muse Glimmer 30B**, running locally on one DGX Spark. Every day it reads the fleet's events and decides what is worth carrying for weeks. When a failure keeps coming back, it tells you, and it cites the exact events behind every claim.

**Demo video:** _VIDEO_URL_

![viewer](docs/viewer.png)

## What it does

| Step | Model | What happens |
|---|---|---|
| **WRITE** | Muse Glimmer 30B, local | Reads the day's events and decides for each: remember a new problem, link it to an earlier memory, resolve it, supersede an old belief, or ignore routine noise. Every operation must cite real event ids. Ops without valid ids are rejected. |
| **RECALL → ALERT** | Muse Glimmer 30B, local | When an open memory collects device-health failures on 3 or more days, Glimmer writes an operator alert using only that memory and its events. |
| **CHECK** | code | Every event the alert cites must exist and belong to the same pole. If there's no receipt, there's no alert. |
| **VERIFY / ESCALATE** | Muse Spark 1.3, Meta Model API | Independently verifies each alert's counts, dates and causes against the raw events. Diagnoses days when five or more poles fail the same way (a shared cause, such as a rollout, versus independent faults). |

The memory store is an append-only ledger. Memories are linked, resolved or superseded, never silently rewritten, and each keeps the ids of the events that support it.

## Results (recorded live run)

_RESULTS_

These come from one recorded run on our own data. They are not a benchmark. Model outputs vary between runs.

## Data and privacy

- **Ops logbook:** 255 events written down by our team from Oct 9 to Nov 4: reboots, holds, rollouts, disk alarms, power and network faults. Muse Spark converted them into structured events.
- **Archive telemetry:** 36,692 hourly archive records (file present and size only) from all poles, used to derive reporting gaps.
- **Never used:** camera images, plate numbers and people's names. Site and pole names are replaced (`P-01`…`P-39`). The anonymised stream is in `data/stream.json`.

## Run it

Requirements: Python 3.9+ with no extra packages, a local Muse Glimmer server, and a Meta Model API key for the Muse Spark verifier (optional).

```bash
# 1. Serve Muse Glimmer locally (we use llama.cpp on a DGX Spark)
huggingface-cli download meta-models/Muse-Glimmer-30B-GGUF Muse-Glimmer-30B-KQuant-17GB-Q4_K_M.gguf --local-dir .
llama-server -m Muse-Glimmer-30B-KQuant-17GB-Q4_K_M.gguf --port 30111 -ngl 99 -c 32768 --parallel 1 -fa on -ctk q8_0 -ctv q8_0 --jinja

# 2. Replay four weeks through the always-on agent
cp .env.example .env                  # optional: MODEL_API_KEY for the Muse Spark verifier
python3 run_replay.py                 # -> runs/fleet_replay.json
python3 eval_qa.py                    # operator questions: memory vs full log vs none

# 3. Look at it
python3 tools/build_viewer.py && python3 -m http.server 8000   # open http://localhost:8000/web/
```

## Code map

- `fleetmem/agent.py`: the always-on agent: WRITE, alert from memory, receipt check, Muse Spark verify and escalation.
- `fleetmem/fleet_ledger.py`: append-only fleet memory (remember, link, resolve, supersede).
- `fleetmem/client.py`: one stdlib client for both local Muse Glimmer (llama.cpp, OpenAI-compatible) and the Meta Model API.
- `run_replay.py`, `eval_qa.py`: four-week replay and the question evaluation.
- `tools/extract_logbook.py`, `tools/build_stream.py`: how the anonymised event stream was built.
- `web/index.html`: the fleet viewer used in the video.

## What was built during the hackathon

Everything in this repository was written during the hackathon, on Nov 4-5, 2026: the agent, the ledger, the clients, the stream builder, the replay, the evaluation and the viewer. The Muse Glimmer server setup on our DGX Spark was also new.

**Pre-existing, not part of this submission:**
- The smart-parking deployment itself, and the ops logbook our team kept while running it.
- Multi-turn's hosted memory product, [Forget](https://forget.sh). Its ideas (receipts, supersede rather than overwrite) shaped this design, but this repo neither contains nor calls Forget's engine.

## License

MIT
