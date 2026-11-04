"""Merge the ops logbook events and real hourly telemetry into one anonymised event stream.

Inputs (private, not in the public repo):
  data/logbook_chunk*.json      structured events from our ops logbook (no plates, no names)
  data/hourly_presence_utc.csv  per-pole hourly archive metadata (file present + size), no content
Output:
  data/stream.json              [{id, ts, day, pole, type, summary, resolution, source}]
"""
import csv, datetime, glob, json, re, statistics
from collections import defaultdict

KST = datetime.timezone(datetime.timedelta(hours=9))
DAY0, DAY_END = "2026-10-09", "2026-11-04"


def anon_pole(p: str) -> str:
    p = (p or "").strip()
    m = re.fullmatch(r"CG([AB])0*(\d+)", p) or re.fullmatch(r"([AB])-0*(\d+)", p)
    if m:
        row, n = m.group(1), int(m.group(2))
        return f"P-{n:02d}" if row == "A" else f"P-{23 + n:02d}"
    return "server" if p.lower() in ("server", "servers") else "fleet"


SCRUB = [
    (r"\b([AB])-0*(\d{1,2})\b", lambda m: anon_pole(m.group(0))),
    (r"\bCG[AB]\d{3}\b", lambda m: anon_pole(m.group(0))),
    (r"(?i)chilgok|칠곡|silla\s*enc|신라이앤씨|신라|maehodong|매호동|daegu|대구|wonju|원주|\bDIP\b", ""),
    (r"(?i)deploy172", "archive server"), (r"(?i)gpu4090|gpu5080", "GPU server"),
    (r"안현준|현준|김남주|남주|명범|종헌|예미나|김정훈|정훈|Hyun-?jun|Namju|Myeongbeom|Jongheon", "a team member"),
    (r"[0-9]{2,3}[가-힣][0-9]{4}", "[plate]"),
]


def scrub(s: str) -> str:
    s = s or ""
    for pat, rep in SCRUB:
        s = re.sub(pat, rep, s)
    return re.sub(r"\s{2,}", " ", s).strip()


def logbook_events():
    out = []
    for f in sorted(glob.glob("data/logbook_chunk*.json")):
        for e in json.load(open(f)):
            ts = (e.get("ts_kst") or "").strip()
            if not re.match(r"\d{4}-\d\d-\d\d", ts):
                continue
            if len(ts) == 10:
                ts += " 12:00"
            ts = ts[:16]
            if not (DAY0 <= ts[:10] <= DAY_END):
                continue
            out.append({"ts": ts, "pole": anon_pole(e.get("pole", "")), "type": e.get("type", "other"),
                        "summary": scrub(e.get("summary", "")), "resolution": scrub(e.get("resolution", "")), "source": "logbook"})
    # drop exact duplicates produced by overlapping notes
    seen, uniq = set(), []
    for e in sorted(out, key=lambda x: x["ts"]):
        k = (e["ts"][:13], e["pole"], e["type"], e["summary"][:40])
        if k not in seen:
            seen.add(k); uniq.append(e)
    return uniq


def telemetry_events():
    per = defaultdict(dict)
    for r in csv.DictReader(open("data/hourly_presence_utc.csv")):
        t = datetime.datetime.strptime(r["date_utc"] + r["hour_utc"], "%Y%m%d%H").replace(tzinfo=datetime.timezone.utc).astimezone(KST)
        per[r["pole"]][t] = int(r["bytes"])
    start = datetime.datetime(2026, 10, 9, tzinfo=KST)
    end = datetime.datetime(2026, 11, 4, 13, tzinfo=KST)   # archive is complete up to here
    out, digest = [], defaultdict(lambda: {"full": 0, "low": 0, "gap": 0})
    for pole, hs in per.items():
        ap = anon_pole(pole)
        first = max(min(hs), start).replace(minute=0, second=0, microsecond=0)
        hours, t = [], first
        while t < end:
            hours.append(t); t += datetime.timedelta(hours=1)
        present = [h for h in hours if h in hs]
        med = statistics.median([hs[h] for h in present]) if present else 0
        by_day = defaultdict(list)
        for h in hours:
            by_day[h.strftime("%Y-%m-%d")].append(h)
        run = None
        for h in hours + [end]:
            missing = h < end and h not in hs
            if missing and run is None:
                run = h
            elif not missing and run is not None:
                n = int((h - run).total_seconds() // 3600)
                d = run.strftime("%Y-%m-%d")
                if n >= 48:
                    out.append({"ts": run.strftime("%Y-%m-%d %H:%M"), "pole": ap, "type": "telemetry_gap", "source": "telemetry",
                                "summary": f"Stopped reporting to the archive at {run.strftime('%H:%M')}; no data since.", "resolution": ""})
                    k = run + datetime.timedelta(days=3)
                    while k < h:
                        out.append({"ts": k.strftime("%Y-%m-%d 09:00"), "pole": ap, "type": "telemetry_gap", "source": "telemetry",
                                    "summary": f"Still not reporting to the archive (silent for {int((k - run).total_seconds() // 86400)} days).", "resolution": ""})
                        k += datetime.timedelta(days=3)
                else:
                    out.append({"ts": run.strftime("%Y-%m-%d %H:%M"), "pole": ap, "type": "telemetry_gap", "source": "telemetry",
                                "summary": f"Stopped reporting for {n} h ({run.strftime('%H:%M')}-{h.strftime('%H:%M')}).", "resolution": ""})
                run = None
        for d, hh in by_day.items():
            low = [h for h in hh if h in hs and med and hs[h] < 0.2 * med and 8 <= h.hour < 20]
            gaps = [h for h in hh if h not in hs]
            if not gaps:
                digest[d]["full"] += 1
            digest[d]["low"] += len(low)
            if len(low) >= 4:
                out.append({"ts": low[0].strftime("%Y-%m-%d %H:%M"), "pole": ap, "type": "low_activity", "source": "telemetry",
                            "summary": f"Archive volume below 20% of this pole's normal for {len(low)} daytime hours (from {low[0].strftime('%H:%M')}).", "resolution": ""})
    n_poles = len(per)
    for d, v in sorted(digest.items()):
        out.append({"ts": f"{d} 23:59", "pole": "fleet", "type": "digest", "source": "telemetry",
                    "summary": f"Daily digest: {v['full']}/{n_poles} poles reported every hour; {v['low']} quiet daytime pole-hours fleet-wide.", "resolution": ""})
    return out


if __name__ == "__main__":
    lb, tm = logbook_events(), telemetry_events()
    ev = sorted(lb + tm, key=lambda e: (e["ts"], e["pole"]))
    ev = [e for e in ev if DAY0 <= e["ts"][:10] <= DAY_END]
    for i, e in enumerate(ev, 1):
        e["id"] = f"e{i}"
        e["day"] = e["ts"][:10]
    json.dump(ev, open("data/stream.json", "w"), ensure_ascii=False, indent=1)
    from collections import Counter
    print("logbook", len(lb), "telemetry", len(tm), "total", len(ev))
    print(Counter(e["type"] for e in ev).most_common())
    print(Counter(e["day"] for e in ev).most_common(5))
