#!/usr/bin/env python3
import json
import urllib.request
from datetime import datetime, timezone, date
from calendar import monthrange

BZN = "DE-LU"

def fetch_json(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=30) as r:
        return json.loads(r.read().decode("utf-8"))

def iso(d: date) -> str:
    return d.strftime("%Y-%m-%d")

def group_to_6h(unix_seconds, prices):
    # buckets: (YYYY,MM,DD,block) where block = 0..3
    buckets = {}
    for u, p in zip(unix_seconds, prices):
        if p is None:
            continue
        try:
            p = float(p)
        except Exception:
            continue
        dt = datetime.fromtimestamp(int(u), tz=timezone.utc)
        # for monthly overview we don't need exact Berlin offsets; block grouping by UTC is acceptable for this estimate
        block = dt.hour // 6
        key = (dt.year, dt.month, dt.day, block)
        b = buckets.get(key)
        if b is None:
            buckets[key] = [p, 1, dt]
        else:
            b[0] += p
            b[1] += 1

    out = []
    for (y,m,d,block), (s,n,dt_any) in buckets.items():
        out.append({
            "ts": int(dt_any.timestamp()),
            "label": f"{d:02d}.{m:02d} {block*6:02d}:00",
            "value": s / max(n,1)
        })
    out.sort(key=lambda x: x["ts"])
    return out

def avg(vals):
    xs = [v for v in vals if isinstance(v,(int,float)) and v == v]
    return sum(xs)/len(xs) if xs else None

def main():
    now_utc = datetime.now(timezone.utc)
    today = now_utc.date()
    y, m = today.year, today.month
    last_day = monthrange(y, m)[1]
    start = date(y, m, 1)
    end = date(y, m, last_day)

    url = f"https://api.energy-charts.info/price?bzn={BZN}&start={iso(start)}&end={iso(end)}"
    j = fetch_json(url)

    unix_seconds = j.get("unix_seconds", [])
    prices = j.get("price", [])
    series6 = group_to_6h(unix_seconds, prices)

    # today index (first 6h-bucket of today)
    today_prefix = f"{today.day:02d}.{today.month:02d}"
    today_index = next((i for i,x in enumerate(series6) if x["label"].startswith(today_prefix)), len(series6)-1)

    values = [x["value"] for x in series6]
    so_far = values[:max(1, today_index+1)]
    so_far_mean = avg(so_far)

    # last 7 unique days
    day_labels = []
    seen = set()
    for x in series6:
        dlab = x["label"][:5]  # dd.mm
        if dlab not in seen:
            seen.add(dlab)
            day_labels.append(dlab)
    last7 = day_labels[-7:]
    last7_vals = [x["value"] for x in series6 if x["label"][:5] in last7]
    rest_mean = avg(last7_vals)

    part = max(0.0, min(1.0, (today.day-1) / float(last_day)))
    estimate = None
    if so_far_mean is not None and rest_mean is not None:
        estimate = so_far_mean * part + rest_mean * (1.0 - part)
    else:
        estimate = so_far_mean

    out = {
        "updated_utc": now_utc.isoformat().replace("+00:00", "Z"),
        "bzn": BZN,
        "month": f"{y:04d}-{m:02d}",
        "estimate_eur_mwh": round(estimate, 2) if estimate is not None else None,
        "today_index": int(today_index),
        "official_last_month_eur_mwh": None,  # später befüllen
        "series_6h": [{"label": x["label"], "value": round(x["value"], 2)} for x in series6]
    }

    with open("data/price_current_month.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
