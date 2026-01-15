#!/usr/bin/env python3
# scripts/update_prices.py
#
# Erstellt (Berlin-Zeit):
# - data/price_current_month.json  (1..jetzt, 6h-Serie + laufende Schätzung)
# - data/price_prev_month.json     (Vormonat komplett, 6h-Serie + Monatsmittel)
#
# Datenquelle: SMARD (Bundesnetzagentur)
# - 4169 = Großhandelsstrompreis / Day-Ahead (€/MWh)
# - 4067 = Stromerzeugung: Wind Onshore (MW)
#
# Monatsmarktwert-Schätzung:
#   MW = sum(price_hour * wind_hour) / sum(wind_hour)
#
# Hinweis: Wir nutzen Stundenauflösung für beide Reihen.

import json
import urllib.request
from dataclasses import dataclass
from datetime import datetime, date, timezone
from calendar import monthrange
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

TZ = ZoneInfo("Europe/Berlin")

OUT_DIR = Path("data")
OUT_DIR.mkdir(parents=True, exist_ok=True)

REGION = "DE"
FILTER_PRICE = 4169  # Großhandelsstrompreis / Day-Ahead (€/MWh)
FILTER_WIND  = 4067  # Wind Onshore (MW)
RESOLUTION = "hour"

SMARD_BASE = "https://www.smard.de/app/chart_data"


def fetch_json(url: str) -> Dict[str, Any]:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "github-actions (windkompass)", "Accept": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=45) as r:
        return json.loads(r.read().decode("utf-8"))


def month_start_end_local(year: int, month: int) -> Tuple[datetime, datetime]:
    last = monthrange(year, month)[1]
    start = datetime(year, month, 1, 0, 0, 0, tzinfo=TZ)
    end = datetime(year, month, last, 23, 59, 59, tzinfo=TZ)
    return start, end


def prev_month(year: int, month: int) -> Tuple[int, int]:
    if month == 1:
        return year - 1, 12
    return year, month - 1


def to_ms(dt: datetime) -> int:
    # SMARD-Timestamps sind ms (UTC-basiert)
    return int(dt.astimezone(timezone.utc).timestamp() * 1000)


def pick_chunk_timestamp(index_ts: List[int], want_from_ms: int) -> int:
    """
    SMARD liefert in index_*.json mehrere 'chunk'-Startzeitpunkte.
    Wir wählen den größten Timestamp <= want_from_ms.
    Falls keiner passt: den kleinsten.
    """
    xs = [int(x) for x in index_ts if isinstance(x, (int, float, str))]
    xs = sorted(xs)
    if not xs:
        raise RuntimeError("SMARD index enthält keine timestamps.")
    cand = [x for x in xs if x <= want_from_ms]
    return cand[-1] if cand else xs[0]


def smard_index_url(filter_id: int, region: str, resolution: str) -> str:
    return f"{SMARD_BASE}/{filter_id}/{region}/index_{resolution}.json"


def smard_series_url(filter_id: int, region: str, resolution: str, chunk_ts: int) -> str:
    # Format laut SMARD/bundesAPI: {filter}_{region}_{resolution}_{timestamp}.json
    return f"{SMARD_BASE}/{filter_id}/{region}/{filter_id}_{region}_{resolution}_{chunk_ts}.json"


def load_smard_series(filter_id: int, region: str, resolution: str, month_start: datetime) -> List[Tuple[int, float]]:
    """
    Lädt eine SMARD-Zeitreihe (Liste von [timestamp_ms, value]).
    Wir holen genau 1 Chunk: den, der den Monatsstart sicher abdeckt.
    """
    idx = fetch_json(smard_index_url(filter_id, region, resolution))
    index_ts = idx.get("timestamps") or []
    if not isinstance(index_ts, list):
        raise RuntimeError("SMARD index format unerwartet: timestamps fehlt/kein array.")

    chunk_ts = pick_chunk_timestamp(index_ts, want_from_ms=to_ms(month_start))

    data = fetch_json(smard_series_url(filter_id, region, resolution, chunk_ts))
    series = data.get("series") or []
    if not isinstance(series, list):
        raise RuntimeError("SMARD series format unerwartet: series fehlt/kein array.")

    out: List[Tuple[int, float]] = []
    for row in series:
        if not (isinstance(row, list) and len(row) >= 2):
            continue
        try:
            t_ms = int(row[0])
            v = float(row[1]) if row[1] is not None else float("nan")
        except Exception:
            continue
        out.append((t_ms, v))
    return out


@dataclass
class Bucket:
    ts: int        # epoch seconds (Berlin, repr)
    label: str     # "dd.mm HH:00"
    value: float   # €/MWh (wind-gewichtet)
    day_key: str   # "dd.mm"


def hour_key_local(ts_ms: int) -> datetime:
    dt_utc = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
    dt_loc = dt_utc.astimezone(TZ)
    return dt_loc.replace(minute=0, second=0, microsecond=0)


def build_weighted_buckets_6h(
    price_series: List[Tuple[int, float]],
    wind_series: List[Tuple[int, float]],
    month_start: datetime,
    month_end: datetime,
    cut_after: Optional[datetime],
) -> Tuple[List[Bucket], int, int]:
    """
    Align stündlich über Local-Hour-Key.
    Bucket-Wert = sum(price*wind)/sum(wind) innerhalb 6h-Block.
    """
    price_map: Dict[datetime, float] = {}
    wind_map: Dict[datetime, float] = {}

    for t_ms, v in price_series:
        if v != v:
            continue
        k = hour_key_local(t_ms)
        price_map[k] = float(v)

    for t_ms, v in wind_series:
        if v != v:
            continue
        k = hour_key_local(t_ms)
        wind_map[k] = float(v)

    # Schnittmenge und Range-Filter
    keys = sorted(set(price_map.keys()) & set(wind_map.keys()))

    buckets: Dict[Tuple[int, int, int, int], Dict[str, float]] = {}  # sums
    used_price_hours = 0
    used_wind_hours = 0

    for k in keys:
        if k < month_start or k > month_end:
            continue
        if cut_after is not None and k > cut_after:
            continue

        p = price_map[k]
        w = wind_map[k]
        used_price_hours += 1
        used_wind_hours += 1

        block = k.hour // 6
        bkey = (k.year, k.month, k.day, block)
        o = buckets.get(bkey)
        if o is None:
            o = {"pw": 0.0, "w": 0.0, "rep_ts": k.timestamp()}
            buckets[bkey] = o

        o["pw"] += p * w
        o["w"] += w

    out: List[Bucket] = []
    for (y, m, d, block), o in buckets.items():
        hh = block * 6
        label = f"{d:02d}.{m:02d} {hh:02d}:00"
        day_key = f"{d:02d}.{m:02d}"
        wsum = o["w"]
        val = (o["pw"] / wsum) if wsum > 0 else float("nan")
        out.append(
            Bucket(
                ts=int(o["rep_ts"]),
                label=label,
                value=val,
                day_key=day_key,
            )
        )

    out.sort(key=lambda x: x.ts)
    return out, used_price_hours, used_wind_hours


def safe_avg(xs: List[float]) -> Optional[float]:
    ys = [x for x in xs if isinstance(x, (int, float)) and x == x]
    return (sum(ys) / len(ys)) if ys else None


def estimate_from_buckets(buckets: List[Bucket], now_loc: datetime, days_in_month: int) -> Tuple[Optional[float], int]:
    """
    Aktueller Monat: "bis heute" vs "Rest" (letzte 7 Tage) wie vorher – nur jetzt auf Marktwert-Buckets.
    """
    if not buckets:
        return None, 0

    today_key = f"{now_loc.day:02d}.{now_loc.month:02d}"
    today_index = next((i for i, b in enumerate(buckets) if b.day_key == today_key), len(buckets) - 1)

    so_far_vals = [b.value for b in buckets[: max(1, today_index + 1)]]
    mean_so_far = safe_avg(so_far_vals)

    # letzte 7 unique Tage
    seen: List[str] = []
    seen_set = set()
    for b in buckets[: max(1, today_index + 1)]:
        if b.day_key not in seen_set:
            seen_set.add(b.day_key)
            seen.append(b.day_key)
    last7_days = seen[-7:]
    last7_vals = [b.value for b in buckets if b.day_key in last7_days]
    rest_est = safe_avg(last7_vals)

    part = max(0.0, min(1.0, (now_loc.day - 1) / float(max(days_in_month, 1))))
    if mean_so_far is None:
        return rest_est, int(today_index)
    if rest_est is None:
        return mean_so_far, int(today_index)

    est = mean_so_far * part + rest_est * (1.0 - part)
    return est, int(today_index)


def build_month_payload(year: int, month: int, mode: str) -> Dict[str, Any]:
    month_start, month_end = month_start_end_local(year, month)
    now_loc = datetime.now(TZ)
    cut_after = now_loc if mode == "current" else None

    # SMARD laden
    price_series = load_smard_series(FILTER_PRICE, REGION, RESOLUTION, month_start=month_start)
    wind_series  = load_smard_series(FILTER_WIND,  REGION, RESOLUTION, month_start=month_start)

    buckets, used_price_h, used_wind_h = build_weighted_buckets_6h(
        price_series=price_series,
        wind_series=wind_series,
        month_start=month_start,
        month_end=month_end,
        cut_after=cut_after,
    )

    days_in_month = monthrange(year, month)[1]

    if mode == "current":
        est, today_index = estimate_from_buckets(buckets=buckets, now_loc=now_loc, days_in_month=days_in_month)
        return {
            "updated_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "bzn": "MW Wind Onshore",
            "tz": "Europe/Berlin",
            "month": f"{year:04d}-{month:02d}",
            "range": {
                "start_local": month_start.isoformat(),
                "end_local": month_end.isoformat(),
                "cut_after_local": now_loc.isoformat(),
            },
            "estimate_eur_mwh": round(est, 2) if est is not None else None,
            "official_eur_mwh": None,
            "official_available": False,
            "official_checked_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "today_index": today_index,
            "debug": {
                "price_points_loaded": len(price_series),
                "wind_points_loaded": len(wind_series),
                "hours_aligned_used": min(used_price_h, used_wind_h),
                "bucket_count": len(buckets),
                "filters": {"price": FILTER_PRICE, "wind": FILTER_WIND, "region": REGION, "resolution": RESOLUTION},
            },
            "series_6h": [{"label": b.label, "value": round(b.value, 2)} for b in buckets if b.value == b.value],
        }

    # prev month: Monatsmittel direkt aus allen Buckets (gewichtet pro 6h-Block bereits)
    mean_month = safe_avg([b.value for b in buckets])
    return {
        "updated_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "bzn": "MW Wind Onshore",
        "tz": "Europe/Berlin",
        "month": f"{year:04d}-{month:02d}",
        "range": {
            "start_local": month_start.isoformat(),
            "end_local": month_end.isoformat(),
            "cut_after_local": None,
        },
        "estimate_eur_mwh": round(mean_month, 2) if mean_month is not None else None,
        "official_eur_mwh": None,
        "official_available": False,
        "official_checked_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "today_index": None,
        "debug": {
            "price_points_loaded": len(price_series),
            "wind_points_loaded": len(wind_series),
            "hours_aligned_used": min(used_price_h, used_wind_h),
            "bucket_count": len(buckets),
            "filters": {"price": FILTER_PRICE, "wind": FILTER_WIND, "region": REGION, "resolution": RESOLUTION},
        },
        "series_6h": [{"label": b.label, "value": round(b.value, 2)} for b in buckets if b.value == b.value],
    }


def main() -> None:
    now = datetime.now(TZ)
    y, m = now.year, now.month
    py, pm = prev_month(y, m)

    current = build_month_payload(y, m, mode="current")
    prev = build_month_payload(py, pm, mode="prev")

    (OUT_DIR / "price_current_month.json").write_text(json.dumps(current, ensure_ascii=False, indent=2), encoding="utf-8")
    (OUT_DIR / "price_prev_month.json").write_text(json.dumps(prev, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
