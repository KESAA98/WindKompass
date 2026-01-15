#!/usr/bin/env python3
# scripts/update_prices.py
#
# Ohne Netztransparenz (keine Secrets / kein OAuth).
# Schätzung MW Wind Onshore (gewichtet) aus:
#   - Energy-Charts Day-Ahead Preis (€/MWh, stündlich)
#   - SMARD Wind Onshore Erzeugung (MW, 15-min) -> Energie (MWh)
#
# Erstellt:
# - data/price_current_month.json
# - data/price_prev_month.json
#
# JSON bleibt kompatibel zum Frontend:
# - estimate_eur_mwh
# - series_6h[].label + series_6h[].value  (€/MWh)
# - today_index
# - official_* Felder als Platzhalter (immer false/null)

import json
import urllib.request
from dataclasses import dataclass
from datetime import datetime, date, timezone, timedelta
from calendar import monthrange
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

BZN = "DE-LU"
TZ = ZoneInfo("Europe/Berlin")

OUT_DIR = Path("data")
OUT_DIR.mkdir(parents=True, exist_ok=True)

SMARD_BASE = "https://www.smard.de/app/chart_data"
SMARD_FILTER_WIND_ONSHORE = "4067"
SMARD_REGION = "DE"
SMARD_RESOLUTION = "quarterhour"


def http_get(url: str, headers: Optional[Dict[str, str]] = None, timeout: int = 60) -> bytes:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "github-actions (mw-wind-onshore estimator)",
            "Accept": "application/json",
            **(headers or {}),
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def fetch_json(url: str) -> Any:
    raw = http_get(url, timeout=90)
    return json.loads(raw.decode("utf-8"))


def iso(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def month_start_end_local(year: int, month: int) -> Tuple[datetime, datetime]:
    last = monthrange(year, month)[1]
    start = datetime(year, month, 1, 0, 0, 0, tzinfo=TZ)
    end = datetime(year, month, last, 23, 59, 59, tzinfo=TZ)
    return start, end


def prev_month(year: int, month: int) -> Tuple[int, int]:
    if month == 1:
        return year - 1, 12
    return year, month - 1


def safe_float(x: Any) -> Optional[float]:
    try:
        v = float(x)
        if v != v:
            return None
        return v
    except Exception:
        return None


# ---------- Energy-Charts price (hourly, €/MWh) ----------

def fetch_energy_charts_prices_eur_mwh_by_hour_utc(
    month_start_local: datetime,
    month_end_local: datetime
) -> Dict[datetime, float]:
    url = (
        f"https://api.energy-charts.info/price"
        f"?bzn={BZN}&start={iso(month_start_local.date())}&end={iso(month_end_local.date())}"
    )
    j = fetch_json(url)

    unix_seconds = j.get("unix_seconds") or []
    prices = j.get("price") or []
    if not isinstance(unix_seconds, list) or not isinstance(prices, list):
        return {}

    out: Dict[datetime, float] = {}
    for u, p in zip(unix_seconds, prices):
        try:
            uu = int(u)
        except Exception:
            continue
        pv = safe_float(p)
        if pv is None:
            continue

        dt_utc = datetime.fromtimestamp(uu, tz=timezone.utc).replace(minute=0, second=0, microsecond=0)
        out[dt_utc] = pv

    return out


# ---------- SMARD wind onshore (15-min MW) -> hourly MWh ----------

def smard_get_index_timestamps_ms() -> List[int]:
    """
    SMARD liefert je nach Endpoint-Variante:
      - {"timestamps": {"0-99":[...], "100-199":[...], ...}}
      - {"timestamps":[...]}
      - oder direkt [...]
    """
    url = f"{SMARD_BASE}/{SMARD_FILTER_WIND_ONSHORE}/{SMARD_REGION}/index_{SMARD_RESOLUTION}.json"
    j = fetch_json(url)

    ts: List[int] = []

    # Variante A: {"timestamps": {"0-99":[...], ...}}
    if isinstance(j, dict) and isinstance(j.get("timestamps"), dict):
        for v in j["timestamps"].values():
            if isinstance(v, list):
                ts.extend(v)

    # Variante B: {"timestamps":[...]}
    elif isinstance(j, dict) and isinstance(j.get("timestamps"), list):
        ts = j["timestamps"]

    # Variante C: direkt [...]
    elif isinstance(j, list):
        ts = j

    return sorted(int(x) for x in ts if isinstance(x, (int, float)))


def smard_fetch_timeseries_from_timestamp_ms(ts_ms: int) -> List[Tuple[int, Optional[float]]]:
    url = (
        f"{SMARD_BASE}/{SMARD_FILTER_WIND_ONSHORE}/{SMARD_REGION}/"
        f"{SMARD_FILTER_WIND_ONSHORE}_{SMARD_REGION}_{SMARD_RESOLUTION}_{ts_ms}.json"
    )
    j = fetch_json(url)
    series = j.get("series") if isinstance(j, dict) else None
    if not isinstance(series, list):
        return []

    out: List[Tuple[int, Optional[float]]] = []
    for item in series:
        if not isinstance(item, list) or len(item) < 2:
            continue
        try:
            t_ms = int(item[0])
        except Exception:
            continue
        out.append((t_ms, safe_float(item[1])))
    return out


def smard_wind_onshore_energy_mwh_by_hour_utc(start_utc: datetime, end_utc: datetime) -> Dict[datetime, float]:
    """
    Robust:
    - SMARD hat mehrere Chunk-Files (Index).
    - Wir laden: (Chunk direkt vor start) + (alle Chunks im Zeitraum) + (notfalls der letzte <= end).
    - Viertelstunde MW -> MWh pro Viertelstunde: MW * 0.25, dann stündlich aggregieren.
    """
    idx = smard_get_index_timestamps_ms()
    if not idx:
        return {}

    start_ms = int(start_utc.timestamp() * 1000)
    end_ms = int(end_utc.timestamp() * 1000)

    in_range = [t for t in idx if start_ms <= t <= end_ms]
    prev = max((t for t in idx if t < start_ms), default=None)
    last_le_end = max((t for t in idx if t <= end_ms), default=None)

    ts_list: List[int] = []
    if prev is not None:
        ts_list.append(prev)
    ts_list.extend(in_range)

    if not ts_list and last_le_end is not None:
        ts_list = [last_le_end]

    ts_list = sorted(set(ts_list))

    out: Dict[datetime, float] = {}

    for ts0 in ts_list:
        series = smard_fetch_timeseries_from_timestamp_ms(ts0)
        if not series:
            continue

        for t_ms, mw in series:
            if mw is None:
                continue
            if t_ms < start_ms or t_ms >= end_ms:
                continue

            dt_utc = datetime.fromtimestamp(t_ms / 1000.0, tz=timezone.utc)
            hour_start = dt_utc.replace(minute=0, second=0, microsecond=0)

            e_mwh = mw * 0.25
            out[hour_start] = out.get(hour_start, 0.0) + e_mwh

    return out


# ---------- Weighted aggregation + 6h buckets (Berlin) ----------

@dataclass
class Bucket:
    ts: int
    label: str
    value: float  # €/MWh
    day_key: str


def weighted_month_estimate_eur_mwh(
    price_by_hour_utc: Dict[datetime, float],
    energy_mwh_by_hour_utc: Dict[datetime, float],
    start_utc: datetime,
    end_utc: datetime,
) -> Optional[float]:
    wsum = 0.0
    esum = 0.0
    for h, price in price_by_hour_utc.items():
        if h < start_utc or h >= end_utc:
            continue
        e = energy_mwh_by_hour_utc.get(h)
        if e is None or e <= 0:
            continue
        wsum += price * e
        esum += e
    if esum <= 0:
        return None
    return wsum / esum


def group_weighted_to_6h_berlin(
    price_by_hour_utc: Dict[datetime, float],
    energy_mwh_by_hour_utc: Dict[datetime, float],
    month_start_local: datetime,
    month_end_local: datetime,
    cut_after_local: Optional[datetime] = None,
) -> List[Bucket]:
    buckets: Dict[Tuple[int, int, int, int], Tuple[float, float, datetime]] = {}

    for hour_utc, price in price_by_hour_utc.items():
        e = energy_mwh_by_hour_utc.get(hour_utc)
        if e is None or e <= 0:
            continue

        dt_loc = hour_utc.astimezone(TZ)
        if dt_loc < month_start_local or dt_loc > month_end_local:
            continue
        if cut_after_local is not None and dt_loc > cut_after_local:
            continue

        block = dt_loc.hour // 6
        key = (dt_loc.year, dt_loc.month, dt_loc.day, block)

        wsum, esum, rep = buckets.get(key, (0.0, 0.0, dt_loc))
        buckets[key] = (wsum + price * e, esum + e, rep)

    out: List[Bucket] = []
    for (y, m, d, block), (wsum, esum, rep) in buckets.items():
        if esum <= 0:
            continue
        hh = block * 6
        label = f"{d:02d}.{m:02d} {hh:02d}:00"
        day_key = f"{d:02d}.{m:02d}"
        out.append(Bucket(ts=int(rep.timestamp()), label=label, value=wsum / esum, day_key=day_key))

    out.sort(key=lambda x: x.ts)
    return out


def build_month_payload(year: int, month: int, mode: str) -> Dict[str, Any]:
    month_start_local, month_end_local = month_start_end_local(year, month)

    start_utc = month_start_local.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    end_utc_full = (month_end_local.astimezone(timezone.utc) + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    now_local = datetime.now(TZ)
    cut_after_local = now_local if mode == "current" else None

    end_utc = end_utc_full
    if mode == "current":
        end_utc = min(end_utc_full, now_local.astimezone(timezone.utc))

    price_by_hour = fetch_energy_charts_prices_eur_mwh_by_hour_utc(month_start_local, month_end_local)
    energy_by_hour = smard_wind_onshore_energy_mwh_by_hour_utc(start_utc, end_utc)

    est = weighted_month_estimate_eur_mwh(price_by_hour, energy_by_hour, start_utc, end_utc)

    buckets = group_weighted_to_6h_berlin(
        price_by_hour_utc=price_by_hour,
        energy_mwh_by_hour_utc=energy_by_hour,
        month_start_local=month_start_local,
        month_end_local=month_end_local,
        cut_after_local=cut_after_local,
    )

    days_in_month = monthrange(year, month)[1]
    today_index = max(0, min(days_in_month - 1, (now_local.day - 1)))

    return {
        "updated_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "bzn": "MW Wind Onshore",
        "tz": "Europe/Berlin",
        "month": f"{year:04d}-{month:02d}",
        "range": {
            "start_local": month_start_local.isoformat(),
            "end_local": month_end_local.isoformat(),
            "cut_after_local": now_local.isoformat() if mode == "current" else None,
        },
        "estimate_eur_mwh": round(est, 2) if est is not None else None,

        # placeholders
        "official_eur_mwh": None,
        "official_available": False,
        "official_checked_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),

        "today_index": int(today_index),

        "debug": {
            "price_hours": len(price_by_hour),
            "wind_hours": len(energy_by_hour),
            "bucket_count": len(buckets),
        },

        "series_6h": [{"label": b.label, "value": round(b.value, 2)} for b in buckets],
    }


def main() -> None:
    now = datetime.now(TZ)
    y, m = now.year, now.month
    py, pm = prev_month(y, m)

    current = build_month_payload(y, m, mode="current")
    prev = build_month_payload(py, pm, mode="prev")

    (OUT_DIR / "price_current_month.json").write_text(
        json.dumps(current, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (OUT_DIR / "price_prev_month.json").write_text(
        json.dumps(prev, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
