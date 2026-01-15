#!/usr/bin/env python3
# scripts/update_prices.py
#
# Ziel:
# - data/price_current_month.json  -> MW Wind Onshore (ct/kWh) geschätzt (gewichtet: Spot * WindOnshore Energie)
#                                    + sobald verfügbar: official (Monatsmarktwert aus Netztransparenz)
# - data/price_prev_month.json     -> MW Wind Onshore (ct/kWh) geschätzt + optional official (wenn schon verfügbar)
#
# Quellen:
# - Netztransparenz Daten-API: Spotmarktpreise (Format 15, ct/kWh)  https://ds.netztransparenz.de/api/v1/data/Spotmarktpreise
# - Netztransparenz Daten-API: Marktpraemie (Monatsmarktwerte, Format 12) https://ds.netztransparenz.de/api/v1/data/Marktpraemie
# - SMARD: Wind Onshore Erzeugung (Filter 4067), quarterhour
#
# OAuth:
# Netztransparenz v1.14: client_credentials -> https://identity.netztransparenz.de/users/connect/token
#
# Hinweis zu Einheiten:
# - Spot: ct/kWh
# - SMARD: MW (Leistung) in 15-min Schritten -> Energie je Viertelstunde = MW * 0.25 (MWh)
# - gewichteter Preis: sum(price_ctkwh * energy_MWh) / sum(energy_MWh)  (Faktor 1000 kWh/MWh kürzt sich raus)
#
# Ausgabe enthält ct/kWh (primär) + EUR/MWh als Komfort.

import json
import os
import urllib.parse
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

NT_BASE = "https://ds.netztransparenz.de/api/v1"
NT_TOKEN_URL = "https://identity.netztransparenz.de/users/connect/token"

SMARD_BASE = "https://www.smard.de/app/chart_data"
SMARD_FILTER_WIND_ONSHORE = "4067"
SMARD_REGION = "DE"
SMARD_RESOLUTION = "quarterhour"


def iso_date(d: date) -> str:
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


def eur_mwh_to_ct_kwh(x: float) -> float:
    # 1 EUR/MWh = 0.1 ct/kWh
    return x / 10.0


def ct_kwh_to_eur_mwh(x: float) -> float:
    # 1 ct/kWh = 10 EUR/MWh
    return x * 10.0


def http_get(url: str, headers: Optional[Dict[str, str]] = None, timeout: int = 45) -> bytes:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "github-actions (mw-wind-onshore updater)",
            **(headers or {}),
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def http_post_form(url: str, data: Dict[str, str], headers: Optional[Dict[str, str]] = None, timeout: int = 45) -> bytes:
    body = urllib.parse.urlencode(data).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "User-Agent": "github-actions (mw-wind-onshore updater)",
            "Content-Type": "application/x-www-form-urlencoded",
            **(headers or {}),
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def parse_csv_semicolon(data: str) -> List[List[str]]:
    # Robust: trim, split lines, then split by ';'
    rows: List[List[str]] = []
    for line in data.splitlines():
        line = line.strip()
        if not line:
            continue
        rows.append([c.strip() for c in line.split(";")])
    return rows


def parse_de_decimal(s: str) -> Optional[float]:
    # "25,791" -> 25.791 ; empty -> None
    s = (s or "").strip()
    if not s:
        return None
    s = s.replace(".", "").replace(",", ".")  # defensive
    try:
        return float(s)
    except Exception:
        return None


def nt_get_token() -> str:
    client_id = os.getenv("NT_CLIENT_ID", "").strip()
    client_secret = os.getenv("NT_CLIENT_SECRET", "").strip()
    if not client_id or not client_secret:
        raise RuntimeError(
            "Netztransparenz OAuth fehlt: bitte NT_CLIENT_ID und NT_CLIENT_SECRET als env setzen."
        )

    raw = http_post_form(
        NT_TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
        headers={"Accept": "application/json"},
        timeout=45,
    )
    j = json.loads(raw.decode("utf-8"))
    token = j.get("access_token")
    if not token:
        raise RuntimeError(f"Kein access_token in Token-Response: {j!r}")
    return token


def nt_get_csv(path: str, token: str, date_from_utc: Optional[datetime] = None, date_to_utc: Optional[datetime] = None) -> str:
    # Doku: https://ds.netztransparenz.de/api/v1/data/[datatype]/[product]/dateFrom=.../dateTo=...
    # Spotmarktpreise hat laut Endpunktliste keinen product-Teil (Format 15) -> GET api/v1/data/Spotmarktpreise
    # Wir hängen dateFrom/dateTo nur an, wenn gesetzt.
    url = f"{NT_BASE}{path}"
    if date_from_utc or date_to_utc:
        parts = [url]
        if date_from_utc:
            parts.append("/dateFrom=" + urllib.parse.quote(date_from_utc.strftime("%Y-%m-%dT%H:%M:%S")))
        if date_to_utc:
            parts.append("/dateTo=" + urllib.parse.quote(date_to_utc.strftime("%Y-%m-%dT%H:%M:%S")))
        url = "".join(parts)

    raw = http_get(
        url,
        headers={
            "Accept": "text/csv, text/plain, */*",
            "Authorization": f"Bearer {token}",
        },
        timeout=60,
    )
    # Netztransparenz CSV kommt i.d.R. in UTF-8
    return raw.decode("utf-8", errors="replace")


def fetch_spot_prices_ct_kwh_by_hour_utc(token: str, start_utc: datetime, end_utc: datetime) -> Dict[datetime, float]:
    """
    Netztransparenz Spotmarktpreise (Format 15):
      Datum; von; Zeitzone von; bis; Zeitzone bis; Spotmarktpreis in ct/kWh
    Liefert dict: hour_start_utc -> price_ct_kwh
    """
    csv_text = nt_get_csv("/data/Spotmarktpreise", token, date_from_utc=start_utc, date_to_utc=end_utc)
    rows = parse_csv_semicolon(csv_text)
    if not rows:
        return {}

    header = rows[0]
    # Heuristik: Header enthält "Spotmarktpreis" oder "ct/kWh"
    data_rows = rows[1:] if any("Spot" in h or "ct/kWh" in h for h in header) else rows

    out: Dict[datetime, float] = {}
    for r in data_rows:
        if len(r) < 6:
            continue
        d_str, from_str, tz_from, to_str, tz_to, price_str = r[0], r[1], r[2], r[3], r[4], r[5]
        # Datum in Format 22.06.2022 oder 2022-06-22 (je nach API-Variante)
        dt_date: Optional[date] = None
        for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
            try:
                dt_date = datetime.strptime(d_str.strip(), fmt).date()
                break
            except Exception:
                pass
        if not dt_date:
            continue

        # Zeit "23:00" etc.
        try:
            hh, mm = [int(x) for x in from_str.strip().split(":")[:2]]
        except Exception:
            continue

        # Zeiten sind laut Format 15 in UTC
        dt = datetime(dt_date.year, dt_date.month, dt_date.day, hh, mm, 0, tzinfo=timezone.utc)

        price = parse_de_decimal(price_str)
        if price is None:
            continue

        # nur stündliche Starts (wir akzeptieren auch :00)
        out[dt] = price

    return out


def smard_get_index_timestamps_ms() -> List[int]:
    url = f"{SMARD_BASE}/{SMARD_FILTER_WIND_ONSHORE}/{SMARD_REGION}/index_{SMARD_RESOLUTION}.json"
    raw = http_get(url, headers={"Accept": "application/json"}, timeout=60)
    j = json.loads(raw.decode("utf-8"))
    # Erwartet: array of timestamps (ms)
    return [int(x) for x in j if isinstance(x, (int, float))]


def smard_fetch_timeseries_from_timestamp_ms(ts_ms: int) -> List[Tuple[int, Optional[float]]]:
    # URL: .../{filter}/{region}/{filterCopy}_{regionCopy}_{resolution}_{timestamp}.json
    url = f"{SMARD_BASE}/{SMARD_FILTER_WIND_ONSHORE}/{SMARD_REGION}/{SMARD_FILTER_WIND_ONSHORE}_{SMARD_REGION}_{SMARD_RESOLUTION}_{ts_ms}.json"
    raw = http_get(url, headers={"Accept": "application/json"}, timeout=90)
    j = json.loads(raw.decode("utf-8"))
    series = j.get("series") if isinstance(j, dict) else None
    if not isinstance(series, list):
        return []
    out: List[Tuple[int, Optional[float]]] = []
    for item in series:
        # item: [timestamp_ms, value]
        if not isinstance(item, list) or len(item) < 2:
            continue
        t = item[0]
        v = item[1]
        try:
            t_ms = int(t)
        except Exception:
            continue
        val: Optional[float]
        if v is None:
            val = None
        else:
            try:
                val = float(v)
            except Exception:
                val = None
        out.append((t_ms, val))
    return out


def smard_wind_onshore_energy_mwh_by_hour_utc(start_utc: datetime, end_utc: datetime) -> Dict[datetime, float]:
    """
    Aggregiert SMARD quarterhour MW zu Energie pro Stunde (MWh) keyed by hour_start_utc.
    """
    idx = smard_get_index_timestamps_ms()
    if not idx:
        return {}

    # wir brauchen einen Start-Timestamp <= start_utc
    start_ms = int(start_utc.timestamp() * 1000)
    candidates = [t for t in idx if t <= start_ms]
    ts0 = max(candidates) if candidates else min(idx)

    series = smard_fetch_timeseries_from_timestamp_ms(ts0)
    if not series:
        return {}

    out: Dict[datetime, float] = {}

    for t_ms, mw in series:
        if mw is None:
            continue
        dt = datetime.fromtimestamp(t_ms / 1000.0, tz=timezone.utc)
        if dt < start_utc or dt >= end_utc:
            continue

        # Viertelstunde -> Energie in MWh
        e_mwh = mw * 0.25

        hour_start = dt.replace(minute=0, second=0, microsecond=0)
        out[hour_start] = out.get(hour_start, 0.0) + e_mwh

    return out


@dataclass
class Bucket:
    ts: int        # epoch seconds (Berlin rep)
    label: str     # "dd.mm HH:00"
    value: float   # bucket avg (ct/kWh)
    day_key: str   # "dd.mm"


def group_weighted_to_6h_berlin(
    hour_prices_ctkwh: Dict[datetime, float],
    hour_energy_mwh: Dict[datetime, float],
    month_start_local: datetime,
    month_end_local: datetime,
    cut_after_local: Optional[datetime] = None,
) -> List[Bucket]:
    """
    Bildet 6h-Buckets in Berlin-Zeit (00/06/12/18) als gewichteten Durchschnitt (Gewicht = Energie).
    """
    buckets: Dict[Tuple[int, int, int, int], Tuple[float, float, datetime]] = {}
    # key=(Y,M,D,block) -> (sum(price*energy), sum(energy), rep_dt_local)

    for hour_utc, price in hour_prices_ctkwh.items():
        energy = hour_energy_mwh.get(hour_utc)
        if energy is None or energy <= 0:
            continue

        dt_loc = hour_utc.astimezone(TZ)

        if dt_loc < month_start_local or dt_loc > month_end_local:
            continue
        if cut_after_local is not None and dt_loc > cut_after_local:
            continue

        block = dt_loc.hour // 6
        key = (dt_loc.year, dt_loc.month, dt_loc.day, block)

        wsum, esum, rep = buckets.get(key, (0.0, 0.0, dt_loc))
        buckets[key] = (wsum + price * energy, esum + energy, rep)

    out: List[Bucket] = []
    for (y, m, d, block), (wsum, esum, rep) in buckets.items():
        if esum <= 0:
            continue
        hh = block * 6
        label = f"{d:02d}.{m:02d} {hh:02d}:00"
        day_key = f"{d:02d}.{m:02d}"
        out.append(
            Bucket(
                ts=int(rep.timestamp()),
                label=label,
                value=wsum / esum,
                day_key=day_key,
            )
        )

    out.sort(key=lambda x: x.ts)
    return out


def weighted_month_estimate_ctkwh(
    hour_prices_ctkwh: Dict[datetime, float],
    hour_energy_mwh: Dict[datetime, float],
    start_utc: datetime,
    end_utc: datetime,
) -> Optional[float]:
    wsum = 0.0
    esum = 0.0
    for h, price in hour_prices_ctkwh.items():
        if h < start_utc or h >= end_utc:
            continue
        e = hour_energy_mwh.get(h)
        if e is None or e <= 0:
            continue
        wsum += price * e
        esum += e
    if esum <= 0:
        return None
    return wsum / esum


def try_fetch_official_mw_wind_onshore_ctkwh(token: str, year: int, month: int) -> Tuple[Optional[float], bool]:
    """
    Versucht, Monatsmarktwert MW Wind Onshore (ct/kWh) aus Marktpraemie (Format 12) zu holen.
    Gibt (value, available) zurück.
    """
    # wir fragen den Zeitraum großzügig um den Monat herum ab
    month_start_local, month_end_local = month_start_end_local(year, month)
    start_utc = month_start_local.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    end_utc = (month_end_local.astimezone(timezone.utc) + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

    try:
        csv_text = nt_get_csv("/data/Marktpraemie", token, date_from_utc=start_utc, date_to_utc=end_utc)
    except Exception:
        # falls der Endpunkt temporär anders heißt/gesperrt ist: sauber als "nicht verfügbar"
        return None, False

    rows = parse_csv_semicolon(csv_text)
    if not rows:
        return None, False

    header = rows[0]
    data_rows = rows[1:] if any("MW Wind Onshore" in h for h in header) else rows

    # Format 12 Beispiel: "1/2020;3,503;3,503;..."
    target = f"{month}/{year}"
    # manchmal "01/2020" -> tolerieren
    target2 = f"{month:02d}/{year}"

    # Spalte suchen
    idx_col = None
    for i, h in enumerate(header):
        if "MW Wind Onshore" in h:
            idx_col = i
            break
    if idx_col is None:
        # fallback: zweite Spalte nach "MW-EPEX"? -> laut Format: Monat;MW-EPEX;MW Wind Onshore;...
        idx_col = 2 if len(header) >= 3 else None

    if idx_col is None:
        return None, False

    for r in data_rows:
        if len(r) <= idx_col:
            continue
        mon = (r[0] or "").strip()
        if mon == target or mon == target2:
            val = parse_de_decimal(r[idx_col])
            if val is None:
                return None, False
            return val, True

    return None, False


# timedelta gebraucht in official fetch
from datetime import timedelta


def build_month_payload(year: int, month: int, mode: str) -> Dict[str, Any]:
    """
    mode:
      - "current": bis jetzt (Berlin) + estimate (ct/kWh) + optional official (wenn schon da)
      - "prev": kompletter Vormonat + estimate + optional official
    """
    month_start_local, month_end_local = month_start_end_local(year, month)

    # für stündliche Keys arbeiten wir in UTC (hour_start_utc)
    start_utc = month_start_local.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    end_utc = (month_end_local.astimezone(timezone.utc) + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

    now_local = datetime.now(TZ)
    cut_after_local = now_local if mode == "current" else None
    if mode == "current":
        # end_utc auf "jetzt" begrenzen (stündlich)
        end_utc = min(end_utc, now_local.astimezone(timezone.utc))

    # Netztransparenz + SMARD holen
    token = nt_get_token()
    spot_by_hour = fetch_spot_prices_ct_kwh_by_hour_utc(token, start_utc, end_utc)
    wind_energy_by_hour = smard_wind_onshore_energy_mwh_by_hour_utc(start_utc, end_utc)

    est_ctkwh = weighted_month_estimate_ctkwh(spot_by_hour, wind_energy_by_hour, start_utc, end_utc)

    # 6h-Serie (gewichtete Bucket-Preise)
    buckets = group_weighted_to_6h_berlin(
        hour_prices_ctkwh=spot_by_hour,
        hour_energy_mwh=wind_energy_by_hour,
        month_start_local=month_start_local,
        month_end_local=month_end_local,
        cut_after_local=cut_after_local,
    )

    # official versuchen (auch für current – kann gegen Monatsende schon verfügbar sein)
    official_ctkwh, official_avail = try_fetch_official_mw_wind_onshore_ctkwh(token, year, month)

    payload: Dict[str, Any] = {
        "updated_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "tz": "Europe/Berlin",
        "month": f"{year:04d}-{month:02d}",
        "range": {
            "start_local": month_start_local.isoformat(),
            "end_local": month_end_local.isoformat(),
            "cut_after_local": now_local.isoformat() if mode == "current" else None,
        },

        # Schätzung (immer da, solange genug Daten)
        "estimate_ct_kwh": round(est_ctkwh, 4) if est_ctkwh is not None else None,
        "estimate_eur_mwh": round(ct_kwh_to_eur_mwh(est_ctkwh), 2) if est_ctkwh is not None else None,

        # Official (wenn verfügbar)
        "official_ct_kwh": round(official_ctkwh, 4) if official_ctkwh is not None else None,
        "official_eur_mwh": round(ct_kwh_to_eur_mwh(official_ctkwh), 2) if official_ctkwh is not None else None,
        "official_available": bool(official_avail),
        "official_checked_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),

        "debug": {
            "spot_hours": len(spot_by_hour),
            "wind_hours": len(wind_energy_by_hour),
            "bucket_count": len(buckets),
        },

        # 6h Buckets als ct/kWh (geschätzt, gewichtet)
        "series_6h": [{"label": b.label, "value_ct_kwh": round(b.value, 4)} for b in buckets],
    }

    return payload


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
