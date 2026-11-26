import math
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

UNITS = {
    "pm10": ("µg/m³", {"ug/m3": "1", "µg/m³": "1", "mg/m3": "1000"}),
    "pm2_5": ("µg/m³", {"ug/m3": "1", "µg/m³": "1", "mg/m3": "1000"}),
    "noise": ("dBA", {"dBA": "1"}),
}


def timestamp(value):
    if not isinstance(value, str):
        raise ValueError("observed_at должен быть строкой ISO 8601")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00" if value.endswith("Z") else value)
        if parsed.utcoffset() is None:
            raise ValueError
        return parsed.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
    except (ValueError, OverflowError):
        raise ValueError("observed_at должен содержать дату, время и часовой пояс") from None


def normalize(row):
    if not isinstance(row, dict):
        raise ValueError("ожидается объект измерения")
    for key in ("event_id", "station"):
        if not isinstance(row.get(key), str) or not re.fullmatch(r"[\w.:-]{1,100}", row[key]):
            raise ValueError(f"некорректный идентификатор {key}")
    revision = str(row.get("revision", 1))
    if not re.fullmatch(r"[1-9][0-9]{0,8}", revision):
        raise ValueError("revision должен быть целым числом от 1 до 999999999")
    metric, unit = row.get("metric"), row.get("unit")
    if not isinstance(metric, str) or metric not in UNITS:
        raise ValueError("неизвестная метрика")
    canonical, factors = UNITS[metric]
    if not isinstance(unit, str) or unit not in factors:
        raise ValueError("единица измерения несовместима с метрикой")
    try:
        dec = Decimal(str(row.get("value")))
        if not dec.is_finite() or dec.copy_abs() > Decimal("1e12") or (metric != "noise" and dec < 0):
            raise ValueError
        value = float(dec * Decimal(factors[unit]))
        if not math.isfinite(value) or (value == 0 and dec != 0):
            raise ValueError
    except (InvalidOperation, ValueError, OverflowError):
        raise ValueError("некорректное значение измерения") from None
    return (row["event_id"], int(revision), row["station"], metric,
            timestamp(row.get("observed_at")), value, canonical)
