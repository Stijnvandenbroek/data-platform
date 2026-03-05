"""Data formatting and serialisation helpers."""

import json


def safe(val):
    """Convert non-serialisable values for JSONB storage."""
    if isinstance(val, list | dict | tuple):
        return json.dumps(val, default=str)
    return val


def safe_int(val):
    """Try to cast to int, return None on failure."""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        try:
            return int(float(val))
        except (ValueError, TypeError):
            return None


def md_preview_table(
    rows: list[dict],
    columns: list[tuple[str, str]],
    formatters: dict[str, callable] | None = None,
) -> str:
    """Build a markdown table from a list of row dicts."""
    formatters = formatters or {}
    headers = [label for _, label in columns]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for r in rows:
        cells = []
        for key, _ in columns:
            val = r.get(key)
            if key in formatters:
                cells.append(formatters[key](val))
            else:
                cells.append(str(val) if val is not None else "–")
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def format_euro(val) -> str:
    """Format an integer as €-prefixed, or '–'."""
    return f"€{val:,}" if val else "–"


def format_area(val) -> str:
    """Format an integer as m², or '–'."""
    return f"{val} m²" if val else "–"
