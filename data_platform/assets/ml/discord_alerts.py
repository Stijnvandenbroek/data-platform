"""Discord notification asset for high-ELO listings."""

from pathlib import Path

import pandas as pd
import requests
from dagster import (
    AssetExecutionContext,
    Config,
    MaterializeResult,
    MetadataValue,
    asset,
)
from sqlalchemy import text

from data_platform.helpers import format_euro, format_area, render_sql
from data_platform.resources import DiscordResource, PostgresResource

_SQL_DIR = Path(__file__).parent / "sql"


class DiscordNotificationConfig(Config):
    """Configuration for Discord ELO notifications."""

    min_elo: float = 1600


def _build_embed(row) -> dict:
    """Build a Discord smart-card embed for a single listing."""
    # Address
    address_parts = [p for p in [row.postcode, row.city, row.neighbourhood] if p]
    address = ", ".join(address_parts) if address_parts else "–"

    # Property type
    prop_parts = [p for p in [row.object_type, row.house_type] if p]
    prop_type = " · ".join(prop_parts) if prop_parts else None

    # Description
    lines = [f"📍 **{address}**"]
    if prop_type:
        year = f" ({row.construction_year})" if row.construction_year else ""
        lines.append(f"🏠 {prop_type}{year}")
    if getattr(row, "broker_name", None):
        lines.append(f"🏢 {row.broker_name}")
    description = "\n".join(lines)

    # Stats fields
    fields = [
        {"name": "💰 Price", "value": format_euro(row.current_price), "inline": True},
        {"name": "⭐ ELO Score", "value": f"{row.predicted_elo:.0f}", "inline": True},
        {
            "name": "📐 Living area",
            "value": format_area(row.living_area),
            "inline": True,
        },
    ]
    if row.price_per_sqm:
        fields.append(
            {"name": "€/m²", "value": format_euro(row.price_per_sqm), "inline": True}
        )
    fields.append(
        {
            "name": "🛏️ Rooms",
            "value": f"{row.bedrooms or '–'} bed / {row.rooms or '–'} total",
            "inline": True,
        }
    )
    if row.energy_label:
        fields.append({"name": "⚡ Energy", "value": row.energy_label, "inline": True})

    # Embed
    return {
        "title": row.title or row.global_id,
        "url": row.url,
        "description": description,
        "color": 0x00B894,  # green
        "fields": fields,
        "footer": {
            "text": f"Funda · {row.offering_type or 'Listing'}"
            if row.offering_type
            else "Funda"
        },
    }


@asset(
    deps=["elo_inference"],
    group_name="ml",
    kinds={"python", "discord"},
    description=(
        "Send a Discord notification for newly scored listings whose "
        "predicted ELO exceeds a configurable threshold."
    ),
)
def listing_alert(
    context: AssetExecutionContext,
    config: DiscordNotificationConfig,
    postgres: PostgresResource,
    discord: DiscordResource,
) -> MaterializeResult:
    engine = postgres.get_engine()

    with engine.begin() as conn:
        conn.execute(text(render_sql(_SQL_DIR, "ensure_elo_schema.sql")))
        conn.execute(text(render_sql(_SQL_DIR, "ensure_notified_table.sql")))

    query = render_sql(_SQL_DIR, "select_top_predictions.sql")
    df = pd.read_sql(
        text(query),
        engine,
        params={"min_elo": config.min_elo},
    )
    context.log.info(f"Found {len(df)} listings above ELO threshold {config.min_elo}.")

    if df.empty:
        return MaterializeResult(
            metadata={
                "notified": 0,
                "status": MetadataValue.text("No listings above threshold."),
            }
        )

    # Send in batches of 10 (Discord limit)
    webhook_url = discord.get_webhook_url()
    batch_size = 10
    sent = 0

    for i in range(0, len(df), batch_size):
        batch = df.iloc[i : i + batch_size]
        embeds = [_build_embed(row) for row in batch.itertuples()]
        payload = {
            "username": "ELO Scout",
            "content": (
                f"**{len(embeds)} listing(s) scored above ELO {config.min_elo:.0f}**"
                if i == 0
                else None
            ),
            "embeds": embeds,
        }
        resp = requests.post(webhook_url, json=payload, timeout=15)
        resp.raise_for_status()
        sent += len(embeds)

    # Mark as notified to avoid duplicates
    insert_notified = render_sql(_SQL_DIR, "insert_notified.sql")
    notified_rows = [{"global_id": gid} for gid in df["global_id"]]
    with engine.begin() as conn:
        conn.execute(text(insert_notified), notified_rows)

    context.log.info(f"Sent {sent} notification(s) to Discord.")

    return MaterializeResult(
        metadata={
            "notified": sent,
            "min_elo_threshold": MetadataValue.float(float(config.min_elo)),
        }
    )
