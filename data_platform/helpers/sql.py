"""SQL rendering utilities."""

from pathlib import Path

from jinja2 import Environment, FileSystemLoader


def render_sql(sql_dir: Path, path: str, **kwargs: object) -> str:
    """Load and render a Jinja2 SQL template relative to sql_dir."""
    env = Environment(loader=FileSystemLoader(str(sql_dir)), autoescape=False)
    return env.get_template(path).render(**kwargs)
