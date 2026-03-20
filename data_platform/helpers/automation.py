"""Global automation condition for all assets.

Applies eager auto-materialization to every asset except those tagged "manual".
Also prevents duplicate runs by skipping assets that have any dependencies
currently in progress.
"""

from collections.abc import Iterable

from dagster import AssetsDefinition, AutomationCondition

_MAX_ANCESTOR_DEPTH = 3


def _any_ancestor_in_progress(
    max_depth: int = _MAX_ANCESTOR_DEPTH,
) -> AutomationCondition:
    """True if any transitive ancestor up to *max_depth* hops away is in progress."""
    inner = AutomationCondition.in_progress()
    result = AutomationCondition.any_deps_match(inner)  # depth 1 (parent)
    for _ in range(max_depth - 1):
        inner = AutomationCondition.any_deps_match(inner)
        result = result | inner
    return result


# Eager: materialize when any dependency updates, but skip when any ancestor
# anywhere in the transitive dependency graph is still in progress.
AUTOMATION_CONDITION = AutomationCondition.eager() & ~_any_ancestor_in_progress()


def apply_automation(
    assets: Iterable[AssetsDefinition],
) -> list[AssetsDefinition]:
    """Return a new list with the global automation condition applied.

    Assets (or individual specs inside multi-asset groups) tagged ``"manual"``
    are left untouched and will only run when triggered explicitly.
    """
    result: list[AssetsDefinition] = []
    for asset_def in assets:
        if _is_manual(asset_def):
            result.append(asset_def)
        else:
            result.append(
                asset_def.with_attributes(automation_condition=AUTOMATION_CONDITION)
            )
    return result


def _is_manual(asset_def: AssetsDefinition) -> bool:
    """Check whether *any* spec in the asset definition carries a ``manual`` tag."""
    for spec in asset_def.specs:
        if spec.tags.get("manual"):
            return True
    return False
