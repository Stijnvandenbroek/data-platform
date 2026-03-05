"""Global automation condition for all assets.

Applies eager auto-materialization to every asset except those tagged "manual".
Also prevents duplicate runs by skipping assets that have any dependencies
currently in progress.
"""

from collections.abc import Iterable

from dagster import AssetsDefinition, AutomationCondition

# Eager: materialize when any dependency updates, but skip when any upstream
# dependency is still in progress to avoid wasted / duplicate runs.
AUTOMATION_CONDITION = (
    AutomationCondition.eager() & ~AutomationCondition.any_deps_in_progress()
)


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
