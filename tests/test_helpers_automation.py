"""Tests for data_platform.helpers.automation — apply_automation."""

from unittest.mock import MagicMock

from dagster import AssetSpec

from data_platform.helpers.automation import (
    AUTOMATION_CONDITION,
    _is_manual,
    apply_automation,
)


def _make_asset_def(tags: dict | None = None):
    """Create a minimal mock AssetsDefinition with the given tags."""
    spec = MagicMock(spec=AssetSpec)
    spec.tags = tags or {}

    asset_def = MagicMock()
    asset_def.specs = [spec]

    updated = MagicMock()
    asset_def.with_attributes.return_value = updated

    return asset_def, updated


class TestIsManual:
    def test_manual_when_tagged(self):
        asset_def, _ = _make_asset_def(tags={"manual": "true"})
        assert _is_manual(asset_def) is True

    def test_not_manual_when_no_tags(self):
        asset_def, _ = _make_asset_def(tags={})
        assert _is_manual(asset_def) is False

    def test_not_manual_when_other_tags(self):
        asset_def, _ = _make_asset_def(tags={"owner": "team-data"})
        assert _is_manual(asset_def) is False


class TestApplyAutomation:
    def test_manual_asset_unchanged(self):
        asset_def, _ = _make_asset_def(tags={"manual": "true"})
        result = apply_automation([asset_def])
        assert result == [asset_def]
        asset_def.with_attributes.assert_not_called()

    def test_non_manual_asset_gets_condition(self):
        asset_def, updated = _make_asset_def(tags={})
        result = apply_automation([asset_def])
        assert result == [updated]
        asset_def.with_attributes.assert_called_once_with(
            automation_condition=AUTOMATION_CONDITION
        )

    def test_empty_list(self):
        assert apply_automation([]) == []

    def test_mixed_assets(self):
        manual, _ = _make_asset_def(tags={"manual": "true"})
        auto, auto_updated = _make_asset_def(tags={})
        result = apply_automation([manual, auto])
        assert result == [manual, auto_updated]
