"""Regression: an accepted raw Astronomy source seed must use the harness chart."""
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from sregym.service import app_workspace


class AstronomyChartSourceOverrideTest(unittest.TestCase):
    def test_raw_astronomy_source_uses_independently_pinned_harness_chart(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw_source = root / "accepted-source"
            (raw_source / "kubernetes").mkdir(parents=True)
            charts = root / "harness-applications"
            relative = Path("astronomy-shop/charts/opentelemetry-demo")
            expected = charts / relative
            expected.mkdir(parents=True)
            with patch.dict(os.environ, {"SREGYM_APP_SOURCE_DIR": str(raw_source)}), patch.object(
                app_workspace, "_target_microservices_root", return_value=charts
            ):
                self.assertEqual(app_workspace.resolve_workspace_path(relative), expected)
                self.assertTrue(app_workspace.resolve_workspace_path(relative).is_dir())

    def test_nonchart_astronomy_paths_remain_in_accepted_workspace(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch.dict(os.environ, {"SREGYM_APP_SOURCE_DIR": str(root)}):
                relative = Path("astronomy-shop/src/adservice/main.go")
                self.assertTrue(app_workspace.resolve_workspace_path(relative).is_relative_to(root))
                self.assertEqual(app_workspace.resolve_workspace_path(
                    "src/adservice/main.go"), root / "src/adservice/main.go")

    def test_other_source_override_paths_still_resolve_inside_accepted_source(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch.dict(os.environ, {"SREGYM_APP_SOURCE_DIR": str(root)}):
                self.assertEqual(app_workspace.resolve_workspace_path(
                    "hotelReservation/kubernetes"), root / "kubernetes")
                self.assertEqual(app_workspace.resolve_workspace_path("socialNetwork/helm-chart"), root / "helm-chart")


if __name__ == "__main__":
    unittest.main()
