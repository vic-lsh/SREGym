"""Application worker counts must not scale with the evaluation host's CPUs."""
import re
import unittest
from pathlib import Path


class NginxWorkerPortabilityTest(unittest.TestCase):
    def test_all_application_nginx_configs_have_host_independent_worker_counts(self):
        applications = Path(__file__).resolve().parents[2] / "SREGym-applications"
        observed = []
        for pattern in ("*.conf", "*.tpl"):
            for path in applications.rglob(pattern):
                for count in re.findall(r"(?m)^\s*worker_processes\s+([^;]+);", path.read_text()):
                    observed.append(path)
                    self.assertTrue(count.strip().isdigit(), str(path.relative_to(applications)))
                    self.assertGreater(int(count), 0)
        self.assertTrue(observed, "application config inventory is unavailable")
