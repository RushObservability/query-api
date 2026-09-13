"""Exercise advisory reporting without a registry, scanner, or app build."""

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


class TrivySummaryTests(unittest.TestCase):
    def render(self, report, outcome="success"):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report_path = root / "report.json"
            if report is not None:
                report_path.write_text(json.dumps(report))
            summary = root / "summary.md"
            result = subprocess.run(
                ["bash", str(Path(__file__).with_name("summarize-trivy.sh")), str(report_path)],
                env={**os.environ, "GITHUB_STEP_SUMMARY": str(summary), "TRIVY_OUTCOME": outcome},
                capture_output=True,
                text=True,
                check=True,
            )
            return result.stdout, summary.read_text()

    def test_findings_warn_without_failing(self):
        output, summary = self.render({
            "SchemaVersion": 2,
            "Results": [{"Vulnerabilities": [
                {"Severity": "HIGH"}, {"Severity": "HIGH"}, {"Severity": "CRITICAL"},
            ]}],
        })
        self.assertIn("::warning", output)
        self.assertIn("| Critical | 1 |", summary)
        self.assertIn("| High | 2 |", summary)

    def test_clean_scan_has_no_warning(self):
        output, summary = self.render({"SchemaVersion": 2, "Results": [{}]})
        self.assertNotIn("::warning", output)
        self.assertIn("No fixable HIGH or CRITICAL", summary)

    def test_unavailable_scan_never_looks_clean(self):
        for report, outcome in [
            (None, "failure"),
            (None, "success"),
            ({}, "success"),
            ({"SchemaVersion": 2, "Results": []}, "failure"),
        ]:
            with self.subTest(report=report, outcome=outcome):
                output, summary = self.render(report, outcome)
                self.assertIn("::warning", output)
                self.assertIn("Vulnerability status is unknown", summary)
                self.assertNotIn("No fixable", summary)


if __name__ == "__main__":
    unittest.main()
