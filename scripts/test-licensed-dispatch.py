from pathlib import Path
import unittest


class LicensedDispatchTests(unittest.TestCase):
    def test_dispatch_requires_successful_release_and_uses_shared_scoped_credentials(self):
        workflow = (Path(__file__).parents[1] / ".github/workflows/release.yml").read_text()
        job = workflow.split("  dispatch-licensed-build:", 1)[1]
        for expected in [
            "needs: [security-gate, build-and-push]", "if: github.ref == 'refs/heads/main'",
            "secrets.LICENSE_BUILDER_APP_CLIENT_ID", "secrets.LICENSE_BUILDER_APP_PRIVATE_KEY",
            "repositories: query-api-license", "permission-actions: write",
            "/repos/RushObservability/query-api-license/actions/workflows/release.yml/dispatches",
            "needs.security-gate.outputs.version", "needs.security-gate.outputs.tag",
            "needs.build-and-push.outputs.image_digest", "inputs[query_api_sha]",
        ]:
            self.assertIn(expected, job)
        self.assertNotIn("always()", job)
        self.assertIn("image_digest: ${{ steps.push.outputs.digest }}", workflow)


if __name__ == "__main__":
    unittest.main()
