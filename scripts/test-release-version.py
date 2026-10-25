from pathlib import Path
import os
import shutil
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).parents[0] / "release.sh"
STOP = 97  # the stubbed `make security` exits with this right after the plan


def git(cwd, *args):
    subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True)


class ReleaseVersionTests(unittest.TestCase):
    def plan(self, tags=(), branches=(), **env):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            origin, work, stubs = tmp / "origin.git", tmp / "work", tmp / "bin"
            git(tmp, "init", "-q", "--bare", str(origin))
            git(tmp, "clone", "-q", str(origin), str(work))
            (work / "scripts").mkdir()
            shutil.copy(SCRIPT, work / "scripts/release.sh")
            (work / "Cargo.toml").write_text('[package]\nname = "rush-api"\nversion = "0.1.8"\n')
            (work / "Cargo.lock").write_text('[[package]]\nname = "rush-api"\nversion = "0.1.8"\n')
            git(work, "add", ".")
            git(work, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "init")
            for tag in tags:
                git(work, "tag", tag)
            for branch in branches:
                git(work, "branch", branch)
            git(work, "push", "-q", "origin", "HEAD:refs/heads/main", *branches, "--tags")
            if tags:
                git(work, "tag", "-d", *tags)  # the script must fetch them
            stubs.mkdir()
            for tool in ["cargo", "gh", "cargo-audit", "cargo-deny", "rg", "docker", "trivy"]:
                (stubs / tool).write_text("#!/bin/sh\nexit 0\n")
            (stubs / "make").write_text(f"#!/bin/sh\nexit {STOP}\n")
            for stub in stubs.iterdir():
                stub.chmod(0o755)
            run = subprocess.run(
                ["bash", "scripts/release.sh"], cwd=work, capture_output=True, text=True,
                env={**os.environ, "PATH": f"{stubs}:{os.environ['PATH']}", "VERSION": "", "RC": "0", **env},
            )
            if run.returncode != STOP:
                return run.returncode, run.stderr
            line = next(l for l in run.stdout.splitlines() if l.strip().startswith("New version:"))
            return run.returncode, line.split()[-1]

    def test_final_release_bumps_the_latest_stable_tag(self):
        self.assertEqual(self.plan(["v0.1.8"]), (STOP, "0.1.9"))
        self.assertEqual(self.plan(["v0.1.8", "v0.1.9-rc.1", "v0.1.9-rc.2"]), (STOP, "0.1.9"))

    def test_release_candidates_count_up_from_the_next_patch(self):
        self.assertEqual(self.plan(["v0.1.8"], RC="1"), (STOP, "0.1.9-rc.1"))
        self.assertEqual(self.plan(["v0.1.8", "v0.1.9-rc.1", "v0.1.9-rc.2"], RC="1"), (STOP, "0.1.9-rc.3"))
        self.assertEqual(self.plan(["v0.1.8", "v0.1.9-rc.9", "v0.1.9-rc.10"], RC="1"), (STOP, "0.1.9-rc.11"))
        # An open release PR has a branch but no tag yet.
        self.assertEqual(
            self.plan(["v0.1.8", "v0.1.9-rc.1"], ["release/v0.1.9-rc.2"], RC="1"), (STOP, "0.1.9-rc.3")
        )

    def test_release_candidate_base_can_be_chosen_but_not_already_released(self):
        self.assertEqual(self.plan(["v0.1.8", "v0.1.9-rc.4"], RC="1", VERSION="0.2.0"), (STOP, "0.2.0-rc.1"))
        code, error = self.plan(["v0.1.8"], RC="1", VERSION="0.1.8")
        self.assertEqual(code, 1)
        self.assertIn("already released", error)
        code, error = self.plan(["v0.1.8"], RC="1", VERSION="0.2.0-rc.1")
        self.assertEqual(code, 2)


if __name__ == "__main__":
    unittest.main()
