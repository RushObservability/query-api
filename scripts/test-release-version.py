from pathlib import Path
import os
import shutil
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).parents[0] / "release.sh"
STOP = 97  # the stubbed `make security` exits with this right after the plan
# Records each docker call and the Cargo files of the build context it was given.
DOCKER_STUB = """#!/bin/sh
echo "docker $*" >> "$DOCKER_LOG"
for context; do :; done
[ -f "$context/Cargo.toml" ] && grep -h '^version' "$context/Cargo.toml" "$context/Cargo.lock" >> "$DOCKER_LOG"
exit 0
"""


def git(cwd, *args):
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout


class ReleaseVersionTests(unittest.TestCase):
    def run_release(self, tmp, tags=(), publish=False, **env):
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
        git(work, "push", "-q", "origin", "HEAD:refs/heads/main", "--tags")
        if tags:
            git(work, "tag", "-d", *tags)  # the script must fetch them
        stubs.mkdir()
        for tool in ["cargo", "gh", "cargo-audit", "cargo-deny", "rg", "trivy"]:
            (stubs / tool).write_text("#!/bin/sh\nexit 0\n")
        (stubs / "docker").write_text(DOCKER_STUB)
        (stubs / "make").write_text(f"#!/bin/sh\nexit {0 if publish else STOP}\n")
        for stub in stubs.iterdir():
            stub.chmod(0o755)
        return subprocess.run(
            ["bash", "scripts/release.sh"], cwd=work, capture_output=True, text=True,
            env={**os.environ, "PATH": f"{stubs}:{os.environ['PATH']}", "DOCKER_LOG": str(tmp / "docker.log"),
                 "VERSION": "", "RC": "0", "PLATFORMS": "", **env},
        )

    def plan(self, tags=(), **env):
        with tempfile.TemporaryDirectory() as tmp:
            run = self.run_release(Path(tmp), tags, **env)
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

    def test_release_candidate_base_can_be_chosen_but_not_already_released(self):
        self.assertEqual(self.plan(["v0.1.8", "v0.1.9-rc.4"], RC="1", VERSION="0.2.0"), (STOP, "0.2.0-rc.1"))
        code, error = self.plan(["v0.1.8"], RC="1", VERSION="0.1.8")
        self.assertEqual(code, 1)
        self.assertIn("already released", error)
        code, error = self.plan(["v0.1.8"], RC="1", VERSION="0.2.0-rc.1")
        self.assertEqual(code, 2)

    def test_release_candidate_pushes_an_image_and_tag_without_a_pr(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            run = self.run_release(tmp, ["v0.1.8", "v0.1.9-rc.1"], publish=True, RC="1")
            self.assertEqual(run.returncode, 0, run.stderr)
            calls = (tmp / "docker.log").read_text()
            push = next(l for l in calls.splitlines() if l.startswith("docker buildx build"))
            self.assertIn("--platform linux/amd64,linux/arm64", push)
            self.assertIn("--tag ghcr.io/rushobservability/query-api:0.1.9-rc.2", push)
            self.assertIn("--push", push)
            # Both the scan build and the push saw the candidate version; the checkout did not change.
            self.assertEqual(calls.count('version = "0.1.9-rc.2"'), 4)
            self.assertIn('version = "0.1.8"', (tmp / "work/Cargo.toml").read_text())
            self.assertEqual(git(tmp / "work", "status", "--porcelain"), "")
            head = git(tmp / "work", "rev-parse", "HEAD").strip()
            self.assertEqual(git(tmp / "origin.git", "rev-parse", "v0.1.9-rc.2").strip(), head)
            self.assertEqual(git(tmp / "origin.git", "branch", "--list", "release/*"), "")


if __name__ == "__main__":
    unittest.main()
