"""Regression coverage for runtime APK metadata selection, without Docker."""

from pathlib import Path
import subprocess
import unittest


SELECTOR = Path(__file__).with_name("select-openssl-packages.awk")


def package(name, library, directory="usr/lib"):
    return f"P:{name}\nV:3.6.5-r6\nF:{directory}\nR:{library}\n\n"


class OpenSSLPackageTests(unittest.TestCase):
    def select(self, database):
        return subprocess.run(
            ["awk", "-f", str(SELECTOR)],
            input=database,
            text=True,
            capture_output=True,
        )

    def test_legacy_package_names(self):
        database = package("libssl3", "libssl.so.3") + package("libcrypto3", "libcrypto.so.3")
        result = self.select(database)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, database)

    def test_versioned_names_preserve_records_and_exclude_openssl_four(self):
        expected = package("openssl-3.6-libssl", "libssl.so.3") + package("openssl-3.6-libcrypto", "libcrypto.so.3")
        database = expected + package("openssl-4.0-libssl", "libssl.so.4") + package("openssl-4.0-libcrypto", "libcrypto.so.4")
        result = self.select(database)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, expected)

    def test_missing_library_fails(self):
        result = self.select(package("libssl3", "libssl.so.3"))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("exactly one installed owner", result.stderr)

    def test_duplicate_owner_fails(self):
        database = package("libssl3", "libssl.so.3") * 2 + package("libcrypto3", "libcrypto.so.3")
        self.assertNotEqual(self.select(database).returncode, 0)

    def test_same_filename_in_another_directory_is_not_a_runtime_library(self):
        database = package("libssl3", "libssl.so.3", "other/lib") + package("libcrypto3", "libcrypto.so.3")
        self.assertNotEqual(self.select(database).returncode, 0)

    def test_empty_database_fails(self):
        self.assertNotEqual(self.select("").returncode, 0)


if __name__ == "__main__":
    unittest.main()
