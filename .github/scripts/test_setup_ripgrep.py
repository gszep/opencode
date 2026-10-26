import hashlib
import io
import os
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch
import zipfile

import setup_ripgrep


class InstallTests(unittest.TestCase):
    def archive(self, platform, missing=False, link=False):
        asset, _ = setup_ripgrep.ASSETS[platform]
        filename = "rg.exe" if platform == "Windows" else "rg"
        member = f"ripgrep-15.1.0-{asset.removesuffix('.tar.gz').removesuffix('.zip')}/{filename}"
        data = io.BytesIO()
        if platform == "Windows":
            with zipfile.ZipFile(data, "w") as archive:
                if not missing:
                    archive.writestr(member, b"fixture executable")
                archive.writestr("../escaped", b"must not extract")
        else:
            with tarfile.open(fileobj=data, mode="w:gz") as archive:
                if not missing:
                    entry = tarfile.TarInfo(member)
                    entry.size = len(b"fixture executable")
                    if link:
                        entry.type = tarfile.SYMTYPE
                        entry.linkname = "../escaped"
                    archive.addfile(entry, io.BytesIO(b"fixture executable"))
                entry = tarfile.TarInfo("../escaped")
                archive.addfile(entry)
        return data.getvalue()

    def test_pins_and_runtime_version(self):
        source = (Path(__file__).parents[2] / "packages/core/src/ripgrep/binary.ts").read_text()
        self.assertIn(f'const VERSION = "{setup_ripgrep.VERSION}"', source)
        self.assertEqual(set(setup_ripgrep.ASSETS), {"Linux", "Windows"})
        for _, digest in setup_ripgrep.ASSETS.values():
            self.assertRegex(digest, r"^[a-f0-9]{64}$")

    def test_exact_member_and_no_clobber(self):
        for platform in setup_ripgrep.ASSETS:
            with self.subTest(platform=platform), tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary) / "bin"
                directory.mkdir()
                data = self.archive(platform)
                asset, _ = setup_ripgrep.ASSETS[platform]
                with patch.dict(setup_ripgrep.ASSETS, {platform: (asset, hashlib.sha256(data).hexdigest())}):
                    target = setup_ripgrep.install(data, platform, directory)
                    self.assertEqual(target.read_bytes(), b"fixture executable")
                    self.assertFalse((Path(temporary) / "escaped").exists())
                    with self.assertRaises(FileExistsError):
                        setup_ripgrep.install(data, platform, directory)
                    self.assertEqual(target.read_bytes(), b"fixture executable")

    def test_checksum_rejects_before_writing(self):
        for platform in setup_ripgrep.ASSETS:
            with self.subTest(platform=platform), tempfile.TemporaryDirectory() as temporary:
                with self.assertRaisesRegex(ValueError, "checksum mismatch"):
                    setup_ripgrep.install(self.archive(platform), platform, Path(temporary))
                self.assertEqual(list(Path(temporary).iterdir()), [])

    def test_missing_member_and_tar_link_fail_closed(self):
        for platform, missing, link in [("Linux", True, False), ("Windows", True, False), ("Linux", False, True)]:
            with self.subTest(platform=platform, missing=missing), tempfile.TemporaryDirectory() as temporary:
                data = self.archive(platform, missing=missing, link=link)
                asset, _ = setup_ripgrep.ASSETS[platform]
                with patch.dict(setup_ripgrep.ASSETS, {platform: (asset, hashlib.sha256(data).hexdigest())}):
                    with self.assertRaises((KeyError, ValueError)):
                        setup_ripgrep.install(data, platform, Path(temporary))
                self.assertEqual(list(Path(temporary).iterdir()), [])

    def test_publish_only_after_version_check(self):
        for platform in setup_ripgrep.ASSETS:
            for output, valid in [
                ("ripgrep 15.1.0\n", True),
                ("ripgrep 15.1.0 (rev af60c2de9d)\n", True),
                ("ripgrep 15.1.0 (rev af60c2de9d)\r\n", True),
                ("ripgrep 15.1.0", True),
                ("ripgrep 15.1.0\nripgrep 99\n", False),
                ("ripgrep 15.1.0 (rev af60c2de9d)\narbitrary tail\n", False),
                ("ripgrep 15.1.0\n\n", False),
                ("ripgrep 15.1.0\r", False),
                ("ripgrep 14.0.0\n", False),
                ("ripgrep 15.1.01\n", False),
                ("ripgrep 15.1.0-unreviewed\n", False),
                ("ripgrep 15.1.0 (rev not-a-hash)\n", False),
                ("", False),
            ]:
                with self.subTest(platform=platform, output=output), tempfile.TemporaryDirectory() as temporary:
                    data = self.archive(platform)
                    asset, _ = setup_ripgrep.ASSETS[platform]
                    environment = {
                        "RUNNER_ARCH": "X64",
                        "RUNNER_OS": platform,
                        "RUNNER_TEMP": temporary,
                        "GITHUB_PATH": str(Path(temporary) / "path"),
                    }
                    with (
                        patch.dict(os.environ, environment, clear=True),
                        patch.dict(setup_ripgrep.ASSETS, {platform: (asset, hashlib.sha256(data).hexdigest())}),
                        patch.object(setup_ripgrep.urllib.request, "urlopen", return_value=io.BytesIO(data)) as download,
                        patch.object(
                            setup_ripgrep.subprocess, "check_output",
                            return_value=output,
                        ) as version,
                    ):
                        if valid:
                            setup_ripgrep.main()
                            directory = Path(environment["GITHUB_PATH"]).read_text().strip()
                            self.assertEqual(str(Path(version.call_args.args[0][0]).parent), directory)
                            self.assertEqual(Path(version.call_args.args[0][0]).read_bytes(), b"fixture executable")
                        else:
                            with self.assertRaisesRegex(ValueError, "unexpected ripgrep version"):
                                setup_ripgrep.main()
                            self.assertEqual(list(Path(temporary).iterdir()), [])
                        self.assertEqual(version.call_args.args[0][1:], ["-V"])
                        self.assertEqual(version.call_args.kwargs, {"text": True, "timeout": 10})
                        download.assert_called_once_with(f"https://github.com/BurntSushi/ripgrep/releases/download/15.1.0/ripgrep-15.1.0-{asset}", timeout=60)


if __name__ == "__main__":
    unittest.main()
