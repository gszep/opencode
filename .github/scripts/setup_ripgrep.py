"""Provision the Core ripgrep prerequisite before the test network guard starts."""

import hashlib
import io
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import urllib.request
import zipfile

VERSION = "15.1.0"  # packages/core/src/ripgrep/binary.ts
# SHA-256 from BurntSushi/ripgrep's 15.1.0 release assets and .sha256 files.
ASSETS = {
    "Linux": (
        "x86_64-unknown-linux-musl.tar.gz",
        "1c9297be4a084eea7ecaedf93eb03d058d6faae29bbc57ecdaf5063921491599",
    ),
    "Windows": (
        "x86_64-pc-windows-msvc.zip",
        "124510b94b6baa3380d051fdf4650eaa80a302c876d611e9dba0b2e18d87493a",
    ),
}


def install(data, platform, directory):
    asset, digest = ASSETS[platform]
    if hashlib.sha256(data).hexdigest() != digest:
        raise ValueError("ripgrep archive checksum mismatch")
    filename = "rg.exe" if platform == "Windows" else "rg"
    member = f"ripgrep-{VERSION}-{asset.removesuffix('.tar.gz').removesuffix('.zip')}/{filename}"
    # Read only the exact executable, never extract archive-controlled paths.
    if platform == "Windows":
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            executable = archive.read(member)
    else:
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
            entry = archive.getmember(member)
            if not entry.isfile():
                raise ValueError("ripgrep executable is not a regular file")
            executable = archive.extractfile(entry).read()
    target = directory / filename
    with target.open("xb") as output:
        output.write(executable)
    target.chmod(0o755)
    return target


def main():
    if os.environ["RUNNER_ARCH"] != "X64":
        raise ValueError("ripgrep test setup supports only the x64 CI matrix")
    platform = os.environ["RUNNER_OS"]
    asset, _ = ASSETS[platform]
    url = f"https://github.com/BurntSushi/ripgrep/releases/download/{VERSION}/ripgrep-{VERSION}-{asset}"
    directory = Path(tempfile.mkdtemp(prefix="test-ripgrep-", dir=os.environ["RUNNER_TEMP"]))
    try:
        with urllib.request.urlopen(url, timeout=60) as response:
            data = response.read(8 * 1024 * 1024 + 1)
        if len(data) > 8 * 1024 * 1024:
            raise ValueError("ripgrep archive exceeds setup size bound")
        target = install(data, platform, directory)
        version = subprocess.check_output([str(target), "--version"], text=True, timeout=10)
        if version.splitlines()[0] != f"ripgrep {VERSION}":
            raise ValueError("unexpected ripgrep version")
        with open(os.environ["GITHUB_PATH"], "a", encoding="utf-8") as output:
            output.write(str(directory) + "\n")
        print(version, end="")
    except BaseException:
        shutil.rmtree(directory)
        raise
    # Successful installation lives in this invocation's RUNNER_TEMP until VM teardown.


if __name__ == "__main__":
    main()
