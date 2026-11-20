"""Writes the release tarball of one architecture (ADR 0126).

    python3 packaging/tarball.py BINARY VERSION ARCH OUT_DIR

The tarball lotse-<version>-linux-<arch>.tar.gz holds the static binary,
the systemd unit and sysusers file, the OpenRC and sysvinit scripts, the
example cluster file, the licence and INSTALL.md. It is reproducible: entries
in a fixed order, owned by root, every time SOURCE_DATE_EPOCH, and a gzip
header without a name or time.
"""

import gzip
import io
import os
import sys
import tarfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PACKAGING = ROOT / "packaging"

# (path in the tarball, source, mode)
FILES = [
    ("bin/lotse", None, 0o755),
    ("INSTALL.md", PACKAGING / "tarball/INSTALL.md", 0o644),
    ("LICENSE", ROOT / "LICENSE", 0o644),
    ("cluster.toml.example", PACKAGING / "cluster.toml.example", 0o644),
    ("openrc/lotse.confd", PACKAGING / "openrc/lotse.confd", 0o644),
    ("openrc/lotse.initd", PACKAGING / "openrc/lotse.initd", 0o755),
    ("systemd/lotse.sysusers", PACKAGING / "systemd/lotse.sysusers", 0o644),
    ("systemd/lotse@.service", PACKAGING / "systemd/lotse@.service", 0o644),
    ("sysvinit/lotse.default", PACKAGING / "sysvinit/lotse.default", 0o644),
    ("sysvinit/lotse.init", PACKAGING / "sysvinit/lotse.init", 0o755),
]


def entry(name: str, mtime: int, mode: int, size: int = 0, directory: bool = False) -> tarfile.TarInfo:
    info = tarfile.TarInfo(name)
    info.mtime = mtime
    info.mode = mode
    info.uid = info.gid = 0
    info.uname = info.gname = "root"
    if directory:
        info.type = tarfile.DIRTYPE
    else:
        info.size = size
    return info


def main() -> None:
    if len(sys.argv) != 5:
        sys.exit(__doc__)
    binary, version, arch, out = sys.argv[1:]
    mtime = int(os.environ["SOURCE_DATE_EPOCH"])
    top = f"lotse-{version}-linux-{arch}"

    contents = {}
    for name, source, mode in FILES:
        contents[name] = (Path(binary if source is None else source).read_bytes(), mode)
    dirs = sorted({str(Path(name).parent) for name in contents} - {"."})

    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w", format=tarfile.USTAR_FORMAT) as tar:
        tar.addfile(entry(top, mtime, 0o755, directory=True))
        for d in dirs:
            tar.addfile(entry(f"{top}/{d}", mtime, 0o755, directory=True))
        for name in sorted(contents):
            data, mode = contents[name]
            tar.addfile(entry(f"{top}/{name}", mtime, mode, len(data)), io.BytesIO(data))

    path = Path(out) / f"{top}.tar.gz"
    with open(path, "wb") as f:
        with gzip.GzipFile(filename="", mode="wb", fileobj=f, mtime=0, compresslevel=9) as gz:
            gz.write(raw.getvalue())


if __name__ == "__main__":
    main()
