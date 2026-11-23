"""Writes a release tarball of one architecture (ADR 0126, ADR 0137).

    python3 packaging/tarball.py [--program lotse|lotse-console] BINARY VERSION ARCH OUT_DIR

The tarball <program>-<version>-linux-<arch>.tar.gz holds the static binary,
the systemd unit and sysusers file, the OpenRC and sysvinit scripts, the
example config, the licence and the install notes. It is reproducible:
entries in a fixed order, owned by root, every time SOURCE_DATE_EPOCH, and a
gzip header without a name or time.
"""

import gzip
import io
import os
import sys
import tarfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PACKAGING = ROOT / "packaging"

# (path in the tarball, source, mode) per program.
NODE = [
    ("bin/lotse", None, 0o755),
    ("INSTALL.md", PACKAGING / "tarball/INSTALL.md", 0o644),
    ("LICENSE", ROOT / "LICENSE", 0o644),
    ("cluster.toml.example", PACKAGING / "cluster.toml.example", 0o644),
    ("restart-on-upgrade", PACKAGING / "restart-on-upgrade.sh", 0o755),
    ("openrc/lotse.confd", PACKAGING / "openrc/lotse.confd", 0o644),
    ("openrc/lotse.initd", PACKAGING / "openrc/lotse.initd", 0o755),
    ("systemd/lotse.sysusers", PACKAGING / "systemd/lotse.sysusers", 0o644),
    ("systemd/lotse@.service", PACKAGING / "systemd/lotse@.service", 0o644),
    ("sysvinit/lotse.default", PACKAGING / "sysvinit/lotse.default", 0o644),
    ("sysvinit/lotse.init", PACKAGING / "sysvinit/lotse.init", 0o755),
]
CONSOLE = [
    ("bin/lotse-console", None, 0o755),
    ("INSTALL.md", PACKAGING / "tarball/INSTALL-console.md", 0o644),
    ("LICENSE", ROOT / "LICENSE", 0o644),
    ("lotse-console.toml.example", PACKAGING / "lotse-console.toml.example", 0o644),
    ("openrc/lotse-console.confd", PACKAGING / "openrc/lotse-console.confd", 0o644),
    ("openrc/lotse-console.initd", PACKAGING / "openrc/lotse-console.initd", 0o755),
    ("systemd/lotse-console.service", PACKAGING / "systemd/lotse-console.service", 0o644),
    ("systemd/lotse-console.sysusers", PACKAGING / "systemd/lotse-console.sysusers", 0o644),
    ("sysvinit/lotse-console.default", PACKAGING / "sysvinit/lotse-console.default", 0o644),
    ("sysvinit/lotse-console.init", PACKAGING / "sysvinit/lotse-console.init", 0o755),
]
PROGRAMS = {"lotse": NODE, "lotse-console": CONSOLE}


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
    args = sys.argv[1:]
    program = "lotse"
    if args[:1] == ["--program"]:
        program, args = args[1] if len(args) > 1 else "", args[2:]
    if len(args) != 4 or program not in PROGRAMS:
        sys.exit(__doc__)
    binary, version, arch, out = args
    mtime = int(os.environ["SOURCE_DATE_EPOCH"])
    top = f"{program}-{version}-linux-{arch}"

    contents = {}
    for name, source, mode in PROGRAMS[program]:
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
