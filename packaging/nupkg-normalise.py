#!/usr/bin/env python3
"""Rewrites .nupkg and .snupkg files so the same commit gives the same bytes (ADR 0142).

`dotnet pack` on .NET 10 names the package's core-properties part with a random GUID, gives the
relationships in _rels/.rels random ids, and stamps every zip entry with the build time. The
assemblies inside are already deterministic. This keeps every entry and its content, and replaces
the three varying things: the part is named from the package's own file name, the relationship
ids are derived from their targets, and every entry gets the commit's time (SOURCE_DATE_EPOCH).
.NET 11's `dotnet pack` does this by itself; this script can go once the SDK moves there.

usage: nupkg-normalise.py FILE...
"""

import hashlib
import os
import re
import sys
import time
import zipfile

PSMDCP = re.compile(r"package/services/metadata/core-properties/[0-9a-f]{32}\.psmdcp")
REL_ID = re.compile(r'Target="([^"]*)" Id="R[0-9A-F]+"')


def stable_hex(text: str, length: int) -> str:
    return hashlib.sha256(text.encode()).hexdigest()[:length]


def normalise(path: str, when: tuple) -> None:
    with zipfile.ZipFile(path) as src:
        entries = [(info, src.read(info.filename)) for info in src.infolist()]

    part = f"package/services/metadata/core-properties/{stable_hex(os.path.basename(path), 32)}.psmdcp"
    rewritten = []
    for info, data in entries:
        name = PSMDCP.sub(part, info.filename)
        if name == "_rels/.rels":
            text = PSMDCP.sub(part, data.decode())
            text = REL_ID.sub(lambda m: f'Target="{m.group(1)}" Id="R{stable_hex(m.group(1), 16).upper()}"', text)
            data = text.encode()
        rewritten.append((name, data))

    tmp = path + ".tmp"
    with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as dst:
        for name, data in rewritten:
            info = zipfile.ZipInfo(name, date_time=when)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            info.create_system = 3
            dst.writestr(info, data)
    os.replace(tmp, path)


def main() -> None:
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    epoch = int(os.environ.get("SOURCE_DATE_EPOCH", "315532800"))
    # Zip times start in 1980 and have two-second steps.
    when = time.gmtime(max(epoch, 315532800))[:6]
    when = when[:5] + (when[5] - when[5] % 2,)
    for path in sys.argv[1:]:
        normalise(path, when)


if __name__ == "__main__":
    main()
