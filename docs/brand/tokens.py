"""Generates tokens.css from tokens.json: python3 docs/brand/tokens.py"""

import json
import pathlib

here = pathlib.Path(__file__).parent
t = json.loads((here / "tokens.json").read_text())
colors = {c["name"]: c["value"] for c in t["color"]["tokens"]}


def value(name, theme):
    v = colors[name]
    if isinstance(v, str) and v.startswith("{"):
        return value(v.strip("{}"), theme)
    return v[theme]


def block(theme):
    lines = [f"  --{name}: {value(name, theme)};" for name in colors]
    return "\n".join(lines)


fixed = []
for fam, stack in t["type"]["families"].items():
    fixed.append(f"  --font-{fam}: {stack};")
for group in ("spacing", "radius"):
    for tok in t[group]["tokens"]:
        fixed.append(f"  --{tok['name']}: {tok['value']};")

css = f"""/* Lotse design tokens. Generated from tokens.json by tokens.py; do not edit. */

:root {{
{block("light")}
{chr(10).join(fixed)}
}}

@media (prefers-color-scheme: dark) {{
  :root:not([data-theme="light"]) {{
{chr(10).join("  " + l for l in block("dark").splitlines())}
  }}
}}

:root[data-theme="dark"] {{
{block("dark")}
}}
"""
(here / "tokens.css").write_text(css)
print(f"wrote {here / 'tokens.css'}")
