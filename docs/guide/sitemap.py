#!/usr/bin/env python3
"""Write sitemap.xml into the built guides, from the pages SUMMARY.md lists.

Run after `mdbook build docs/guide`. The list comes from SUMMARY.md, so a new
chapter is in the sitemap without anyone editing it (ADR 0081).
"""
import re
import sys
from pathlib import Path
from xml.sax.saxutils import escape

BASE = "https://pierrejochem.github.io/lotse/"
guide = Path(__file__).resolve().parent
summary = (guide / "src" / "SUMMARY.md").read_text(encoding="utf-8")

pages = [""]  # the root, which is also introduction.html
for target in re.findall(r"\]\(([^)#]+\.md)\)", summary):
    if target != "introduction.md":
        pages.append(re.sub(r"\.md$", ".html", target))

out = guide / "book"
if not out.is_dir():
    sys.exit("docs/guide/book is missing: run `mdbook build docs/guide` first")

lines = ['<?xml version="1.0" encoding="UTF-8"?>',
         '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
for page in pages:
    if page and not (out / page).is_file():
        sys.exit(f"SUMMARY.md lists {page}, but the build has no such page")
    lines.append(f"  <url><loc>{escape(BASE + page)}</loc></url>")
lines.append("</urlset>")
(out / "sitemap.xml").write_text("\n".join(lines) + "\n", encoding="utf-8")
print(f"sitemap.xml: {len(pages)} URLs")
