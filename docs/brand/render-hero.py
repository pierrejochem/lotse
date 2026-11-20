"""Renders the README banner in both themes from readme-hero.html.

    python3 docs/brand/render-hero.py

Needs Playwright with Chromium and the Schibsted Grotesk font installed
locally (or network access to Google Fonts). Writes readme-hero-light.png
and readme-hero-dark.png next to this file, at twice the CSS size.
"""

from pathlib import Path

from playwright.sync_api import sync_playwright

HERE = Path(__file__).resolve().parent


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 360}, device_scale_factor=2)
        for theme in ("light", "dark"):
            page.goto(f"{(HERE / 'readme-hero.html').as_uri()}?theme={theme}")
            page.evaluate("document.fonts.ready")
            page.locator(".hero").screenshot(path=str(HERE / f"readme-hero-{theme}.png"), omit_background=True)
        browser.close()


if __name__ == "__main__":
    main()
