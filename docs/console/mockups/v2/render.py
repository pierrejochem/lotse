"""Renders every mockup at 1440, 768 and 390 px, light and dark, into png/.

    python3 docs/console/mockups/v2/render.py

Needs google-chrome (headless) and Pillow. Each page is shot in a tall window,
then trimmed below its last content row, because the height Chrome reports
before painting can be shorter than the painted page.
"""
import subprocess, sys
from pathlib import Path
from PIL import Image

HERE = Path(__file__).resolve().parent
PAGES = ["shell", "tables", "access", "messages", "system", "overview"]
WIDTHS = [1440, 768, 390]
TALL = 16000
CHROME = ["google-chrome", "--headless=new", "--disable-gpu", "--hide-scrollbars", "--no-sandbox", "--force-device-scale-factor=1"]


def trim(path: Path) -> None:
    im = Image.open(path).convert("RGB")
    w, h = im.size
    bg = im.getpixel((w - 1, h - 1))  # page background, bottom-right corner
    px = im.load()
    y = h - 1
    # last row with any pixel unlike the background, scanning the main column only
    x0 = 0 if w < 700 else 248 if w > 1100 else 64
    while y > 0 and all(px[x, y] == bg for x in range(x0, w, 2)):
        y -= 1
    im.crop((0, 0, w, min(h, y + 33))).save(path)


def main():
    (HERE / "png").mkdir(exist_ok=True)
    for page in PAGES:
        for theme in ("light", "dark"):
            url = f"{(HERE / (page + '.html')).as_uri()}?theme={theme}"
            for w in WIDTHS:
                out = HERE / "png" / f"{page}-{w}-{theme}.png"
                subprocess.run(CHROME + [f"--window-size={w},{TALL}", "--virtual-time-budget=2000", f"--screenshot={out}", url], capture_output=True, timeout=120)
                trim(out)
                print(out.name, Image.open(out).size)


if __name__ == "__main__":
    sys.exit(main())
