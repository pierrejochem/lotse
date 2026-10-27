# Lotse brand

<p><img src="logos/lotse-wordmark.svg" alt="Lotse" height="48"></p>

Lotse is a message broker written in Rust: reliable queues and persistent streams on one replicated log, with exactly-once delivery. A *Lotse* is a harbour pilot, the person who boards a ship and guides it safely to its berth. The broker does the same for messages.

The brand borrows from the pilot's own signal. International signal flag **H** is split vertically, white and red, and means "I have a pilot on board". Lotse keeps the split, swaps white for Elbe navy, and draws what the broker does across it: three queue lanes on the navy side converge into one stream on the red side. The mark is called **Fairway**, after the marked channel a pilot steers through.

## Voice

Lotse talks like an experienced pilot on the radio: calm, exact, short.

- Do: state guarantees precisely ("committed to 2 of 3 replicas"), name the unit (ms, MiB, offsets), use the domain's own words: partition, offset, ack, leader, term.
- Do: write errors as what happened plus what to do ("Partition 3 has a new leader. Retrying on node 2.").
- Don't: hype words (blazing, magical, revolutionary), exclamation marks, emoji, nautical puns in product copy. The maritime idea lives in the mark and the name, not in every sentence.
- The name is always **Lotse**, capital L, never all caps, never translated. Crates and binaries are lowercase: `lotse-broker`, `lotse-client`.

## Logo

The mark, Fairway, is a rounded square split down the middle: `elbe` on the left, `signal` on the right. Three short white lanes on the navy half curve into one wider white stream that runs across the red half. Queues in, stream out, one log underneath. Files are in [`logos/`](logos/).

- **Primary:** [`lotse-wordmark.svg`](logos/lotse-wordmark.svg) on light grounds, [`lotse-wordmark-reversed.svg`](logos/lotse-wordmark-reversed.svg) on `surface` in dark mode or on `elbe` bands.
- **Mark alone:** [`lotse-mark.svg`](logos/lotse-mark.svg) for favicons, avatars, CLI banners and the diagram header. Minimum size 16px; keep clear space of a quarter of the mark's width on every side.
- **Single colour:** [`lotse-wordmark-mono.svg`](logos/lotse-wordmark-mono.svg) and [`lotse-mark-mono.svg`](logos/lotse-mark-mono.svg) use `currentColor`; the red half drops to 55% of the ink so the split survives in one colour. Use these for print, embossing and inline SVG in docs.
- Never recolour the halves, swap their order, put the mark on `signal`, rotate or mirror it (the flow always runs left to right), change the number of lanes, or set the wordmark in another typeface.

## Colour

Two identity colours carry the brand: `elbe` (navy) and `signal` (pilot red). Everything else is neutral, tinted slightly toward the navy.

- Pages sit on `surface`; cards and code blocks on `surface-raised`; quiet bands on `fog`.
- `signal` is spent sparingly: the primary action, the active item, the one thing that needs attention. One red element per view is the target.
- `brass` is a small accent for offsets, markers and highlights in diagrams, never for buttons.
- States use `ok`, `warn` and `critical`. Because `critical` shares the brand red, every error carries an icon and a word, never colour alone.
- Text pairs that pass 4.5:1 in both themes: `ink` and `ink-muted` on `surface` and `surface-raised`; `signal` on `surface`, `surface-raised` and `signal-soft`; `on-signal` on `signal`; `on-elbe` on `elbe`.

## Typography

**Schibsted Grotesk** for everything people read: a sturdy Nordic grotesk with open counters that stays legible in dense tables and holds up as a display face. **IBM Plex Mono** for code, offsets, IDs, error codes and config.

- `display` and `h1` at weight 700–800 with slight negative tracking; `body` at 16/24.
- `label` is uppercase with 0.08em tracking, for table headers and section eyebrows.
- Numbers that line up in columns use tabular figures (`font-variant-numeric: tabular-nums`).
- Anything a user would type or copy (`GroupHeartbeat`, `NOT_LEADER`, `__txn_state`) is set in `code`.

## Shape and layout

- Spacing runs on a 4px base: `space-1` through `space-12`.
- Corners are restrained: `radius-sm` for chips and inputs, `radius-md` for buttons and cards. `radius-lg` belongs to the mark and large feature blocks.
- Borders, not shadows. Separate surfaces with `line` hairlines.
- The recurring graphic device is the **log segment**: a short pill, `space-2` tall with `radius-sm` corners, set in rows at a fixed pitch. Rows of segments stand for the append-only log in diagrams, covers and slides.
- At large scale, segments follow the **Fairway** motion of the mark: three lanes of segments converge into one wider stream (`space-4` tall, `radius-md`), always left to right, navy side first. Use it on covers, title slides and section openers, once per view.

## Iconography

No custom icon set yet. Use a plain 1.5px-stroke outline set at 16 and 20px, coloured with `ink` or `ink-muted`, and `signal` only for the active state. Avoid ship, anchor and wheel icons for product features.

## Files

- [`tokens.json`](tokens.json): colours (light and dark), type styles, spacing and radii; the source of truth.
- [`tokens.css`](tokens.css): the same tokens as CSS custom properties, generated from `tokens.json` with `python3 docs/brand/tokens.py`.
- [`logos/`](logos/): the five lockups described above.
- [`cover.html`](cover.html) and [`cover.png`](cover.png): the cover with the Fairway motif.

Fonts: [Schibsted Grotesk](https://fonts.google.com/specimen/Schibsted+Grotesk) and [IBM Plex Mono](https://fonts.google.com/specimen/IBM+Plex+Mono), both under the SIL Open Font License; load them from Google Fonts or self-host the files from there. The logos have their lettering outlined and need neither.
