# 140. The console has a navy primary action and red only for danger

- Status: Accepted
- Date: 2026-11-23

## Context
The brand guide made `signal` (pilot red) the colour of the primary action, the active item and links. `critical` shares that hue ([brand](../brand/README.md)), so in the console red meant create, link, focus, error and delete at once. A review of the console against its mockups (`docs/console/mockups/v2/`) found that a delete looked no weightier than a create, that links read as errors, that a field in keyboard focus looked invalid, and that the borders of inputs (`line`, 1.3:1) and the switch in dark mode (1.1:1) fell below the 3:1 that WCAG 2.2 asks of controls.

## Decision
- The primary action, the active toggle and the switch track use a new token `primary`: `elbe` navy in light, `#3B7BA8` in dark (`elbe` itself stays `#2E5F80` there, which is 2.4:1 on a card). `on-primary` is white.
- `signal` and `critical` mean attention, error and danger only. Red is not the colour of links or of focus.
- Links use `link`, focus uses `focus-ring` (`#1F6AA5` light, `#7CC0F0` dark), selection uses `selected` and `selected-bg`, always with a bar and a word.
- Borders of inputs, selects, switches and toggle groups use a new token `control` (3:1 or more on `surface`, `surface-raised` and `fog`); `line` stays for hairlines.
- The solid danger button (`danger-solid`) appears only in the confirm dialog of a destructive action. Rows and menus keep the tinted `danger` button.
- The tokens live in `docs/brand/tokens.json`; `tokens.css` is generated from it and `console/web/src/styles.css` carries the same values.

## Consequences
- The brand rule "one red element per view" now holds for what it meant: red draws the eye only to something wrong.
- Components that used `signal` as primary move to `primary` in the console; the logo mark keeps its red half.
- The primary button in dark mode is a lighter blue than the navy in light. Its label is white at 4.58:1, so it stays at 14 px and 600 weight or larger.
- The rest of the console follows the mockups in steps: shell and navigation, tables, access, record browser and overview, and the copy. Each is its own change.
- The console screenshots in `docs/console/` show the red primary; they are retaken when the steps are done.
