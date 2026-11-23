# Console mockups, revision 6 (variant B)

Static, self-contained HTML (inline CSS, one small script that reads `?theme=light|dark` and reports page height). No frameworks, no network. Tokens come from `console/web/src/styles.css`; additions are listed below. `python3 render.py` writes `png/<page>-<width>-<theme>.png` at 1440, 768 and 390 px (needs `google-chrome`). [index.html](index.html) lists everything with thumbnails.

## Pages

| Page | Shows |
| --- | --- |
| [shell](shell.html) | Three layouts of one shell. 1440 px: 248 px sidebar. 768 px: 64 px icon rail (names are `title` and screen-reader text, active item has a 3 px bar). 390 px: one 56 px row with menu button, truncated breadcrumb and avatar; navigation is a drawer. Live and Demo leave the bar: Live goes into the avatar menu, Demo into a 32 px strip. Specimens: skip link focused, account menu, drawer. |
| [tables](tables.html) | Streams, queues, nodes. 390 px: cards with name plus two key columns. 768 px: focusable scroll region with a fade at the edge that has more. Variant B: sticky name column. Selected row has a 3 px bar plus tint. Names are links. Chips have icon and word. Meters have a labelled state (OK, Near limit, Below disk limit). Empty state and the purge dialog. |
| [access](access.html) | People and credentials with Users, Tokens, Certificates, Sessions as first-class nav entries and tabs. Row menu (Delete last, danger), confirm dialog naming the target and what breaks, empty Tokens, Certificates Requests and Accepted, create-token dialog with Owner combobox (default `me`). |
| [messages](messages.html) | Records with sticky header, side panel (text and hex, headers), Older and Newer, start offset, labelled switch Show uncommitted records with helper text. |
| [system](system.html) | Primary colour variants A and B side by side, all controls, focus rings, control border, measured contrast of every pair (the build fails if one is below 4.5 or 3). |
| [overview](overview.html) | Chart stretches to the Health card height; series 2 is dashed with square markers and a legend swatch that matches; Show data table; Nodes below disk limit reads 1 of 3. |

## Copy applied

Nodes (not Broker nodes), Partitions (not Parts), Requested by (not Asked by), the purge sentence, Cancel for non-destructive dismissals, units in headers (MiB/s, messages, bytes, GiB), empty states with a next step. Navigation: Tenants, People and credentials.

## Tokens added

- `--control` `#6b8090` light, `#5f7686` dark: borders of inputs, selects, toggle group, switch and outlined buttons (3.1 to 4.1 against surface, raised and fog). `--line` stays decoration.
- `--focus` `#1f6aa5` light, `#7cc0f0` dark, plus a 4 px halo in the surface colour; white ring on the navy nav. Never red.
- `--sel`, `--sel-bg`: selected row and nav cue (bar plus tint).
- `--link`: `#a8261f` light (variant A). `--signal` on fog is only 4.38:1, so red text never sits on fog.
- `--danger-bg`/`--danger-fg`: solid danger button, 7.1:1 light and 8.7:1 dark.
- Dark variant B primary is `#3b7ba8` (4.58:1 with white, 3.58:1 against raised), not `#2e5f80`, which is 2.4:1 against raised.

## Revision 3 changes

Variant B (navy primary) is the default on every page; `system.html` keeps A as the alternative. The pages show one incident, consistently: node-3 holds 143 GiB free of 2 TiB (93 % used), and because 3 replicas sit on 3 nodes, every partition has a replica there, so every stream and queue shows "Refusing writes" and the cluster is Degraded (chip on the overview, "degraded" in the cluster switch name). Disk wording: nodes and meters say "Below disk limit", resources say "Refusing writes", the overview metric says "Nodes below disk limit". Sizes are for tenant default only, summed over all replicas; node disks also hold data not shown here (logs, snapshots). 48 partitions are 36 in streams and 12 in queues; 61.5 MiB/s produced, of which the four busiest resources account for 56.8.

The page formerly called Messages is now the Record browser: it reads stream records. Records are listed oldest first, so Older means lower offsets and Newer higher ones; the Order toggle is gone. Only the newest records can be uncommitted, and the chip sits in the offset cell, so it shows in cards too. Choosing a record moves focus to the panel heading, the only announcement.

Dialogs use real buttons and open with focus on Cancel; typed-name confirmation for delete user, revoke certificate and revoke token (revoke sessions has none, because users can sign in again). Create token opens on the Name field, and its second step has no X. Cards at 640 px and below carry table roles, an actions button on the name row and a More disclosure. Sub-items are nested lists under People and credentials, which gets `aria-current="true"` while a child is current. Rail tooltips are `role="tooltip"` children of the links. Forced-colors rules use outlines and borders. `prefers-color-scheme` sets the default theme, and reduced motion removes transitions.

`render.py` shoots each page in a tall window and trims below the last content row (needs Pillow), because the height Chrome reports before painting was too short for some pages.

## Revision 4 changes

- One page header everywhere: title and subtitle left, actions and the tenant control right, a full-width row below 640 px. Each credentials tab has its own primary action (Create user, Create token, Request certificate).
- The Streams table no longer has a disk column: every stream has a replica on node-3, so the column would be the same on every row. Streams, queues and nodes all end with an actions column and fit at 1440 px without scrolling. Node cards show identical meter bars with the label under the bar.
- Scroll fades are an overlay on the table wrapper, so they show on every row, selected included; tables that fit (users, tokens, busiest resources) have none. The sticky-name variant is shown at 1099 px and below.
- The cluster entry is a plain link with an EU badge while there is one cluster; the amber dot on it is decoration and the state is in its name ("degraded").
- Added: Sessions mock, row menus for user, token and certificate, delete-your-own-user dialog, start offset with Apply and an out-of-range error. The switch is a button with `role="switch"`.
- Rule for numbers: a space before the unit and before %, as in 93 % and 61.5 MiB/s.

## Revision 5 changes

- Every credentials view starts with title, subtitle and primary action, then the tab bar (shown at 1099 px and below). The Users page is titled Users, under People and credentials. Spec captions are small eyebrow labels, not headings.
- Scroll fades and the focusable scroll region exist only where a table actually overflows (a small script in each page sets this); cards and tables that fit have neither.
- The degraded cluster shows a warning icon and the word Degraded in the navigation (an icon with a different shape in the rail, plus the tooltip).
- Sessions are named by principal and peer, with an action that revokes all sessions of the principal and a dialog for your own sessions. Tokens and certificates show absolute dates, Expired and Revoked states, and link to detail like users.
- Menus use `role="separator"`, the account identity sits outside the menu, every menu button has `aria-expanded`. Record browser: Apply is a submit button, an Oldest button, page size stated, empty-partition and out-of-range states.
- Not done: a non-red stream row. Every stream has a replica on node-3, which is below its disk limit, so showing one healthy stream would contradict the data.

## Revision 6 changes

- Table headers wrap instead of being cut, and Streams, Queues and Nodes keep fixed name and status widths at 1100 px and above. All six pages have no horizontal page scroll at 320 px (checked in a 320 px frame).
- The fade also updates on scroll and resize and hides at the end. Newer and Latest use `aria-disabled`. Sessions rows use the same "..." menu as other rows, and your own session is marked "This session". Deleting yourself shows the last-cluster-admin refusal.
- Dates are written once as 2027-02-21 (in 90 days). The record browser uses offsets in the tens of millions with timestamps in microseconds, so they match a stream at 38.4 MiB/s. The node-disk note names the tenants analytics and payments.

## Decisions (all five decided)

1. **Decided: B.** Primary colour: B (navy, red only for danger and errors) over A (red, as today). It needs an ADR and a change to the brand guide, which makes signal the primary action.
2. **Decided: confirm dialog only.** The solid danger button appears only in the confirm dialog; menus and rows keep the tinted button.
3. **Decided: cards for lists, sticky name column for wide read-only data.** Cards for lists people scan (streams, queues, nodes, users, tokens); sticky column for wide read-only tables such as Busiest resources.
4. **Decided: as shown.** Navigation duplication: sidebar lists Users, Tokens, Certificates, Sessions; page tabs repeat them only at 1099 px and below. Recommendation: as shown.
5. **Decided: page header.** Tenant scope: a control in each data page header (shown) or one in the top bar. Recommendation: page header now; top bar if cluster pages need it too.

## Known limits

- Static: focus, open menus and dialogs are fixed example states. Escape, focus trap, inert and focus return are described, not built.
- Schibsted Grotesk and IBM Plex Mono are not installed here, so PNGs use the fallbacks from `styles.css`.
- Contrast is computed for the colours used (system.html table, asserted at build), not measured on rendered pixels.
- Tooltips on the rail and the hover and focus states are specimens; the PNGs at 768 px show the rail without a tooltip.
- Row name links are 32 px high (the minimum); other targets are 36 to 44 px.
- Cards drop all but two columns at 640 px and below; the rest sit under More.
