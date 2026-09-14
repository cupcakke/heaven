# Refractive Nocturne

## Essence

A dark, liquid-glass interface that behaves less like flat UI and more like polished optical
material floating in deep space. Nearly everything sits on an almost-black field (near `#09090f`)
lit only by a slow-drifting blue-violet aurora, so the product itself becomes the only real light
source. Surfaces are not filled shapes — they are simulated glass rendered in WebGL, refracting,
dispersing and glaring against whatever passes behind them. The personality is calm, expensive and
physical: hairline borders, system-native type with tight tracking, springy overshoot motion, and a
cool blue-to-mint accent range. It reads as precision instrumentation rather than decoration.

---

## Color Palette

### Backgrounds

- **App base**: `#09090f` — the fixed canvas/backdrop painted behind everything.
- **Shader field base**: `rgb(0.02, 0.024, 0.05)` ≈ `#05060d` — the WebGL background's floor value.
- **Aurora bloom**: base `rgb(0.04, 0.06, 0.16)` ≈ `#0a0f29`, modulated ±0.18 (~±46/255) by a cosine
  ramp (frequencies 0.22 / 0.32 / 0.45 per channel) — produces slow, drifting blue-indigo clouds.
- **CSS fallback field** (used when WebGL is unavailable): `linear-gradient(135deg, #4b6cf7 0%, #000 50%, #0009cd 100%)`
  at 15% opacity, `filter: blur(80px)`, over `#000000`, then scrimmed with
  `linear-gradient(180deg, rgba(0,0,0,.6), rgba(0,0,0,.98))`.
- **Glass surface fill (fallback)**: `linear-gradient(145deg, rgba(0,40,218,.033), rgba(255,255,255,.015) 48%, rgba(246,246,250,.018))`
  — barely-there tint; the real glass is rendered, not painted.
- **Opaque-ish panel** (code blocks): `rgba(10,14,12,.58)`.
- **Token**: `--bg: #000000` (legacy root token).

### Text

- Primary: `#f5f5f7`
- Secondary: `rgba(245,245,247,.58)`
- Tertiary / muted: `rgba(245,245,247,.28)`
- On-accent (filled bubble, toasts): `#f7fffb` / `#ffffff`
- Code body: `rgba(245,245,247,.86)`; line numbers: `rgba(245,245,247,.24)`; comments: `rgba(245,245,247,.35)`
- Monospace metadata (timestamps, clocks): `rgba(245,245,247,.38)`

### Accents

- **Electric blue**: `#0000ff` (root accent token), plus the working UI blues
  `#505aff` (`80,90,255`), `#5f6af7`, `#8e9bff`, `#4b6cf7`, `#0009cd`, `#007aff`.
- **Mint / aqua**: `#00ff9d` (`0,255,157`), `#00cc9c` (`0,204,156`), `#62ffbf`. Used for interactive
  text, links, attachment chips, and success confirmations.
- **Primary action gradient** (send/submit):
  `linear-gradient(142deg, rgba(0,122,255,.82), rgba(0,204,156,.78) 50%, rgba(0,255,157,.72))`
- **Accent surface gradient** (self-authored message):
  `linear-gradient(138deg, rgba(0,9,205,.78), rgba(80,90,255,.72) 48%, rgba(95,106,247,.66))`
- **Selected row gradient**: `linear-gradient(135deg, rgba(11,0,246,.075), rgba(80,90,255,.07))`

### Semantic

| Role | Value | Context |
|---|---|---|
| Error / destructive | `#ff453a`, `rgba(255,59,48,.9)` | destructive menu items, delete press state |
| Error surface | `rgba(200,35,30,.52)` | error toast |
| Success | `#62ffbf` | copy confirmation, completed steps |
| Success surface | `rgba(0,150,90,.52)` | success toast |
| Warning / tool activity | `#ffcf70` | tool-in-progress step markers |
| Info surface | `rgba(0,96,120,.52)` | neutral toast |
| Notice / reconnect | bg `rgba(0,40,218,.08)`, border `rgba(0,40,218,.16)`, text `rgba(80,90,255,.9)` | inline status bar |
| Focus ring | `rgba(100,120,255,.7)` | 2px `:focus-visible` outline, 2px offset |

### Syntax highlighting (code surfaces)

keyword `#8e9bff` · string `#78e6b0` · number `#f3c37a` · tag `#ff98c8` · attribute `#91d8ff` ·
boolean `#d5a8ff` · property `#8ed7ff` · comment `rgba(245,245,247,.35)` (italic)

### Borders, dividers, shadows

- Divider: `rgba(255,255,255,.055)` (`--sep`) at `.5px`–`1px`
- Hover fill: `rgba(255,255,255,.055)` (`--hover`)
- Glass border: `rgba(0,0,0,.068)`; stronger variant `rgba(232,232,246,.118)`
- Inner top highlight: `rgba(255,255,255,.105)`; inner bottom shadow `rgba(0,0,0,.14)`
- Cast shadow color: `rgba(0,0,10,.34)` (`--liquid-shadow`)

---

## Typography

### Families

- **UI / body**: `-apple-system, BlinkMacSystemFont, "SF Pro Text", "SF Pro Display", "Helvetica Neue", Arial, sans-serif`
  — system-native stack, no webfonts loaded.
- **Monospace**: `ui-monospace, "SF Mono", Menlo, monospace` — code, timestamps, language labels, counters.

### Weights

Only three weights are ever used: **400** (default), **500** (emphasized labels, rows),
**600** (titles, section labels, headings). No bold/black.

### Scale

| Level | Size | Weight | Line height | Usage |
|---|---|---|---|---|
| Display | 20px | 500/600 | 1.3 | sheet actions |
| H1 | 19px | 600 | 1.3 | markdown heading 1 |
| H2 | 17px | 600 | 1.3 | markdown heading 2 |
| H3 | 15.5px | 600 | 1.3 | markdown heading 3 |
| Input | 16px | 400 | 1.45 | textarea / text field (never smaller — prevents iOS zoom) |
| Body | 15px | 400/500 | 1.5 | message bubbles, panel title |
| Body alt | 14.5px | 400 | 1.4 | menu items |
| Emphasized | 13.5px | 500 | 1.45 | list rows, notices |
| Secondary | 13px | 400/500/600 | 1.35–1.45 | cards, sheet copy |
| Code | 12.5px | 400 | 1.55 | code lines |
| Small | 12px | 400 | 1.4–1.5 | controls, chips, tool rows |
| Meta | 11.5px | 400 | — | message actions |
| Micro | 11px | 400/600 | 1.45 | section labels, timestamps, table headers |
| Nano | 10px | 400 | — | sort affordances |

### Treatments

- **Negative tracking** grows with size: `-.3px` on 15px titles, `-.18px` on 15px body,
  `-.15px` on 13px row text, `-.1px` on 14.5px menu items.
- **Positive tracking** reserved for uppercase micro-labels: `+.5px` at 11px/600, `+.3px` for table headers.
- Uppercase + semibold + muted color is the standard section-label treatment.
- `font-variant-numeric: tabular-nums` on counters, clocks and branch labels so digits don't jitter.
- Rendering: `-webkit-font-smoothing: antialiased`, `-moz-osx-font-smoothing: grayscale`,
  `text-rendering: optimizeLegibility`.
- Selection is intentional: body copy, message bubbles and code are selectable; chrome (buttons,
  labels, icons) is not.

---

## Spacing

### Base Unit

Not a strict multiple — a **2px-grained scale** that clusters on 4 / 6 / 8 / 12 / 16, with odd
5 / 7 / 11 / 13px values used deliberately for optical alignment inside compact controls.

### Common Values

- `2px` — icon-to-text micro gaps, badge offsets
- `4px` — tight control internals, dot indicators
- `6px` — chip/row inner rhythm
- `8px` — default gap between sibling controls, card padding
- `10–12px` — panel padding, composer padding
- `13–14px` — horizontal page gutters, bubble padding
- `16–18px` — sheet padding, empty-state padding
- `22–48px` — section and empty-state breathing room

### Layout

- Full-viewport fixed shell (`position: fixed; inset: 0`), no document scrolling — scrolling happens
  inside a single content region with `overscroll-behavior: contain` and hidden scrollbars.
- Gutters are safe-area aware: `calc(12px + env(safe-area-inset-top))`, `calc(14px + env(safe-area-inset-right))`,
  etc., exposed as `--sat / --sab / --sal / --sar`.
- Width caps: message bubble 85% of column; inline image `min(280px, 85vw)`; composer max height
  176px (input 160px); toasts `min(88vw, 320px)`; lightbox image `max 92vw / 86vh`; sheet/menu
  `min 200px / max 260px`.

---

## Elevation

### Shadows (CSS layer, used when WebGL glass is unavailable)

- **Glass / default surface**:
  `0 .7px 0 rgba(255,255,255,.105) inset, 0 -.45px 0 rgba(0,0,0,.14) inset, 0 0 0 .35px rgba(80,90,255,.07), 0 14px 42px rgba(0,0,10,.34)`
- **Filled action button**:
  `0 1px 0 rgba(255,255,255,.18) inset, 0 -.5px 0 rgba(0,40,28,.24) inset, 0 3px 16px rgba(80,90,255,.18)`
- **Accent message surface**:
  `0 5px 26px rgba(80,90,255,.15), 0 3px 18px rgba(11,0,246,.08), 0 1.2px 0 rgba(255,255,255,.22) inset, 0 -.7px 0 rgba(0,0,0,.16) inset`
- **Selected row**: `0 0 0 .35px rgba(80,90,255,.06), 0 8px 28px rgba(80,90,255,.05)`
- **Input focus glow**: `0 0 0 2.5px rgba(11,0,246,.118), 0 0 22px rgba(0,255,157,.045)`
- **Ambient overlay shadows**: `rgba(0,0,0,.28)` context scrim, `rgba(0,0,0,.38)` sheet scrim,
  `rgba(0,0,0,.5)` drawer scrim, `rgba(0,0,0,.82)` media lightbox.

### Glass optics (the real elevation system)

Rendered in WebGL rather than CSS. Parameters, in the renderer's own units:

- Refraction: edge thickness `20px` (× DPR), refraction factor `1.4`
- Dispersion (chromatic split): `7px` — red/blue channels sampled at 0.98× / 1.02× offsets
- Composite: `mix(blur, refracted, 0.92)` — 92% refracted, 8% raw blurred background
- Fresnel: range `30px`, hardness `20`, factor `20`; edge tint interpolates from `rgb(0.08,0.12,0.28)`
  toward a configurable tint (default white, alpha 0)
- Glare: range `30px`, hardness `20`, factor `28`, convergence `73.66`, opposite-side factor `40`,
  angle `-45°`, glare tint `rgb(0.1,0.15,0.35)`
- Cast shadow: expand `25`, factor `15`, offset `{ x: 0, y: -10 }`
- Shapes: max `48`, default 200×200 with radius `80`, roundness `5`, merge rate `0.05`
- Background blur radius `0` by default — depth comes from refraction, not from blurring

### Backdrop filters (CSS fallback tier)

- Primary glass: `blur(36px) saturate(190%) contrast(1.08) brightness(1.04)`
- Full-screen drawer: `blur(48px) saturate(185%) contrast(1.05)`
- Menus, cards, code, chips: `blur(18–28px) saturate(170–190%)`
- Scrims: `blur(4px)` (sheet) → `blur(12px)` (drawer backdrop) → `blur(22px)` (context scrim)

### Border Radii

- `3px` favicon chips · `6–8px` small buttons and inputs
- `10–14px` cards, images, code blocks, thumbnails
- `15–16px` list rows, toasts
- `18px` menus, sheets, search field
- `20px` message bubble · `23px` composer pill
- `50%` circular icon buttons, indicator dots, typing dots

### Layering

`-1` backdrop · `0` glass canvas · `1` content · `190` scroll-to-bottom · `200` composer ·
`309/310` drawer + scrim · `500` toasts / sheet · `600/610` context overlay + menu · `800` lightbox

---

## Interactive States

### Buttons

- **Default**: no fill — glass only (transparent background, hairline border, refraction handled by
  the renderer). Circular icon buttons are 36×36px.
- **Press**: spring-driven scale to `0.94` (mass 1, stiffness 300, damping 30) on pointerdown,
  returning to 1 on release; CSS fallbacks use `scale(.85)–(.97)` with opacity `~.72`.
- **Disabled**: reverts to neutral glass, icon stroke drops to `rgba(245,245,247,.28)`.
- **Filled/primary**: blue→mint gradient with white icon; a "stop" variant keeps the neutral glass
  shell but restores the primary icon.
- **Transition**: `transform .22s cubic-bezier(.34,1.4,.64,1), opacity .12s`; press micro-feedback `.08–.12s`.

### Rows & Cards

- Hover (fine pointers only, behind `@media (hover:hover) and (pointer:fine)`): background
  `rgba(255,255,255,.055)`, border `rgba(232,232,246,.118)`, `backdrop-filter: blur(22px) saturate(170%)`.
- Active/selected: blue-tinted gradient, border `rgba(80,90,255,.25)`, soft blue glow.
- Press: `scale(.97)`; destructive press tints red.

### Links & Inline Actions

- Mint accent color, no underline; underlined affordance only for explicit "expand" text controls
  (`text-underline-offset: 2px`).

### Form Inputs

- Composer pill: 23px radius, min-height 40px, max-height 176px.
- Focus-within: border `rgba(128,255,220,.2)`, inset hairlines retained, 2.5px blue focus ring plus a
  soft mint bloom (`0 0 22px rgba(0,255,157,.045)`).
- Placeholders use tertiary text (`rgba(245,245,247,.28)`); input font is locked to 16px.
- Keyboard focus: `outline: 2px solid rgba(100,120,255,.7); outline-offset: 2px`.

### Cursors & Feedback

`cursor: zoom-in` on images, `grab`/`grabbing` in the lightbox, default elsewhere; tap highlight and
callouts suppressed; markup carries haptic intents (light impact, medium impact, selection) for
press, send and sheet interactions.

---

## Motion

### Principles

Motion is physical and spring-led rather than linear. Anything a finger touches compresses and
springs back with visible overshoot; anything that travels across the screen decelerates like a
sheet of material, never like a fading box. Ambient motion is extremely slow (background drift time
scale `0.045`) so the interface feels alive but never busy. State changes are quick enough to feel
instrument-like (120–260ms), while panels and drawers take longer (320–420ms).

### Easing

| Curve | Character | Used for |
|---|---|---|
| `cubic-bezier(.34,1.4,.64,1)` | overshoot, springy | buttons, popovers, scroll-to-bottom |
| `cubic-bezier(.34,1.56,.64,1)` | strong overshoot | toast entrance |
| `cubic-bezier(.34,1.28,.64,1)` | bouncy height | expanding search field |
| `cubic-bezier(.32,.72,0,1)` | decelerating, no overshoot | bottom sheet |
| `cubic-bezier(.32,.72,0,1.18)` | decelerating with slight overshoot | side drawer |
| `cubic-bezier(.25,1,.5,1)` | ease-out | scrim fades |
| `cubic-bezier(.2,1,.3,1)` | gentle ease | row hover |

### Durations

- Press / drag: `.08s–.12s`
- Color, background, border: `.14s–.22s`
- Menus, popovers, chips: `.2s–.26s`
- Sheets, drawers, search reveal: `.32s–.42s`
- Toast in `.38s`, out `.18–.22s`, auto-dismiss at `2.8s`

### Springs

`GlassKit.Spring` presets (mass 1, rest delta 0.001): **smooth** `stiffness 100 / damping 20`,
**snappy** `300 / 30` (the default for press feedback), **bouncy** `180 / 12`. Exponential damping
helpers (`lambda`-based `damp()`) are available for continuous values.

### Loops & Ambient

- Typing indicator: three 7px dots, `1.3s` ease-in-out bounce, `.18s` stagger, opacity `.4 → 1`.
- Spinners: `1.1s` linear rotation.
- Copy confirmation: `.36s` scale pop to `1.08`.
- Glass: shape merging at rate `0.05`, size spring factor `10`, continuous rAF at DPR capped to 2,
  DOM re-collection every `600ms`, loop paused when the tab is hidden.

### Accessibility

`prefers-reduced-motion` zeroes all animations, transitions and smooth scrolling;
`prefers-reduced-transparency` strips every backdrop filter and substitutes a flat
`rgba(128,160,145,.13)` surface.

---

## Design Principles

1. **Material over metaphor.** Depth is simulated optically — refraction, dispersion, Fresnel rims
   and glare — not faked with gradients and drop shadows. The CSS layer exists only as a fallback.
2. **The field is dark, the content is light.** A near-black environment lets blue-to-mint accents
   and glass edges carry all the hierarchy; nothing competes with the content for brightness.
3. **Hairlines and half-pixels.** Structure is drawn with `0.45–0.5px` borders and single-pixel
   inset highlights. Restraint at the edges is what makes the glass believable.
4. **Native, quiet typography.** No webfonts, three weights, tight negative tracking. Type is a
   precision instrument, not a voice.
5. **Touch-first physics.** Every press compresses; every panel decelerates; every continuous value
   runs through a spring. Motion is how the material proves it has mass.
6. **Graceful degradation is designed, not patched.** Reduced motion, reduced transparency, missing
   WebGL2 and missing float buffers each have a deliberate, still-on-brand fallback.

---

## Implementation Notes

- **Provenance.** Values above are read from the shipped stylesheet, root custom-property block and
  the WebGL shader source of the same codebase that serves the audited URL. The live build adds a
  four-section tab shell (messaging, optics studio, control, media surface) rendered largely inside
  the canvas, so its DOM exposes only the tab labels and one line of descriptive text; the optics
  parameters documented above are the studio's own control set, read from the renderer's profile
  object. No product or interface copy is reproduced here.
- **Token architecture.** A single `:root` block holds the system: semantic text/background tokens
  (`--fg`, `--fg2`, `--fg3`, `--bg`, `--sep`, `--hover`), a glass family (`--liquid`, `--liquid2`,
  `--liquid3`, `--liquid-border`, `--liquid-border-strong`, `--liquid-inset`, `--liquid-edge`,
  `--liquid-shadow`), an input family (`--input-glow`), an action family (`--send-border`,
  `--send-inset`, `--send-shadow`), and safe-area tokens (`--sat/--sab/--sal/--sar`). Note: several
  legacy tokens are misnamed relative to their value (the "green" token resolves to white, the
  "blue" token to pure `#0000ff`), so read values, not names.
- **One class carries the material.** A single `.lg` utility applies the entire glass recipe
  (gradient fill, hairline border, inset highlight/shadow, cast shadow, backdrop filter) and is
  composed onto panels, pills, menus, toasts and chips.
- **WebGL-first, CSS-second.** A `.glass-host` opt-in list is collected by the renderer; when WebGL2
  or `EXT_color_buffer_float` is missing, an injected stylesheet restores the full CSS glass recipe
  with `!important`, and an override layer makes every glass element transparent so the canvas can
  draw it instead. Any new surface must be added to that selector list.
- **Mobile shell conventions.** Fixed `html/body`, `overscroll-behavior: none`, hidden scrollbars
  (`scrollbar-width: none`), 16px minimum input font, `touch-action` tuned per region, safe-area
  padding on all four edges, and PWA meta (`theme-color #050510`, `color-scheme: dark`).
- **Performance posture.** Device pixel ratio clamped to 2, half-float FBOs, separable two-pass
  Gaussian blur (kernel up to 201 taps), shape cap of 48, `contain: layout paint` and
  `will-change: transform` on glass elements, renderer paused on `visibilitychange`.
- **Debug affordance.** The shader exposes stepped debug outputs (SDF, normals, edge band, blur,
  refraction, Fresnel, glare, mask, background) — useful when re-tuning the optics.
