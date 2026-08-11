# Dummy EPG Template Engine

Shared template engine used by the dummy EPG system to render channel titles,
descriptions, and URLs from regex groups. The Python engine lives in
`backend/template_engine.py` and is mirrored one-for-one by
`frontend/src/utils/templateEngine.ts` so the in-browser live preview and the
server-side XMLTV output are always byte-identical.

> **Supported surface: Dummy EPG Profiles.** This engine backs the **Dummy EPG
> Profiles** section of the EPG Manager tab (the `dummy_epg_profiles` table and
> the `/api/dummy-epg/*` endpoints) — the supported way to author dummy EPG in
> ECM, and the one Event Sync integrates with. A second, older surface exists:
> Dispatcharr-native EPG sources of `source_type=dummy` (created via the legacy
> "Dummy EPG Sources" section) also store the same fields in `custom_properties`.
> That legacy path is **deprecated** (bead 09x38.4): its section only appears
> when such sources already exist, and it no longer offers new-creation.
> Existing legacy sources are grandfathered (still editable); nothing is
> deleted. New dummy EPG should be authored as a Dummy EPG Profile.

## Syntax

### Placeholders

- `{name}` — insert the value of the named regex group (or an empty string if
  it's absent).
- `{name_normalize}` — legacy shortcut preserved from the pre-v0.14 engine:
  lowercase the value and strip everything that isn't `a-z` or `0-9`.

### Pipes

Chain left-to-right with `|`; each pipe receives the previous pipe's output.

| Pipe | Effect |
|-|-|
| `uppercase` | `str.upper()` |
| `lowercase` | `str.lower()` |
| `titlecase` | Title-case (first letter of each word) |
| `trim` | Strip leading & trailing whitespace |
| `strip:<chars>` | Strip any of `<chars>` from both ends |
| `replace:<from>:<to>` | Replace every occurrence (`to` may be empty) |
| `normalize` | Same as the `_normalize` suffix |
| `lookup:<table>` | Resolve the value through a table; miss → passthrough |

### Conditionals

Content inside `{if:...}...{/if}` renders only when the condition is true.
Conditionals may nest; no `{else}` branch.

| Form | Evaluates true when… |
|-|-|
| `{if:group}…{/if}` | Group value is non-empty |
| `{if:group=value}…{/if}` | Group value equals `value` exactly |
| `{if:group~regex}…{/if}` | Regex matches the group value |

Invalid regex inside a conditional evaluates to **false** (the engine never
throws from a typo). Oversized regex (> 500 chars) also evaluates to false,
which prevents catastrophic backtracking on untrusted input.

### Lookup tables

Two sources resolve at render time, merged with **inline overrides global**:

- **Inline** — `inline_lookups` on the dummy EPG source's custom_properties,
  or equivalent field on the `POST /api/dummy-epg/preview` request.
- **Global** — saved tables managed under *Settings → Lookup Tables*, attached
  to a source by ID via `global_lookup_ids`.

Referencing a table that doesn't exist raises `TemplateSyntaxError`. The
higher-level `render_template()` wrapper in `dummy_epg_engine.py` catches this
and falls back to the raw template text so a single profile typo can't tank
an XMLTV refresh — the broken tokens become visible in the output, which is
the intended signal to the user.

## Limits

| Limit | Value | Behavior on violation |
|-|-|-|
| Template length | 4096 chars | `TemplateSyntaxError` |
| Group value length | 1024 chars | Silently truncated before any transform or regex |
| Conditional regex length | 500 chars | Conditional evaluates false |

## Example

```
{league|uppercase}: {if:team}{team|titlecase}{/if}
```

With groups `league=nfl, team=chiefs` → `NFL: Chiefs`.
With `team` absent → `NFL: `.

With `team=chiefs` and a global lookup table `teams={chiefs: "Kansas City Chiefs"}`:

```
{league|uppercase}: {team|lookup:teams}
```

→ `NFL: Kansas City Chiefs`.

## Trace mode

Both engines expose a trace-producing variant used by the enhanced preview UI:

- Python: `TemplateEngine.render_with_trace(template, groups, lookups) -> (str, list[dict])`
- TypeScript: `new TemplateEngine().renderWithTrace(template, groups, lookups) -> { output, trace }`

A `trace` is a list of `TraceStep` entries:

```json
[
  {"kind": "literal", "text": "Go "},
  {
    "kind": "placeholder",
    "raw": "{team|titlecase}",
    "group_name": "team",
    "initial_value": "chiefs",
    "pipes": [
      {"transform": "titlecase", "arg": null, "input": "chiefs", "output": "Chiefs"}
    ],
    "final_value": "Chiefs"
  },
  {
    "kind": "conditional",
    "condition": "season=2026",
    "kind_detail": "equality",
    "taken": false,
    "value": "2025",
    "body": []
  }
]
```

Lookup pipes additionally carry `{source: <table>, matched: bool}`. The trace
preserves order, so rendering the `output` strings concatenated from each
step reproduces the final output exactly.

## Per-variant program duration

A profile carries one `program_duration` in minutes, and every event it renders
gets that length. A pattern variant can set its own `program_duration` to
override it, so a profile whose default is 180 can still give baseball 240
minutes by putting the longer value on the variant that matches baseball
channels.

| Where | Key | Type | Meaning |
|-|-|-|-|
| Profile | `program_duration` | integer, required, default 180 | Length of every event the profile renders |
| Pattern variant | `program_duration` | integer 0 to 1440, optional | Length of every event this variant matches |

The variant key is optional and **absent means "use the profile value"**, so a
profile written before this key existed keeps rendering exactly as it did. The
profile editor leaves the variant's Program Duration field blank in that case;
clearing the field removes the key again rather than writing a number back.

What each stored value resolves to, with a profile default of 180:

| Stored on the variant | Resolves to |
|-|-|
| key absent | 180 |
| `null` | 180, the same as absent |
| `300` | 300 |
| `0` | 0, honoured rather than read as unset |
| `"240"` | 240, a numeric string is read as a number |

### A name matching no variant

Variants are tried in order and the first whose patterns match wins. A name that
matches none of them falls back to the **profile's own** `title_pattern`,
`time_pattern` and `date_pattern`, and to the profile's `program_duration`.

This matters because a variant list is a set of special cases, not a replacement
for the profile. Without the fallback, adding a single variant to a working
profile would leave every channel that variant does not match with no parsed
start at all, and each one would render as a single full-day block instead of
its event.

A variant should therefore carry the profile's `time_pattern` and `date_pattern`
unless it genuinely needs different ones. `extract_groups_from_variants` passes
all three of a variant's patterns to the extractor, so a variant that sets only
`title_pattern` loses the event's start time even when it matches.

The floor is 0 rather than 1 because the engine honours a stored 0. Values
outside 0 to 1440 are rejected with HTTP 422 by `PatternVariantModel` in
`backend/routers/dummy_epg.py` on create, update and preview. The YAML import
path stores variants without that model, so
`backend/tasks/rule_lint_scan.py` re-checks stored profiles and reports a
duration it cannot read, or one outside the range, as a lint finding in the UI.

## Ended templates label the block after the predicted end (opt-in)

`ended_title_template` and `ended_description_template` exist at both profile
level and variant level. When set, they title the block that runs from the
event's predicted end to midnight, so an over event reads as over in the
guide instead of looking active until midnight. When left empty, that block
keeps the event's own title, so a broadcast running past its predicted length
is not declared finished mid-air.

Which way to lean is the operator's call, because the predicted end is a
guess (event start plus `program_duration`): set the templates and a game in
overtime reads as ended while still playing; leave them empty and a finished
game reads as active until midnight. Either way the block never carries the
live or new tag.

Variants inherit the profile-level ended templates only when the profile
templates' placeholders resolve; a variant whose patterns capture different
groups (e.g. `away`/`home` instead of `title`) must declare its own ended
templates, exactly like the upcoming pair.

## Related files

- `backend/template_engine.py`, `backend/tests/unit/test_template_engine.py`
- `backend/dummy_epg_engine.py` — calls `render_template()` from the engine
- `backend/routers/dummy_epg.py` — `/preview`, `/preview/batch`, `include_trace`
- `backend/routers/lookup_tables.py` — CRUD for global tables
- `frontend/src/utils/templateEngine.ts`, `frontend/src/utils/templateEngine.test.ts`
- `frontend/src/components/TemplateHelp.tsx` — in-app syntax reference
- `frontend/src/components/settings/LookupTableSection.tsx` — global table management UI
- `frontend/src/components/DummyEPGSourceModal.tsx` — inline tables + global attachment + preview UI
