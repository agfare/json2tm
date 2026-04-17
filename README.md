# json2tm

Convert JSON translation files (EN / DE / RU) into **TMX** and **XLSX** translation memories.

---

## Features

- Converts deeply nested JSON translation files into industry-standard TMX and Excel formats
- Supports both `snake_case` (`label_en`) and `camelCase` (`labelEn`) field naming conventions
- Processes a single file triplet or an entire directory tree recursively
- Deduplicates segments globally across all processed files
- Validates JSON structure, UUID field format, and translation field types before extraction
- Warns on structural mismatches between language files (missing keys, array length differences)
- Runs 7 translation quality checks per segment pair and flags or excludes bad segments
- Live progress bars for extraction and output writing (requires `tqdm`)
- Prints a full processing summary: files loaded, segments created, duplicates skipped, QA issues, errors

---

## Requirements

- Python 3.10+
- `tqdm` — progress bars
- `openpyxl` — XLSX output

```bash
pip install tqdm openpyxl
```

Both dependencies are optional. The script runs without them but will skip XLSX output if `openpyxl` is missing, and will print plain progress messages instead of bars if `tqdm` is missing.

---

## JSON structure

The script expects three files with the same nested structure. Translatable fields are identified by their language suffix:

| Style | EN source | DE target | RU target |
|-------|-----------|-----------|-----------|
| snake_case | `label_en` | `label_de` | `label_ru` |
| camelCase | `labelEn` | `labelDe` | `labelRu` |

Any field whose name ends with one of these suffixes is treated as translatable. All other fields are treated as structural and recursed into. Fields ending in `_de` / `_ru` (or `De` / `Ru`) are read as target text and not recursed into.

Example input structure:

```json
{
  "categoryUuid": "1bf0c51a-026a-4130-a423-f6d48264452d",
  "categoryName": "Andrology",
  "labelEn": "Diseases of male genital organs",
  "pathologies": [
    {
      "types": [
        {
          "bundles": [
            {
              "groups": [
                {
                  "group_uuid": "05d20a20-9e6e-4bf0-9eef-4d4c896a9ef1",
                  "group_name_en": "Integument",
                  "group_name_de": null,
                  "group_name_ru": null,
                  "parts": [
                    {
                      "part_uuid": "79a04a2d-7ffe-42dc-9a49-ac87eb2fc0e9",
                      "label_en": "Skin",
                      "label_de": null,
                      "label_ru": null,
                      "article_en": null,
                      "article_de": null,
                      "article_ru": null
                    }
                  ]
                }
              ]
            }
          ]
        }
      ]
    }
  ]
}
```

---

## Usage

### Single file mode

Pass one JSON file per language. All three flags are required.

```bash
python json2tm.py \
  --en translations_en.json \
  --de translations_de.json \
  --ru translations_ru.json
```

### Directory mode (recursive)

Pass a directory per language. The script recursively finds every `*.json` file under the EN directory and pairs it with the file at the same relative path in the DE and RU directories.

```bash
python json2tm.py \
  --en data/en/ \
  --de data/de/ \
  --ru data/ru/
```

Expected layout:

```
data/
├── en/
│   ├── andrology.json
│   ├── cardiology.json
│   └── subspecialties/
│       └── neurology.json
├── de/
│   ├── andrology.json
│   ├── cardiology.json
│   └── subspecialties/
│       └── neurology.json
└── ru/
    ├── andrology.json
    ├── cardiology.json
    └── subspecialties/
        └── neurology.json
```

Files are matched by relative path: `en/subspecialties/neurology.json` → `de/subspecialties/neurology.json` → `ru/subspecialties/neurology.json`. Missing counterparts are reported as load failures but do not stop the rest of the run.

### Strict mode (clean output only)

Add `--strict` to exclude any segment that fails a QA check from the TMX and XLSX output:

```bash
python json2tm.py \
  --en data/en/ \
  --de data/de/ \
  --ru data/ru/ \
  --strict
```

Without `--strict` all segments are written and QA issues are visible in the XLSX **QA Issues** column. With `--strict` only segments that pass all checks are written.

---

## Flags

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `--en FILE/DIR` | Yes | — | English source file or directory |
| `--de FILE/DIR` | Yes | — | German target file or directory |
| `--ru FILE/DIR` | Yes | — | Russian target file or directory |
| `--out DIR` | No | `output/` | Output directory |
| `--strict` | No | off | Exclude segments that fail any QA check from all output |
| `--same-keys` | No | off | Use when all three files share the same field names (e.g. all use `label_en`) and the text values differ per language |
| `--text-fields FIELD[,...]` | No | — | Comma-separated plain field names (no language suffix) whose values are translatable; matched across files by JSON path |
| `--no-tmx` | No | off | Skip TMX output |
| `--no-xlsx` | No | off | Skip XLSX output |

---

## Output

All output files are written to the directory specified by `--out` (default: `output/`).

### TMX files

Two TMX 1.4 files, one per language pair:

| File | Content |
|------|---------|
| `en-de.tmx` | English → German translation units |
| `en-ru.tmx` | English → Russian translation units |

Each file contains only clean `<tu>` / `<tuv>` / `<seg>` elements with no metadata or UUIDs. Language codes follow ISO 639-1 (`en`, `de`, `ru`). The files are compatible with SDL Trados, memoQ, OmegaT, and any other TMX 1.4-compliant CAT tool.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE tmx SYSTEM "tmx14.dtd">
<tmx version="1.4">
  <header creationtool="json2tm" creationtoolversion="1.0"
          datatype="plaintext" segtype="sentence"
          adminlang="en" srclang="en" o-tmf="json2tm"/>
  <body>
    <tu>
      <tuv xml:lang="en"><seg>Skin</seg></tuv>
      <tuv xml:lang="de"><seg>Haut</seg></tuv>
    </tu>
  </body>
</tmx>
```

### XLSX file

`translations.xlsx` contains three sheets:

| Sheet | Content |
|-------|---------|
| **Summary** | Total segments, EN→DE count, EN→RU count, generation timestamp |
| **EN-DE** | Row per segment: `#`, Segment ID, EN source, DE target, Context UUID, JSON path, QA issues |
| **EN-RU** | Same layout for EN→RU pairs |

Row highlighting in the translation sheets:

| Colour | Meaning |
|--------|---------|
| Red | Segment has at least one QA error |
| Amber | Segment has QA warnings only |
| None | Segment is clean |

---

## Structural validation

Before extraction the script runs the following checks on each file:

| Check | Severity |
|-------|----------|
| JSON syntax and UTF-8 encoding | Error — file is skipped |
| UUID fields match standard UUID format | Error |
| Language-tagged fields contain strings or null | Error |
| Language-tagged fields contain empty strings | Warning |
| Key-set mismatch between EN and DE/RU at the same path | Warning |
| Array length mismatch between EN and DE/RU at the same path | Warning |
| Missing DE or RU translation for an EN source field | Warning |

Structural errors cause the affected file triplet to be skipped. Warnings are collected and printed in the summary but do not stop processing.

---

## QA checks

After deduplication, every extracted segment pair is checked for translation quality. QA runs separately for each language pair (EN→DE and EN→RU).

| Code | Level | Description |
|------|-------|-------------|
| `UNTRANSLATED` | Warning | Target text is identical to the source. Only applied to strings longer than 5 characters that contain lowercase letters, so short abbreviations (OK, DNA, MRI) are not flagged. |
| `LENGTH_RATIO` | Warning | The character-count ratio between target and source is outside the expected range. Thresholds: EN→DE 0.25–4.0×, EN→RU 0.20–5.0×. Only applied to strings of 10 or more characters. |
| `NUMBER_MISMATCH` | Error | Digit sequences present in the source are absent from, or changed in, the target — or extra digits appear in the target. |
| `PLACEHOLDER_MISMATCH` | Error | Interpolation markers (`{var}`, `{{var}}`, `%s`, `%d`, `%(name)s`, etc.) differ between source and target. |
| `NO_CYRILLIC` | Warning | The Russian target contains no Cyrillic characters, despite the source containing lowercase Latin letters. |
| `WRONG_SCRIPT` | Warning | The German target contains Cyrillic characters, suggesting the file may be mislabelled or swapped. |
| `TAG_MISMATCH` | Error | HTML or XML tags present in the source are absent from or different in the target. |

**Without `--strict`** all segments are included in the output. QA issues are recorded in the **QA Issues** column of the XLSX file and counted in the processing summary.

**With `--strict`** any segment that has at least one QA issue (error or warning) is excluded from the TMX and XLSX output entirely. The count of excluded segments is shown in the summary.

---

## Deduplication

A segment is considered a duplicate if its `(en, de, ru)` text triplet was already seen in a previous file or earlier in the same file. The first occurrence is kept; all subsequent ones are counted in *Skipped — duplicate* in the summary. Deduplication is global across the entire run.

---

## `--same-keys` mode

By default the script looks for language-specific field names: it reads `label_en` from the EN file and looks for `label_de` in the DE file at the same path.

If your files all use the same field names and the language is determined by which file you are reading — e.g. the DE file has `label_en` but its value is in German — pass `--same-keys`:

```bash
python json2tm.py --en en.json --de de.json --ru ru.json --same-keys
```

In this mode the value of every `*_en` / `*En` field is read from each respective file as-is.

---

## `--text-fields` mode

By default the script only extracts fields that carry an explicit language suffix
(`label_en`, `labelEn`, etc.).  Some JSON structures also contain **plain-name fields** — fields
with no language tag in their name — where the text value simply differs between the EN, DE and RU
files at the same JSON path.  Common examples: `categoryName`, `pathologyName`,
`pathology_type_name`.

Pass `--text-fields` with a comma-separated list of those field names to include them in the
output:

```bash
python json2tm.py \
  --en data/en/ --de data/de/ --ru data/ru/ \
  --text-fields categoryName,pathology_name,pathology_type_name
```

**How matching works**

The script walks all three JSON trees simultaneously (lock-step by structure).  When it reaches
a dict that contains one of the listed field names, it reads:

- the EN value from the EN file's dict
- the DE value from the DE file's dict at the **same JSON path**
- the RU value from the RU file's dict at the **same JSON path**

A UUID found in the same enclosing object is captured as the segment's **Context UUID** in the
XLSX output — confirming that the three dicts represent the same record.

**When to use it vs. `--same-keys`**

| Scenario | Flag to use |
|----------|-------------|
| All fields use `*_en` / `*En` names; the DE file has the same names but with German text | `--same-keys` |
| Some fields have no language suffix at all (`categoryName`, etc.) and you want only those specific fields extracted | `--text-fields categoryName,...` |
| Both situations apply to different fields in the same files | Use both flags together |

---

## Example output

```
── Directory mode  (3 file triplets) ────────────────────────
  ✓  [1/3] EN  andrology.json  (84,201 bytes)
  ✓  [1/3] DE  andrology.json  (91,455 bytes)
  ✓  [1/3] RU  andrology.json  (98,302 bytes)
  [1/3] lint [EN]  ✓
  [1/3] lint [DE]  ✓
  [1/3] lint [RU]  ✓
  [1/3] extracting: 100%|██████████| 1,842 fields [00:00]
  ✓  [2/3] EN  cardiology.json  (76,540 bytes)
  ...

  5,214 candidate segments collected across all files

── Deduplicating ────────────────────────────────────────────
  308 duplicates removed → 4,906 unique segments

── QA checks ────────────────────────────────────────────────
  ⚠  143 issue(s) found
  4,906 segments ready for output

── Writing TMX ──────────────────────────────────────────────
  ✓  en-de.tmx  (4,906 TUs, 1,204.3 KB)
  ✓  en-ru.tmx  (4,906 TUs, 1,318.7 KB)

── Writing XLSX ─────────────────────────────────────────────
  ✓  translations.xlsx  (4,906 segments, 2,847.1 KB)

════════════════════════════════════════════════════════════
                     PROCESSING SUMMARY
════════════════════════════════════════════════════════════
  Files processed:               9
    ✓  loaded OK:                9
    ✗  load failures:            0
────────────────────────────────────────────────────────────
  Segments created:              4,906
  Skipped — duplicate:           308
  Skipped — null/empty:          147
────────────────────────────────────────────────────────────
  QA issues found:               143
    excluded (--strict):         0
────────────────────────────────────────────────────────────
  Struct errors:                 0
  Struct warnings:               0
════════════════════════════════════════════════════════════
```

---

## Exit codes

| Code | Meaning |
|------|---------|
| `0` | Success (QA issues and warnings do not affect exit code) |
| `1` | One or more structural errors were encountered |
