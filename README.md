# json2tm

# Usage

`pip install tqdm openpyxl`

```
python json2tm.py \
  --en translations_en.json \
  --de translations_de.json \
  --ru translations_ru.json \
  --out output/
```

# Flags
  
| Flag    | Purpose |
| -------- | ------- |
| --out DIR  | Output directory (default: output/)    |
| --same-keys | All three files use identical field names; text values differ per language     |
| --no-tmx    | Skip TMX output    |
| --no-xlsx    | Skip XLSX output    |
