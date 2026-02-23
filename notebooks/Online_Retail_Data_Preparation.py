#!/usr/bin/env python3
"""
Data preparation pipeline covering:
1) Load from CSV / Excel / SQL
2) Understand (head, info, describe, missing values)
3) Clean (missing values, duplicates, data types)
4) Transform (standardization + categorical encoding)
5) Feature engineering + simple feature selection
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import sqlite3
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple
import xml.etree.ElementTree as ET


SHEET_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
WB_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"

NS_SHEET = {"a": SHEET_NS}
NS_REL = {"r": REL_NS}
NS_WB = {"a": SHEET_NS, "r": WB_NS}


def qi(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def normalize_header(header: str) -> str:
    text = re.sub(r"\s+", "_", str(header).strip().lower())
    text = re.sub(r"[^0-9a-zA-Z_]+", "_", text).strip("_")
    if not text:
        text = "column"
    if text[0].isdigit():
        text = f"c_{text}"
    return text


def make_unique_headers(headers: Sequence[str]) -> List[str]:
    out: List[str] = []
    used: Dict[str, int] = {}
    for header in headers:
        base = normalize_header(header)
        if base not in used:
            used[base] = 1
            out.append(base)
            continue
        used[base] += 1
        out.append(f"{base}_{used[base]}")
    return out


def col_to_index(cell_ref: str) -> int:
    letters = "".join(ch for ch in cell_ref if ch.isalpha()).upper()
    val = 0
    for ch in letters:
        val = val * 26 + (ord(ch) - ord("A") + 1)
    return max(val - 1, 0)


def get_shared_strings(zf: zipfile.ZipFile) -> List[str]:
    if "xl/sharedStrings.xml" not in zf.namelist():
        return []
    root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    values = []
    for si in root.findall("a:si", NS_SHEET):
        txt = "".join(t.text or "" for t in si.findall(".//a:t", NS_SHEET))
        values.append(txt)
    return values


def get_sheet_path(zf: zipfile.ZipFile, sheet_name: Optional[str]) -> str:
    workbook = ET.fromstring(zf.read("xl/workbook.xml"))
    sheets = workbook.findall("a:sheets/a:sheet", NS_WB)
    if not sheets:
        raise ValueError("No sheets found in workbook.")

    selected_rel_id: Optional[str] = None
    if sheet_name:
        for sheet in sheets:
            if sheet.attrib.get("name") == sheet_name:
                selected_rel_id = sheet.attrib.get(
                    "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"
                )
                break
        if selected_rel_id is None:
            available = [s.attrib.get("name", "") for s in sheets]
            raise ValueError(f"Sheet '{sheet_name}' not found. Available: {available}")
    else:
        selected_rel_id = sheets[0].attrib.get(
            "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"
        )

    rels = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
    target = None
    for rel in rels.findall("r:Relationship", NS_REL):
        if rel.attrib.get("Id") == selected_rel_id:
            target = rel.attrib.get("Target")
            break
    if not target:
        raise ValueError("Could not resolve worksheet path in workbook relations.")
    return target if target.startswith("xl/") else f"xl/{target}"


def read_cell_value(cell: ET.Element, shared: Sequence[str]) -> str:
    ctype = cell.attrib.get("t")
    if ctype == "inlineStr":
        t = cell.find("a:is/a:t", NS_SHEET)
        return (t.text or "") if t is not None else ""

    v = cell.find("a:v", NS_SHEET)
    if v is None:
        return ""
    text = v.text or ""
    if ctype == "s" and text.isdigit():
        idx = int(text)
        if 0 <= idx < len(shared):
            return shared[idx]
    return text


def iter_xlsx_rows(path: Path, sheet_name: Optional[str]) -> Iterator[Dict[str, str]]:
    with zipfile.ZipFile(path) as zf:
        shared = get_shared_strings(zf)
        ws_path = get_sheet_path(zf, sheet_name)
        headers: List[str] = []

        row_tag = f"{{{SHEET_NS}}}row"
        cell_tag = f"{{{SHEET_NS}}}c"

        with zf.open(ws_path) as ws:
            for _, elem in ET.iterparse(ws, events=("end",)):
                if elem.tag != row_tag:
                    continue

                cells: Dict[int, str] = {}
                for cell in elem.findall(cell_tag):
                    ref = cell.attrib.get("r", "")
                    idx = col_to_index(ref) if ref else len(cells)
                    cells[idx] = read_cell_value(cell, shared)

                if cells:
                    max_idx = max(cells.keys())
                    ordered = [cells.get(i, "") for i in range(max_idx + 1)]
                    if not headers:
                        headers = make_unique_headers(ordered)
                    else:
                        if len(ordered) < len(headers):
                            ordered.extend([""] * (len(headers) - len(ordered)))
                        yield {headers[i]: ordered[i] if i < len(ordered) else "" for i in range(len(headers))}

                elem.clear()


def iter_csv_rows(path: Path) -> Iterator[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise ValueError("CSV has no header row.")
        norm_headers = make_unique_headers(reader.fieldnames)
        original = list(reader.fieldnames)
        for row in reader:
            out = {}
            for i, src in enumerate(original):
                out[norm_headers[i]] = (row.get(src) or "").strip()
            yield out


def iter_sql_rows(path: Path, query: str) -> Iterator[Dict[str, str]]:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute(query)
        cols = [normalize_header(d[0]) for d in cur.description]
        cols = make_unique_headers(cols)
        for rec in cur:
            out = {}
            for i, col in enumerate(cols):
                val = rec[i]
                out[col] = "" if val is None else str(val).strip()
            yield out
    finally:
        conn.close()


def guess_source_type(path: Path, source_type: str) -> str:
    if source_type != "auto":
        return source_type
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return "csv"
    if suffix in {".xlsx", ".xlsm"}:
        return "excel"
    if suffix in {".db", ".sqlite", ".sqlite3"}:
        return "sql"
    raise ValueError(f"Cannot infer source type for file: {path}")


def parse_float(value: object) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    try:
        return float(text)
    except ValueError:
        cleaned = re.sub(r"[^0-9.\-]", "", text)
        if cleaned in {"", "-", ".", "-."}:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None


def excel_serial_to_datetime(serial: float) -> datetime:
    base = datetime(1899, 12, 30)
    return base + timedelta(days=float(serial))


def parse_datetime(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None

    maybe_float = parse_float(text)
    if maybe_float is not None and 20_000 <= maybe_float <= 80_000:
        try:
            dt = excel_serial_to_datetime(maybe_float)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass

    patterns = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%d-%m-%Y %H:%M",
        "%d-%m-%Y",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y",
    ]
    for pattern in patterns:
        try:
            dt = datetime.strptime(text, pattern)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    return None


def safe_real(value: object) -> Optional[float]:
    return parse_float(value)


def is_id_like(name: str) -> bool:
    low = name.lower()
    return any(token in low for token in ("_id", "id", "code", "no", "number"))


def slugify(text: str, max_len: int = 28) -> str:
    s = re.sub(r"[^0-9a-zA-Z]+", "_", str(text).strip().lower()).strip("_")
    if not s:
        s = "value"
    return s[:max_len]


def infer_types(col_stats: Dict[str, Dict[str, object]]) -> Dict[str, str]:
    inferred: Dict[str, str] = {}
    for col, st in col_stats.items():
        non_missing = int(st["non_missing"])
        numeric_hits = int(st["numeric_hits"])
        datetime_hits = int(st["datetime_hits"])

        name = col.lower()
        numeric_ratio = numeric_hits / max(non_missing, 1)
        datetime_ratio = datetime_hits / max(non_missing, 1)

        if "date" in name or "time" in name:
            inferred[col] = "datetime"
        elif is_id_like(name):
            inferred[col] = "categorical"
        elif numeric_ratio >= 0.9:
            inferred[col] = "numeric"
        elif datetime_ratio >= 0.9:
            inferred[col] = "datetime"
        else:
            inferred[col] = "categorical"
    return inferred


def describe_numeric_column(conn: sqlite3.Connection, table: str, col: str) -> Optional[Dict[str, float]]:
    qcol = qi(col)
    n = conn.execute(f"SELECT COUNT({qcol}) FROM {table} WHERE {qcol} IS NOT NULL").fetchone()[0]
    if not n:
        return None

    avg_, avg_sq, min_, max_ = conn.execute(
        f"SELECT AVG({qcol}), AVG({qcol} * {qcol}), MIN({qcol}), MAX({qcol}) FROM {table} WHERE {qcol} IS NOT NULL"
    ).fetchone()
    variance = max((avg_sq or 0.0) - (avg_ or 0.0) ** 2, 0.0)
    std_ = math.sqrt(variance)

    def percentile(p: float) -> float:
        idx = int((n - 1) * p)
        row = conn.execute(
            f"SELECT {qcol} FROM {table} WHERE {qcol} IS NOT NULL ORDER BY {qcol} LIMIT 1 OFFSET ?",
            (idx,),
        ).fetchone()
        return float(row[0]) if row else float("nan")

    return {
        "count": float(n),
        "mean": float(avg_ or 0.0),
        "std": float(std_),
        "min": float(min_ or 0.0),
        "25%": percentile(0.25),
        "50%": percentile(0.50),
        "75%": percentile(0.75),
        "max": float(max_ or 0.0),
    }


def export_table_csv(conn: sqlite3.Connection, table: str, out_path: Path, limit: Optional[int] = None) -> None:
    sql = f"SELECT * FROM {table}"
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    cur = conn.execute(sql)
    headers = [d[0] for d in cur.description]

    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for row in cur:
            writer.writerow(row)


def build_row_iterator(args: argparse.Namespace) -> Iterator[Dict[str, str]]:
    src = Path(args.input)
    source_type = guess_source_type(src, args.source_type)
    if source_type == "csv":
        return iter_csv_rows(src)
    if source_type == "excel":
        return iter_xlsx_rows(src, args.sheet)
    if source_type == "sql":
        if args.sql_query:
            query = args.sql_query
        elif args.sql_table:
            query = f"SELECT * FROM {args.sql_table}"
        else:
            raise ValueError("SQL source requires --sql-query or --sql-table.")
        return iter_sql_rows(src, query)
    raise ValueError(f"Unsupported source type: {source_type}")


def run_pipeline(args: argparse.Namespace) -> Dict[str, object]:
    source_path = Path(args.input).expanduser()
    output_dir = Path(args.output_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    db_path = output_dir / "prep_work.sqlite"
    if db_path.exists():
        db_path.unlink()

    rows = build_row_iterator(args)
    first = next(rows, None)
    if first is None:
        raise ValueError("No rows found in source.")

    headers = list(first.keys())
    columns = make_unique_headers(headers)

    conn = sqlite3.connect(db_path)
    conn.create_function("safe_real", 1, safe_real)
    conn.create_function("parse_datetime", 1, parse_datetime)

    try:
        conn.execute(
            "CREATE TABLE raw (" + ", ".join(f"{qi(col)} TEXT" for col in columns) + ")"
        )
        insert_sql = (
            f"INSERT INTO raw ({', '.join(qi(c) for c in columns)}) VALUES "
            f"({', '.join(['?'] * len(columns))})"
        )

        stats = {
            c: {"total": 0, "non_missing": 0, "missing": 0, "numeric_hits": 0, "datetime_hits": 0}
            for c in columns
        }

        def normalize_row(row: Dict[str, str]) -> Tuple[str, ...]:
            vals: List[str] = []
            for col in columns:
                raw_val = row.get(col, "")
                val = "" if raw_val is None else str(raw_val).strip()
                vals.append(val)
            return tuple(vals)

        def update_stats(values: Sequence[str]) -> None:
            for i, val in enumerate(values):
                col = columns[i]
                st = stats[col]
                st["total"] += 1
                if val == "":
                    st["missing"] += 1
                    continue
                st["non_missing"] += 1
                if parse_float(val) is not None:
                    st["numeric_hits"] += 1
                if parse_datetime(val) is not None:
                    st["datetime_hits"] += 1

        first_vals = normalize_row(first)
        update_stats(first_vals)
        batch: List[Tuple[str, ...]] = [first_vals]
        batch_size = 5000

        for row in rows:
            vals = normalize_row(row)
            update_stats(vals)
            batch.append(vals)
            if len(batch) >= batch_size:
                conn.executemany(insert_sql, batch)
                batch.clear()
        if batch:
            conn.executemany(insert_sql, batch)
        conn.commit()

        row_count = conn.execute("SELECT COUNT(*) FROM raw").fetchone()[0]
        inferred = infer_types(stats)

        head_rows = []
        cur = conn.execute("SELECT * FROM raw LIMIT 5")
        head_cols = [d[0] for d in cur.description]
        for row in cur.fetchall():
            head_rows.append({head_cols[i]: row[i] for i in range(len(head_cols))})

        missing_before = {c: int(stats[c]["missing"]) for c in columns}

        if args.missing_strategy == "drop":
            non_null_checks = " AND ".join(f"{qi(c)} IS NOT NULL AND TRIM({qi(c)}) <> ''" for c in columns)
            conn.execute(f"CREATE TABLE cleaned AS SELECT * FROM raw WHERE {non_null_checks}")
        else:
            select_exprs = []
            for c in columns:
                dtype = inferred[c]
                if dtype == "numeric":
                    select_exprs.append(f"safe_real({qi(c)}) AS {qi(c)}")
                elif dtype == "datetime":
                    select_exprs.append(f"parse_datetime({qi(c)}) AS {qi(c)}")
                else:
                    select_exprs.append(f"NULLIF(TRIM({qi(c)}), '') AS {qi(c)}")
            conn.execute("CREATE TABLE cleaned AS SELECT " + ", ".join(select_exprs) + " FROM raw")

            for c in columns:
                qcol = qi(c)
                dtype = inferred[c]
                if dtype == "numeric":
                    mean_val = conn.execute(f"SELECT AVG({qcol}) FROM cleaned WHERE {qcol} IS NOT NULL").fetchone()[0]
                    fill_val = float(mean_val) if mean_val is not None else 0.0
                    conn.execute(f"UPDATE cleaned SET {qcol} = ? WHERE {qcol} IS NULL", (fill_val,))
                elif dtype == "datetime":
                    mode_val = conn.execute(
                        f"SELECT {qcol} FROM cleaned WHERE {qcol} IS NOT NULL "
                        f"GROUP BY {qcol} ORDER BY COUNT(*) DESC LIMIT 1"
                    ).fetchone()
                    fill_val = mode_val[0] if mode_val else "1970-01-01 00:00:00"
                    conn.execute(f"UPDATE cleaned SET {qcol} = ? WHERE {qcol} IS NULL", (fill_val,))
                else:
                    mode_val = conn.execute(
                        f"SELECT {qcol} FROM cleaned WHERE {qcol} IS NOT NULL "
                        f"GROUP BY {qcol} ORDER BY COUNT(*) DESC LIMIT 1"
                    ).fetchone()
                    fill_val = mode_val[0] if mode_val else "Unknown"
                    conn.execute(f"UPDATE cleaned SET {qcol} = ? WHERE {qcol} IS NULL", (fill_val,))

        cleaned_before_dedup = conn.execute("SELECT COUNT(*) FROM cleaned").fetchone()[0]
        conn.execute("CREATE TABLE cleaned_dedup AS SELECT DISTINCT * FROM cleaned")
        conn.execute("DROP TABLE cleaned")
        conn.execute("ALTER TABLE cleaned_dedup RENAME TO cleaned")
        cleaned_after_dedup = conn.execute("SELECT COUNT(*) FROM cleaned").fetchone()[0]
        duplicates_removed = cleaned_before_dedup - cleaned_after_dedup

        missing_after = {}
        for c in columns:
            qcol = qi(c)
            missing_after[c] = conn.execute(
                f"SELECT COUNT(*) FROM cleaned WHERE {qcol} IS NULL OR TRIM(CAST({qcol} AS TEXT)) = ''"
            ).fetchone()[0]

        numeric_columns = [c for c in columns if inferred[c] == "numeric"]
        categorical_columns = [c for c in columns if inferred[c] == "categorical"]
        datetime_columns = [c for c in columns if inferred[c] == "datetime"]

        describe = {}
        for c in numeric_columns:
            stats_row = describe_numeric_column(conn, "cleaned", c)
            if stats_row:
                describe[c] = stats_row

        conn.execute("CREATE TABLE transformed AS SELECT * FROM cleaned")

        scaling = {}
        for c in numeric_columns:
            qcol = qi(c)
            mean_, avg_sq = conn.execute(
                f"SELECT AVG({qcol}), AVG({qcol} * {qcol}) FROM transformed"
            ).fetchone()
            mean_val = float(mean_ or 0.0)
            variance = max(float(avg_sq or 0.0) - mean_val * mean_val, 0.0)
            std_val = math.sqrt(variance)
            zcol = f"{c}_z"
            conn.execute(f"ALTER TABLE transformed ADD COLUMN {qi(zcol)} REAL")
            if std_val > 0:
                conn.execute(
                    f"UPDATE transformed SET {qi(zcol)} = ({qcol} - ?) / ?",
                    (mean_val, std_val),
                )
            else:
                conn.execute(f"UPDATE transformed SET {qi(zcol)} = 0.0")
            scaling[c] = {"mean": mean_val, "std": std_val}

        encoding_summary = {"one_hot": {}, "label_encoded": {}}
        added_columns = set(columns + [f"{c}_z" for c in numeric_columns])

        for idx, c in enumerate(categorical_columns):
            qcol = qi(c)
            unique_count = conn.execute(f"SELECT COUNT(DISTINCT {qcol}) FROM transformed").fetchone()[0]
            can_one_hot = (not is_id_like(c)) and 1 < unique_count <= int(args.onehot_max_levels)

            if can_one_hot:
                categories = [
                    row[0]
                    for row in conn.execute(
                        f"SELECT DISTINCT {qcol} FROM transformed ORDER BY {qcol}"
                    ).fetchall()
                ]
                out_cols = []
                for cat in categories:
                    base = f"{c}__{slugify(cat)}"
                    new_col = base
                    suffix = 2
                    while new_col in added_columns:
                        new_col = f"{base}_{suffix}"
                        suffix += 1
                    added_columns.add(new_col)
                    out_cols.append(new_col)

                    conn.execute(f"ALTER TABLE transformed ADD COLUMN {qi(new_col)} INTEGER")
                    conn.execute(
                        f"UPDATE transformed SET {qi(new_col)} = CASE WHEN {qcol} = ? THEN 1 ELSE 0 END",
                        (cat,),
                    )
                encoding_summary["one_hot"][c] = out_cols
            else:
                enc_col = f"{c}__encoded"
                if enc_col in added_columns:
                    enc_col = f"{enc_col}_{idx+1}"
                added_columns.add(enc_col)
                conn.execute(f"ALTER TABLE transformed ADD COLUMN {qi(enc_col)} INTEGER")

                map_table = f"map_{idx}_{c}"
                map_table = re.sub(r"[^0-9a-zA-Z_]+", "_", map_table)
                conn.execute(f"CREATE TEMP TABLE {qi(map_table)} (value TEXT PRIMARY KEY, code INTEGER)")
                labels = conn.execute(
                    f"SELECT DISTINCT {qcol} FROM transformed ORDER BY {qcol}"
                ).fetchall()
                conn.executemany(
                    f"INSERT INTO {qi(map_table)} (value, code) VALUES (?, ?)",
                    [(r[0], i) for i, r in enumerate(labels)],
                )
                conn.execute(
                    f"UPDATE transformed SET {qi(enc_col)} = (SELECT code FROM {qi(map_table)} m WHERE m.value = {qcol})"
                )
                conn.execute(f"DROP TABLE {qi(map_table)}")
                encoding_summary["label_encoded"][c] = enc_col

        engineered = []
        if "quantity" in columns and "unitprice" in columns:
            conn.execute("ALTER TABLE transformed ADD COLUMN total_amount REAL")
            conn.execute("UPDATE transformed SET total_amount = quantity * unitprice")
            engineered.append("total_amount")

        if "quantity" in columns:
            conn.execute("ALTER TABLE transformed ADD COLUMN is_return INTEGER")
            conn.execute("UPDATE transformed SET is_return = CASE WHEN quantity < 0 THEN 1 ELSE 0 END")
            engineered.append("is_return")

        if "invoicedate" in columns:
            conn.execute("ALTER TABLE transformed ADD COLUMN invoice_year INTEGER")
            conn.execute("ALTER TABLE transformed ADD COLUMN invoice_month INTEGER")
            conn.execute("ALTER TABLE transformed ADD COLUMN invoice_day INTEGER")
            conn.execute("ALTER TABLE transformed ADD COLUMN invoice_hour INTEGER")
            conn.execute("ALTER TABLE transformed ADD COLUMN invoice_weekday INTEGER")
            conn.execute(
                "UPDATE transformed SET "
                "invoice_year = CAST(strftime('%Y', invoicedate) AS INTEGER), "
                "invoice_month = CAST(strftime('%m', invoicedate) AS INTEGER), "
                "invoice_day = CAST(strftime('%d', invoicedate) AS INTEGER), "
                "invoice_hour = CAST(strftime('%H', invoicedate) AS INTEGER), "
                "invoice_weekday = CAST(strftime('%w', invoicedate) AS INTEGER)"
            )
            engineered.extend(
                ["invoice_year", "invoice_month", "invoice_day", "invoice_hour", "invoice_weekday"]
            )

        target_candidates = ["total_amount", "sales_amount", "revenue", "unitprice"]
        target = next((t for t in target_candidates if conn.execute(
            "SELECT COUNT(*) FROM pragma_table_info('transformed') WHERE name = ?",
            (t,),
        ).fetchone()[0]), None)

        table_info = conn.execute("PRAGMA table_info(transformed)").fetchall()
        numeric_like_cols = [
            row[1]
            for row in table_info
            if row[2].upper().startswith("INT") or row[2].upper().startswith("REAL")
        ]

        feature_scores = []
        if target and target in numeric_like_cols:
            qt = qi(target)
            for c in numeric_like_cols:
                if c == target:
                    continue
                qc = qi(c)
                avgx, avgy, avgxy, avgxx, avgyy = conn.execute(
                    f"SELECT AVG({qc}), AVG({qt}), AVG({qc}*{qt}), AVG({qc}*{qc}), AVG({qt}*{qt}) "
                    "FROM transformed"
                ).fetchone()
                if avgx is None or avgy is None:
                    continue
                varx = max((avgxx or 0.0) - (avgx or 0.0) ** 2, 0.0)
                vary = max((avgyy or 0.0) - (avgy or 0.0) ** 2, 0.0)
                if varx <= 1e-12 or vary <= 1e-12:
                    continue
                cov = (avgxy or 0.0) - (avgx or 0.0) * (avgy or 0.0)
                corr = cov / math.sqrt(varx * vary)
                feature_scores.append((c, abs(corr), varx))
            feature_scores.sort(key=lambda x: (x[1], x[2]), reverse=True)
        else:
            for c in numeric_like_cols:
                qc = qi(c)
                avg_, avg_sq = conn.execute(
                    f"SELECT AVG({qc}), AVG({qc}*{qc}) FROM transformed"
                ).fetchone()
                if avg_ is None:
                    continue
                var_ = max((avg_sq or 0.0) - (avg_ or 0.0) ** 2, 0.0)
                if var_ <= 1e-12:
                    continue
                feature_scores.append((c, 0.0, var_))
            feature_scores.sort(key=lambda x: x[2], reverse=True)

        top_k = max(int(args.top_features), 1)
        selected = [x[0] for x in feature_scores[:top_k]]
        if target and target not in selected:
            selected.append(target)
        if not selected:
            selected = numeric_like_cols[:top_k]

        conn.execute("DROP TABLE IF EXISTS selected_features")
        conn.execute(
            "CREATE TABLE selected_features AS SELECT "
            + ", ".join(qi(c) for c in selected)
            + " FROM transformed"
        )
        conn.commit()

        cleaned_csv = output_dir / "cleaned_data.csv"
        transformed_preview_csv = output_dir / "transformed_preview.csv"
        selected_csv = output_dir / "selected_features.csv"
        report_json = output_dir / "preparation_report.json"

        export_table_csv(conn, "cleaned", cleaned_csv)
        export_table_csv(conn, "transformed", transformed_preview_csv, limit=2000)
        export_table_csv(conn, "selected_features", selected_csv)

        report = {
            "source": str(source_path),
            "output_directory": str(output_dir),
            "row_counts": {
                "raw": int(row_count),
                "cleaned_before_dedup": int(cleaned_before_dedup),
                "cleaned_after_dedup": int(cleaned_after_dedup),
                "duplicates_removed": int(duplicates_removed),
            },
            "head": head_rows,
            "info": {
                "columns": columns,
                "inferred_types": inferred,
            },
            "missing_values": {
                "before_cleaning": missing_before,
                "after_cleaning": missing_after,
            },
            "describe": describe,
            "transform": {
                "standardized_numeric_columns": list(numeric_columns),
                "scaling_parameters": scaling,
                "categorical_encoding": encoding_summary,
            },
            "feature_engineering": {
                "created_features": engineered,
            },
            "feature_selection": {
                "target": target,
                "selected_features": selected,
                "top_scores": [
                    {"feature": f, "abs_corr": float(c), "variance": float(v)}
                    for f, c, v in feature_scores[:top_k]
                ],
            },
            "artifacts": {
                "cleaned_data_csv": str(cleaned_csv),
                "transformed_preview_csv": str(transformed_preview_csv),
                "selected_features_csv": str(selected_csv),
                "work_db": str(db_path),
            },
        }

        report_json.write_text(json.dumps(report, indent=2), encoding="utf-8")
        report["artifacts"]["report_json"] = str(report_json)
        return report
    finally:
        conn.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generic data preparation pipeline.")
    parser.add_argument("--input", required=True, help="Input path (.csv, .xlsx, .db/.sqlite)")
    parser.add_argument(
        "--source-type",
        choices=["auto", "csv", "excel", "sql"],
        default="auto",
        help="Source type. Default auto-infers from file extension.",
    )
    parser.add_argument("--sheet", default=None, help="Excel sheet name (optional).")
    parser.add_argument("--sql-query", default=None, help="SQL query for SQL sources.")
    parser.add_argument("--sql-table", default=None, help="SQL table for SQL sources.")
    parser.add_argument(
        "--missing-strategy",
        choices=["fill", "drop"],
        default="fill",
        help="Missing-value strategy.",
    )
    parser.add_argument(
        "--onehot-max-levels",
        type=int,
        default=40,
        help="Max distinct categories to one-hot encode.",
    )
    parser.add_argument(
        "--top-features",
        type=int,
        default=12,
        help="Number of features to keep in selected_features output.",
    )
    parser.add_argument(
        "--output-dir",
        default="./data_prep_output",
        help="Directory for output artifacts.",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    report = run_pipeline(args)
    print("Data preparation complete.")
    print(f"Input source: {report['source']}")
    print(f"Rows loaded: {report['row_counts']['raw']}")
    print(f"Rows after cleaning/dedup: {report['row_counts']['cleaned_after_dedup']}")
    print(f"Duplicates removed: {report['row_counts']['duplicates_removed']}")
    print("Selected features:", ", ".join(report["feature_selection"]["selected_features"]))
    print("Artifacts:")
    for k, v in report["artifacts"].items():
        print(f"  - {k}: {v}")


if __name__ == "__main__":
    main()
