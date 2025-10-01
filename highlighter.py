#!/usr/bin/env python3
import argparse
from pathlib import Path
from typing import Set
from openpyxl import load_workbook


def normalize(s: str, ci: bool) -> str:
    s = s.strip()
    return s.lower() if ci else s


def read_targets(path: Path, ci: bool) -> Set[str]:
    targets = set()
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if s:
                targets.add(normalize(s, ci))
    return targets


def mark_yes_in_col_f(
    xlsx: Path,
    out: Path,
    sheet_name: str | None,
    lines_path: Path,
    case_insensitive: bool,
    has_header: bool,
    clear_existing_f: bool,
) -> None:
    targets = read_targets(lines_path, case_insensitive)
    if not targets:
        print("[WARN] No targets found in lines file (after trimming). Exiting.")
        return

    wb = load_workbook(filename=str(xlsx))
    ws = wb[sheet_name] if sheet_name else wb.active

    start_row = 2 if has_header else 1
    max_row = ws.max_row

    if clear_existing_f:
        for r in range(start_row, max_row + 1):
            ws.cell(row=r, column=6).value = None  # Column F

    found_count = 0
    scanned = 0

    for r in range(start_row, max_row + 1):
        a_val = ws.cell(row=r, column=1).value  # Column A
        if a_val is None:
            continue
        scanned += 1
        if normalize(str(a_val), case_insensitive) in targets:
            ws.cell(row=r, column=6).value = "YES"  # Column F
            found_count += 1

    wb.save(str(out))
    print(f"[INFO] Sheet: '{ws.title}'")
    print(f"[INFO] Targets loaded: {len(targets)}")
    print(f"[INFO] Rows scanned (Column A): {scanned}")
    print(f"[INFO] Rows marked YES in Column F: {found_count}")
    print(f"[INFO] Output written to: {out}")


def main():
    p = argparse.ArgumentParser(description="Write 'YES' to Column F for rows whose Column A matches any line in a text file.")
    p.add_argument("--xlsx", required=True, help="Path to input .xlsx file.")
    p.add_argument("--sheet", default=None, help="Worksheet name (defaults to active sheet).")
    p.add_argument("--lines", required=True, help="Path to text file with one value per line.")
    p.add_argument("--out", default=None, help="Output .xlsx (default: <input>_marked.xlsx).")
    p.add_argument("--case-insensitive", action="store_true", help="Case-insensitive matching.")
    p.add_argument("--has-header", action="store_true", help="Treat row 1 as header (start from row 2).")
    p.add_argument("--clear-f", action="store_true", help="Clear Column F before marking YES.")
    args = p.parse_args()

    xlsx = Path(args.xlsx)
    out = Path(args.out) if args.out else xlsx.with_name(xlsx.stem + "_marked.xlsx")

    mark_yes_in_col_f(
        xlsx=xlsx,
        out=out,
        sheet_name=args.sheet,
        lines_path=Path(args.lines),
        case_insensitive=args.case_insensitive,
        has_header=args.has_header,
        clear_existing_f=args.clear_f,
    )


if __name__ == "__main__":
    main()
