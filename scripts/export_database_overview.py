"""Export both databases to a single browsable Excel workbook."""
import sqlite3
import pandas as pd
from pathlib import Path

PROJECT = Path(__file__).parent.parent
OUT = PROJECT / "reports" / "database_overview.xlsx"
OUT.parent.mkdir(parents=True, exist_ok=True)

lab_db = str(PROJECT / "data" / "lab_results.db")
matcher_db = str(PROJECT / "data" / "reg153_matcher.db")

with pd.ExcelWriter(OUT, engine="openpyxl") as writer:

    # ── Lab Submissions ───────────────────────────────────────────────
    conn = sqlite3.connect(lab_db)
    df_sub = pd.read_sql_query(
        "SELECT * FROM lab_submissions ORDER BY submission_id", conn
    )
    df_sub.to_excel(writer, sheet_name="Submissions", index=False)
    print(f"Submissions: {len(df_sub)} rows")

    # ── Lab Results ───────────────────────────────────────────────────
    df_res = pd.read_sql_query("""
        SELECT
            r.result_id, r.submission_id,
            s.original_filename, s.lab_vendor,
            r.chemical_raw, r.chemical_normalized,
            r.analyte_id, r.correct_analyte_id,
            r.match_method, r.match_confidence,
            r.sample_id, r.client_id, r.sample_date,
            r.result_value, r.units, r.qualifier,
            r.detection_limit, r.lab_method, r.chemical_group,
            r.validation_status, r.human_override, r.validation_notes
        FROM lab_results r
        JOIN lab_submissions s ON r.submission_id = s.submission_id
        ORDER BY r.submission_id, r.row_number
    """, conn)
    df_res.to_excel(writer, sheet_name="Lab Results", index=False)
    print(f"Lab Results: {len(df_res)} rows")

    # ── Pending Review ────────────────────────────────────────────────
    df_pending = df_res[df_res["validation_status"] == "pending"].copy()
    df_pending.to_excel(writer, sheet_name="Needs Review", index=False)
    print(f"Needs Review: {len(df_pending)} rows")

    conn.close()

    # ── Analytes (canonical list) ─────────────────────────────────────
    conn2 = sqlite3.connect(matcher_db)
    df_analytes = pd.read_sql_query(
        "SELECT * FROM analytes ORDER BY analyte_id", conn2
    )
    df_analytes.to_excel(writer, sheet_name="Analytes", index=False)
    print(f"Analytes: {len(df_analytes)} rows")

    # ── Synonyms ──────────────────────────────────────────────────────
    df_syn = pd.read_sql_query("""
        SELECT s.id, s.analyte_id, a.preferred_name,
               s.synonym_raw, s.synonym_norm, s.synonym_type,
               s.harvest_source, s.confidence, s.lab_vendor
        FROM synonyms s
        JOIN analytes a ON s.analyte_id = a.analyte_id
        ORDER BY a.preferred_name, s.synonym_raw
    """, conn2)
    df_syn.to_excel(writer, sheet_name="Synonyms", index=False)
    print(f"Synonyms: {len(df_syn)} rows")

    # ── Summary stats ─────────────────────────────────────────────────
    conn_lab = sqlite3.connect(lab_db)
    summary_data = []
    summary_data.append(("Total Lab Submissions", len(df_sub)))
    summary_data.append(("Total Lab Results", len(df_res)))
    summary_data.append(("", ""))

    for row in conn_lab.execute(
        "SELECT lab_vendor, COUNT(*) FROM lab_submissions GROUP BY lab_vendor ORDER BY lab_vendor"
    ):
        summary_data.append((f"Submissions — {row[0]}", row[1]))
    summary_data.append(("", ""))

    for row in conn_lab.execute(
        "SELECT validation_status, COUNT(*) FROM lab_results GROUP BY validation_status ORDER BY validation_status"
    ):
        summary_data.append((f"Results — {row[0]}", row[1]))
    summary_data.append(("", ""))

    summary_data.append(("Total Analytes", len(df_analytes)))
    summary_data.append(("Total Synonyms", len(df_syn)))
    conn_lab.close()

    for row in conn2.execute(
        "SELECT harvest_source, COUNT(*) FROM synonyms GROUP BY harvest_source ORDER BY harvest_source"
    ):
        summary_data.append((f"Synonyms — {row[0]}", row[1]))

    conn2.close()

    df_summary = pd.DataFrame(summary_data, columns=["Metric", "Value"])
    df_summary.to_excel(writer, sheet_name="Summary", index=False)

    # ── Auto-width columns ────────────────────────────────────────────
    for ws in writer.book.worksheets:
        for col in ws.columns:
            max_len = max(len(str(cell.value or "")) for cell in col)
            header_len = len(str(col[0].value or ""))
            ws.column_dimensions[col[0].column_letter].width = min(
                max(max_len, header_len) + 2, 50
            )

print(f"\nExported to: {OUT}")
