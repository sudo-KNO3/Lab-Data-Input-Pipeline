"""
Review Station — Human Validation UI for Lab Results

Streamlit web app for reviewing and correcting chemical matches.

Views:
  1. Pending Review   — results needing human validation (< 95% confidence)
  2. Unmatched         — results with no match at all (0% confidence)
  3. All Results       — full searchable/filterable table
  4. Submission Browser — review by file

Usage:
    streamlit run scripts/review_station.py
"""

import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st

# ═══════════════════════════════════════════════════════════════════════════════
#  Config
# ═══════════════════════════════════════════════════════════════════════════════

PROJECT_ROOT = Path(__file__).parent.parent
LAB_DB = str(PROJECT_ROOT / "data" / "lab_results.db")
MATCHER_DB = str(PROJECT_ROOT / "data" / "reg153_matcher.db")

st.set_page_config(
    page_title="Reg 153 — Review Station",
    page_icon="🔬",
    layout="wide",
)


# ═══════════════════════════════════════════════════════════════════════════════
#  Database helpers
# ═══════════════════════════════════════════════════════════════════════════════

def get_lab_conn():
    return sqlite3.connect(LAB_DB)


def get_matcher_conn():
    return sqlite3.connect(MATCHER_DB)


def get_file_path(filename: str) -> Path | None:
    """Look up the archived file_path for a given original_filename."""
    conn = get_lab_conn()
    row = conn.execute(
        "SELECT file_path FROM lab_submissions WHERE original_filename = ? LIMIT 1",
        (filename,),
    ).fetchone()
    conn.close()
    if row and row[0]:
        p = PROJECT_ROOT / row[0]
        return p if p.exists() else None
    return None


def render_file_preview(filename: str, key: str):
    """Show a preview button for an Excel source file. When clicked, renders the
    raw spreadsheet contents and a download button inside the review station."""
    fp = get_file_path(filename)
    if fp is None:
        st.caption(f"📄 {filename} (source file not found)")
        return

    if st.button(f"📄 Preview: {filename}", key=f"preview_{key}"):
        try:
            # Read all sheets
            sheets = pd.read_excel(fp, sheet_name=None, header=None, dtype=str)
            for sheet_name, sheet_df in sheets.items():
                st.markdown(f"**Sheet: {sheet_name}** ({len(sheet_df)} rows × {len(sheet_df.columns)} cols)")
                st.dataframe(sheet_df, width='stretch', hide_index=False, height=400)
        except Exception as e:
            st.error(f"Could not read file: {e}")

        # Download button
        with open(fp, "rb") as f:
            st.download_button(
                label=f"⬇️ Download {filename}",
                data=f.read(),
                file_name=filename,
                mime="application/vnd.ms-excel",
                key=f"download_{key}",
            )


@st.cache_data(ttl=30)
def load_analytes() -> pd.DataFrame:
    """Load all canonical analytes for the correction dropdown."""
    conn = get_matcher_conn()
    df = pd.read_sql_query(
        """SELECT a.analyte_id, a.preferred_name, a.analyte_type,
                  a.cas_number, a.group_code, a.chemical_group,
                  a.parent_analyte_id, p.preferred_name AS parent_name
           FROM analytes a
           LEFT JOIN analytes p ON a.parent_analyte_id = p.analyte_id
           ORDER BY a.preferred_name""",
        conn,
    )
    conn.close()
    return df


@st.cache_data(ttl=5)
def load_results(where_clause: str = "1=1", params: tuple = ()) -> pd.DataFrame:
    conn = get_lab_conn()
    # Also join the matcher DB to resolve analyte_id -> preferred_name
    conn.execute("ATTACH DATABASE ? AS matcher", (MATCHER_DB,))
    df = pd.read_sql_query(
        f"""
        SELECT
            r.result_id, r.submission_id,
            s.original_filename, s.lab_vendor,
            r.chemical_raw, r.chemical_normalized,
            r.analyte_id, a.preferred_name AS matched_analyte,
            r.correct_analyte_id,
            r.match_method, r.match_confidence,
            r.sample_id, r.client_id, r.sample_date,
            r.result_value, r.units, r.qualifier,
            r.detection_limit, r.lab_method, r.chemical_group,
            r.validation_status, r.validation_notes, r.medium
        FROM lab_results r
        JOIN lab_submissions s ON r.submission_id = s.submission_id
        LEFT JOIN matcher.analytes a ON r.analyte_id = a.analyte_id
        WHERE {where_clause}
        ORDER BY r.match_confidence ASC, r.chemical_raw
        """,
        conn,
        params=params,
    )
    conn.execute("DETACH DATABASE matcher")
    conn.close()
    return df


@st.cache_data(ttl=5)
def load_submissions() -> pd.DataFrame:
    conn = get_lab_conn()
    df = pd.read_sql_query(
        """
        SELECT s.submission_id, s.original_filename, s.lab_vendor,
               COUNT(r.result_id) as total_results,
               SUM(CASE WHEN r.validation_status = 'accepted' THEN 1 ELSE 0 END) as accepted,
               SUM(CASE WHEN r.validation_status = 'pending' THEN 1 ELSE 0 END) as pending,
               SUM(CASE WHEN r.match_confidence = 0 THEN 1 ELSE 0 END) as unmatched
        FROM lab_submissions s
        LEFT JOIN lab_results r ON s.submission_id = r.submission_id
        GROUP BY s.submission_id
        ORDER BY pending DESC, s.submission_id DESC
        """,
        conn,
    )
    conn.close()
    return df


@st.cache_data(ttl=5)
def get_stats() -> dict:
    conn = get_lab_conn()
    stats = {}
    stats["total_results"] = conn.execute("SELECT COUNT(*) FROM lab_results").fetchone()[0]
    stats["total_submissions"] = conn.execute("SELECT COUNT(*) FROM lab_submissions").fetchone()[0]
    stats["accepted"] = conn.execute(
        "SELECT COUNT(*) FROM lab_results WHERE validation_status = 'accepted'"
    ).fetchone()[0]
    stats["pending"] = conn.execute(
        "SELECT COUNT(*) FROM lab_results WHERE validation_status = 'pending'"
    ).fetchone()[0]
    stats["not_chemical"] = conn.execute(
        "SELECT COUNT(*) FROM lab_results WHERE validation_status = 'not_chemical'"
    ).fetchone()[0]
    stats["unmatched"] = conn.execute(
        "SELECT COUNT(*) FROM lab_results WHERE match_confidence = 0"
    ).fetchone()[0]
    stats["low_conf"] = conn.execute(
        "SELECT COUNT(*) FROM lab_results WHERE match_confidence > 0 AND match_confidence < 0.95"
    ).fetchone()[0]
    conn.close()
    return stats


def update_result(result_id: int, action: str, correct_analyte_id: str = None, notes: str = None):
    """Apply a validation action to a single result."""
    conn = get_lab_conn()
    if action == "accept":
        # Accept the current match
        conn.execute(
            """UPDATE lab_results 
               SET validation_status = 'accepted', 
                   correct_analyte_id = analyte_id,
                   human_override = 1,
                   validation_notes = ?
               WHERE result_id = ?""",
            (notes or "Manually accepted", result_id),
        )
    elif action == "correct":
        # Accept with a different analyte
        conn.execute(
            """UPDATE lab_results 
               SET validation_status = 'accepted', 
                   correct_analyte_id = ?,
                   human_override = 1,
                   validation_notes = ?
               WHERE result_id = ?""",
            (correct_analyte_id, notes or f"Corrected to {correct_analyte_id}", result_id),
        )
    elif action == "not_chemical":
        # Mark as not a chemical (header row, moisture, etc.)
        conn.execute(
            """UPDATE lab_results 
               SET validation_status = 'not_chemical', 
                   human_override = 1,
                   validation_notes = ?
               WHERE result_id = ?""",
            (notes or "Marked as not a chemical", result_id),
        )
    elif action == "reject":
        conn.execute(
            """UPDATE lab_results 
               SET validation_status = 'rejected', 
                   human_override = 1,
                   validation_notes = ?
               WHERE result_id = ?""",
            (notes or "Rejected", result_id),
        )
    conn.commit()
    conn.close()
    # Clear caches so the UI refreshes
    load_results.clear()
    get_stats.clear()
    load_submissions.clear()


def bulk_accept_chemical(chemical_raw: str):
    """Accept all pending results for a given chemical_raw (across all samples)."""
    conn = get_lab_conn()
    conn.execute(
        """UPDATE lab_results 
           SET validation_status = 'accepted',
               correct_analyte_id = analyte_id,
               human_override = 1,
               validation_notes = 'Bulk accepted via review station'
           WHERE chemical_raw = ? AND validation_status = 'pending' AND match_confidence > 0""",
        (chemical_raw,),
    )
    affected = conn.execute("SELECT changes()").fetchone()[0]
    conn.commit()
    conn.close()
    load_results.clear()
    get_stats.clear()
    load_submissions.clear()
    return affected


def bulk_not_chemical(chemical_raw: str):
    """Mark all results for a chemical_raw as not_chemical."""
    conn = get_lab_conn()
    conn.execute(
        """UPDATE lab_results 
           SET validation_status = 'not_chemical',
               human_override = 1,
               validation_notes = 'Bulk marked not_chemical via review station'
           WHERE chemical_raw = ? AND validation_status = 'pending'""",
        (chemical_raw,),
    )
    affected = conn.execute("SELECT changes()").fetchone()[0]
    conn.commit()
    conn.close()
    load_results.clear()
    get_stats.clear()
    load_submissions.clear()
    return affected


def add_synonym_and_accept(chemical_raw: str, analyte_id: str):
    """Add the raw name as a synonym in the matcher DB and accept all matching results."""
    # Add synonym
    matcher_conn = get_matcher_conn()
    from src.normalization.text_normalizer import TextNormalizer
    normalizer = TextNormalizer()
    norm = normalizer.normalize(chemical_raw)
    
    # Check if synonym already exists
    existing = matcher_conn.execute(
        "SELECT id FROM synonyms WHERE synonym_norm = ? AND analyte_id = ?",
        (norm, analyte_id),
    ).fetchone()
    
    if not existing:
        matcher_conn.execute(
            """INSERT INTO synonyms (analyte_id, synonym_raw, synonym_norm, synonym_type, 
                                     harvest_source, confidence, lab_vendor)
               VALUES (?, ?, ?, 'lab_observed', 'review_station', 1.0, '')""",
            (analyte_id, chemical_raw, norm),
        )
        matcher_conn.commit()
    matcher_conn.close()
    
    # Accept all matching results with this chemical
    lab_conn = get_lab_conn()
    lab_conn.execute(
        """UPDATE lab_results 
           SET validation_status = 'accepted',
               correct_analyte_id = ?,
               analyte_id = ?,
               match_method = 'review_station',
               match_confidence = 1.0,
               human_override = 1,
               validation_notes = 'Matched and synonym added via review station'
           WHERE chemical_raw = ? AND validation_status = 'pending'""",
        (analyte_id, analyte_id, chemical_raw),
    )
    affected = lab_conn.execute("SELECT changes()").fetchone()[0]
    lab_conn.commit()
    lab_conn.close()
    load_results.clear()
    get_stats.clear()
    load_submissions.clear()
    load_analytes.clear()
    return affected


# ═══════════════════════════════════════════════════════════════════════════════
#  UI Layout
# ═══════════════════════════════════════════════════════════════════════════════

st.title("🔬 Reg 153 — Review Station")

# ── Sidebar: Stats ────────────────────────────────────────────────────────────
stats = get_stats()
with st.sidebar:
    st.header("Database Summary")
    st.metric("Total Results", f"{stats['total_results']:,}")
    st.metric("Submissions", stats["total_submissions"])
    
    col1, col2 = st.columns(2)
    col1.metric("✅ Accepted", f"{stats['accepted']:,}")
    col2.metric("⏳ Pending", f"{stats['pending']:,}")
    
    col3, col4 = st.columns(2)
    col3.metric("❌ Unmatched", stats["unmatched"])
    col4.metric("⚠️ Low Conf", stats["low_conf"])
    
    pct = stats["accepted"] / stats["total_results"] * 100 if stats["total_results"] else 0
    st.progress(pct / 100, text=f"{pct:.1f}% validated")
    
    st.divider()
    if st.button("🔄 Refresh Data"):
        load_results.clear()
        get_stats.clear()
        load_submissions.clear()
        st.rerun()


# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_pending, tab_unmatched, tab_submissions, tab_all, tab_manage = st.tabs([
    f"⏳ Pending Review ({stats['pending']})",
    f"❌ Unmatched ({stats['unmatched']})",
    "📁 By Submission",
    "📊 All Results",
    "🧪 Manage Analytes",
])


# ═══════════════════════════════════════════════════════════════════════════════
#  Tab 1: Pending Review
# ═══════════════════════════════════════════════════════════════════════════════

with tab_pending:
    st.subheader("Results Needing Human Review")
    st.caption("These results matched below 95% confidence or have no match. "
               "Review each unique chemical and accept, correct, or reject.")
    
    df_pending = load_results("r.validation_status = 'pending' AND r.match_confidence > 0")
    
    if df_pending.empty:
        st.success("🎉 No pending results to review!")
    else:
        # Group by unique chemical for efficient review
        grouped = (
            df_pending.groupby("chemical_raw")
            .agg(
                count=("result_id", "size"),
                confidence=("match_confidence", "first"),
                matched_to=("analyte_id", "first"),
                matched_name=("matched_analyte", "first"),
                method=("match_method", "first"),
                sample_file=("original_filename", "first"),
                vendor=("lab_vendor", "first"),
            )
            .reset_index()
            .sort_values("confidence")
        )
        
        st.info(f"**{len(grouped)} unique chemicals** across {len(df_pending):,} results")
        
        for idx, row in grouped.iterrows():
            matched_display = row['matched_name'] or row['matched_to'] or '(none)'
            with st.expander(
                f"**{row['chemical_raw']}**  →  {matched_display}  "
                f"({row['confidence']:.0%}, {row['count']} results)",
                expanded=(row["confidence"] < 0.80),
            ):
                col_info, col_action = st.columns([3, 2])
                
                with col_info:
                    st.markdown(f"### Lab reported:  `{row['chemical_raw']}`")
                    st.write(f"**System matched to:** {matched_display} (`{row['matched_to']}`)")
                    st.write(f"**Match method:** {row['method']}")
                    st.write(f"**Confidence:** {row['confidence']:.1%}")
                    st.write(f"**Occurrences:** {row['count']} results")
                    st.write(f"**Source file:** {row['sample_file']} ({row['vendor']})")
                    render_file_preview(row['sample_file'], key=f"pending_{idx}")
                
                with col_action:
                    key_prefix = f"pending_{row['chemical_raw']}"
                    
                    if st.button(f"✅ Accept match ({row['count']} results)",
                                 key=f"{key_prefix}_accept", type="primary"):
                        n = bulk_accept_chemical(row["chemical_raw"])
                        st.success(f"Accepted {n} results")
                        st.rerun()
                    
                    if st.button(f"🚫 Not a chemical ({row['count']} results)",
                                 key=f"{key_prefix}_notchem"):
                        n = bulk_not_chemical(row["chemical_raw"])
                        st.success(f"Marked {n} as not_chemical")
                        st.rerun()
                    
                    # Correct to a different analyte
                    analytes_df = load_analytes()
                    options = analytes_df["preferred_name"].tolist()
                    selected = st.selectbox(
                        "Correct to:",
                        options=[""] + options,
                        key=f"{key_prefix}_correct_select",
                        format_func=lambda x: x if x else "— select analyte —",
                    )
                    if selected and st.button("✏️ Apply correction", key=f"{key_prefix}_correct_btn"):
                        aid = analytes_df.loc[
                            analytes_df["preferred_name"] == selected, "analyte_id"
                        ].iloc[0]
                        n = add_synonym_and_accept(row["chemical_raw"], aid)
                        st.success(f"Corrected {n} results to {selected} and added synonym")
                        st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
#  Tab 2: Unmatched
# ═══════════════════════════════════════════════════════════════════════════════

with tab_unmatched:
    st.subheader("Completely Unmatched Chemicals")
    st.caption("These chemicals had 0% match confidence — no candidate found in "
               "the synonym database. Assign the correct analyte to add it as a synonym.")
    
    df_unmatched = load_results("r.match_confidence = 0 AND r.validation_status = 'pending'")
    
    if df_unmatched.empty:
        st.success("🎉 No unmatched chemicals!")
    else:
        grouped_um = (
            df_unmatched.groupby("chemical_raw")
            .agg(
                count=("result_id", "size"),
                sample_file=("original_filename", "first"),
                vendor=("lab_vendor", "first"),
                units=("units", "first"),
                sample_result=("result_value", "first"),
            )
            .reset_index()
        )
        
        st.warning(f"**{len(grouped_um)} unique unmatched chemicals** across {len(df_unmatched)} results")
        
        analytes_df = load_analytes()
        options = analytes_df["preferred_name"].tolist()
        
        for idx, row in grouped_um.iterrows():
            with st.expander(f"**{row['chemical_raw']}** — {row['count']} result(s)", expanded=True):
                col_info, col_action = st.columns([3, 2])
                
                with col_info:
                    st.write(f"**Raw name:** {row['chemical_raw']}")
                    st.write(f"**Units:** {row['units']}")
                    st.write(f"**Sample value:** {row['sample_result']}")
                    st.write(f"**From:** {row['sample_file']} ({row['vendor']})")
                    st.write(f"**Occurrences:** {row['count']}")
                    render_file_preview(row['sample_file'], key=f"unmatched_{idx}")
                
                with col_action:
                    key_prefix = f"unmatched_{row['chemical_raw']}"
                    
                    # Assign to existing analyte
                    selected = st.selectbox(
                        "Match to analyte:",
                        options=[""] + options,
                        key=f"{key_prefix}_select",
                        format_func=lambda x: x if x else "— select analyte —",
                    )
                    if selected and st.button("✅ Match & add synonym", 
                                               key=f"{key_prefix}_match", type="primary"):
                        aid = analytes_df.loc[
                            analytes_df["preferred_name"] == selected, "analyte_id"
                        ].iloc[0]
                        n = add_synonym_and_accept(row["chemical_raw"], aid)
                        st.success(f"Matched to {selected}, updated {n} results, synonym added")
                        st.rerun()
                    
                    if st.button("🚫 Not a chemical", key=f"{key_prefix}_notchem"):
                        n = bulk_not_chemical(row["chemical_raw"])
                        st.success(f"Marked {n} as not_chemical")
                        st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
#  Tab 3: By Submission
# ═══════════════════════════════════════════════════════════════════════════════

with tab_submissions:
    st.subheader("Review by File Submission")
    
    df_subs = load_submissions()
    
    # Highlight rows that need attention
    st.dataframe(
        df_subs.style.apply(
            lambda row: [
                "background-color: #ffe0e0" if row["pending"] > 0 else ""
            ] * len(row),
            axis=1,
        ),
        width='stretch',
        hide_index=True,
    )
    
    # Drill into a specific submission
    sub_ids = df_subs["submission_id"].tolist()
    selected_sub = st.selectbox(
        "Select submission to review:",
        options=[""] + [str(s) for s in sub_ids],
        format_func=lambda x: (
            f"#{x} — {df_subs.loc[df_subs['submission_id'] == int(x), 'original_filename'].iloc[0]} "
            f"({df_subs.loc[df_subs['submission_id'] == int(x), 'pending'].iloc[0]} pending)"
            if x else "— select —"
        ),
    )
    
    if selected_sub:
        sub_id = int(selected_sub)
        df_sub_results = load_results("r.submission_id = ?", (sub_id,))
        
        sub_filename = df_subs.loc[df_subs['submission_id'] == sub_id, 'original_filename'].iloc[0]
        st.write(f"**{len(df_sub_results)} results** in this submission")
        render_file_preview(sub_filename, key=f"submission_{sub_id}")
        
        # Show summary statistics
        col1, col2, col3 = st.columns(3)
        col1.metric("Accepted", len(df_sub_results[df_sub_results["validation_status"] == "accepted"]))
        col2.metric("Pending", len(df_sub_results[df_sub_results["validation_status"] == "pending"]))
        col3.metric("Avg Confidence", f"{df_sub_results['match_confidence'].mean():.1%}")
        
        # Show the data — chemical_raw first and prominent
        display_cols = [
            "chemical_raw", "matched_analyte", "match_confidence",
            "match_method", "sample_id", "result_value", "units",
            "detection_limit", "validation_status", "result_id",
        ]
        
        # Color-code by status
        st.dataframe(
            df_sub_results[display_cols].style.apply(
                lambda row: [
                    "background-color: #e0ffe0" if row["validation_status"] == "accepted"
                    else "background-color: #ffe0e0" if row["validation_status"] == "pending"
                    else ""
                ] * len(row),
                axis=1,
            ),
            width='stretch',
            hide_index=True,
            height=500,
        )
        
        # Bulk actions for this submission
        st.divider()
        col_a, col_b = st.columns(2)
        with col_a:
            if st.button(f"✅ Accept all pending in submission #{sub_id}", type="primary"):
                conn = get_lab_conn()
                conn.execute(
                    """UPDATE lab_results 
                       SET validation_status = 'accepted',
                           correct_analyte_id = analyte_id,
                           human_override = 1,
                           validation_notes = 'Bulk accepted via review station (submission)'
                       WHERE submission_id = ? AND validation_status = 'pending' AND match_confidence > 0""",
                    (sub_id,),
                )
                affected = conn.execute("SELECT changes()").fetchone()[0]
                conn.commit()
                conn.close()
                load_results.clear()
                get_stats.clear()
                load_submissions.clear()
                st.success(f"Accepted {affected} results")
                st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
#  Tab 4: All Results
# ═══════════════════════════════════════════════════════════════════════════════

with tab_all:
    st.subheader("All Lab Results")
    
    # Filters
    col_f1, col_f2, col_f3, col_f4 = st.columns(4)
    with col_f1:
        vendor_filter = st.selectbox("Vendor", ["All", "SGS", "Caduceon", "Eurofins"])
    with col_f2:
        status_filter = st.selectbox("Status", ["All", "pending", "accepted", "not_chemical", "rejected"])
    with col_f3:
        conf_filter = st.selectbox("Confidence", ["All", "100%", "95-99%", "70-94%", "0%"])
    with col_f4:
        search = st.text_input("Search chemical name")
    
    # Build where clause
    conditions = []
    params = []
    if vendor_filter != "All":
        conditions.append("s.lab_vendor = ?")
        params.append(vendor_filter)
    if status_filter != "All":
        conditions.append("r.validation_status = ?")
        params.append(status_filter)
    if conf_filter == "100%":
        conditions.append("r.match_confidence >= 1.0")
    elif conf_filter == "95-99%":
        conditions.append("r.match_confidence >= 0.95 AND r.match_confidence < 1.0")
    elif conf_filter == "70-94%":
        conditions.append("r.match_confidence >= 0.70 AND r.match_confidence < 0.95")
    elif conf_filter == "0%":
        conditions.append("r.match_confidence = 0")
    if search:
        conditions.append("r.chemical_raw LIKE ?")
        params.append(f"%{search}%")
    
    where = " AND ".join(conditions) if conditions else "1=1"
    df_all = load_results(where, tuple(params))
    
    st.write(f"**{len(df_all):,} results** matching filters")
    
    st.dataframe(
        df_all,
        width='stretch',
        hide_index=True,
        height=600,
    )
    
    # Export filtered results
    if not df_all.empty:
        csv = df_all.to_csv(index=False)
        st.download_button(
            "📥 Download filtered results as CSV",
            data=csv,
            file_name="filtered_lab_results.csv",
            mime="text/csv",
        )


# ═══════════════════════════════════════════════════════════════════════════════
#  Tab 5: Manage Analytes
# ═══════════════════════════════════════════════════════════════════════════════

with tab_manage:
    st.subheader("Manage Analytes")
    st.caption(
        "Browse existing analytes and add new child analytes "
        "(e.g. individual PCB congeners under Polychlorinated biphenyls)."
    )

    analytes_df = load_analytes()

    # ── Browse existing analytes ──────────────────────────────────────────────
    st.markdown("### Current Analytes")
    group_filter = st.selectbox(
        "Filter by group",
        ["All"] + sorted(analytes_df["chemical_group"].dropna().unique().tolist()),
        key="manage_group_filter",
    )
    browse_df = analytes_df if group_filter == "All" else analytes_df[
        analytes_df["chemical_group"] == group_filter
    ]
    display_cols_manage = [
        "analyte_id", "preferred_name", "analyte_type",
        "cas_number", "group_code", "chemical_group", "parent_name",
    ]
    st.dataframe(
        browse_df[display_cols_manage].rename(columns={"parent_name": "Parent Analyte"}),
        width='stretch',
        hide_index=True,
        height=350,
    )
    st.write(f"**{len(browse_df)} analytes** shown")

    st.divider()

    # ── Add new child analyte ─────────────────────────────────────────────────
    st.markdown("### ➕ Add New Child Analyte")
    st.caption(
        "Create a new sub-compound under an existing parent analyte. "
        "For example, adding individual PCB congeners under the PCBs total suite."
    )

    # Parent analyte selector — only show suite/fraction/group types, or all
    parent_options = analytes_df["preferred_name"].tolist()
    selected_parent = st.selectbox(
        "Parent analyte:",
        options=[""] + parent_options,
        key="new_child_parent",
        format_func=lambda x: x if x else "— select parent analyte —",
    )

    col_name, col_cas = st.columns(2)
    with col_name:
        new_name = st.text_input("New analyte name", key="new_child_name",
                                 placeholder="e.g. Decachlorobiphenyl (PCB-209)")
    with col_cas:
        new_cas = st.text_input("CAS number (optional)", key="new_child_cas",
                                placeholder="e.g. 2051-24-3")

    new_synonyms_raw = st.text_area(
        "Additional synonyms (one per line)",
        key="new_child_synonyms",
        placeholder="PCB-209\nPCB 209\nDecachlorobiphenyl",
        height=100,
    )

    if selected_parent and new_name:
        parent_row = analytes_df[analytes_df["preferred_name"] == selected_parent].iloc[0]
        parent_id = parent_row["analyte_id"]
        parent_group = parent_row["group_code"] or ""
        parent_chem_group = parent_row["chemical_group"] or ""
        parent_table = int(parent_row.get("table_number", 0) or 0) or None

        # Auto-generate the next ID for this group
        group_prefix = parent_id.rsplit("_", 1)[0]  # e.g. REG153_PCBS
        existing_ids = analytes_df[
            analytes_df["analyte_id"].str.startswith(group_prefix + "_")
        ]["analyte_id"].tolist()
        max_num = max(
            (int(aid.rsplit("_", 1)[1]) for aid in existing_ids), default=0
        )
        next_id = f"{group_prefix}_{max_num + 1:03d}"

        st.info(
            f"**Preview:** `{next_id}` — {new_name}  \n"
            f"**Parent:** {selected_parent} (`{parent_id}`)  \n"
            f"**Group:** {parent_chem_group} / {parent_group}  \n"
            f"**CAS:** {new_cas or '(none)'}  \n"
            f"**Type:** single_substance"
        )

        if st.button("🧪 Create Child Analyte", type="primary", key="create_child_btn"):
            try:
                matcher_conn = get_matcher_conn()

                # Check if ID already exists
                existing = matcher_conn.execute(
                    "SELECT analyte_id FROM analytes WHERE analyte_id = ?", (next_id,)
                ).fetchone()
                if existing:
                    st.error(f"Analyte ID {next_id} already exists!")
                else:
                    from datetime import datetime as dt
                    now = dt.utcnow().isoformat()

                    # Insert analyte
                    matcher_conn.execute(
                        """INSERT INTO analytes
                           (analyte_id, preferred_name, analyte_type, cas_number,
                            group_code, table_number, chemical_group,
                            parent_analyte_id, created_at, updated_at)
                           VALUES (?, ?, 'SINGLE_SUBSTANCE', ?, ?, ?, ?, ?, ?, ?)""",
                        (next_id, new_name, new_cas or None,
                         parent_group, parent_table, parent_chem_group,
                         parent_id, now, now),
                    )

                    # Add the name itself as a synonym
                    from src.normalization.text_normalizer import TextNormalizer
                    normalizer = TextNormalizer()

                    all_synonyms = [new_name]
                    if new_cas:
                        all_synonyms.append(new_cas)
                    if new_synonyms_raw:
                        all_synonyms.extend(
                            line.strip()
                            for line in new_synonyms_raw.strip().split("\n")
                            if line.strip()
                        )

                    added_norms = set()
                    for syn_raw in all_synonyms:
                        norm = normalizer.normalize(syn_raw)
                        if norm in added_norms:
                            continue
                        added_norms.add(norm)
                        matcher_conn.execute(
                            """INSERT INTO synonyms
                               (analyte_id, synonym_raw, synonym_norm, synonym_type,
                                harvest_source, confidence, lab_vendor,
                                normalization_version, created_at)
                               VALUES (?, ?, ?, 'COMMON', 'review_station', 1.0, '', 1, ?)""",
                            (next_id, syn_raw, norm, now),
                        )

                    matcher_conn.commit()
                    matcher_conn.close()

                    load_analytes.clear()
                    st.success(
                        f"Created **{next_id}** — {new_name} "
                        f"with {len(added_norms)} synonym(s), "
                        f"linked to parent {selected_parent}"
                    )
                    st.rerun()

            except Exception as e:
                st.error(f"Error creating analyte: {e}")
    elif not selected_parent:
        st.warning("Select a parent analyte to continue.")
    elif not new_name:
        st.warning("Enter a name for the new analyte.")
