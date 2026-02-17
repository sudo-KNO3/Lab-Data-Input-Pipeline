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


@st.cache_data(ttl=30)
def load_analytes() -> pd.DataFrame:
    """Load all canonical analytes for the correction dropdown."""
    conn = get_matcher_conn()
    df = pd.read_sql_query(
        "SELECT analyte_id, preferred_name FROM analytes ORDER BY preferred_name",
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
            r.validation_status, r.validation_notes
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
tab_pending, tab_unmatched, tab_submissions, tab_all = st.tabs([
    f"⏳ Pending Review ({stats['pending']})",
    f"❌ Unmatched ({stats['unmatched']})",
    "📁 By Submission",
    "📊 All Results",
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
        
        st.write(f"**{len(df_sub_results)} results** in this submission")
        
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
