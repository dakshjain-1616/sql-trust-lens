"""
SQL Trust Lens – Streamlit Application
Real-time schema validation for Text-to-SQL outputs.
"""

import json
import os
import csv
import io
from datetime import datetime, timezone
from typing import Optional

import streamlit as st
import pandas as pd

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from sql_trust_lens import EvalEngine, ValidationResult, LLMBackend

# ---------------------------------------------------------------------------
# Environment-driven configuration
# ---------------------------------------------------------------------------
APP_TITLE = os.getenv("APP_TITLE", "SQL Trust Lens")
DB_PATH = os.getenv("DB_PATH", ":memory:")
MAX_HISTORY = int(os.getenv("MAX_HISTORY", "50"))
ENABLE_SUGGESTIONS = os.getenv("ENABLE_SUGGESTIONS", "true").lower() != "false"

# ---------------------------------------------------------------------------
# Color / label helpers
# ---------------------------------------------------------------------------

def _trust_color(score: float) -> str:
    if score >= 80:
        return "#28a745"
    if score >= 50:
        return "#ffc107"
    return "#dc3545"


def _complexity_color(label: str) -> str:
    return {"simple": "#28a745", "moderate": "#ffc107", "complex": "#dc3545"}.get(label, "#6c757d")


def _render_trust_bar(score: float) -> None:
    color = _trust_color(score)
    label = "SAFE" if score >= 80 else ("CAUTION" if score >= 50 else "DANGER")
    st.markdown(
        f"""
        <div style="margin-top:8px;margin-bottom:4px;">
          <span style="font-weight:600;font-size:0.9rem;">Schema Health</span>
          <span style="margin-left:8px;padding:2px 8px;border-radius:4px;
                       background:{color};color:#fff;font-size:0.8rem;">
            {label}
          </span>
        </div>
        <div style="background:#e9ecef;border-radius:6px;height:18px;width:100%;">
          <div style="background:{color};border-radius:6px;height:18px;
                      width:{score:.0f}%;transition:width 0.4s ease;">
          </div>
        </div>
        <p style="margin-top:4px;font-size:1.1rem;font-weight:700;color:{color};">
          Trust Score: {score:.1f}%
        </p>
        """,
        unsafe_allow_html=True,
    )


def _highlight_sql(sql: str, invalid_tables: list, invalid_columns: dict) -> str:
    """Return an HTML-highlighted version of the SQL."""
    bad_tokens: set = set()
    for t in invalid_tables:
        bad_tokens.add(t.lower())
    for cols in invalid_columns.values():
        for c in cols:
            bad_tokens.add(c.lower())

    keywords = {
        "select", "from", "where", "join", "inner", "left", "right",
        "outer", "on", "group", "by", "order", "having", "limit",
        "as", "and", "or", "not", "in", "is", "null", "count",
        "sum", "avg", "min", "max", "distinct", "insert", "update",
        "delete", "create", "drop", "table", "into", "values",
    }

    import re
    tokens = re.findall(r"[a-zA-Z_][a-zA-Z0-9_]*|[^a-zA-Z_]+", sql)
    parts: list = []
    for tok in tokens:
        lower = tok.lower()
        if lower in bad_tokens:
            parts.append(
                f'<span style="background:#dc3545;color:#fff;border-radius:3px;'
                f'padding:1px 4px;font-weight:bold;" title="Hallucinated token">{tok}</span>'
            )
        elif lower in keywords:
            parts.append(
                f'<span style="color:#89b4fa;font-weight:600;">{tok}</span>'
            )
        else:
            parts.append(tok)

    return (
        '<pre style="background:#1e1e2e;color:#cdd6f4;padding:14px;'
        'border-radius:8px;font-size:0.9rem;overflow-x:auto;line-height:1.6;">'
        + "".join(parts)
        + "</pre>"
    )


def _render_issues(result: ValidationResult) -> None:
    if not result.issues:
        st.success("No schema violations detected.")
        return
    for issue in result.issues:
        st.error(f"⚠ {issue}")


def _render_complexity(result: ValidationResult) -> None:
    """Render SQL complexity metrics as a mini dashboard row."""
    if result.complexity is None:
        return
    c = result.complexity
    color = _complexity_color(c.complexity_label)
    badge = (
        f'<span style="background:{color};color:#fff;padding:2px 8px;'
        f'border-radius:4px;font-size:0.75rem;font-weight:600;">'
        f'{c.complexity_label.upper()}</span>'
    )
    st.markdown(
        f"**Complexity** {badge} &nbsp;"
        f"JOINs: **{c.join_count}** &nbsp;|&nbsp; "
        f"Subqueries: **{c.subquery_count}** &nbsp;|&nbsp; "
        f"Aggregations: **{c.aggregation_count}** &nbsp;|&nbsp; "
        f"WHERE conditions: **{c.where_conditions}**",
        unsafe_allow_html=True,
    )


def _render_schema_sidebar(engine: EvalEngine) -> None:
    st.sidebar.title("📊 Schema Browser")
    summary = engine.get_schema_summary()
    for table, cols in sorted(summary.items()):
        with st.sidebar.expander(f"🗂 {table} ({len(cols)} cols)"):
            for col in cols:
                col_info = engine.schema[table].columns[col]
                st.markdown(
                    f"`{col}` <small style='color:#888;'>— {col_info.data_type}</small>",
                    unsafe_allow_html=True,
                )


def _history_to_json(history: list) -> str:
    """Serialise session history to a JSON string."""
    export = []
    for entry in history:
        r: ValidationResult = entry["result"]
        export.append({
            "query": entry["query"],
            "sql": entry["sql"],
            "backend": entry.get("backend", "unknown"),
            "validated_at": entry.get("validated_at", ""),
            "trust_score": r.trust_score,
            "confidence": r.confidence,
            "can_execute": r.can_execute,
            "valid_tables": r.valid_tables,
            "invalid_tables": r.invalid_tables,
            "valid_columns": r.valid_columns,
            "invalid_columns": r.invalid_columns,
            "issues": r.issues,
            "suggestions": r.suggestions,
            "complexity": r.complexity.model_dump() if r.complexity else None,
            "row_count": r.row_count,
        })
    return json.dumps(export, indent=2)


def _history_to_csv(history: list) -> str:
    """Serialise session history to a CSV string."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([
        "validated_at", "query", "trust_score", "confidence",
        "can_execute", "invalid_tables", "invalid_columns",
        "complexity_label", "complexity_score", "backend",
    ])
    for entry in history:
        r: ValidationResult = entry["result"]
        writer.writerow([
            entry.get("validated_at", ""),
            entry["query"],
            r.trust_score,
            r.confidence,
            r.can_execute,
            ";".join(r.invalid_tables),
            ";".join(f"{t}:{','.join(c)}" for t, c in r.invalid_columns.items()),
            r.complexity.complexity_label if r.complexity else "",
            r.complexity.complexity_score if r.complexity else "",
            entry.get("backend", ""),
        ])
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Streamlit app entry point
# ---------------------------------------------------------------------------

def main() -> None:
    st.set_page_config(
        page_title=APP_TITLE,
        page_icon="🔍",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # -- Init engine (cached in session_state) --
    if "engine" not in st.session_state:
        st.session_state.engine = EvalEngine(db_path=DB_PATH)
    engine: EvalEngine = st.session_state.engine

    # -- Init LLM backend --
    if "llm_backend" not in st.session_state:
        st.session_state.llm_backend = LLMBackend()
    backend: LLMBackend = st.session_state.llm_backend

    # -- History --
    if "history" not in st.session_state:
        st.session_state.history = []

    # -- Sidebar --
    _render_schema_sidebar(engine)

    with st.sidebar:
        st.markdown("---")
        st.subheader("⚙ Settings")

        # Backend status badge
        bn = backend.backend_name
        badge_color = "#28a745" if bn.startswith("openrouter") else ("#0066cc" if bn == "mock" else "#6c757d")
        st.markdown(
            f'<small>Backend: <span style="background:{badge_color};color:#fff;'
            f'padding:1px 6px;border-radius:3px;font-size:0.75rem;">{bn}</span></small>',
            unsafe_allow_html=True,
        )

        # Only show mock toggle if no real LLM is configured
        if not bn.startswith("openrouter") and not bn.startswith("llama"):
            mock_enabled = st.toggle(
                "Mock LLM (keyword SQL)",
                value=backend.use_mock,
                help="Disable to use the hardcoded FALLBACK_SQL",
            )
            backend.use_mock = mock_enabled

        # OpenRouter hint
        if not os.getenv("OPENROUTER_API_KEY"):
            st.info("Set OPENROUTER_API_KEY to enable real LLM text-to-SQL.", icon="💡")

        st.markdown("---")

        # Export history buttons
        if st.session_state.history:
            st.subheader("📤 Export History")
            st.download_button(
                label="⬇ Download JSON",
                data=_history_to_json(st.session_state.history),
                file_name=f"sql_trust_lens_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}.json",
                mime="application/json",
            )
            st.download_button(
                label="⬇ Download CSV",
                data=_history_to_csv(st.session_state.history),
                file_name=f"sql_trust_lens_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}.csv",
                mime="text/csv",
            )
            if st.button("🗑 Clear History"):
                st.session_state.history = []
                st.rerun()

        st.markdown("---")
        st.caption("Northwind demo dataset · DuckDB in-memory")

    # -- Main header --
    st.title(f"🔍 {APP_TITLE}")
    st.markdown(
        "Real-time schema validation for Text-to-SQL. "
        "Ask a question in plain English – see the generated SQL **audited** "
        "against the live schema before any bytes hit the database."
    )

    # -- Quick-demo buttons --
    st.markdown("##### Quick demos")
    col1, col2, col3, col4, col5 = st.columns(5)
    demo_query: Optional[str] = None
    if col1.button("✅ Valid query"):
        demo_query = "Show all users"
    if col2.button("❌ Missing table"):
        demo_query = "Show data from fake table"
    if col3.button("⚠ Bad column"):
        demo_query = "Select bad column from users"
    if col4.button("🔗 JOIN example"):
        demo_query = "List customers with their orders"
    if col5.button("📊 Aggregation"):
        demo_query = "Count orders per customer"

    st.markdown("---")

    # -- Chat input --
    user_input = st.chat_input("Ask a question about the data…")
    if demo_query and not user_input:
        user_input = demo_query

    if user_input:
        schema_hint = json.dumps(engine.get_schema_summary(), indent=2)
        sql, used_backend = backend.generate(user_input, schema_hint)

        result: ValidationResult = engine.validate_sql(sql)
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        entry = {
            "query": user_input,
            "sql": sql,
            "result": result,
            "backend": used_backend,
            "validated_at": ts,
        }

        history = st.session_state.history
        history.insert(0, entry)
        if len(history) > MAX_HISTORY:
            history.pop()

    # -- Render history --
    for entry in st.session_state.history:
        q = entry["query"]
        sql = entry["sql"]
        r: ValidationResult = entry["result"]
        ts = entry.get("validated_at", "")
        used_backend = entry.get("backend", "")

        with st.container(border=True):
            hdr_col, meta_col = st.columns([3, 1])
            with hdr_col:
                st.markdown(f"**🗣 {q}**")
            with meta_col:
                if ts:
                    st.caption(f"🕐 {ts}")
                if used_backend:
                    st.caption(f"🤖 {used_backend}")

            # Trust bar
            _render_trust_bar(r.trust_score)

            # Confidence indicator (only interesting when < 1.0)
            if r.confidence < 1.0:
                st.caption(
                    f"⚡ Validation confidence: {r.confidence:.0%} "
                    "(some column-table assignments were inferred)"
                )

            # Highlighted SQL
            st.markdown(
                _highlight_sql(sql, r.invalid_tables, r.invalid_columns),
                unsafe_allow_html=True,
            )

            # Issues
            _render_issues(r)

            # Execution result
            if r.can_execute and r.execution_result:
                with st.expander(f"▶ Results – {r.execution_result}"):
                    try:
                        df = engine.execute_sql(sql)
                        st.dataframe(df, use_container_width=True)
                    except Exception as exc:
                        st.error(f"Execution error: {exc}")
            elif r.execution_error:
                st.warning(f"Execution blocked: {r.execution_error}")

            # Details expander
            with st.expander("🔎 Validation details"):
                # Complexity row
                _render_complexity(r)
                st.markdown("---")

                c1, c2 = st.columns(2)
                with c1:
                    st.markdown("**Valid tables**")
                    if r.valid_tables:
                        for t in r.valid_tables:
                            st.success(f"✓ {t}")
                    else:
                        st.write("—")
                    st.markdown("**Invalid tables**")
                    if r.invalid_tables:
                        for t in r.invalid_tables:
                            hint = r.suggestions.get(t, "")
                            label = f"✗ {t}"
                            if hint and ENABLE_SUGGESTIONS:
                                label += f"  →  did you mean `{hint}`?"
                            st.error(label)
                    else:
                        st.write("—")
                with c2:
                    st.markdown("**Valid columns**")
                    if r.valid_columns:
                        for tbl, cols in r.valid_columns.items():
                            st.success(f"✓ {tbl}.{', '.join(cols)}")
                    else:
                        st.write("—")
                    st.markdown("**Invalid columns**")
                    if r.invalid_columns:
                        for tbl, cols in r.invalid_columns.items():
                            for col in cols:
                                hint = r.suggestions.get(col, "")
                                label = f"✗ {tbl}.{col}"
                                if hint and ENABLE_SUGGESTIONS:
                                    label += f"  →  did you mean `{hint}`?"
                                st.error(label)
                    else:
                        st.write("—")

        st.markdown("")

    if not st.session_state.history:
        st.info(
            "👆 Type a question above or click a quick-demo button to see "
            "SQL Trust Lens in action."
        )


if __name__ == "__main__":
    main()
