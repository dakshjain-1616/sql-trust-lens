"""
03 – Custom Config
Shows how to customise behaviour via environment variables and constructor args.
Covers: DB_PATH, ENABLE_SUGGESTIONS, FALLBACK_SQL, USE_MOCK_LLM, LLM_MODEL.
Run from any directory:  python examples/03_custom_config.py
"""
import sys, os; sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

# Set env vars BEFORE importing the package so module-level constants pick them up.
os.environ.setdefault("DB_PATH", ":memory:")          # keep in-memory for this example
os.environ.setdefault("ENABLE_SUGGESTIONS", "true")   # show did-you-mean hints
os.environ.setdefault("USE_MOCK_LLM", "true")         # use keyword mock (no API key needed)
os.environ.setdefault("FALLBACK_SQL", "SELECT * FROM products LIMIT 5")
os.environ.setdefault("LLM_MODEL", "google/gemini-2.5-flash-lite")

from sql_trust_lens import EvalEngine, LLMBackend, FALLBACK_SQL

# ── 1. Engine with explicit DB path ───────────────────────────────────────
db_path = os.environ["DB_PATH"]
engine = EvalEngine(db_path=db_path)
print(f"Engine connected to: {db_path!r}")
print(f"Tables loaded: {sorted(engine.get_schema_summary().keys())}\n")

# ── 2. Inspect the active LLM backend ────────────────────────────────────
backend = LLMBackend()
print(f"Active LLM backend : {backend.backend_name}")
print(f"Mock LLM enabled   : {backend.use_mock}")

# ── 3. Toggle mock off → FALLBACK_SQL takes over ─────────────────────────
backend.use_mock = False
sql_off, name_off = backend.generate("show me products")
print(f"\nWith mock=False, backend={name_off!r}, SQL -> {sql_off!r}")

# ── 4. Toggle mock back on and generate SQL from a prompt ─────────────────
backend.use_mock = True
sql_on, name_on = backend.generate("show me products")
print(f"With mock=True,  backend={name_on!r},  SQL -> {sql_on!r}")

# ── 5. Validate both results ──────────────────────────────────────────────
print()
for label, sql in [("fallback", FALLBACK_SQL), ("mock", sql_on)]:
    r = engine.validate_sql(sql)
    print(f"  [{label:8s}] trust={r.trust_score:5.1f}%  can_execute={r.can_execute}")

engine.close()
