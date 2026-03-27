"""
02 – Advanced Usage
Demonstrates hallucination detection, did-you-mean suggestions, JOIN validation,
complexity metrics, and batch validation.
Run from any directory:  python examples/02_advanced_usage.py
"""
import sys, os; sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from sql_trust_lens import EvalEngine, ValidationResult

engine = EvalEngine()

# ── 1. Hallucinated table ──────────────────────────────────────────────────
r: ValidationResult = engine.validate_sql("SELECT * FROM fake_table")
print("=== Hallucinated table ===")
print(f"  Trust Score    : {r.trust_score}%")
print(f"  Invalid tables : {r.invalid_tables}")
print(f"  Issues         : {r.issues}")

# ── 2. Hallucinated column + did-you-mean suggestion ──────────────────────
r = engine.validate_sql("SELECT product_name, fake_discount FROM products")
print("\n=== Hallucinated column ===")
print(f"  Trust Score     : {r.trust_score}%")
print(f"  Invalid columns : {r.invalid_columns}")
print(f"  Suggestions     : {r.suggestions}")

# ── 3. Multi-table JOIN with qualified columns ─────────────────────────────
r = engine.validate_sql(
    "SELECT c.company_name, o.order_id, o.order_date "
    "FROM customers c JOIN orders o ON c.customer_id = o.customer_id"
)
print("\n=== Valid JOIN query ===")
print(f"  Trust Score : {r.trust_score}%")
print(f"  JOIN count  : {r.complexity.join_count}")
print(f"  Complexity  : {r.complexity.complexity_label}")
print(f"  Confidence  : {r.confidence:.0%}")

# ── 4. Batch validation ────────────────────────────────────────────────────
queries = [
    "SELECT * FROM users",
    "SELECT * FROM nonexistent",
    "SELECT username, email, ghost_col FROM users",
    "SELECT COUNT(*) AS total FROM orders GROUP BY customer_id",
]
print("\n=== Batch validation ===")
results = engine.validate_sql_batch(queries)
for sql, res in zip(queries, results):
    status = "✓" if res.trust_score >= 80 else "✗"
    print(f"  {status} [{res.trust_score:5.1f}%] {sql[:60]}")

engine.close()
