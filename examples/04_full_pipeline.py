"""
04 – Full Pipeline
End-to-end workflow: natural-language prompts → LLM generates SQL →
EvalEngine validates → results printed as a report table.
Run from any directory:  python examples/04_full_pipeline.py
"""
import sys, os; sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import json
from sql_trust_lens import EvalEngine, LLMBackend

# ── Bootstrap ──────────────────────────────────────────────────────────────
engine = EvalEngine()
backend = LLMBackend()
schema_hint = json.dumps(engine.get_schema_summary(), indent=2)

print("SQL Trust Lens – Full Pipeline Demo")
print(f"Backend : {backend.backend_name}")
print(f"Schema  : {len(engine.schema)} tables\n")

# ── Prompts to run through the full pipeline ──────────────────────────────
prompts = [
    "Show all active users",
    "List customers with their orders",
    "Count orders per customer",
    "Show the top 5 most expensive products",
    "Show data from fake table",       # intentional hallucination
    "Select bad column from users",    # intentional hallucination
    "Total freight across all orders",
]

# ── Run pipeline ──────────────────────────────────────────────────────────
header = f"{'#':>2}  {'Prompt':<38} {'Trust':>6}  {'Exec':>4}  {'Complexity':<10}  SQL (truncated)"
print(header)
print("-" * len(header))

for i, prompt in enumerate(prompts, 1):
    sql, used_backend = backend.generate(prompt, schema_hint)
    result = engine.validate_sql(sql)

    trust_str = f"{result.trust_score:5.1f}%"
    exec_str = "Yes" if result.can_execute else "No"
    cx_label = result.complexity.complexity_label if result.complexity else "?"
    sql_short = sql[:45] + ("…" if len(sql) > 45 else "")

    # Print issues inline for failed queries
    issues_str = ""
    if result.issues:
        issues_str = f"\n     ↳ {result.issues[0]}"
        if result.suggestions:
            for bad, hint in result.suggestions.items():
                issues_str += f"\n       💡 '{bad}' → did you mean '{hint}'?"

    print(f"{i:>2}. {prompt:<38} {trust_str}  {exec_str:<4}  {cx_label:<10}  {sql_short}{issues_str}")

# ── Summary ───────────────────────────────────────────────────────────────
print()
all_results = [engine.validate_sql(s) for s, _ in (backend.generate(p, schema_hint) for p in prompts)]
safe = sum(1 for r in all_results if r.can_execute)
print(f"Summary: {safe}/{len(prompts)} queries safe to execute")
print(f"Average trust score: {sum(r.trust_score for r in all_results) / len(all_results):.1f}%")

engine.close()
