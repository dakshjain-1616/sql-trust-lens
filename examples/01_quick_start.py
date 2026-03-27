"""
01 – Quick Start
Minimal example: validate a single SQL query and print the Trust Score.
Run from any directory:  python examples/01_quick_start.py
"""
import sys, os; sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from sql_trust_lens import EvalEngine

engine = EvalEngine()

sql = "SELECT * FROM users"
result = engine.validate_sql(sql)

print(f"SQL        : {sql}")
print(f"Trust Score: {result.trust_score}%")
print(f"Can Execute: {result.can_execute}")
print(f"Complexity : {result.complexity.complexity_label}")

engine.close()
