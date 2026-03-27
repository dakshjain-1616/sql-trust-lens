# SQL Trust Lens – Score LLM-generated SQL against your real schema before it runs

> *Made autonomously using [NEO](https://heyneo.so) · [![Install NEO Extension](https://img.shields.io/badge/VS%20Code-Install%20NEO-7B61FF?logo=visual-studio-code)](https://marketplace.visualstudio.com/items?itemName=NeoResearchInc.heyneo)*

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Tests](https://img.shields.io/badge/tests-58%20passed-brightgreen.svg)]()

> Validate any LLM-generated SQL against a live DuckDB schema — get a 0–100 trust score, catch hallucinated tables and columns, and get spelling suggestions before a single bad query hits your database.

## Install

```bash
git clone https://github.com/dakshjain-1616/sql-trust-lens
cd sql-trust-lens
pip install -r requirements.txt
```

## What problem this solves

Text-to-SQL LLMs hallucinate table names and columns constantly. GPT-4o will confidently generate `SELECT * FROM invoices JOIN ghost_table ON ...` against a schema that has no `invoices` or `ghost_table`. The standard approach is to run the query and catch the database error — but by then you've already sent malformed SQL to production and have no structured trace of what was wrong. SQL Trust Lens validates the SQL against a DuckDB schema snapshot before execution: it returns a 0–100 trust score, lists every invalid table and column, computes a complexity breakdown, and suggests the closest real name for typos like `usr` → `users`.

## Real world examples

```python
from sql_trust_lens import EvalEngine, NORTHWIND_SCHEMA_SQL
import duckdb

# Spin up DuckDB with your schema, point the engine at it
conn = duckdb.connect(":memory:")
conn.execute(NORTHWIND_SCHEMA_SQL)
engine = EvalEngine(db_path=":memory:")

# Valid query — 100% trust score
r = engine.validate_sql("SELECT customer_id, company_name FROM customers WHERE country = ?")
print(r.trust_score, r.can_execute)
# 100.0  False  (parametric — safe once ? is bound)
```

```python
# LLM hallucinated tables that don't exist
r = engine.validate_sql("SELECT * FROM invoices JOIN ghost_table ON invoices.id = ghost_table.id")
print(r.trust_score)       # 0.0
print(r.invalid_tables)    # ['ghost_table', 'invoices']
print(r.issues[:2])
# ['Table "ghost_table" does not exist in schema',
#  'Table "invoices" does not exist in schema']
```

```python
# Typo in table name — automatic suggestion
r = engine.validate_sql("SELECT name FROM usr WHERE deleted = 0")
print(r.trust_score)    # 0.0
print(r.suggestions)    # {'usr': 'users'}
```

```python
# Batch validate a list of LLM-generated queries
queries = [
    "SELECT * FROM customers",
    "SELECT order_id FROM orders WHERE ghost_col = 1",
    "SELECT product_name, unit_price FROM products WHERE discontinued = 0",
]
results = engine.validate_sql_batch(queries)
for sql, r in zip(queries, results):
    print(f"{r.trust_score:5.1f}  {sql[:55]}")
# 100.0  SELECT * FROM customers
#   0.0  SELECT order_id FROM orders WHERE ghost_col = 1
# 100.0  SELECT product_name, unit_price FROM products WHERE di...
```

```python
# Complexity breakdown — flag expensive queries before they hit prod
r = engine.validate_sql("""
    SELECT c.company_name, SUM(od.quantity * od.unit_price) AS total
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    JOIN order_details od ON o.order_id = od.order_id
    GROUP BY c.company_name ORDER BY total DESC
""")
print(r.trust_score)                    # 100.0
print(r.complexity.complexity_label)    # complex
print(r.complexity.join_count)          # 2
print(r.complexity.aggregation_count)   # 1
```

## Who it's for

Backend engineers building text-to-SQL features with GPT-4o, Claude, or local models who need a cheap pre-execution validation layer. Also useful for data teams who receive ad-hoc SQL from non-technical stakeholders and want an automated sanity check before running anything against the warehouse.

## Key features

- 0–100 trust score per query — easy to threshold in an API response or CI gate
- Detects hallucinated tables and columns separately with per-table column attribution
- Spelling suggestions using difflib — catches `usr` → `users`, `ordr` → `orders`
- SQL complexity breakdown: join count, subquery count, aggregation count, 0–100 score
- Batch validation with `validate_sql_batch()` for scoring entire LLM output sets
- Bundled Northwind schema for instant demos — swap in your own DuckDB `.db` file

## Run tests

```
$ pytest tests/ -v --tb=no -q --no-header

tests/test_eval.py ..................................................... [ 91%]
.....                                                                    [100%]

58 passed in 0.75s
```

## Project structure

```
sql-trust-lens/
├── sql_trust_lens/
│   ├── eval_engine.py   ← EvalEngine, ValidationResult, ComplexityMetrics
│   ├── llm.py           ← LLMBackend, MockLLM, OpenRouterLLM
│   └── __init__.py
├── tests/
│   └── test_eval.py     ← 58 tests
├── scripts/demo.py      ← runnable demo with HTML/CSV/JSON output
└── requirements.txt
```
