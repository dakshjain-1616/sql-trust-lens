# sql-trust-lens – Local-first validator for Text-to-SQL safety

> *Made autonomously using [NEO](https://heyneo.so) · [![Install NEO Extension](https://img.shields.io/badge/VS%20Code-Install%20NEO-7B61FF?logo=visual-studio-code)](https://marketplace.visualstudio.com/items?itemName=NeoResearchInc.heyneo)*

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Tests](https://img.shields.io/badge/tests-58%20passed-brightgreen.svg)]()

> Prevent schema hallucinations in LLM-generated SQL with instant, local trust scoring before query execution.

## Install

```bash
git clone https://github.com/dakshjain-1616/sql-trust-lens
cd sql-trust-lens
pip install -r requirements.txt
```

## The Problem

Generic linters like sqlfluff catch syntax errors but miss semantic hallucinations where LLMs invent columns that don't exist. This leads to runtime crashes or silent data corruption when executing unverified queries against production schemas. Existing validators require shipping schema data to third-party clouds, introducing latency and security risks.

## Who it's for

Data engineers and AI developers building natural language interfaces for databases. You need this when deploying chatbots that query warehouses, ensuring the AI references actual table structures without exposing your schema to external tracking servers.

## Quickstart

```python
from sql_trust_lens.eval_engine import TrustEngine

# Initialize with a local SQLite or DuckDB file
engine = TrustEngine(db_path="data/northwind.db")

# Evaluate a generated query
query = "SELECT * FROM employees WHERE salary > 50000"
score, details = engine.evaluate(query)

print(f"Trust Score: {score}%")
if score < 80:
    print(f"Warning: {details['issues']}")
```

## Key features

- **Trust Score (0–100%)** — Weighted calculation of table and column validity against live schema.
- **Visual Highlighting** — Streamlit UI highlights hallucinated tokens in red before execution.
- **Local-First** — Runs entirely on your machine using DuckDB; no data leaves your environment.
- **Actionable Feedback** — Provides specific suggestions for fixing invalid column references.

## Run tests

```bash
pytest tests/ -q
# 58 passed
```

## Project structure

```
sql-trust-lens/
├── sql_trust_lens/      ← main library
├── tests/               ← test suite
├── scripts/             ← demo scripts
├── examples/            ← usage examples
└── requirements.txt
```