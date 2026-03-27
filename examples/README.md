# SQL Trust Lens – Examples

Run any script from the project root or from the `examples/` directory — the path setup at the top of each script handles imports automatically.

```bash
python examples/01_quick_start.py
python examples/02_advanced_usage.py
python examples/03_custom_config.py
python examples/04_full_pipeline.py
```

## Scripts

| Script | What it demonstrates |
|--------|---------------------|
| [`01_quick_start.py`](01_quick_start.py) | Minimal 10-line example: create `EvalEngine`, validate one SQL query, print Trust Score |
| [`02_advanced_usage.py`](02_advanced_usage.py) | Hallucination detection, did-you-mean suggestions, JOIN validation, complexity metrics, batch validation |
| [`03_custom_config.py`](03_custom_config.py) | Customising behaviour via env vars (`DB_PATH`, `USE_MOCK_LLM`, `FALLBACK_SQL`), toggling the mock LLM on/off at runtime |
| [`04_full_pipeline.py`](04_full_pipeline.py) | End-to-end: natural-language prompts → `LLMBackend` generates SQL → `EvalEngine` validates → formatted report with trust scores, complexity, and did-you-mean hints |

## Prerequisites

```bash
pip install -r requirements.txt
```

No API key is required — all examples use the built-in mock LLM and the in-memory Northwind dataset.
Set `OPENROUTER_API_KEY` to swap in a real LLM for `03_custom_config.py` and `04_full_pipeline.py`.
