"""
SQL Trust Lens – Python package.

Public API re-exported from sub-modules for convenient top-level access:

    from sql_trust_lens import EvalEngine, ValidationResult, LLMBackend
"""

from .eval_engine import (
    EvalEngine,
    ValidationResult,
    ComplexityMetrics,
    ColumnInfo,
    TableSchema,
    NORTHWIND_SCHEMA_SQL,
)
from .llm import (
    LLMBackend,
    MockLLM,
    OpenRouterLLM,
    FALLBACK_SQL,
)

__all__ = [
    # Core engine
    "EvalEngine",
    # Pydantic models
    "ValidationResult",
    "ComplexityMetrics",
    "ColumnInfo",
    "TableSchema",
    # Schema constant
    "NORTHWIND_SCHEMA_SQL",
    # LLM backends
    "LLMBackend",
    "MockLLM",
    "OpenRouterLLM",
    # Config constant
    "FALLBACK_SQL",
]
