"""
SQL Trust Lens – LLM Backend
Unified SQL generation interface with priority chain:
  OpenRouter API → Llama.cpp → Mock (keyword) → FALLBACK_SQL

Set OPENROUTER_API_KEY to enable real LLM calls.
Set LLM_MODEL to override the model (default: google/gemini-2.5-flash-lite).
"""

import os
import re
import time
from typing import Optional, Tuple

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

LLM_MODEL = os.getenv("LLM_MODEL", "google/gemini-2.5-flash-lite")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_BASE_URL = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
LLM_MAX_RETRIES = int(os.getenv("LLM_MAX_RETRIES", "2"))
LLM_TIMEOUT = int(os.getenv("LLM_TIMEOUT", "15"))
LLM_MAX_TOKENS = int(os.getenv("LLM_MAX_TOKENS", "256"))
LLM_TEMPERATURE = float(os.getenv("LLM_TEMPERATURE", "0.1"))
LLAMA_CTX_SIZE = int(os.getenv("LLAMA_CTX_SIZE", "512"))
USE_MOCK_LLM = os.getenv("USE_MOCK_LLM", "true").lower() != "false"
FALLBACK_SQL = os.getenv("FALLBACK_SQL", "SELECT * FROM users LIMIT 10")

# ---------------------------------------------------------------------------
# Mock LLM (keyword-based, no API required)
# ---------------------------------------------------------------------------

_KEYWORD_RULES = [
    # (pattern, sql_template)
    (r"\busers?\b",        "SELECT * FROM users"),
    (r"\borders?\b",       "SELECT * FROM orders"),
    (r"\bproducts?\b",     "SELECT * FROM products"),
    (r"\bcustomers?\b",    "SELECT * FROM customers"),
    (r"\bemployees?\b",    "SELECT * FROM employees"),
    (r"\bcategori",        "SELECT * FROM categories"),
    (r"\bsuppli",          "SELECT * FROM suppliers"),
    (r"\border.detail",    "SELECT * FROM order_details"),
    # aggregations
    (r"\bhow many\b.*\buser",     "SELECT COUNT(*) AS user_count FROM users"),
    (r"\bhow many\b.*\border",    "SELECT COUNT(*) AS order_count FROM orders"),
    (r"\bhow many\b.*\bproduct",  "SELECT COUNT(*) AS product_count FROM products"),
    (r"\btotal.*freight\b",
     "SELECT SUM(freight) AS total_freight FROM orders"),
    (r"\bactive\b.*\buser",
     "SELECT * FROM users WHERE is_active = true"),
    (r"\binactive\b.*\buser",
     "SELECT * FROM users WHERE is_active = false"),
    (r"\bjoin.*order",
     "SELECT c.company_name, o.order_id, o.order_date "
     "FROM customers c JOIN orders o ON c.customer_id = o.customer_id"),
    (r"\btop.*product",
     "SELECT product_name, unit_price, units_in_stock FROM products ORDER BY unit_price DESC LIMIT 5"),
    (r"\brecent.*order",
     "SELECT order_id, customer_id, order_date, freight FROM orders ORDER BY order_date DESC LIMIT 10"),
    (r"\bcount.*order.*customer",
     "SELECT customer_id, COUNT(*) AS order_count FROM orders GROUP BY customer_id ORDER BY order_count DESC"),
    # deliberate hallucination demos
    (r"\bfake\b|\bhallucin",
     "SELECT invalid_col, ghost_column FROM fake_table"),
    (r"\bbad.?column\b",
     "SELECT invalid_col FROM users"),
    (r"\bmissing.?table\b",
     "SELECT * FROM nonexistent_table"),
]


class MockLLM:
    """
    Keyword-based SQL generator used when no real LLM is available.
    Intentionally produces occasional hallucinations for demo purposes.
    """

    def __init__(self, enabled: bool = True):
        self.enabled = enabled

    def generate(self, prompt: str) -> str:
        """Return SQL for the given natural-language prompt using keyword rules."""
        if not self.enabled:
            return FALLBACK_SQL

        lower = prompt.lower()
        for pattern, sql in _KEYWORD_RULES:
            if re.search(pattern, lower):
                return sql

        return FALLBACK_SQL


# ---------------------------------------------------------------------------
# OpenRouter LLM (real API calls via openai-compatible interface)
# ---------------------------------------------------------------------------

class OpenRouterLLM:
    """
    Calls OpenRouter's API to generate SQL from natural language.
    Uses the openai SDK pointed at https://openrouter.ai/api/v1.
    """

    _SYSTEM_PROMPT = (
        "You are a SQL expert. Given the database schema below and a natural language "
        "question, return ONLY a single valid SQL SELECT statement — no explanation, "
        "no markdown, no code fences.\n\nSchema (JSON):\n{schema}"
    )

    def __init__(self, api_key: str, model: str = LLM_MODEL):
        self.api_key = api_key
        self.model = model
        self._client = None

    def _get_client(self):
        """Lazily initialise and return the OpenAI-compatible client."""
        if self._client is None:
            from openai import OpenAI  # deferred import – optional dependency
            self._client = OpenAI(
                api_key=self.api_key,
                base_url=OPENROUTER_BASE_URL,
            )
        return self._client

    def generate(
        self,
        prompt: str,
        schema_hint: str = "",
        max_retries: int = LLM_MAX_RETRIES,
    ) -> str:
        """Return SQL string for *prompt*, with retry on transient errors."""
        client = self._get_client()
        system_msg = self._SYSTEM_PROMPT.format(schema=schema_hint or "{}")

        last_exc: Optional[Exception] = None
        for attempt in range(max_retries + 1):
            try:
                resp = client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": system_msg},
                        {"role": "user", "content": prompt},
                    ],
                    max_tokens=LLM_MAX_TOKENS,
                    temperature=LLM_TEMPERATURE,
                    timeout=LLM_TIMEOUT,
                )
                raw = resp.choices[0].message.content.strip()
                # Strip markdown code fences if the model adds them
                raw = re.sub(r"^```(?:sql)?\s*", "", raw, flags=re.IGNORECASE)
                raw = re.sub(r"\s*```$", "", raw)
                return raw.strip()
            except Exception as exc:
                last_exc = exc
                if attempt < max_retries:
                    time.sleep(2 ** attempt)  # exponential back-off: 1s, 2s

        raise RuntimeError(
            f"OpenRouter call failed after {max_retries + 1} attempts: {last_exc}"
        )


# ---------------------------------------------------------------------------
# Llama.cpp shim (unchanged interface, kept for backward compat)
# ---------------------------------------------------------------------------

def _try_load_llama(model_path: str):
    """Attempt to load a GGUF model via llama-cpp-python; return None if unavailable."""
    try:
        from llama_cpp import Llama  # type: ignore
        if not os.path.isfile(model_path):
            return None
        llm = Llama(model_path=model_path, n_ctx=LLAMA_CTX_SIZE, verbose=False)
        return llm
    except Exception:
        return None


def _llama_generate(llm, prompt: str, schema_hint: str) -> str:
    """Generate a SQL string from a natural-language prompt using a local Llama model."""
    system = (
        "You are a SQL expert. Given a database schema and a question, "
        "return only a single valid SQL SELECT statement with no explanation.\n"
        f"Schema:\n{schema_hint}"
    )
    full_prompt = f"[INST] {system}\n\nQuestion: {prompt} [/INST]"
    result = llm(full_prompt, max_tokens=LLM_MAX_TOKENS, stop=["</s>", "\n\n"])
    return result["choices"][0]["text"].strip()


# ---------------------------------------------------------------------------
# Unified LLM Backend
# ---------------------------------------------------------------------------

class LLMBackend:
    """
    Priority chain for SQL generation:
      1. OpenRouter  (if OPENROUTER_API_KEY set)
      2. Llama.cpp   (if LLAMA_MODEL_PATH set and file exists)
      3. Mock LLM    (if USE_MOCK_LLM=true, default)
      4. FALLBACK_SQL (hardcoded safe query)
    """

    def __init__(self):
        self._openrouter: Optional[OpenRouterLLM] = None
        self._llama = None
        self._mock = MockLLM(enabled=USE_MOCK_LLM)
        self._backend_name = "fallback"
        self._init_backends()

    def _init_backends(self) -> None:
        """Detect available backends and initialise the highest-priority one."""
        api_key = OPENROUTER_API_KEY
        if api_key:
            self._openrouter = OpenRouterLLM(api_key=api_key, model=LLM_MODEL)
            self._backend_name = f"openrouter/{LLM_MODEL}"
            return

        model_path = os.getenv("LLAMA_MODEL_PATH", "")
        if model_path:
            llama = _try_load_llama(model_path)
            if llama is not None:
                self._llama = llama
                self._backend_name = f"llama/{os.path.basename(model_path)}"
                return

        if self._mock.enabled:
            self._backend_name = "mock"
        else:
            self._backend_name = "fallback"

    @property
    def backend_name(self) -> str:
        return self._backend_name

    @property
    def use_mock(self) -> bool:
        return self._mock.enabled

    @use_mock.setter
    def use_mock(self, value: bool) -> None:
        self._mock.enabled = value
        # Only update backend_name if no real backend is active
        if self._openrouter is None and self._llama is None:
            self._backend_name = "mock" if value else "fallback"

    def generate(self, prompt: str, schema_hint: str = "") -> Tuple[str, str]:
        """
        Generate SQL for the given prompt.
        Returns (sql, backend_name_used).
        """
        if self._openrouter is not None:
            try:
                sql = self._openrouter.generate(prompt, schema_hint)
                return sql, self._backend_name
            except Exception:
                pass  # fall through to next backend

        if self._llama is not None:
            try:
                sql = _llama_generate(self._llama, prompt, schema_hint)
                return sql, self._backend_name
            except Exception:
                pass

        if self._mock.enabled:
            return self._mock.generate(prompt), "mock"

        return FALLBACK_SQL, "fallback"
