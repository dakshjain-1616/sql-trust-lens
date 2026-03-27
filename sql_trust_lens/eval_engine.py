"""
SQL Trust Lens - Evaluation Engine
Validates Text-to-SQL outputs against live database schemas.
Detects hallucinated tables/columns and computes a Trust Score.
"""

import difflib
import re
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

import duckdb
from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class ColumnInfo(BaseModel):
    name: str
    data_type: str
    nullable: bool = True


class TableSchema(BaseModel):
    name: str
    columns: Dict[str, ColumnInfo]


class ComplexityMetrics(BaseModel):
    """SQL structural complexity breakdown."""
    join_count: int = 0
    subquery_count: int = 0
    aggregation_count: int = 0
    where_conditions: int = 0
    complexity_score: int = 0          # 0–100
    complexity_label: str = "simple"   # simple / moderate / complex


class ValidationResult(BaseModel):
    trust_score: float            # 0.0 – 100.0
    valid_tables: List[str]
    invalid_tables: List[str]
    valid_columns: Dict[str, List[str]]
    invalid_columns: Dict[str, List[str]]
    type_mismatches: List[str]
    issues: List[str]
    sql: str
    can_execute: bool
    execution_result: Optional[str] = None
    execution_error: Optional[str] = None
    row_count: Optional[int] = None
    # --- Enhanced fields ---
    validated_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    confidence: float = 1.0          # How reliably columns were assigned to tables
    suggestions: Dict[str, str] = Field(default_factory=dict)   # bad_token → suggestion
    complexity: Optional[ComplexityMetrics] = None


# ---------------------------------------------------------------------------
# Northwind-like dataset bundled with the app
# ---------------------------------------------------------------------------

NORTHWIND_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS customers (
    customer_id  VARCHAR PRIMARY KEY,
    company_name VARCHAR NOT NULL,
    contact_name VARCHAR,
    contact_title VARCHAR,
    address      VARCHAR,
    city         VARCHAR,
    region       VARCHAR,
    postal_code  VARCHAR,
    country      VARCHAR,
    phone        VARCHAR,
    fax          VARCHAR
);

CREATE TABLE IF NOT EXISTS employees (
    employee_id INTEGER PRIMARY KEY,
    last_name   VARCHAR NOT NULL,
    first_name  VARCHAR NOT NULL,
    title       VARCHAR,
    birth_date  DATE,
    hire_date   DATE,
    address     VARCHAR,
    city        VARCHAR,
    country     VARCHAR,
    home_phone  VARCHAR,
    notes       TEXT,
    reports_to  INTEGER
);

CREATE TABLE IF NOT EXISTS orders (
    order_id       INTEGER PRIMARY KEY,
    customer_id    VARCHAR,
    employee_id    INTEGER,
    order_date     DATE,
    required_date  DATE,
    shipped_date   DATE,
    freight        DECIMAL(10,2),
    ship_name      VARCHAR,
    ship_city      VARCHAR,
    ship_country   VARCHAR
);

CREATE TABLE IF NOT EXISTS products (
    product_id       INTEGER PRIMARY KEY,
    product_name     VARCHAR NOT NULL,
    supplier_id      INTEGER,
    category_id      INTEGER,
    unit_price       DECIMAL(10,2),
    units_in_stock   INTEGER,
    units_on_order   INTEGER,
    discontinued     BOOLEAN
);

CREATE TABLE IF NOT EXISTS categories (
    category_id   INTEGER PRIMARY KEY,
    category_name VARCHAR NOT NULL,
    description   TEXT
);

CREATE TABLE IF NOT EXISTS suppliers (
    supplier_id  INTEGER PRIMARY KEY,
    company_name VARCHAR NOT NULL,
    contact_name VARCHAR,
    city         VARCHAR,
    country      VARCHAR,
    phone        VARCHAR
);

CREATE TABLE IF NOT EXISTS order_details (
    order_id   INTEGER,
    product_id INTEGER,
    unit_price DECIMAL(10,2),
    quantity   INTEGER,
    discount   DECIMAL(5,4)
);

CREATE TABLE IF NOT EXISTS users (
    user_id       INTEGER PRIMARY KEY,
    username      VARCHAR NOT NULL,
    email         VARCHAR NOT NULL,
    full_name     VARCHAR,
    created_at    TIMESTAMP,
    last_login    TIMESTAMP,
    is_active     BOOLEAN
);
"""

_NORTHWIND_DATA_SQL = """
INSERT INTO categories VALUES
(1, 'Beverages',    'Soft drinks, coffees, teas, beers, and ales'),
(2, 'Condiments',   'Sweet and savory sauces, relishes, spreads, and seasonings'),
(3, 'Dairy Products','Cheeses');

INSERT INTO suppliers VALUES
(1, 'Exotic Liquids',                  'Charlotte Cooper', 'London',      'UK',  '(171) 555-2222'),
(2, 'New Orleans Cajun Delights',       'Shelley Burke',    'New Orleans', 'USA', '(100) 555-4822');

INSERT INTO customers VALUES
('ALFKI', 'Alfreds Futterkiste', 'Maria Anders', 'Sales Rep',
 'Obere Str. 57', 'Berlin', NULL, '12209', 'Germany', '030-0074321', '030-0076545'),
('ANATR', 'Ana Trujillo Emparedados', 'Ana Trujillo', 'Owner',
 'Avda. de la Constitución 2222', 'México D.F.', NULL, '05021', 'Mexico', '(5) 555-4729', NULL),
('BONAP', 'Bon app', 'Laurence Lebihan', 'Owner',
 '12, rue des Bouchers', 'Marseille', NULL, '13008', 'France', '91.24.45.40', '91.24.45.41');

INSERT INTO employees VALUES
(1, 'Davolio', 'Nancy', 'Sales Representative', '1948-12-08', '1992-05-01',
 '507 - 20th Ave. E.', 'Seattle', 'USA', '(206) 555-9857', 'Education: BA', NULL),
(2, 'Fuller',  'Andrew','Vice President, Sales','1952-02-19', '1992-08-14',
 '908 W. Capital Way',  'Tacoma',  'USA', '(206) 555-9482', 'BTS degree',  NULL);

INSERT INTO products VALUES
(1, 'Chai',           1, 1, 18.00, 39, 0,  false),
(2, 'Chang',          1, 1, 19.00, 17, 40, false),
(3, 'Aniseed Syrup',  1, 2, 10.00, 13, 70, false);

INSERT INTO orders VALUES
(10248, 'ALFKI', 1, '1996-07-04', '1996-08-01', '1996-07-16', 32.38, 'Vins et alcools', 'Reims',     'France'),
(10249, 'ANATR', 2, '1996-07-05', '1996-08-16', '1996-07-10', 11.61, 'Toms Spezialitten','Münster',  'Germany'),
(10250, 'BONAP', 1, '1996-07-08', '1996-08-05', '1996-07-12', 65.83, 'Hanari Carnes',   'Rio',       'Brazil');

INSERT INTO order_details VALUES
(10248, 1, 14.00, 12, 0.00),
(10248, 2,  9.80, 10, 0.00),
(10249, 3, 18.60,  9, 0.00);

INSERT INTO users VALUES
(1, 'johndoe',  'john@example.com', 'John Doe',  '2024-01-15 10:30:00', '2024-03-20 14:22:00', true),
(2, 'janedoe',  'jane@example.com', 'Jane Doe',  '2024-02-01 09:15:00', '2024-03-21 11:30:00', true),
(3, 'bobsmith', 'bob@example.com',  'Bob Smith', '2024-02-15 16:45:00', NULL,                  false);
"""


# ---------------------------------------------------------------------------
# EvalEngine
# ---------------------------------------------------------------------------

class EvalEngine:
    """Core SQL validation engine against a live DuckDB schema."""

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or os.getenv("DB_PATH", ":memory:")
        self.conn = duckdb.connect(self.db_path)
        self.schema: Dict[str, TableSchema] = {}
        self._setup_database()
        self._load_schema()

    # ------------------------------------------------------------------
    # Database bootstrap
    # ------------------------------------------------------------------

    def _setup_database(self) -> None:
        """Create Northwind schema and insert sample data."""
        for stmt in NORTHWIND_SCHEMA_SQL.strip().split(";"):
            s = stmt.strip()
            if s:
                self.conn.execute(s)
        for stmt in _NORTHWIND_DATA_SQL.strip().split(";"):
            s = stmt.strip()
            if s:
                try:
                    self.conn.execute(s)
                except Exception:
                    pass  # idempotent – ignore duplicate inserts

    def _load_schema(self) -> None:
        """Populate self.schema from information_schema."""
        rows = self.conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main' ORDER BY table_name"
        ).fetchall()

        for (table_name,) in rows:
            cols = self.conn.execute(
                "SELECT column_name, data_type, is_nullable "
                "FROM information_schema.columns "
                "WHERE table_name = ? ORDER BY ordinal_position",
                [table_name],
            ).fetchall()

            col_dict: Dict[str, ColumnInfo] = {}
            for col_name, data_type, is_nullable in cols:
                col_dict[col_name.lower()] = ColumnInfo(
                    name=col_name,
                    data_type=data_type,
                    nullable=(is_nullable.upper() == "YES"),
                )

            self.schema[table_name.lower()] = TableSchema(
                name=table_name, columns=col_dict
            )

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------

    def get_schema_summary(self) -> Dict[str, List[str]]:
        """Return {table: [column, ...]} for all tables."""
        return {
            tbl: list(ts.columns.keys()) for tbl, ts in self.schema.items()
        }

    def reload_schema(self) -> None:
        """Re-read schema from the live connection."""
        self.schema.clear()
        self._load_schema()

    # ------------------------------------------------------------------
    # SQL parsing helpers
    # ------------------------------------------------------------------

    def extract_table_names(self, sql: str) -> List[str]:
        """Return lower-cased table names referenced in the SQL."""
        tables: Set[str] = set()

        _RESERVED = {
            "select", "where", "order", "group", "having", "limit",
            "set", "values", "into", "as", "on",
        }

        # FROM <table> [AS alias]
        for m in re.finditer(
            r"\bFROM\s+([`\"\[]?[a-zA-Z_][a-zA-Z0-9_]*[`\"\]]?)",
            sql, re.IGNORECASE,
        ):
            tbl = m.group(1).strip("`\"[]").lower()
            if tbl not in _RESERVED:
                tables.add(tbl)

        # [... JOIN] <table>
        for m in re.finditer(
            r"\b(?:JOIN|INNER\s+JOIN|LEFT(?:\s+OUTER)?\s+JOIN"
            r"|RIGHT(?:\s+OUTER)?\s+JOIN|FULL(?:\s+OUTER)?\s+JOIN"
            r"|CROSS\s+JOIN)\s+([`\"\[]?[a-zA-Z_][a-zA-Z0-9_]*[`\"\]]?)",
            sql, re.IGNORECASE,
        ):
            tbl = m.group(1).strip("`\"[]").lower()
            tables.add(tbl)

        return sorted(tables)

    def extract_column_names(
        self, sql: str, tables: List[str]
    ) -> Dict[str, List[str]]:
        """
        Return {table: [col, ...]} for non-wildcard column references
        in the SELECT and WHERE clauses.
        """
        # Build alias → real-table mapping
        alias_map: Dict[str, str] = {t: t for t in tables}
        for pat in (
            r"\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+(?:AS\s+)?([a-zA-Z_][a-zA-Z0-9_]*)\b",
            r"\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+(?:AS\s+)?([a-zA-Z_][a-zA-Z0-9_]*)\b",
        ):
            for m in re.finditer(pat, sql, re.IGNORECASE):
                tbl, alias = m.group(1).lower(), m.group(2).lower()
                skip = {
                    "where", "on", "join", "inner", "left", "right",
                    "group", "order", "having", "limit", "set",
                }
                if alias not in skip:
                    alias_map[alias] = tbl

        columns_by_table: Dict[str, List[str]] = {t: [] for t in tables}

        # ---- SELECT columns ----
        sel_m = re.match(
            r"\bSELECT\b\s+(.*?)\bFROM\b", sql, re.IGNORECASE | re.DOTALL
        )
        if sel_m:
            select_clause = sel_m.group(1).strip()
            for raw in self._split_select_columns(select_clause):
                part = raw.strip()
                if not part:
                    continue
                # Remove trailing alias  e.g.  col AS c
                part = re.split(r"\s+AS\s+", part, flags=re.IGNORECASE)[0].strip()

                if "." in part:
                    tbl_ref, col = part.split(".", 1)
                    tbl_ref = tbl_ref.strip("`\"[]").lower()
                    col = col.strip("`\"[]").lower()
                    if col == "*":
                        continue
                    actual = alias_map.get(tbl_ref, tbl_ref)
                    if actual in columns_by_table:
                        columns_by_table[actual].append(col)
                else:
                    col = part.strip("`\"[]").lower()
                    if col == "*":
                        continue
                    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", col):
                        continue  # skip expressions / functions
                    if len(tables) == 1:
                        columns_by_table[tables[0]].append(col)
                    else:
                        # Assign to first table that owns the column
                        assigned = False
                        for t in tables:
                            if t in self.schema and col in self.schema[t].columns:
                                columns_by_table[t].append(col)
                                assigned = True
                                break
                        if not assigned and tables:
                            columns_by_table[tables[0]].append(col)

        # ---- WHERE columns ----
        where_m = re.search(
            r"\bWHERE\b\s+(.*?)(?:\bGROUP\b|\bORDER\b|\bHAVING\b|\bLIMIT\b|$)",
            sql, re.IGNORECASE | re.DOTALL,
        )
        if where_m:
            where_clause = where_m.group(1)
            for col in re.findall(
                r"\b([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:=|!=|<>|<=?|>=?|LIKE|IN\b|IS\b|BETWEEN\b)",
                where_clause, re.IGNORECASE,
            ):
                col = col.lower()
                _skip = {"and", "or", "not", "null", "true", "false", "is", "in"}
                if col in _skip:
                    continue
                if len(tables) == 1:
                    if col not in columns_by_table[tables[0]]:
                        columns_by_table[tables[0]].append(col)
                else:
                    for t in tables:
                        if t in self.schema and col in self.schema[t].columns:
                            if col not in columns_by_table[t]:
                                columns_by_table[t].append(col)
                            break

        return columns_by_table

    @staticmethod
    def _split_select_columns(clause: str) -> List[str]:
        """Split a SELECT column list respecting nested parentheses."""
        parts: List[str] = []
        depth = 0
        buf: List[str] = []
        for ch in clause:
            if ch == "(":
                depth += 1
                buf.append(ch)
            elif ch == ")":
                depth -= 1
                buf.append(ch)
            elif ch == "," and depth == 0:
                parts.append("".join(buf).strip())
                buf = []
            else:
                buf.append(ch)
        if buf:
            parts.append("".join(buf).strip())
        return parts

    # ------------------------------------------------------------------
    # Complexity analysis
    # ------------------------------------------------------------------

    def _calculate_complexity(self, sql: str) -> ComplexityMetrics:
        """Analyse structural complexity of a SQL statement."""
        sql_upper = sql.upper()

        join_count = len(re.findall(r"\bJOIN\b", sql_upper))
        subquery_count = len(re.findall(r"\(\s*SELECT\b", sql_upper))
        agg_count = len(re.findall(
            r"\b(COUNT|SUM|AVG|MIN|MAX|GROUP_CONCAT|STRING_AGG)\s*\(", sql_upper
        ))

        where_conds = 0
        where_m = re.search(
            r"\bWHERE\b(.*?)(?:\bGROUP\b|\bORDER\b|\bHAVING\b|\bLIMIT\b|$)",
            sql, re.IGNORECASE | re.DOTALL,
        )
        if where_m:
            where_conds = len(re.findall(r"\b(AND|OR)\b", where_m.group(1), re.IGNORECASE))

        score = min(100, join_count * 20 + subquery_count * 30 + agg_count * 10 + where_conds * 5)

        if score < 20:
            label = "simple"
        elif score < 50:
            label = "moderate"
        else:
            label = "complex"

        return ComplexityMetrics(
            join_count=join_count,
            subquery_count=subquery_count,
            aggregation_count=agg_count,
            where_conditions=where_conds,
            complexity_score=score,
            complexity_label=label,
        )

    # ------------------------------------------------------------------
    # Suggestion engine (did-you-mean)
    # ------------------------------------------------------------------

    def _suggest_similar(
        self, token: str, candidates: List[str], threshold: float = 0.6
    ) -> Optional[str]:
        """Return the closest candidate to *token*, or None if no match above threshold."""
        lower_candidates = [c.lower() for c in candidates]
        matches = difflib.get_close_matches(
            token.lower(), lower_candidates, n=1, cutoff=threshold
        )
        if not matches:
            return None
        # Return the original-case version
        lower_to_orig = {c.lower(): c for c in candidates}
        return lower_to_orig.get(matches[0])

    def _build_suggestions(
        self,
        invalid_tables: List[str],
        invalid_columns: Dict[str, List[str]],
        valid_tables: List[str],
    ) -> Dict[str, str]:
        """Build a {bad_token: suggestion} map for invalid tables and columns."""
        suggestions: Dict[str, str] = {}
        all_table_names = list(self.schema.keys())

        for tbl in invalid_tables:
            hit = self._suggest_similar(tbl, all_table_names)
            if hit:
                suggestions[tbl] = hit

        # Collect candidate columns from every valid table
        all_col_candidates: List[str] = []
        for t in valid_tables:
            if t in self.schema:
                all_col_candidates.extend(self.schema[t].columns.keys())

        for tbl, cols in invalid_columns.items():
            # Prefer columns from the specific table if it's valid
            tbl_cols = (
                list(self.schema[tbl].columns.keys())
                if tbl in self.schema
                else all_col_candidates
            )
            pool = tbl_cols if tbl_cols else all_col_candidates
            for col in cols:
                hit = self._suggest_similar(col, pool)
                if hit:
                    suggestions[col] = hit

        return suggestions

    # ------------------------------------------------------------------
    # Confidence score
    # ------------------------------------------------------------------

    def _calculate_confidence(
        self,
        sql: str,
        valid_tables: List[str],
        all_col_refs: Dict[str, List[str]],
    ) -> float:
        """
        Confidence measures how reliably column-to-table assignments were made.
        1.0 = single table or all refs fully qualified.
        Decreases as unqualified column refs in multi-table queries increase.
        """
        if len(valid_tables) <= 1:
            return 1.0

        sel_m = re.match(
            r"\bSELECT\b\s+(.*?)\bFROM\b", sql, re.IGNORECASE | re.DOTALL
        )
        if not sel_m:
            return 1.0

        total = 0
        qualified = 0
        for raw in self._split_select_columns(sel_m.group(1)):
            part = re.split(r"\s+AS\s+", raw.strip(), flags=re.IGNORECASE)[0].strip()
            if not part or part.strip("`\"[]") == "*":
                continue
            total += 1
            if "." in part:
                qualified += 1

        if total == 0:
            return 1.0

        # Min 0.3 (all unqualified multi-table), max 1.0 (all qualified)
        return round(0.3 + 0.7 * (qualified / total), 2)

    # ------------------------------------------------------------------
    # Core validation
    # ------------------------------------------------------------------

    def validate_sql(self, sql: str) -> ValidationResult:
        """
        Validate *sql* against the live schema.
        Returns a ValidationResult with trust_score in [0, 100].
        """
        sql = sql.strip()

        # 1. Extract table references
        referenced_tables = self.extract_table_names(sql)

        # 2. Classify tables
        valid_tables: List[str] = []
        invalid_tables: List[str] = []
        for tbl in referenced_tables:
            (valid_tables if tbl in self.schema else invalid_tables).append(tbl)

        # 3. Extract + classify columns (only for valid tables)
        all_col_refs = self.extract_column_names(sql, valid_tables)

        valid_columns: Dict[str, List[str]] = {}
        invalid_columns: Dict[str, List[str]] = {}

        for tbl, cols in all_col_refs.items():
            if tbl not in self.schema:
                continue
            ts = self.schema[tbl]
            ok, bad = [], []
            for c in cols:
                (ok if c.lower() in ts.columns else bad).append(c)
            if ok:
                valid_columns[tbl] = ok
            if bad:
                invalid_columns[tbl] = bad

        # 4. Trust score
        trust_score = self._calculate_trust_score(
            referenced_tables, valid_tables, invalid_tables,
            all_col_refs, invalid_columns,
        )

        # 5. Suggestions (did-you-mean)
        suggestions = self._build_suggestions(
            invalid_tables, invalid_columns, valid_tables
        )

        # 6. Human-readable issues (with hints)
        issues: List[str] = []
        for tbl in invalid_tables:
            hint = f" — did you mean '{suggestions[tbl]}'?" if tbl in suggestions else ""
            issues.append(f"Table '{tbl}' does not exist in schema{hint}")
        for tbl, cols in invalid_columns.items():
            for col in cols:
                hint = f" — did you mean '{suggestions[col]}'?" if col in suggestions else ""
                issues.append(
                    f"Column '{col}' does not exist in table '{tbl}'{hint}"
                )

        # 7. Confidence
        confidence = self._calculate_confidence(sql, valid_tables, all_col_refs)

        # 8. Complexity
        complexity = self._calculate_complexity(sql)

        # 9. Try execution only when schema is clean
        can_execute = not invalid_tables and not invalid_columns
        execution_result: Optional[str] = None
        execution_error: Optional[str] = None
        row_count: Optional[int] = None

        if can_execute:
            try:
                df = self.conn.execute(sql).fetchdf()
                row_count = len(df)
                execution_result = f"{row_count} row(s) returned"
            except Exception as exc:
                execution_error = str(exc)
                can_execute = False

        return ValidationResult(
            trust_score=trust_score,
            valid_tables=valid_tables,
            invalid_tables=invalid_tables,
            valid_columns=valid_columns,
            invalid_columns=invalid_columns,
            type_mismatches=[],
            issues=issues,
            sql=sql,
            can_execute=can_execute,
            execution_result=execution_result,
            execution_error=execution_error,
            row_count=row_count,
            confidence=confidence,
            suggestions=suggestions,
            complexity=complexity,
        )

    # ------------------------------------------------------------------
    # Batch validation
    # ------------------------------------------------------------------

    def validate_sql_batch(self, sql_list: List[str]) -> List[ValidationResult]:
        """Validate a list of SQL statements and return results in order."""
        return [self.validate_sql(sql) for sql in sql_list]

    # ------------------------------------------------------------------
    # Trust Score formula
    # ------------------------------------------------------------------

    def _calculate_trust_score(
        self,
        all_tables: List[str],
        valid_tables: List[str],
        invalid_tables: List[str],
        all_col_refs: Dict[str, List[str]],
        invalid_columns: Dict[str, List[str]],
    ) -> float:
        """
        trust_score = (table_component * 0.5 + col_component * 0.5) * 100

        table_component = valid_tables / total_tables
        col_component:
          - 1.0 when there are no explicit (non-wildcard) column refs
          - (valid_cols / total_cols) otherwise
        """
        if not all_tables:
            return 0.0

        table_comp = len(valid_tables) / len(all_tables)

        # Collect all non-wildcard column references across valid tables
        all_explicit: List[str] = []
        for cols in all_col_refs.values():
            all_explicit.extend(cols)

        if not all_explicit:
            # Only wildcards → column health inherits table health
            col_comp = table_comp
        else:
            total_invalid = sum(len(c) for c in invalid_columns.values())
            valid_count = len(all_explicit) - total_invalid
            col_comp = max(0.0, valid_count / len(all_explicit))

        score = (table_comp * 0.5 + col_comp * 0.5) * 100.0
        return round(score, 1)

    # ------------------------------------------------------------------
    # Convenience
    # ------------------------------------------------------------------

    def execute_sql(self, sql: str):
        """Execute SQL and return a pandas DataFrame."""
        return self.conn.execute(sql).fetchdf()

    def close(self) -> None:
        """Close the underlying DuckDB connection."""
        self.conn.close()
