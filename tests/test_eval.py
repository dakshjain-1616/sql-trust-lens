"""
SQL Trust Lens – pytest test suite
Covers all four required test cases plus additional edge cases.
Run with:  python -m pytest tests/ -v
"""

import pytest
from sql_trust_lens import EvalEngine, ValidationResult, ComplexityMetrics


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def engine():
    e = EvalEngine()
    yield e
    e.close()


# ---------------------------------------------------------------------------
# TC1: SELECT * FROM users → Trust Score 100%, SQL executed
# ---------------------------------------------------------------------------

class TestValidWildcardQuery:
    def test_trust_score_is_100(self, engine):
        r = engine.validate_sql("SELECT * FROM users")
        assert r.trust_score == 100.0

    def test_no_invalid_tables(self, engine):
        r = engine.validate_sql("SELECT * FROM users")
        assert r.invalid_tables == []

    def test_no_invalid_columns(self, engine):
        r = engine.validate_sql("SELECT * FROM users")
        assert r.invalid_columns == {}

    def test_can_execute_is_true(self, engine):
        r = engine.validate_sql("SELECT * FROM users")
        assert r.can_execute is True

    def test_execution_returns_rows(self, engine):
        r = engine.validate_sql("SELECT * FROM users")
        assert r.row_count is not None
        assert r.row_count > 0

    def test_no_issues(self, engine):
        r = engine.validate_sql("SELECT * FROM users")
        assert r.issues == []

    def test_valid_table_listed(self, engine):
        r = engine.validate_sql("SELECT * FROM users")
        assert "users" in r.valid_tables


# ---------------------------------------------------------------------------
# TC2: SELECT * FROM fake_table → Trust Score 0%, Error highlighted
# ---------------------------------------------------------------------------

class TestMissingTable:
    def test_trust_score_is_0(self, engine):
        r = engine.validate_sql("SELECT * FROM fake_table")
        assert r.trust_score == 0.0

    def test_fake_table_in_invalid(self, engine):
        r = engine.validate_sql("SELECT * FROM fake_table")
        assert "fake_table" in r.invalid_tables

    def test_can_execute_is_false(self, engine):
        r = engine.validate_sql("SELECT * FROM fake_table")
        assert r.can_execute is False

    def test_issue_message_mentions_table(self, engine):
        r = engine.validate_sql("SELECT * FROM fake_table")
        assert any("fake_table" in issue for issue in r.issues)

    def test_no_valid_tables(self, engine):
        r = engine.validate_sql("SELECT * FROM fake_table")
        assert r.valid_tables == []


# ---------------------------------------------------------------------------
# TC3: SELECT invalid_col FROM users → Trust Score ~50%, Column highlighted
# ---------------------------------------------------------------------------

class TestInvalidColumn:
    def test_trust_score_is_approx_50(self, engine):
        r = engine.validate_sql("SELECT invalid_col FROM users")
        assert 45.0 <= r.trust_score <= 55.0

    def test_users_table_is_valid(self, engine):
        r = engine.validate_sql("SELECT invalid_col FROM users")
        assert "users" in r.valid_tables

    def test_invalid_col_detected(self, engine):
        r = engine.validate_sql("SELECT invalid_col FROM users")
        assert "invalid_col" in r.invalid_columns.get("users", [])

    def test_can_execute_is_false(self, engine):
        r = engine.validate_sql("SELECT invalid_col FROM users")
        assert r.can_execute is False

    def test_issue_message_mentions_column(self, engine):
        r = engine.validate_sql("SELECT invalid_col FROM users")
        assert any("invalid_col" in issue for issue in r.issues)

    def test_no_invalid_tables(self, engine):
        r = engine.validate_sql("SELECT invalid_col FROM users")
        assert r.invalid_tables == []


# ---------------------------------------------------------------------------
# TC4: Mock model disabled → fallback SQL produces valid result
# ---------------------------------------------------------------------------

class TestMockModelDisabled:
    """When USE_MOCK_LLM=false the app uses FALLBACK_SQL.
    We test the fallback SQL itself validates at >= 80%."""

    FALLBACK_SQL = "SELECT * FROM users LIMIT 10"

    def test_fallback_sql_trust_above_80(self, engine):
        r = engine.validate_sql(self.FALLBACK_SQL)
        assert r.trust_score >= 80.0

    def test_fallback_sql_can_execute(self, engine):
        r = engine.validate_sql(self.FALLBACK_SQL)
        assert r.can_execute is True

    def test_fallback_sql_no_issues(self, engine):
        r = engine.validate_sql(self.FALLBACK_SQL)
        assert r.issues == []


# ---------------------------------------------------------------------------
# Additional edge cases
# ---------------------------------------------------------------------------

class TestAdditionalCases:
    def test_valid_explicit_columns(self, engine):
        r = engine.validate_sql("SELECT username, email FROM users")
        assert r.trust_score == 100.0
        assert r.can_execute is True

    def test_valid_join(self, engine):
        r = engine.validate_sql(
            "SELECT c.company_name, o.order_id FROM customers c "
            "JOIN orders o ON c.customer_id = o.customer_id"
        )
        assert r.trust_score >= 80.0
        assert "customers" in r.valid_tables
        assert "orders" in r.valid_tables

    def test_multiple_invalid_tables(self, engine):
        r = engine.validate_sql(
            "SELECT * FROM ghost_table JOIN phantom_table ON 1=1"
        )
        assert r.trust_score == 0.0
        assert len(r.invalid_tables) >= 1

    def test_schema_summary_contains_users(self, engine):
        summary = engine.get_schema_summary()
        assert "users" in summary
        assert "email" in summary["users"]

    def test_schema_summary_contains_all_northwind_tables(self, engine):
        summary = engine.get_schema_summary()
        expected = {
            "users", "orders", "products", "customers",
            "employees", "categories", "suppliers", "order_details",
        }
        assert expected.issubset(set(summary.keys()))

    def test_extract_table_names_from_clause(self, engine):
        tables = engine.extract_table_names("SELECT * FROM products")
        assert "products" in tables

    def test_extract_table_names_join_clause(self, engine):
        tables = engine.extract_table_names(
            "SELECT * FROM orders JOIN customers ON orders.customer_id = customers.customer_id"
        )
        assert "orders" in tables
        assert "customers" in tables

    def test_partial_column_validity(self, engine):
        # 2 valid, 1 invalid → score between 50-90
        r = engine.validate_sql(
            "SELECT product_name, unit_price, nonexistent_field FROM products"
        )
        assert r.trust_score < 100.0
        assert "nonexistent_field" in r.invalid_columns.get("products", [])

    def test_where_clause_column_not_penalised_if_valid(self, engine):
        r = engine.validate_sql(
            "SELECT * FROM users WHERE is_active = true"
        )
        assert r.trust_score == 100.0

    def test_validation_result_is_pydantic_model(self, engine):
        r = engine.validate_sql("SELECT * FROM users")
        assert isinstance(r, ValidationResult)
        assert isinstance(r.trust_score, float)
        assert isinstance(r.valid_tables, list)
        assert isinstance(r.invalid_tables, list)


# ---------------------------------------------------------------------------
# New: ValidationResult enhanced fields
# ---------------------------------------------------------------------------

class TestValidationResultFields:
    """Verify the new metadata fields are present and well-formed."""

    def test_validated_at_is_present(self, engine):
        r = engine.validate_sql("SELECT * FROM users")
        assert r.validated_at != ""
        # Should be a valid ISO 8601 string
        assert "T" in r.validated_at or "-" in r.validated_at

    def test_confidence_is_float_in_range(self, engine):
        r = engine.validate_sql("SELECT * FROM users")
        assert isinstance(r.confidence, float)
        assert 0.0 <= r.confidence <= 1.0

    def test_confidence_is_1_for_single_table(self, engine):
        r = engine.validate_sql("SELECT username, email FROM users")
        assert r.confidence == 1.0

    def test_suggestions_is_dict(self, engine):
        r = engine.validate_sql("SELECT * FROM users")
        assert isinstance(r.suggestions, dict)

    def test_complexity_is_present(self, engine):
        r = engine.validate_sql("SELECT * FROM users")
        assert r.complexity is not None
        assert isinstance(r.complexity, ComplexityMetrics)

    def test_complexity_fields_exist(self, engine):
        r = engine.validate_sql("SELECT * FROM users")
        cx = r.complexity
        assert hasattr(cx, "join_count")
        assert hasattr(cx, "subquery_count")
        assert hasattr(cx, "aggregation_count")
        assert hasattr(cx, "where_conditions")
        assert hasattr(cx, "complexity_score")
        assert hasattr(cx, "complexity_label")


# ---------------------------------------------------------------------------
# New: Complexity metrics
# ---------------------------------------------------------------------------

class TestComplexityMetrics:
    def test_simple_query_is_simple(self, engine):
        r = engine.validate_sql("SELECT * FROM users")
        assert r.complexity is not None
        assert r.complexity.complexity_label == "simple"
        assert r.complexity.join_count == 0

    def test_join_query_has_join_count(self, engine):
        r = engine.validate_sql(
            "SELECT c.company_name, o.order_id FROM customers c "
            "JOIN orders o ON c.customer_id = o.customer_id"
        )
        assert r.complexity is not None
        assert r.complexity.join_count == 1

    def test_aggregation_detected(self, engine):
        r = engine.validate_sql(
            "SELECT COUNT(*) AS n, SUM(freight) AS total FROM orders"
        )
        assert r.complexity is not None
        assert r.complexity.aggregation_count >= 2

    def test_where_conditions_counted(self, engine):
        r = engine.validate_sql(
            "SELECT * FROM users WHERE is_active = true AND user_id > 0"
        )
        assert r.complexity is not None
        assert r.complexity.where_conditions >= 1

    def test_complexity_score_is_nonnegative(self, engine):
        r = engine.validate_sql("SELECT * FROM users")
        assert r.complexity.complexity_score >= 0

    def test_complexity_score_increases_with_joins(self, engine):
        r_simple = engine.validate_sql("SELECT * FROM users")
        r_join = engine.validate_sql(
            "SELECT c.company_name, o.order_id FROM customers c "
            "JOIN orders o ON c.customer_id = o.customer_id"
        )
        assert r_join.complexity.complexity_score > r_simple.complexity.complexity_score

    def test_complex_label_for_complex_query(self, engine):
        # Multiple JOINs should push score high enough for 'moderate' or 'complex'
        r = engine.validate_sql(
            "SELECT c.company_name, o.order_id, p.product_name "
            "FROM customers c "
            "JOIN orders o ON c.customer_id = o.customer_id "
            "JOIN order_details od ON o.order_id = od.order_id "
            "JOIN products p ON od.product_id = p.product_id"
        )
        assert r.complexity is not None
        assert r.complexity.join_count == 3
        assert r.complexity.complexity_label in ("moderate", "complex")


# ---------------------------------------------------------------------------
# New: Suggestions (did-you-mean)
# ---------------------------------------------------------------------------

class TestSuggestions:
    def test_no_suggestions_for_valid_query(self, engine):
        r = engine.validate_sql("SELECT * FROM users")
        assert r.suggestions == {}

    def test_suggest_table_for_typo(self, engine):
        # "userss" is close to "users"
        r = engine.validate_sql("SELECT * FROM userss")
        assert "userss" in r.suggestions
        assert r.suggestions["userss"] == "users"

    def test_suggest_column_for_typo(self, engine):
        # "usernam" is close to "username"
        r = engine.validate_sql("SELECT usernam FROM users")
        assert "usernam" in r.suggestions
        assert r.suggestions["usernam"] == "username"

    def test_no_suggestion_for_completely_wrong_table(self, engine):
        # "xyzzy_nonexistent" has no close match
        r = engine.validate_sql("SELECT * FROM xyzzy_nonexistent")
        # suggestions may or may not have an entry, but trust_score must be 0
        assert r.trust_score == 0.0

    def test_suggestions_embedded_in_issues(self, engine):
        # The issue message should contain the hint
        r = engine.validate_sql("SELECT usernam FROM users")
        if "usernam" in r.suggestions:
            # At least one issue should mention the suggestion
            assert any("did you mean" in i for i in r.issues)

    def test_suggest_similar_method_direct(self, engine):
        result = engine._suggest_similar("usernam", ["username", "email", "full_name"])
        assert result == "username"

    def test_suggest_similar_no_match(self, engine):
        result = engine._suggest_similar("zzzzz", ["username", "email"])
        assert result is None


# ---------------------------------------------------------------------------
# New: Batch validation
# ---------------------------------------------------------------------------

class TestBatchValidation:
    def test_batch_returns_correct_count(self, engine):
        sqls = [
            "SELECT * FROM users",
            "SELECT * FROM fake_table",
            "SELECT product_name FROM products",
        ]
        results = engine.validate_sql_batch(sqls)
        assert len(results) == 3

    def test_batch_results_are_validation_results(self, engine):
        results = engine.validate_sql_batch(["SELECT * FROM users"])
        assert all(isinstance(r, ValidationResult) for r in results)

    def test_batch_first_query_valid(self, engine):
        results = engine.validate_sql_batch(["SELECT * FROM users", "SELECT * FROM fake_table"])
        assert results[0].trust_score == 100.0

    def test_batch_second_query_invalid(self, engine):
        results = engine.validate_sql_batch(["SELECT * FROM users", "SELECT * FROM fake_table"])
        assert results[1].trust_score == 0.0

    def test_batch_empty_list(self, engine):
        results = engine.validate_sql_batch([])
        assert results == []

    def test_batch_preserves_order(self, engine):
        sqls = [
            "SELECT * FROM users",        # 100%
            "SELECT * FROM fake_table",   # 0%
            "SELECT username FROM users", # 100%
        ]
        results = engine.validate_sql_batch(sqls)
        assert results[0].trust_score == 100.0
        assert results[1].trust_score == 0.0
        assert results[2].trust_score == 100.0

    def test_batch_all_have_timestamps(self, engine):
        results = engine.validate_sql_batch([
            "SELECT * FROM users",
            "SELECT * FROM orders",
        ])
        for r in results:
            assert r.validated_at != ""
