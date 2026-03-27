"""
SQL Trust Lens – CLI Demo
Runs validation scenarios and saves real output files to outputs/.
No API key or LLM required.

Usage:
  python demo.py                         # run all scenarios, save all formats
  python demo.py -o /tmp/out -f html     # custom output dir, HTML only
  python demo.py -s tc1,tc3             # run specific scenarios
  python demo.py -b queries.json        # batch-validate SQL from a JSON file
  python demo.py --version              # print version and exit
  python demo.py -v                      # verbose output
"""

import argparse
import csv
import json
import os
import sys
from datetime import datetime
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from rich.console import Console
from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.table import Table

from sql_trust_lens import EvalEngine, ValidationResult

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

VERSION = "1.0.0"
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "outputs"))
DB_PATH = os.getenv("DB_PATH", ":memory:")
CONSOLE = Console()

# ---------------------------------------------------------------------------
# Demo scenarios
# ---------------------------------------------------------------------------

SCENARIOS = [
    {
        "id": "tc1",
        "name": "Valid wildcard query",
        "description": "All columns and table exist → expect 100% trust",
        "sql": "SELECT * FROM users",
        "expected_score_min": 100.0,
        "expected_score_max": 100.0,
        "expect_can_execute": True,
        "expect_invalid_tables": [],
        "expect_invalid_columns": {},
    },
    {
        "id": "tc2",
        "name": "Missing table",
        "description": "Table does not exist → expect 0% trust",
        "sql": "SELECT * FROM fake_table",
        "expected_score_min": 0.0,
        "expected_score_max": 0.0,
        "expect_can_execute": False,
        "expect_invalid_tables": ["fake_table"],
        "expect_invalid_columns": {},
    },
    {
        "id": "tc3",
        "name": "Invalid column",
        "description": "Table exists but column is hallucinated → ~50% trust",
        "sql": "SELECT invalid_col FROM users",
        "expected_score_min": 45.0,
        "expected_score_max": 55.0,
        "expect_can_execute": False,
        "expect_invalid_tables": [],
        "expect_invalid_columns": {"users": ["invalid_col"]},
    },
    {
        "id": "tc4",
        "name": "Mock model disabled – fallback SQL",
        "description": "When mock LLM is off, app uses FALLBACK_SQL env var",
        "sql": os.getenv("FALLBACK_SQL", "SELECT * FROM users LIMIT 10"),
        "expected_score_min": 80.0,
        "expected_score_max": 100.0,
        "expect_can_execute": True,
        "expect_invalid_tables": [],
        "expect_invalid_columns": {},
    },
    {
        "id": "tc5",
        "name": "Valid JOIN query",
        "description": "Multi-table join with valid columns",
        "sql": (
            "SELECT c.company_name, o.order_id, o.order_date "
            "FROM customers c "
            "JOIN orders o ON c.customer_id = o.customer_id"
        ),
        "expected_score_min": 80.0,
        "expected_score_max": 100.0,
        "expect_can_execute": True,
        "expect_invalid_tables": [],
        "expect_invalid_columns": {},
    },
    {
        "id": "tc6",
        "name": "Multiple hallucinated columns",
        "description": "Several non-existent columns in the SELECT list",
        "sql": "SELECT ghost_col, phantom_field, shadow_attr FROM products",
        "expected_score_min": 0.0,
        "expected_score_max": 55.0,
        "expect_can_execute": False,
        "expect_invalid_tables": [],
        "expect_invalid_columns": {
            "products": ["ghost_col", "phantom_field", "shadow_attr"]
        },
    },
    {
        "id": "tc7",
        "name": "Mixed valid + invalid columns",
        "description": "Some columns exist, some are hallucinated",
        "sql": "SELECT product_name, unit_price, fake_discount FROM products",
        "expected_score_min": 50.0,
        "expected_score_max": 85.0,
        "expect_can_execute": False,
        "expect_invalid_tables": [],
        "expect_invalid_columns": {"products": ["fake_discount"]},
    },
    {
        "id": "tc8",
        "name": "Fully valid specific columns",
        "description": "Explicit valid column list without wildcard",
        "sql": "SELECT username, email, is_active FROM users WHERE is_active = true",
        "expected_score_min": 95.0,
        "expected_score_max": 100.0,
        "expect_can_execute": True,
        "expect_invalid_tables": [],
        "expect_invalid_columns": {},
    },
]

# ---------------------------------------------------------------------------
# Rich UI helpers
# ---------------------------------------------------------------------------

def print_banner() -> None:
    """Print the startup banner with project name, version, and NEO attribution."""
    content = (
        f"[bold cyan]SQL Trust Lens[/bold cyan]  [dim]v{VERSION}[/dim]\n"
        "[dim]Local-first schema validator for LLM-generated SQL queries[/dim]\n\n"
        "[dim]Scores every query 0–100% for trust before it touches your database.[/dim]\n"
        "[dim]Built autonomously by [/dim][bold magenta][link=https://heyneo.so]NEO[/link][/bold magenta]"
    )
    CONSOLE.print(
        Panel(content, title="[bold]🔍  SQL Trust Lens[/bold]", border_style="cyan", padding=(1, 2))
    )


def print_schema_table(schema_summary: dict) -> None:
    """Display the loaded database schema as a Rich table."""
    table = Table(
        title=f"[bold]Loaded Schema[/bold] — {len(schema_summary)} tables",
        header_style="bold blue",
        show_lines=False,
        expand=False,
    )
    table.add_column("Table", style="bold cyan", no_wrap=True)
    table.add_column("Columns", style="dim")

    for tbl, cols in sorted(schema_summary.items()):
        table.add_row(tbl, ", ".join(cols))

    CONSOLE.print(table)
    CONSOLE.print()


def print_results_table(results: list) -> None:
    """Display scenario validation results as a Rich table with colour-coded trust scores."""
    table = Table(
        title="[bold]Validation Results[/bold]",
        header_style="bold magenta",
        show_lines=True,
        expand=False,
    )
    table.add_column("ID", style="dim", no_wrap=True)
    table.add_column("Name", max_width=32)
    table.add_column("Trust %", justify="right", no_wrap=True)
    table.add_column("Conf", justify="right", no_wrap=True)
    table.add_column("Exec", justify="center", no_wrap=True)
    table.add_column("Complexity", justify="center", no_wrap=True)
    table.add_column("Status", justify="center", no_wrap=True)

    for r in results:
        score = r["trust_score"]
        if score >= 80:
            score_str = f"[green]{score:.1f}%[/green]"
        elif score >= 50:
            score_str = f"[yellow]{score:.1f}%[/yellow]"
        else:
            score_str = f"[red]{score:.1f}%[/red]"

        conf_str = f"{r.get('confidence', 1.0):.0%}"
        exec_str = "[green]Yes[/green]" if r["can_execute"] else "[red]No[/red]"

        cx = r.get("complexity") or {}
        cx_label = cx.get("complexity_label", "?")
        cx_colors = {"simple": "green", "moderate": "yellow", "complex": "red"}
        cx_color = cx_colors.get(cx_label, "dim")
        cx_str = f"[{cx_color}]{cx_label}[/{cx_color}]"

        status = "[green]PASS ✅[/green]" if r.get("test_passed", True) else "[red]FAIL ❌[/red]"

        table.add_row(
            r.get("id", str(r.get("index", "?"))),
            r.get("name", "—"),
            score_str,
            conf_str,
            exec_str,
            cx_str,
            status,
        )

    CONSOLE.print(table)


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run_scenario(engine: EvalEngine, scenario: dict) -> dict:
    """Execute one scenario and return a result dict."""
    result: ValidationResult = engine.validate_sql(scenario["sql"])

    passed_score = (
        scenario["expected_score_min"]
        <= result.trust_score
        <= scenario["expected_score_max"]
    )
    passed_tables = set(result.invalid_tables) == set(
        scenario["expect_invalid_tables"]
    )

    expected_bad = scenario["expect_invalid_columns"]
    actual_bad = result.invalid_columns
    passed_cols = all(
        set(actual_bad.get(t, [])) >= set(cols)
        for t, cols in expected_bad.items()
    )

    passed = passed_score and passed_tables and passed_cols

    cx = result.complexity
    return {
        "id": scenario["id"],
        "name": scenario["name"],
        "description": scenario["description"],
        "sql": scenario["sql"],
        "trust_score": result.trust_score,
        "confidence": result.confidence,
        "can_execute": result.can_execute,
        "valid_tables": result.valid_tables,
        "invalid_tables": result.invalid_tables,
        "valid_columns": result.valid_columns,
        "invalid_columns": result.invalid_columns,
        "issues": result.issues,
        "suggestions": result.suggestions,
        "complexity": cx.model_dump() if cx else None,
        "execution_result": result.execution_result,
        "execution_error": result.execution_error,
        "row_count": result.row_count,
        "test_passed": passed,
        "expected_score_range": [
            scenario["expected_score_min"],
            scenario["expected_score_max"],
        ],
        "validated_at": result.validated_at,
    }


def run_batch(engine: EvalEngine, sql_list: list[str], verbose: bool = False) -> list[dict]:
    """Batch-validate a list of SQL strings, displaying a Rich progress bar."""
    results = []
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        console=CONSOLE,
        transient=True,
    ) as progress:
        task = progress.add_task(f"Validating {len(sql_list)} queries…", total=len(sql_list))
        for i, sql in enumerate(sql_list, 1):
            r = engine.validate_sql(sql)
            cx = r.complexity
            row = {
                "index": i,
                "sql": sql,
                "trust_score": r.trust_score,
                "confidence": r.confidence,
                "can_execute": r.can_execute,
                "invalid_tables": r.invalid_tables,
                "invalid_columns": r.invalid_columns,
                "issues": r.issues,
                "suggestions": r.suggestions,
                "complexity": cx.model_dump() if cx else None,
                "validated_at": r.validated_at,
            }
            results.append(row)
            progress.advance(task)
            if verbose:
                color = "green" if r.trust_score >= 80 else ("yellow" if r.trust_score >= 50 else "red")
                CONSOLE.print(f"  [[dim]{i:>3}[/dim]] [{color}]{r.trust_score:5.1f}%[/{color}]  {sql[:60]}")
    return results


# ---------------------------------------------------------------------------
# HTML report builder
# ---------------------------------------------------------------------------

def build_html_report(results: list[dict], schema_summary: dict, ts: str) -> str:
    """Build a self-contained HTML report from validation results and schema summary."""
    def score_color(s):
        if s >= 80:
            return "#28a745"
        if s >= 50:
            return "#ffc107"
        return "#dc3545"

    rows = ""
    for r in results:
        color = score_color(r["trust_score"])
        status = "✅ PASS" if r.get("test_passed", True) else "❌ FAIL"
        issues_html = (
            "<br>".join(f"• {i}" for i in r["issues"]) if r["issues"] else "—"
        )
        suggestions = r.get("suggestions", {})
        sugg_html = (
            "<br>".join(f"'{k}' → '{v}'" for k, v in suggestions.items())
            if suggestions
            else "—"
        )
        cx = r.get("complexity") or {}
        complexity_label = cx.get("complexity_label", "—")
        cx_color = {"simple": "#28a745", "moderate": "#ffc107", "complex": "#dc3545"}.get(complexity_label, "#6c757d")
        rows += f"""
        <tr>
          <td><code>{r.get('id', r.get('index', '?'))}</code></td>
          <td>{r.get('name', '—')}</td>
          <td><code style="font-size:0.8em">{r['sql'][:80]}{'…' if len(r['sql'])>80 else ''}</code></td>
          <td style="color:{color};font-weight:bold">{r['trust_score']:.1f}%</td>
          <td>{r['confidence']:.0%}</td>
          <td>{'Yes' if r['can_execute'] else 'No'}</td>
          <td style="font-size:0.85em">{issues_html}</td>
          <td style="font-size:0.85em">{sugg_html}</td>
          <td><span style="background:{cx_color};color:#fff;padding:1px 6px;border-radius:3px;font-size:0.75rem;">{complexity_label}</span></td>
          <td>{status}</td>
        </tr>"""

    schema_rows = ""
    for tbl, cols in sorted(schema_summary.items()):
        schema_rows += f"<tr><td><b>{tbl}</b></td><td>{', '.join(cols)}</td></tr>"

    total = len(results)
    passed = sum(1 for r in results if r.get("test_passed", True))

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <title>SQL Trust Lens – Demo Report</title>
  <style>
    body {{ font-family: system-ui, sans-serif; margin: 40px; background: #f8f9fa; color: #212529; }}
    h1 {{ color: #0066cc; }}
    table {{ border-collapse: collapse; width: 100%; background: #fff;
             box-shadow: 0 1px 4px rgba(0,0,0,.12); border-radius: 8px; overflow: hidden; margin-bottom:24px; }}
    th {{ background: #343a40; color: #fff; padding: 10px 14px; text-align: left; }}
    td {{ padding: 9px 14px; border-bottom: 1px solid #dee2e6; vertical-align: top; }}
    tr:last-child td {{ border-bottom: none; }}
    .summary {{ background: #fff; padding: 20px; border-radius: 8px;
                box-shadow: 0 1px 4px rgba(0,0,0,.12); margin-bottom: 24px; }}
  </style>
</head>
<body>
  <h1>🔍 SQL Trust Lens – Demo Report</h1>
  <p>Generated: {ts}</p>

  <div class="summary">
    <h2>Summary</h2>
    <p>Total scenarios: <b>{total}</b> &nbsp;|&nbsp;
       Passed: <b style="color:#28a745">{passed}</b> &nbsp;|&nbsp;
       Failed: <b style="color:#dc3545">{total - passed}</b></p>
  </div>

  <h2>Validation Results</h2>
  <table>
    <thead>
      <tr>
        <th>ID</th><th>Name</th><th>SQL</th>
        <th>Trust</th><th>Conf.</th><th>Exec.</th>
        <th>Issues</th><th>Suggestions</th><th>Complexity</th><th>Status</th>
      </tr>
    </thead>
    <tbody>{rows}</tbody>
  </table>

  <h2>Database Schema (Northwind)</h2>
  <table>
    <thead><tr><th>Table</th><th>Columns</th></tr></thead>
    <tbody>{schema_rows}</tbody>
  </table>

  <footer style="margin-top:40px;color:#6c757d;font-size:0.85rem">
    SQL Trust Lens · local-first schema validator ·
    <a href="https://heyneo.so">Built with NEO</a>
  </footer>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    """Parse command-line arguments and return the populated namespace."""
    parser = argparse.ArgumentParser(
        description="SQL Trust Lens – demo runner and batch validator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"SQL Trust Lens {VERSION}",
    )
    parser.add_argument(
        "-o", "--output-dir",
        default=str(OUTPUT_DIR),
        help="Directory for output files (default: %(default)s)",
    )
    parser.add_argument(
        "-f", "--format",
        choices=["json", "html", "csv", "all"],
        default="all",
        help="Output format(s) to generate (default: all)",
    )
    parser.add_argument(
        "--db-path",
        default=DB_PATH,
        help="DuckDB database path (default: %(default)s)",
    )
    parser.add_argument(
        "-s", "--scenarios",
        default="",
        help="Comma-separated scenario IDs to run (e.g. tc1,tc3). Default: all.",
    )
    parser.add_argument(
        "-b", "--batch-file",
        default="",
        help="Path to a JSON file containing a list of SQL strings to batch-validate.",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Print per-scenario detail.",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    """Entry point: run validation scenarios or batch-validate a SQL file."""
    args = parse_args()

    output_dir = Path(args.output_dir)
    db_path = args.db_path
    fmt = args.format
    verbose = args.verbose

    print_banner()

    output_dir.mkdir(parents=True, exist_ok=True)
    engine = EvalEngine(db_path=db_path)
    schema_summary = engine.get_schema_summary()

    print_schema_table(schema_summary)

    # ----- Batch mode -----
    if args.batch_file:
        batch_path = Path(args.batch_file)
        if not batch_path.exists():
            CONSOLE.print(f"[red][ERROR] Batch file not found: {batch_path}[/red]")
            engine.close()
            return 1

        sql_list = json.loads(batch_path.read_text())
        if not isinstance(sql_list, list):
            CONSOLE.print("[red][ERROR] Batch file must contain a JSON array of SQL strings.[/red]")
            engine.close()
            return 1

        CONSOLE.print(f"[cyan]Batch mode:[/cyan] validating [bold]{len(sql_list)}[/bold] queries from {batch_path}\n")
        batch_results = run_batch(engine, sql_list, verbose=verbose)
        engine.close()

        ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        batch_report = {
            "generated_at": ts,
            "source_file": str(batch_path),
            "total": len(batch_results),
            "can_execute": sum(1 for r in batch_results if r["can_execute"]),
            "results": batch_results,
        }

        if fmt in ("json", "all"):
            p = output_dir / "batch_results.json"
            p.write_text(json.dumps(batch_report, indent=2))
            CONSOLE.print(f"[green]✓[/green] Batch JSON saved → {p}")

        if fmt in ("csv", "all"):
            p = output_dir / "batch_scores.csv"
            with open(p, "w", newline="") as fh:
                w = csv.writer(fh)
                w.writerow(["index", "sql", "trust_score", "confidence",
                             "can_execute", "complexity_label", "issues_count"])
                for r in batch_results:
                    cx = r.get("complexity") or {}
                    w.writerow([
                        r["index"], r["sql"], r["trust_score"], r["confidence"],
                        r["can_execute"], cx.get("complexity_label", ""),
                        len(r["issues"]),
                    ])
            CONSOLE.print(f"[green]✓[/green] Batch CSV saved  → {p}")

        safe = batch_report["can_execute"]
        total = batch_report["total"]
        CONSOLE.print(
            Panel(
                f"[green]{safe}[/green] / [bold]{total}[/bold] queries safe to execute",
                title="[bold]Batch Complete[/bold]",
                border_style="green" if safe == total else "yellow",
            )
        )
        return 0

    # ----- Scenario mode -----
    selected_ids = {s.strip() for s in args.scenarios.split(",") if s.strip()}
    scenarios = [
        sc for sc in SCENARIOS
        if not selected_ids or sc["id"] in selected_ids
    ]

    CONSOLE.print(f"[cyan]Running[/cyan] [bold]{len(scenarios)}[/bold] scenario(s)…\n")

    results = []
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        console=CONSOLE,
        transient=True,
    ) as progress:
        task = progress.add_task("Validating…", total=len(scenarios))
        for sc in scenarios:
            progress.update(task, description=f"[cyan]{sc['id']}[/cyan] {sc['name'][:30]}…")
            r = run_scenario(engine, sc)
            results.append(r)
            progress.advance(task)

    engine.close()

    print_results_table(results)

    if verbose:
        for r in results:
            if r.get("suggestions"):
                for bad, good in r["suggestions"].items():
                    CONSOLE.print(f"  [dim]💡 [yellow]'{bad}'[/yellow] → did you mean [green]'{good}'[/green]?[/dim]")

    ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    report = {
        "generated_at": ts,
        "total_scenarios": len(results),
        "passed": sum(1 for r in results if r["test_passed"]),
        "failed": sum(1 for r in results if not r["test_passed"]),
        "schema_summary": schema_summary,
        "scenarios": results,
    }

    if fmt in ("json", "all"):
        json_path = output_dir / "results.json"
        json_path.write_text(json.dumps(report, indent=2))
        CONSOLE.print(f"[green]✓[/green] JSON report saved → {json_path}")

    if fmt in ("html", "all"):
        html = build_html_report(results, schema_summary, ts)
        html_path = output_dir / "report.html"
        html_path.write_text(html)
        CONSOLE.print(f"[green]✓[/green] HTML report saved → {html_path}")

    if fmt in ("csv", "all"):
        csv_path = output_dir / "scores.csv"
        with open(csv_path, "w", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow([
                "id", "name", "trust_score", "confidence", "can_execute",
                "invalid_tables", "invalid_columns", "complexity_label",
                "complexity_score", "test_passed", "validated_at",
            ])
            for r in results:
                cx = r.get("complexity") or {}
                writer.writerow([
                    r["id"], r["name"], r["trust_score"], r["confidence"],
                    r["can_execute"],
                    ";".join(r["invalid_tables"]),
                    ";".join(
                        f"{t}:{','.join(c)}" for t, c in r["invalid_columns"].items()
                    ),
                    cx.get("complexity_label", ""),
                    cx.get("complexity_score", ""),
                    r["test_passed"],
                    r.get("validated_at", ""),
                ])
        CONSOLE.print(f"[green]✓[/green] CSV scores saved  → {csv_path}")

    passed = report["passed"]
    total = report["total_scenarios"]
    border = "green" if passed == total else "red"
    CONSOLE.print()
    CONSOLE.print(
        Panel(
            f"[{'green' if passed == total else 'red'}]{passed}[/{'green' if passed == total else 'red'}] / [bold]{total}[/bold] scenarios passed",
            title="[bold]Results[/bold]",
            border_style=border,
        )
    )

    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())
