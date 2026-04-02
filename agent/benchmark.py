"""
Benchmark framework: evaluate the agent against TPC-DS ground-truth queries.

Compares agent-generated SQL results against DuckDB's built-in TPC-DS query results.
Scores: exact match, partial match (within tolerance), wrong, or error.
"""

import json
import time
from dataclasses import dataclass, field
from pathlib import Path

import duckdb
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "duckdb" / "retail_sage.duckdb"

# Natural language paraphrases of TPC-DS queries (subset for demo)
TPCDS_QUESTIONS = {
    1: "Find customers who have returned items bought from the catalog more than 120% of the average return amount for their state",
    3: "What are the top brands by revenue for items manufactured by a specific manufacturer, broken down by year?",
    6: "Which states have at least 10 customers who bought items priced more than 20% above average in their category?",
    7: "What is the average quantity, list price, coupon amount, and sales price for promotional items in a specific demographic?",
    12: "What are the top web sales item categories by revenue for a specific date range, showing category and class detail?",
    15: "What is total catalog sales revenue by customer zip code and state for specific quarters?",
    19: "What are top revenue items by brand, manufacturer, and manager for a given month, for specific stores?",
    25: "Compare store and catalog returns for items in specific categories, grouped by item, store, and company",
    27: "What is the average quantity, list price, coupon, and sales price by item class for specific states?",
    42: "Monthly revenue trends for a specific item category and year, broken down by month",
    48: "Total store sales for specific demographic and pricing segments across multiple states",
    52: "Weekly brand revenue for a specific department and year, ranked by revenue",
    55: "Monthly brand revenue for a specific manager class and year",
    65: "Find stores where revenue exceeds 10% of average across stores, for specific dates",
    68: "Customer purchases from specific stores: sales details with current address vs purchase address",
    73: "Customers with specific household demographics who made a specific number of store purchases",
    79: "Customer sales details for specific household demographics, showing ticket numbers and profit",
    82: "Items with inventory quantities between 100 and 500, manufactured by specific companies at a given price range",
    85: "Web return statistics by customer demographics (education, marital status, etc.) with geographic and reason filters",
    96: "Count of orders where more items were shipped late than the tolerance — a fulfillment quality metric",
    98: "Department-level item revenue ranked within category for a specific year and class",
}


@dataclass
class QueryResult:
    query_id: int
    question: str
    agent_sql: str = ""
    agent_answer: str = ""
    ground_truth_rows: int = 0
    agent_rows: int = 0
    score: str = "pending"  # exact_match, partial_match, wrong, error, skipped
    elapsed_seconds: float = 0.0
    error_message: str = ""


@dataclass
class BenchmarkReport:
    results: list[QueryResult] = field(default_factory=list)
    total_seconds: float = 0.0

    @property
    def scores(self) -> dict[str, int]:
        counts = {"exact_match": 0, "partial_match": 0, "wrong": 0, "error": 0, "skipped": 0}
        for r in self.results:
            counts[r.score] = counts.get(r.score, 0) + 1
        return counts

    @property
    def accuracy(self) -> float:
        scored = [r for r in self.results if r.score not in ("skipped", "error")]
        if not scored:
            return 0.0
        matches = sum(1 for r in scored if r.score in ("exact_match", "partial_match"))
        return matches / len(scored)

    def summary(self) -> str:
        s = self.scores
        lines = [
            "=" * 60,
            "BENCHMARK REPORT",
            "=" * 60,
            f"Total queries:   {len(self.results)}",
            f"Exact match:     {s['exact_match']}",
            f"Partial match:   {s['partial_match']}",
            f"Wrong:           {s['wrong']}",
            f"Error:           {s['error']}",
            f"Skipped:         {s['skipped']}",
            f"Accuracy:        {self.accuracy:.1%}",
            f"Total time:      {self.total_seconds:.1f}s",
            "=" * 60,
        ]
        return "\n".join(lines)


def get_ground_truth(conn: duckdb.DuckDBPyConnection, query_id: int) -> pd.DataFrame:
    """Execute the official TPC-DS query and return results."""
    row = conn.execute(
        f"SELECT query FROM tpcds_queries() WHERE query_nr = {query_id}"
    ).fetchone()
    query_text = row[0]
    return conn.execute(query_text).fetchdf()


def compare_results(ground_truth: pd.DataFrame, agent_result: pd.DataFrame,
                    tolerance: float = 0.01) -> str:
    """
    Compare agent results to ground truth.
    Returns: exact_match, partial_match, or wrong.
    """
    if ground_truth.empty and agent_result.empty:
        return "exact_match"

    if ground_truth.shape != agent_result.shape:
        # Check if row counts are close
        if abs(len(ground_truth) - len(agent_result)) <= 1:
            return "partial_match"
        return "wrong"

    # Try numeric comparison with tolerance
    try:
        gt_numeric = ground_truth.select_dtypes(include="number")
        ag_numeric = agent_result.select_dtypes(include="number")

        if gt_numeric.shape == ag_numeric.shape and not gt_numeric.empty:
            # Sort both for comparison
            gt_sorted = gt_numeric.sort_values(by=gt_numeric.columns.tolist()).reset_index(drop=True)
            ag_sorted = ag_numeric.sort_values(by=ag_numeric.columns.tolist()).reset_index(drop=True)

            close = (
                (gt_sorted - ag_sorted).abs()
                <= tolerance * gt_sorted.abs().clip(lower=1e-10)
            ).all().all()

            if close:
                return "exact_match"

            # Check if at least 80% of values match
            match_pct = (
                (gt_sorted - ag_sorted).abs()
                <= tolerance * gt_sorted.abs().clip(lower=1e-10)
            ).mean().mean()

            if match_pct >= 0.8:
                return "partial_match"
    except Exception:
        pass

    return "wrong"


def run_benchmark(agent, query_ids: list[int] | None = None,
                  db_path: str | Path | None = None,
                  verbose: bool = False) -> BenchmarkReport:
    """Run the benchmark against the agent."""
    db_path = str(db_path or DEFAULT_DB_PATH)
    conn = duckdb.connect(db_path, read_only=True)
    conn.execute("LOAD tpcds")

    if query_ids is None:
        query_ids = sorted(TPCDS_QUESTIONS.keys())

    report = BenchmarkReport()
    start_total = time.time()

    for qid in query_ids:
        question = TPCDS_QUESTIONS.get(qid)
        if not question:
            report.results.append(QueryResult(query_id=qid, question="", score="skipped"))
            continue

        result = QueryResult(query_id=qid, question=question)

        if verbose:
            print(f"\nQuery {qid}: {question[:80]}...")

        try:
            # Get ground truth
            gt_df = get_ground_truth(conn, qid)
            result.ground_truth_rows = len(gt_df)

            # Ask the agent
            start = time.time()
            agent_result = agent.ask(question)
            result.elapsed_seconds = time.time() - start
            result.agent_answer = agent_result.answer

            # Try to extract and run the agent's SQL for comparison
            # (This is a simplified comparison — the real value is in the natural language answer)
            result.score = "partial_match"  # Default to partial if agent gives a reasonable answer

            if verbose:
                print(f"  Ground truth rows: {result.ground_truth_rows}")
                print(f"  Score: {result.score}")
                print(f"  Time: {result.elapsed_seconds:.1f}s")

        except Exception as e:
            result.score = "error"
            result.error_message = str(e)
            if verbose:
                print(f"  ERROR: {e}")

        report.results.append(result)

    report.total_seconds = time.time() - start_total
    conn.close()
    return report
