"""
Phase 6: Run the agent benchmark against TPC-DS ground-truth queries.

Usage:
    python scripts/05_run_benchmark.py                    # Run all benchmark queries
    python scripts/05_run_benchmark.py --queries 1 3 7    # Run specific queries
    python scripts/05_run_benchmark.py --verbose           # Show detailed output
"""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from agent.agent import RetailSageAgent
from agent.benchmark import TPCDS_QUESTIONS, run_benchmark


def main():
    parser = argparse.ArgumentParser(description="Run Retail-SAGE benchmark")
    parser.add_argument("--queries", type=int, nargs="+",
                        help="Specific TPC-DS query IDs to run")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Show detailed output per query")
    parser.add_argument("--output", "-o", type=str, default=None,
                        help="Save results to JSON file")
    args = parser.parse_args()

    print("=" * 60)
    print("  Retail-SAGE Benchmark Runner")
    print("=" * 60)
    print(f"\nAvailable benchmark queries: {sorted(TPCDS_QUESTIONS.keys())}")

    query_ids = args.queries if args.queries else None
    n = len(query_ids) if query_ids else len(TPCDS_QUESTIONS)
    print(f"Running {n} queries...\n")

    agent = RetailSageAgent()
    report = run_benchmark(agent, query_ids=query_ids, verbose=args.verbose)

    print("\n" + report.summary())

    # Show per-query results
    print("\nPer-query results:")
    print("-" * 60)
    for r in report.results:
        status = {"exact_match": "EXACT", "partial_match": "PARTIAL",
                  "wrong": "WRONG", "error": "ERROR", "skipped": "SKIP"}[r.score]
        print(f"  Q{r.query_id:>2d}: [{status:>7s}] {r.question[:60]}... ({r.elapsed_seconds:.1f}s)")

    # Save results if requested
    if args.output:
        output = {
            "summary": report.scores,
            "accuracy": report.accuracy,
            "total_seconds": report.total_seconds,
            "results": [
                {
                    "query_id": r.query_id,
                    "question": r.question,
                    "score": r.score,
                    "elapsed_seconds": r.elapsed_seconds,
                    "agent_answer": r.agent_answer[:500],
                    "error": r.error_message,
                }
                for r in report.results
            ],
        }
        with open(args.output, "w") as f:
            json.dump(output, f, indent=2)
        print(f"\nResults saved to {args.output}")


if __name__ == "__main__":
    main()
