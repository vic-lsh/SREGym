"""
Backward-compatible wrapper. Delegates to clients.common.summarize.

Preserves the old CLI interface (--logs-dir, --model, --summary-dir) so that
any external scripts that invoke this module directly continue to work.
"""

import argparse
import sys
from pathlib import Path

# Add SREGym root to path
sregym_root = Path(__file__).resolve().parents[2]
if str(sregym_root) not in sys.path:
    sys.path.insert(0, str(sregym_root))

from clients.common.summarize import ResultSummarizer


def main():
    parser = argparse.ArgumentParser(description="Summarize Gemini CLI results")
    parser.add_argument("--logs-dir", type=str, required=True, help="Path to logs directory")
    parser.add_argument("--model", type=str, required=True, help="Model name used")
    parser.add_argument("--summary-dir", type=str, required=False, help="Path to summary directory")
    args = parser.parse_args()

    summary_dir = Path(args.summary_dir) if args.summary_dir else None
    summarizer = ResultSummarizer(
        Path(args.logs_dir),
        args.model,
        output_filename="gemini-cli.txt",
        summary_dir=summary_dir,
    )
    summarizer.run()


if __name__ == "__main__":
    main()
