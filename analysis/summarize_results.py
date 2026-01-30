import argparse
import csv
import glob
import os


def summarize_results(target_path=None):
    # Determine which files to process
    if target_path:
        if os.path.isdir(target_path):
            # If user provided a directory, search recursively for results inside
            print(f"Searching for result files in directory: '{target_path}'")
            # recursive=True requires the pattern to include ** for that part, but let's just use it on the pattern
            pattern = os.path.join(target_path, "**", "*_results.csv")
            files = glob.glob(pattern, recursive=True)
        else:
            # User provided a file pattern
            files = glob.glob(target_path, recursive=True)
            print(f"Searching for files matching: '{target_path}'")
    else:
        # Default behavior: find all CSV files recursively in current directory
        print("Searching for *_results.csv files recursively in current directory...")
        files = glob.glob("**/*_results.csv", recursive=True)

    all_runs = []

    print(f"Scanning {len(files)} CSV files for results...")

    for file_path in sorted(files):
        # Skip aggregate files and output files ONLY IF we are running in default mode.
        # If the user explicitly requested a file (e.g. ALL_results.csv), we should probably read it.
        # However, keeping safety logic is usually good, but let's relax it if the user specified a target.
        if not target_path:
             if "ALL_results" in file_path or "_output.csv" in file_path:
                continue

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                if not reader.fieldnames:
                    continue

                has_results_columns = "Diagnosis.success" in reader.fieldnames

                for row in reader:
                    pid = row.get("problem_id")
                    if not pid:
                        continue

                    row["source_file"] = os.path.basename(file_path)

                    if has_results_columns and row.get("Diagnosis.success"):
                        row["status"] = "Completed"
                    else:
                        row["status"] = "Incomplete"

                    all_runs.append(row)

        except Exception as e:
            print(f"Error reading {file_path}: {e}")

    if not all_runs:
        print("No result data found in the CSV files.")
        return

    # --- Aggregation ---
    total_runs = len(all_runs)
    completed_runs = [r for r in all_runs if r["status"] == "Completed"]
    incomplete_count = total_runs - len(completed_runs)

    diag_success_count = sum(1 for r in completed_runs if r.get("Diagnosis.success") == "True")
    mitig_success_count = sum(1 for r in completed_runs if r.get("Mitigation.success") == "True")

    ttls = []
    ttms = []
    for r in completed_runs:
        try:
            if r.get("TTL"):
                ttls.append(float(r["TTL"]))
            if r.get("TTM"):
                ttms.append(float(r["TTM"]))
        except ValueError:
            pass

    avg_ttl = sum(ttls) / len(ttls) if ttls else 0.0
    avg_ttm = sum(ttms) / len(ttms) if ttms else 0.0

    # --- Print Summary Table ---
    problem_id_width = 50
    table_width = 130
    print("\n" + "=" * table_width)
    print(f"{'SREGym Benchmark Run History':^{table_width}}")
    print("=" * table_width)
    print(f"Total Runs Detected: {total_runs}")
    print(f"  - Completed: {len(completed_runs)}")
    print(f"  - Incomplete: {incomplete_count}")
    print("-" * table_width)
    print(
        f"Diagnosis Success Rate (of completed): {diag_success_count}/{len(completed_runs)} ({diag_success_count / len(completed_runs) * 100 if completed_runs else 0:.1f}%)"
    )
    print(
        f"Mitigation Success Rate (of completed): {mitig_success_count}/{len(completed_runs)} ({mitig_success_count / len(completed_runs) * 100 if completed_runs else 0:.1f}%)"
    )
    print(f"Average Time to Locate:   {avg_ttl:.2f}s")
    print(f"Average Time to Mitigate: {avg_ttm:.2f}s")
    print("-" * table_width)

    header = f"{'Run Date':<14} | {'Problem ID':<{problem_id_width}} | {'Diag':<8} | {'Mitig':<8} | {'TTL(s)':<7} | {'TTM(s)':<7} | {'Status'}"
    print(header)
    print("-" * table_width)

    for r in all_runs:
        fname = r.get("source_file", "")
        # Extract timestamp 'MMDD_HHMM'
        run_date = fname[:9]

        pid = r.get("problem_id", "Unknown")
        if len(pid) > problem_id_width - 1:
            pid = pid[: problem_id_width - 4] + "..."

        if r["status"] == "Completed":
            d_res = "✅ PASS" if r.get("Diagnosis.success") == "True" else "❌ FAIL"
            m_res = "✅ PASS" if r.get("Mitigation.success") == "True" else "❌ FAIL"
            try:
                ttl = f"{float(r.get('TTL', 0)):.1f}"
            except:
                ttl = "N/A"
            try:
                ttm = f"{float(r.get('TTM', 0)):.1f}"
            except:
                ttm = "N/A"
            status = "DONE"
        else:
            d_res = "-"
            m_res = "-"
            ttl = "-"
            ttm = "-"
            status = "⚠️  INCOMPLETE"

        print(f"{run_date:<14} | {pid:<{problem_id_width}} | {d_res:<8} | {m_res:<8} | {ttl:<7} | {ttm:<7} | {status}")

    print("=" * table_width + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Summarize SREGym benchmark results.")
    parser.add_argument(
        "path",
        nargs="?",
        help="Optional path to a log directory (searched recursively) or a specific glob pattern.",
    )
    args = parser.parse_args()
    summarize_results(args.path)
