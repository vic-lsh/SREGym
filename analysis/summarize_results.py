import argparse
import csv
import glob
import os

try:
    import matplotlib.pyplot as plt
    import numpy as np

    HAS_PLOTTING = True
except ImportError:
    HAS_PLOTTING = False
    plt = None
    np = None


def load_results(target_path=None):
    # Determine which files to process
    if target_path:
        if os.path.isdir(target_path):
            # If user provided a directory, search recursively for results inside
            print(f"Searching for result files in directory: '{target_path}'")
            # recursive=True requires the pattern to include ** for that part
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

    all_runs_raw = []

    print(f"Scanning {len(files)} CSV files for results...")

    for file_path in sorted(files):
        # Skip aggregate files and output files ONLY IF we are running in default mode.
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

                    all_runs_raw.append(row)

        except Exception as e:
            print(f"Error reading {file_path}: {e}")

    # --- Filter: Keep only latest run per problem_id ---
    runs_by_id = {}
    for run in all_runs_raw:
        runs_by_id[run["problem_id"]] = run

    sorted_runs = list(runs_by_id.values())
    # Sort by source_file to restore roughly chronological order in the list
    sorted_runs.sort(key=lambda x: x.get("source_file", ""))

    return runs_by_id, sorted_runs


def summarize_results(target_path=None):
    _, all_runs = load_results(target_path)

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

    # --- Plot CDFs ---
    valid_ttls = [t for t in ttls if t > 0]
    valid_ttms = [t for t in ttms if t > 0]

    if not valid_ttls and not valid_ttms:
        print("No valid TTL or TTM data for plotting.")
        return

    if not HAS_PLOTTING:
        print("Matplotlib/Numpy not found. Skipping plots.")
        return

    plt.figure(figsize=(10, 6))

    if valid_ttls:
        valid_ttls.sort()
        y_ttls = np.arange(1, len(valid_ttls) + 1) / len(valid_ttls)
        plt.plot(
            valid_ttls,
            y_ttls,
            marker=".",
            linestyle="-",
            color="tab:blue",
            label=f"Time to Diagnosis (n={len(valid_ttls)})",
        )

    if valid_ttms:
        valid_ttms.sort()
        y_ttms = np.arange(1, len(valid_ttms) + 1) / len(valid_ttms)
        plt.plot(
            valid_ttms,
            y_ttms,
            marker=".",
            linestyle="-",
            color="tab:orange",
            label=f"Time to Mitigation (n={len(valid_ttms)})",
        )

    plt.xlabel("Time (s)")
    plt.ylabel("CDF")
    plt.title("CDF of Time to Diagnosis and Mitigation")
    plt.grid(True)
    plt.legend()

    if target_path and os.path.isdir(target_path):
        output_plot = os.path.join(target_path, "cdf_results.png")
    else:
        output_plot = "cdf_results.png"

    plt.savefig(output_plot)
    print(f"CDF plot saved to {output_plot}")


def diff_results(dir1, dir2):
    print(f"\n--- Loading results from {dir1} ---")
    runs1_map, _ = load_results(dir1)
    print(f"\n--- Loading results from {dir2} ---")
    runs2_map, _ = load_results(dir2)

    # Determine output directory
    name1 = os.path.basename(os.path.normpath(dir1))
    name2 = os.path.basename(os.path.normpath(dir2))
    output_dir = os.path.join("logs", "diff", f"{name1}--{name2}")
    os.makedirs(output_dir, exist_ok=True)
    print(f"\nDiff results will be stored in: {output_dir}")

    # Comparison Logic
    all_pids = sorted(list(set(runs1_map.keys()) | set(runs2_map.keys())))

    # Dynamic PID width based on data
    max_pid_len = max([len(p) for p in all_pids] + [len("Problem ID")])
    pid_width = max_pid_len

    # Dynamic column widths based on name length, minimum 10
    n1 = name1 if len(name1) <= 20 else name1[:17] + "..."
    n2 = name2 if len(name2) <= 20 else name2[:17] + "..."

    w_d1 = max(10, len(n1) + 2)  # "D:name"
    w_d2 = max(10, len(n2) + 2)
    w_m1 = max(10, len(n1) + 2)
    w_m2 = max(10, len(n2) + 2)
    diff_width = 12

    header = (
        f"{'Problem ID':<{pid_width}} | "
        f"{'D:' + n1:<{w_d1}} | {'D:' + n2:<{w_d2}} | {'Diff(TTL)':<{diff_width}} | "
        f"{'M:' + n1:<{w_m1}} | {'M:' + n2:<{w_m2}} | {'Diff(TTM)':<{diff_width}}"
    )
    sep = "-" * len(header)
    summary_lines = []
    summary_lines.append(header)
    summary_lines.append(sep)

    ttd1, ttd2 = [], []
    ttm1, ttm2 = [], []

    def pad_emoji(s, width):
        # Calculate visual length: emojis (checked by presence of check/cross) + rest
        # In many terminals, emoji is 2 chars wide. Python len() counts it as 1.
        # So visual length = len(s) + 1 if emoji present.
        has_emoji = "✅" in s or "❌" in s
        visual_len = len(s) + (1 if has_emoji else 0)
        padding = max(0, width - visual_len)
        return s + " " * padding

    for pid in all_pids:
        r1 = runs1_map.get(pid)
        r2 = runs2_map.get(pid)

        # Helper to get status and times
        def get_data(r):
            if not r:
                return "MISSING", "MISSING", None, None

            # Helper to safe parse float
            def parse_float(val):
                try:
                    return float(val)
                except (ValueError, TypeError):
                    return None

            t_d = parse_float(r.get("TTL"))
            t_m = parse_float(r.get("TTM"))

            if r["status"] == "Completed":
                d_stat = "✅ PASS" if r.get("Diagnosis.success") == "True" else "❌ FAIL"
                m_stat = "✅ PASS" if r.get("Mitigation.success") == "True" else "❌ FAIL"
            else:
                d_stat = "-"
                m_stat = "-"

            return d_stat, m_stat, t_d, t_m

        d1_s, m1_s, td1, tm1 = get_data(r1)
        d2_s, m2_s, td2, tm2 = get_data(r2)

        # Collect for CDFs
        if r1 and td1 is not None and td1 > 0:
            ttd1.append(td1)
        if r2 and td2 is not None and td2 > 0:
            ttd2.append(td2)

        if r1 and tm1 is not None and tm1 > 0:
            ttm1.append(tm1)
        if r2 and tm2 is not None and tm2 > 0:
            ttm2.append(tm2)

        # Diff strings
        if td1 is not None and td2 is not None:
            diff_td = f"{td1 - td2:+.1f}s"
        else:
            diff_td = "-"

        if tm1 is not None and tm2 is not None:
            diff_tm = f"{tm1 - tm2:+.1f}s"
        else:
            diff_tm = "-"

        # Use pad_emoji for status columns
        d1_str = pad_emoji(d1_s, w_d1)
        d2_str = pad_emoji(d2_s, w_d2)
        m1_str = pad_emoji(m1_s, w_m1)
        m2_str = pad_emoji(m2_s, w_m2)

        line = (
            f"{pid:<{pid_width}} | "
            f"{d1_str} | {d2_str} | {diff_td:<{diff_width}} | "
            f"{m1_str} | {m2_str} | {diff_tm:<{diff_width}}"
        )
        summary_lines.append(line)

    # Write summary
    summary_path = os.path.join(output_dir, "summary.txt")
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("\n".join(summary_lines))

    print("\n".join(summary_lines))
    print(f"\nSummary saved to {summary_path}")

    # Plotting
    plot_cdfs(
        ttd1,
        ttd2,
        name1,
        name2,
        "Time to Diagnosis (TTL)",
        os.path.join(output_dir, "cdf_diagnosis.png"),
        colors=["#004d99", "#66b3ff"],  # Dark Blue, Lighter Blue
    )
    plot_cdfs(
        ttm1,
        ttm2,
        name1,
        name2,
        "Time to Mitigation (TTM)",
        os.path.join(output_dir, "cdf_mitigation.png"),
        colors=["#cc5200", "#ff9933"],  # Dark Orange, Lighter Orange
    )


def plot_cdfs(data1, data2, label1, label2, title_metric, output_path, colors=None):
    if not HAS_PLOTTING:
        print(f"Matplotlib/Numpy not found. Skipping plot: {output_path}")
        return

    if colors is None:
        colors = ["tab:blue", "tab:orange"]

    plt.figure(figsize=(10, 6))

    data1.sort()
    data2.sort()

    has_data = False

    if data1:
        y1 = np.arange(1, len(data1) + 1) / len(data1)
        plt.plot(
            data1,
            y1,
            marker=".",
            linestyle="-",
            color=colors[0],
            label=f"{label1} (n={len(data1)})",
        )
        has_data = True

    if data2:
        y2 = np.arange(1, len(data2) + 1) / len(data2)
        plt.plot(
            data2,
            y2,
            marker="x",
            linestyle="--",
            color=colors[1],
            label=f"{label2} (n={len(data2)})",
        )
        has_data = True

    if not has_data:
        print(f"No valid data to plot for {title_metric}")
        plt.close()
        return

    plt.xlabel("Time (s)")
    plt.ylabel("CDF")
    plt.title(f"CDF of {title_metric}")
    plt.grid(True)
    plt.legend()
    plt.savefig(output_path)
    plt.close()
    print(f"Plot saved to {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Summarize SREGym benchmark results.")
    parser.add_argument(
        "--diff", nargs=2, metavar=("DIR1", "DIR2"), help="Compare results between two log directories."
    )
    parser.add_argument(
        "path",
        nargs="?",
        help="Optional path to a log directory (searched recursively) or a specific glob pattern.",
    )
    args = parser.parse_args()

    if args.diff:
        diff_results(args.diff[0], args.diff[1])
    else:
        summarize_results(args.path)
