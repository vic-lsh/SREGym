import argparse
import csv
import glob
import json
import os
import re
import sys

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
        elif not os.path.exists(target_path) and not any(c in target_path for c in "*?[]"):
            print(f"Error: The path '{target_path}' does not exist.")
            sys.exit(1)
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


def load_stratus_tokens(log_dir):
    """Load token usage from Stratus *_stratus_output.csv files.
    Sums diagnosis + mitigation (all agent rows) per problem.
    Returns dict[problem_id, total_tokens] or empty dict if none found.
    """
    pattern = os.path.join(log_dir, "**", "*_stratus_output.csv")
    files = glob.glob(pattern, recursive=True)
    if not files:
        return {}

    tokens_by_pid = {}
    for file_path in files:
        # Extract problem_id: {MMDD_HHMM}_{problem_id}_stratus_output.csv
        basename = os.path.basename(file_path)
        if not basename.endswith("_stratus_output.csv"):
            continue
        rest = basename[: -len("_stratus_output.csv")]
        parts = rest.split("_", 2)  # MMDD, HHMM, problem_id (may contain underscores)
        if len(parts) < 3:
            continue
        problem_id = parts[2]

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                total = 0
                for row in reader:
                    try:
                        total += int(row.get("total_tokens", 0) or 0)
                    except (ValueError, TypeError):
                        pass
                if total > 0:
                    tokens_by_pid[problem_id] = total
        except Exception as e:
            print(f"Warning: could not read {file_path}: {e}")
    return tokens_by_pid


def load_gemini_tokens(log_dir):
    """Load token usage from Gemini gemini_cli_results_*.json files.
    Returns dict[problem_id, total_tokens] or empty dict if none found.
    """
    # Check log_dir/gemini_cli/ and log_dir/
    for subdir in ["gemini_cli", ""]:
        base = os.path.join(log_dir, subdir) if subdir else log_dir
        pattern = os.path.join(base, "gemini_cli_results_*.json")
        files = glob.glob(pattern)
        if not files:
            continue

        tokens_by_pid = {}
        for file_path in files:
            basename = os.path.basename(file_path)
            m = re.match(r"gemini_cli_results_(.+)_\d{8}_\d{6}\.json", basename)
            if not m:
                continue
            problem_id = m.group(1)

            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                um = data.get("usage_metrics", {})
                inp = int(um.get("input_tokens", 0) or 0)
                out = int(um.get("output_tokens", 0) or 0)
                total = inp + out
                if total > 0:
                    tokens_by_pid[problem_id] = total
            except Exception as e:
                print(f"Warning: could not read {file_path}: {e}")
        if tokens_by_pid:
            return tokens_by_pid
    return {}


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

    # --- Statistics Summary ---
    def get_stats(run_map):
        runs = list(run_map.values())
        total = len(runs)
        completed = [r for r in runs if r["status"] == "Completed"]
        n_comp = len(completed)
        d_succ = sum(1 for r in completed if r.get("Diagnosis.success") == "True")
        m_succ = sum(1 for r in completed if r.get("Mitigation.success") == "True")
        ttls, ttms = [], []
        for r in completed:
            try:
                if r.get("TTL"):
                    ttls.append(float(r["TTL"]))
            except (ValueError, TypeError):
                pass
            try:
                if r.get("TTM"):
                    ttms.append(float(r["TTM"]))
            except (ValueError, TypeError):
                pass
        avg_ttl = sum(ttls) / len(ttls) if ttls else 0.0
        avg_ttm = sum(ttms) / len(ttms) if ttms else 0.0
        return total, n_comp, d_succ, m_succ, avg_ttl, avg_ttm

    t1, c1, d1, m1, attl1, attm1 = get_stats(runs1_map)
    t2, c2, d2, m2, attl2, attm2 = get_stats(runs2_map)

    w_col = max(len(name1), len(name2), 18)

    print("\n" + "=" * (30 + 2 * w_col + 15))
    print(f"{'Statistic':<25} | {name1:<{w_col}} | {name2:<{w_col}} | {'Diff':<10}")
    print("-" * (30 + 2 * w_col + 15))

    print(f"{'Total Runs':<25} | {t1:<{w_col}} | {t2:<{w_col}} | {t1 - t2:+d}")
    print(f"{'Completed Runs':<25} | {c1:<{w_col}} | {c2:<{w_col}} | {c1 - c2:+d}")

    d1_pct = (d1 / c1 * 100) if c1 else 0.0
    d2_pct = (d2 / c2 * 100) if c2 else 0.0
    d1_s = f"{d1}/{c1} ({d1_pct:.1f}%)"
    d2_s = f"{d2}/{c2} ({d2_pct:.1f}%)"
    print(f"{'Diagnosis Success':<25} | {d1_s:<{w_col}} | {d2_s:<{w_col}} | {d1_pct - d2_pct:+.1f}%")

    m1_pct = (m1 / c1 * 100) if c1 else 0.0
    m2_pct = (m2 / c2 * 100) if c2 else 0.0
    m1_s = f"{m1}/{c1} ({m1_pct:.1f}%)"
    m2_s = f"{m2}/{c2} ({m2_pct:.1f}%)"
    print(f"{'Mitigation Success':<25} | {m1_s:<{w_col}} | {m2_s:<{w_col}} | {m1_pct - m2_pct:+.1f}%")

    attl1_s = f"{attl1:.1f}s"
    attl2_s = f"{attl2:.1f}s"
    print(f"{'Avg TTL':<25} | {attl1_s:<{w_col}} | {attl2_s:<{w_col}} | {attl1 - attl2:+.1f}s")

    attm1_s = f"{attm1:.1f}s"
    attm2_s = f"{attm2:.1f}s"
    print(f"{'Avg TTM':<25} | {attm1_s:<{w_col}} | {attm2_s:<{w_col}} | {attm1 - attm2:+.1f}s")

    print("=" * (30 + 2 * w_col + 15) + "\n")

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
    ttd1_success, ttd2_success = [], []
    ttm1_success, ttm2_success = [], []
    comp_diag_data, comp_mitig_data = [], []
    comp_diag_success_data, comp_mitig_success_data = [], []
    comp_diag_fail_data, comp_mitig_fail_data = [], []

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
            if r1.get("Diagnosis.success") == "True":
                ttd1_success.append(td1)

        if r2 and td2 is not None and td2 > 0:
            ttd2.append(td2)
            if r2.get("Diagnosis.success") == "True":
                ttd2_success.append(td2)

        if r1 and tm1 is not None and tm1 > 0:
            ttm1.append(tm1)
            if r1.get("Mitigation.success") == "True":
                ttm1_success.append(tm1)

        if r2 and tm2 is not None and tm2 > 0:
            ttm2.append(tm2)
            if r2.get("Mitigation.success") == "True":
                ttm2_success.append(tm2)

        # Helper vars for readability
        diag_s1 = r1 and r1.get("Diagnosis.success") == "True"
        diag_s2 = r2 and r2.get("Diagnosis.success") == "True"
        mitig_s1 = r1 and r1.get("Mitigation.success") == "True"
        mitig_s2 = r2 and r2.get("Mitigation.success") == "True"

        if (td1 is not None and td1 > 0) or (td2 is not None and td2 > 0):
            comp_diag_data.append((pid, td1, td2, diag_s1, diag_s2))

        td1_succ = td1 if diag_s1 else None
        td2_succ = td2 if diag_s2 else None
        if (td1_succ is not None and td1_succ > 0) and (td2_succ is not None and td2_succ > 0):
            comp_diag_success_data.append((pid, td1_succ, td2_succ, True, True))

        td1_fail = td1 if (r1 and not diag_s1) else None
        td2_fail = td2 if (r2 and not diag_s2) else None
        if (td1_fail is not None and td1_fail > 0) and (td2_fail is not None and td2_fail > 0):
            comp_diag_fail_data.append((pid, td1_fail, td2_fail, False, False))

        if (tm1 is not None and tm1 > 0) or (tm2 is not None and tm2 > 0):
            comp_mitig_data.append((pid, tm1, tm2, mitig_s1, mitig_s2))

        tm1_succ = tm1 if mitig_s1 else None
        tm2_succ = tm2 if mitig_s2 else None
        if (tm1_succ is not None and tm1_succ > 0) and (tm2_succ is not None and tm2_succ > 0):
            comp_mitig_success_data.append((pid, tm1_succ, tm2_succ, True, True))

        tm1_fail = tm1 if (r1 and not mitig_s1) else None
        tm2_fail = tm2 if (r2 and not mitig_s2) else None
        if (tm1_fail is not None and tm1_fail > 0) and (tm2_fail is not None and tm2_fail > 0):
            comp_mitig_fail_data.append((pid, tm1_fail, tm2_fail, False, False))

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

    # Plotting Success Only
    plot_cdfs(
        ttd1_success,
        ttd2_success,
        name1,
        name2,
        "Time to Diagnosis (Success Only)",
        os.path.join(output_dir, "cdf_diagnosis_success.png"),
        colors=["#004d99", "#66b3ff"],  # Dark Blue, Lighter Blue
    )
    plot_cdfs(
        ttm1_success,
        ttm2_success,
        name1,
        name2,
        "Time to Mitigation (Success Only)",
        os.path.join(output_dir, "cdf_mitigation_success.png"),
        colors=["#cc5200", "#ff9933"],  # Dark Orange, Lighter Orange
    )

    plot_comparison_by_problem(
        comp_diag_data,
        name1,
        name2,
        "Diagnosis Time",
        os.path.join(output_dir, "comparison_diagnosis.png"),
        colors=["#004d99", "#66b3ff"],
        use_status_colors=True,
    )
    plot_comparison_by_problem(
        comp_diag_data,
        name1,
        name2,
        "Diagnosis Time",
        os.path.join(output_dir, "comparison_diagnosis_compact.png"),
        colors=["#004d99", "#66b3ff"],
        compact=True,
        use_status_colors=True,
    )
    plot_comparison_by_problem(
        comp_mitig_data,
        name1,
        name2,
        "Mitigation Time",
        os.path.join(output_dir, "comparison_mitigation.png"),
        colors=["#cc5200", "#ff9933"],
        use_status_colors=True,
    )
    plot_comparison_by_problem(
        comp_mitig_data,
        name1,
        name2,
        "Mitigation Time",
        os.path.join(output_dir, "comparison_mitigation_compact.png"),
        colors=["#cc5200", "#ff9933"],
        compact=True,
        use_status_colors=True,
    )
    plot_comparison_by_problem(
        comp_diag_success_data,
        name1,
        name2,
        "Diagnosis Time (Success Only)",
        os.path.join(output_dir, "comparison_diagnosis_success.png"),
        colors=["#004d99", "#66b3ff"],
    )
    plot_comparison_by_problem(
        comp_diag_success_data,
        name1,
        name2,
        "Diagnosis Time (Success Only)",
        os.path.join(output_dir, "comparison_diagnosis_success_compact.png"),
        colors=["#004d99", "#66b3ff"],
        compact=True,
    )
    plot_comparison_by_problem(
        comp_mitig_success_data,
        name1,
        name2,
        "Mitigation Time (Success Only)",
        os.path.join(output_dir, "comparison_mitigation_success.png"),
        colors=["#cc5200", "#ff9933"],
    )
    plot_comparison_by_problem(
        comp_mitig_success_data,
        name1,
        name2,
        "Mitigation Time (Success Only)",
        os.path.join(output_dir, "comparison_mitigation_success_compact.png"),
        colors=["#cc5200", "#ff9933"],
        compact=True,
    )
    plot_comparison_by_problem(
        comp_diag_fail_data,
        name1,
        name2,
        "Diagnosis Time (Failure Only)",
        os.path.join(output_dir, "comparison_diagnosis_failure.png"),
        colors=["#004d99", "#66b3ff"],
    )
    plot_comparison_by_problem(
        comp_diag_fail_data,
        name1,
        name2,
        "Diagnosis Time (Failure Only)",
        os.path.join(output_dir, "comparison_diagnosis_failure_compact.png"),
        colors=["#004d99", "#66b3ff"],
        compact=True,
    )
    plot_comparison_by_problem(
        comp_mitig_fail_data,
        name1,
        name2,
        "Mitigation Time (Failure Only)",
        os.path.join(output_dir, "comparison_mitigation_failure.png"),
        colors=["#cc5200", "#ff9933"],
    )
    plot_comparison_by_problem(
        comp_mitig_fail_data,
        name1,
        name2,
        "Mitigation Time (Failure Only)",
        os.path.join(output_dir, "comparison_mitigation_failure_compact.png"),
        colors=["#cc5200", "#ff9933"],
        compact=True,
    )

    # --- By Name Variations ---
    plot_comparison_by_problem(
        comp_diag_data,
        name1,
        name2,
        "Diagnosis Time (By Name)",
        os.path.join(output_dir, "comparison_diagnosis_by_name.png"),
        colors=["#004d99", "#66b3ff"],
        use_status_colors=True,
        sort_by_name=True,
    )
    plot_comparison_by_problem(
        comp_mitig_data,
        name1,
        name2,
        "Mitigation Time (By Name)",
        os.path.join(output_dir, "comparison_mitigation_by_name.png"),
        colors=["#cc5200", "#ff9933"],
        use_status_colors=True,
        sort_by_name=True,
    )
    plot_comparison_by_problem(
        comp_diag_success_data,
        name1,
        name2,
        "Diagnosis Time (Success Only, By Name)",
        os.path.join(output_dir, "comparison_diagnosis_success_by_name.png"),
        colors=["#004d99", "#66b3ff"],
        sort_by_name=True,
    )
    plot_comparison_by_problem(
        comp_mitig_success_data,
        name1,
        name2,
        "Mitigation Time (Success Only, By Name)",
        os.path.join(output_dir, "comparison_mitigation_success_by_name.png"),
        colors=["#cc5200", "#ff9933"],
        sort_by_name=True,
    )
    plot_comparison_by_problem(
        comp_diag_fail_data,
        name1,
        name2,
        "Diagnosis Time (Failure Only, By Name)",
        os.path.join(output_dir, "comparison_diagnosis_failure_by_name.png"),
        colors=["#004d99", "#66b3ff"],
        sort_by_name=True,
    )
    plot_comparison_by_problem(
        comp_mitig_fail_data,
        name1,
        name2,
        "Mitigation Time (Failure Only, By Name)",
        os.path.join(output_dir, "comparison_mitigation_failure_by_name.png"),
        colors=["#cc5200", "#ff9933"],
        sort_by_name=True,
    )

    # --- Token-based comparison plots ---
    plot_success_rates(
        d1,
        m1,
        c1,
        name1,
        d2,
        m2,
        c2,
        name2,
        os.path.join(output_dir, "success_rates_comparison.png"),
        colors=["#1f77b4", "#ff7f0e"],
    )

    tokens1_map = load_stratus_tokens(dir1) or load_gemini_tokens(dir1)
    tokens2_map = load_stratus_tokens(dir2) or load_gemini_tokens(dir2)

    if tokens1_map or tokens2_map:
        tokens1_list = [t for t in tokens1_map.values() if t and t > 0]
        tokens2_list = [t for t in tokens2_map.values() if t and t > 0]

        # 1) CDF of token usage
        plot_cdf_tokens(
            tokens1_list,
            tokens2_list,
            name1,
            name2,
            os.path.join(output_dir, "cdf_tokens.png"),
            colors=["#004d99", "#66b3ff"],
        )

        # 2) Scatter plot: Y=problem labels, X=diag+mitigation token usage (dots like time-based)
        comp_token_data = []
        for pid in all_pids:
            t1 = tokens1_map.get(pid)
            t2 = tokens2_map.get(pid)
            if (t1 is not None and t1 > 0) or (t2 is not None and t2 > 0):
                r1 = runs1_map.get(pid)
                r2 = runs2_map.get(pid)
                d1 = r1 and r1.get("Diagnosis.success") == "True"
                m1 = r1 and r1.get("Mitigation.success") == "True"
                d2 = r2 and r2.get("Diagnosis.success") == "True"
                m2 = r2 and r2.get("Mitigation.success") == "True"
                comp_token_data.append((pid, t1 or 0, t2 or 0, d1 and m1, d2 and m2))
        plot_token_comparison_by_problem(
            comp_token_data,
            name1,
            name2,
            os.path.join(output_dir, "comparison_tokens.png"),
            colors=["#004d99", "#66b3ff"],
            use_status_colors=True,
        )
        plot_token_comparison_by_problem(
            comp_token_data,
            name1,
            name2,
            os.path.join(output_dir, "comparison_tokens_by_name.png"),
            colors=["#004d99", "#66b3ff"],
            use_status_colors=True,
            sort_by_name=True,
        )

        # 3) Scatter: tokens vs aggregate time (TTL + TTM)
        tokens_time_data1 = []
        tokens_time_data2 = []
        for pid in all_pids:
            tok1 = tokens1_map.get(pid)
            tok2 = tokens2_map.get(pid)
            r1 = runs1_map.get(pid)
            r2 = runs2_map.get(pid)

            def parse_float(val):
                try:
                    return float(val)
                except (ValueError, TypeError):
                    return None

            ttl1 = parse_float(r1.get("TTL")) if r1 else None
            ttm1 = parse_float(r1.get("TTM")) if r1 else None
            ttl2 = parse_float(r2.get("TTL")) if r2 else None
            ttm2 = parse_float(r2.get("TTM")) if r2 else None

            agg1 = (ttl1 or 0) + (ttm1 or 0)
            agg2 = (ttl2 or 0) + (ttm2 or 0)

            if tok1 and tok1 > 0 and agg1 > 0:
                tokens_time_data1.append((tok1, agg1, pid))
            if tok2 and tok2 > 0 and agg2 > 0:
                tokens_time_data2.append((tok2, agg2, pid))

        plot_tokens_vs_time(
            tokens_time_data1,
            tokens_time_data2,
            name1,
            name2,
            os.path.join(output_dir, "scatter_tokens_vs_time.png"),
            colors=["#004d99", "#66b3ff"],
        )


def plot_tokens_vs_time(data1, data2, label1, label2, output_path, colors=None):
    """Scatter plot: X=tokens (diag+mitigation), Y=aggregate time (TTL+TTM)."""
    if not HAS_PLOTTING:
        print(f"Matplotlib/Numpy not found. Skipping plot: {output_path}")
        return

    if not data1 and not data2:
        print("No tokens+time data for scatter plot.")
        return

    if colors is None:
        colors = ["tab:blue", "tab:orange"]

    plt.figure(figsize=(10, 6))
    has_data = False

    if data1:
        x1 = [d[0] for d in data1]
        y1 = [d[1] for d in data1]
        plt.scatter(x1, y1, color=colors[0], label=f"{label1} (n={len(data1)})", marker="o", alpha=0.7)
        has_data = True

    if data2:
        x2 = [d[0] for d in data2]
        y2 = [d[1] for d in data2]
        plt.scatter(x2, y2, color=colors[1], label=f"{label2} (n={len(data2)})", marker="x", alpha=0.7)
        has_data = True

    if has_data:
        plt.xlabel("Tokens (diagnosis + mitigation)")
        plt.ylabel("Time (s) — TTL + TTM")
        plt.title("Tokens vs Aggregate Time (Diagnosis + Mitigation)")
        plt.grid(True)
        plt.legend()
        plt.tight_layout()
        plt.savefig(output_path)
        plt.close()
        print(f"Tokens vs time scatter plot saved to {output_path}")


def plot_cdf_tokens(data1, data2, label1, label2, output_path, colors=None):
    """Plot CDF of token usage for two agents."""
    if not HAS_PLOTTING:
        print(f"Matplotlib/Numpy not found. Skipping plot: {output_path}")
        return

    if not data1 and not data2:
        print("No token data for CDF plot.")
        return

    if colors is None:
        colors = ["tab:blue", "tab:orange"]

    plt.figure(figsize=(10, 6))
    has_data = False

    if data1:
        data1 = sorted(data1)
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
        data2 = sorted(data2)
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

    if has_data:
        plt.xlabel("Tokens (diagnosis + mitigation)")
        plt.ylabel("CDF")
        plt.title("CDF of Token Usage (Diagnosis + Mitigation)")
        plt.grid(True)
        plt.legend()
        plt.savefig(output_path)
        plt.close()
        print(f"Token CDF plot saved to {output_path}")


def plot_token_comparison_by_problem(
    data,
    name1,
    name2,
    output_path,
    colors=None,
    use_status_colors=False,
    sort_by_name=False,
):
    """Plot scatter: Y=problem labels, X=token usage for both agents (dots like time-based)."""
    if not HAS_PLOTTING:
        print(f"Matplotlib/Numpy not found. Skipping plot: {output_path}")
        return

    if not data:
        print("No token data for comparison plot.")
        return

    if colors is None:
        colors = ["tab:blue", "tab:orange"]

    if sort_by_name:
        data = sorted(data, key=lambda x: x[0])
    else:
        data = sorted(data, key=lambda x: x[1] if x[1] else 0)

    pids = [d[0] for d in data]
    fig_height = max(6, len(pids) * 0.3)
    plt.figure(figsize=(10, fig_height))

    y_vals = np.arange(len(pids))

    x1_succ, x1_fail, y1_succ, y1_fail = [], [], [], []
    x2_succ, x2_fail, y2_succ, y2_fail = [], [], [], []

    for i, (pid, t1, t2, s1, s2) in enumerate(data):
        if t1 and t1 > 0:
            if s1:
                x1_succ.append(t1)
                y1_succ.append(i)
            else:
                x1_fail.append(t1)
                y1_fail.append(i)
        if t2 and t2 > 0:
            if s2:
                x2_succ.append(t2)
                y2_succ.append(i)
            else:
                x2_fail.append(t2)
                y2_fail.append(i)

    c_succ, c_fail = "tab:green", "tab:red"
    m1, m2 = "o", "x"

    if use_status_colors:
        if x1_succ:
            plt.scatter(x1_succ, y1_succ, color=c_succ, label=f"{name1} (Success)", marker=m1, alpha=0.7)
        if x1_fail:
            plt.scatter(x1_fail, y1_fail, color=c_fail, label=f"{name1} (Fail)", marker=m1, alpha=0.7)
        if x2_succ:
            plt.scatter(x2_succ, y2_succ, color=c_succ, label=f"{name2} (Success)", marker=m2, alpha=0.7)
        if x2_fail:
            plt.scatter(x2_fail, y2_fail, color=c_fail, label=f"{name2} (Fail)", marker=m2, alpha=0.7)
    else:
        x1_all = x1_succ + x1_fail
        y1_all = y1_succ + y1_fail
        x2_all = x2_succ + x2_fail
        y2_all = y2_succ + y2_fail
        if x1_all:
            plt.scatter(x1_all, y1_all, color=colors[0], label=name1, marker=m1, alpha=0.7)
        if x2_all:
            plt.scatter(x2_all, y2_all, color=colors[1], label=name2, marker=m2, alpha=0.7)

    plt.yticks(y_vals, pids)
    plt.xlabel("Tokens (diagnosis + mitigation)")
    plt.title("Per-Problem Token Usage")
    plt.grid(True, axis="y", linestyle=":", alpha=0.3)
    plt.grid(True, axis="x", linestyle="--", alpha=0.7)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()
    print(f"Token comparison plot saved to {output_path}")


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


def plot_comparison_by_problem(
    data,
    name1,
    name2,
    title_metric,
    output_path,
    colors=None,
    compact=False,
    use_status_colors=False,
    sort_by_name=False,
):
    if not HAS_PLOTTING:
        print(f"Matplotlib/Numpy not found. Skipping plot: {output_path}")
        return

    if not data:
        print(f"No valid data to plot for {title_metric}")
        return

    if colors is None:
        colors = ["tab:blue", "tab:orange"]

    if sort_by_name:
        data.sort(key=lambda x: x[0])
    else:
        # Sort by first dir's output time (x[1]).
        # Place None/Missing at the end (top of graph).
        data.sort(key=lambda x: x[1] if x[1] is not None else float("inf"))

    pids = [d[0] for d in data]

    if compact:
        # Compact mode: tighter vertical spacing
        fig_height = max(6, len(pids) * 0.05)
    else:
        # Standard mode: enough space for labels
        fig_height = max(6, len(pids) * 0.3)

    plt.figure(figsize=(10, fig_height))

    y_vals = np.arange(len(pids))

    # Extract valid points for series 1
    x1_succ, y1_succ = [], []
    x1_fail, y1_fail = [], []

    # Extract valid points for series 2
    x2_succ, y2_succ = [], []
    x2_fail, y2_fail = [], []

    for i, item in enumerate(data):
        # Unpack with default for backward compatibility if needed, though we updated all calls
        # item structure: (pid, v1, v2, s1, s2)
        v1 = item[1]
        v2 = item[2]
        s1 = item[3] if len(item) > 3 else True
        s2 = item[4] if len(item) > 4 else True

        if v1 is not None and v1 > 0:
            if s1:
                x1_succ.append(v1)
                y1_succ.append(i)
            else:
                x1_fail.append(v1)
                y1_fail.append(i)

        if v2 is not None and v2 > 0:
            if s2:
                x2_succ.append(v2)
                y2_succ.append(i)
            else:
                x2_fail.append(v2)
                y2_fail.append(i)

    # Plot Series 1
    # Colors: Success=Green, Fail=Red
    # Shapes: Series1=Circle('o'), Series2=Cross('x')
    c_succ = "tab:green"
    c_fail = "tab:red"

    m1 = "o"
    m2 = "x"

    if use_status_colors:
        if x1_succ:
            plt.scatter(x1_succ, y1_succ, color=c_succ, label=f"{name1} (Success)", marker=m1, alpha=0.7)
        if x1_fail:
            plt.scatter(x1_fail, y1_fail, color=c_fail, label=f"{name1} (Fail)", marker=m1, alpha=0.7)

        # Plot Series 2
        if x2_succ:
            plt.scatter(x2_succ, y2_succ, color=c_succ, label=f"{name2} (Success)", marker=m2, alpha=0.7)
        if x2_fail:
            plt.scatter(x2_fail, y2_fail, color=c_fail, label=f"{name2} (Fail)", marker=m2, alpha=0.7)
    else:
        # Use provided colors for series distinction
        # Combine succ/fail lists for each series since color is uniform
        x1_all = x1_succ + x1_fail
        y1_all = y1_succ + y1_fail
        x2_all = x2_succ + x2_fail
        y2_all = y2_succ + y2_fail

        if x1_all:
            plt.scatter(x1_all, y1_all, color=colors[0], label=name1, marker=m1, alpha=0.7)
        if x2_all:
            plt.scatter(x2_all, y2_all, color=colors[1], label=name2, marker=m2, alpha=0.7)

    if not compact:
        plt.yticks(y_vals, pids)
        plt.grid(True, axis="y", linestyle=":", alpha=0.3)
    else:
        plt.yticks([])

    plt.xlabel("Time (s)")
    plt.title(f"Per-Problem {title_metric}")
    plt.grid(True, axis="x", linestyle="--", alpha=0.7)
    plt.legend()

    # Do NOT invert Y axis, so lowest time (index 0) is at the bottom (y=0)
    # plt.gca().invert_yaxis()

    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()
    print(f"Comparison plot saved to {output_path}")


def plot_success_rates(d1, m1, n1, name1, d2, m2, n2, name2, output_path, colors=None):
    """
    Bar chart comparing diagnosis and mitigation success rates.
    """
    if not HAS_PLOTTING:
        print(f"Matplotlib/Numpy not found. Skipping plot: {output_path}")
        return

    if colors is None:
        colors = ["tab:blue", "tab:orange"]

    # Calculate rates
    d1_rate = (d1 / n1 * 100) if n1 > 0 else 0.0
    m1_rate = (m1 / n1 * 100) if n1 > 0 else 0.0
    d2_rate = (d2 / n2 * 100) if n2 > 0 else 0.0
    m2_rate = (m2 / n2 * 100) if n2 > 0 else 0.0

    labels = ["Diagnosis", "Mitigation"]
    x = np.arange(len(labels))
    width = 0.35

    plt.figure(figsize=(8, 6))

    # Agent 1 bars
    rects1 = plt.bar(
        x - width / 2,
        [d1_rate, m1_rate],
        width,
        label=f"{name1} (n={n1})",
        color=colors[0],
    )
    # Agent 2 bars
    rects2 = plt.bar(
        x + width / 2,
        [d2_rate, m2_rate],
        width,
        label=f"{name2} (n={n2})",
        color=colors[1],
    )

    plt.ylabel("Success Rate (%)")
    plt.title("Diagnosis & Mitigation Success Rates")
    plt.xticks(x, labels)
    plt.ylim(0, 110)  # Extra space for labels
    plt.legend()
    plt.grid(True, axis="y", linestyle="--", alpha=0.7)

    def autolabel(rects):
        """Attach a text label above each bar in *rects*, displaying its height."""
        for rect in rects:
            height = rect.get_height()
            plt.annotate(
                f"{height:.1f}%",
                xy=(rect.get_x() + rect.get_width() / 2, height),
                xytext=(0, 3),  # 3 points vertical offset
                textcoords="offset points",
                ha="center",
                va="bottom",
            )

    autolabel(rects1)
    autolabel(rects2)

    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()
    print(f"Success rate plot saved to {output_path}")


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
