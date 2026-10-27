"""Every Locust oracle alert must forget startup/earlier-incident failures."""

from pathlib import Path

import pytest
import yaml


@pytest.mark.parametrize("namespace", ["astronomy-shop", "train-ticket"])
def test_locust_failure_alerts_use_recent_counter_rates(namespace):
    values = yaml.safe_load(
        (Path(__file__).resolve().parents[2] / "sregym/observer/prometheus/prometheus/values.yaml").read_text()
    )
    rules = [rule for group in values["serverFiles"]["alerting_rules.yml"]["groups"] for rule in group["rules"]]
    matched = [
        rule["expr"]
        for rule in rules
        if f'locust_requests_num_failures{{namespace="{namespace}"' in rule.get("expr", "")
    ]
    assert matched
    for expr in matched:
        assert f'rate(locust_requests_num_failures{{namespace="{namespace}", name!="Aggregated"}}[2m])' in expr
        assert expr.count(f'rate(locust_requests_num_requests{{namespace="{namespace}", name!="Aggregated"}}[2m])') == 2


def test_locust_rules_recover_after_startup_faults_and_counter_reset(tmp_path):
    """Execute the real alert expressions, including their `for`, in Prometheus."""
    import os
    import shutil
    import subprocess

    promtool = os.environ.get("PROMTOOL") or shutil.which("promtool")
    if not promtool:
        pytest.skip("install promtool or set PROMTOOL for the Prometheus rule evaluator")
    values = yaml.safe_load(
        (Path(__file__).resolve().parents[2] / "sregym/observer/prometheus/prometheus/values.yaml").read_text()
    )
    rules = [
        rule
        for group in values["serverFiles"]["alerting_rules.yml"]["groups"]
        for rule in group["rules"]
        if "locust_requests_num_failures" in rule.get("expr", "")
    ]
    rule_path = tmp_path / "rules.yml"
    rule_path.write_text(yaml.safe_dump({"groups": [{"name": "locust", "rules": rules}]}))
    series = []
    for namespace in ("astronomy-shop", "train-ticket"):
        for endpoint, failures in (
            ("healthy", "80+0x10"),  # startup errors, then healthy recent requests
            ("fault", "0+30x4 120+0x6"),  # ongoing fault, then repaired
            ("reset", "0+30x4 0+0x6"),  # restart must not create a false positive
        ):
            labels = f'namespace="{namespace}",name="/{endpoint}",method="GET"'
            requests = "100+100x10" if endpoint != "reset" else "0+100x4 0+100x6"
            series.extend(
                [
                    {"series": f"locust_requests_num_requests{{{labels}}}", "values": requests},
                    {"series": f"locust_requests_num_failures{{{labels}}}", "values": failures},
                ]
            )
    fixture = {
        "rule_files": [str(rule_path)],
        "evaluation_interval": "1m",
        "tests": [
            {
                "interval": "1m",
                "input_series": series,
                "promql_expr_test": [
                    {
                        "expr": 'count(ALERTS{alertname="HighRequestErrorRate",alertstate="firing"}) or vector(0)',
                        "eval_time": at,
                        "exp_samples": [{"labels": "{}", "value": count}],
                    }
                    for at, count in (("3m", 4), ("9m", 0))
                ],
            }
        ],
    }
    test_path = tmp_path / "test.yml"
    test_path.write_text(yaml.safe_dump(fixture))
    subprocess.run([promtool, "test", "rules", str(test_path)], check=True, capture_output=True, text=True, timeout=30)
