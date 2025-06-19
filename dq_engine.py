import pandas as pd
import yaml

from dq_checks.null_checks import check_nulls
from dq_checks.uniqueness_checks import check_duplicates
from dq_checks.range_checks import check_age_range
from dq_checks.pattern_checks import check_pattern
from dq_checks.value_domain_checks import check_allowed_values

# Load rules once
with open("config/rules_config.yaml", "r") as file:
    rules = yaml.safe_load(file)

def run_all_checks(df, rules):
    results = {}

    # Null check
    results["nulls"] = check_nulls(df).to_dict()

    # Duplicate check
    results["duplicates"] = check_duplicates(df).to_dict(orient="records")

    # Range check
    if "age" in rules.get("columns", {}):
        min_age = rules["columns"]["age"].get("min", 0)
        max_age = rules["columns"]["age"].get("max", 999)
        range_result = check_age_range(df, min_age, max_age)
        if isinstance(range_result, pd.DataFrame):
            results["range_violations"] = range_result.to_dict(orient="records")
        else:
            results["range_violations"] = range_result

    # Pattern checks
    pattern_violations = []
    for col, config in rules.get("columns", {}).items():
        if "pattern" in config:
            pattern_result = check_pattern(df, col, config["pattern"])
            if isinstance(pattern_result, pd.DataFrame) and not pattern_result.empty:
                pattern_violations.append({
                    "column": col,
                    "violations": pattern_result.to_dict(orient="records")
                })
    results["pattern_violations"] = pattern_violations

    # Value domain (allowed values) check
    value_violations = []
    for col, config in rules.get("columns", {}).items():
        if "allowed_values" in config:
            domain_result = check_allowed_values(df, col, config["allowed_values"])
            if isinstance(domain_result, pd.DataFrame) and not domain_result.empty:
                value_violations.append({
                    "column": col,
                    "violations": domain_result.to_dict(orient="records")
                })
    results["value_domain_violations"] = value_violations

    return results
