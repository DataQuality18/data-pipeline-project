from dq_checks.null_checks import check_nulls
from dq_checks.uniqueness_checks import check_duplicates
from dq_checks.range_checks import check_age_range
import yaml

# Load rules once
with open("config/rules_config.yaml", "r") as file:
    rules = yaml.safe_load(file)

def run_all_checks(df):
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
        results["range_violations"] = range_result.to_dict(orient="records")

    return results
