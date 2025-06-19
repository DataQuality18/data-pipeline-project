import pandas as pd
import yaml

from dq_checks.null_checks import check_nulls
from dq_checks.uniqueness_checks import check_duplicates
from dq_checks.range_checks import check_age_range
from dq_checks.pattern_checks import check_pattern
from dq_checks.value_domain_checks import check_allowed_values

# Load rules from YAML
with open("config/rules_config.yaml", "r") as file:
    rules = yaml.safe_load(file)

# Load sample data
df = pd.read_csv("data/sample_data.csv")

# Run checks
print("\n✅ Null Check Report:")
print(check_nulls(df))

print("\n🔁 Duplicate Check Report:")
print(check_duplicates(df))

# Range check
if "age" in rules.get("columns", {}):
    min_age = rules["columns"]["age"].get("min", 0)
    max_age = rules["columns"]["age"].get("max", 999)
    print(f"\n📏 Range Check Report (age < {min_age} or > {max_age}):")
    print(check_age_range(df, min_age, max_age))

# Pattern checks
for col, cfg in rules.get("columns", {}).items():
    if "pattern" in cfg:
        pattern = cfg["pattern"]
        print(f"\n🔤 Pattern Check Report for {col}:")
        print(check_pattern(df, col, pattern))

# Value domain checks
for col, cfg in rules.get("columns", {}).items():
    if "allowed" in cfg:
        allowed_values = cfg["allowed"]
        print(f"\n🔢 Allowed Value Check for {col}:")
        print(check_allowed_values(df, col, allowed_values))
