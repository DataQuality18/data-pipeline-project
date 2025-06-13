import pandas as pd
import yaml

from dq_checks.null_checks import check_nulls
from dq_checks.uniqueness_checks import check_duplicates
from dq_checks.range_checks import check_age_range

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

# Range check based on config
if "age" in rules.get("columns", {}):
    min_age = rules["columns"]["age"].get("min", 0)
    max_age = rules["columns"]["age"].get("max", 999)
    print(f"\n📏 Range Check Report (age < {min_age} or > {max_age}):")
    print(check_age_range(df, min_age, max_age))
