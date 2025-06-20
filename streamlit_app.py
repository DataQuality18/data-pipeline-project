import streamlit as st
import pandas as pd
import yaml
from dq_engine import run_all_checks
import tempfile

st.title("Data Quality Checker")

uploaded_file = st.file_uploader("Upload CSV", type=["csv"])
rules_yaml = st.text_area(
    "Edit your rules (YAML)",
    value="""
columns:
  age:
    min: 18
    max: 60
    required: true
  email:
    required: true
    pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.com$"
  department:
    required: true
    allowed:
      - HR
      - Operations
      - Finance
      -IT
  name:
    required: true
"""
)

if st.button("Run Checks") and uploaded_file is not None:
    df = pd.read_csv(uploaded_file)
    rules = yaml.safe_load(rules_yaml)
    result = run_all_checks(df, rules)
    st.write(result)

    # Generate Excel and provide download link
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
        with pd.ExcelWriter(tmp.name) as writer:
            # Nulls
            nulls = result.get("nulls", {})
            if isinstance(nulls, dict):
                pd.DataFrame.from_dict(nulls, orient="index", columns=["null_count"]).to_excel(writer, sheet_name="Nulls")
            else:
                pd.DataFrame({"error": [nulls]}).to_excel(writer, sheet_name="Nulls")
            
            # Duplicates
            duplicates = result.get("duplicates", [])
            if isinstance(duplicates, list):
                pd.DataFrame(duplicates).to_excel(writer, sheet_name="Duplicates", index=False)
            else:
                pd.DataFrame({"error": [duplicates]}).to_excel(writer, sheet_name="Duplicates", index=False)

            # Range Issues
            range_violations = result.get("range_violations", [])
            if isinstance(range_violations, list):
                pd.DataFrame(range_violations).to_excel(writer, sheet_name="Range Issues", index=False)
            else:
                pd.DataFrame({"error": [range_violations]}).to_excel(writer, sheet_name="Range Issues", index=False)

            # Pattern Mismatches
            pattern_violations = result.get("pattern_violations", [])
            if isinstance(pattern_violations, list):
                pd.DataFrame(pattern_violations).to_excel(writer, sheet_name="Pattern Issues", index=False)
            else:
                pd.DataFrame({"error": [pattern_violations]}).to_excel(writer, sheet_name="Pattern Issues", index=False)

            # Allowed Value Mismatches
            allowed_violations = result.get("value_domain_violations", [])
            if isinstance(allowed_violations, list):
                pd.DataFrame(allowed_violations).to_excel(writer, sheet_name="Allowed Values", index=False)
            else:
                pd.DataFrame({"error": [allowed_violations]}).to_excel(writer, sheet_name="Allowed Values", index=False)

        st.success("Checks completed! Download your Excel report below.")
        st.download_button(
            label="Download Excel Report",
            data=open(tmp.name, "rb").read(),
            file_name="DQ_Report.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
