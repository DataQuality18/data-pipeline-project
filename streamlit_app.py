import streamlit as st
import pandas as pd
import yaml
from dq_engine import run_all_checks
import tempfile

st.title("Data Quality Checker")

# File uploader for CSV files
uploaded_file = st.file_uploader("Upload CSV", type=["csv"])

# YAML rules editor (editable in the UI)
rules_yaml = st.text_area(
    "Edit your rules (YAML)",
    value="""
columns:
  age:
    min: 18
    max: 60
    required: true
  name:
    required: true
"""
)

if st.button("Run Checks") and uploaded_file is not None:
    # Load CSV into DataFrame
    df = pd.read_csv(uploaded_file)
    # Parse YAML from text area
    rules = yaml.safe_load(rules_yaml)
    # Run all checks using current rules
    result = run_all_checks(df, rules)
    # Show the summary (dictionary)
    st.write(result)
    # Prepare downloadable Excel file
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
        st.success("Checks completed! Download your Excel report below.")
        st.download_button(
            label="Download Excel Report",
            data=open(tmp.name, "rb").read(),
            file_name="DQ_Report.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
