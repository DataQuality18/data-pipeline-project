from fastapi import FastAPI, UploadFile, File
from fastapi.responses import FileResponse
import pandas as pd
import yaml
import os
from dq_engine import run_all_checks

app = FastAPI()

@app.post("/run-checks/")
async def run_checks(file: UploadFile = File(...), rules_file: UploadFile = File(None)):
    df = pd.read_csv(file.file)

    # Load rules
    if rules_file:
        rules = yaml.safe_load(rules_file.file)
    else:
        with open("config/rules_config.yaml", "r") as f:
            rules = yaml.safe_load(f)

    results = run_all_checks(df, rules)

    output_file = "DQ_Report.xlsx"
    with pd.ExcelWriter(output_file) as writer:
        # Nulls
        nulls = results["nulls"]
        if isinstance(nulls, dict):
            pd.DataFrame.from_dict(nulls, orient="index", columns=["null_count"]).to_excel(writer, sheet_name="Nulls")
        else:
            pd.DataFrame({"error": [nulls]}).to_excel(writer, sheet_name="Nulls")

        # Duplicates
        duplicates = results["duplicates"]
        if isinstance(duplicates, list):
            pd.DataFrame(duplicates).to_excel(writer, sheet_name="Duplicates", index=False)
        else:
            pd.DataFrame({"error": [duplicates]}).to_excel(writer, sheet_name="Duplicates", index=False)

        # Range Issues
        range_violations = results["range_violations"]
        if isinstance(range_violations, list):
            pd.DataFrame(range_violations).to_excel(writer, sheet_name="Range Issues", index=False)
        else:
            pd.DataFrame({"error": [range_violations]}).to_excel(writer, sheet_name="Range Issues", index=False)

        # Pattern Issues
        pattern_violations = results.get("pattern_violations", [])
        if isinstance(pattern_violations, list):
            pd.DataFrame(pattern_violations).to_excel(writer, sheet_name="Pattern Issues", index=False)
        else:
            pd.DataFrame({"error": [pattern_violations]}).to_excel(writer, sheet_name="Pattern Issues", index=False)

        # Value Domain Issues
        domain_violations = results.get("domain_violations", [])
        if isinstance(domain_violations, list):
            pd.DataFrame(domain_violations).to_excel(writer, sheet_name="Allowed Value Issues", index=False)
        else:
            pd.DataFrame({"error": [domain_violations]}).to_excel(writer, sheet_name="Allowed Value Issues", index=False)

    return FileResponse(
        path=output_file,
        filename=output_file,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )