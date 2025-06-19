from fastapi import FastAPI, UploadFile, File
from fastapi.responses import FileResponse
import pandas as pd
import os
from dq_engine import run_all_checks

app = FastAPI()

@app.post("/run-checks/")
async def run_checks(file: UploadFile = File(...)):
    df = pd.read_csv(file.file)
    results = run_all_checks(df)

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

    return FileResponse(
        path=output_file,
        filename=output_file,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
