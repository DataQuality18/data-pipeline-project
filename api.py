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

    # Generate Excel report
    output_file = "DQ_Report.xlsx"
    with pd.ExcelWriter(output_file) as writer:
        pd.DataFrame.from_dict(results["nulls"], orient="index", columns=["null_count"]).to_excel(writer, sheet_name="Nulls")
        pd.DataFrame(results["duplicates"]).to_excel(writer, sheet_name="Duplicates", index=False)
        pd.DataFrame(results["range_violations"]).to_excel(writer, sheet_name="Range Issues", index=False)

    return FileResponse(
        path=output_file,
        filename=output_file,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
