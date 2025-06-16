from fastapi import FastAPI, UploadFile, File
import pandas as pd
from dq_engine import run_all_checks

app = FastAPI()

@app.post("/run-checks/")
async def run_checks(file: UploadFile = File(...)):
    df = pd.read_csv(file.file)
    result = run_all_checks(df)
    return result