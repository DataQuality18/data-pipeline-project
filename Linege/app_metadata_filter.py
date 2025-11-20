# app_metadata_filter.py

from fastapi import FastAPI, Query
import requests

app = FastAPI()

METADATA_API_URL = ""

@app.get("/filter-metadata")
def filter_metadata(
    
):
    
    # 1) Call existing metadata API
    response = requests.get(METADATA_API_URL)
    response.raise_for_status()   # will raise error if API call fails

    # 2) Expect JSON list (e.g. [ {...}, {...}, ... ])
    data = response.json()

    # if not isinstance(data, list):
    #     return {
    #         "status": "error",
    #         "message": "Metadata API did not return a list",
    #         "raw_type": str(type(data))
    #     }

    # # 3) Loop and filter
    # filtered = []
    # for item in data:
    #     # Make sure it's a dict and has "last_name"
    #     if isinstance(item, dict) and item.get("last_name") == target_last_name:
    #         filtered.append(item)

    # # 4) Return result
    # return {
    #     "status": "success",
    #     "target_last_name": target_last_name,
    #     "count": len(filtered),
    #     "results": filtered
    # }
    print(data)
