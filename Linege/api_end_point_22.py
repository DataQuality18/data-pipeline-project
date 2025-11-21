from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import requests
import logging

router = APIRouter()
logger = logging.getLogger("service_logger")


# ----------------------------
# ✅ Request Models (Swagger will show correct fields)
# ----------------------------
class CompareRequest(BaseModel):
    currentbranch: str
    previousbranch: str


class ProcessRequest(BaseModel):
    currentbranch: str
    regulation: Optional[str] = None


# ----------------------------
# ✅ Fetch raw metadata list (USED BY process endpoint)
# ----------------------------
def fetch_metadata_list(branch_url: str) -> List[Dict[str, Any]]:
    try:
        resp = requests.get(branch_url, timeout=30, verify=False)
        if resp.status_code != 200:
            raise HTTPException(
                status_code=resp.status_code,
                detail=f"Failed to fetch metadata from {branch_url}"
            )
        return resp.json().get("metadataList", [])
    except Exception as e:
        logger.exception(f"Exception during fetch_metadata_list: {e}")
        raise


# ----------------------------
# ✅ Fetch dict (USED BY compare endpoint)
# ----------------------------
def fetch_metadata_dict(branch_url: str) -> Dict[str, Any]:
    metadata_list = fetch_metadata_list(branch_url)
    return {item.get("name"): item.get("value") for item in metadata_list if item.get("name")}


# ----------------------------
# ✅ Compare Branches Endpoint
# ----------------------------
@router.post("/reghub-py-api/rhoo/metadata/compare")
def process_map(req: CompareRequest):
    try:
        logger.info(f"Comparing branches: {req.currentbranch} vs {req.previousbranch}")

        response1_dict = fetch_metadata_dict(req.currentbranch)
        response2_dict = fetch_metadata_dict(req.previousbranch)

        diff_names = [
            name for name in response1_dict
            if name in response2_dict and response1_dict[name] != response2_dict[name]
        ]

        names_in_response1_only = set(response1_dict.keys()) - set(response2_dict.keys())
        names_in_response2_only = set(response2_dict.keys()) - set(response1_dict.keys())

        result = {
            "new_meta": sorted(list(names_in_response1_only)),
            "purged_meta": sorted(list(names_in_response2_only)),
            "diff_meta": sorted(diff_names)
        }

        return {"processed_data": result}

    except Exception as e:
        logger.exception(f"Error in compare: {e}")
        return {"error": str(e)}


# ----------------------------
# ✅ Process Branch Endpoint
# ----------------------------
@router.post("/reghub-py-api/rhoo/metadata/process")
def process_branch(req: ProcessRequest):
    try:
        logger.info(f"Processing branch URL: {req.currentbranch}")
        logger.info(f"Regulation filter: {req.regulation}")

        metadata_items = fetch_metadata_list(req.currentbranch)

        # If regulation provided, filter
        if req.regulation:
            metadata_items = [
                item for item in metadata_items
                if item.get("regulation") == req.regulation
            ]

        # Return metadata directly (NO extra download)
        results = [
            {
                "name": item.get("name"),
                "regulation": item.get("regulation"),
                "stream": item.get("stream"),
                "metadataType": item.get("metadataType"),
                "value": item.get("value")
            }
            for item in metadata_items
        ]

        return {
            "status": "success",
            "total_files": len(results),
            "files": results
        }

    except Exception as e:
        logger.exception(f"Error in process_branch: {e}")
        return {"error": str(e)}
