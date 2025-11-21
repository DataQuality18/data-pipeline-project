from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
import requests
import logging

router = APIRouter()
logger = logging.getLogger("service_logger")


# =========================
# Common request model
# =========================
class InputMap(BaseModel):
    data: Dict[str, Any]


# =========================
# Helper 1:
#  used by COMPARE endpoint
#  returns dict {name: value}
# =========================
def fetch_metadata_map(branch_url: str) -> Dict[str, Any]:
    try:
        resp = requests.get(branch_url, timeout=60)
        if resp.status_code != 200:
            logger.error(
                f"Failed to fetch metadata from {branch_url}. HTTP {resp.status_code}"
            )
            raise HTTPException(
                status_code=resp.status_code,
                detail=f"Failed to fetch metadata from {branch_url}",
            )

        payload = resp.json()
        metadata_list = payload.get("metadataList", [])

        # convert list -> dict {name: value}
        return {
            item.get("name"): item.get("value")
            for item in metadata_list
            if item.get("name")
        }

    except Exception as e:
        logger.exception(f"Exception during fetch_metadata_map: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =========================
# Helper 2:
#  used by PROCESS endpoint
#  returns full metadataList (list of dicts)
# =========================
def fetch_metadata_list(branch_url: str) -> List[Dict[str, Any]]:
    try:
        resp = requests.get(branch_url, timeout=60)
        if resp.status_code != 200:
            logger.error(
                f"Failed to fetch metadata from {branch_url}. HTTP {resp.status_code}"
            )
            raise HTTPException(
                status_code=resp.status_code,
                detail=f"Failed to fetch metadata from {branch_url}",
            )

        payload = resp.json()
        metadata_list = payload.get("metadataList", [])
        logger.info(
            f"Fetched {len(metadata_list)} metadata records from {branch_url}"
        )
        return metadata_list

    except Exception as e:
        logger.exception(f"Exception during fetch_metadata_list: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# 1) EXISTING ENDPOINT: COMPARE current vs previous branch
# ============================================================
@router.post("/reghub-py-api/rhoo/metadata/compare")
def process_map(input_map: InputMap):
    try:
        current_branch_url = input_map.data.get("currentbranch")
        previous_branch_url = input_map.data.get("previousbranch")

        if not current_branch_url or not previous_branch_url:
            raise HTTPException(
                status_code=400,
                detail="currentbranch and previousbranch are required",
            )

        response1_dict = fetch_metadata_map(current_branch_url)
        response2_dict = fetch_metadata_map(previous_branch_url)

        diff_names = [
            name
            for name in response1_dict
            if name in response2_dict
            and response1_dict[name] != response2_dict[name]
        ]

        names_in_response1_only = set(response1_dict.keys()) - set(
            response2_dict.keys()
        )
        names_in_response2_only = set(response2_dict.keys()) - set(
            response1_dict.keys()
        )

        result = {
            "new_meta": list(names_in_response1_only),
            "purged_meta": list(names_in_response2_only),
            "diff_meta": diff_names,
        }

        logger.info(f"Processed metadata comparison: {result}")
        return {"processed_data": result}

    except Exception as e:
        logger.exception(f"Exception during process_map: {e}")
        return {"error": str(e)}


# ============================================================
# 2) NEW ENDPOINT: PROCESS single branch
#    Pull & filter metadataList by regulation + classname
# ============================================================
@router.post("/reghub-py-api/rhoo/metadata/process")
def process_branch(input_map: InputMap):
    try:
        current_branch_url: Optional[str] = input_map.data.get("currentbranch")
        regulation: Optional[str] = input_map.data.get("regulation")
        classname_filter: Optional[str] = input_map.data.get("classname")

        if not current_branch_url:
            raise HTTPException(status_code=400, detail="currentbranch is required")

        # 1) Fetch all metadata records from branch URL
        metadata_list = fetch_metadata_list(current_branch_url)

        # 2) Filter according to Dilip’s logic
        filtered: List[Dict[str, Any]] = []

        for item in metadata_list:
            # filter by regulation if provided
            if regulation and item.get("regulation") != regulation:
                continue

            value = item.get("value") or {}
            classname = value.get("classname")

            # filter by classname if provided
            if classname_filter and classname != classname_filter:
                continue

            filtered.append(
                {
                    "id": item.get("id"),
                    "name": item.get("name"),
                    "regulation": item.get("regulation"),
                    "classname": classname,
                    "value": value,
                }
            )

        logger.info(
            f"/metadata/process: branch={current_branch_url}, "
            f"regulation={regulation}, classname={classname_filter}, "
            f"matches={len(filtered)}"
        )

        # 3) Return summary + items
        return {
            "status": "success",
            "total_records": len(filtered),
            "names": [rec["name"] for rec in filtered],
            "items": filtered,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error in /metadata/process: {e}")
        raise HTTPException(status_code=500, detail=str(e))
