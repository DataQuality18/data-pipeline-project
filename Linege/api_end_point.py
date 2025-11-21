# ------------------------------------------------------------
# NEW ENDPOINT: /reghub-py-api/rhoo/metadata/process
# Dilip wants this endpoint for:
# 1. hitting only currentbranch
# 2. taking regulation = "RHO"
# 3. fetching the metadataList (list of JSON URLs)
# 4. returning ALL the JSON files
# ------------------------------------------------------------

@router.post("/reghub-py-api/rhoo/metadata/process")
def process_branch(input_map: InputMap):
    """
    New API created for Dilip's requirement.
    Accepts: currentbranch + regulation
    Returns: list of JSON files downloaded from metadataList
    """
    try:
        # Extract values from request
        current_branch_url = input_map.data.get("currentbranch")
        regulation = input_map.data.get("regulation")

        logger.info(f"Processing branch URL: {current_branch_url}")
        logger.info(f"Regulation received: {regulation}")

        # Step 1: Fetch all JSON file URLs from the branch
        metadata_list = fetch_metadata(current_branch_url)

        results = []

        # Step 2: Download each JSON file from metadataList
        for file_url in metadata_list:
            try:
                resp = requests.get(file_url)
                
                if resp.status_code == 200:
                    file_content = resp.json()
                    results.append({
                        "file_name": file_url.split("/")[-1],
                        "file_url": file_url,
                        "content": file_content
                    })
                else:
                    results.append({
                        "file_name": file_url.split("/")[-1],
                        "file_url": file_url,
                        "error": f"Status {resp.status_code}"
                    })
            except Exception as inner_err:
                logger.error(f"Error downloading {file_url}: {inner_err}")
                results.append({
                    "file_name": file_url.split("/")[-1],
                    "file_url": file_url,
                    "error": str(inner_err)
                })

        return {
            "status": "success",
            "total_files": len(metadata_list),
            "files": results
        }

    except Exception as e:
        logger.exception(f"Error in /metadata/process: {e}")
        return {"error": str(e)}
