import requests
import pandas as pd
import os
from typing import Dict, List, Any


def fetch_lineage_data_post(
    api_url: str,
    payload: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Calls Swagger parse_lineage_from_url POST API (no authentication)
    and returns lineage data.
    """

    try:
        # POST request to Swagger API
        response = requests.post(
            api_url,
            json=payload,                 # Automatically converts payload to JSON
            headers={"Content-Type": "application/json"},
            timeout=30
        )

        # Raise exception for HTTP error status codes
        response.raise_for_status()

        # Convert API response into JSON
        response_json = response.json()

        # Validate response structure
        if "data" not in response_json:
            raise ValueError("API response missing 'data' field")

        return response_json["data"]

    except requests.exceptions.Timeout:
        raise RuntimeError("Swagger API request timed out")

    except requests.exceptions.ConnectionError:
        raise RuntimeError("Unable to connect to Swagger API")

    except requests.exceptions.HTTPError as http_err:
        raise RuntimeError(f"Swagger API HTTP error: {http_err}")

    except ValueError as val_err:
        raise RuntimeError(f"Invalid API response format: {val_err}")

    except Exception as ex:
        raise RuntimeError(f"Unexpected API error: {ex}")


def write_csv_using_pandas(output_path: str, records: List[Dict[str, Any]]) -> None:
    """
    Writes lineage records to CSV using Pandas.
    """

    try:
        if not records:
            raise ValueError("No lineage records available")

        # Convert JSON records to DataFrame
        df = pd.DataFrame(records)

        # Create directory if not exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Write CSV
        df.to_csv(
            output_path,
            index=False,
            encoding="utf-8"
        )

    except pd.errors.EmptyDataError:
        raise RuntimeError("Empty data passed to Pandas")

    except IOError as io_err:
        raise RuntimeError(f"CSV write failed: {io_err}")

    except Exception as ex:
        raise RuntimeError(f"Unexpected CSV error: {ex}")


def main():
    """
    Main execution flow
    """

    # Swagger POST API endpoint
    api_url = "http://localhost:8080/api/parse_lineage_from_url"

    # replace POST request payload (from Swagger definition)
    payload = {
            "url": "string",
            "regulation": "string",
            "metadatakey": "string",
            "class_name": "string",
            "view_names": [
                "string"
            ],
            "headers": {
                "additionalProp1": "string",
                "additionalProp2": "string",
                "additionalProp3": "string"
            }
            }

    # Output CSV path
    output_csv = "output/lineage_report.csv"

    try:
        # Step 1: Call Swagger API
        lineage_data = fetch_lineage_data_post(api_url, payload)

        # Step 2: Write to CSV using Pandas
        write_csv_using_pandas(output_csv, lineage_data)

        print(f"CSV generated successfully at: {output_csv}")

    except RuntimeError as err:
        print(f"PIPELINE FAILED  : {err}")


if __name__ == "__main__":
    main()
