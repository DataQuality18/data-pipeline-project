from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)


def test_sql_lineage_record_count():
    payload = {
        "url": "http://rhoo-gateway-vip-uat.nam.nsroot.net/reghub-api/rhoo/metadata-service/metadata/all/branch/1.25.12.1-uat",
        "regulation": "rhoo",
        "view_names": [
            "APP_REGHUB_RHOO_OTC_TSR_ESMA_DATA_MERGED_WITH_ESMA_TS_FACT_DATA"
        ]
    }

    response = client.post(
        "/reghub-py-api/rhoo/json_parser/parse_lineage_from_url",
        json=payload
    )

    # Step 1: API should work
    assert response.status_code == 200, f"API failed: {response.text}"

    data = response.json()

    # Step 2: Extract lineage output
    lineage_data = data.get("lineage_data", [])

    current_count = len(lineage_data)

    # IMPORTANT: Set your baseline here
    expected_baseline = 400000   # example → replace with your actual count

    min_allowed = int(expected_baseline * 0.9)

    # Step 3: Validate drop
    assert current_count >= min_allowed, (
        f"Record drop detected! Expected >= {min_allowed}, got {current_count}"
    )