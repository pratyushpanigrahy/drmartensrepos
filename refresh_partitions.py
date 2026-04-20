"""
refresh_partitions.py
Refreshes specific Power BI dataset partitions via the Power BI REST API
using a service principal (client credentials flow).
"""

import json
import os
import sys
import time
from pathlib import Path

import msal
import requests
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
load_dotenv(override=True)

TENANT_ID = os.environ["AZURE_TENANT_ID"]
CLIENT_ID = os.environ["AZURE_CLIENT_ID"]
CLIENT_SECRET = os.environ["AZURE_CLIENT_SECRET"]
WORKSPACE_ID = os.environ["PBI_WORKSPACE_ID"]
DATASET_ID = os.environ["PBI_DATASET_ID"]

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPE = ["https://analysis.windows.net/powerbi/api/.default"]
PBI_BASE = "https://api.powerbi.com/v1.0/myorg"

# How long to poll for refresh completion (seconds)
POLL_TIMEOUT = 600
POLL_INTERVAL = 15


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------
def get_access_token() -> str:
    """Acquire an OAuth2 token via client credentials (service principal)."""
    app = msal.ConfidentialClientApplication(
        CLIENT_ID,
        authority=AUTHORITY,
        client_credential=CLIENT_SECRET,
    )
    result = app.acquire_token_for_client(scopes=SCOPE)
    if "access_token" not in result:
        raise RuntimeError(
            f"Failed to acquire token: {result.get('error_description', result)}"
        )
    return result["access_token"]


# ---------------------------------------------------------------------------
# Power BI REST helpers
# ---------------------------------------------------------------------------
def _headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def trigger_selective_refresh(token: str, partitions: list[dict]) -> str:
    """
    Start an enhanced (selective) refresh for the specified partitions.
    Returns the request ID from the Location header.

    Each entry in `partitions` must have:
        - table (str)  : table name in the dataset
        - partition (str): partition name
    """
    objects = [
        {
            "table": p["table"],
            "partition": p["partition"],
        }
        for p in partitions
    ]

    body = {
        "type": "full",
        "commitMode": "transactional",
        "objects": objects,
    }

    url = f"{PBI_BASE}/groups/{WORKSPACE_ID}/datasets/{DATASET_ID}/refreshes"
    resp = requests.post(url, headers=_headers(token), json=body, timeout=30)

    if resp.status_code not in (200, 202):
        raise RuntimeError(
            f"Refresh trigger failed [{resp.status_code}]: {resp.text}"
        )

    # The API returns 202 with a Location header containing the refresh request ID
    location = resp.headers.get("Location", "")
    request_id = location.rstrip("/").split("/")[-1] if location else None

    if not request_id:
        # Some responses embed the id in the body
        try:
            request_id = resp.json().get("requestId")
        except Exception:
            pass

    if not request_id:
        raise RuntimeError("Could not determine refresh request ID from response.")

    print(f"Refresh triggered. Request ID: {request_id}")
    return request_id


def poll_refresh_status(token: str, request_id: str) -> str:
    """
    Poll until the refresh completes or times out.
    Returns the final status string (e.g. 'Completed', 'Failed').
    """
    url = (
        f"{PBI_BASE}/groups/{WORKSPACE_ID}/datasets/{DATASET_ID}"
        f"/refreshes/{request_id}"
    )
    elapsed = 0

    while elapsed < POLL_TIMEOUT:
        resp = requests.get(url, headers=_headers(token), timeout=30)

        if resp.status_code == 200:
            data = resp.json()
            status = data.get("status", "Unknown")
            print(f"  [{elapsed:>4}s] Status: {status}")

            if status in ("Completed", "Failed", "Cancelled"):
                if status == "Failed":
                    error = data.get("serviceExceptionJson", "")
                    print(f"Refresh failed. Details: {error}", file=sys.stderr)
                return status
        else:
            print(f"  Warning: status check returned {resp.status_code}", file=sys.stderr)

        time.sleep(POLL_INTERVAL)
        elapsed += POLL_INTERVAL

    raise TimeoutError(
        f"Refresh did not complete within {POLL_TIMEOUT} seconds."
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    partitions_file = Path(__file__).parent / "partitions.json"
    if not partitions_file.exists():
        print(
            f"partitions.json not found at {partitions_file}. "
            "Please create it listing tables/partitions to refresh.",
            file=sys.stderr,
        )
        sys.exit(1)

    with partitions_file.open() as f:
        partitions = json.load(f)

    if not partitions:
        print("No partitions listed in partitions.json. Nothing to refresh.")
        sys.exit(0)

    print(f"Partitions to refresh ({len(partitions)}):")
    for p in partitions:
        print(f"  - {p['table']} / {p['partition']}")

    print("\nAcquiring access token...")
    token = get_access_token()

    print("Triggering selective refresh...")
    request_id = trigger_selective_refresh(token, partitions)

    print(f"\nPolling for completion (timeout: {POLL_TIMEOUT}s)...")
    final_status = poll_refresh_status(token, request_id)

    if final_status == "Completed":
        print("\nRefresh completed successfully.")
    else:
        print(f"\nRefresh ended with status: {final_status}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
