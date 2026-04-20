from dotenv import load_dotenv
load_dotenv(override=True)
import os, msal, requests

token = msal.ConfidentialClientApplication(
    os.environ["AZURE_CLIENT_ID"],
    authority=f"https://login.microsoftonline.com/{os.environ['AZURE_TENANT_ID']}",
    client_credential=os.environ["AZURE_CLIENT_SECRET"],
).acquire_token_for_client(["https://analysis.windows.net/powerbi/api/.default"])["access_token"]

headers = {"Authorization": f"Bearer {token}"}

# List all workspaces the service principal has access to
print("=== Workspaces accessible by this service principal ===")
r = requests.get("https://api.powerbi.com/v1.0/myorg/groups", headers=headers, timeout=30)
print(f"Status: {r.status_code}")
if r.status_code == 200:
    for g in r.json().get("value", []):
        print(f"  {g['id']}  {g['name']}")
else:
    print(r.text)

# Try the target workspace
ws = os.environ["PBI_WORKSPACE_ID"]
print(f"\n=== Datasets in target workspace {ws} ===")
r2 = requests.get(f"https://api.powerbi.com/v1.0/myorg/groups/{ws}/datasets",
                   headers=headers, timeout=30)
print(f"Status: {r2.status_code}")
if r2.status_code == 200:
    for d in r2.json().get("value", []):
        print(f"  {d['id']}  {d['name']}")
else:
    print(r2.text)
