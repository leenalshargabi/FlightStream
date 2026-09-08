import os

import requests
from dotenv import load_dotenv


# ============================================================
# Configuration
# ============================================================

load_dotenv()

client_id = os.getenv("OPENSKY_CLIENT_ID")
client_secret = os.getenv("OPENSKY_CLIENT_SECRET")

TOKEN_URL = (
    "https://auth.opensky-network.org/"
    "auth/realms/opensky-network/protocol/openid-connect/token"
)

API_URL = "https://opensky-network.org/api/states/all"


# ============================================================
# Authentication
# ============================================================

print("Testing OpenSky authentication...")

token_response = requests.post(
    TOKEN_URL,
    data={
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    },
    timeout=30,
)

print(f"Authentication HTTP status: {token_response.status_code}")

token_response.raise_for_status()

token_data = token_response.json()

access_token = token_data["access_token"]

print("Authentication successful! ✅")
print(f"Token type: {token_data.get('token_type')}")
print(f"Expires in: {token_data.get('expires_in')} seconds")


# ============================================================
# Flight Data Request
# ============================================================

print("\nRequesting live aircraft data from OpenSky...")

response = requests.get(
    API_URL,
    headers={
        "Authorization": f"Bearer {access_token}"
    },
    timeout=30,
)

print(f"Flight data HTTP status: {response.status_code}")

response.raise_for_status()

data = response.json()


# ============================================================
# Display Results
# ============================================================

timestamp = data.get("time")
states = data.get("states") or []

print("\n========================================")
print("       FLIGHTSTREAM - OPENSKY TEST")
print("========================================")
print(f"API timestamp      : {timestamp}")
print(f"Aircraft received  : {len(states)}")
print("========================================")

if states:
    print("\nFirst aircraft record:")
    print(states[0])
else:
    print("\nNo aircraft states were returned.")