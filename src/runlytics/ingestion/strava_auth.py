import os
import requests
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

def refresh_access_token():
    """
    Exchanges the refresh token for a new access token.
    Returns the new token string.
    """
    client_id = os.getenv("STRAVA_CLIENT_ID")
    client_secret = os.getenv("STRAVA_CLIENT_SECRET")
    refresh_token = os.getenv("STRAVA_REFRESH_TOKEN")

    if not all([client_id, client_secret, refresh_token]):
        raise ValueError("Missing Strava credentials in .env file.")

    resp = requests.post(
        "https://www.strava.com/oauth/token",
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        },
    )
    
    if resp.status_code != 200:
        raise Exception(f"Failed to refresh token: {resp.text}")
        
    data = resp.json()
    return data["access_token"]

def update_env(new_tokens, env_path=".env"):
    """
    Updates the .env file with the new access and refresh tokens
    to ensure persistence across runs.
    """
    env_file = Path(env_path)
    lines = []
    
    # Read existing file
    if env_file.exists():
        with open(env_file, "r") as f:
            lines = f.readlines()

    # Helper to replace or append lines
    def upsert(key, value):
        keyline = f"{key}={value}\n"
        found = False
        for i, line in enumerate(lines):
            if line.startswith(f"{key}="):
                lines[i] = keyline
                found = True
                break
        if not found:
            lines.append(keyline)

    # Strava response sometimes returns full object or just token string
    # We handle both cases to be safe
    if isinstance(new_tokens, dict):
        upsert("STRAVA_ACCESS_TOKEN", new_tokens.get("access_token"))
        if new_tokens.get("refresh_token"):
            upsert("STRAVA_REFRESH_TOKEN", new_tokens.get("refresh_token"))
    else:
        # If simple string passed
        upsert("STRAVA_ACCESS_TOKEN", new_tokens)

    with open(env_file, "w") as f:
        f.writelines(lines)
    
    print("Updated .env with new Strava tokens.")

if __name__ == "__main__":
    token = refresh_access_token()
    update_env(token)
    print("Test complete. Token refreshed.")