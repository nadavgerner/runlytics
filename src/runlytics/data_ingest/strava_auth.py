import os
import requests
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

def refresh_access_token():
    resp = requests.post(
        "https://www.strava.com/oauth/token",
        data={
            "client_id": os.getenv("STRAVA_CLIENT_ID"),
            "client_secret": os.getenv("STRAVA_CLIENT_SECRET"),
            "grant_type": "refresh_token",
            "refresh_token": os.getenv("STRAVA_REFRESH_TOKEN"),
        },
    )
    resp.raise_for_status()
    data = resp.json()
    return data

def update_env(new_tokens, env_path=".env"):
    """Update .env with new access + refresh tokens"""
    env_file = Path(env_path)
    lines = []
    if env_file.exists():
        with open(env_file, "r") as f:
            lines = f.readlines()

    # replace or add lines
    def upsert(key, value):
        keyline = f"{key}={value}\n"
        for i, line in enumerate(lines):
            if line.startswith(f"{key}="):
                lines[i] = keyline
                return
        lines.append(keyline)

    upsert("STRAVA_ACCESS_TOKEN", new_tokens["access_token"])
    upsert("STRAVA_REFRESH_TOKEN", new_tokens["refresh_token"])

    with open(env_file, "w") as f:
        f.writelines(lines)

    print(f"Updated {env_path} with new tokens")

if __name__ == "__main__":
    tokens = refresh_access_token()
    update_env(tokens)
    print("New access token:", tokens["access_token"][:20], "...")
