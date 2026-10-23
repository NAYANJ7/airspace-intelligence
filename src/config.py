'''Configuration file for the application.'''

import os 
import sys
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

#  ---opensky oauth2 credentials (in local .env file)---
opensky_clientid=os.getenv("OPENSKY_CLIENT_ID")
opensky_clientsecret=os.getenv("OPENSKY_CLIENT_SECRET")
# --- Added for containerized ingestion / credit tracking ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DAILY_CREDIT_BUDGET = int(os.getenv("DAILY_CREDIT_BUDGET", "4000"))
CREDIT_HEARTBEAT_SECONDS = int(os.getenv("CREDIT_HEARTBEAT_SECONDS", "3"))

# --- Fixed OpenSky endpoints ---
OPENSKY_TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/opensky-network/"
    "protocol/openid-connect/token"
)
OPENSKY_STATES_URL = "https://opensky-network.org/api/states/all"

request_timeout= 15 #seconds
token_refresh_buffer= 30

POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))
PROCESSED_DATA_DIR = os.getenv("PROCESSED_DATA_DIR", "data/processed")

def validate_config()->None:
    missing=[]
    if not opensky_clientid:
        missing.append("OPENSKY_CLIENT_ID")
    if not opensky_clientsecret:
        missing.append("opensky_clientsecret")

    if missing:
        print(
            "ERROR: Missing required environment variable(s): "
            + ", ".join(missing)
            + "\nCreate a '.env' file in the project root (see .env.example) "
            "and set them there.",
            file=sys.stderr,
        )
        sys.exit(1)

