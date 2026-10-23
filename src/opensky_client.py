'''Minimal Oauth client for opensky network rest api
Handles:
-client credentials token acquisition with auto refresh feature
-a single call to /api/states/all endpoint with automatic token refresh if needed
-conversion of the raw state vector array 

* deliberately does not retry on failure and not poll continuosly
to keep a 4000 credits/day budget

'''

import time
from typing import Any, Optional , Dict, List
import pandas as pd 
import requests
from . import config

class OpenSkyError(Exception):
    """Base class for all OpenSky client errors."""


class OpenSkyAuthError(OpenSkyError):
    """Raised when OAuth2 authentication fails."""


class OpenSkyRateLimitError(OpenSkyError):
    """Raised on HTTP 429 - do not retry, credits would be wasted."""


class OpenSkyAPIError(OpenSkyError):
    """Raised for any other non-2xx response from the OpenSky API."""


class OpenSkyNetworkError(OpenSkyError):
    """Raised for timeouts, connection errors, or invalid JSON."""

# Order of fields in each OpenSky "state vector" array, per the official docs:
# https://openskynetwork.github.io/opensky-api/rest.html#response
# "category" (index 17) is only populated when the request includes extended=1,
# which this client always sends.

state_vector_fields=[
    "icao24",
    "callsign",
    "origin_country",
    "time_position",
    "last_contact",
    "longitude",
    "latitude",
    "baro_altitude",
    "on_ground",
    "velocity",
    "true_track",
    "vertical_rate",
    "sensors",
    "geo_altitude",
    "squawk",
    "spi",
    "position_source",
    "category",
]

# final dataframe columns 
# The columns in the final DataFrame will match the state_vector_fields list.
# not aircraft telemetry , so its dropped rather than exposed 

output_columns=[c for c in state_vector_fields if c not in ["sensors"]]

class OpenSkyClient:
    '''Small wrapper for Opensky's oauth2 token endpoint and states/all endpoint'''
    def __init__(self,client_id:str,client_secret:str,timeout: int=config.request_timeout,):
        if not client_id or not client_secret:
            raise OpenSkyAuthError("Missing OpenSky client ID or secret.")
        self.client_id=client_id
        self.client_secret=client_secret
        self.timeout=timeout
        self.access_token:Optional[str]=None
        self._token_expires_at:float=0.0 # unix timestamp

    # Authentication

    def _fetch_new_token(self)->None:
        '''request a fresh access token using the client credentials grant'''
        try:
            response=requests.post(
                config.OPENSKY_TOKEN_URL,
                data={
                    "grant_type":"client_credentials",
                    "client_id":self.client_id,
                    "client_secret":self.client_secret,
                    },
                 timeout=self.timeout,    
        )
        except requests.exceptions.Timeout:
            raise OpenSkyNetworkError("Timeout while requesting access token.")
        except requests.exception.ConnectionError:
            raise OpenSkyNetworkError("could not connect to OpenSky auth server.")

        if response.status_code in (401,403):
            raise OpenSkyAuthError("OpenSky rejected the client credentials"f"HTTP {response.status_code} check your .env file.")

        if response.status_code!=200:
            raise OpenSkyAuthError(f"unexpected response from OpenSky auth server: HTTP {response.status_code}")

        try:
            payload=response.json()


        except ValueError:
            raise OpenSkyNetworkError("Auth server returned invalid JSON")

        token=payload.get("access_token")
        expires_in=payload.get("expires_in",0)

        if not token:
            raise OpenSkyAuthError(" auth server did not include an access token ")

        self.access_token=token
        self._token_expires_at=(time.time()+expires_in-config.token_refresh_buffer)

    def get_valid_token(self)->str:
        ''''return a valid access token, refreshing if needed'''
        if self.access_token is None or time.time()>=self._token_expires_at:
            self._fetch_new_token()
        return self.access_token

    #states endpoint
    def get_states(
            self,
            lamin:Optional[float]=None,
            lomin:Optional[float]=None,
            lamax:Optional[float]=None,
            lomax:Optional[float]=None,
    )->Dict[str,Any]:
        '''make a single request to /api/states/all 
        returns {"time": int|None, "states": list, "rate_limit_remaining": str|None}
        Raises OpenSkyRateLimitError on HTTP 429, OpenSkyAPIError on other non-2xx responses, and OpenSkyNetworkError on timeouts or connection errors.
        Never retries automatically  
        '''
        token=self.get_valid_token()

        Params: Dict[str,Any]={"extended":1} #include the category field
        bbox={"lamin":lamin,"lomin":lomin,"lamax":lamax,"lomax":lomax}
        provided={k:v for k,v in bbox.items() if v is not None}
        if provided and len(provided)!=4:
            raise ValueError("To filter by bounding box, you must provide"
                            "all four coordinates: lamin, lomin, lamax, lomax.")
        Params.update(provided)
        headers={"AUthorization":f"Bearer {token}"}
        try:
            response=requests.get(
                config.OPENSKY_STATES_URL,
                params=Params,
                headers=headers,
                timeout=self.timeout,
            )
        except requests.exceptions.Timeout:
            raise OpenSkyNetworkError("Timed out while requesting states data.")
        except requests.exceptions.ConnectionError:
            raise OpenSkyNetworkError("could not connect to OpenSky API check your internet connection.")

        rate_limit_remaining=response.headers.get("X-RateLimit-Remaining")

        if response.status_code==401:
            raise OpenSkyAuthError("OpenSky rejected the access token. "
                                   "This should not happen if the client is working correctly.")
        if response.status_code==403:
            raise OpenSkyAuthError("Access forbidden (HTTP 403). Check your client credentials.")
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After", "unknown")
            raise OpenSkyRateLimitError(
                "OpenSky rate limit reached (HTTP 429). "
                f"Retry-After: {retry_after}. Not retrying automatically - "
                "wait before running the program again."
            )
        if response.status_code in (500, 502, 503, 504):
            raise OpenSkyAPIError(
                f"OpenSky server error (HTTP {response.status_code}). "
                "Not retrying automatically - try again later."
            )
        if response.status_code != 200:
            raise OpenSkyAPIError(
                f"Unexpected response from OpenSky API: HTTP {response.status_code}. "
                "Not retrying automatically - check your request parameters."
            )
        try:
            payload=response.json()
        except ValueError:
            raise OpenSkyNetworkError("OpenSky API returned invalid JSON.")
        return {
            "time":payload.get("time"),
            "states":payload.get("states",[]),
            "rate_limit_remaining":rate_limit_remaining
        }

    # Parsing

    @staticmethod
    def states_to_dataframe(states:List[list])->pd.DataFrame:
        ''' Convert raw OpenSky state vector arrays into a clean pandas DataFrame.

        - Missing values are left as null (None/NaT), never invented.
        - Callsigns are stripped of padding whitespace; empty strings become null.
        - time_position / last_contact are converted to UTC datetimes.'''

        if not states:
            return pd.DataFrame(columns=output_columns)
        rows=[]
        for state in states:
            #pad short arrays (eg. if a field set changes upstream) with None
            #instead of guessing values
            padded=list(state)+[None]*(len(state_vector_fields)-len(state))
            record=dict(zip(state_vector_fields,padded))
            record.pop("sensors",None) #drop telemetry data, not needed
            rows.append(record)

        df=pd.DataFrame(rows, columns=output_columns)
        # Clean callsigns: strip whitespace, convert empty strings to None
        df["callsign"] = df["callsign"].apply(
            lambda x: x.strip() if isinstance(x, str) and x.strip() else None
        )
        for ts_col in ("time_position","last_contact"):
            df[ts_col]=pd.to_datetime(df[ts_col],unit="s",utc=True,errors="coerce")
        return df
