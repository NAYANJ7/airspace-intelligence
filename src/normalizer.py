"""Normalization: raw opensky state vectors + response metadata -> clean pandas dataframe.
   Adds two distinct timestamps (see main.py for why both matter):
    -observation_time : OpenSky's own snapshot time( the response level "time" field - same value Phase 1 printed as "API-timestamp" shared by every row in the response)
    -ingested_at: when our pipeline processed this batch,in UTC

   observation_date(the date part of observation_time) is added for parquet partitioning. The ingested_at timestamp is also added to the parquet metadata for provenance.
 """

from datetime import datetime, timezone
from typing import List, Optional 

import pandas as pd 


state_vector_columns = [
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
    "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk",
    "spi", "position_source", "category",
]

Numeric_field=[
    "longitude", "latitude", "baro_altitude", "velocity",
    "true_track", "vertical_rate", "geo_altitude",
]


schema_columns=[
    "icao24", "callsign", "origin_country", "observation_time", "ingested_at",
    "longitude", "latitude", "baro_altitude", "geo_altitude", "velocity",
    "true_track", "vertical_rate", "on_ground", "squawk", "spi",
    "position_source", "category", "observation_date",
]

def normalize_states(states:List[list],
        api_time:Optional[int],
        ingested_at:Optional[datetime]=None,
    )->pd.DataFrame:
    ingested_at=ingested_at or datetime.now(timezone.utc)
    observation_time=(pd.Timestamp(api_time, unit="s",tz="UTC") if api_time else pd.NaT)

    if not states:
        return pd.DataFrame(columns=schema_columns)
    rows=[]
    for state in states:
        padded=list(state)+[None]*(len(state_vector_columns)-len(state))
        rows.append(dict(zip(state_vector_columns,padded)))
    
    df=pd.DataFrame(rows).drop(columns=["sensors"])
    df["callsign"]=df["callsign"].apply(
        lambda c: c.strip() if isinstance(c,str) and c.strip() else None
    )
    for col in Numeric_field:
        df[col]=pd.to_numeric(df[col],errors="coerce")
    df["observation_time"]=observation_time
    df["ingested_at"]=pd.Timestamp(ingested_at)
    df["observation_date"]=(
        df["observation_time"].dt.date.astype(str) if api_time else None
    )

    return df[schema_columns]
