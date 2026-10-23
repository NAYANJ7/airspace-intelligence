"""Row level validation, invalid rows are logged and excluded from the processed dataset, never silently dropped without a trace 
Rules:
    -icao24 must be present 
    -observation_time must be valid and present for proper partitioning 
    - -90 <= latitude <= 90, when present
    - -180 <= longitude <= 180, when present 
    - velocity => 0, when present 

    baro_altitude had no explicit range rule - the normalizer already guarantees it's numeric-or-null , 
    and an arbitrary altitude ceiling isn't a real data-quality check , so it passes through as-is.
"""

import logging
import pandas as pd 
logger=logging.getLogger(__name__)

def validate_observations(df:pd.DataFrame)->"tuple[pd.DataFrame,pd.DataFrame]":
    if df.empty:
        return df,df
    reasons= pd.Series([None]*len(df),index=df.index,dtype="object")
    def flag(mask:pd.Series,reason:str)->None:
        reasons.loc[mask & reasons.isna()]=reason   

    flag(
        df["icao24"].isna() | (df["icao24"].astype(str).str.strip()==""),
        "missing icao24"
    )
    flag(df["observation_time"].isna(),"missing/invalid observation_time")
    flag(
        df["latitude"].notna() & ((df["latitude"] < -90) | (df["latitude"] > 90)),
        "latitude out of range",
    )
    flag(
        df["longitude"].notna() & ((df["longitude"] < -180) | (df["longitude"] > 180)),
        "longitude out of range",
    )
    flag(df["velocity"].notna() & (df["velocity"] < 0), "negative velocity")

    valid_mask=reasons.isna()
    valid_df=df[valid_mask].copy()
    invalid_df=df[~valid_mask].copy()
    invalid_df["rejection_reason"]=reasons[~valid_mask]

    for _,row in invalid_df.iterrows():
        logger.warning(
            "rejected observation icao24=%s reason=%s",
            row.get("icao24"),row.get("rejection_reason"),
        )


    return valid_df,invalid_df

