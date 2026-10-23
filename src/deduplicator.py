"""Change detection, keyed by icao24 ,An aircraft is a "new observation" 
if we haven't seen it this run, or if position/altitude/speed/heading/etc.
differ from what we last recorded. An exact repeat is a duplicate and is skipped. State is in-memory only and resets if the service starts"""

import math
from typing import Dict,List,Optional,Tuple
import pandas as pd

tracked_fields=["latitude","longitude","baro_altitude","velocity","true_track","vertical_rate","on_ground"]

def is_missing(value)->bool:
    return value is None or (isinstance(value,float) and math.isnan(value))

def values_equal(a,b)->bool:
    #plain a==b would treat two missing values as "different" (NaN != NaN)
    #in python, which would make e.g. an on-ground aircraft with a consistenly-null altitude look "changed" every single cycle 

    a_missing,b_missing=is_missing(a),is_missing(b)
    if a_missing or b_missing:
        return a_missing and b_missing

    return a==b

class ChangeDetector:
    def __init__(self)->None:
        self.last_state:Dict[str,tuple]={}

    def filter_changed(self,df:pd.DataFrame)-> Tuple[pd.DataFrame,int]:
        if df.empty:
            return df,0
        cols=["icao24"]+tracked_fields

        keep_flags=[]
        for row in df[cols].itertuples(index=False,name=None):
            icao24,current=row[0],row[1:]
            previous:Optional[tuple]=self.last_state.get(icao24)
            changed=previous is None or any(
                not values_equal(p,c) for p,c in zip(previous,current)
            )
            keep_flags.append(changed)
            if changed:
                self.last_state[icao24]=current

        changed_df=df[keep_flags].copy()
        duplicate_skipped = len(df)-len(changed_df)
        return changed_df,duplicate_skipped




