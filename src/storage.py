"""Parquet persistence. One file per poll cycle(when there's anything new to save), written to <base/dir>/observation_date=<date>/part-<uuid>.parquet.
 All rows in a single cycle share one observatio_time,so one write per
 cycle needs no grouping. Kept simple on purpose: reliability over optimization for now."""

import uuid 
from pathlib import Path
from typing import Optional
import pandas as pd

def write_processed(df:pd.DataFrame,base_dir:str)->Optional[Path]:
    """Write df as one parquet file. Returns the file path, Or None if df is empty"""
    date_value=df["observation_date"].iloc[0]
    partition_dir=Path(base_dir) / f"Observation_date={date_value}"
    partition_dir.mkdir(parents=True,exist_ok=True)
    file_path=partition_dir/f"part-{uuid.uuid4().hex}.parquet"
    df.to_parquet(file_path,index=False,engine="pyarrow")
    return file_path

    