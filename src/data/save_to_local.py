import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from src.utils.config import TICKER_OVERVIEW_PATH


def save_ticker_overviews_as_parquet(df: pd.DataFrame, subdir: str = None) -> None:
    table = pa.Table.from_pandas(df, preserve_index=False)
    ds.write_dataset(
        table,
        base_dir=TICKER_OVERVIEW_PATH / subdir,
        format="parquet",
        partitioning=["snapshot_month"],
        existing_data_behavior="overwrite_or_ignore",
    )
