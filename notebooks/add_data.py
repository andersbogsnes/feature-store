import pathlib
import re

import polars as pl

from feature_store import Client

client = Client()
date_match = re.compile(r"\d{4}-\d{2}-\d{2}")

for data_file in pathlib.Path.cwd().glob("*.csv"):
    date = date_match.search(data_file.name).group()
    shared_cols = [pl.col("id"), pl.col("date")]
    df = pl.scan_csv(data_file).with_columns(
        pl.lit(date).str.strptime(pl.Date, "%Y-%m-%d").alias("date")
    )

    for feature in ["neighbourhood", "room_type", "price"]:
        feature_df = df.select(*shared_cols, pl.col(feature))

        client.upload_feature_data(feature, feature_df.collect().to_pandas())
