from os import environ as os_environ
from time import time

from dlt import pipeline as dlt_pipeline

from rickandmorty import get_characters

# duckdb
rickandmorty_pipeline = dlt_pipeline(
    pipeline_name="rickandmorty_pipeline",
    destination="duckdb",
    dataset_name="rickandmorty",
)
start = time()
pipeline_info = rickandmorty_pipeline.run(
    get_characters(), table_name="characters", write_disposition="replace"
)
print(f"Pipeline took {time() - start} seconds")
print(pipeline_info)


# filesystem parquet
os_environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = (
    "file:///Users/flo/Developer/tools/dlt_learn/data"
)
rickandmorty_pipeline = dlt_pipeline(
    pipeline_name="rickandmorty_pipeline",
    destination="filesystem",
    dataset_name="rickandmorty",
)
pipeline_info = rickandmorty_pipeline.run(
    get_characters(),
    table_name="characters",
    loader_file_format="parquet",
)
print(pipeline_info)
