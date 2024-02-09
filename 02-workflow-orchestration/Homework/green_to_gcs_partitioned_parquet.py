import pyarrow as pa
import pyarrow.parquet as pq
import os


if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

# This tells pyarrow where our credentials are located
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/dtc-de-course-411702-58cefc200622.json"

bucket_name = 'mage_zoomcamp_juruchi'
project_id = 'dtc-de-course-411702'

table_name = "nyc_taxi_data"

root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data(data, *args, **kwargs):
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    # Read dataframe into a pyarrow table
    table = pa.Table.from_pandas(data)

    # Find GCS object
    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols = ['lpep_pickup_date'],
        filesystem=gcs
    )


