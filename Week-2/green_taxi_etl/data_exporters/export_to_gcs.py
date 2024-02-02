import pyarrow as pa
import pyarrow.parquet as pq
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


os.environ['GOOGLE_APPLICATION_CREDENTIALS']="/home/src/my-creds.json"
bucket_name = 'evident-time-410307-example-bucket'
project_id = 'evident-time-410307'

table_name = 'green_taxi_data_final_quater'

root_path = f"{bucket_name}/{table_name}"

@data_exporter
def export_data(data, *args, **kwargs):
    table = pa.Table.from_pandas(data)
    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['lpep_pickup_date'],
        filesystem=gcs
    )