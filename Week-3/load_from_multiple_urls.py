import io
import pandas as pd
import requests
import pyarrow.parquet as pq
import pyarrow as pa
import os


if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/src/my-creds.json'


@data_loader
def load_data_from_api(*args, **kwargs):
    BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-{}.parquet"
    
    bucket_name = 'evident-time-410307-example-bucket'
    project_id ='evident-time-410307'
    folder_name = 'nyc_green_taxi_data_2022'
    root_path = f"{bucket_name}/{folder_name}"
    gcs = pa.fs.GcsFileSystem()
    result = pd.DataFrame()
    for j in range(12):
        month = '0' + str(j+1)
        month = month[-2:]
        url = BASE_URL.format(month)
        # file_name = "green_taxi_data_{i}.parquet"
        response = requests.get(url)
        # data = io.BytesIO(response.content)

        with open('temp.parquet', 'wb') as file:
            file.write(response.content)
        df = pq.read_table('temp.parquet').to_pandas()
        result = pd.concat([result,df],axis=0)

    table = pa.Table.from_pandas(result)
    # pq.write_table(table, 'nyc_green_taxi_data_2022.parquet')

    pq.write_to_dataset(
        table,
        root_path=root_path,
        filesystem=gcs,
        use_deprecated_int96_timestamps=True
    )
    print("Successfully uploaded the data")

# @test
# def test_output(output, *args) -> None:
#     """
#     Template code for testing the output of the block.
#     """
#     assert output is not None, 'The output is undefined'