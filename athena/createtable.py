import boto3
import pandas as pd
import numpy as np
from IPython.display import display, Markdown
import seaborn as sns
%matplotlib inline
import matplotlib.pyplot as plt
athena = boto3.client('athena')

def athena_query(query, bucket, folder):
    output = 's3://' + bucket + '/' + folder + '/'
    response = athena.start_query_execution(QueryString=query, 
                                        ResultConfiguration={'OutputLocation': output})
    qid = response['QueryExecutionId']
    response = athena.get_query_execution(QueryExecutionId=qid)
    state = response['QueryExecution']['Status']['State']
    while state == 'RUNNING':
        response = athena.get_query_execution(QueryExecutionId=qid)
        state = response['QueryExecution']['Status']['State']
    key = folder + '/' + qid + '.csv'
    data_source = {'Bucket': bucket, 'Key': key}
    url = s3.generate_presigned_url(ClientMethod = 'get_object', Params = data_source)
    data = pd.read_csv(url)
    return data

bucket = 'open-data-analytics-taxi-trips'
folder = 'queries'
query = 'SELECT * FROM "taxicatalog"."many_trips_well_formed" TABLESAMPLE BERNOULLI(100) LIMIT 1000;'

df = athena_query(query, bucket, folder)
df.head()

"""create crime_stats table from transformed data"""
CREATE TABLE 
IF NOT EXISTS "crime_stats"."chicago_crime" 
WITH (
    external_location = 's3://bucke.name/folder/',
    format = 'Parquet',
    field_delimiter = ','
)
AS SELECT vendorid AS vendor,
         passenger_count AS passengers,
         trip_distance AS distance,
         ratecodeid AS rate,
         pulocationid AS pick_location,
         dolocationid AS drop_location,
         payment_type AS payment_type,
         fare_amount AS fare,
         extra AS extra_fare,
         mta_tax AS tax,
         tip_amount AS tip,
         tolls_amount AS toll,
         improvement_surcharge AS surcharge,
         total_amount AS total_fare,
         tpep_pickup_datetime AS pick_when,
         tpep_dropoff_datetime AS drop_when
FROM "taxicatalog"."many_trips";