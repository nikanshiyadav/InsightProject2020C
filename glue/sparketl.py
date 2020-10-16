import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import col
from pyspark.sql.functions import to_date, split


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#load data from glue catalog
source = glueContext.create_dynamic_frame.from_catalog(database = "chicago-crime", table_name = "raw", transformation_ctx = "datasource0")

dynamic_frame = ApplyMapping.apply(frame = source, mappings = [("row_id", "long", "row_id", "long"), ("key", "long", "key", "long"), 
("case_number", "string", "case_number", "string"), ("date", "string", "date", "string"), ("block", "string", "block", "string"), ("iucr", "int", "iucr", "int"), 
("primary_type", "string", "primary_type", "string"), ("description", "string", "description", "string"), ("location_description", "string", "location_description", "string"), 
("arrest", "boolean", "arrest", "boolean"), ("domestic", "boolean", "domestic", "boolean"), ("beat", "long", "beat", "long"), ("district", "int", "district", "int"), 
("ward", "int", "ward", "int"), ("community_area", "int", "community_area", "int"), ("fbi_code", "string", "fbi_code", "string"), ("x_coordinate", "double", "x_coordinate", "double"), 
("y_coordinate", "double", "y_coordinate", "double"), ("year", "int", "year", "int"), ("updated_on", "string", "updated_on", "timestamp"), ("latitude", "double", "latitude", "double"), 
("longitude", "double", "longitude", "double"), ("location", "string", "location", "string")])

#convert to spark dataframe
df = dynamic_frame.toDF()
df.show()

# convert date columns to day & month
df = df.withColumn("date_added", to_date(split(df["date"], " ").getItem(0).cast("string"), 'MM/dd/yyyy')) \
    .withColumn("month", split(col("date_added"), "-").getItem(1)) \
    .withColumn("day", split(col("date_added"), "-").getItem(2)) \
    .orderBy('date_added')
print("Dataframe sorted")

partitioned_dataframe = df.repartition("day")

# Convert back to dynamic frame
dynamic_frame2 = DynamicFrame.fromDF(partitioned_dataframe, glue_context, "dynamic_frame_write",
                                          transformation_ctx = "applymapping1")
#resolve discrepency in columns data types 
resolvechoice = ResolveChoice.apply(frame = dynamic_frame2, choice = "make_struct", transformation_ctx = "resolvechoice2")

#transformation function
def ReplaceValue(rec):
    for field in rec:
        if rec[field] == '999' or rec[field] == 999.0 or rec[field] == 'nan' or rec[field] == 0 or rec[field] == '0':
            rec[field] = None
    rec["category_a"] = False
    rec["category_b"] = False
    if rec["primary_type"] in ['NARCOTICS', 'PROSTITUTION', 'WEAPONS VIOLATION', 'HOMICIDE']:
        rec["category_a"] = True
    else:
        rec["category_b"] = True
    return rec

#drop null fields and map transformation function
dropnullfields = DropNullFields.apply(frame = resolvechoice, transformation_ctx = "dropnullfields3").map(f = ReplaceValue)

datasink = glueContext.write_dynamic_frame.from_options(frame = dropnullfields, connection_type = "s3", connection_options = {"path": "s3://nikanshi.etlsansserversdemo/parquet/crime_stats/"}, format = "parquet", transformation_ctx = "datasink4")
job.commit()