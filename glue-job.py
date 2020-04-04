from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
import boto3
import botocore
from datetime import date
from datetime import datetime
import sys
import os
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
import pyspark.sql.functions as sf
from pyspark.sql.functions import year, month, dayofmonth, lpad


def get_table_list(s3_client, bucket, prefix):
    """List all subfolders/tables from s3://bucket/prefix
    
    Arguments:
        s3_client {string} -- Boto3 S3 client
        bucket {string} -- S3 Bucket name
        prefix {string} -- S3 key/prefix
    """

    subfolder_lst = []
    paginator = s3_client.get_paginator('list_objects')
    result = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/')
    for obj in result.search('CommonPrefixes'):
        subfolder_lst.append(obj.get('Prefix'))

    return subfolder_lst


def process_partition(glueContext, src_bucket, table_name, prefix, partition):
    """Transform files in s3://src_bucket/table_path/partition_date to parquet format
        and moves from /raw to /enriched subfolder
    
    Arguments:
        src_bucket {string} -- Source bucket name
        table_name {string} -- Table name
        prefix {string} -- S3 prefix, e.g. raw/data_source/job_name/
        partition_date {string} -- Partition date key to be processed
    """

    partition_date = datetime.strptime(partition, "%Y-%m-%d, %H:%M:%S")

    # Build destination S3 key
    dest_prefix = prefix.replace('raw','enriched')
    dest_key = os.path.join('s3://',src_bucket, dest_prefix, table_name)

    # Build DB name and DB table name from prefix and table name
    # Extract Data source name from raw/data_source/job_name/
    db_name = os.path.join(*prefix.split("/")[1:2]) + '_raw'

    # Glue tables cannot have '-', automatically replaced by '_' and all lower case
    db_table_name = db_name + '_' + table_name.lower().replace('-','_')
    
    # Push down predicate are used to target a specific partition
    pushdownpredicate = "(year == " + str(partition_date.year) + " and month == " + str(partition_date.month) + " and day == " + str(partition_date.day) + ")"

    # Read raw data
    gdf = glueContext.create_dynamic_frame.from_catalog(database = db_name, table_name = db_table_name, transformation_ctx = db_table_name + partition, push_down_predicate = pushdownpredicate)
    #gdf = glueContext.create_dynamic_frame.from_options("s3", {"paths": [src_key], "partitionKeys": ["year","month","day", "hour"]}, format="csv", format_options={"separator": ",", "quoteChar": '"'}) # , "escaper" : "\\"
    
    # Write data if GDF not empty
    if gdf.count() != 0:
        glueContext.write_dynamic_frame.from_options(
            frame = gdf,
            connection_type = "s3", 
            connection_options = {
                "path": dest_key, 
                "partitionKeys": ["year", "month", "day", "hour"]
            },
            format = "parquet")
    else:
        print('No new data in table: {}'.format(table_name))


## main ## 

sc = SparkContext()
sqlContext = SQLContext(sc)
glueContext = GlueContext(sc)
spark = glueContext.spark_session

glue_client = boto3.client('glue', region_name='us-east-1')

# Get job arguments
args = getResolvedOptions(sys.argv,
                          ['JOB_NAME',
                           'src_bucket',
                           'prefix',
                           'partition_date'])

# Instantiate job object to later commit
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

s3_resource = boto3.resource('s3')
s3_client = boto3.client("s3")

table_lst = []
table_lst = get_table_list(s3_client, args['src_bucket'], args['prefix'])

for table_path in table_lst:
    # Extract table name from path
    table_name = os.path.join(*table_path.split("/")[-2:-1])
    process_partition(glueContext, args['src_bucket'], table_name, args['prefix'], args['partition_date'])

# Commit job
job.commit()