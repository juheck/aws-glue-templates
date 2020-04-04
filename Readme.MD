# AWS Glue job template

AWS Glue job example using Dynamic Frames, Job Bookmarks and push down predicates to move data from /raw to /enriched subfolder in parquet format. 

## Parameters

Parameters are:

- **src_bucket**: Data source bucket, i.e. S3 bucket name
- **prefix**: S3 prefix
- **partition_date**: Partition key date in `prefix` to be processed (e.g. for 2020-04-04, s3://src_bucket/prefix/year=2020/month=04/day=04)
