import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.transforms import Join

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load accelerometer_trusted and customer_trusted
acc = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="acc"
)
cust = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="cust"
)

# Join on user = email
joined = Join.apply(cust, acc, 'email', 'user')

# Project only customer fields
projected = joined.drop_fields(['user', 'x', 'y', 'z', 'timeStamp'])

# Write curated customer data
glueContext.write_dynamic_frame.from_options(
    frame=projected,
    connection_type="s3",
    format="json",
    connection_options={"path": "s3://jb-bucket-2025/customer/curated/", "partitionKeys": []},
    transformation_ctx="datasink"
)

job.commit()

