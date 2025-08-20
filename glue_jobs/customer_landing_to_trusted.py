import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load customer data
datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_landing",
    transformation_ctx="datasource0"
)

# Filter records with shareWithResearchAsOfDate not null
filtered = datasource0.filter(lambda x: x["shareWithResearchAsOfDate"] is not None)

# Write trusted data to S3
glueContext.write_dynamic_frame.from_options(
    frame=filtered,
    connection_type="s3",
    format="json",
    connection_options={"path": "s3://jb-bucket-2025/customer/trusted/", "partitionKeys": []},
    transformation_ctx="datasink"
)

job.commit()
