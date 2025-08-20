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

# Load accelerometer and customer_trusted
datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="datasource0"
)
customer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted"
)

# Join on user = email
joined = Join.apply(datasource0, customer_trusted, 'user', 'email')

# Drop PII fields
projected = joined.drop_fields([
    'customerName', 'email', 'phone', 'birthDay',
    'serialNumber', 'registrationDate', 'lastUpdateDate',
    'shareWithResearchAsOfDate', 'shareWithPublicAsOfDate',
    'shareWithFriendsAsOfDate'
])

# Write to trusted zone
glueContext.write_dynamic_frame.from_options(
    frame=projected,
    connection_type="s3",
    format="json",
    connection_options={"path": "s3://jb-bucket-2025/accelerometer/trusted/", "partitionKeys": []},
    transformation_ctx="datasink"
)

job.commit()
