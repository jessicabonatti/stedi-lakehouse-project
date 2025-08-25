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

# Load data
step_landing = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_landing",
    transformation_ctx="step_landing"
)

customer_curated = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="customer_curated"
)

# Join by serialNumber
joined = Join.apply(step_landing, customer_curated, 'serialNumber', 'serialNumber')

# Write to trusted zone
glueContext.write_dynamic_frame.from_options(
    frame=joined,
    connection_type="s3",
    format="json",
    connection_options={"path": "s3://jb-bucket-2025/step_trainer/trusted/", "partitionKeys": []},
    transformation_ctx="step_trainer_trusted"
)

job.commit()
