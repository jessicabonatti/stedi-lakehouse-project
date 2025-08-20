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

# Load trusted data
acc = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="acc"
)
step = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="step"
)

# Join on timeStamp == sensorReadingTime
joined = Join.apply(acc, step, 'timeStamp', 'sensorReadingTime')

# Write machine learning curated data
glueContext.write_dynamic_frame.from_options(
    frame=joined,
    connection_type="s3",
    format="json",
    connection_options={"path": "s3://jb-bucket-2025/machine_learning/curated/", "partitionKeys": []},
    transformation_ctx="datasink"
)

job.commit()
