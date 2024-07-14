import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1720873538115 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-step-trainer-data/customer/trusted/"], "recurse": True}, transformation_ctx="AmazonS3_node1720873538115")

# Script generated for node Amazon S3
AmazonS3_node1720873508511 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-step-trainer-data/step_trainer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1720873508511")

# Script generated for node SQL Query
SqlQuery4411 = '''
select * from step_trainer_landing
join customer_trusted 
on step_trainer_landing.serialNumber = customer_trusted.serialNumber

'''
SQLQuery_node1720873611392 = sparkSqlQuery(glueContext, query = SqlQuery4411, mapping = {"step_trainer_landing":AmazonS3_node1720873508511, "customer_trusted":AmazonS3_node1720873538115}, transformation_ctx = "SQLQuery_node1720873611392")

# Script generated for node Amazon S3
AmazonS3_node1720873688758 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1720873611392, connection_type="s3", format="json", connection_options={"path": "s3://stedi-step-trainer-data/step_trainer/trusted/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1720873688758")

job.commit()
