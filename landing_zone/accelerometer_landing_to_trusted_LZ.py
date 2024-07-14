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
AmazonS3_node1720871320215 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-step-trainer-data/accelerometer/landing/"]}, transformation_ctx="AmazonS3_node1720871320215")

# Script generated for node Amazon S3
AmazonS3_node1720871522362 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-step-trainer-data/customer/trusted/"], "recurse": True}, transformation_ctx="AmazonS3_node1720871522362")

# Script generated for node SQL Query
SqlQuery4158 = '''
select * from accelerometer_landing 
join customer_trusted 
on accelerometer_landing.user = customer_trusted.email

'''
SQLQuery_node1720871680640 = sparkSqlQuery(glueContext, query = SqlQuery4158, mapping = {"accelerometer_landing":AmazonS3_node1720871320215, "customer_trusted":AmazonS3_node1720871522362}, transformation_ctx = "SQLQuery_node1720871680640")

# Script generated for node Amazon S3
AmazonS3_node1720871859235 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1720871680640, connection_type="s3", format="json", connection_options={"path": "s3://stedi-step-trainer-data/accelerometer/trusted/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1720871859235")

job.commit()
