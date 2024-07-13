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
AmazonS3_node1720896158536 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-step-trainer-data/customer/curated/"], "recurse": True}, transformation_ctx="AmazonS3_node1720896158536")

# Script generated for node Amazon S3
AmazonS3_node1720895925882 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-step-trainer-data/step_trainer/trusted/"], "recurse": True}, transformation_ctx="AmazonS3_node1720895925882")

# Script generated for node SQL Query
SqlQuery4073 = '''
SELECT DISTINCT s.*, c.*
FROM step_trainer_trusted s
INNER JOIN customer_curated c ON s.serialNumber = c.serialNumber
'''
SQLQuery_node1720896194732 = sparkSqlQuery(glueContext, query = SqlQuery4073, mapping = {"step_trainer_trusted":AmazonS3_node1720895925882, "customer_curated":AmazonS3_node1720896158536}, transformation_ctx = "SQLQuery_node1720896194732")

# Script generated for node Amazon S3
AmazonS3_node1720896291082 = glueContext.getSink(path="s3://stedi-step-trainer-data/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1720896291082")
AmazonS3_node1720896291082.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_curated")
AmazonS3_node1720896291082.setFormat("json")
AmazonS3_node1720896291082.writeFrame(SQLQuery_node1720896194732)
job.commit()
