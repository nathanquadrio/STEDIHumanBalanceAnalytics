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
AmazonS3_node1720894515632 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-step-trainer-data/customer/trusted/"], "recurse": True}, transformation_ctx="AmazonS3_node1720894515632")

# Script generated for node Amazon S3
AmazonS3_node1720894589138 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-step-trainer-data/accelerometer/trusted"], "recurse": True}, transformation_ctx="AmazonS3_node1720894589138")

# Script generated for node SQL Query
SqlQuery4511 = '''
SELECT DISTINCT c.customerName, c.email, c.phone, c.birthDay, c.serialNumber, c.registrationDate, c.lastUpdateDate, c.shareWithResearchAsOfDate, c.shareWithPublicAsOfDate, c.shareWithFriendsAsOfDate
FROM customer_trusted c
INNER JOIN accelerometer_trusted 
ON accelerometer_trusted.email = c.email

'''
SQLQuery_node1720894688790 = sparkSqlQuery(glueContext, query = SqlQuery4511, mapping = {"customer_trusted":AmazonS3_node1720894515632, "accelerometer_trusted":AmazonS3_node1720894589138}, transformation_ctx = "SQLQuery_node1720894688790")

# Script generated for node Amazon S3
AmazonS3_node1720894866985 = glueContext.getSink(path="s3://stedi-step-trainer-data/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1720894866985")
AmazonS3_node1720894866985.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
AmazonS3_node1720894866985.setFormat("json")
AmazonS3_node1720894866985.writeFrame(SQLQuery_node1720894688790)
job.commit()
