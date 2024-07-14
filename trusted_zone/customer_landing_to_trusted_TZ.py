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
AmazonS3_node1720870263070 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-step-trainer-data/customer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1720870263070")

# Script generated for node SQL Query
SqlQuery4584 = '''
SELECT * FROM source WHERE shareWithResearchAsOfDate IS NOT NULL

'''
SQLQuery_node1720870669052 = sparkSqlQuery(glueContext, query = SqlQuery4584, mapping = {"source":AmazonS3_node1720870263070}, transformation_ctx = "SQLQuery_node1720870669052")

# Script generated for node Amazon S3
AmazonS3_node1720870757802 = glueContext.getSink(path="s3://stedi-step-trainer-data/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1720870757802")
AmazonS3_node1720870757802.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
AmazonS3_node1720870757802.setFormat("json")
AmazonS3_node1720870757802.writeFrame(SQLQuery_node1720870669052)
job.commit()
