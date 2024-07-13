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
AmazonS3_node1720897587585 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-step-trainer-data/step_trainer/trusted"], "recurse": True}, transformation_ctx="AmazonS3_node1720897587585")

# Script generated for node Amazon S3
AmazonS3_node1720897623941 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-step-trainer-data/accelerometer/trusted"], "recurse": True}, transformation_ctx="AmazonS3_node1720897623941")

# Script generated for node SQL Query
SqlQuery4206 = '''
SELECT DISTINCT s.*, a.*
FROM step_trainer_trusted s
INNER JOIN accelerometer_trusted a ON s.sensorReadingTime = a.timestamp
'''
SQLQuery_node1720897669959 = sparkSqlQuery(glueContext, query = SqlQuery4206, mapping = {"step_trainer_trusted":AmazonS3_node1720897587585, "accelerometer_trusted":AmazonS3_node1720897623941}, transformation_ctx = "SQLQuery_node1720897669959")

# Script generated for node Amazon S3
AmazonS3_node1720897758255 = glueContext.getSink(path="s3://stedi-step-trainer-data/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1720897758255")
AmazonS3_node1720897758255.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
AmazonS3_node1720897758255.setFormat("glueparquet", compression="snappy")
AmazonS3_node1720897758255.writeFrame(SQLQuery_node1720897669959)
job.commit()
