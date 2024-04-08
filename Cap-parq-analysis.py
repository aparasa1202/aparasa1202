import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1681921579132 = glueContext.create_dynamic_frame.from_catalog(
    database="capstone-database1",
    table_name="raw_stat",
    transformation_ctx="AWSGlueDataCatalog_node1681921579132",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1681921565922 = glueContext.create_dynamic_frame.from_catalog(
    database="cleansed-json-data",
    table_name="json-cleansed-data",
    transformation_ctx="AWSGlueDataCatalog_node1681921565922",
)

# Script generated for node Join
Join_node1681921599566 = Join.apply(
    frame1=AWSGlueDataCatalog_node1681921565922,
    frame2=AWSGlueDataCatalog_node1681921579132,
    keys1=["id"],
    keys2=["category_id"],
    transformation_ctx="Join_node1681921599566",
)

# Script generated for node Amazon S3
AmazonS3_node1681924023548 = glueContext.getSink(
    path="s3://capstone-analytics",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["region", "category_id"],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1681924023548",
)
AmazonS3_node1681924023548.setCatalogInfo(
    catalogDatabase="capstone_analytics", catalogTableName="analytics_final"
)
AmazonS3_node1681924023548.setFormat("glueparquet")
AmazonS3_node1681924023548.writeFrame(Join_node1681921599566)
job.commit()
