import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
print ("The args: ", args)


# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1708237359808 = glueContext.create_dynamic_frame.from_catalog(database="aws-poc2-db", table_name="world_bank_indicators_custom_python_source")
#AWSGlueDataCatalog_node1708237359808.printSchema()
#AWSGlueDataCatalog_node1708237359808.show()
#AWSGlueDataCatalog_node1708237359808.toDF().show()


# Script generated for node Remove Prior to 1990
RemovePriorto1990_node1708238527332 = ApplyMapping.apply(frame=AWSGlueDataCatalog_node1708237359808, mappings=[("country_name", "string", "country_name", "string"), ("country_code", "string", "country_code", "string"), ("indicator_name", "string", "indicator_name", "string"), ("indicator_code", "string", "indicator_code", "string"), ("1990", "decimal", "1990", "decimal"), ("1991", "decimal", "1991", "decimal"), ("1992", "decimal", "1992", "decimal"), ("1993", "decimal", "1993", "decimal"), ("1994", "decimal", "1994", "decimal"), ("1995", "decimal", "1995", "decimal"), ("1996", "decimal", "1996", "decimal"), ("1997", "decimal", "1997", "decimal"), ("1998", "decimal", "1998", "decimal"), ("1999", "decimal", "1999", "decimal"), ("2000", "decimal", "2000", "decimal"), ("2001", "decimal", "2001", "decimal"), ("2002", "decimal", "2002", "decimal"), ("2003", "decimal", "2003", "decimal"), ("2004", "decimal", "2004", "decimal"), ("2005", "decimal", "2005", "decimal"), ("2006", "decimal", "2006", "decimal"), ("2007", "decimal", "2007", "decimal"), ("2008", "decimal", "2008", "decimal"), ("2009", "decimal", "2009", "decimal"), ("2010", "decimal", "2010", "decimal"), ("2011", "decimal", "2011", "decimal"), ("2012", "decimal", "2012", "decimal"), ("2013", "decimal", "2013", "decimal"), ("2014", "decimal", "2014", "decimal"), ("2015", "decimal", "2015", "decimal"), ("2016", "decimal", "2016", "decimal"), ("2017", "decimal", "2017", "decimal"), ("2018", "decimal", "2018", "decimal"), ("2019", "decimal", "2019", "decimal"), ("2020", "decimal", "2020", "decimal"), ("2021", "decimal", "2021", "decimal"), ("2022", "decimal", "2022", "decimal")])
#RemovePriorto1990_node1708238527332.toDF().show()

# Script generated for node Amazon S3
AmazonS3_node1708238624148 = glueContext.write_dynamic_frame.from_options(frame=RemovePriorto1990_node1708238527332, connection_type="s3", format="parquet", connection_options={"path": "s3://poc-world-bank-data/GC.DOD.TOTL.GD.ZS/", "partitionKeys": []}, format_options={"compression": "uncompressed"})

#print ("Completed the import")
job.commit()
