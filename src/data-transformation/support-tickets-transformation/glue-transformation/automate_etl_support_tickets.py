import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.gluetypes import *
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame
import re

def _find_null_fields(ctx, schema, path, output, nullStringSet, nullIntegerSet, frame):
    if isinstance(schema, StructType):
        for field in schema:
            new_path = path + "." if path != "" else path
            output = _find_null_fields(ctx, field.dataType, new_path + field.name, output, nullStringSet, nullIntegerSet, frame)
    elif isinstance(schema, ArrayType):
        if isinstance(schema.elementType, StructType):
            output = _find_null_fields(ctx, schema.elementType, path, output, nullStringSet, nullIntegerSet, frame)
    elif isinstance(schema, NullType):
        output.append(path)
    else:
        x, distinct_set = frame.toDF(), set()
        for i in x.select(path).distinct().collect():
            distinct_ = i[path.split('.')[-1]]
            if isinstance(distinct_, list):
                distinct_set |= set([item.strip() if isinstance(item, str) else item for item in distinct_])
            elif isinstance(distinct_, str) :
                distinct_set.add(distinct_.strip())
            else:
                distinct_set.add(distinct_)
        if isinstance(schema, StringType):
            if distinct_set.issubset(nullStringSet):
                output.append(path)
        elif isinstance(schema, IntegerType) or isinstance(schema, LongType) or isinstance(schema, DoubleType):
            if distinct_set.issubset(nullIntegerSet):
                output.append(path)
    return output

def drop_nulls(glueContext, frame, nullStringSet, nullIntegerSet, transformation_ctx) -> DynamicFrame:
    nullColumns = _find_null_fields(frame.glue_ctx, frame.schema(), "", [], nullStringSet, nullIntegerSet, frame)
    return DropFields.apply(frame=frame, paths=nullColumns, transformation_ctx=transformation_ctx)

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

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Data Ingest-support-tickets
DataIngestsupporttickets_node1771256560424 = glueContext.create_dynamic_frame.from_catalog(database="careplus_db", table_name="crawler_support_tickets_raw", transformation_ctx="DataIngestsupporttickets_node1771256560424")

# Script generated for node Change Schema
ChangeSchema_node1771257331794 = ApplyMapping.apply(frame=DataIngestsupporttickets_node1771256560424, mappings=[("ticket_id", "string", "ticket_id", "string"), ("created_at", "string", "created_at", "timestamp"), ("resolved_at", "string", "resolved_at", "timestamp"), ("agent", "string", "agent", "string"), ("priority", "string", "priority", "string"), ("num_interactions", "long", "num_interactions", "int"), ("issuecat", "string", "issuecat", "string"), ("channel", "string", "channel", "string"), ("status", "string", "status", "string"), ("agent_feedback", "string", "agent_feedback", "string"), ("is_change", "double", "is_change", "double"), ("manager", "string", "manager", "string")], transformation_ctx="ChangeSchema_node1771257331794")

# Script generated for node Drop Null Fields
DropNullFields_node1771278041741 = drop_nulls(glueContext, frame=ChangeSchema_node1771257331794, nullStringSet={""}, nullIntegerSet={}, transformation_ctx="DropNullFields_node1771278041741")

# Script generated for node Rename Field-issue cat
RenameFieldissuecat_node1771278323443 = RenameField.apply(frame=DropNullFields_node1771278041741, old_name="issuecat", new_name="issue_category", transformation_ctx="RenameFieldissuecat_node1771278323443")

# Script generated for node Filter-num of iterations
Filternumofiterations_node1771278524889 = Filter.apply(frame=RenameFieldissuecat_node1771278323443, f=lambda row: (row["num_interactions"] > 0), transformation_ctx="Filternumofiterations_node1771278524889")

# Script generated for node SQL Query--priority
SqlQuery0 = '''
select 
*,
case
when priority='Hgh'then 'High'
when priority='Lw'then 'Low'
when priority='Medum'then 'Medium'
else priority
end as priority

from myDataSource
'''
SQLQuerypriority_node1771278643463 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":Filternumofiterations_node1771278524889}, transformation_ctx = "SQLQuerypriority_node1771278643463")

# Script generated for node Select Fields
SelectFields_node1771280090154 = SelectFields.apply(frame=SQLQuerypriority_node1771278643463, paths=["ticket_id", "created_at", "resolved_at", "issue_category", "agent", "num_interactions", "priority", "channel", "status", "is_change", "manager"], transformation_ctx="SelectFields_node1771280090154")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SelectFields_node1771280090154, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1771277858992", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (SelectFields_node1771280090154.count() >= 1):
   SelectFields_node1771280090154 = SelectFields_node1771280090154.coalesce(1)
AmazonS3_node1771279505585 = glueContext.getSink(path="s3://amzn-careplus-data-store/support-tickets/processed/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1771279505585")
AmazonS3_node1771279505585.setCatalogInfo(catalogDatabase="careplus_db",catalogTableName="support_ticket_processed_after_glue")
AmazonS3_node1771279505585.setFormat("glueparquet", compression="snappy")
AmazonS3_node1771279505585.writeFrame(SelectFields_node1771280090154)
job.commit()