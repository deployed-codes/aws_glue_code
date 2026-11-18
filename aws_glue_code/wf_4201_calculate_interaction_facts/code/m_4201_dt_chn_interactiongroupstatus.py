import sys
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from utils import *

args = getResolvedOptions(sys.argv, ["JOB_NAME", "ENV", "GLUE_DATABASE", "S3_OUTPUT_BUCKET", "SNOWFLAKE_SECRET_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()

# Resolved once here so every mapping script stops inventing its own differently-spelled
# placeholder for the same handful of values (six spellings of "the Glue database" alone,
# across scripts, before this). Node-level code must reference these directly, never
# redeclare them.
ENV = args["ENV"]
GLUE_DATABASE = args["GLUE_DATABASE"]
S3_OUTPUT_BUCKET = args["S3_OUTPUT_BUCKET"]

SNOWFLAKE_SECRET_NAME = args["SNOWFLAKE_SECRET_NAME"]
SNOWFLAKE_URL, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD = get_snowflake_connection(SNOWFLAKE_SECRET_NAME)


# Top-of-script placeholders for mapping parameters referenced in SQL overrides
INTERACTION_FND_DB = "REPLACE_WITH_INTERACTION_FND_DB_VALUE"
LOAD_EVENT_ID = "REPLACE_WITH_LOAD_EVENT_ID_VALUE"

# Source: InteractionGroupStatus1 (try S3 parquet first because this table is produced earlier in the workflow)
try:
    logger.info("Attempting to read InteractionGroupStatus from S3 parquet path")
    df_InteractionGroupStatus1_jovial_babbage = spark.read.parquet(f"s3://{S3_OUTPUT_BUCKET}/InteractionGroupStatus/")
    logger.info("Read InteractionGroupStatus from S3 parquet path")
except Exception as e:
    logger.warning("S3 parquet read for InteractionGroupStatus failed or path missing, falling back to Glue Data Catalog read")
    try:
        logger.info("Reading InteractionGroupStatus from Glue Data Catalog")
        dyf = glueContext.create_dynamic_frame_from_catalog(
            database=GLUE_DATABASE,
            table_name="InteractionGroupStatus",
            transformation_ctx="df_InteractionGroupStatus1_jovial_babbage",
        )
        df_InteractionGroupStatus1_jovial_babbage = dyf.toDF()
        logger.info("Read InteractionGroupStatus from Glue Data Catalog")
    except Exception as e2:
        logger.error(f"Failed reading InteractionGroupStatus from Glue Data Catalog: {e2}", exc_info=True)
        raise

# Guard empty source so an empty upstream is visible in logs
try:
    if df_InteractionGroupStatus1_jovial_babbage.isEmpty():
        logger.warning("Source InteractionGroupStatus is empty")
except Exception as e:
    logger.error(f"Failed while checking emptiness of InteractionGroupStatus source: {e}", exc_info=True)
    raise

# SQ_DateChain: staged-case SQL override - run the provided SQL against the temp view for InteractionGroupStatus
sql_query = f"""SELECT
O.InteractionGroupStatus_Id,
O.InteractionGroup_Id,
O.NewExpiration_Dt,
O.NewRevision_Ts,
O.NewSequenceEnd_It,
O.Source_Cd
FROM
(
SELECT InteractionGroupStatus_Id,
       InteractionGroup_Id,
       Source_Cd,
       COALESCE (
          MIN (
             Effective_Dt)
          OVER (PARTITION BY InteractionGroup_Id
                ORDER BY Effective_Dt ASC, SequenceStart_It ASC
                ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING),
          DATE '3500-01-01')
          AS NewExpiration_Dt,
       COALESCE (
          MIN (
             Transaction_Ts)
          OVER (PARTITION BY InteractionGroup_Id
                ORDER BY Transaction_Ts ASC, SequenceStart_It ASC
                ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING),
          TIMESTAMP '3500-01-01 00:00:00.000000')
          AS NewRevision_Ts,
       COALESCE (
          MIN (
             SequenceStart_It)
          OVER (PARTITION BY InteractionGroup_Id
                ORDER BY SequenceStart_It ASC
                ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING),
          350036535003659999)
          AS NewSequenceEnd_It
  FROM InteractionGroupStatus tgt
 WHERE EXISTS
          (SELECT 1
             FROM  InteractionGroupStatus tgt1
            WHERE     tgt.InteractionGroup_Id = tgt1.InteractionGroup_Id
                  AND tgt.Source_Cd = tgt1.Source_Cd
                  AND tgt1.LoadEvent_Id >= {LOAD_EVENT_ID} )
QUALIFY NOT (Expiration_Dt  = NewExpiration_Dt
        AND  Revision_Ts    = NewRevision_Ts
        AND  SequenceEnd_It = NewSequenceEnd_It))O"""

try:
    logger.info("Registering InteractionGroupStatus dataframe as temp view and executing SQ_DateChain SQL override via spark.sql")
    # register the already-read/staged dataframe as the base table the override expects
    df_InteractionGroupStatus1_jovial_babbage.createOrReplaceTempView("InteractionGroupStatus")
    df_SQ_DateChain_hopeful_kant = spark.sql(sql_query)
    logger.info("SQ_DateChain query executed successfully")
except Exception as e:
    logger.error(f"Failed running SQ_DateChain override query: {e}", exc_info=True)
    raise

# Output: InteractionGroupStatus - reuse the incoming dataframe and write as parquet to S3 (overwrite)
df_InteractionGroupStatus_clever_tesla = df_SQ_DateChain_hopeful_kant

# write intermediate / final InteractionGroupStatus table as parquet to S3 (overwrite)
try:
    logger.info("Writing InteractionGroupStatus to S3 as parquet (overwrite)")
    df_InteractionGroupStatus_clever_tesla.write.mode("overwrite").parquet(
        f"s3://{S3_OUTPUT_BUCKET}/InteractionGroupStatus/"
    )
    logger.info("Successfully wrote InteractionGroupStatus to S3")
except Exception as e:
    logger.error(f"Failed writing InteractionGroupStatus to S3: {e}", exc_info=True)
    raise



job.commit()
