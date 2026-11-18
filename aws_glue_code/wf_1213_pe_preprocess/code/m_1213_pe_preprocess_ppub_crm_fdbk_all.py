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


# placeholder for flat-file output directory referenced by Output node File_PPUB_CRM_FDBK_ALL_Processed
WORKING_DIR = "REPLACE_WITH_WORKING_DIR_VALUE"

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os
import glob
import shutil
import pandas as pd

# Source: File_PPUB_CRM_FDBK_ALL -> df_File_PPUB_CRM_FDBK_ALL_laughing_aristotle
try:
    logger.info("Reading source File_PPUB_CRM_FDBK_ALL from Glue Data Catalog into df_File_PPUB_CRM_FDBK_ALL_laughing_aristotle")
    dyf_File_PPUB_CRM_FDBK_ALL_laughing_aristotle = glueContext.create_dynamic_frame_from_catalog(
        database=GLUE_DATABASE,
        table_name="File_PPUB_CRM_FDBK_ALL",
        transformation_ctx="df_File_PPUB_CRM_FDBK_ALL_laughing_aristotle",
    )
    df_File_PPUB_CRM_FDBK_ALL_laughing_aristotle = dyf_File_PPUB_CRM_FDBK_ALL_laughing_aristotle.toDF().select(
        "SEGMENT_ID",
        "TARGET_KEY",
        "TGT_LVL_CD",
        "OPERATIONALIZED_KEY",
        "STAT_DT",
        "STAT_TS",
        "STAT_CD",
        "WEIGHT",
        "DIRECTION",
        "OWNING_RES_AGCY_NO",
        "OWNING_RES_PDCR_NO",
        "UPDATEDBY_RES_AGCY_NO",
        "UPDATEDBY_RES_PDCR_NO",
        "APPLICATION_ID",
        "LAST_UPDT_ID",
        "LAST_UPDT_DOM",
        "ORDER_ID",
        "LINE_ITEM_ID",
        "OPP_ID",
        "OPP_DTL_ID",
        "OPP_LIB_ID",
        "EXP_DT",
        "PCN_STG_DT",
        "PRIORITY",
        "PRIORITY_STAR",
        "AGENT_ADDED_IN",
        "ARM_TOUCHPOINT",
        "ARM_LEAD_KEY_ID",
        "ARM_LEAD_INSTANCE_ID",
        "ARM_LOGICAL_SEGMENT_ID",
        "LOAD_DT",
        "CurrentlyProcessedFileName"
    )
    if df_File_PPUB_CRM_FDBK_ALL_laughing_aristotle.isEmpty():
        logger.warning("Source File_PPUB_CRM_FDBK_ALL returned no rows")
except Exception as e:
    logger.error(f"Failed reading File_PPUB_CRM_FDBK_ALL from Glue Catalog: {e}", exc_info=True)
    raise

# Source: File_PPUB_MLDB_OPP_DTL -> df_File_PPUB_MLDB_OPP_DTL_focused_newton
try:
    logger.info("Reading source File_PPUB_MLDB_OPP_DTL from Glue Data Catalog into df_File_PPUB_MLDB_OPP_DTL_focused_newton")
    dyf_File_PPUB_MLDB_OPP_DTL_focused_newton = glueContext.create_dynamic_frame_from_catalog(
        database=GLUE_DATABASE,
        table_name="File_PPUB_MLDB_OPP_DTL",
        transformation_ctx="df_File_PPUB_MLDB_OPP_DTL_focused_newton",
    )
    df_File_PPUB_MLDB_OPP_DTL_focused_newton = dyf_File_PPUB_MLDB_OPP_DTL_focused_newton.toDF().select(
        "Opp_Dtl_Id",
        "Opp_Lib_Id",
        "Source_Cd",
        "Source_Dtl_Key",
        "Exp_Dt",
        "Create_Dt",
        "DM_URL",
        "Long_Ds"
    )
    if df_File_PPUB_MLDB_OPP_DTL_focused_newton.isEmpty():
        logger.warning("Source File_PPUB_MLDB_OPP_DTL returned no rows")
except Exception as e:
    logger.error(f"Failed reading File_PPUB_MLDB_OPP_DTL from Glue Catalog: {e}", exc_info=True)
    raise

# SQ: SQ_File_PPUB_CRM_FDBK_ALL (pass-through projection) -> df_SQ_File_PPUB_CRM_FDBK_ALL_sleepy_franklin
try:
    logger.info("Projecting SQ_File_PPUB_CRM_FDBK_ALL into df_SQ_File_PPUB_CRM_FDBK_ALL_sleepy_franklin")
    # select exactly the ports declared on the Source Qualifier
    df_SQ_File_PPUB_CRM_FDBK_ALL_sleepy_franklin = df_File_PPUB_CRM_FDBK_ALL_laughing_aristotle.select(
        "LOAD_DT",
        "SEGMENT_ID",
        "TARGET_KEY",
        "TGT_LVL_CD",
        "OPERATIONALIZED_KEY",
        "STAT_DT",
        "STAT_TS",
        "STAT_CD",
        "WEIGHT",
        "DIRECTION",
        "OWNING_RES_AGCY_NO",
        "OWNING_RES_PDCR_NO",
        "UPDATEDBY_RES_AGCY_NUM",  # note: source had UPDATEDBY_RES_AGCY_NO naming; keep SQ's declared name
        "UPDATEDBY_RES_PDCR_NUM",
        "APPLICATION_ID",
        "LAST_UPDT_ID",
        "LAST_UPDT_DOM",
        "ORDER_ID",
        "LINE_ITEM_ID",
        "OPP_ID",
        "OPP_DTL_ID",
        "OPP_LIB_ID",
        "EXP_DT",
        "PCN_STG_DT",
        "PRIORITY",
        "PRIORITY_STAR",
        "AGENT_ADDED_IN",
        "ARM_TOUCHPOINT",
        "ARM_LEAD_KEY_ID",
        "ARM_LOGICAL_SEGMENT_ID",
        "ARM_LEAD_INSTANCE_ID",
        "CurrentlyProcessedFileName"
    )
except Exception as e:
    logger.error(f"Failed projecting SQ_File_PPUB_CRM_FDBK_ALL: {e}", exc_info=True)
    raise

# SQ: SQ_File_PPUB_MLDB_OPP_DTL (pass-through projection) -> df_SQ_File_PPUB_MLDB_OPP_DTL_heroic_hopper
try:
    logger.info("Projecting SQ_File_PPUB_MLDB_OPP_DTL into df_SQ_File_PPUB_MLDB_OPP_DTL_heroic_hopper")
    df_SQ_File_PPUB_MLDB_OPP_DTL_heroic_hopper = df_File_PPUB_MLDB_OPP_DTL_focused_newton.select(
        "Opp_Dtl_Id",
        "Opp_Lib_Id",
        "Source_Cd",
        "Source_Dtl_Key",
        "Exp_Dt",
        "Create_Dt",
        "DM_URL",
        "Long_Ds"
    )
except Exception as e:
    logger.error(f"Failed projecting SQ_File_PPUB_MLDB_OPP_DTL: {e}", exc_info=True)
    raise

# Joiner: Join_Fdbk_All_with_MLDB_Dtl (detail=CRM feedback, master=MLDB) -> df_Join_Fdbk_All_with_MLDB_Dtl_mystifying_feynman
try:
    logger.info("Joining df_SQ_File_PPUB_CRM_FDBK_ALL_sleepy_franklin (detail) to df_SQ_File_PPUB_MLDB_OPP_DTL_heroic_hopper (master) into df_Join_Fdbk_All_with_MLDB_Dtl_mystifying_feynman")
    d = df_SQ_File_PPUB_CRM_FDBK_ALL_sleepy_franklin.alias("d")
    m = df_SQ_File_PPUB_MLDB_OPP_DTL_heroic_hopper.alias("m")
    joined = d.join(m, F.col("d.FDBK_ALL_OPP_DTL_ID") == F.col("m.Opp_Dtl_Id"), "left")

    # Project exactly the Joiner's declared output ports, mapping source columns where names differ
    df_Join_Fdbk_All_with_MLDB_Dtl_mystifying_feynman = joined.select(
        F.col("m.Opp_Dtl_Id").alias("MLDB_Opp_Dtl_Id"),
        F.col("m.Opp_Lib_Id").alias("MLDB_Opp_Lib_Id"),
        F.col("m.DM_URL").alias("MLDB_DM_URL"),
        F.col("m.Source_Cd").alias("MLDB_Source_Cd"),
        F.col("d.OPP_DTL_ID").alias("FDBK_ALL_OPP_DTL_ID"),
        F.col("d.OPP_LIB_ID").alias("FDBK_ALL_OPP_LIB_ID"),
        F.col("d.LOAD_DT").alias("LOAD_DT"),
        F.col("d.SEGMENT_ID").alias("SEGMENT_ID"),
        F.col("d.TARGET_KEY").alias("TARGET_KEY"),
        F.col("d.TGT_LVL_CD").alias("TGT_LVL_CD"),
        F.col("d.OPERATIONALIZED_KEY").alias("OPERATIONALIZED_KEY"),
        F.col("d.STAT_DT").alias("STAT_DT"),
        F.col("d.STAT_TS").alias("STAT_TS"),
        F.col("d.STAT_CD").alias("STAT_CD"),
        F.col("d.WEIGHT").alias("WEIGHT"),
        F.col("d.DIRECTION").alias("DIRECTION"),
        F.col("d.OWNING_RES_AGCY_NO").alias("OWNING_RES_AGCY_NO"),
        F.col("d.OWNING_RES_PDCR_NO").alias("OWNING_RES_PDCR_NO"),
        F.col("d.UPDATEDBY_RES_AGCY_NUM").alias("UPDATEDBY_RES_AGCY_NUM"),
        F.col("d.UPDATEDBY_RES_PDCR_NUM").alias("UPDATEDBY_RES_PDCR_NUM"),
        F.col("d.APPLICATION_ID").alias("APPLICATION_ID"),
        F.col("d.LAST_UPDT_ID").alias("LAST_UPDT_ID"),
        F.col("d.LAST_UPDT_DOM").alias("LAST_UPDT_DOM"),
        F.col("d.ORDER_ID").alias("ORDER_ID"),
        F.col("d.LINE_ITEM_ID").alias("LINE_ITEM_ID"),
        F.col("d.OPP_ID").alias("OPP_ID"),
        F.col("d.EXP_DT").alias("EXP_DT"),
        F.col("d.PCN_STG_DT").alias("PCN_STG_DT"),
        F.col("d.PRIORITY").alias("PRIORITY"),
        F.col("d.PRIORITY_STAR").alias("PRIORITY_STAR"),
        F.col("d.AGENT_ADDED_IN").alias("AGENT_ADDED_IN"),
        F.col("d.ARM_TOUCHPOINT").alias("ARM_TOUCHPOINT"),
        F.col("d.ARM_LEAD_KEY_ID").alias("ARM_LEAD_KEY_ID"),
        F.col("d.ARM_LOGICAL_SEGMENT_ID").alias("ARM_LOGICAL_SEGMENT_ID"),
        F.col("d.ARM_LEAD_INSTANCE_ID").alias("ARM_LEAD_INSTANCE_ID"),
        # map the file attribute to the declared joiner output name
        F.col("d.CurrentlyProcessedFileName").alias("SRC_FILE_NM_CRM_FDBK_ALL")
    )
except Exception as e:
    logger.error(f"Failed during Join_Fdbk_All_with_MLDB_Dtl: {e}", exc_info=True)
    raise

# Expression: Exp_Generate_Source_Order_And_Interaction_Groupkeycd -> df_Exp_Generate_Source_Order_And_Interaction_Groupkeycd_optimistic_pascal
try:
    logger.info("Generating Source_Order and o_ARM_LEAD_INSTANCE_ID in df_Exp_Generate_Source_Order_And_Interaction_Groupkeycd_optimistic_pascal")
    df_tmp = df_Join_Fdbk_All_with_MLDB_Dtl_mystifying_feynman

    df_tmp = df_tmp.withColumn(
        "Source_Order",
        F.when(F.col("STAT_CD") == "U", F.lit(1))
        .when(F.col("STAT_CD") == "F", F.lit(2))
        .when(F.col("STAT_CD").isin(["C", "D", "I", "M", "N", "Z"]), F.lit(3))
        .when(F.col("STAT_CD").isin(["H", "K", "L", "O", "P", "R", "V", "W", "E"]), F.lit(4))
        .otherwise(F.lit(5)).cast("integer")
    )

    df_tmp = df_tmp.withColumn(
        "o_ARM_LEAD_INSTANCE_ID",
        F.when(
            (F.col("ARM_LEAD_INSTANCE_ID").isNull()) | (F.trim(F.col("ARM_LEAD_INSTANCE_ID")) == ""),
            F.lit("@"),
        ).otherwise(F.col("ARM_LEAD_INSTANCE_ID"))
    )

    # preserve all existing columns plus the new ones
    df_Exp_Generate_Source_Order_And_Interaction_Groupkeycd_optimistic_pascal = df_tmp
except Exception as e:
    logger.error(f"Failed generating Source_Order / o_ARM_LEAD_INSTANCE_ID: {e}", exc_info=True)
    raise

# Sorter: Srt_to_Generate_PE_Line_Nbr -> df_Srt_to_Generate_PE_Line_Nbr_hopeful_euclid
try:
    logger.info("Unioning upstream inputs and sorting into df_Srt_to_Generate_PE_Line_Nbr_hopeful_euclid")
    # union the two upstream inputs by name if both present
    df_a = df_Exp_Generate_Source_Order_And_Interaction_Groupkeycd_optimistic_pascal
    df_b = df_Join_Fdbk_All_with_MLDB_Dtl_mystifying_feynman

    # ensure same columns for unionByName: add missing columns as nulls if needed
    cols_union = list({*df_a.columns, *df_b.columns})
    def ensure_columns(df, cols):
        for c in cols:
            if c not in df.columns:
                df = df.withColumn(c, F.lit(None))
        return df.select(*cols)

    df_a_sel = ensure_columns(df_a, cols_union)
    df_b_sel = ensure_columns(df_b, cols_union)

    df_union = df_a_sel.unionByName(df_b_sel)

    # apply the Sorter's global orderBy (ascending for all keys)
    sort_keys = [
        "OPP_ID",
        "STAT_TS",
        "Source_Order",
        "ARM_LEAD_KEY_ID",
        "ARM_LEAD_INSTANCE_ID",
        "ARM_LOGICAL_SEGMENT_ID",
        "MLDB_DM_URL",  # master DM_URL mapped earlier
        "PCN_STG_DT",
        "OPERATIONALIZED_KEY",
    ]

    # Some keys may not exist in the union; filter to existing columns
    sort_keys_existing = [k for k in sort_keys if k in df_union.columns]
    if sort_keys_existing:
        df_Srt_to_Generate_PE_Line_Nbr_hopeful_euclid = df_union.orderBy(sort_keys_existing)
    else:
        df_Srt_to_Generate_PE_Line_Nbr_hopeful_euclid = df_union

except Exception as e:
    logger.error(f"Failed sorting/unioning in Srt_to_Generate_PE_Line_Nbr: {e}", exc_info=True)
    raise

# Expression: Exp_Generate_PE_Line_Nbr -> df_Exp_Generate_PE_Line_Nbr_nice_socrates
try:
    logger.info("Generating PE line numbers and extracting source filename into df_Exp_Generate_PE_Line_Nbr_nice_socrates")
    df_in = df_Srt_to_Generate_PE_Line_Nbr_hopeful_euclid

    # window: partition by OPP_ID, order by Sorter's ordering keys (exclude OPP_ID)
    ordering = [k for k in ["STAT_TS", "Source_Order", "ARM_LEAD_KEY_ID", "ARM_LEAD_INSTANCE_ID", "ARM_LOGICAL_SEGMENT_ID", "MLDB_DM_URL", "PCN_STG_DT", "OPERATIONALIZED_KEY"] if k in df_in.columns]
    w = Window.partitionBy("OPP_ID").orderBy([F.col(c).asc() for c in ordering]) if ordering else Window.partitionBy("OPP_ID").orderBy(F.lit(1))

    df_tmp2 = df_in.withColumn("PE_LINE_NB", F.row_number().over(w).cast("bigint"))

    # extract filename after last '/' into o_SRC_File_Nm and also provide FileName for the target
    if "SRC_FILE_NM_CRM_FDBK_ALL" in df_tmp2.columns:
        df_tmp2 = df_tmp2.withColumn("o_SRC_File_Nm", F.regexp_extract(F.col("SRC_FILE_NM_CRM_FDBK_ALL"), r"([^/]+)$", 1))
    else:
        df_tmp2 = df_tmp2.withColumn("o_SRC_File_Nm", F.lit(None))

    # also expose a FileName column expected by the final target (map from o_SRC_File_Nm)
    df_Exp_Generate_PE_Line_Nbr_nice_socrates = df_tmp2.withColumn("FileName", F.col("o_SRC_File_Nm"))

except Exception as e:
    logger.error(f"Failed generating PE line numbers in Exp_Generate_PE_Line_Nbr: {e}", exc_info=True)
    raise

# Output: File_PPUB_CRM_FDBK_ALL_Processed (flat file target) -> df_File_PPUB_CRM_FDBK_ALL_Processed_wonderful_euclid
try:
    logger.info("Preparing final flat-file output df_File_PPUB_CRM_FDBK_ALL_Processed_wonderful_euclid and writing single delimited file to WORKING_DIR")

    # combine incoming inputs as required: union the sorter output and the PE line-numbered frame if distinct
    left_df = df_Srt_to_Generate_PE_Line_Nbr_hopeful_euclid
    right_df = df_Exp_Generate_PE_Line_Nbr_nice_socrates

    # align columns for union
    cols_final = list({*left_df.columns, *right_df.columns})
    def ensure_cols(df, cols):
        for c in cols:
            if c not in df.columns:
                df = df.withColumn(c, F.lit(None))
        return df.select(*cols)

    left_sel = ensure_cols(left_df, cols_final)
    right_sel = ensure_cols(right_df, cols_final)

    df_File_PPUB_CRM_FDBK_ALL_Processed_wonderful_euclid = left_sel.unionByName(right_sel)

    # write as a single delimited file (delimiter \x04) via Spark staging to a tmp dir, then move the single part file
    tmp_dir = os.path.join(WORKING_DIR, "_tmp_File_PPUB_CRM_FDBK_ALL_Processed")
    final_path = os.path.join(WORKING_DIR, "PE_Preprocess_Outupt.out")

    # remove tmp dir if exists
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)

    # coalesce to 1 partition and write as CSV without header
    df_File_PPUB_CRM_FDBK_ALL_Processed_wonderful_euclid.coalesce(1).write.mode("overwrite").option("header", False).option("sep", "\x04").csv(tmp_dir)

    # find the single part file
    part_files = glob.glob(os.path.join(tmp_dir, "part-*"))
    if not part_files:
        raise RuntimeError(f"No part file found in temporary CSV directory {tmp_dir}")
    part_file = part_files[0]

    # ensure target directory exists
    os.makedirs(os.path.dirname(final_path), exist_ok=True)

    # move part file to final path (overwrite if exists)
    if os.path.exists(final_path):
        os.remove(final_path)
    shutil.move(part_file, final_path)

    # cleanup tmp dir
    shutil.rmtree(tmp_dir)

except Exception as e:
    logger.error(f"Failed writing flat-file target File_PPUB_CRM_FDBK_ALL_Processed: {e}", exc_info=True)
    raise



job.commit()
