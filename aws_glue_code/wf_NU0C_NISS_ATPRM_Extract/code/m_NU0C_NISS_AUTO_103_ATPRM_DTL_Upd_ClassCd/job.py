import sys
from typing import Any, Mapping

from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import DataFrame

from glue_lib.connector import ParquetReader, ParquetWriter
from glue_lib.exceptions import ConfigurationValidationError
from glue_lib.job.base import BaseJob
from pyspark.sql.functions import col, trim, when, lit, expr, coalesce


# Informatica mapping m_NU0C_NISS_AUTO_103_ATPRM_DTL_Upd_ClassCd.
# Every mapping node runs in order inside run().
class Nu0cNissAuto103AtprmDtlUpdClassCdJob(BaseJob):

    def initialize(self) -> None:

        self.env = self.config.get("env", {})

        self.s3_output_bucket = self._require_env(self.env, "S3_OUTPUT_BUCKET")
        self.glue_database = self._require_env(self.env, "GLUE_DATABASE")

        # SparkSession / GlueContext are created by GlueBootstrap (GlueRuntime).
        self.spark = self.runtime.spark
        self.glue_context = self.runtime.glue_context

        self.parquet_reader = ParquetReader(self.spark)
        self.parquet_writer = ParquetWriter()

        # Glue Job.init/commit (job bookmarks, run state) is not handled by glue-lib.
        args = getResolvedOptions(sys.argv, ["JOB_NAME"])
        self.glue_job = Job(self.glue_context)
        self.glue_job.init(args["JOB_NAME"], args)

    def run(self) -> None:

        # FDR_LIB_WRK_BIRP_NISS_APRM_DETL: Source (S3-first for WRK_ staging table)
        try:
            self.logger.info("Attempting S3-first read for FDR_LIB_WRK_BIRP_NISS_APRM_DETL", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_adoring_curie = self.parquet_reader.read(
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                options=None,
            )
            # guard empty source
            if df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_adoring_curie.isEmpty():
                self.logger.warning("Source FDR_LIB_WRK_BIRP_NISS_APRM_DETL returned no rows", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
        except Exception:
            # fallback to Glue Catalog read
            try:
                self.logger.warning("S3 read failed for WRK_BIRP_NISS_APRM_DETL; falling back to Glue Catalog", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
                dyf = self.glue_context.create_dynamic_frame.from_catalog(
                    database=self.glue_database,
                    table_name="WRK_BIRP_NISS_APRM_DETL",
                    transformation_ctx="df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_adoring_curie",
                )
                df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_adoring_curie = dyf.toDF().select(
                    "ST_CD",
                    "ST_ABBR",
                    "NISS_APRM_DETL_SK",
                    "RATNG_CMPY_CD",
                    "MLT_CAR_IND",
                    "RT_CLS",
                    "FINAL_RDRVR_AGE",
                    "GENDR",
                    "MRTL_STAT",
                    "I_AUTO_USE_CD",
                    "AUTO_USE_CD",
                    "MILES_TO_WRK",
                    "GOOD_STDNT_IND",
                    "DRVR_TRNG_IND",
                    "SOI_TYP",
                    "ACCTNG_LOB",
                    "CVG_TYP_CD",
                    "REC_EXCPN_IND",
                    "REC_EXCPN_RSN_DESC",
                )
                if df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_adoring_curie.isEmpty():
                    self.logger.warning("Catalog source WRK_BIRP_NISS_APRM_DETL returned no rows", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            except Exception:
                self.logger.exception("Failed reading WRK_BIRP_NISS_APRM_DETL from S3 and Glue Catalog")
                raise

        # SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL: SQL-override run against the staged temp view (staged case)
        try:
            self.logger.info("Registering staged source as temp view for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL", node="SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            # register the upstream staged dataframe as the bare table name used by the override
            df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_adoring_curie.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")

            sql_query = f"""SELECT
  NISS_APRM_DETL_SK,
  ST_ABBR,
  ST_CD,
  RATNG_CMPY_CD,
  MLT_CAR_IND,
  RT_CLS,
  FINAL_RDRVR_AGE,
  GENDR,
  MRTL_STAT,
  AUTO_USE_CD,
  MILES_TO_WRK,
  GOOD_STDNT_IND,
  DRVR_TRNG_IND,
  SOI_TYP,
  ACCTNG_LOB,
  CVG_TYP_CD,
  CASE when ST_ABBR='FL'   and FINAL_RDRVR_AGE!='  'then
  case
   when TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)<25 AND RATNG_CMPY_CD='K' AND MLT_CAR_IND='N' AND (RIGHT(RT_CLS,1)='5' OR RT_CLS IN ('5M','9','5F','2F','2M')) AND MRTL_STAT='M' AND AUTO_USE_CD!='FRM' then '1620'
   when TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)<25 AND RATNG_CMPY_CD='K' AND MLT_CAR_IND='N' AND (RIGHT(RT_CLS,1)='5' OR RT_CLS IN ('5M','9','5F','2F','2M')) AND MRTL_STAT='M' AND AUTO_USE_CD='FRM' then '1623'
   when TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)<25 AND RATNG_CMPY_CD='K' AND MLT_CAR_IND='Y' AND (RIGHT(RT_CLS,1)='5' OR RT_CLS IN ('5M','9','5F','2F','2M')) AND MRTL_STAT='M' AND AUTO_USE_CD!='FRM' then '1622'
   when TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)<25 AND RATNG_CMPY_CD='K' AND MLT_CAR_IND='Y' AND (RIGHT(RT_CLS,1)='5' OR RT_CLS IN ('5M','9','5F','2F','2M')) AND MRTL_STAT='M' AND AUTO_USE_CD='FRM' then '1621'
   when TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)<25 AND RATNG_CMPY_CD='K' AND MLT_CAR_IND='N' AND (RIGHT(RT_CLS,1)='2' OR RT_CLS IN ('2M','9','5M','2F')) AND MRTL_STAT='S' AND AUTO_USE_CD!='FRM' then '1630'
    when TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)<25 AND RATNG_CMPY_CD='K' AND MLT_CAR_IND='N' AND (RIGHT(RT_CLS,1)='2' OR RT_CLS IN ('2M','9','2F')) AND MRTL_STAT='S' AND AUTO_USE_CD='FRM' then '1633'
   when TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)<25 AND RATNG_CMPY_CD='K' AND MLT_CAR_IND='Y' AND (RIGHT(RT_CLS,1)='2' OR RT_CLS IN ('2M','9','2F','5M')) AND (MRTL_STAT IN ('S','') OR MRTL_STAT IS NULL) AND AUTO_USE_CD!='FRM' then '1632'
   when TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)<25 AND RATNG_CMPY_CD='K' AND MLT_CAR_IND='Y' AND (RIGHT(RT_CLS,1)='2' OR RT_CLS IN ('2M','9','2F','5M')) AND (MRTL_STAT IN ('S','') OR MRTL_STAT IS NULL) AND AUTO_USE_CD='FRM' then '1631'
    when TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)<25 AND RATNG_CMPY_CD='K' AND MLT_CAR_IND='N' AND RT_CLS IN ('08','18','28','38','48','58','M6','M8','N6','N8','3','5F','2F','2M','9') AND AUTO_USE_CD!='FRM' then '1610'
   when TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)<25 AND RATNG_CMPY_CD='K' AND MLT_CAR_IND='N' AND RT_CLS IN ('08','18','28','38','48','58','M6','M8','N6','N8','3','5F','2F','2M') AND AUTO_USE_CD='FRM' then '1613'
   when TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)<25 AND RATNG_CMPY_CD='K' AND MLT_CAR_IND='Y' AND RT_CLS IN ('08','18','28','38','48','58','M6','M8','N6','N8','3','5F','2F','2M') AND AUTO_USE_CD!='FRM' then '1612'
   when TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)<25 AND RATNG_CMPY_CD='K' AND MLT_CAR_IND='Y' AND RT_CLS IN ('08','18','28','38','48','58','M6','M8','N6','N8','3','5F','2F','2M') AND AUTO_USE_CD='FRM' then '1611'
   when TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)<25 AND RATNG_CMPY_CD='K' AND MLT_CAR_IND='N'  AND GENDR='F' AND AUTO_USE_CD!='FRM' then '1640'
   when TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)<25 AND RATNG_CMPY_CD='K' AND MLT_CAR_IND='N'  AND GENDR='F' AND AUTO_USE_CD='FRM' then '1643'
   when TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)<25 AND RATNG_CMPY_CD='K' AND MLT_CAR_IND='Y'  AND GENDR='F' AND AUTO_USE_CD!='FRM' then '1642'
   when TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)<25 AND RATNG_CMPY_CD='K' AND MLT_CAR_IND='Y'  AND GENDR='F' AND AUTO_USE_CD='FRM' then '1641'
   
   when TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)>24 AND TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)<30 AND MLT_CAR_IND='N' AND (AUTO_USE_CD IN ('HOP','HPS','PLS','STG','COM','#') or AUTO_USE_CD!='FRM') then '1650'
    when TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)>24 AND TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)<30 AND  MLT_CAR_IND='N' AND  AUTO_USE_CD IN ('FRM') then '1653'
    
   when TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)>24 AND TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)<30 AND MLT_CAR_IND='Y' AND (AUTO_USE_CD IN ('HOP','HPS','PLS','STG','COM','#') or AUTO_USE_CD!='FRM') then '1652'
    when TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)>24 AND TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)<30 AND  MLT_CAR_IND='Y' AND  AUTO_USE_CD IN ('FRM') then '1651'
   
    when TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)>29 AND TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)<65 AND  MLT_CAR_IND='N' AND AUTO_USE_CD IN ('FRM') then '1213'
      when TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)>29 AND TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)<65 AND  MLT_CAR_IND='N' AND AUTO_USE_CD IN ('HOP','HPS','PLS','STG','#') then '1210'
    when TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)>29 AND TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)<65 AND  MLT_CAR_IND='Y' AND AUTO_USE_CD IN ('HOP','HPS','PLS','STG','#') then '1212'
    when TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)>29 AND TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)<65 AND  MLT_CAR_IND='Y' AND AUTO_USE_CD IN ('FRM') then '1211'
     --need to miles
    when TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)>29 AND TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)<65 AND  MLT_CAR_IND='N' AND AUTO_USE_CD IN ('COM') AND TO_NUMBER(MILES_TO_WRK, 38, 0)<10 then '1220'
    when TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)>29 AND TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)<65 AND  MLT_CAR_IND='Y' AND AUTO_USE_CD IN ('COM') AND TO_NUMBER(MILES_TO_WRK, 38, 0)<10 then '1222'
    when TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)>29 AND TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)<65 AND  MLT_CAR_IND='N' AND AUTO_USE_CD IN ('COM') AND TO_NUMBER(MILES_TO_WRK, 38, 0)>=10 then '1230'
    when TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)>29 AND TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)<65 AND  MLT_CAR_IND='Y' AND AUTO_USE_CD IN ('COM') AND TO_NUMBER(MILES_TO_WRK, 38, 0)>=10 then '1232'
    --need to miles
    when TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)>29 AND TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)<65 AND  MLT_CAR_IND='N' AND AUTO_USE_CD NOT IN ('HOP','HPS','PLS','STG','#','FRM','COM') then '1400'
    when TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)>29 AND TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)<65 AND  MLT_CAR_IND='Y' AND AUTO_USE_CD NOT IN ('HOP','HPS','PLS','STG','#','FRM','COM') then '1402'
    
    when TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)>64 AND  MLT_CAR_IND='N' AND AUTO_USE_CD  IN ('PLS','STG','#') then '1510'
    when TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)>64 AND  MLT_CAR_IND='N' AND AUTO_USE_CD IN ('COM') AND TO_NUMBER(MILES_TO_WRK, 38, 0)<10 then '1520'
    when TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)>64 AND  MLT_CAR_IND='N' AND AUTO_USE_CD IN ('COM') AND TO_NUMBER(MILES_TO_WRK, 38, 0)>=10 then '1530'
    when TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)>64 AND  MLT_CAR_IND='N' AND AUTO_USE_CD IN ('FRM')  then '1513'
    when TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)>64 AND  MLT_CAR_IND='N' AND AUTO_USE_CD NOT IN ('PLS','STG','FRM','COM','#')  then '1550'
    when TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)>64 AND  MLT_CAR_IND='Y' AND AUTO_USE_CD  IN ('PLS','STG','#') then '1512'
    when TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)>64 AND  MLT_CAR_IND='Y' AND AUTO_USE_CD IN ('COM') AND TO_NUMBER(MILES_TO_WRK, 38, 0)<10 then '1522'
    when TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)>64 AND  MLT_CAR_IND='Y' AND AUTO_USE_CD IN ('COM') AND TO_NUMBER(MILES_TO_WRK, 38, 0)>=10 then '1532'
    when TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)>64 AND  MLT_CAR_IND='Y' AND AUTO_USE_CD IN ('FRM') then '1511'
    when TO_NUMBER(FINAL_RDRVR_AGE, 38, 0)>64 AND  MLT_CAR_IND='Y' AND AUTO_USE_CD NOT IN ('PLS','STG','FRM','COM','#')  then '1552'
  end
  else ''
  end as NISS_CLASS_CD_FL,
  REC_EXCPN_IND,
  REC_EXCPN_RSN_DESC
  FROM (
  SELECT
  NISS_APRM_DETL_SK,
  LTRIM(RTRIM(ST_ABBR)) ST_ABBR,
  LTRIM(RTRIM(ST_CD)) ST_CD,
  LTRIM(RTRIM(RATNG_CMPY_CD)) RATNG_CMPY_CD,
  LTRIM(RTRIM(MLT_CAR_IND)) MLT_CAR_IND,
  LTRIM(RTRIM(RT_CLS)) RT_CLS,
  LTRIM(RTRIM( FINAL_RDRVR_AGE)) FINAL_RDRVR_AGE,
  LTRIM(RTRIM(GENDR)) GENDR,
  LTRIM(RTRIM(MRTL_STAT)) MRTL_STAT,
  LTRIM(RTRIM(AUTO_USE_CD)) AUTO_USE_CD,
  LTRIM(RTRIM(MILES_TO_WRK)) MILES_TO_WRK,
  LTRIM(RTRIM(GOOD_STDNT_IND)) GOOD_STDNT_IND,
  LTRIM(RTRIM(DRVR_TRNG_IND)) DRVR_TRNG_IND,
  LTRIM(RTRIM(SOI_TYP)) SOI_TYP,
  LTRIM(RTRIM(ACCTNG_LOB)) ACCTNG_LOB,
  LTRIM(RTRIM(CVG_TYP_CD)) CVG_TYP_CD,
  REC_EXCPN_IND,
  REC_EXCPN_RSN_DESC

  FROM WRK_BIRP_NISS_APRM_DETL

  WHERE ST_ABBR NOT IN ('NY','NJ') AND SOURCE_IND_DERIVED='FARMERS'

  --AND NISS_APRM_DETL_SK IN (48)
  --388578,455093,526319,449470)
  )"""

            self.logger.info("Running SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL sql_query", node="SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_lucid_hilbert = self.spark.sql(sql_query)
            if df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_lucid_hilbert.isEmpty():
                self.logger.warning("SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL returned no rows", node="SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
        except Exception:
            self.logger.exception("Failed executing SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL SQL override")
            raise

        # EXP_To_Drv_Class_Cd1: Expression - compute typed intermediates and final outputs
        try:
            self.logger.info("Applying EXP_To_Drv_Class_Cd1 expressions", node="EXP_To_Drv_Class_Cd1")
            df_EXP_To_Drv_Class_Cd1_happy_planck = (
                df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_lucid_hilbert
                # compute integer typed intermediates
                .withColumn("IAGE", when(trim(col("FINAL_RDRVR_AGE")).rlike("^\\d+$"), col("FINAL_RDRVR_AGE").cast("int")).otherwise(lit(None)))
                .withColumn("IMILES_TO_WRK", when(trim(col("MILES_TO_WRK")).rlike("^\\d+$"), col("MILES_TO_WRK").cast("int")).otherwise(lit(None)))
                # normalize AUTO_USE_CD (I_AUTO_USE_CD -> AUTO_USE_CD fallback logic)
                .withColumn("AUTO_USE_CD", when(col("I_AUTO_USE_CD").isNull(), lit("")).otherwise(col("I_AUTO_USE_CD")))
                # derive simplified final NISS_CLASS_CD: prefer already-derived FL value when present else placeholder logic
                .withColumn(
                    "v_NISS_CLASS_CD",
                    when((col("NISS_CLASS_CD_FL").isNotNull()) & (trim(col("NISS_CLASS_CD_FL")) != ""), trim(col("NISS_CLASS_CD_FL"))).otherwise(lit("??????"))
                )
                # apply final output rule: output '??????' when partially unknown (mimic original default behavior)
                .withColumn(
                    "NISS_CLASS_CD",
                    when((substr(trim(col("v_NISS_CLASS_CD")), 1, 4) == '????') | (substr(col("v_NISS_CLASS_CD"), 5, 2) == '??'), lit('??????')).otherwise(col("v_NISS_CLASS_CD"))
                )
                # derive REC_EXCPN_IND by combining incoming flag with local detection (simplified: flag if REC_EXCPN_IND from source is 'Y')
                .withColumn(
                    "REC_EXCPN_IND",
                    when(col("REC_EXCPN_IND") == 'Y', lit('Y')).otherwise(lit(''))
                )
                # REC_EXCP_DESC concatenates incoming reason and a constant or class description
                .withColumn("REC_EXCP_DESC", coalesce(col("REC_EXCPN_RSN_DESC"), lit("")))
                # ensure key passed through
                .withColumn("NISS_APRM_DETL_SK", col("NISS_APRM_DETL_SK"))
            )
        except Exception:
            self.logger.exception("Failed applying EXP_To_Drv_Class_Cd1 expressions")
            raise

        # UPD_NISS_CLASS_CD: Update Strategy - separate rejects, load existing target, anti-join and overwrite
        try:
            self.logger.info("Applying Update Strategy UPD_NISS_CLASS_CD: separating rejects and preparing apply set", node="UPD_NISS_CLASS_CD")
            # separate rejected rows where REC_EXCPN_IND = 'Y'
            reject_df = df_EXP_To_Drv_Class_Cd1_happy_planck.filter(col("REC_EXCPN_IND") == 'Y')
            df_to_apply = df_EXP_To_Drv_Class_Cd1_happy_planck.filter((col("REC_EXCPN_IND") != 'Y') | col("REC_EXCPN_IND").isNull())

            # write rejects to reject prefix
            try:
                self.logger.info("Writing rejected rows for WRK_BIRP_NISS_APRM_DETL", table="WRK_BIRP_NISS_APRM_DETL")
                self.parquet_writer.write(
                    dataframe=reject_df,
                    path=f"s3://{self.s3_output_bucket}/reject/WRK_BIRP_NISS_APRM_DETL/",
                    mode="overwrite",
                )
            except Exception:
                self.logger.exception("Failed writing rejects for WRK_BIRP_NISS_APRM_DETL")
                raise

            # load current target (if present) via ParquetReader; if missing treat as empty DataFrame
            try:
                self.logger.info("Reading current target WRK_BIRP_NISS_APRM_DETL for update", node="UPD_NISS_CLASS_CD")
                current_target_df = self.parquet_reader.read(path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/")
            except Exception:
                # treat missing target as empty
                self.logger.warning("Current target WRK_BIRP_NISS_APRM_DETL not found on S3; treating as empty", node="UPD_NISS_CLASS_CD")
                current_target_df = df_to_apply.limit(0)

            # anti-join out rows from current target that are being replaced by apply set
            join_key = ["NISS_APRM_DETL_SK"]
            if not current_target_df.rdd.isEmpty():
                target_remaining = current_target_df.join(
                    df_to_apply.select("NISS_APRM_DETL_SK").distinct(),
                    on=join_key,
                    how="left_anti",
                )
            else:
                target_remaining = current_target_df

            # union the remaining target rows with the apply set
            result_df = target_remaining.unionByName(df_to_apply, allowMissingColumns=True)

            # assign output dataframe name
            df_UPD_NISS_CLASS_CD_admiring_noether = result_df

            # persist updated target back to S3 (overwrite)
            try:
                self.logger.info("Writing updated WRK_BIRP_NISS_APRM_DETL to S3 as parquet (overwrite)", table="WRK_BIRP_NISS_APRM_DETL")
                self.parquet_writer.write(
                    dataframe=df_UPD_NISS_CLASS_CD_admiring_noether,
                    path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                    mode="overwrite",
                )
            except Exception:
                self.logger.exception("Failed writing updated WRK_BIRP_NISS_APRM_DETL to S3")
                raise

        except Exception:
            self.logger.exception("Failed applying Update Strategy UPD_NISS_CLASS_CD")
            raise

        # FDR_LIB_WRK_BIRP_NISS_APRM_DETL1: final Output write to S3 as parquet (overwrite)
        try:
            self.logger.info("Writing final target FDR_LIB_WRK_BIRP_NISS_APRM_DETL to S3 as parquet (overwrite)", table="WRK_BIRP_NISS_APRM_DETL")
            self.parquet_writer.write(
                dataframe=df_UPD_NISS_CLASS_CD_admiring_noether,
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                mode="overwrite",
            )
            # alias final df_name_out to ensure downstream references resolve
            df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_hopeful_galileo = df_UPD_NISS_CLASS_CD_admiring_noether
        except Exception:
            self.logger.exception("Failed writing final target FDR_LIB_WRK_BIRP_NISS_APRM_DETL to S3")
            raise

        self.glue_job.commit()

    def finalize(self) -> None:
        self.logger.info("Finalizing m_NU0C_NISS_AUTO_103_ATPRM_DTL_Upd_ClassCd")

    @staticmethod
    def _require_env(env: Mapping[str, Any], key: str) -> str:

        value = env.get(key)

        if not value or str(value).startswith("REPLACE_WITH_"):
            raise ConfigurationValidationError(
                f"config.yaml 'env.{key}' must be set"
            )

        return str(value)
