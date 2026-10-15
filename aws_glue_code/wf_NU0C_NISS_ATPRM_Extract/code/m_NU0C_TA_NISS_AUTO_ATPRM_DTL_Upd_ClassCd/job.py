import sys
from typing import Any, Mapping

from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import DataFrame

from glue_lib.connector import ParquetReader, ParquetWriter
from glue_lib.exceptions import ConfigurationValidationError
from glue_lib.job.base import BaseJob
from pyspark.sql.functions import col, lit, when, trim, expr, concat
from glue_lib.connector.snowflake import SnowflakeReader


# Informatica mapping m_NU0C_TA_NISS_AUTO_ATPRM_DTL_Upd_ClassCd.
# Every mapping node runs in order inside run().
class Nu0cTaNissAutoAtprmDtlUpdClassCdJob(BaseJob):

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

        SNOWFLAKE_CONNECTION_FDWS_FDRWRK = self._require_env(self.env, "SNOWFLAKE_CONNECTION_FDWS_FDRWRK")

        # SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL1: SQL override (staged-preferring)
        sql_query_original = f"""SELECT
 NISS_APRM_DETL_SK,
LTRIM(RTRIM(ST_CD)) ST_CD,
 LTRIM(RTRIM(ST_ABBR)) ST_ABBR,
 LTRIM(RTRIM(ACCTNG_LOB)) ACCTNG_LOB,
LTRIM(RTRIM(CVG_TYP_CD)),
 LTRIM(RTRIM(RATNG_CMPY_CD)) RATNG_CMPY_CD,
 LTRIM(RTRIM(MLT_CAR_IND)) MLT_CAR_IND,
 LTRIM(RTRIM( FINAL_RDRVR_AGE)) FINAL_RDRVR_AGE,
 LTRIM(RTRIM(GENDR)) GENDR,
 LTRIM(RTRIM(MRTL_STAT)) MRTL_STAT,
 LTRIM(RTRIM(AUTO_USE_CD)) AUTO_USE_CD,
 LTRIM(RTRIM(SOI_TYP)) SOI_TYP,
 NISS_CLASS_CD,
 REC_DROP_IND,
 REC_DROP_RSN_DESC,
 REC_EXCPN_IND,
 REC_EXCPN_RSN_DESC,
 LTRIM(RTRIM(PRINCIPAL_OPRT)) PRINCIPAL_OPRT,
 SOURCE_IND_DERIVED

 FROM FDR.WRK_BIRP_NISS_APRM_DETL

 WHERE ST_ABBR NOT IN ('NY','NJ') AND 
SOURCE_IND_DERIVED='TOGGLE AUTO'"""

        # Attempt to use staged parquet for WRK_BIRP_NISS_APRM_DETL first, fall back to Snowflake if not present
        try:
            self.logger.info("Attempting staged parquet read for WRK_BIRP_NISS_APRM_DETL before falling back to Snowflake", node="SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL1")
            df_stage = self.parquet_reader.read(path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/")
            # register temp view for rewrite
            df_stage.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")
            # rewrite query to reference the bare staged view name
            sql_query = sql_query_original.replace("FROM FDR.WRK_BIRP_NISS_APRM_DETL", "FROM WRK_BIRP_NISS_APRM_DETL")
            try:
                self.logger.info("Running rewritten SQL override against staged temp view WRK_BIRP_NISS_APRM_DETL", node="SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL1")
                df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL1_romantic_planck = self.spark.sql(sql_query)
                if df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL1_romantic_planck.isEmpty():
                    self.logger.warning("Source SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL1 returned no rows after staged SQL", node="SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL1")
            except Exception:
                self.logger.exception("Failed executing rewritten staged SQL for SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL1")
                raise
        except Exception:
            # staged read failed; fall back to Snowflake (log the branch)
            try:
                self.logger.info("Staged parquet read failed or missing; falling back to Snowflake for SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL1", node="SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL1")
                sql_query = sql_query_original
                df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL1_romantic_planck = SnowflakeReader(self.glue_context).read(
                    connection_name=SNOWFLAKE_CONNECTION_FDWS_FDRWRK,
                    query=sql_query,
                )
                if df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL1_romantic_planck.isEmpty():
                    self.logger.warning("Source SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL1 returned no rows from Snowflake", node="SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL1")
            except Exception:
                self.logger.exception("Failed reading SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL1 from Snowflake")
                raise

        # EXP_PASS_THROUGH: explicit pass-through of listed ports
        try:
            self.logger.info("Applying EXP_PASS_THROUGH passthrough projection", node="EXP_PASS_THROUGH")
            df_EXP_PASS_THROUGH_amazing_ramanujan = df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL1_romantic_planck.selectExpr(
                "NISS_APRM_DETL_SK",
                "ST_CD",
                "ST_ABBR",
                "ACCTNG_LOB",
                "RATNG_CMPY_CD",
                "MLT_CAR_IND",
                "AGE",
                "GENDR",
                "MRTL_STAT",
                "AUTO_USE_CD",
                "SOI_TYP",
                "NISS_CLASS_CD",
                "REC_DROP_IND",
                "REC_DROP_RSN_DESC",
                "REC_EXCPN_IND",
                "REC_EXCPN_RSN_DESC",
                "PRINCIPAL_OPRT",
                "SOURCE_IND_DERIVED",
                "CVG_TYP_CD"
            )
        except Exception:
            self.logger.exception("Failed applying EXP_PASS_THROUGH projection")
            raise

        # EXP_DERV_CLASS_CD: compute many intermediate vars and final outputs (NISS_CLASS_CD, REC_EXCPN_IND, REC_EXCP_DESC)
        try:
            self.logger.info("Applying EXP_DERV_CLASS_CD expressions", node="EXP_DERV_CLASS_CD")
            df_temp = df_EXP_PASS_THROUGH_amazing_ramanujan

            # IAGE: integer conversion of AGE
            df_temp = df_temp.withColumn("IAGE", expr("CAST(AGE AS INT)"))

            # normalize AUTO_USE_CD (treat null as empty string)
            df_temp = df_temp.withColumn("AUTO_USE_CD", when(col("AUTO_USE_CD").isNull(), lit("")).otherwise(col("AUTO_USE_CD")))

            # v_CLASS_CD_Indemnity (DECODE -> CASE WHEN)
            df_temp = df_temp.withColumn(
                "v_CLASS_CD_Indemnity",
                expr(
                    "CASE WHEN (ST_ABBR NOT IN ('NY','NJ','NC','AR','SD','PA') AND ACCTNG_LOB = '1923D') THEN '941400' "
            "WHEN (ST_ABBR = 'AR' AND ACCTNG_LOB = '191') THEN '946400' "
            "WHEN (ST_ABBR = 'SD' AND ACCTNG_LOB = '1923D') THEN '946400' "
            "WHEN (ST_ABBR = 'PA' AND ACCTNG_LOB = '191' AND CVG_TYP_CD IN ('35043','35041','30005','34008','30016')) THEN '999700' "
            "ELSE '??????' END"
                )
            )

            # v_CLASS_CODE_TAuto (nested IIFs -> CASE WHEN)
            df_temp = df_temp.withColumn(
                "v_CLASS_CODE_TAuto",
                expr(
                    "CASE WHEN ST_ABBR NOT IN ('NY','NJ','NC','MI','MT','PA','FL','NH','LA') THEN ("
            "CASE WHEN MLT_CAR_IND = 'N' AND IAGE >= 25 AND IAGE < 65 AND AUTO_USE_CD = 'PLS' THEN '1200' "
            "WHEN MLT_CAR_IND = 'Y' AND IAGE >= 25 AND IAGE < 65 AND AUTO_USE_CD = 'PLS' THEN '1202' "
            "WHEN MLT_CAR_IND = 'N' AND IAGE >= 25 AND IAGE < 65 AND AUTO_USE_CD = 'TPF' THEN '1400' "
            "WHEN MLT_CAR_IND = 'Y' AND IAGE >= 25 AND IAGE < 65 AND AUTO_USE_CD = 'TPF' THEN '1402' "
            "WHEN MLT_CAR_IND = 'N' AND IAGE < 25 AND GENDR = 'M' AND MRTL_STAT = 'M' THEN '1620' "
            "WHEN MLT_CAR_IND = 'Y' AND IAGE < 25 AND GENDR = 'M' AND MRTL_STAT = 'M' THEN '1622' "
            "WHEN MLT_CAR_IND = 'N' AND IAGE < 25 AND GENDR = 'M' THEN '1600' "
            "WHEN MLT_CAR_IND = 'Y' AND IAGE < 25 AND GENDR = 'M' THEN '1602' "
            "WHEN MLT_CAR_IND = 'N' AND IAGE < 25 AND GENDR = 'F' THEN '1640' "
            "WHEN MLT_CAR_IND = 'Y' AND IAGE < 25 AND GENDR = 'F' THEN '1642' "
            "WHEN MLT_CAR_IND = 'N' AND IAGE < 25 AND (GENDR IN ('U','') OR GENDR = 'X' OR GENDR IS NULL) THEN '1600' "
            "WHEN MLT_CAR_IND = 'Y' AND IAGE < 25 AND (GENDR IN ('U','') OR GENDR = 'X' OR GENDR IS NULL) THEN '1602' "
            "WHEN IAGE >= 65 THEN '1700' ELSE '' END) "
            "ELSE (CASE WHEN IAGE >= 65 THEN '1700' ELSE '' END) END"
                )
            )

            # Normalize final TAUTO code
            df_temp = df_temp.withColumn(
                "v_CLASS_CODE_TAuto_Final",
                expr("CASE WHEN v_CLASS_CODE_TAuto != '' THEN v_CLASS_CODE_TAuto ELSE '????' END")
            )

            # v_CLASS_CODE_Secondary
            df_temp = df_temp.withColumn(
                "v_CLASS_CODE_Secondary",
                expr(
                    "CASE WHEN ST_ABBR = 'PA' AND MLT_CAR_IND = 'N' THEN '10' "
            "WHEN ST_ABBR = 'PA' AND MLT_CAR_IND = 'Y' THEN '20' "
            "WHEN ST_ABBR NOT IN ('NY','NJ','NC','PA') THEN '00' "
            "ELSE '??' END"
                )
            )

            # v_CLASS_CD_Misc_1 (LA rules)
            df_temp = df_temp.withColumn(
                "v_CLASS_CD_Misc_1",
                expr(
                    "CASE WHEN ST_ABBR = 'LA' AND IAGE >= 25 AND IAGE < 65 AND SOI_TYP = '07' THEN '950900' "
            "WHEN ST_ABBR = 'LA' AND IAGE >= 25 AND IAGE < 65 AND SOI_TYP = '02' THEN '939200' "
            "WHEN ST_ABBR = 'LA' AND IAGE >= 25 AND IAGE < 65 AND SOI_TYP = '09' THEN '952900' "
            "WHEN ST_ABBR = 'LA' AND IAGE >= 25 AND IAGE < 65 AND SOI_TYP = '03' THEN '949200' "
            "WHEN ST_ABBR = 'LA' AND IAGE >= 25 AND IAGE < 65 AND SOI_TYP = '04' THEN '934000' "
            "WHEN ST_ABBR = 'LA' AND IAGE >= 25 AND IAGE < 65 AND SOI_TYP = '08' THEN '941300' "
            "WHEN ST_ABBR = 'LA' AND IAGE > 64 AND SOI_TYP = '07' THEN '950800' "
            "WHEN ST_ABBR = 'LA' AND IAGE > 64 AND SOI_TYP = '02' THEN '958800' "
            "WHEN ST_ABBR = 'LA' AND IAGE > 64 AND SOI_TYP = '09' THEN '952800' "
            "WHEN ST_ABBR = 'LA' AND IAGE > 64 AND SOI_TYP = '03' THEN '959800' "
            "WHEN ST_ABBR = 'LA' AND IAGE > 64 AND SOI_TYP = '04' THEN '954800' "
            "WHEN ST_ABBR = 'LA' AND IAGE > 64 AND SOI_TYP = '08' THEN '951800' "
            "WHEN ST_ABBR = 'LA' AND NOT SOI_TYP IN ('02','03','04','07','08','09') THEN 'E1212' ELSE '' END"
                )
            )

            # v_CLASS_CD_Misc_2 (MI/MT/PA <25 and SOI_TYPE)
            df_temp = df_temp.withColumn(
                "v_CLASS_CD_Misc_2",
                expr(
                    "CASE WHEN ST_ABBR IN ('MI','MT','PA') AND IAGE < 25 AND SOI_TYP = '07' THEN '950700' "
            "WHEN ST_ABBR IN ('MI','MT','PA') AND IAGE < 25 AND SOI_TYP = '02' THEN '958700' "
            "WHEN ST_ABBR IN ('MI','MT','PA') AND IAGE < 25 AND SOI_TYP = '09' THEN '952700' "
            "WHEN ST_ABBR IN ('MI','MT','PA') AND IAGE < 25 AND SOI_TYP = '03' THEN '959700' "
            "WHEN ST_ABBR IN ('MI','MT','PA') AND IAGE < 25 AND SOI_TYP = '04' THEN '954700' "
            "WHEN ST_ABBR IN ('MI','MT','PA') AND IAGE < 25 AND SOI_TYP = '08' THEN '951700' "
            "WHEN ST_ABBR = 'PA' AND NOT SOI_TYP IN ('01','05','30','02','03','04','07','08','09') THEN 'E88712' ELSE '' END"
                )
            )

            # v_CLASS_CD_Misc_3 (other states <25 with gender splits)
            df_temp = df_temp.withColumn(
                "v_CLASS_CD_Misc_3",
                expr(
                    "CASE WHEN ST_ABBR NOT IN ('NY','NC','NJ','MI','MT','PA') AND IAGE < 25 AND SOI_TYP = '07' AND GENDR = 'M' THEN '950700' "
            "WHEN ST_ABBR NOT IN ('NY','NC','NJ','MI','MT','PA') AND IAGE < 25 AND SOI_TYP = '07' AND GENDR = 'F' THEN '950900' "
            "WHEN ST_ABBR NOT IN ('NY','NC','NJ','MI','MT','PA') AND IAGE < 25 AND SOI_TYP = '02' AND GENDR = 'M' THEN '958700' "
            "WHEN ST_ABBR NOT IN ('NY','NC','NJ','MI','MT','PA') AND IAGE < 25 AND SOI_TYP = '02' AND GENDR = 'F' THEN '939200' "
            "WHEN ST_ABBR NOT IN ('NY','NC','NJ','MI','MT','PA') AND IAGE < 25 AND SOI_TYP = '09' AND GENDR = 'M' THEN '952700' "
            "WHEN ST_ABBR NOT IN ('NY','NC','NJ','MI','MT','PA') AND IAGE < 25 AND SOI_TYP = '09' AND GENDR = 'F' THEN '952900' "
            "WHEN ST_ABBR NOT IN ('NY','NC','NJ','MI','MT','PA') AND IAGE < 25 AND SOI_TYP = '03' AND GENDR = 'M' THEN '959700' "
            "WHEN ST_ABBR NOT IN ('NY','NC','NJ','MI','MT','PA') AND IAGE < 25 AND SOI_TYP = '03' AND GENDR = 'F' THEN '949200' "
            "WHEN ST_ABBR NOT IN ('NY','NC','NJ','MI','MT','PA') AND IAGE < 25 AND SOI_TYP = '04' AND GENDR = 'M' THEN '954700' "
            "WHEN ST_ABBR NOT IN ('NY','NC','NJ','MI','MT','PA') AND IAGE < 25 AND SOI_TYP = '04' AND GENDR = 'F' THEN '934000' "
            "WHEN ST_ABBR NOT IN ('NY','NC','NJ','MI','MT','PA') AND IAGE < 25 AND SOI_TYP = '08' AND GENDR = 'M' THEN '951700' "
            "WHEN ST_ABBR NOT IN ('NY','NC','NJ','MI','MT','PA') AND IAGE < 25 AND SOI_TYP = '08' AND GENDR = 'F' THEN '941300' ELSE '' END"
                )
            )

            # v_CLASS_CD_Misc_4 (other states >=25)
            df_temp = df_temp.withColumn(
                "v_CLASS_CD_Misc_4",
                expr(
                    "CASE WHEN ST_ABBR NOT IN ('NY','NC','NJ','LA') AND IAGE >= 25 AND SOI_TYP = '07' THEN '950900' "
            "WHEN ST_ABBR NOT IN ('NY','NC','NJ','LA') AND IAGE >= 25 AND SOI_TYP = '02' THEN '939200' "
            "WHEN ST_ABBR NOT IN ('NY','NC','NJ','LA') AND IAGE >= 25 AND SOI_TYP = '09' THEN '952900' "
            "WHEN ST_ABBR NOT IN ('NY','NC','NJ','LA') AND IAGE >= 25 AND SOI_TYP = '03' THEN '949200' "
            "WHEN ST_ABBR NOT IN ('NY','NC','NJ','LA') AND IAGE >= 25 AND SOI_TYP = '04' THEN '934000' "
            "WHEN ST_ABBR NOT IN ('NY','NC','NJ','LA') AND IAGE >= 25 AND SOI_TYP = '08' THEN '941300' ELSE '' END"
                )
            )

            # v_CLASS_CD_Misc_5 (other combinations - ACCTNG_LOB and CVG_TYP_CD)
            df_temp = df_temp.withColumn(
                "v_CLASS_CD_Misc_5",
                expr(
                    "CASE WHEN ST_ABBR NOT IN ('NY','NC','NJ') AND ACCTNG_LOB IN ('211CC','211CL','2110F','211MH','211TW') AND SOI_TYP = '05' THEN '933200' "
            "WHEN ST_ABBR NOT IN ('NY','NC','NJ') AND ACCTNG_LOB IN ('211CC','211CL','2110F','211MH','211TW') AND SOI_TYP = '30' THEN '933100' "
            "WHEN ST_ABBR != 'NH' AND SOI_TYP != '01' AND CVG_TYP_CD = '40030' THEN '958000' "
            "WHEN ST_ABBR != 'NH' AND SOI_TYP != '01' AND CVG_TYP_CD = '40031' THEN '949000' "
            "WHEN ST_ABBR NOT IN ('NY','NC','NJ','PA') AND SOI_TYP NOT IN ('01','05','30') THEN 'E1202' ELSE '' END"
                )
            )

            # v_CLASS_CD_Misc_6_FL (FL-specific complex rules)
            df_temp = df_temp.withColumn(
                "v_CLASS_CD_Misc_6_FL",
                expr(
                    "CASE WHEN ST_ABBR = 'FL' THEN (CASE "
            "WHEN SOI_TYP = '07' AND IAGE < 25 AND GENDR = 'M' THEN '950700' "
            "WHEN SOI_TYP = '07' AND IAGE < 25 AND GENDR = 'F' THEN '950900' "
            "WHEN SOI_TYP = '07' AND IAGE < 25 AND NOT (GENDR IN ('F','M')) THEN '950700' "
            "WHEN SOI_TYP = '07' AND IAGE >= 25 AND IAGE < 65 THEN '950900' "
            "WHEN SOI_TYP = '07' AND IAGE > 64 THEN '950800' "
            "WHEN SOI_TYP = '02' AND IAGE < 25 AND GENDR = 'M' THEN '958700' "
            "WHEN SOI_TYP = '02' AND IAGE < 25 AND GENDR = 'F' THEN '939200' "
            "WHEN SOI_TYP = '02' AND IAGE < 25 AND NOT (GENDR IN ('F','M')) THEN '958700' "
            "WHEN SOI_TYP = '02' AND IAGE >= 25 AND IAGE < 65 THEN '939200' "
            "WHEN SOI_TYP = '02' AND IAGE > 64 THEN '958800' "
            "WHEN SOI_TYP = '09' AND IAGE < 25 AND GENDR = 'M' THEN '952700' "
            "WHEN SOI_TYP = '09' AND IAGE < 25 AND GENDR = 'F' THEN '952900' "
            "WHEN SOI_TYP = '09' AND IAGE < 25 AND NOT (GENDR IN ('F','M')) THEN '952700' "
            "WHEN SOI_TYP = '09' AND IAGE >= 25 AND IAGE < 65 THEN '952900' "
            "WHEN SOI_TYP = '09' AND IAGE > 64 THEN '952800' "
            "WHEN SOI_TYP = '03' AND IAGE < 25 AND GENDR = 'M' THEN '959700' "
            "WHEN SOI_TYP = '03' AND IAGE < 25 AND GENDR = 'F' THEN '949200' "
            "WHEN SOI_TYP = '03' AND IAGE < 25 AND NOT (GENDR IN ('F','M')) THEN '959700' "
            "WHEN SOI_TYP = '03' AND IAGE >= 25 AND IAGE < 65 THEN '949200' "
            "WHEN SOI_TYP = '03' AND IAGE > 64 THEN '959800' "
            "WHEN SOI_TYP = '04' AND IAGE < 25 AND GENDR = 'M' THEN '954700' "
            "WHEN SOI_TYP = '04' AND IAGE < 25 AND GENDR = 'F' THEN '934000' "
            "WHEN SOI_TYP = '04' AND IAGE < 25 AND NOT (GENDR IN ('F','M')) THEN '954700' "
            "WHEN SOI_TYP = '04' AND IAGE >= 25 AND IAGE < 65 THEN '934000' "
            "WHEN SOI_TYP = '04' AND IAGE > 64 THEN '954800' "
            "WHEN SOI_TYP = '08' AND IAGE < 25 AND GENDR = 'M' THEN '951700' "
            "WHEN SOI_TYP = '08' AND IAGE < 25 AND GENDR = 'F' THEN '941300' "
            "WHEN SOI_TYP = '08' AND IAGE < 25 AND NOT (GENDR IN ('F','M')) THEN '951700' "
            "WHEN SOI_TYP = '08' AND IAGE >= 25 AND IAGE < 65 THEN '941300' "
            "WHEN SOI_TYP = '08' AND IAGE > 64 THEN '951800' "
            "WHEN ACCTNG_LOB IN ('211CC','211CL','2110F','211MH','211TW') AND SOI_TYP = '05' THEN '933200' "
            "WHEN ACCTNG_LOB IN ('211CC','211CL','2110F','211MH','211TW') AND SOI_TYP = '30' THEN '933100' "
            "WHEN NOT SOI_TYP IN ('01','09','08','02','03','04','05','30','07') THEN 'E9492' ELSE '' END) ELSE '' END"
                )
            )

            # v_CLASS_CD_Misc_Final: first non-empty misc candidate
            df_temp = df_temp.withColumn(
                "v_CLASS_CD_Misc_Final",
                expr(
                    "CASE WHEN v_CLASS_CD_Misc_1 != '' THEN v_CLASS_CD_Misc_1 "
            "WHEN v_CLASS_CD_Misc_2 != '' THEN v_CLASS_CD_Misc_2 "
            "WHEN v_CLASS_CD_Misc_3 != '' THEN v_CLASS_CD_Misc_3 "
            "WHEN v_CLASS_CD_Misc_4 != '' THEN v_CLASS_CD_Misc_4 "
            "WHEN v_CLASS_CD_Misc_5 != '' THEN v_CLASS_CD_Misc_5 "
            "WHEN v_CLASS_CD_Misc_6_FL != '' THEN v_CLASS_CD_Misc_6_FL ELSE '??????' END"
                )
            )

            # v_NISS_CLASS_CD: pick indemnity, TAUTO+secondary, or misc final
            df_temp = df_temp.withColumn(
                "v_NISS_CLASS_CD",
                expr(
                    "CASE WHEN v_CLASS_CD_Indemnity != '??????' THEN v_CLASS_CD_Indemnity "
            "WHEN SOI_TYP = '01' THEN CONCAT(v_CLASS_CODE_TAuto_Final, v_CLASS_CODE_Secondary) "
            "WHEN SOI_TYP != '01' THEN v_CLASS_CD_Misc_Final ELSE '??????' END"
                )
            )

            # NISS_CLASS_CD final output with validation
            df_temp = df_temp.withColumn(
                "NISS_CLASS_CD",
                expr(
                    "CASE WHEN substr(trim(v_NISS_CLASS_CD),1,4) = '????' OR substr(v_NISS_CLASS_CD,5,2) = '??' THEN '??????' ELSE v_NISS_CLASS_CD END"
                )
            )

            # REC_EXCP_IND and REC_EXCP_DESC_CLASS and REC_EXCPN_IND/REC_EXCP_DESC
            df_temp = df_temp.withColumn(
                "REC_EXCP_IND",
                expr("CASE WHEN substr(trim(v_CLASS_CODE_TAuto_Final),1,1) = 'E' OR substr(trim(v_CLASS_CD_Misc_Final),1,1) = 'E' THEN 'Y' ELSE '' END")
            )

            df_temp = df_temp.withColumn(
                "REC_EXCP_DESC_CLASS",
                expr("CASE WHEN substr(trim(v_CLASS_CODE_TAuto_Final),1,1) = 'E' OR substr(trim(v_CLASS_CD_Misc_Final),1,1) = 'E' THEN 'DEFAULT_CLASS_CD' ELSE '' END")
            )

            df_temp = df_temp.withColumn(
                "REC_EXCPN_IND",
                expr("CASE WHEN i_REC_EXCPN_IND = 'Y' OR REC_EXCP_IND = 'Y' THEN 'Y' ELSE '' END")
            )

            df_temp = df_temp.withColumn(
                "REC_EXCP_DESC",
                concat(col("REC_EXCPN_RSN_DESC_ZIP"), col("REC_EXCP_DESC_CLASS"))
            )

            # Project the node's INPUT/OUTPUT and OUTPUT ports explicitly
            df_EXP_DERV_CLASS_CD_gentle_curie = df_temp.select(
                col("NISS_APRM_DETL_SK"),
                col("ST_CD"),
                col("ST_ABBR"),
                col("ACCTNG_LOB"),
                col("CVG_TYP_CD"),
                col("RATNG_CMPY_CD"),
                col("MLT_CAR_IND"),
                col("AGE"),
                col("GENDR"),
                col("MRTL_STAT"),
                col("AUTO_USE_CD").alias("i_AUTO_USE_CD"),
                col("SOI_TYP"),
                col("i_REC_EXCPN_IND"),
                col("REC_EXCPN_RSN_DESC_ZIP"),
                col("NISS_CLASS_CD"),
                col("REC_EXCPN_IND"),
                col("REC_EXCP_DESC")
            )

        except Exception:
            self.logger.exception("Failed applying EXP_DERV_CLASS_CD expressions")
            raise

        # UPD_NISS_CLASS_CD: derive dd_op and route REJECTs to reject sink
        try:
            self.logger.info("Applying UPD_NISS_CLASS_CD update-strategy: marking dd_op and routing rejects", node="UPD_NISS_CLASS_CD")
            # default assumption: all rows are UPDATE per plan
            df_marked = df_EXP_DERV_CLASS_CD_gentle_curie.withColumn("dd_op", lit("UPDATE"))

            # route REJECT rows (none expected with default), write if any
            df_rejects = df_marked.filter(col("dd_op") == 'REJECT')
            if not df_rejects.isEmpty():
                try:
                    self.logger.info("Writing reject rows for WRK_BIRP_NISS_APRM_DETL to reject prefix", node="UPD_NISS_CLASS_CD")
                    self.parquet_writer.write(
                        dataframe=df_rejects.withColumn("error_reason", lit("DD_REJECT")),
                        path=f"s3://{self.s3_output_bucket}/reject/WRK_BIRP_NISS_APRM_DETL/",
                        mode="overwrite",
                    )
                except Exception:
                    self.logger.exception("Failed writing reject rows for UPD_NISS_CLASS_CD")
                    raise

            # drop rejects from apply
            df_UPD_NISS_CLASS_CD_blissful_ramanujan = df_marked.filter(col("dd_op") != 'REJECT')

        except Exception:
            self.logger.exception("Failed applying UPD_NISS_CLASS_CD update strategy")
            raise

        # FDR_LIB_WRK_BIRP_NISS_APRM_DETL: apply load-modify-store-back to the WRK target on S3
        try:
            self.logger.info("Applying load-modify-store-back for WRK_BIRP_NISS_APRM_DETL (full read-and-rewrite) - note this is expensive for large targets", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            # load existing target
            df_existing = self.parquet_reader.read(path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/")

            # determine keys being changed (UPDATE/INSERT rows)
            df_changed = df_UPD_NISS_CLASS_CD_blissful_ramanujan.filter(col("dd_op").isin("INSERT", "UPDATE"))

            # anti-join to remove existing rows that will be updated/deleted
            df_existing_survivors = df_existing.join(
                df_changed.select(col("NISS_APRM_DETL_SK").alias("chg_sk")).distinct(),
                df_existing["NISS_APRM_DETL_SK"] == col("chg_sk"),
                how="left_anti",
            )

            # union survivors with incoming insert/update rows (for UPDATE we assume incoming row is the new full row)
            # drop dd_op before final write
            df_changed_to_apply = df_changed.drop("dd_op")

            # ensure schema alignment: pick unionByName with allowMissingColumns True
            df_final = df_existing_survivors.unionByName(df_changed_to_apply, allowMissingColumns=True)

            # write final combined dataframe back to the same S3 target (overwrite)
            self.parquet_writer.write(
                dataframe=df_final,
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                mode="overwrite",
            )

            # assign output df name
            df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_cool_planck = df_UPD_NISS_CLASS_CD_blissful_ramanujan

        except Exception:
            self.logger.exception("Failed applying load-modify-store-back for WRK_BIRP_NISS_APRM_DETL")
            raise

        self.glue_job.commit()

    def finalize(self) -> None:
        self.logger.info("Finalizing m_NU0C_TA_NISS_AUTO_ATPRM_DTL_Upd_ClassCd")

    @staticmethod
    def _require_env(env: Mapping[str, Any], key: str) -> str:

        value = env.get(key)

        if not value or str(value).startswith("REPLACE_WITH_"):
            raise ConfigurationValidationError(
                f"config.yaml 'env.{key}' must be set"
            )

        return str(value)
