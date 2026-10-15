import sys
from typing import Any, Mapping

from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import DataFrame

from glue_lib.connector import ParquetReader, ParquetWriter
from glue_lib.exceptions import ConfigurationValidationError
from glue_lib.job.base import BaseJob
from pyspark.sql.functions import col, lit, trim, when, substring, instr, concat_ws


# Informatica mapping m_NU0C_NISS_AUTO_114_ATPRM_DTL_Upd_ExcpnsAndDrops.
# Every mapping node runs in order inside run().
class Nu0cNissAuto114AtprmDtlUpdExcpnsAndDropsJob(BaseJob):

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

        # Read staged WRK_BIRP_NISS_APRM_DETL first (S3-first, fallback to Glue Catalog)
        try:
            self.logger.info("Attempting parquet read for FDR_LIB_WRK_BIRP_NISS_APRM_DETL (S3-first)", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL2")
            df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL2_affectionate_tesla = self.parquet_reader.read(
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                options=None,
            ).select(
                "NISS_APRM_DETL_SK", "CLNDR_YR", "CALL_YR", "NAIC_CMPNY_CD", "NISS_CMPNY_CD", "ST_NM", "ST_CD", "NISS_ST_CD",
                "ST_ABBR", "ACCTNG_LOB", "CVG_TYP_CD", "CVG_AMT", "BI_LMT", "GA_ADDED_AT_FAULT_IND", "FA2_PLCY_IND", "UM_UMI_STACKING",
                "PIP_WVR_WL_IND", "PIP_MED_SEC_IND", "PIP_LOSS_INCOME_IND", "MI_PPO_IND", "PRD_GRP_CD", "NJ_HLTH_INSR_PRIM", "NJ_EXTR_PIP_PKG",
                "NJ_RESDNC_RLTNSHP_PIP_IND", "NY_SSL_IND", "NY_FULL_CVG_GLASS_COMP_IND", "GRGNG_ZIP_5", "NISS_TERR_CD", "RATNG_CMPY_CD",
                "MLT_CAR_IND", "RT_CLS", "AGE", "GENDR", "MRTL_STAT", "AUTO_USE_CD", "MILES_TO_WRK", "GOOD_STDNT_IND", "DRVR_TRNG_IND",
                "SOI_TYP", "PHY_DMG_IND", "NJ_RATD_PNTS", "VEH_MDL_YR", "NJ_EXCPTION_CD", "NJ_FGVN_PNTS", "PASSV_RESTRA_DISC", "SNR_DRVR_IND",
                "DEFNS_DRVR_DISC_IND", "ANTI_THFT_DISC", "DAY_TM_RUN_LIGHTS", "LMT_TORT", "ANNL_STMNT_LOB_CD", "CVG_TYP_IND", "CVG_EXPS_VAL",
                "TTL_WRITTN_PREM_AMT", "LINE_CD", "ACCDNT_YR", "NISS_CVG_CD", "RTNG_ZNE_CD", "TERM_ZNE_CD", "NISS_CLASS_CD", "NISS_ELIG_PNTS_CD",
                "NISS_AGE_GRP_CD", "NISS_CMMCL_IND_CD", "NISS_EXCPN_CD", "NISS_FGVNS_CD", "NISS_PASSV_RESTRA_CD", "NISS_DEFNS_DRVR_CRD_CD",
                "NISS_ANTI_THFT_DVC_CD", "NISS_DAY_TM_RUN_LAMPS_DISC_CD", "NISS_PLCY_LMT_CD", "NISS_DEDUC_CD", "NISS_SSL_LIAB_CD", "NISS_SUBLOB_CD",
                "NISS_TYP_LOSS_CD", "NISS_LIAB_OR_NO_FAULT_CD", "NISS_ANNL_STMNT_LOB_CD", "NISS_PD_LOSS", "NISS_PD_ALLOC_ADJUS_EXPNS", "NISS_OUTSTNDG_LOSS",
                "NISS_NO_PD_CLMS", "NISS_NO_OUTSTND_CLMS", "RSVD_NISS_USE", "NISS_RSVD_CMPNY_USE", "NISS_MNFCTRS_MDL_YR", "CR_BY_MAPNG_ID", "DW_CR_TMSP",
                "UPD_BY_MAPNG_ID", "DW_UPD_TMSP", "WRK_FLOW_RUN_ID", "NJ_NO_LWST_LMT_IND", "NJ_NMD_DRVR_EXCL_IND", "EXPS_VAL_ROLLED", "CVG_CNT_IND",
                "CVG_CNT", "CVG_CD_SK", "CVG_ATTR_SK", "REC_DROP_IND", "REC_DROP_RSN_DESC", "REC_EXCPN_IND", "REC_EXCPN_RSN_DESC", "CVG_ATTR_CHCKSUM",
                "COMP_DED", "COLL_DED", "PLCY_CNTRCT_NUM", "UNIT_NUM", "EFF_DT", "NUM_OF_CARS_IN_HH", "RDRVR_DT_OF_BRTH", "TERM_STRT_DT", "SRC_SYS_CD",
                "DERIVED_RDRVR_AGE", "FINAL_RDRVR_AGE", "PNI_AGE", "LOB", "PRINCIPAL_OPRT", "SOURCE_IND_DERIVED"
            )
            if df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL2_affectionate_tesla.isEmpty():
                self.logger.warning("Source WRK_BIRP_NISS_APRM_DETL returned no rows", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL2")
        except Exception:
            self.logger.info("Falling back to Glue Catalog read for WRK_BIRP_NISS_APRM_DETL", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL2")
            try:
                dyf = self.glue_context.create_dynamic_frame.from_catalog(
                    database=self.glue_database,
                    table_name="WRK_BIRP_NISS_APRM_DETL",
                    transformation_ctx="df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL2_affectionate_tesla",
                )
                df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL2_affectionate_tesla = dyf.toDF().select(
                    "NISS_APRM_DETL_SK", "CLNDR_YR", "CALL_YR", "NAIC_CMPNY_CD", "NISS_CMPNY_CD", "ST_NM", "ST_CD", "NISS_ST_CD",
                    "ST_ABBR", "ACCTNG_LOB", "CVG_TYP_CD", "CVG_AMT", "BI_LMT", "GA_ADDED_AT_FAULT_IND", "FA2_PLCY_IND", "UM_UMI_STACKING",
                    "PIP_WVR_WL_IND", "PIP_MED_SEC_IND", "PIP_LOSS_INCOME_IND", "MI_PPO_IND", "PRD_GRP_CD", "NJ_HLTH_INSR_PRIM", "NJ_EXTR_PIP_PKG",
                    "NJ_RESDNC_RLTNSHP_PIP_IND", "NY_SSL_IND", "NY_FULL_CVG_GLASS_COMP_IND", "GRGNG_ZIP_5", "NISS_TERR_CD", "RATNG_CMPY_CD",
                    "MLT_CAR_IND", "RT_CLS", "AGE", "GENDR", "MRTL_STAT", "AUTO_USE_CD", "MILES_TO_WRK", "GOOD_STDNT_IND", "DRVR_TRNG_IND",
                    "SOI_TYP", "PHY_DMG_IND", "NJ_RATD_PNTS", "VEH_MDL_YR", "NJ_EXCPTION_CD", "NJ_FGVN_PNTS", "PASSV_RESTRA_DISC", "SNR_DRVR_IND",
                    "DEFNS_DRVR_DISC_IND", "ANTI_THFT_DISC", "DAY_TM_RUN_LIGHTS", "LMT_TORT", "ANNL_STMNT_LOB_CD", "CVG_TYP_IND", "CVG_EXPS_VAL",
                    "TTL_WRITTN_PREM_AMT", "LINE_CD", "ACCDNT_YR", "NISS_CVG_CD", "RTNG_ZNE_CD", "TERM_ZNE_CD", "NISS_CLASS_CD", "NISS_ELIG_PNTS_CD",
                    "NISS_AGE_GRP_CD", "NISS_CMMCL_IND_CD", "NISS_EXCPN_CD", "NISS_FGVNS_CD", "NISS_PASSV_RESTRA_CD", "NISS_DEFNS_DRVR_CRD_CD",
                    "NISS_ANTI_THFT_DVC_CD", "NISS_DAY_TM_RUN_LAMPS_DISC_CD", "NISS_PLCY_LMT_CD", "NISS_DEDUC_CD", "NISS_SSL_LIAB_CD", "NISS_SUBLOB_CD",
                    "NISS_TYP_LOSS_CD", "NISS_LIAB_OR_NO_FAULT_CD", "NISS_ANNL_STMNT_LOB_CD", "NISS_PD_LOSS", "NISS_PD_ALLOC_ADJUS_EXPNS", "NISS_OUTSTNDG_LOSS",
                    "NISS_NO_PD_CLMS", "NISS_NO_OUTSTND_CLMS", "RSVD_NISS_USE", "NISS_RSVD_CMPNY_USE", "NISS_MNFCTRS_MDL_YR", "CR_BY_MAPNG_ID", "DW_CR_TMSP",
                    "UPD_BY_MAPNG_ID", "DW_UPD_TMSP", "WRK_FLOW_RUN_ID", "NJ_NO_LWST_LMT_IND", "NJ_NMD_DRVR_EXCL_IND", "EXPS_VAL_ROLLED", "CVG_CNT_IND",
                    "CVG_CNT", "CVG_CD_SK", "CVG_ATTR_SK", "REC_DROP_IND", "REC_DROP_RSN_DESC", "REC_EXCPN_IND", "REC_EXCPN_RSN_DESC", "CVG_ATTR_CHCKSUM",
                    "COMP_DED", "COLL_DED", "PLCY_CNTRCT_NUM", "UNIT_NUM", "EFF_DT", "NUM_OF_CARS_IN_HH", "RDRVR_DT_OF_BRTH", "TERM_STRT_DT", "SRC_SYS_CD",
                    "DERIVED_RDRVR_AGE", "FINAL_RDRVR_AGE", "PNI_AGE", "LOB", "PRINCIPAL_OPRT", "SOURCE_IND_DERIVED"
                )
                if df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL2_affectionate_tesla.isEmpty():
                    self.logger.warning("Catalog source WRK_BIRP_NISS_APRM_DETL returned no rows", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL2")
            except Exception:
                self.logger.exception("Failed reading WRK_BIRP_NISS_APRM_DETL from catalog")
                raise

        # Read Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2 (S3-first for WRK_ table)
        try:
            self.logger.info("Attempting parquet read for Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2 (S3-first)", node="Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2")
            df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2_keen_darwin = self.parquet_reader.read(
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL_2/",
                options=None,
            ).select(
                "NISS_APRM_DETL_SK", "NISS_CMPNY_CD", "NISS_ST_CD", "NISS_CVG_CD", "NISS_TERR_CD", "GRGNG_ZIP_5", "NISS_CLASS_CD",
                "NISS_EXCPN_CD", "NISS_FGVNS_CD", "NISS_PASSV_RESTRA_CD", "NISS_DEFNS_DRVR_CRD_CD", "NISS_ANTI_THFT_DVC_CD",
                "NISS_DAY_TM_RUN_LAMPS_DISC_CD", "NISS_PLCY_LMT_CD", "NISS_DEDUC_CD", "NISS_SSL_LIAB_CD", "NISS_SUBLOB_CD",
                "NISS_LIAB_OR_NO_FAULT_CD", "REC_EXCPN_IND", "REC_EXCPN_RSN_DESC", "AGE", "ACCTNG_LOB", "NUM_OF_CARS_IN_HH"
            )
            if df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2_keen_darwin.isEmpty():
                self.logger.warning("Source WRK_BIRP_NISS_APRM_DETL_2 returned no rows", node="Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2")
        except Exception:
            self.logger.info("Falling back to Glue Catalog read for WRK_BIRP_NISS_APRM_DETL_2", node="Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2")
            try:
                dyf = self.glue_context.create_dynamic_frame.from_catalog(
                    database=self.glue_database,
                    table_name="WRK_BIRP_NISS_APRM_DETL_2",
                    transformation_ctx="df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2_keen_darwin",
                )
                df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2_keen_darwin = dyf.toDF().select(
                    "NISS_APRM_DETL_SK", "NISS_CMPNY_CD", "NISS_ST_CD", "NISS_CVG_CD", "NISS_TERR_CD", "GRGNG_ZIP_5", "NISS_CLASS_CD",
                    "NISS_EXCPN_CD", "NISS_FGVNS_CD", "NISS_PASSV_RESTRA_CD", "NISS_DEFNS_DRVR_CRD_CD", "NISS_ANTI_THFT_DVC_CD",
                    "NISS_DAY_TM_RUN_LAMPS_DISC_CD", "NISS_PLCY_LMT_CD", "NISS_DEDUC_CD", "NISS_SSL_LIAB_CD", "NISS_SUBLOB_CD",
                    "NISS_LIAB_OR_NO_FAULT_CD", "REC_EXCPN_IND", "REC_EXCPN_RSN_DESC", "AGE", "ACCTNG_LOB", "NUM_OF_CARS_IN_HH"
                )
                if df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2_keen_darwin.isEmpty():
                    self.logger.warning("Catalog source WRK_BIRP_NISS_APRM_DETL_2 returned no rows", node="Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2")
            except Exception:
                self.logger.exception("Failed reading WRK_BIRP_NISS_APRM_DETL_2 from catalog")
                raise

        # SQ_WRK_BIRP_NISS_APRM_DETL_ZeroPremDrops: staged SQL override against temp view of WRK_BIRP_NISS_APRM_DETL
        sql_query = f"""SELECT DET.NISS_APRM_DETL_SK
FROM WRK_BIRP_NISS_APRM_DETL DET
WHERE DET.CVG_ATTR_SK IN (SELECT DETL.CVG_ATTR_SK
FROM WRK_BIRP_NISS_APRM_DETL DETL
WHERE DETL.REC_EXCPN_IND='Y'
GROUP BY DETL.CVG_ATTR_SK
HAVING SUM(DETL.TTL_WRITTN_PREM_AMT) = 0 AND SUM(DETL.EXPS_VAL_ROLLED) = 0
)
"""
        try:
            # the upstream WRK table was read earlier; expose it as the bare view name the SQL expects
            df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL2_affectionate_tesla.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")
            self.logger.info("Executing SQ_WRK_BIRP_NISS_APRM_DETL_ZeroPremDrops via spark.sql against staged temp view", node="SQ_WRK_BIRP_NISS_APRM_DETL_ZeroPremDrops")
            df_SQ_WRK_BIRP_NISS_APRM_DETL_ZeroPremDrops_loving_faraday = self.spark.sql(sql_query)
            if df_SQ_WRK_BIRP_NISS_APRM_DETL_ZeroPremDrops_loving_faraday.isEmpty():
                self.logger.warning("SQ_WRK_BIRP_NISS_APRM_DETL_ZeroPremDrops returned no rows", node="SQ_WRK_BIRP_NISS_APRM_DETL_ZeroPremDrops")
        except Exception:
            self.logger.exception("Failed executing SQ_WRK_BIRP_NISS_APRM_DETL_ZeroPremDrops")
            raise

        # SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2: staged custom SQL selecting many columns from the same WRK table
        sql_query = f"""SELECT DETL.NISS_APRM_DETL_SK,
DETL.NISS_CMPNY_CD,
DETL.NISS_ST_CD,
DETL.NISS_CVG_CD,
DETL.NISS_TERR_CD,
DETL.GRGNG_ZIP_5,
DETL.NISS_CLASS_CD,
DETL.NISS_EXCPN_CD,
DETL.NISS_FGVNS_CD,
DETL.NISS_PASSV_RESTRA_CD,
DETL.NISS_DEFNS_DRVR_CRD_CD,
DETL.NISS_ANTI_THFT_DVC_CD,
DETL.NISS_DAY_TM_RUN_LAMPS_DISC_CD,
DETL.NISS_PLCY_LMT_CD,
DETL.NISS_DEDUC_CD,
DETL.NISS_SSL_LIAB_CD,
DETL.NISS_SUBLOB_CD,
DETL.NISS_LIAB_OR_NO_FAULT_CD ,
DETL.REC_EXCPN_IND,
DETL.REC_EXCPN_RSN_DESC,
DETL.AGE,
DETL.ACCTNG_LOB,
DETL.NUM_OF_CARS_IN_HH

FROM WRK_BIRP_NISS_APRM_DETL DETL"""
        try:
            # run against the staged WRK table already present as a temp view
            df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL2_affectionate_tesla.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")
            self.logger.info("Executing SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2 via spark.sql against staged temp view", node="SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2")
            df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2_jovial_einstein = self.spark.sql(sql_query)
            if df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2_jovial_einstein.isEmpty():
                self.logger.warning("SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2 returned no rows", node="SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2")
        except Exception:
            self.logger.exception("Failed executing SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2")
            raise

        # EXP_PassThrough_Src1: add constant derived columns and pass through the key
        try:
            self.logger.info("Applying EXP_PassThrough_Src1 expressions", node="EXP_PassThrough_Src1")
            df_EXP_PassThrough_Src1_dazzling_curie = (
                df_SQ_WRK_BIRP_NISS_APRM_DETL_ZeroPremDrops_loving_faraday
                .select("NISS_APRM_DETL_SK")
                .withColumn("REC_DROP_IND", lit('Y'))
                .withColumn("REC_DROP_RSN_DESC", lit('997'))
            )
        except Exception:
            self.logger.exception("Failed applying EXP_PassThrough_Src1 expressions")
            raise

        # EXP_PassThrough_Src: explicit passthrough select from the ASQ result
        try:
            self.logger.info("Applying EXP_PassThrough_Src passthrough", node="EXP_PassThrough_Src")
            df_EXP_PassThrough_Src_mystifying_hopper = (
                df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_2_jovial_einstein
                .select(
                    "NISS_APRM_DETL_SK", "NISS_CMPNY_CD", "NISS_ST_CD", "NISS_CVG_CD", "NISS_TERR_CD", "GRGNG_ZIP_5",
                    "NISS_CLASS_CD", "NISS_EXCPN_CD", "NISS_FGVNS_CD", "NISS_PASSV_RESTRA_CD", "NISS_DEFNS_DRVR_CRD_CD",
                    "NISS_ANTI_THFT_DVC_CD", "NISS_DAY_TM_RUN_LAMPS_DISC_CD", "NISS_PLCY_LMT_CD", "NISS_DEDUC_CD", "NISS_SSL_LIAB_CD",
                    "NISS_SUBLOB_CD", "NISS_LIAB_OR_NO_FAULT_CD", "REC_EXCPN_IND", "REC_EXCPN_RSN_DESC", "AGE", "ACCTNG_LOB", "NUM_OF_CARS_IN_HH"
                )
            )
        except Exception:
            self.logger.exception("Failed applying EXP_PassThrough_Src passthrough")
            raise

        # EXP_Gen_Exceptions: compute many local fragments, concatenate and set REC_EXCPN_IND
        try:
            self.logger.info("Applying EXP_Gen_Exceptions expressions", node="EXP_Gen_Exceptions")
            d = df_EXP_PassThrough_Src_mystifying_hopper
            df_exp_ex = (
                d
                .withColumn("NISS_CMPNY_CD_RecExcpnRsn", when(substring(trim(col("NISS_CMPNY_CD")), 1, 1) == '?', lit('NISS_CMPNY_CD;')).otherwise(lit('')))
                .withColumn("NISS_ST_CD_RecExcpnRsn", when(substring(trim(col("NISS_ST_CD")), 1, 1) == '?', lit('NISS_ST_CD;')).otherwise(lit('')))
                .withColumn("NISS_CVG_CD_RecExcpnRsn",
                    when(substring(trim(col("NISS_CVG_CD")), 1, 1) == '?', lit('NISS_CVG_CD;'))
                    .when((col("REC_EXCPN_IND") == 'Y') & (substring(trim(col("NISS_CVG_CD")), 1, 1) == 'E'), lit('Dflt NISS_CVG_CD;'))
                    .otherwise(lit(''))
                )
                .withColumn("NISS_TERR_CD_RecExcpnRsn", when(substring(trim(col("NISS_TERR_CD")), 1, 1) == '?', lit('NISS_TERR_CD;')).otherwise(lit('')))
                .withColumn("GRGNG_ZIP_5_RecExcpnRsn", when((substring(trim(col("GRGNG_ZIP_5")), 1, 1) == '?') | (col("GRGNG_ZIP_5") == '00000'), lit('GRGNG_ZIP;')).otherwise(lit('')))
                .withColumn("NISS_CLASS_CD_RecExcpnRsn",
                    when((col("REC_EXCPN_IND") == 'Y') & (instr(col("REC_EXCPN_RSN_DESC"), 'DEFAULT_CLASS_CD') != 0), lit('DEFAULT_CLASS_CD;'))
                    .when(substring(trim(col("NISS_CLASS_CD")), 1, 1) == '?', lit('106:DEFAULT_CLASS_CD;'))
                    .when(substring(trim(col("NISS_CLASS_CD")), 1, 1) == 'E', lit('118:DEFAULT_CLASS_CD;'))
                    .otherwise(lit(''))
                )
                .withColumn("NISS_DFLT_ZIP_RecExcpnRsn", when((col("REC_EXCPN_IND") == 'Y') & (instr(col("REC_EXCPN_RSN_DESC"), 'assigned default zip code') != 0), lit('assigned default zip code;')).otherwise(lit('')))
                .withColumn("NISS_EXCPN_CD_RecExcpnRsn", when(substring(trim(col("NISS_EXCPN_CD")), 1, 1) == '?', lit('NISS_EXCPN_CD;')).otherwise(lit('')))
                .withColumn("NISS_FGVNS_CD_RecExcpnRsn", when(substring(trim(col("NISS_FGVNS_CD")), 1, 1) == '?', lit('NISS_FGVNS_CD;')).otherwise(lit('')))
                .withColumn("NISS_PASSV_RESTRA_CD_RecExcpnRsn", when(substring(trim(col("NISS_PASSV_RESTRA_CD")), 1, 1) == '?', lit('NISS_PASSV_RESTRA_CD;')).otherwise(lit('')))
                .withColumn("NISS_DEFNS_DRVR_CRD_CD_RecExcpnRsn", when(substring(trim(col("NISS_DEFNS_DRVR_CRD_CD")), 1, 1) == '?', lit('DEFNS_DRVR_CRD_CD;')).otherwise(lit('')))
                .withColumn("NISS_ANTI_THFT_DVC_CD_RecExcpnRsn", when(substring(trim(col("NISS_ANTI_THFT_DVC_CD")), 1, 1) == '?', lit('ANTI_THFT_DVC_CD;')).otherwise(lit('')))
                .withColumn("NISS_DAY_TM_RUN_LAMPS_DISC_CD_RecExcpnRsn", when(substring(trim(col("NISS_DAY_TM_RUN_LAMPS_DISC_CD")), 1, 1) == '?', lit('DAY_TM_RUN_LAMPS_DISC_CD;')).otherwise(lit('')))
                .withColumn("NISS_PLCY_LMT_CD_RecExcpnRsn",
                    when(substring(trim(col("NISS_PLCY_LMT_CD")), 1, 1) == '?', lit('PLCY_LMT_CD;'))
                    .when(col("NISS_PLCY_LMT_CD") == '??', lit('INVALID PLCY_LMT_CD;'))
                    .when(substring(trim(col("NISS_PLCY_LMT_CD")), 1, 1) == 'E', lit('DFLT_PLCY_LMT_CD;'))
                    .otherwise(lit(''))
                )
                .withColumn("NISS_DEDUC_CD_RecExcpnRsn",
                    when(substring(trim(col("NISS_DEDUC_CD")), 1, 1) == '?', lit('DEDUC_CD;'))
                    .when(substring(trim(col("NISS_DEDUC_CD")), 1, 1) == 'E', lit('DFLT_DEDUC_CD;'))
                    .otherwise(lit(''))
                )
                .withColumn("NISS_SSL_LIAB_CD_RecExcpnRsn", when(substring(trim(col("NISS_SSL_LIAB_CD")), 1, 1) == '?', lit('SSL_LIAB_CD;')).otherwise(lit('')))
                .withColumn("NISS_SUBLOB_CD_RecExcpnRsn", when(substring(trim(col("NISS_SUBLOB_CD")), 1, 1) == '?', lit('SUBLOB_CD;')).otherwise(lit('')))
                .withColumn("NISS_LIAB_OR_NO_FAULT_CD_RecExcpnRsn", when(substring(trim(col("NISS_LIAB_OR_NO_FAULT_CD")), 1, 1) == '?', lit('IAB_OR_NO_FAULT_CD;')).otherwise(lit('')))
                .withColumn("NUM_OF_CARS_IN_HH_RecExcpnRsn", when((col("NISS_ST_CD") == '09') & (col("NUM_OF_CARS_IN_HH").isNull()), lit('BLANK VEHICLE COUNT;')).otherwise(lit('')))
                .withColumn("ACCTNG_LOB_RecExcpnRsn", when((col("ACCTNG_LOB").isNull()) | (trim(col("ACCTNG_LOB")) == ''), lit('Blank ACCNTG_LOB;')).otherwise(lit('')))
                .withColumn("AGE_RecExcpnRsn", when((trim(col("AGE")) == '') | (col("AGE").cast('int') > 120) | (col("AGE").cast('int') < 14), lit('Invalid Rated Driver Age;')).otherwise(lit('')))
            )
            # concatenate into final reason
            df_exp_ex = df_exp_ex.withColumn(
                "v_REC_EXCPN_RSN_DESC",
                concat_ws('',
                    col("NISS_DFLT_ZIP_RecExcpnRsn"), col("NISS_CMPNY_CD_RecExcpnRsn"), col("NISS_ST_CD_RecExcpnRsn"), col("NISS_CVG_CD_RecExcpnRsn"),
                    col("NISS_TERR_CD_RecExcpnRsn"), col("GRGNG_ZIP_5_RecExcpnRsn"), col("NISS_CLASS_CD_RecExcpnRsn"), col("NISS_EXCPN_CD_RecExcpnRsn"),
                    col("NISS_FGVNS_CD_RecExcpnRsn"), col("NISS_PASSV_RESTRA_CD_RecExcpnRsn"), col("NISS_DEFNS_DRVR_CRD_CD_RecExcpnRsn"),
                    col("NISS_ANTI_THFT_DVC_CD_RecExcpnRsn"), col("NISS_DAY_TM_RUN_LAMPS_DISC_CD_RecExcpnRsn"), col("NISS_PLCY_LMT_CD_RecExcpnRsn"),
                    col("NISS_DEDUC_CD_RecExcpnRsn"), col("NISS_SSL_LIAB_CD_RecExcpnRsn"), col("NISS_SUBLOB_CD_RecExcpnRsn"), col("NISS_LIAB_OR_NO_FAULT_CD_RecExcpnRsn"),
                    col("AGE_RecExcpnRsn"), col("NUM_OF_CARS_IN_HH_RecExcpnRsn"), col("ACCTNG_LOB_RecExcpnRsn")
                )
            ).withColumn(
                "REC_EXCPN_RSN_DESC",
                col("v_REC_EXCPN_RSN_DESC")
            ).withColumn(
                "REC_EXCPN_IND",
                when((trim(coalesce(col("REC_EXCPN_IND"), lit(''))) == '') & (col("v_REC_EXCPN_RSN_DESC") == ''), lit('')).otherwise(lit('Y'))
            ).drop("v_REC_EXCPN_RSN_DESC")
            df_EXP_Gen_Exceptions_romantic_maxwell = df_exp_ex.select(
                "NISS_APRM_DETL_SK", "REC_EXCPN_IND", "REC_EXCPN_RSN_DESC", "NISS_CMPNY_CD", "NISS_ST_CD", "NISS_CVG_CD", "NISS_TERR_CD",
                "GRGNG_ZIP_5", "NISS_CLASS_CD", "NISS_EXCPN_CD", "NISS_FGVNS_CD", "NISS_PASSV_RESTRA_CD", "NISS_DEFNS_DRVR_CRD_CD",
                "NISS_ANTI_THFT_DVC_CD", "NISS_DAY_TM_RUN_LAMPS_DISC_CD", "NISS_PLCY_LMT_CD", "NISS_DEDUC_CD", "NISS_SSL_LIAB_CD",
                "NISS_SUBLOB_CD", "NISS_LIAB_OR_NO_FAULT_CD", "AGE", "ACCTNG_LOB", "NUM_OF_CARS_IN_HH"
            )
        except Exception:
            self.logger.exception("Failed applying EXP_Gen_Exceptions expressions")
            raise

        # EXP_Gen_RecDrops: compute drop-reason fragments, concatenate and set REC_DROP_IND/REC_DROP_RSN_DESC
        try:
            self.logger.info("Applying EXP_Gen_RecDrops expressions", node="EXP_Gen_RecDrops")
            d = df_EXP_PassThrough_Src_mystifying_hopper
            df_exp_drop = (
                d
                .withColumn("NISS_CLASS_CD_RecDropRsn", when(substring(trim(col("NISS_CLASS_CD")), 1, 1) == '?', lit('INVALID CLASS_CD;')).otherwise(lit('')))
                .withColumn("NISS_CVG_CD_RecDropRsn", when((col("NISS_CVG_CD").isin('na', 'n/a')) | (substring(trim(col("NISS_CVG_CD")), 1, 1) == '?'), lit('999;')).otherwise(lit('')))
                .withColumn("NISS_PLCY_LMT_CD_RecDropRsn", when(col("NISS_PLCY_LMT_CD").isin('na', '??'), lit('998;')).otherwise(lit('')))
                .withColumn("NISS_DEDUC_CD_RecDropRsn", when(col("NISS_DEDUC_CD") == '??', lit('INVALID DEDUC CD;')).otherwise(lit('')))
                .withColumn("ACCTNG_LOB_RecDropRsn", when((col("ACCTNG_LOB").isNull()) | (trim(col("ACCTNG_LOB")) == ''), lit('Blank ACCNTG_LOB;')).otherwise(lit('')))
                .withColumn("NO_OF_CARS_HH_RecDropRsn", when((col("NISS_ST_CD") == '09') & (col("NUM_OF_CARS_IN_HH").isNull()), lit('Blank Vehicle Count;')).otherwise(lit('')))
                .withColumn("NISS_ST_CD_RecDropRsn", when(col("NISS_ST_CD") == '??', lit('INVALID ST_CD;')).otherwise(lit('')))
            )
            df_exp_drop = df_exp_drop.withColumn(
                "v_REC_DROP_RSN_DESC",
                concat_ws('', col("NISS_CVG_CD_RecDropRsn"), col("NISS_PLCY_LMT_CD_RecDropRsn"), col("NISS_ST_CD_RecDropRsn"), col("NISS_CLASS_CD_RecDropRsn"),
                          col("NISS_DEDUC_CD_RecDropRsn"), col("ACCTNG_LOB_RecDropRsn"), col("NO_OF_CARS_HH_RecDropRsn"))
            ).withColumn("REC_DROP_RSN_DESC", col("v_REC_DROP_RSN_DESC")).withColumn("REC_DROP_IND", when(col("v_REC_DROP_RSN_DESC") != '', lit('Y')).otherwise(lit(''))).drop("v_REC_DROP_RSN_DESC")
            df_EXP_Gen_RecDrops_jolly_faraday = df_exp_drop.select(
                "NISS_APRM_DETL_SK", "REC_DROP_IND", "REC_DROP_RSN_DESC", "NISS_CVG_CD", "NISS_PLCY_LMT_CD", "NISS_ST_CD", "NISS_CLASS_CD",
                "NISS_DEDUC_CD", "ACCTNG_LOB", "NUM_OF_CARS_IN_HH"
            )
        except Exception:
            self.logger.exception("Failed applying EXP_Gen_RecDrops expressions")
            raise

        # EXP_Dummy: union three upstream expression results and pass through selected ports
        try:
            self.logger.info("Consolidating inputs in EXP_Dummy", node="EXP_Dummy")
            sel_cols = ["NISS_APRM_DETL_SK", "REC_DROP_IND", "REC_DROP_RSN_DESC", "REC_EXCPN_IND", "REC_EXCPN_RSN_DESC"]
            part1 = df_EXP_Gen_RecDrops_jolly_faraday.select(*[c for c in sel_cols if c in df_EXP_Gen_RecDrops_jolly_faraday.columns])
            part2 = df_EXP_Gen_Exceptions_romantic_maxwell.select(*[c for c in sel_cols if c in df_EXP_Gen_Exceptions_romantic_maxwell.columns])
            part3 = df_EXP_PassThrough_Src_mystifying_hopper.select(*[c for c in sel_cols if c in df_EXP_PassThrough_Src_mystifying_hopper.columns])
            # union missing columns will be allowed by allowMissingColumns=True via unionByName
            df_EXP_Dummy_tender_hilbert = part1.unionByName(part2, allowMissingColumns=True).unionByName(part3, allowMissingColumns=True)
        except Exception:
            self.logger.exception("Failed building EXP_Dummy consolidation")
            raise

        # Upd_RecDrop_Ind: mark dd_op = 'UPDATE', write REJECTs (none expected) then forward remaining
        try:
            self.logger.info("Applying Upd_RecDrop_Ind update-strategy (mark dd_op)", node="Upd_RecDrop_Ind")
            df_marked = df_EXP_PassThrough_Src1_dazzling_curie.withColumn("dd_op", lit('UPDATE'))
            df_rejects = df_marked.filter(col("dd_op") == 'REJECT')
            try:
                if not df_rejects.isEmpty():
                    df_rejects = df_rejects.withColumn('error_reason', lit('DD_REJECT'))
                    self.logger.info("Writing REJECT rows for FDR_LIB_WRK_BIRP_NISS_APRM_DETL to reject prefix", node="Upd_RecDrop_Ind")
                    self.parquet_writer.write(
                        dataframe=df_rejects,
                        path=f"s3://{self.s3_output_bucket}/reject/FDR_LIB_WRK_BIRP_NISS_APRM_DETL/",
                        mode="overwrite",
                        partition_by=None,
                        options=None,
                    )
            except Exception:
                self.logger.exception("Failed writing reject rows from Upd_RecDrop_Ind")
                raise
            df_Upd_RecDrop_Ind_sleepy_leibniz = df_marked.filter(col("dd_op") != 'REJECT')
        except Exception:
            self.logger.exception("Failed applying Upd_RecDrop_Ind update-strategy")
            raise

        # FIL_OnlyExceptions_or_Drops: filter REC_DROP_IND='Y' OR REC_EXCPN_IND='Y'
        try:
            self.logger.info("Applying FIL_OnlyExceptions_or_Drops filter", node="FIL_OnlyExceptions_or_Drops")
            df_FIL_OnlyExceptions_or_Drops_affectionate_turing = df_EXP_Dummy_tender_hilbert.filter((col("REC_DROP_IND") == 'Y') | (col("REC_EXCPN_IND") == 'Y'))
        except Exception:
            self.logger.exception("Failed applying FIL_OnlyExceptions_or_Drops filter")
            raise

        # Upd_CVG_CD_SK: mark dd_op = 'UPDATE' and write rejects if any, then forward remaining
        try:
            self.logger.info("Applying Upd_CVG_CD_SK update-strategy (mark dd_op)", node="Upd_CVG_CD_SK")
            df_marked2 = df_FIL_OnlyExceptions_or_Drops_affectionate_turing.withColumn('dd_op', lit('UPDATE'))
            df_rejects2 = df_marked2.filter(col('dd_op') == 'REJECT')
            try:
                if not df_rejects2.isEmpty():
                    df_rejects2 = df_rejects2.withColumn('error_reason', lit('DD_REJECT'))
                    self.logger.info("Writing REJECT rows for FDR_LIB_WRK_BIRP_NISS_APRM_DETL to reject prefix", node="Upd_CVG_CD_SK")
                    self.parquet_writer.write(
                        dataframe=df_rejects2,
                        path=f"s3://{self.s3_output_bucket}/reject/FDR_LIB_WRK_BIRP_NISS_APRM_DETL/",
                        mode="overwrite",
                        partition_by=None,
                        options=None,
                    )
            except Exception:
                self.logger.exception("Failed writing reject rows from Upd_CVG_CD_SK")
                raise
            df_Upd_CVG_CD_SK_gentle_maxwell = df_marked2.filter(col('dd_op') != 'REJECT')
        except Exception:
            self.logger.exception("Failed applying Upd_CVG_CD_SK update-strategy")
            raise

        # FDR_LIB_WRK_BIRP_NISS_APRM_DETL_ZeroPremDrops: apply load-modify-store-back to the target FDR_LIB_WRK_BIRP_NISS_APRM_DETL
        try:
            self.logger.info("Applying output write for FDR_LIB_WRK_BIRP_NISS_APRM_DETL_ZeroPremDrops (load-modify-store-back)", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL_ZeroPremDrops")
            incoming_df = df_Upd_RecDrop_Ind_sleepy_leibniz
            # read existing target (treat missing path as empty)
            try:
                existing_df = self.parquet_reader.read(path=f"s3://{self.s3_output_bucket}/FDR_LIB_WRK_BIRP_NISS_APRM_DETL/", options=None)
            except Exception:
                self.logger.info("Target FDR_LIB_WRK_BIRP_NISS_APRM_DETL not present on S3; treating existing as empty", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL_ZeroPremDrops")
                existing_df = self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), schema=incoming_df.schema)
            # build changed keys
            changed_keys_df = incoming_df.filter(col('dd_op').isin('INSERT', 'UPDATE', 'DELETE')).select('NISS_APRM_DETL_SK').distinct()
            preserved_df = existing_df.join(changed_keys_df, on=['NISS_APRM_DETL_SK'], how='left_anti')
            to_apply_df = preserved_df.unionByName(incoming_df.filter(col('dd_op').isin('INSERT', 'UPDATE')), allowMissingColumns=True)
            # note: full table rewrite is expensive on large targets
            self.parquet_writer.write(
                dataframe=to_apply_df,
                path=f"s3://{self.s3_output_bucket}/FDR_LIB_WRK_BIRP_NISS_APRM_DETL/",
                mode="overwrite",
                partition_by=None,
                options=None,
            )
            # assign the node's output dataframe name
            df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_sleepy_turing = to_apply_df
        except Exception:
            self.logger.exception("Failed applying load-modify-store-back for FDR_LIB_WRK_BIRP_NISS_APRM_DETL_ZeroPremDrops")
            raise

        # FDR_LIB_WRK_BIRP_NISS_APRM_DETL: final target load-modify-store-back (overwrite) against s3://.../FDR_LIB_WRK_BIRP_NISS_APRM_DETL/
        try:
            self.logger.info("Applying output write for FDR_LIB_WRK_BIRP_NISS_APRM_DETL (load-modify-store-back)", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            incoming_df2 = df_Upd_CVG_CD_SK_gentle_maxwell
            try:
                existing_df2 = self.parquet_reader.read(path=f"s3://{self.s3_output_bucket}/FDR_LIB_WRK_BIRP_NISS_APRM_DETL/", options=None)
            except Exception:
                self.logger.info("Target FDR_LIB_WRK_BIRP_NISS_APRM_DETL not present on S3; treating existing as empty", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
                existing_df2 = self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), schema=incoming_df2.schema)
            changed_keys_df2 = incoming_df2.filter(col('dd_op').isin('INSERT', 'UPDATE', 'DELETE')).select('NISS_APRM_DETL_SK').distinct()
            preserved_df2 = existing_df2.join(changed_keys_df2, on=['NISS_APRM_DETL_SK'], how='left_anti')
            apply_df2 = preserved_df2.unionByName(incoming_df2.filter(col('dd_op').isin('INSERT', 'UPDATE')), allowMissingColumns=True)
            # note: full read-anti-join-overwrite applied here; expensive on large targets
            self.parquet_writer.write(
                dataframe=apply_df2,
                path=f"s3://{self.s3_output_bucket}/FDR_LIB_WRK_BIRP_NISS_APRM_DETL/",
                mode="overwrite",
                partition_by=None,
                options=None,
            )
            df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_eager_archimedes = apply_df2
        except Exception:
            self.logger.exception("Failed applying load-modify-store-back for FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            raise

        self.glue_job.commit()

    def finalize(self) -> None:
        self.logger.info("Finalizing m_NU0C_NISS_AUTO_114_ATPRM_DTL_Upd_ExcpnsAndDrops")

    @staticmethod
    def _require_env(env: Mapping[str, Any], key: str) -> str:

        value = env.get(key)

        if not value or str(value).startswith("REPLACE_WITH_"):
            raise ConfigurationValidationError(
                f"config.yaml 'env.{key}' must be set"
            )

        return str(value)
