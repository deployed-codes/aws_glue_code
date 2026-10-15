import sys
from typing import Any, Mapping

from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import DataFrame

from glue_lib.connector import ParquetReader, ParquetWriter
from glue_lib.exceptions import ConfigurationValidationError
from glue_lib.job.base import BaseJob
from pyspark.sql.functions import (
    col,
    trim,
    substring,
    when,
    lit,
    regexp_replace,
    split,
    element_at,
    concat_ws,
    lpad,
    broadcast,
    coalesce
)
import os
import glob
import shutil
import tempfile
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


# Informatica mapping m_NU0C_NISS_AUTO_119_ATPRM_CreateReports.
# Every mapping node runs in order inside run().
class Nu0cNissAuto119AtprmCreateReportsJob(BaseJob):

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

        # Environment values for flat-file targets
        PM_TARGET_FILE_DIR = self._require_env(self.env, "PM_TARGET_FILE_DIR")
        OUTPUTFILE_PREMRPT3_SUM = self._require_env(self.env, "OUTPUTFILE_PREMRPT3_SUM")
        OUTPUTFILE_PREMRPT1_DET = self._require_env(self.env, "OUTPUTFILE_PREMRPT1_DET")
        OUTPUTFILE_PREMRPT_EXP = self._require_env(self.env, "OUTPUTFILE_PREMRPT_EXP")
        OUTPUTFILE_PREMRPT2_FNLCSV = self._require_env(self.env, "OUTPUTFILE_PREMRPT2_FNLCSV")
        OUTPUTFILE_PREMRPT2_FNLTXT = self._require_env(self.env, "OUTPUTFILE_PREMRPT2_FNLTXT")

        # Shortcut_to_WRK_BIRP_NISS_APRM_DETL1: try S3 parquet first, fall back to Glue Catalog
        try:
            self.logger.info("Attempting to read staged parquet for Shortcut_to_WRK_BIRP_NISS_APRM_DETL1 from S3", node="Shortcut_to_WRK_BIRP_NISS_APRM_DETL1")
            df_WRK_BIRP_NISS_APRM_DETL_nostalgic_planck = self.parquet_reader.read(
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                options=None,
            ).select("COLL_DED")
        except Exception:
            self.logger.warning("Staged parquet for WRK_BIRP_NISS_APRM_DETL not available, falling back to Glue Catalog read", node="Shortcut_to_WRK_BIRP_NISS_APRM_DETL1")
            try:
                dyf = self.glue_context.create_dynamic_frame_from_catalog(
                    database=self.glue_database,
                    table_name="WRK_BIRP_NISS_APRM_DETL",
                    transformation_ctx="df_WRK_BIRP_NISS_APRM_DETL_nostalgic_planck",
                )
                df_WRK_BIRP_NISS_APRM_DETL_nostalgic_planck = dyf.toDF().select("COLL_DED")
            except Exception:
                self.logger.exception("Failed reading WRK_BIRP_NISS_APRM_DETL from Glue Catalog")
                raise

        if df_WRK_BIRP_NISS_APRM_DETL_nostalgic_planck.isEmpty():
            self.logger.warning("Source Shortcut_to_WRK_BIRP_NISS_APRM_DETL1 returned no rows", node="Shortcut_to_WRK_BIRP_NISS_APRM_DETL1")

        # Shortcut_to_WRK_BIRP_NISS_APRM_DETL: S3-first, project SNR_DRVR_IND
        try:
            self.logger.info("Attempting to read staged parquet for Shortcut_to_WRK_BIRP_NISS_APRM_DETL from S3", node="Shortcut_to_WRK_BIRP_NISS_APRM_DETL")
            df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_dazzling_lovelace = self.parquet_reader.read(
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                options=None,
            ).select("SNR_DRVR_IND")
        except Exception:
            self.logger.warning("Staged parquet for WRK_BIRP_NISS_APRM_DETL not available, falling back to Glue Catalog read", node="Shortcut_to_WRK_BIRP_NISS_APRM_DETL")
            try:
                dyf = self.glue_context.create_dynamic_frame_from_catalog(
                    database=self.glue_database,
                    table_name="WRK_BIRP_NISS_APRM_DETL",
                    transformation_ctx="df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_dazzling_lovelace",
                )
                df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_dazzling_lovelace = dyf.toDF().select("SNR_DRVR_IND")
            except Exception:
                self.logger.exception("Failed reading WRK_BIRP_NISS_APRM_DETL from Glue Catalog for Shortcut_to_WRK_BIRP_NISS_APRM_DETL")
                raise

        if df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_dazzling_lovelace.isEmpty():
            self.logger.warning("Source Shortcut_to_WRK_BIRP_NISS_APRM_DETL returned no rows", node="Shortcut_to_WRK_BIRP_NISS_APRM_DETL")

        # FDR_LIB_WRK_BIRP_NISS_APRM_FINAL: S3-first read, project NISS_OUTSTNDG_LOSS (and minimal ports if needed downstream)
        try:
            self.logger.info("Attempting to read staged parquet for FDR_LIB_WRK_BIRP_NISS_APRM_FINAL from S3", node="FDR_LIB_WRK_BIRP_NISS_APRM_FINAL")
            df_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL_lucid_shannon = self.parquet_reader.read(
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_FINAL/",
                options=None,
            ).select("NISS_OUTSTNDG_LOSS")
        except Exception:
            self.logger.warning("Staged parquet for WRK_BIRP_NISS_APRM_FINAL not available, falling back to Glue Catalog read", node="FDR_LIB_WRK_BIRP_NISS_APRM_FINAL")
            try:
                dyf = self.glue_context.create_dynamic_frame_from_catalog(
                    database=self.glue_database,
                    table_name="WRK_BIRP_NISS_APRM_FINAL",
                    transformation_ctx="df_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL_lucid_shannon",
                )
                df_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL_lucid_shannon = dyf.toDF().select("NISS_OUTSTNDG_LOSS")
            except Exception:
                self.logger.exception("Failed reading WRK_BIRP_NISS_APRM_FINAL from Glue Catalog")
                raise

        if df_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL_lucid_shannon.isEmpty():
            self.logger.warning("Source FDR_LIB_WRK_BIRP_NISS_APRM_FINAL returned no rows", node="FDR_LIB_WRK_BIRP_NISS_APRM_FINAL")

        # SQ_FDR_LIB_WRK_BIRP_NISS_APRM_SUMRY: SQL override executed against staged temp view
        sql_query = f"""SELECT
NISS_CMPNY_CD,
CLNDR_YR,
NISS_ST_CD,
ACCDNT_YR,
NISS_CVG_CD,
NISS_CLASS_CD,
NISS_SUBLOB_CD,
NISS_TYP_LOSS_CD,
NISS_ANNL_STMNT_LOB_CD,
SUM(CVG_EXPS_VAL) AS CVG_EXPS_VAL,
SUM(TTL_WRITTN_PREM_AMT) AS TTL_WRITTN_PREM_AMT,
NISS_PD_LOSS,
NISS_PD_ALLOC_ADJUS_EXPNS,
NISS_OUTSTNDG_LOSS,
NISS_NO_PD_CLMS,
NISS_NO_OUTSTND_CLMS,
NISS_TERR_CD,
COUNT(*) AS REC_COUNT

FROM WRK_BIRP_NISS_APRM_FINAL
GROUP BY
NISS_CMPNY_CD,
CLNDR_YR,
NISS_ST_CD,
ACCDNT_YR,
NISS_CVG_CD,
NISS_CLASS_CD,
NISS_SUBLOB_CD,
NISS_TYP_LOSS_CD,
NISS_ANNL_STMNT_LOB_CD,
NISS_PD_LOSS,
NISS_PD_ALLOC_ADJUS_EXPNS,
NISS_OUTSTNDG_LOSS,
NISS_NO_PD_CLMS,
NISS_NO_OUTSTND_CLMS,
NISS_TERR_CD"""

        try:
            # register the staged dataframe as a temp view for the override
            self.logger.info("Registering WRK_BIRP_NISS_APRM_FINAL temp view for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_SUMRY", node="SQ_FDR_LIB_WRK_BIRP_NISS_APRM_SUMRY")
            df_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL_lucid_shannon.createOrReplaceTempView("WRK_BIRP_NISS_APRM_FINAL")

            self.logger.info("Executing SQ_FDR_LIB_WRK_BIRP_NISS_APRM_SUMRY SQL override via spark.sql", node="SQ_FDR_LIB_WRK_BIRP_NISS_APRM_SUMRY")
            df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_SUMRY_heroic_archimedes = self.spark.sql(sql_query)
            if df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_SUMRY_heroic_archimedes.isEmpty():
                self.logger.warning("SQ_FDR_LIB_WRK_BIRP_NISS_APRM_SUMRY returned no rows", node="SQ_FDR_LIB_WRK_BIRP_NISS_APRM_SUMRY")
        except Exception:
            self.logger.exception("Failed executing SQ_FDR_LIB_WRK_BIRP_NISS_APRM_SUMRY SQL override")
            raise

        # SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL: SQL override executed against staged temp view
        sql_query = f"""SELECT
CLNDR_YR,
CALL_YR,
NAIC_CMPNY_CD,
NISS_CMPNY_CD,
ST_NM,
ST_CD,
NISS_ST_CD,
ST_ABBR,
ACCTNG_LOB,
CVG_TYP_CD,
CVG_AMT,
BI_LMT,
GA_ADDED_AT_FAULT_IND,
FA2_PLCY_IND,
UM_UMI_STACKING,
PIP_WVR_WL_IND,
PIP_MED_SEC_IND,
PIP_LOSS_INCOME_IND,
MI_PPO_IND,
PRD_GRP_CD,
NJ_HLTH_INSR_PRIM,
NJ_EXTR_PIP_PKG,
NJ_RESDNC_RLTNSHP_PIP_IND,
NY_SSL_IND,
NY_FULL_CVG_GLASS_COMP_IND,
GRGNG_ZIP_5,
NISS_TERR_CD,
RATNG_CMPY_CD,
MLT_CAR_IND,
RT_CLS,
AGE,
GENDR,
MRTL_STAT,
AUTO_USE_CD,
MILES_TO_WRK,
GOOD_STDNT_IND,
DRVR_TRNG_IND,
SOI_TYP,
PHY_DMG_IND,
NJ_RATD_PNTS,
VEH_MDL_YR,
NJ_EXCPTION_CD,
NJ_FGVN_PNTS,
PASSV_RESTRA_DISC,
SNR_DRVR_IND,
DEFNS_DRVR_DISC_IND,
ANTI_THFT_DISC,
DAY_TM_RUN_LIGHTS,
LMT_TORT,
CVG_TYP_IND,
SUM(CVG_EXPS_VAL) AS CVG_EXPS_VAL,
SUM(TTL_WRITTN_PREM_AMT) TTL_WRITTN_PREM_AMT,
LINE_CD,
ACCDNT_YR,
CASE
WHEN NISS_CVG_CD = 'E16' THEN '616'
WHEN NISS_CVG_CD = 'E36' THEN '636'
WHEN NISS_CVG_CD = 'E56' THEN '656'
WHEN NISS_CVG_CD = 'E76' THEN '676'
ELSE NISS_CVG_CD END NISS_CVG_CD,
RTNG_ZNE_CD,
TERM_ZNE_CD,
CASE WHEN LEFT(NISS_CLASS_CD,1)='E' THEN '933100' ELSE NISS_CLASS_CD END NISS_CLASS_CD,
NISS_ELIG_PNTS_CD,
NISS_AGE_GRP_CD,
NISS_CMMCL_IND_CD,
NISS_EXCPN_CD,
NISS_FGVNS_CD,
NISS_PASSV_RESTRA_CD,
NISS_DEFNS_DRVR_CRD_CD,
NISS_ANTI_THFT_DVC_CD,
NISS_DAY_TM_RUN_LAMPS_DISC_CD,
NISS_PLCY_LMT_CD,
NISS_DEDUC_CD,
NISS_SSL_LIAB_CD,
NISS_SUBLOB_CD,
NISS_TYP_LOSS_CD,
NISS_LIAB_OR_NO_FAULT_CD,
NISS_ANNL_STMNT_LOB_CD,
NISS_PD_LOSS,
NISS_PD_ALLOC_ADJUS_EXPNS,
NISS_OUTSTNDG_LOSS,
NISS_NO_PD_CLMS,
NISS_NO_OUTSTND_CLMS,
NISS_MNFCTRS_MDL_YR,
NJ_NO_LWST_LMT_IND,
NJ_NMD_DRVR_EXCL_IND,
SUM(EXPS_VAL_ROLLED) AS EXPS_VAL_ROLLED,
REC_DROP_IND,
REC_DROP_RSN_DESC,
REC_EXCPN_IND,
COMP_DED,
COLL_DED,
NUM_OF_CARS_IN_HH,
RDRVR_DT_OF_BRTH,
TO_CHAR(TERM_STRT_DT,'MM/DD/YYYY')TERM_STRT_DT,
SRC_SYS_CD,
DERIVED_RDRVR_AGE,
FINAL_RDRVR_AGE,
PNI_AGE,
LOB,
PRINCIPAL_OPRT,
SOURCE_IND_DERIVED

FROM WRK_BIRP_NISS_APRM_DETL

GROUP BY
CLNDR_YR,
CALL_YR,
NAIC_CMPNY_CD,
NISS_CMPNY_CD,
ST_NM,
ST_CD,
NISS_ST_CD,
ST_ABBR,
ACCTNG_LOB,
CVG_TYP_CD,
CVG_TYP_IND,
CVG_AMT,
BI_LMT,
GA_ADDED_AT_FAULT_IND,
FA2_PLCY_IND,
UM_UMI_STACKING,
PIP_WVR_WL_IND,
PIP_MED_SEC_IND,
PIP_LOSS_INCOME_IND,
MI_PPO_IND,
PRD_GRP_CD,
NJ_HLTH_INSR_PRIM,
NJ_EXTR_PIP_PKG,
NJ_RESDNC_RLTNSHP_PIP_IND,
NY_SSL_IND,
NY_FULL_CVG_GLASS_COMP_IND,
GRGNG_ZIP_5,
NISS_TERR_CD,
RATNG_CMPY_CD,
MLT_CAR_IND,
RT_CLS,
AGE,
GENDR,
MRTL_STAT,
AUTO_USE_CD,
MILES_TO_WRK,
GOOD_STDNT_IND,
DRVR_TRNG_IND,
SOI_TYP,
PHY_DMG_IND,
NJ_RATD_PNTS,
VEH_MDL_YR,
NJ_EXCPTION_CD,
NJ_FGVN_PNTS,
PASSV_RESTRA_DISC,
SNR_DRVR_IND,
DEFNS_DRVR_DISC_IND,
ANTI_THFT_DISC,
DAY_TM_RUN_LIGHTS,
LMT_TORT,
LINE_CD,
ACCDNT_YR,
CASE
WHEN NISS_CVG_CD = 'E16' THEN '616'
WHEN NISS_CVG_CD = 'E36' THEN '636'
WHEN NISS_CVG_CD = 'E56' THEN '656'
WHEN NISS_CVG_CD = 'E76' THEN '676'
ELSE NISS_CVG_CD END,
RTNG_ZNE_CD,
TERM_ZNE_CD,
CASE WHEN LEFT(NISS_CLASS_CD,1)='E' THEN '933100' ELSE NISS_CLASS_CD END,
NISS_ELIG_PNTS_CD,
NISS_AGE_GRP_CD,
NISS_CMMCL_IND_CD,
NISS_EXCPN_CD,
NISS_FGVNS_CD,
NISS_PASSV_RESTRA_CD,
NISS_DEFNS_DRVR_CRD_CD,
NISS_ANTI_THFT_DVC_CD,
NISS_DAY_TM_RUN_LAMPS_DISC_CD,
NISS_PLCY_LMT_CD,
NISS_DEDUC_CD,
NISS_SSL_LIAB_CD,
NISS_SUBLOB_CD,
NISS_TYP_LOSS_CD,
NISS_LIAB_OR_NO_FAULT_CD,
NISS_ANNL_STMNT_LOB_CD,
NISS_PD_LOSS,
NISS_PD_ALLOC_ADJUS_EXPNS,
NISS_OUTSTNDG_LOSS,
NISS_NO_PD_CLMS,
NISS_NO_OUTSTND_CLMS,
NISS_MNFCTRS_MDL_YR,
NJ_NO_LWST_LMT_IND,
NJ_NMD_DRVR_EXCL_IND,
REC_EXCPN_IND,
REC_DROP_IND,
REC_DROP_RSN_DESC,
COMP_DED,
COLL_DED,
NUM_OF_CARS_IN_HH,
TO_CHAR(TERM_STRT_DT,'MM/DD/YYYY'),
RDRVR_DT_OF_BRTH,
SRC_SYS_CD,
PNI_AGE,
DERIVED_RDRVR_AGE,
FINAL_RDRVR_AGE,
LOB,
PRINCIPAL_OPRT,
SOURCE_IND_DERIVED"""

        try:
            self.logger.info("Registering WRK_BIRP_NISS_APRM_DETL temp view for SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL", node="SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL")
            # the staged source is available as df_WRK_BIRP_NISS_APRM_DETL_nostalgic_planck or the shortcut; register the full table view
            # We already have df_WRK_BIRP_NISS_APRM_DETL_nostalgic_planck (may be limited); ensure a view exists - if it's only COLL_DED we still register the main name using the more complete shortcut read if available
            try:
                df_for_view = self.parquet_reader.read(path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/", options=None)
            except Exception:
                # fall back to the already-read smaller df
                df_for_view = df_WRK_BIRP_NISS_APRM_DETL_nostalgic_planck
            df_for_view.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")

            self.logger.info("Executing SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL SQL override via spark.sql", node="SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL")
            df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_jovial_hopper = self.spark.sql(sql_query)
            if df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_jovial_hopper.isEmpty():
                self.logger.warning("SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL returned no rows", node="SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL")
        except Exception:
            self.logger.exception("Failed executing SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL SQL override")
            raise

        # SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_Exception: staged-case SQL override filtering REC_EXCPN_IND='Y'
        sql_query = f"""SELECT
DETL.CLNDR_YR,
DETL.CALL_YR,
DETL.NAIC_CMPNY_CD,
DETL.NISS_CMPNY_CD,
DETL.ST_NM,
DETL.ST_CD,
DETL.NISS_ST_CD,
DETL.ST_ABBR,
DETL.ACCTNG_LOB,
DETL.CVG_TYP_CD,
DETL.CVG_AMT,
DETL.BI_LMT,
DETL.GA_ADDED_AT_FAULT_IND,
DETL.FA2_PLCY_IND,
DETL.UM_UMI_STACKING,
DETL.PIP_WVR_WL_IND,
DETL.PIP_MED_SEC_IND,
DETL.PIP_LOSS_INCOME_IND,
DETL.MI_PPO_IND,
DETL.PRD_GRP_CD,
DETL.NJ_HLTH_INSR_PRIM,
DETL.NJ_EXTR_PIP_PKG,
DETL.NJ_RESDNC_RLTNSHP_PIP_IND,
DETL.NY_SSL_IND,
DETL.NY_FULL_CVG_GLASS_COMP_IND,
DETL.GRGNG_ZIP_5,
DETL.NISS_TERR_CD,
DETL.RATNG_CMPY_CD,
DETL.MLT_CAR_IND,
DETL.RT_CLS,
DETL.AGE,
DETL.GENDR,
DETL.MRTL_STAT,
DETL.AUTO_USE_CD,
DETL.MILES_TO_WRK,
DETL.GOOD_STDNT_IND,
DETL.DRVR_TRNG_IND,
DETL.SOI_TYP,
DETL.PHY_DMG_IND,
DETL.NJ_RATD_PNTS,
DETL.VEH_MDL_YR,
DETL.NJ_EXCPTION_CD,
DETL.NJ_FGVN_PNTS,
DETL.PASSV_RESTRA_DISC,
DETL.SNR_DRVR_IND,
DETL.DEFNS_DRVR_DISC_IND,
DETL.ANTI_THFT_DISC,
DETL.DAY_TM_RUN_LIGHTS,
DETL.LMT_TORT,
DETL.CVG_EXPS_VAL,
DETL.TTL_WRITTN_PREM_AMT,
DETL.LINE_CD,
DETL.ACCDNT_YR,
CASE
WHEN DETL.NISS_CVG_CD = 'E16' THEN '616'
WHEN DETL.NISS_CVG_CD = 'E36' THEN '636'
WHEN DETL.NISS_CVG_CD = 'E56' THEN '656'
WHEN DETL.NISS_CVG_CD = 'E76' THEN '676'
ELSE DETL.NISS_CVG_CD END NISS_CVG_CD,
DETL.RTNG_ZNE_CD,
DETL.TERM_ZNE_CD,
CASE WHEN LEFT(DETL.NISS_CLASS_CD,1)='E' THEN '933100' ELSE DETL.NISS_CLASS_CD END NISS_CLASS_CD,
DETL.NISS_ELIG_PNTS_CD,
DETL.NISS_AGE_GRP_CD,
DETL.NISS_CMMCL_IND_CD,
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
DETL.NISS_TYP_LOSS_CD,
DETL.NISS_LIAB_OR_NO_FAULT_CD,
DETL.NISS_ANNL_STMNT_LOB_CD,
DETL.NISS_PD_LOSS,
DETL.NISS_PD_ALLOC_ADJUS_EXPNS,
DETL.NISS_OUTSTNDG_LOSS,
DETL.NISS_NO_PD_CLMS,
DETL.NISS_NO_OUTSTND_CLMS,
DETL.NISS_MNFCTRS_MDL_YR,
DETL.NJ_NO_LWST_LMT_IND,
DETL.NJ_NMD_DRVR_EXCL_IND,
DETL.REC_DROP_IND,
DETL.REC_EXCPN_IND,
DETL.REC_EXCPN_RSN_DESC,
DETL.COMP_DED,
DETL.COLL_DED,
DETL.NUM_OF_CARS_IN_HH,
DETL.RDRVR_DT_OF_BRTH,
TO_CHAR(DETL.TERM_STRT_DT,'MM/DD/YYYY') TERM_STRT_DT,
DETL.SRC_SYS_CD,
DETL.DERIVED_RDRVR_AGE,
DETL.FINAL_RDRVR_AGE,
DETL.PNI_AGE,
DETL.LOB,
DETL.PRINCIPAL_OPRT,
DETL.SOURCE_IND_DERIVED

FROM WRK_BIRP_NISS_APRM_DETL DETL

WHERE DETL.REC_EXCPN_IND='Y'"""

        try:
            self.logger.info("Ensuring WRK_BIRP_NISS_APRM_DETL temp view for SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_Exception", node="SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_Exception")
            # ensure view exists (reusing earlier staged read where possible)
            try:
                df_temp = self.parquet_reader.read(path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/", options=None)
            except Exception:
                df_temp = df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_dazzling_lovelace
            df_temp.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")

            self.logger.info("Executing SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_Exception SQL override via spark.sql", node="SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_Exception")
            df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_Exception_focused_ramanujan = self.spark.sql(sql_query)
            if df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_Exception_focused_ramanujan.isEmpty():
                self.logger.warning("SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_Exception returned no rows", node="SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_Exception")
        except Exception:
            self.logger.exception("Failed executing SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_Exception SQL override")
            raise

        # SQ_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL: staged override against WRK_BIRP_NISS_APRM_FINAL
        sql_query = f"""SELECT FNL.NISS_CMPNY_CD,
FNL.CLNDR_YR,
FNL.CALL_YR,
FNL.NISS_ST_CD,
FNL.LINE_CD,
FNL.ACCDNT_YR,
FNL.NISS_CVG_CD,
FNL.NISS_TERR_CD,
FNL.RTNG_ZNE_CD,
FNL.TERM_ZNE_CD,
FNL.GRGNG_ZIP_5,
FNL.NISS_CLASS_CD,
FNL.NISS_ELIG_PNTS_CD,
FNL.NISS_AGE_GRP_CD,
FNL.NISS_MNFCTRS_MDL_YR,
FNL.NISS_CMMCL_IND_CD,
FNL.NISS_EXCPN_CD,
FNL.NISS_FGVNS_CD,
FNL.NISS_PASSV_RESTRA_CD,
FNL.NISS_DEFNS_DRVR_CRD_CD,
FNL.NISS_ANTI_THFT_DVC_CD,
FNL.NISS_DAY_TM_RUN_LAMPS_DISC_CD,
FNL.NISS_PLCY_LMT_CD,
FNL.NISS_DEDUC_CD,
FNL.NISS_SSL_LIAB_CD,
FNL.NISS_SUBLOB_CD,
FNL.NISS_TYP_LOSS_CD,
FNL.NISS_LIAB_OR_NO_FAULT_CD,
FNL.NISS_ANNL_STMNT_LOB_CD,
FNL.CVG_EXPS_VAL,
FNL.TTL_WRITTN_PREM_AMT,
FNL.NISS_PD_LOSS,
FNL.NISS_PD_ALLOC_ADJUS_EXPNS,
FNL.NISS_OUTSTNDG_LOSS,
FNL.NISS_NO_PD_CLMS,
FNL.NISS_NO_OUTSTND_CLMS,
FNL.RSVD_NISS_USE,
FNL.NISS_RSVD_CMPNY_USE

FROM WRK_BIRP_NISS_APRM_FINAL FNL"""

        try:
            self.logger.info("Registering WRK_BIRP_NISS_APRM_FINAL temp view for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL", node="SQ_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL")
            # reuse earlier view
            try:
                df_full = self.parquet_reader.read(path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_FINAL/", options=None)
            except Exception:
                df_full = df_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL_lucid_shannon
            df_full.createOrReplaceTempView("WRK_BIRP_NISS_APRM_FINAL")

            self.logger.info("Executing SQ_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL SQL override via spark.sql", node="SQ_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL")
            df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL_relaxed_schrodinger = self.spark.sql(sql_query)
            if df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL_relaxed_schrodinger.isEmpty():
                self.logger.warning("SQ_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL returned no rows", node="SQ_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL")
        except Exception:
            self.logger.exception("Failed executing SQ_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL SQL override")
            raise

        # EX_PASS_SUMRY: expressions - derive o_CLNDR_YR, o_NISS_CVG_CD, o_NISS_CLASS_CD and pass-through ports
        try:
            self.logger.info("Applying EX_PASS_SUMRY expressions", node="EX_PASS_SUMRY")
            df_EX_PASS_SUMRY_amazing_hopper = (
                df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_SUMRY_heroic_archimedes
                .withColumn("o_CLNDR_YR", substring(trim(col("CLNDR_YR")), 3, 2))
                .withColumn("o_NISS_CVG_CD", when(trim(col("NISS_CVG_CD")) == "", lit("???")).otherwise(col("NISS_CVG_CD")))
                .withColumn("o_NISS_CLASS_CD", when(trim(col("NISS_CLASS_CD")) == "", lit("??????")).otherwise(col("NISS_CLASS_CD")))
            )
        except Exception:
            self.logger.exception("Failed applying EX_PASS_SUMRY expressions")
            raise

        # EXP_GenErrReason: split REC_DROP_RSN_DESC tokens and left-join to lookup table LKP_FF_REF_NISS_AUTO_RECDROPERRORS
        try:
            self.logger.info("Applying EXP_GenErrReason: tokenizing REC_DROP_RSN_DESC and resolving lookups", node="EXP_GenErrReason")
            # read the lookup reference from Glue Catalog
            try:
                dyf_lkp = self.glue_context.create_dynamic_frame_from_catalog(
                    database=self.glue_database,
                    table_name="LKP_FF_REF_NISS_AUTO_RECDROPERRORS",
                    transformation_ctx="df_LKP_FF_REF_NISS_AUTO_RECDROPERRORS",
                )
                df_lkp = dyf_lkp.toDF()
            except Exception:
                # if lookup is missing, create empty df to safe-guard joins
                self.logger.exception("Failed reading lookup table LKP_FF_REF_NISS_AUTO_RECDROPERRORS from Glue Catalog")
                raise

            # determine key/value columns heuristically from lookup table
            lkp_cols = df_lkp.columns
            if len(lkp_cols) >= 2:
                lkp_key_col = lkp_cols[0]
                lkp_val_col = lkp_cols[1]
            elif len(lkp_cols) == 1:
                lkp_key_col = lkp_cols[0]
                lkp_val_col = lkp_cols[0]
            else:
                raise RuntimeError("Lookup table LKP_FF_REF_NISS_AUTO_RECDROPERRORS has no columns")

            # prepare the lookup small dataframe for broadcast joins
            lkp_small = df_lkp.select(col(lkp_key_col).alias("lkp_key"), col(lkp_val_col).alias("lkp_val"))

            df_tmp = df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_jovial_hopper.withColumn(
                "_tokens",
                split(trim(coalesce(col("REC_DROP_RSN_DESC"), lit(""))), ";")
            ).withColumn("tok1", trim(element_at(col("_tokens"), 1)))
            df_tmp = df_tmp.withColumn("tok2", trim(element_at(col("_tokens"), 2)))
            df_tmp = df_tmp.withColumn("tok3", trim(element_at(col("_tokens"), 3)))
            df_tmp = df_tmp.withColumn("tok4", trim(element_at(col("_tokens"), 4)))

            # left-join per token to lookup
            df_tmp = df_tmp.join(broadcast(lkp_small), df_tmp["tok1"] == lkp_small["lkp_key"], how="left").withColumnRenamed("lkp_val", "lkp_val1")
            # after join, remove duplicate lkp_key column if present
            if "lkp_key" in df_tmp.columns:
                # preserve lkp_val1 only
                pass

            df_tmp = df_tmp.join(broadcast(lkp_small).withColumnRenamed("lkp_key", "lkp_key2").withColumnRenamed("lkp_val", "lkp_val2"), df_tmp["tok2"] == col("lkp_key2"), how="left")
            df_tmp = df_tmp.join(broadcast(lkp_small).withColumnRenamed("lkp_key", "lkp_key3").withColumnRenamed("lkp_val", "lkp_val3"), df_tmp["tok3"] == col("lkp_key3"), how="left")
            df_tmp = df_tmp.join(broadcast(lkp_small).withColumnRenamed("lkp_key", "lkp_key4").withColumnRenamed("lkp_val", "lkp_val4"), df_tmp["tok4"] == col("lkp_key4"), how="left")

            # coalesce lookup results to empty string when null and concat with ';'
            df_EXP_GenErrReason_eager_tesla = df_tmp.withColumn(
                "REC_DROP_RSN_DESC",
                concat_ws(";",
                          coalesce(col("lkp_val1"), lit("")),
                          coalesce(col("lkp_val2"), lit("")),
                          coalesce(col("lkp_val3"), lit("")),
                          coalesce(col("lkp_val4"), lit(""))
                )
            ).drop("_tokens", "tok1", "tok2", "tok3", "tok4", "lkp_key", "lkp_key2", "lkp_key3", "lkp_key4")

        except Exception:
            self.logger.exception("Failed processing EXP_GenErrReason")
            raise

        # EXP_PASS_DETL: pass-through and derived fields, using REC_DROP_RSN_DESC from EXP_GenErrReason where available
        try:
            self.logger.info("Applying EXP_PASS_DETL expressions and joining generated REC_DROP_RSN_DESC", node="EXP_PASS_DETL")
            # join the two input frames on their common columns to bring in REC_DROP_RSN_DESC from the lookup-enhanced df
            left_df = df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_jovial_hopper
            right_df = df_EXP_GenErrReason_eager_tesla

            # build join keys as intersection of column sets (best-effort). If no common columns, perform a left_concat by adding an index - but we assume common columns exist
            common_cols = [c for c in left_df.columns if c in right_df.columns]
            if common_cols:
                join_cond = [left_df[c] == right_df[c] for c in common_cols]
                from functools import reduce
                join_expr = reduce(lambda a, b: a & b, join_cond)
                df_joined = left_df.join(right_df.select(*right_df.columns), join_expr, how="left")
            else:
                # fallback: bind columns by row-order using zipWithIndex trick
                l_with_idx = left_df.withColumn("__idx", row_number().over(Window.orderBy(lit(1))))
                r_with_idx = right_df.withColumn("__idx", row_number().over(Window.orderBy(lit(1))))
                df_joined = l_with_idx.join(r_with_idx, on="__idx", how="left").drop("__idx")

            # apply pass-through and derived transformations per mapping (examples shown)
            df_EXP_PASS_DETL_optimistic_lovelace = (
                df_joined
                .withColumn("o_NISS_TERR_CD", when(trim(col("NISS_TERR_CD")).eqNullSafe(lit("")), lit("???")).otherwise(col("NISS_TERR_CD")))
                .withColumn("o_CVG_AMT", regexp_replace(trim(col("CVG_AMT")), ',', ''))
                .withColumn("o_BI_LMT", regexp_replace(trim(col("BI_LMT")), ',', ''))
                .withColumn("o_CLNDR_YR", substring(trim(col("CLNDR_YR")), 3, 2))
                .withColumn("o_CALL_YR", substring(trim(col("CALL_YR")), 3, 2))
                .withColumn("o_NISS_CVG_CD", when(trim(col("NISS_CVG_CD")) == "", lit("???")).otherwise(col("NISS_CVG_CD")))
                .withColumn("o_NISS_CLASS_CD", when(trim(col("NISS_CLASS_CD")) == "", lit("??????")).otherwise(col("NISS_CLASS_CD")))
            )
        except Exception:
            self.logger.exception("Failed applying EXP_PASS_DETL expressions")
            raise

        # EXP_PASS_TRGT: pass-through and O_* derived fields
        try:
            self.logger.info("Applying EXP_PASS_TRGT expressions", node="EXP_PASS_TRGT")
            # compute v_NISS_MNFCTRS_MDL_YR intermediate then o_NISS_MNFCTRS_MDL_YR
            df_EXP_PASS_TRGT_calm_dirac = (
                df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_Exception_focused_ramanujan
                .withColumn("v_NISS_MNFCTRS_MDL_YR", trim(col("NISS_MNFCTRS_MDL_YR").cast("string")))
                .withColumn("o_NISS_MNFCTRS_MDL_YR", when((col("v_NISS_MNFCTRS_MDL_YR")).isNotNull() & (col("v_NISS_MNFCTRS_MDL_YR").rlike("^\\d{2}$")), col("v_NISS_MNFCTRS_MDL_YR")).otherwise(lpad(col("v_NISS_MNFCTRS_MDL_YR"), 2, "0")))
                .withColumn("O_CVG_CD", when(trim(col("NISS_CVG_CD")) == "", lit("??????")).otherwise(col("NISS_CVG_CD")))
                .withColumn("O_CLASS_CD", when(trim(col("NISS_CLASS_CD")) == "", lit("??????")).otherwise(col("NISS_CLASS_CD")))
            )
        except Exception:
            self.logger.exception("Failed applying EXP_PASS_TRGT expressions")
            raise

        # EXP_defaults: simple pass-through select
        try:
            self.logger.info("Applying EXP_defaults pass-through select", node="EXP_defaults")
            cols = [
                "NISS_CMPNY_CD", "CLNDR_YR", "CALL_YR", "NISS_ST_CD", "LINE_CD", "ACCDNT_YR",
                "NISS_CVG_CD", "NISS_TERR_CD", "RTNG_ZNE_CD", "TERM_ZNE_CD", "GRGNG_ZIP_5",
                "NISS_CLASS_CD", "NISS_ELIG_PNTS_CD", "NISS_AGE_GRP_CD", "NISS_MNFCTRS_MDL_YR",
                "NISS_CMMCL_IND_CD", "NISS_EXCPN_CD", "NISS_FGVNS_CD", "NISS_PASSV_RESTRA_CD",
                "NISS_DEFNS_DRVR_CRD_CD", "NISS_ANTI_THFT_DVC_CD", "NISS_DAY_TM_RUN_LAMPS_DISC_CD",
                "CVG_EXPS_VAL", "TTL_WRITTN_PREM_AMT", "NISS_PD_LOSS", "NISS_PD_ALLOC_ADJUS_EXPNS",
                "NISS_OUTSTNDG_LOSS", "NISS_NO_PD_CLMS", "NISS_NO_OUTSTND_CLMS", "RSVD_NISS_USE",
                "NISS_RSVD_CMPNY_USE"
            ]
            # select only the columns that actually exist on the incoming dataframe
            cols_to_select = [c for c in cols if c in df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL_relaxed_schrodinger.columns]
            df_EXP_defaults_pensive_maxwell = df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL_relaxed_schrodinger.select(*cols_to_select)
        except Exception:
            self.logger.exception("Failed applying EXP_defaults pass-through")
            raise

        # FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Summary: flat-file CSV write (delimited)
        try:
            self.logger.info("Writing flat-file Summary (stage and move single part) to PM_TARGET_FILE_DIR", node="FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Summary")
            tmp_dir = os.path.join(PM_TARGET_FILE_DIR, "_tmp_FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Summary")
            # stage single CSV part
            df_EX_PASS_SUMRY_amazing_hopper.coalesce(1).write.mode("overwrite").option("header", True).option("sep", ",").csv(tmp_dir)
            part = glob.glob(os.path.join(tmp_dir, "part-*.csv"))[0]
            target_path = os.path.join(PM_TARGET_FILE_DIR, OUTPUTFILE_PREMRPT3_SUM)
            shutil.move(part, target_path)
            shutil.rmtree(tmp_dir)
            df_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Summary_nostalgic_pasteur = df_EX_PASS_SUMRY_amazing_hopper
        except Exception:
            self.logger.exception("Failed writing FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Summary flat file")
            raise

        # FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail: flat-file CSV write (delimited)
        try:
            self.logger.info("Writing flat-file Detail (stage and move single part) to PM_TARGET_FILE_DIR", node="FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail")
            tmp_dir = os.path.join(PM_TARGET_FILE_DIR, "_tmp_FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail")
            df_EXP_PASS_DETL_optimistic_lovelace.coalesce(1).write.mode("overwrite").option("header", True).option("sep", ",").csv(tmp_dir)
            part = glob.glob(os.path.join(tmp_dir, "part-*.csv"))[0]
            target_path = os.path.join(PM_TARGET_FILE_DIR, OUTPUTFILE_PREMRPT1_DET)
            shutil.move(part, target_path)
            shutil.rmtree(tmp_dir)
            df_FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail_careful_turing = df_EXP_PASS_DETL_optimistic_lovelace
        except Exception:
            self.logger.exception("Failed writing FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail flat file")
            raise

        # FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptXcpn: flat-file CSV write (delimited)
        try:
            self.logger.info("Writing flat-file Exception report (stage and move single part) to PM_TARGET_FILE_DIR", node="FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptXcpn")
            tmp_dir = os.path.join(PM_TARGET_FILE_DIR, "_tmp_FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptXcpn")
            df_EXP_PASS_TRGT_calm_dirac.coalesce(1).write.mode("overwrite").option("header", True).option("sep", ",").csv(tmp_dir)
            part = glob.glob(os.path.join(tmp_dir, "part-*.csv"))[0]
            target_path = os.path.join(PM_TARGET_FILE_DIR, OUTPUTFILE_PREMRPT_EXP)
            shutil.move(part, target_path)
            shutil.rmtree(tmp_dir)
            df_ff_BIRP_NU0C_NISS_ATPRM_RptXcpn_tender_euclid = df_EXP_PASS_TRGT_calm_dirac
        except Exception:
            self.logger.exception("Failed writing FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptXcpn flat file")
            raise

        # EXP_PASS_FINAL_Csv: expression transformations for final CSV
        try:
            self.logger.info("Applying EXP_PASS_FINAL_Csv expressions", node="EXP_PASS_FINAL_Csv")
            # compute various o_* outputs and local variable
            df_EXP_PASS_FINAL_Csv_jovial_nash = (
                df_EXP_defaults_pensive_maxwell
                .withColumn("o_CLNDR_YR", substring(trim(col("CLNDR_YR")), 3, 2))
                .withColumn("o_REG_PRD_YR", substring(trim(col("CALL_YR")), 3, 2))
                .withColumn("o_NISS_CVG_CD", when(trim(col("NISS_CVG_CD")) == "", lit("???")).otherwise(col("NISS_CVG_CD")))
                .withColumn("o_NISS_TERR_CD", when(trim(col("NISS_TERR_CD")).eqNullSafe(lit("")), lit("???")).otherwise(col("NISS_TERR_CD")))
                .withColumn("v_NISS_MNFCTRS_MDL_YR", trim(col("NISS_MNFCTRS_MDL_YR").cast("string")))
                .withColumn("o_NISS_MNFCTRS_MDL_YR", when(col("v_NISS_MNFCTRS_MDL_YR").rlike("^\\d{2}$"), col("v_NISS_MNFCTRS_MDL_YR")).otherwise(lpad(col("v_NISS_MNFCTRS_MDL_YR"), 2, "0")))
            )
        except Exception:
            self.logger.exception("Failed applying EXP_PASS_FINAL_Csv expressions")
            raise

        # EXP_PASS_FINAL_Txt: expression transformations for final TXT (padded/LPAD etc.)
        try:
            self.logger.info("Applying EXP_PASS_FINAL_Txt expressions", node="EXP_PASS_FINAL_Txt")
            df_EXP_PASS_FINAL_Txt_magical_faraday = (
                df_EXP_defaults_pensive_maxwell
                .withColumn("o_CLNDR_YR", substring(trim(col("CLNDR_YR")), 3, 2))
                .withColumn("o_REG_PRD_YR", substring(trim(col("CALL_YR")), 3, 2))
                .withColumn("o_NISS_CVG_CD", when(trim(col("NISS_CVG_CD")) == "", lit("???")).otherwise(col("NISS_CVG_CD")))
                .withColumn("o_NISS_TERR_CD", when(trim(col("NISS_TERR_CD")).eqNullSafe(lit("")), lit("???")).otherwise(col("NISS_TERR_CD")))
                .withColumn("v_CVG_EXPS_VAL", trim(col("CVG_EXPS_VAL").cast("string")))
                .withColumn("CVG_EXPS_VAL", lpad(regexp_replace(col("v_CVG_EXPS_VAL"), '\\.', ''), 8, '0'))
                .withColumn("v_TTL_WRITTN_PREM_AMT", trim(col("TTL_WRITTN_PREM_AMT").cast("string")))
                .withColumn("TTL_WRITTN_PREM_AMT", lpad(regexp_replace(col("v_TTL_WRITTN_PREM_AMT"), '\\.', ''), 8, '0'))
                .withColumn("o_NISS_PD_LOSS", lpad(col("NISS_PD_LOSS").cast("string"), 8, '0'))
                .withColumn("o_NISS_PD_ALLOC_ADJUS_EXPNS", lpad(col("NISS_PD_ALLOC_ADJUS_EXPNS").cast("string"), 8, '0'))
                .withColumn("o_NISS_OUTSTNDG_LOSS", lpad(col("NISS_OUTSTNDG_LOSS").cast("string"), 8, '0'))
                .withColumn("o_NISS_NO_PD_CLMS", lpad(col("NISS_NO_PD_CLMS").cast("string"), 5, '0'))
                .withColumn("o_NISS_NO_OUTSTND_CLMS", lpad(col("NISS_NO_OUTSTND_CLMS").cast("string"), 5, '0'))
            )
        except Exception:
            self.logger.exception("Failed applying EXP_PASS_FINAL_Txt expressions")
            raise

        # FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Final_Csv: write final CSV file (single part)
        try:
            self.logger.info("Writing final CSV (stage and move single part) to PM_TARGET_FILE_DIR", node="FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Final_Csv")
            tmp_dir = os.path.join(PM_TARGET_FILE_DIR, "_tmp_FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Final_Csv")
            df_EXP_PASS_FINAL_Csv_jovial_nash.coalesce(1).write.mode("overwrite").option("header", True).option("sep", ",").csv(tmp_dir)
            part = glob.glob(os.path.join(tmp_dir, "part-*.csv"))[0]
            target_path = os.path.join(PM_TARGET_FILE_DIR, OUTPUTFILE_PREMRPT2_FNLCSV)
            shutil.move(part, target_path)
            shutil.rmtree(tmp_dir)
            df_FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Final_fierce_heisenberg = df_EXP_PASS_FINAL_Csv_jovial_nash
        except Exception:
            self.logger.exception("Failed writing FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Final_Csv flat file")
            raise

        # FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Final_Txt: write final TXT file (single part, no header)
        try:
            self.logger.info("Writing final TXT (stage and move single part) to PM_TARGET_FILE_DIR", node="FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Final_Txt")
            tmp_dir = os.path.join(PM_TARGET_FILE_DIR, "_tmp_FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Final_Txt")
            # write without header for positional/fixed layout
            df_EXP_PASS_FINAL_Txt_magical_faraday.coalesce(1).write.mode("overwrite").option("header", False).csv(tmp_dir)
            part = glob.glob(os.path.join(tmp_dir, "part-*.csv"))[0]
            target_path = os.path.join(PM_TARGET_FILE_DIR, OUTPUTFILE_PREMRPT2_FNLTXT)
            shutil.move(part, target_path)
            shutil.rmtree(tmp_dir)
            df_FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Final_Txt_careful_socrates = df_EXP_PASS_FINAL_Txt_magical_faraday
        except Exception:
            self.logger.exception("Failed writing FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Final_Txt flat file")
            raise

        self.glue_job.commit()

    def finalize(self) -> None:
        self.logger.info("Finalizing m_NU0C_NISS_AUTO_119_ATPRM_CreateReports")

    @staticmethod
    def _require_env(env: Mapping[str, Any], key: str) -> str:

        value = env.get(key)

        if not value or str(value).startswith("REPLACE_WITH_"):
            raise ConfigurationValidationError(
                f"config.yaml 'env.{key}' must be set"
            )

        return str(value)
