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
    lit,
    trim,
    when,
    regexp_replace,
    substring,
    lpad,
    length,
    split,
    element_at,
    concat_ws,
    current_date,
    date_format,
    broadcast,
    coalesce,
    lag,
)
from pyspark.sql.window import Window
import os
import glob
import shutil


# Informatica mapping m_NU0C_NISS_AUTO_119_ATPRM_DTL_SPLIT.
# Every mapping node runs in order inside run().
class Nu0cNissAuto119AtprmDtlSplitJob(BaseJob):

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

        # environment / mapping parameters used by expressions and lookup
        TIME_ZONE_LOOKUP_PATH = self._require_env(self.env, "TIME_ZONE_LOOKUP_PATH")
        RUN_TYPE_TZ = self._require_env(self.env, "RUN_TYPE_TZ")
        FILE_YEAR = self._require_env(self.env, "FILE_YEAR")
        PM_TARGET_FILE_DIR = self._require_env(self.env, "PM_TARGET_FILE_DIR")

        # FDR_LIB_WRK_BIRP_NISS_APRM_DETL: Source - try staged parquet first, fall back to Glue Catalog
        try:
            self.logger.info("Attempting to read staged parquet for FDR_LIB_WRK_BIRP_NISS_APRM_DETL from S3", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_relaxed_socrates = self.parquet_reader.read(
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                options=None,
            )
            # read succeeded from staged parquet
            self.logger.info("Read WRK_BIRP_NISS_APRM_DETL from staged parquet", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            if df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_relaxed_socrates.isEmpty():
                self.logger.warning("Source WRK_BIRP_NISS_APRM_DETL returned no rows", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
        except Exception:
            # fallback to Glue Data Catalog read
            try:
                self.logger.warning("Staged parquet not available; falling back to Glue Catalog for WRK_BIRP_NISS_APRM_DETL", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
                dyf = self.glue_context.create_dynamic_frame.from_catalog(
                    database=self.glue_database,
                    table_name="WRK_BIRP_NISS_APRM_DETL",
                    transformation_ctx="df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_relaxed_socrates",
                )
                df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_relaxed_socrates = dyf.toDF().select(
                    "NISS_APRM_DETL_SK",
                    "CLNDR_YR",
                    "CALL_YR",
                    "NAIC_CMPNY_CD",
                    "NISS_CMPNY_CD",
                    "ST_NM",
                    "ST_CD",
                    "NISS_ST_CD",
                    "ST_ABBR",
                    "ACCTNG_LOB",
                    "CVG_TYP_CD",
                    "CVG_AMT",
                    "BI_LMT",
                    "GA_ADDED_AT_FAULT_IND",
                    "FA2_PLCY_IND",
                    "UM_UMI_STACKING",
                    "PIP_WVR_WL_IND",
                    "PIP_MED_SEC_IND",
                    "PIP_LOSS_INCOME_IND",
                    "MI_PPO_IND",
                    "PRD_GRP_CD",
                    "NJ_HLTH_INSR_PRIM",
                    "NJ_EXTR_PIP_PKG",
                    "NJ_RESDNC_RLTNSHP_PIP_IND",
                    "NY_SSL_IND",
                    "NY_FULL_CVG_GLASS_COMP_IND",
                    "GRGNG_ZIP_5",
                    "NISS_TERR_CD",
                    "RATNG_CMPY_CD",
                    "MLT_CAR_IND",
                    "RT_CLS",
                    "AGE",
                    "GENDR",
                    "MRTL_STAT",
                    "AUTO_USE_CD",
                    "MILES_TO_WRK",
                    "GOOD_STDNT_IND",
                    "DRVR_TRNG_IND",
                    "SOI_TYP",
                    "PHY_DMG_IND",
                    "NJ_RATD_PNTS",
                    "VEH_MDL_YR",
                    "NJ_EXCPTION_CD",
                    "NJ_FGVN_PNTS",
                    "PASSV_RESTRA_DISC",
                    "SNR_DRVR_IND",
                    "DEFNS_DRVR_DISC_IND",
                    "ANTI_THFT_DISC",
                    "DAY_TM_RUN_LIGHTS",
                    "LMT_TORT",
                    "ANNL_STMNT_LOB_CD",
                    "CVG_TYP_IND",
                    "CVG_EXPS_VAL",
                    "TTL_WRITTN_PREM_AMT",
                    "LINE_CD",
                    "ACCDNT_YR",
                    "NISS_CVG_CD",
                    "RTNG_ZNE_CD",
                    "TERM_ZNE_CD",
                    "NISS_CLASS_CD",
                    "NISS_ELIG_PNTS_CD",
                    "NISS_AGE_GRP_CD",
                    "NISS_CMMCL_IND_CD",
                    "NISS_EXCPN_CD",
                    "NISS_FGVNS_CD",
                    "NISS_PASSV_RESTRA_CD",
                    "NISS_DEFNS_DRVR_CRD_CD",
                    "NISS_ANTI_THFT_DVC_CD",
                    "NISS_DAY_TM_RUN_LAMPS_DISC_CD",
                    "NISS_PLCY_LMT_CD",
                    "NISS_DEDUC_CD",
                    "NISS_SSL_LIAB_CD",
                    "NISS_SUBLOB_CD",
                    "NISS_TYP_LOSS_CD",
                    "NISS_LIAB_OR_NO_FAULT_CD",
                    "NISS_ANNL_STMNT_LOB_CD",
                    "NISS_PD_LOSS",
                    "NISS_PD_ALLOC_ADJUS_EXPNS",
                    "NISS_OUTSTNDG_LOSS",
                    "NISS_NO_PD_CLMS",
                    "NISS_NO_OUTSTND_CLMS",
                    "RSVD_NISS_USE",
                    "NISS_RSVD_CMPNY_USE",
                    "NISS_MNFCTRS_MDL_YR",
                    "CR_BY_MAPNG_ID",
                    "DW_CR_TMSP",
                    "UPD_BY_MAPNG_ID",
                    "DW_UPD_TMSP",
                    "WRK_FLOW_RUN_ID",
                    "NJ_NO_LWST_LMT_IND",
                    "NJ_NMD_DRVR_EXCL_IND",
                    "EXPS_VAL_ROLLED",
                    "CVG_CNT_IND",
                    "CVG_CNT",
                    "CVG_CD_SK",
                    "CVG_ATTR_SK",
                    "REC_DROP_IND",
                    "REC_DROP_RSN_DESC",
                    "REC_EXCPN_IND",
                    "REC_EXCPN_RSN_DESC",
                    "CVG_ATTR_CHCKSUM",
                    "COMP_DED",
                    "COLL_DED",
                    "PLCY_CNTRCT_NUM",
                    "UNIT_NUM",
                    "EFF_DT",
                    "NUM_OF_CARS_IN_HH",
                    "RDRVR_DT_OF_BRTH",
                    "TERM_STRT_DT",
                    "SRC_SYS_CD",
                    "DERIVED_RDRVR_AGE",
                    "FINAL_RDRVR_AGE",
                    "PNI_AGE",
                    "LOB",
                    "PRINCIPAL_OPRT",
                    "SOURCE_IND_DERIVED",
                )
                if df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_relaxed_socrates.isEmpty():
                    self.logger.warning("Glue Catalog read WRK_BIRP_NISS_APRM_DETL returned no rows", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            except Exception:
                self.logger.exception("Failed reading FDR_LIB_WRK_BIRP_NISS_APRM_DETL from Glue Catalog")
                raise

        # SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL: Application Source Qualifier - SQL Override executed against staged temp view
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
SUM(CVG_EXPS_VAL) AS CVG_EXPS_VAL,
SUM(EXPS_VAL_ROLLED) AS EXPS_VAL_ROLLED,
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
REC_EXCPN_IND,
REC_DROP_IND,
REC_DROP_RSN_DESC,
NUM_OF_CARS_IN_HH,
TO_CHAR(TERM_STRT_DT,'MM/DD/YYYY')TERM_STRT_DT,
RDRVR_DT_OF_BRTH,
SRC_SYS_CD,
PNI_AGE,
DERIVED_RDRVR_AGE,
FINAL_RDRVR_AGE

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
NUM_OF_CARS_IN_HH,
TO_CHAR(TERM_STRT_DT,'MM/DD/YYYY'),
RDRVR_DT_OF_BRTH,
SRC_SYS_CD,
PNI_AGE,
DERIVED_RDRVR_AGE,
FINAL_RDRVR_AGE"""

        try:
            self.logger.info("Running SQL override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL against staged view", node="SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            # register the staged dataframe as a temp view for the SQL override
            df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_relaxed_socrates.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")
            df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_humble_hawking = self.spark.sql(sql_query)
            if df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_humble_hawking.isEmpty():
                self.logger.warning("SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL returned no rows", node="SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
        except Exception:
            self.logger.exception("Failed executing SQL override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            raise

        # EXP_GenErrReason: parse semicolon-delimited i_REC_DROP_RSN_DESC into tokens and reassemble
        try:
            self.logger.info("Applying EXP_GenErrReason parsing expressions", node="EXP_GenErrReason")
            df_EXP_GenErrReason_focused_euclid = (
                df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_humble_hawking
                .withColumn("_tokens", split(col("i_REC_DROP_RSN_DESC"), ";"))
                .withColumn("token1", trim(element_at(col("_tokens"), 1)))
                .withColumn("token2", trim(element_at(col("_tokens"), 2)))
                .withColumn("token3", trim(element_at(col("_tokens"), 3)))
                .withColumn("token4", trim(element_at(col("_tokens"), 4)))
                .withColumn(
                    "REC_DROP_RSN_DESC",
                    when(
                        col("i_REC_DROP_RSN_DESC").isNull() | (trim(col("i_REC_DROP_RSN_DESC")) == ""),
                        lit("")
                    ).otherwise(
                        concat_ws(
                            ";",
                            coalesce(col("token1"), lit("")),
                            coalesce(col("token2"), lit("")),
                            coalesce(col("token3"), lit("")),
                            coalesce(col("token4"), lit("")),
                        )
                    ),
                )
                .drop("_tokens")
            )
        except Exception:
            self.logger.exception("Failed applying EXP_GenErrReason parsing expressions")
            raise

        # EXP_PASS_DETL: union SQ + GenErrReason (if both), then compute normalization/output ports
        try:
            self.logger.info("Applying EXP_PASS_DETL expressions", node="EXP_PASS_DETL")
            # union the two inputs if both present
            df_union_source = None
            if 'df_EXP_GenErrReason_focused_euclid' in globals() and df_EXP_GenErrReason_focused_euclid is not None:
                df_union_source = df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_humble_hawking.unionByName(df_EXP_GenErrReason_focused_euclid)
            else:
                df_union_source = df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_humble_hawking

            df_EXP_PASS_DETL_busy_feynman = (
                df_union_source
                # local var v_NISS_MNFCTRS_MDL_YR: trimmed string rep
                .withColumn("v_NISS_MNFCTRS_MDL_YR", trim(col("NISS_MNFCTRS_MDL_YR").cast("string")))
                # o_NISS_MNFCTRS_MDL_YR: pad to length 2 with leading zero when needed
                .withColumn(
                    "o_NISS_MNFCTRS_MDL_YR",
                    when((col("v_NISS_MNFCTRS_MDL_YR").isNull()) | (trim(col("v_NISS_MNFCTRS_MDL_YR")) == ""), lit("")).otherwise(
                        when(length(col("v_NISS_MNFCTRS_MDL_YR")) == 2, col("v_NISS_MNFCTRS_MDL_YR")).otherwise(lpad(col("v_NISS_MNFCTRS_MDL_YR"), 2, "0"))
                    )
                )
                # o_NISS_CVG_CD: fallback '???' when blank/null
                .withColumn(
                    "o_NISS_CVG_CD",
                    when((col("NISS_CVG_CD").isNull()) | (trim(col("NISS_CVG_CD")) == ""), lit("???")).otherwise(col("NISS_CVG_CD"))
                )
                # o_NISS_CLASS_CD: fallback '??????' when blank/null
                .withColumn(
                    "o_NISS_CLASS_CD",
                    when((col("NISS_CLASS_CD").isNull()) | (trim(col("NISS_CLASS_CD")) == ""), lit("??????")).otherwise(col("NISS_CLASS_CD"))
                )
                # o_CVG_AMT and o_BI_LMT: remove commas and trim
                .withColumn("o_CVG_AMT", regexp_replace(trim(col("CVG_AMT")), ',', ''))
                .withColumn("o_BI_LMT", regexp_replace(trim(col("BI_LMT")), ',', ''))
                # o_CLNDR_YR / o_CALL_YR: SUBSTR(LTRIM(RTRIM(...)),3,2)
                .withColumn("o_CLNDR_YR", substring(trim(col("CLNDR_YR")), 3, 2))
                .withColumn("o_CALL_YR", substring(trim(col("CALL_YR")), 3, 2))
                # EXP_STATE_IND: Y for CA or TX else N
                .withColumn("EXP_STATE_IND", when(col("ST_ABBR").isin("CA", "TX"), lit("Y")).otherwise(lit("N")))
            )

            # filename_DTL construction using mapping-level params
            # v_file_year: if RUN_TYPE_TZ == 'MANUAL' then FILE_YEAR else current year
            v_file_year_col = (when(lit(RUN_TYPE_TZ) == lit('MANUAL'), lit(FILE_YEAR)).otherwise(date_format(current_date(), 'yyyy')))
            df_EXP_PASS_DETL_busy_feynman = df_EXP_PASS_DETL_busy_feynman.withColumn("v_file_year", v_file_year_col)
            df_EXP_PASS_DETL_busy_feynman = df_EXP_PASS_DETL_busy_feynman.withColumn(
                "filename_DTL",
                concat_ws('', lit('FF_FIO_TRIP_DTL_RPT_'), col('ST_ABBR'), lit('_'), col('v_file_year'), lit('.csv'))
            )
        except Exception:
            self.logger.exception("Failed applying EXP_PASS_DETL expressions")
            raise

        # LKPTRANS: Lookup Procedure - read flat-file csv and expose ST_ABBR / TimeZone (cached/broadcast)
        try:
            self.logger.info("Reading lookup flat file for LKPTRANS", node="LKPTRANS")
            lookup_df = (
                self.spark.read.option("header", "true").csv(TIME_ZONE_LOOKUP_PATH)
            )
            # normalize key column name to ST_ABBR for easy join
            df_LKPTRANS_stoic_babbage = (
                lookup_df
                .withColumn("EXPOSURE_STATE_ABB", trim(col("EXPOSURE_STATE_ABB")))
                .withColumnRenamed("EXPOSURE_STATE_ABB", "ST_ABBR")
                .select("ST_ABBR", "TimeZone")
            )
            # cache by broadcasting when joining later
            # (we don't call .cache() here because broadcast is used at join site)
            if df_LKPTRANS_stoic_babbage.rdd.isEmpty():
                self.logger.warning("Lookup source for LKPTRANS returned no rows", node="LKPTRANS")
        except Exception:
            self.logger.exception("Failed reading lookup source for LKPTRANS")
            raise

        # Router: left-join lookup result into detail stream, then create per-group filtered dataframes
        try:
            self.logger.info("Applying Router join and group splits", node="Router")
            # join the lookup results (df_LKPTRANS_stoic_babbage) onto the detail stream
            base_df = df_EXP_PASS_DETL_busy_feynman.join(
                df_LKPTRANS_stoic_babbage.select("ST_ABBR", "TimeZone"),
                on=[df_EXP_PASS_DETL_busy_feynman.ST_ABBR == df_LKPTRANS_stoic_babbage.ST_ABBR],
                how="left",
            ).drop(df_LKPTRANS_stoic_babbage.ST_ABBR)

            df_Router_hopeful_pasteur = base_df

            # group-specific filtered dataframes
            df_Router_hopeful_pasteur_CA = base_df.filter(col("ST_ABBR") == "CA")
            df_Router_hopeful_pasteur_TX = base_df.filter(col("ST_ABBR") == "TX")
            df_Router_hopeful_pasteur_NON_CA_TX = base_df.filter(~col("ST_ABBR").isin("CA", "TX"))
            # DEFAULT1: records not in the union of the other three groups (safe set-difference)
            df_Router_hopeful_pasteur_DEFAULT1 = base_df.exceptAll(
                df_Router_hopeful_pasteur_CA.unionByName(df_Router_hopeful_pasteur_TX).unionByName(df_Router_hopeful_pasteur_NON_CA_TX)
            )
        except Exception:
            self.logger.exception("Failed applying Router join and group splits")
            raise

        # SRT_CA: Sorter - operate on the Router's CA group filtered dataframe and order by NAIC_CMPNY_CD
        try:
            self.logger.info("Applying SRT_CA ordering", node="SRT_CA")
            df_SRT_CA_peaceful_mendel = df_Router_hopeful_pasteur_CA.orderBy(col("NAIC_CMPNY_CD").asc())
        except Exception:
            self.logger.exception("Failed applying SRT_CA ordering")
            raise

        # SRT_TX: Sorter - operate on the Router's TX group filtered dataframe and order by NAIC_CMPNY_CD
        try:
            self.logger.info("Applying SRT_TX ordering", node="SRT_TX")
            df_SRT_TX_sharp_heisenberg = df_Router_hopeful_pasteur_TX.orderBy(col("NAIC_CMPNY_CD").asc())
        except Exception:
            self.logger.exception("Failed applying SRT_TX ordering")
            raise


        # NON_CA_TX: Expression - passthrough ports renamed to suffix '4' and compute O_TIMEZONE4
        try:
            self.logger.info("Applying NON_CA_TX expressions", node="NON_CA_TX")

            # mapping source columns (without suffix) -> target columns (with suffix _4)
            rename_map = [
                ("o_CLNDR_YR", "o_CLNDR_YR4"),
                ("o_CALL_YR", "o_CALL_YR4"),
                ("NAIC_CMPNY_CD", "NAIC_CMPNY_CD4"),
                ("NISS_CMPNY_CD", "NISS_CMPNY_CD4"),
                ("ST_NM", "ST_NM4"),
                ("ST_CD", "ST_CD4"),
                ("NISS_ST_CD", "NISS_ST_CD4"),
                ("ST_ABBR", "ST_ABBR4"),
                ("ACCTNG_LOB", "ACCTNG_LOB4"),
                ("CVG_TYP_CD", "CVG_TYP_CD4"),
                ("CVG_TYP_IND", "CVG_TYP_IND4"),
                ("o_CVG_AMT", "o_CVG_AMT4"),
                ("o_BI_LMT", "o_BI_LMT4"),
                ("GA_ADDED_AT_FAULT_IND", "GA_ADDED_AT_FAULT_IND4"),
                ("FA2_PLCY_IND", "FA2_PLCY_IND4"),
                ("UM_UMI_STACKING", "UM_UMI_STACKING4"),
                ("PIP_WVR_WL_IND", "PIP_WVR_WL_IND4"),
                ("PIP_MED_SEC_IND", "PIP_MED_SEC_IND4"),
                ("PIP_LOSS_INCOME_IND", "PIP_LOSS_INCOME_IND4"),
                ("MI_PPO_IND", "MI_PPO_IND4"),
                ("PRD_GRP_CD", "PRD_GRP_CD4"),
                ("NJ_HLTH_INSR_PRIM", "NJ_HLTH_INSR_PRIM4"),
                ("NJ_EXTR_PIP_PKG", "NJ_EXTR_PIP_PKG4"),
                ("NJ_RESDNC_RLTNSHP_PIP_IND", "NJ_RESDNC_RLTNSHP_PIP_IND4"),
                ("NY_SSL_IND", "NY_SSL_IND4"),
                ("NY_FULL_CVG_GLASS_COMP_IND", "NY_FULL_CVG_GLASS_COMP_IND4"),
                ("GRGNG_ZIP_5", "GRGNG_ZIP_54"),
                ("NISS_TERR_CD", "o_NISS_TERR_CD4"),
                ("RATNG_CMPY_CD", "RATNG_CMPY_CD4"),
                ("MLT_CAR_IND", "MLT_CAR_IND4"),
                ("RT_CLS", "RT_CLS4"),
                ("AGE", "AGE4"),
                ("GENDR", "GENDR4"),
                ("MRTL_STAT", "MRTL_STAT4"),
                ("AUTO_USE_CD", "AUTO_USE_CD4"),
                ("MILES_TO_WRK", "MILES_TO_WRK4"),
                ("GOOD_STDNT_IND", "GOOD_STDNT_IND4"),
                ("DRVR_TRNG_IND", "DRVR_TRNG_IND4"),
                ("SOI_TYP", "SOI_TYP4"),
                ("PHY_DMG_IND", "PHY_DMG_IND4"),
                ("NJ_RATD_PNTS", "NJ_RATD_PNTS4"),
                ("VEH_MDL_YR", "VEH_MDL_YR4"),
                ("NJ_EXCPTION_CD", "NJ_EXCPTION_CD4"),
                ("NJ_FGVN_PNTS", "NJ_FGVN_PNTS4"),
                ("NJ_NO_LWST_LMT_IND", "NJ_NO_LWST_LMT_IND4"),
                ("NJ_NMD_DRVR_EXCL_IND", "NJ_NMD_DRVR_EXCL_IND4"),
                ("PASSV_RESTRA_DISC", "PASSV_RESTRA_DISC4"),
                ("SNR_DRVR_IND", "SNR_DRVR_IND4"),
                ("DEFNS_DRVR_DISC_IND", "DEFNS_DRVR_DISC_IND4"),
                ("ANTI_THFT_DISC", "ANTI_THFT_DISC4"),
                ("DAY_TM_RUN_LIGHTS", "DAY_TM_RUN_LIGHTS4"),
                ("LMT_TORT", "LMT_TORT4"),
                ("CVG_EXPS_VAL", "CVG_EXPS_VAL4"),
                ("EXPS_VAL_ROLLED", "EXPS_VAL_ROLLED4"),
                ("TTL_WRITTN_PREM_AMT", "TTL_WRITTN_PREM_AMT4"),
                ("NUM_OF_CARS_IN_HH", "NUM_OF_CARS_IN_HH4"),
                ("PNI_AGE", "PNI_AGE4"),
                ("RDRVR_DT_OF_BRTH", "RDRVR_DT_OF_BRTH4"),
                ("TERM_STRT_DT", "TERM_STRT_DT4"),
                ("SRC_SYS_CD", "SRC_SYS_CD4"),
                ("DERIVED_RDRVR_AGE", "DERIVED_RDRVR_AGE4"),
                ("FINAL_RDRVR_AGE", "FINAL_RDRVR_AGE4"),
                ("LINE_CD", "LINE_CD4"),
                ("ACCDNT_YR", "ACCDNT_YR4"),
                ("NISS_CVG_CD", "o_NISS_CVG_CD4"),
                ("RTNG_ZNE_CD", "RTNG_ZNE_CD4"),
                ("TERM_ZNE_CD", "TERM_ZNE_CD4"),
                ("NISS_CLASS_CD", "o_NISS_CLASS_CD4"),
                ("NISS_ELIG_PNTS_CD", "NISS_ELIG_PNTS_CD4"),
                ("NISS_AGE_GRP_CD", "NISS_AGE_GRP_CD4"),
                ("NISS_MNFCTRS_MDL_YR", "o_NISS_MNFCTRS_MDL_YR4"),
                ("NISS_CMMCL_IND_CD", "NISS_CMMCL_IND_CD4"),
                ("NISS_EXCPN_CD", "NISS_EXCPN_CD4"),
                ("NISS_FGVNS_CD", "NISS_FGVNS_CD4"),
                ("NISS_PASSV_RESTRA_CD", "NISS_PASSV_RESTRA_CD4"),
                ("NISS_DEFNS_DRVR_CRD_CD", "NISS_DEFNS_DRVR_CRD_CD4"),
                ("NISS_ANTI_THFT_DVC_CD", "NISS_ANTI_THFT_DVC_CD4"),
                ("NISS_DAY_TM_RUN_LAMPS_DISC_CD", "NISS_DAY_TM_RUN_LAMPS_DISC_CD4"),
                ("NISS_PLCY_LMT_CD", "NISS_PLCY_LMT_CD4"),
                ("NISS_DEDUC_CD", "NISS_DEDUC_CD4"),
                ("NISS_SSL_LIAB_CD", "NISS_SSL_LIAB_CD4"),
                ("NISS_SUBLOB_CD", "NISS_SUBLOB_CD4"),
                ("NISS_TYP_LOSS_CD", "NISS_TYP_LOSS_CD4"),
                ("NISS_LIAB_OR_NO_FAULT_CD", "NISS_LIAB_OR_NO_FAULT_CD4"),
                ("NISS_ANNL_STMNT_LOB_CD", "NISS_ANNL_STMNT_LOB_CD4"),
                ("NISS_PD_LOSS", "NISS_PD_LOSS4"),
                ("NISS_PD_ALLOC_ADJUS_EXPNS", "NISS_PD_ALLOC_ADJUS_EXPNS4"),
                ("NISS_OUTSTNDG_LOSS", "NISS_OUTSTNDG_LOSS4"),
                ("NISS_NO_PD_CLMS", "NISS_NO_PD_CLMS4"),
                ("NISS_NO_OUTSTND_CLMS", "NISS_NO_OUTSTND_CLMS4"),
                ("REC_EXCPN_IND", "REC_EXCPN_IND4"),
                ("REC_DROP_IND", "REC_DROP_IND4"),
                ("REC_DROP_RSN_DESC", "REC_DROP_RSN_DESC4"),
                ("EXP_STATE_IND", "EXP_STATE_IND4"),
                ("TimeZone", "TimeZone4"),
            ]

            df_in = df_Router_hopeful_pasteur

            # apply renames
            for src, tgt in rename_map:
                if src in df_in.columns:
                    df_in = df_in.withColumnRenamed(src, tgt)

            # ensure TimeZone4 exists and compute O_TIMEZONE4
            if "TimeZone4" not in df_in.columns:
                df_in = df_in.withColumn("TimeZone4", lit(None).cast("string"))

            df_NON_CA_TX_nice_bohr = (
                df_in
                .withColumn("O_TIMEZONE4", when(col("TimeZone4").isNull(), lit("UNKNOWN")).otherwise(col("TimeZone4")))
                .select(*[col(tgt) for (_, tgt) in rename_map] + [col("TimeZone4"), col("O_TIMEZONE4")])
            )

        except Exception:
            self.logger.exception("Failed applying NON_CA_TX expressions")
            raise


        # EXP_CA_STATE: Expression for CA group - rename inputs to suffix '1', compute previous-company indicator and filename_DTL
        try:
            self.logger.info("Applying EXP_CA_STATE expressions", node="EXP_CA_STATE")

            # rename mapping: source -> target with suffix _1
            rename_map_ca = [
                ("o_CLNDR_YR", "o_CLNDR_YR1"),
                ("o_CALL_YR", "o_CALL_YR1"),
                ("NAIC_CMPNY_CD", "NAIC_CMPNY_CD1"),
                ("NISS_CMPNY_CD", "NISS_CMPNY_CD1"),
                ("ST_NM", "ST_NM1"),
                ("ST_CD", "ST_CD1"),
                ("NISS_ST_CD", "NISS_ST_CD1"),
                ("ST_ABBR", "ST_ABBR1"),
                ("ACCTNG_LOB", "ACCTNG_LOB1"),
                ("CVG_TYP_CD", "CVG_TYP_CD1"),
                ("CVG_TYP_IND", "CVG_TYP_IND1"),
                ("o_CVG_AMT", "o_CVG_AMT1"),
                ("o_BI_LMT", "o_BI_LMT1"),
                ("GA_ADDED_AT_FAULT_IND", "GA_ADDED_AT_FAULT_IND1"),
                ("FA2_PLCY_IND", "FA2_PLCY_IND1"),
                ("UM_UMI_STACKING", "UM_UMI_STACKING1"),
                ("PIP_WVR_WL_IND", "PIP_WVR_WL_IND1"),
                ("PIP_MED_SEC_IND", "PIP_MED_SEC_IND1"),
                ("PIP_LOSS_INCOME_IND", "PIP_LOSS_INCOME_IND1"),
                ("MI_PPO_IND", "MI_PPO_IND1"),
                ("PRD_GRP_CD", "PRD_GRP_CD1"),
                ("NJ_HLTH_INSR_PRIM", "NJ_HLTH_INSR_PRIM1"),
                ("NJ_EXTR_PIP_PKG", "NJ_EXTR_PIP_PKG1"),
                ("NJ_RESDNC_RLTNSHP_PIP_IND", "NJ_RESDNC_RLTNSHP_PIP_IND1"),
                ("NY_SSL_IND", "NY_SSL_IND1"),
                ("NY_FULL_CVG_GLASS_COMP_IND", "NY_FULL_CVG_GLASS_COMP_IND1"),
                ("GRGNG_ZIP_5", "GRGNG_ZIP_51"),
                ("o_NISS_TERR_CD", "o_NISS_TERR_CD1"),
                ("RATNG_CMPY_CD", "RATNG_CMPY_CD1"),
                ("MLT_CAR_IND", "MLT_CAR_IND1"),
                ("RT_CLS", "RT_CLS1"),
                ("AGE", "AGE1"),
                ("GENDR", "GENDR1"),
                ("MRTL_STAT", "MRTL_STAT1"),
                ("AUTO_USE_CD", "AUTO_USE_CD1"),
                ("MILES_TO_WRK", "MILES_TO_WRK1"),
                ("GOOD_STDNT_IND", "GOOD_STDNT_IND1"),
                ("DRVR_TRNG_IND", "DRVR_TRNG_IND1"),
                ("SOI_TYP", "SOI_TYP1"),
                ("PHY_DMG_IND", "PHY_DMG_IND1"),
                ("NJ_RATD_PNTS", "NJ_RATD_PNTS1"),
                ("VEH_MDL_YR", "VEH_MDL_YR1"),
                ("NJ_EXCPTION_CD", "NJ_EXCPTION_CD1"),
                ("NJ_FGVN_PNTS", "NJ_FGVN_PNTS1"),
                ("NJ_NO_LWST_LMT_IND", "NJ_NO_LWST_LMT_IND1"),
                ("NJ_NMD_DRVR_EXCL_IND", "NJ_NMD_DRVR_EXCL_IND1"),
                ("PASSV_RESTRA_DISC", "PASSV_RESTRA_DISC1"),
                ("SNR_DRVR_IND", "SNR_DRVR_IND1"),
                ("DEFNS_DRVR_DISC_IND", "DEFNS_DRVR_DISC_IND1"),
                ("ANTI_THFT_DISC", "ANTI_THFT_DISC1"),
                ("DAY_TM_RUN_LIGHTS", "DAY_TM_RUN_LIGHTS1"),
                ("LMT_TORT", "LMT_TORT1"),
                ("CVG_EXPS_VAL", "CVG_EXPS_VAL1"),
                ("EXPS_VAL_ROLLED", "EXPS_VAL_ROLLED1"),
                ("TTL_WRITTN_PREM_AMT", "TTL_WRITTN_PREM_AMT1"),
                ("NUM_OF_CARS_IN_HH", "NUM_OF_CARS_IN_HH1"),
                ("PNI_AGE", "PNI_AGE1"),
                ("RDRVR_DT_OF_BRTH", "RDRVR_DT_OF_BRTH1"),
                ("TERM_STRT_DT", "TERM_STRT_DT1"),
                ("SRC_SYS_CD", "SRC_SYS_CD1"),
                ("DERIVED_RDRVR_AGE", "DERIVED_RDRVR_AGE1"),
                ("FINAL_RDRVR_AGE", "FINAL_RDRVR_AGE1"),
                ("LINE_CD", "LINE_CD1"),
                ("ACCDNT_YR", "ACCDNT_YR1"),
                ("o_NISS_CVG_CD", "o_NISS_CVG_CD1"),
                ("RTNG_ZNE_CD", "RTNG_ZNE_CD1"),
                ("TERM_ZNE_CD", "TERM_ZNE_CD1"),
                ("o_NISS_CLASS_CD", "o_NISS_CLASS_CD1"),
                ("NISS_ELIG_PNTS_CD", "NISS_ELIG_PNTS_CD1"),
                ("NISS_AGE_GRP_CD", "NISS_AGE_GRP_CD1"),
                ("o_NISS_MNFCTRS_MDL_YR", "o_NISS_MNFCTRS_MDL_YR1"),
                ("NISS_CMMCL_IND_CD", "NISS_CMMCL_IND_CD1"),
                ("NISS_EXCPN_CD", "NISS_EXCPN_CD1"),
                ("NISS_FGVNS_CD", "NISS_FGVNS_CD1"),
                ("NISS_PASSV_RESTRA_CD", "NISS_PASSV_RESTRA_CD1"),
                ("NISS_DEFNS_DRVR_CRD_CD", "NISS_DEFNS_DRVR_CRD_CD1"),
                ("NISS_ANTI_THFT_DVC_CD", "NISS_ANTI_THFT_DVC_CD1"),
                ("NISS_DAY_TM_RUN_LAMPS_DISC_CD", "NISS_DAY_TM_RUN_LAMPS_DISC_CD1"),
                ("NISS_PLCY_LMT_CD", "NISS_PLCY_LMT_CD1"),
                ("NISS_DEDUC_CD", "NISS_DEDUC_CD1"),
                ("NISS_SSL_LIAB_CD", "NISS_SSL_LIAB_CD1"),
                ("NISS_SUBLOB_CD", "NISS_SUBLOB_CD1"),
                ("NISS_TYP_LOSS_CD", "NISS_TYP_LOSS_CD1"),
                ("NISS_LIAB_OR_NO_FAULT_CD", "NISS_LIAB_OR_NO_FAULT_CD1"),
                ("NISS_ANNL_STMNT_LOB_CD", "NISS_ANNL_STMNT_LOB_CD1"),
                ("REC_EXCPN_IND", "REC_EXCPN_IND1"),
                ("REC_DROP_IND", "REC_DROP_IND1"),
                ("REC_DROP_RSN_DESC", "REC_DROP_RSN_DESC1"),
                ("EXP_STATE_IND", "EXP_STATE_IND1"),
                ("TimeZone", "TimeZone1"),
            ]

            df_ca = df_SRT_CA_peaceful_mendel
            for src, tgt in rename_map_ca:
                if src in df_ca.columns:
                    df_ca = df_ca.withColumnRenamed(src, tgt)

            # window to compute previous company by NAIC_CMPNY_CD1 ordering
            w_ca = Window.orderBy(col("NAIC_CMPNY_CD1"))
            prev_company = lag(col("NAIC_CMPNY_CD1")).over(w_ca)

            df_EXP_CA_STATE_humble_darwin = (
                df_ca
                .withColumn("V_PREV_CMPY_CD", prev_company)
                .withColumn("V_CURR_CMPY_CD", col("NAIC_CMPNY_CD1"))
                .withColumn("O_CMPY_CD_IND", when(col("V_CURR_CMPY_CD") == col("V_PREV_CMPY_CD"), lit("N")).otherwise(lit("Y")))
                .withColumn("v_file_year", when(lit(RUN_TYPE_TZ) == lit('MANUAL'), lit(FILE_YEAR)).otherwise(date_format(current_date(), 'yyyy')))
                .withColumn("filename_DTL", concat_ws('', lit('NU0C_NISS_AUTO_CA_'), col('v_file_year'), lit('_AutoPrem_Detail_'), col('NAIC_CMPNY_CD1'), lit('.csv')))
                .select(*[col(tgt) for (_, tgt) in rename_map_ca] + [col("V_PREV_CMPY_CD"), col("V_CURR_CMPY_CD"), col("O_CMPY_CD_IND"), col("filename_DTL")])
            )

        except Exception:
            self.logger.exception("Failed applying EXP_CA_STATE expressions")
            raise


        # EXP_TX_STATE: Expression for TX group - similar logic to CA but with suffix '3'
        try:
            self.logger.info("Applying EXP_TX_STATE expressions", node="EXP_TX_STATE")

            rename_map_tx = []
            # build rename map by reusing CA mapping but target suffix _3
            for src, tgt in rename_map_ca:
                # derive base target name without trailing '1' and make it '3'
                base = tgt[:-1] if tgt.endswith('1') else tgt
                new_tgt = base + '3'
                rename_map_tx.append((src, new_tgt))

            df_tx = df_SRT_TX_sharp_heisenberg
            for src, tgt in rename_map_tx:
                if src in df_tx.columns:
                    df_tx = df_tx.withColumnRenamed(src, tgt)

            w_tx = Window.orderBy(col("NAIC_CMPNY_CD3"))
            prev_company_tx = lag(col("NAIC_CMPNY_CD3")).over(w_tx)

            df_EXP_TX_STATE_jovial_newton = (
                df_tx
                .withColumn("V_PREV_CMPY_CD", prev_company_tx)
                .withColumn("V_CURR_CMPY_CD", col("NAIC_CMPNY_CD3"))
                .withColumn("O_CMPY_CD_IND", when(col("V_CURR_CMPY_CD") == col("V_PREV_CMPY_CD"), lit("N")).otherwise(lit("Y")))
                .withColumn("v_file_year", when(lit(RUN_TYPE_TZ) == lit('MANUAL'), lit(FILE_YEAR)).otherwise(date_format(current_date(), 'yyyy')))
                .withColumn("filename_DTL", concat_ws('', lit('NU0C_NISS_AUTO_TX_'), col('v_file_year'), lit('_AutoPrem_Detail_'), col('NAIC_CMPNY_CD3'), lit('.csv')))
                .select(*[col(tgt) for (_, tgt) in rename_map_tx] + [col("V_PREV_CMPY_CD"), col("V_CURR_CMPY_CD"), col("O_CMPY_CD_IND"), col("filename_DTL")])
            )

        except Exception:
            self.logger.exception("Failed applying EXP_TX_STATE expressions")
            raise


        # SRT_TIMEZONE: Sort NON_CA_TX by O_TIMEZONE4 ascending
        try:
            self.logger.info("Applying SRT_TIMEZONE ordering", node="SRT_TIMEZONE")
            df_SRT_TIMEZONE_pensive_archimedes = df_NON_CA_TX_nice_bohr.orderBy(col("O_TIMEZONE4").asc())
        except Exception:
            self.logger.exception("Failed applying SRT_TIMEZONE ordering")
            raise


        # TCTRANS2: Transaction Control pass-through for CA pipeline (identity)
        try:
            self.logger.info("Applying TCTRANS2 passthrough", node="TCTRANS2")
            df_TCTRANS2_fierce_lovelace = df_EXP_CA_STATE_humble_darwin
        except Exception:
            self.logger.exception("Failed applying TCTRANS2 passthrough")
            raise


        # TCTRANS1: Transaction Control pass-through for TX pipeline (identity)
        try:
            self.logger.info("Applying TCTRANS1 passthrough", node="TCTRANS1")
            df_TCTRANS1_eager_kant = df_EXP_TX_STATE_jovial_newton
        except Exception:
            self.logger.exception("Failed applying TCTRANS1 passthrough")
            raise


        # EXP_Passthrough: compute O_TIMEZONE_IND (lag-based) and FILE_NAME, preserve pass-throughs
        try:
            self.logger.info("Applying EXP_Passthrough expressions", node="EXP_Passthrough")

            # window ordering by O_TIMEZONE4 (same key used by sorter)
            w_tz = Window.orderBy(col("O_TIMEZONE4"))
            prev_tz = lag(col("TIMEZONE4")).over(w_tz)

            df_tmp = (
                df_SRT_TIMEZONE_pensive_archimedes
                .withColumn("V_PREV_TIMEZONE", prev_tz)
                .withColumn("V_CURR_TIMEZONE", col("TIMEZONE4"))
                .withColumn("O_TIMEZONE_IND", when(col("V_PREV_TIMEZONE") == col("V_CURR_TIMEZONE"), lit("N")).otherwise(lit("Y")))
                .withColumn("v_file_year", when(lit(RUN_TYPE_TZ) == lit('MANUAL'), lit(FILE_YEAR)).otherwise(date_format(current_date(), 'yyyy')))
                .withColumn(
                    "FILE_NAME",
                    when(
                        col("TIMEZONE4").isNull(),
                        concat_ws('', lit('NU0C_NISS_AUTO_UNKNOWN_'), col('v_file_year'), lit('_AutoPrem_Detail_.csv'))
                    ).otherwise(
                        concat_ws('', lit('NU0C_NISS_AUTO_'), col('TIMEZONE4'), lit('_'), col('v_file_year'), lit('_AutoPrem_Detail_.csv'))
                    )
                )
            )

            # project explicit set of columns: keep all existing suffixed '4' ports plus our new outputs
            # derive list of existing columns that correspond to the declared passthrough ports and ensure deterministic order
            # We'll include the most important passthrough columns followed by computed ports
            kept_columns = [
                c for c in [
                    "o_CLNDR_YR4","o_CALL_YR4","NAIC_CMPNY_CD4","NISS_CMPNY_CD4","ST_NM4","ST_CD4",
                    "NISS_ST_CD4","ST_ABBR4","ACCTNG_LOB4","CVG_TYP_CD4","CVG_TYP_IND4","o_CVG_AMT4",
                    "o_BI_LMT4","GA_ADDED_AT_FAULT_IND4","FA2_PLCY_IND4","UM_UMI_STACKING4","PIP_WVR_WL_IND4",
                    "PIP_MED_SEC_IND4","PIP_LOSS_INCOME_IND4","MI_PPO_IND4","PRD_GRP_CD4","NJ_HLTH_INSR_PRIM4",
                    "NJ_EXTR_PIP_PKG4","NJ_RESDNC_RLTNSHP_PIP_IND4","NY_SSL_IND4","NY_FULL_CVG_GLASS_COMP_IND4",
                    "GRGNG_ZIP_54","o_NISS_TERR_CD4","RATNG_CMPY_CD4","MLT_CAR_IND4","RT_CLS4","AGE4",
                    "GENDR4","MRTL_STAT4","AUTO_USE_CD4","MILES_TO_WRK4","GOOD_STDNT_IND4","DRVR_TRNG_IND4",
                    "SOI_TYP4","PHY_DMG_IND4","NJ_RATD_PNTS4","VEH_MDL_YR4","NJ_EXCPTION_CD4","NJ_FGVN_PNTS4",
                    "NJ_NO_LWST_LMT_IND4","NJ_NMD_DRVR_EXCL_IND4","PASSV_RESTRA_DISC4","SNR_DRVR_IND4", 
                    "DEFNS_DRVR_DISC_IND4","ANTI_THFT_DISC4","DAY_TM_RUN_LIGHTS4","LMT_TORT4","CVG_EXPS_VAL4",
                    "EXPS_VAL_ROLLED4","TTL_WRITTN_PREM_AMT4","NUM_OF_CARS_IN_HH4","PNI_AGE4","RDRVR_DT_OF_BRTH4",
                    "TERM_STRT_DT4","SRC_SYS_CD4","DERIVED_RDRVR_AGE4","FINAL_RDRVR_AGE4","LINE_CD4","ACCDNT_YR4",
                    "o_NISS_CVG_CD4","RTNG_ZNE_CD4","TERM_ZNE_CD4","o_NISS_CLASS_CD4","NISS_ELIG_PNTS_CD4",
                    "NISS_AGE_GRP_CD4","o_NISS_MNFCTRS_MDL_YR4","NISS_CMMCL_IND_CD4","NISS_EXCPN_CD4","NISS_FGVNS_CD4",
                    "NISS_PASSV_RESTRA_CD4","NISS_DEFNS_DRVR_CRD_CD4","NISS_ANTI_THFT_DVC_CD4","NISS_DAY_TM_RUN_LAMPS_DISC_CD4",
                    "NISS_PLCY_LMT_CD4","NISS_DEDUC_CD4","NISS_SSL_LIAB_CD4","NISS_SUBLOB_CD4","NISS_TYP_LOSS_CD4",
                    "NISS_LIAB_OR_NO_FAULT_CD4","NISS_ANNL_STMNT_LOB_CD4","NISS_PD_LOSS4","NISS_PD_ALLOC_ADJUS_EXPNS4",
                    "NISS_OUTSTNDG_LOSS4","NISS_NO_PD_CLMS4","NISS_NO_OUTSTND_CLMS4","REC_EXCPN_IND4","REC_DROP_IND4",
                    "REC_DROP_RSN_DESC4","EXP_STATE_IND4","TIMEZONE4"
                ] if c in df_tmp.columns
            ]

            # final dataframe with explicit projection and new computed ports
            df_EXP_Passthrough_jovial_schrodinger = df_tmp.select(*[col(c) for c in kept_columns] + [col("V_PREV_TIMEZONE"), col("V_CURR_TIMEZONE"), col("O_TIMEZONE_IND"), col("FILE_NAME")])

        except Exception:
            self.logger.exception("Failed applying EXP_Passthrough expressions")
            raise


        # FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail11: flat-file Output (delimited) - write single CSV and move/rename
        try:
            self.logger.info("Writing FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail11 flat file", node="FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail11")
            # input is df_TCTRANS2_fierce_lovelace
            df_out_11 = df_TCTRANS2_fierce_lovelace

            tmp_dir = os.path.join(PM_TARGET_FILE_DIR, "_tmp_FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail11")
            # ensure tmp_dir is clean
            if os.path.exists(tmp_dir):
                shutil.rmtree(tmp_dir)

            df_out_11.coalesce(1).write.mode("overwrite").option("header", True).option("sep", ",").csv(tmp_dir)

            part_file = glob.glob(os.path.join(tmp_dir, "part-*.csv"))
            if not part_file:
                raise RuntimeError("No part file written by Spark for FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail11")
            src_part = part_file[0]

            # target filename taken from mapping output_details (as exported); use sanitized filename
            target_filename = "fdr_lib_ff_birp_nu0c_niss_atprm_rptext_detail111.out"
            target_path = os.path.join(PM_TARGET_FILE_DIR, target_filename)

            shutil.move(src_part, target_path)
            shutil.rmtree(tmp_dir)

        except Exception:
            self.logger.exception("Failed writing flat file FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail11")
            raise


        # FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail1: flat-file Output (delimited) - write single CSV and move/rename
        try:
            self.logger.info("Writing FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail1 flat file", node="FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail1")
            df_out_1 = df_TCTRANS1_eager_kant

            tmp_dir = os.path.join(PM_TARGET_FILE_DIR, "_tmp_FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail1")
            if os.path.exists(tmp_dir):
                shutil.rmtree(tmp_dir)

            df_out_1.coalesce(1).write.mode("overwrite").option("header", True).option("sep", ",").csv(tmp_dir)

            part_file = glob.glob(os.path.join(tmp_dir, "part-*.csv"))
            if not part_file:
                raise RuntimeError("No part file written by Spark for FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail1")
            src_part = part_file[0]

            target_filename = "fdr_lib_ff_birp_nu0c_niss_atprm_rptext_detail11.out"
            target_path = os.path.join(PM_TARGET_FILE_DIR, target_filename)

            shutil.move(src_part, target_path)
            shutil.rmtree(tmp_dir)

        except Exception:
            self.logger.exception("Failed writing flat file FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail1")
            raise


        # Assign final output dataframe variable names expected by downstream batches
        # (these are already assigned above as part of their steps but ensure they exist in globals)

        # df_NON_CA_TX_nice_bohr assigned in NON_CA_TX block
        # df_EXP_CA_STATE_humble_darwin assigned in EXP_CA_STATE block
        # df_EXP_TX_STATE_jovial_newton assigned in EXP_TX_STATE block
        # df_SRT_TIMEZONE_pensive_archimedes assigned in SRT_TIMEZONE block
        # df_TCTRANS2_fierce_lovelace assigned in TCTRANS2 block
        # df_TCTRANS1_eager_kant assigned in TCTRANS1 block
        # df_EXP_Passthrough_jovial_schrodinger assigned in EXP_Passthrough block
        # df_FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail11_elated_kepler - map to the output file writer's input
        try:
            df_FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail11_elated_kepler = df_TCTRANS2_fierce_lovelace
        except Exception:
            # If assignment fails, log and re-raise to keep parity with mapping expectation
            self.logger.exception("Failed assigning df_FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail11_elated_kepler")
            raise

        try:
            df_FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail1_optimistic_shannon = df_TCTRANS1_eager_kant
        except Exception:
            self.logger.exception("Failed assigning df_FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail1_optimistic_shannon")
            raise

        try:
            self.logger.info("Applying TCTRANS passthrough", node="TCTRANS")
            # Transaction Control is implemented as a passthrough in Spark; the write's atomicity provides the commit semantics.
            df_TCTRANS_quirky_gauss = df_EXP_Passthrough_jovial_schrodinger
        except Exception:
            self.logger.exception("Failed applying TCTRANS passthrough")
            raise


        # FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail: flat-file Output (delimited) - write single CSV and move/rename
        try:
            self.logger.info("Writing FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail flat file", node="FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail")
            # input is df_TCTRANS_quirky_gauss
            df_out_zz = df_TCTRANS_quirky_gauss

            tmp_dir = os.path.join(PM_TARGET_FILE_DIR, "_tmp_FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail")
            # ensure tmp_dir is clean
            if os.path.exists(tmp_dir):
                shutil.rmtree(tmp_dir)

            # stage single CSV part via Spark
            df_out_zz.coalesce(1).write.mode("overwrite").option("header", True).option("sep", ",").csv(tmp_dir)

            part_file = glob.glob(os.path.join(tmp_dir, "part-*.csv"))
            if not part_file:
                raise RuntimeError("No part file written by Spark for FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail")
            src_part = part_file[0]

            # target filename from mapping output_details
            target_filename = "fdr_lib_ff_birp_nu0c_niss_atprm_rptext_detail1.out"
            target_path = os.path.join(PM_TARGET_FILE_DIR, target_filename)

            shutil.move(src_part, target_path)
            shutil.rmtree(tmp_dir)

        except Exception:
            self.logger.exception("Failed writing flat file FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail")
            raise


        # final assignment expected by downstream batches
        try:
            df_FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail_silly_kant = df_TCTRANS_quirky_gauss
        except Exception:
            self.logger.exception("Failed assigning df_FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptExt_Detail_silly_kant")
            raise

        self.glue_job.commit()

    def finalize(self) -> None:
        self.logger.info("Finalizing m_NU0C_NISS_AUTO_119_ATPRM_DTL_SPLIT")

    @staticmethod
    def _require_env(env: Mapping[str, Any], key: str) -> str:

        value = env.get(key)

        if not value or str(value).startswith("REPLACE_WITH_"):
            raise ConfigurationValidationError(
                f"config.yaml 'env.{key}' must be set"
            )

        return str(value)
