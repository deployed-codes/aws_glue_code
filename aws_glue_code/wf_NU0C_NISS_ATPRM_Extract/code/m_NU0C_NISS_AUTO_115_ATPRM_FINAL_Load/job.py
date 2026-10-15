import sys
from typing import Any, Mapping

from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import DataFrame

from glue_lib.connector import ParquetReader, ParquetWriter
from glue_lib.exceptions import ConfigurationValidationError
from glue_lib.job.base import BaseJob
from pyspark.sql.functions import col, lit, current_timestamp


# Informatica mapping m_NU0C_NISS_AUTO_115_ATPRM_FINAL_Load.
# Every mapping node runs in order inside run().
class Nu0cNissAuto115AtprmFinalLoadJob(BaseJob):

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

        # FDR_LIB_WRK_BIRP_NISS_APRM_DETL: Source (S3-first, fallback to Glue Catalog)
        try:
            self.logger.info("Attempting S3 parquet read for WRK_BIRP_NISS_APRM_DETL", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            df_WRK_BIRP_NISS_APRM_DETL_nice_darwin = self.parquet_reader.read(
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                options=None,
            )
            try:
                df_WRK_BIRP_NISS_APRM_DETL_nice_darwin = df_WRK_BIRP_NISS_APRM_DETL_nice_darwin.select(
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
                    "SOURCE_IND_DERIVED"
                )
            except Exception:
                # If projection fails after a successful parquet read, it's a fatal error
                self.logger.exception("Failed projecting columns from S3 parquet read for WRK_BIRP_NISS_APRM_DETL")
                raise
        except Exception:
            # Fallback to Glue Catalog read
            self.logger.warning("S3 parquet read for WRK_BIRP_NISS_APRM_DETL failed, falling back to Glue Catalog read", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            try:
                dyf = self.glue_context.create_dynamic_frame_from_catalog(
                    database=self.glue_database,
                    table_name="WRK_BIRP_NISS_APRM_DETL",
                    transformation_ctx="df_WRK_BIRP_NISS_APRM_DETL_nice_darwin",
                )
                df_WRK_BIRP_NISS_APRM_DETL_nice_darwin = dyf.toDF().select(
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
                    "SOURCE_IND_DERIVED"
                )
            except Exception:
                self.logger.exception("Failed reading WRK_BIRP_NISS_APRM_DETL from Glue Catalog")
                raise

        # guard empty source
        try:
            if df_WRK_BIRP_NISS_APRM_DETL_nice_darwin.isEmpty():
                self.logger.warning("Source WRK_BIRP_NISS_APRM_DETL returned no rows", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
        except Exception:
            self.logger.exception("Failed checking emptiness of WRK_BIRP_NISS_APRM_DETL source")
            raise

        # SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL: Application Source Qualifier with SQL override rewritten to use the staged temp view
        try:
            # mapping parameter used inside the SQL override
            FILTER_COND_FNL = self._require_env(self.env, "FILTER_COND_FNL")
            df_WRK_BIRP_NISS_APRM_DETL_nice_darwin.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")
            sql_query = f"""SELECT
CLNDR_YR,
CALL_YR,
NISS_CMPNY_CD,
ST_NM,
 ST_CD,
ST_ABBR,
NISS_ST_CD,
LINE_CD,

ACCDNT_YR,
CASE 
WHEN NISS_CVG_CD = 'E16' THEN '616'
WHEN NISS_CVG_CD = 'E36' THEN '636'
WHEN NISS_CVG_CD = 'E56' THEN '656'
WHEN NISS_CVG_CD = 'E76' THEN '676'
WHEN NISS_CVG_CD = 'E85' THEN '085'
WHEN NISS_CVG_CD = 'E14' THEN '614'
WHEN NISS_CVG_CD = 'E81' THEN '081'
WHEN NISS_CVG_CD = 'E01' THEN '001'
WHEN NISS_CVG_CD = 'E34' THEN '034'
WHEN NISS_CVG_CD = 'E33' THEN '033'
WHEN NISS_CVG_CD = 'E71' THEN '071'
WHEN NISS_CVG_CD = 'E77' THEN '077'
WHEN NISS_CVG_CD = 'E55' THEN '055'
WHEN NISS_CVG_CD = 'E33' THEN '733'
WHEN NISS_CVG_CD = 'E99' THEN '099'
ELSE NISS_CVG_CD END NISS_CVG_CD,
NISS_TERR_CD,
RTNG_ZNE_CD,
TERM_ZNE_CD,
GRGNG_ZIP_5,
CASE 
WHEN SUBSTR(NISS_CLASS_CD,1,5)= 'E1202' THEN '120200'
WHEN SUBSTR(NISS_CLASS_CD,1,5)= 'E1212' THEN '121200'
WHEN SUBSTR(NISS_CLASS_CD,1,6)= 'E88712' THEN '887120'
WHEN LEFT(NISS_CLASS_CD,4)= 'E202' THEN '1202' ||RIGHT(NISS_CLASS_CD,2) 
WHEN LEFT(NISS_CLASS_CD,4)= 'E600' THEN '1600' ||RIGHT(NISS_CLASS_CD,2) 
WHEN LEFT(NISS_CLASS_CD,4)= 'E602' THEN '1602' ||RIGHT(NISS_CLASS_CD,2) 
WHEN LEFT(NISS_CLASS_CD,4)= 'E620' THEN '1620' ||RIGHT(NISS_CLASS_CD,2) 
WHEN LEFT(NISS_CLASS_CD,4)= 'E622' THEN '1622' ||RIGHT(NISS_CLASS_CD,2) 
WHEN LEFT(NISS_CLASS_CD,4)= 'E630' THEN '1630' ||RIGHT(NISS_CLASS_CD,2)  
WHEN LEFT(NISS_CLASS_CD,4)= 'E632' THEN '1632' ||RIGHT(NISS_CLASS_CD,2) 
WHEN LEFT(NISS_CLASS_CD,4)= 'E610' THEN '1610' ||RIGHT(NISS_CLASS_CD,2) 
WHEN LEFT(NISS_CLASS_CD,4)= 'E612' THEN '1612' ||RIGHT(NISS_CLASS_CD,2) 
WHEN LEFT(NISS_CLASS_CD,4)= 'E696' THEN '1696' ||RIGHT(NISS_CLASS_CD,2) 
WHEN LEFT(NISS_CLASS_CD,4)= 'E698' THEN '1698' ||RIGHT(NISS_CLASS_CD,2) 
WHEN LEFT(NISS_CLASS_CD,4)= 'E124' THEN '8124' ||RIGHT(NISS_CLASS_CD,2)
WHEN LEFT(NISS_CLASS_CD,4)= 'E554' THEN '8554' ||RIGHT(NISS_CLASS_CD,2)
WHEN LEFT(NISS_CLASS_CD,4)= 'E214' THEN '8214' ||RIGHT(NISS_CLASS_CD,2)
WHEN LEFT(NISS_CLASS_CD,4)= 'E211' THEN '8211' ||RIGHT(NISS_CLASS_CD,2)
WHEN LEFT(NISS_CLASS_CD,4)= 'E611' THEN '8611' ||RIGHT(NISS_CLASS_CD,2)
WHEN LEFT(NISS_CLASS_CD,4)= 'E160' THEN '1600' ||RIGHT(NISS_CLASS_CD,2)     
ELSE NISS_CLASS_CD END NISS_CLASS_CD,
NISS_ELIG_PNTS_CD,
NISS_AGE_GRP_CD,
NISS_CMMCL_IND_CD,
NISS_EXCPN_CD,
NISS_FGVNS_CD,
NISS_PASSV_RESTRA_CD,
NISS_DEFNS_DRVR_CRD_CD,
NISS_ANTI_THFT_DVC_CD,
NISS_DAY_TM_RUN_LAMPS_DISC_CD,
CASE 
WHEN NISS_PLCY_LMT_CD = 'E2' THEN '02'
ELSE NISS_PLCY_LMT_CD END NISS_PLCY_LMT_CD,
CASE
WHEN NISS_DEDUC_CD = 'E0' THEN '01'
WHEN NISS_DEDUC_CD = 'E1' THEN '01'
WHEN NISS_DEDUC_CD = 'E7' THEN '07'
ELSE NISS_DEDUC_CD END NISS_DEDUC_CD,
NISS_SSL_LIAB_CD,
NISS_SUBLOB_CD,
NISS_TYP_LOSS_CD,
NISS_LIAB_OR_NO_FAULT_CD,
NISS_ANNL_STMNT_LOB_CD,
SUM(EXPS_VAL_ROLLED) CVG_EXPS_VAL,
SUM(TTL_WRITTN_PREM_AMT) TTL_WRITTN_PREM_AMT,
NISS_PD_LOSS,
NISS_PD_ALLOC_ADJUS_EXPNS,
NISS_OUTSTNDG_LOSS,
NISS_NO_PD_CLMS,
NISS_NO_OUTSTND_CLMS,
RSVD_NISS_USE,
NISS_RSVD_CMPNY_USE,
NISS_MNFCTRS_MDL_YR,
99999 as CVG_ATTR_SK

FROM WRK_BIRP_NISS_APRM_DETL

WHERE {FILTER_COND_FNL}

GROUP BY
CLNDR_YR,
CALL_YR,
NISS_CMPNY_CD,
ST_NM,

ST_ABBR,
NISS_ST_CD,
LINE_CD,
ACCDNT_YR,
CASE 
WHEN NISS_CVG_CD = 'E16' THEN '616'
WHEN NISS_CVG_CD = 'E36' THEN '636'
WHEN NISS_CVG_CD = 'E56' THEN '656'
WHEN NISS_CVG_CD = 'E76' THEN '676'
WHEN NISS_CVG_CD = 'E85' THEN '085'
WHEN NISS_CVG_CD = 'E14' THEN '614'
WHEN NISS_CVG_CD = 'E81' THEN '081'
WHEN NISS_CVG_CD = 'E01' THEN '001'
WHEN NISS_CVG_CD = 'E34' THEN '034'
WHEN NISS_CVG_CD = 'E33' THEN '033'
WHEN NISS_CVG_CD = 'E71' THEN '071'
WHEN NISS_CVG_CD = 'E77' THEN '077'
WHEN NISS_CVG_CD = 'E55' THEN '055'
WHEN NISS_CVG_CD = 'E33' THEN '733'
WHEN NISS_CVG_CD = 'E99' THEN '099'
ELSE NISS_CVG_CD END,
NISS_TERR_CD,
RTNG_ZNE_CD,
TERM_ZNE_CD,
GRGNG_ZIP_5,
CASE 
WHEN SUBSTR(NISS_CLASS_CD,1,5)= 'E1202' THEN '120200'
WHEN SUBSTR(NISS_CLASS_CD,1,5)= 'E1212' THEN '121200'
WHEN SUBSTR(NISS_CLASS_CD,1,6)= 'E88712' THEN '887120'
WHEN LEFT(NISS_CLASS_CD,4)= 'E202' THEN '1202' ||RIGHT(NISS_CLASS_CD,2) 
WHEN LEFT(NISS_CLASS_CD,4)= 'E600' THEN '1600' ||RIGHT(NISS_CLASS_CD,2) 
WHEN LEFT(NISS_CLASS_CD,4)= 'E602' THEN '1602' ||RIGHT(NISS_CLASS_CD,2) 
WHEN LEFT(NISS_CLASS_CD,4)= 'E620' THEN '1620' ||RIGHT(NISS_CLASS_CD,2) 
WHEN LEFT(NISS_CLASS_CD,4)= 'E622' THEN '1622' ||RIGHT(NISS_CLASS_CD,2) 
WHEN LEFT(NISS_CLASS_CD,4)= 'E630' THEN '1630' ||RIGHT(NISS_CLASS_CD,2)  
WHEN LEFT(NISS_CLASS_CD,4)= 'E632' THEN '1632' ||RIGHT(NISS_CLASS_CD,2) 
WHEN LEFT(NISS_CLASS_CD,4)= 'E610' THEN '1610' ||RIGHT(NISS_CLASS_CD,2) 
WHEN LEFT(NISS_CLASS_CD,4)= 'E612' THEN '1612' ||RIGHT(NISS_CLASS_CD,2) 
WHEN LEFT(NISS_CLASS_CD,4)= 'E696' THEN '1696' ||RIGHT(NISS_CLASS_CD,2) 
WHEN LEFT(NISS_CLASS_CD,4)= 'E698' THEN '1698' ||RIGHT(NISS_CLASS_CD,2) 
WHEN LEFT(NISS_CLASS_CD,4)= 'E124' THEN '8124' ||RIGHT(NISS_CLASS_CD,2)
WHEN LEFT(NISS_CLASS_CD,4)= 'E554' THEN '8554' ||RIGHT(NISS_CLASS_CD,2)
WHEN LEFT(NISS_CLASS_CD,4)= 'E214' THEN '8214' ||RIGHT(NISS_CLASS_CD,2)
WHEN LEFT(NISS_CLASS_CD,4)= 'E211' THEN '8211' ||RIGHT(NISS_CLASS_CD,2)
WHEN LEFT(NISS_CLASS_CD,4)= 'E611' THEN '8611' ||RIGHT(NISS_CLASS_CD,2)
WHEN LEFT(NISS_CLASS_CD,4)= 'E160' THEN '1600' ||RIGHT(NISS_CLASS_CD,2)
ELSE NISS_CLASS_CD END,
NISS_ELIG_PNTS_CD,
NISS_AGE_GRP_CD,
NISS_CMMCL_IND_CD,
NISS_EXCPN_CD,
NISS_FGVNS_CD,
NISS_PASSV_RESTRA_CD,
NISS_DEFNS_DRVR_CRD_CD,
NISS_ANTI_THFT_DVC_CD,
NISS_DAY_TM_RUN_LAMPS_DISC_CD,
CASE 
WHEN NISS_PLCY_LMT_CD = 'E2' THEN '02'
ELSE NISS_PLCY_LMT_CD END,
CASE
WHEN NISS_DEDUC_CD = 'E0' THEN '01'
WHEN NISS_DEDUC_CD = 'E1' THEN '01'
WHEN NISS_DEDUC_CD = 'E7' THEN '07'
ELSE NISS_DEDUC_CD END,
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
RSVD_NISS_USE,
NISS_RSVD_CMPNY_USE,
NISS_MNFCTRS_MDL_YR
"""
            self.logger.info("Running rewritten SQ SQL against staged temp view", node="SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            try:
                df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_pensive_plato = self.spark.sql(sql_query)
                if df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_pensive_plato.isEmpty():
                    self.logger.warning("SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL returned no rows", node="SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            except Exception:
                self.logger.exception("Failed executing rewritten SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL SQL")
                raise
        except Exception:
            self.logger.exception("Failed preparing or executing SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            raise

        # EXP_PassThru: explicit pass-through projection and PM system variables -> NULL
        try:
            self.logger.info("Applying EXP_PassThru expressions", node="EXP_PassThru")
            df_EXP_PassThru_upbeat_franklin = (
                df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_pensive_plato.select(
                    "CLNDR_YR",
                    "CALL_YR",
                    "NISS_CMPNY_CD",
                    "ST_NM",
                    "ST_CD",
                    "ST_ABBR",
                    "NISS_ST_CD",
                    "LINE_CD",
                    "ACCDNT_YR",
                    "NISS_CVG_CD",
                    "NISS_TERR_CD",
                    "RTNG_ZNE_CD",
                    "TERM_ZNE_CD",
                    "GRGNG_ZIP_5",
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
                    "CVG_EXPS_VAL",
                    "TTL_WRITTN_PREM_AMT",
                    "NISS_PD_LOSS",
                    "NISS_PD_ALLOC_ADJUS_EXPNS",
                    "NISS_OUTSTNDG_LOSS",
                    "NISS_NO_PD_CLMS",
                    "NISS_NO_OUTSTND_CLMS",
                    "RSVD_NISS_USE",
                    "NISS_RSVD_CMPNY_USE",
                    "NISS_MNFCTRS_MDL_YR",
                    "CVG_ATTR_SK"
                )
                .withColumn("MAAPING_NAME", lit(None).cast("string"))
                .withColumn("FOLDER_NAME", lit(None).cast("string"))
                .withColumn("WORKFLOW_NAME", lit(None).cast("string"))
            )
        except Exception:
            self.logger.exception("Failed applying EXP_PassThru expressions")
            raise

        # mplt_FDR_LIB_ABC_MAPPING_AUDIT: mapplet implementation (reusable candidate)
        try:
            self.logger.info("Running mapplet mplt_FDR_LIB_ABC_MAPPING_AUDIT", node="mplt_FDR_LIB_ABC_MAPPING_AUDIT")
            df_mplt_ABC_MAPPING_AUDIT_laughing_galileo = build_mapping_audit_columns(df_EXP_PassThru_upbeat_franklin.select(lit(1).alias("__dummy")).limit(1))
        except Exception:
            self.logger.exception("Failed executing mapplet mplt_FDR_LIB_ABC_MAPPING_AUDIT")
            raise

        # EXP_PassThru_Tgt: add surrogate key NISS_APRM_FINAL_SK = CVG_ATTR_SK and include audit columns
        try:
            self.logger.info("Applying EXP_PassThru_Tgt expressions", node="EXP_PassThru_Tgt")
            df_EXP_PassThru_Tgt_calm_plato = (
                df_EXP_PassThru_upbeat_franklin
                .withColumn("NISS_APRM_FINAL_SK", col("CVG_ATTR_SK"))
                .withColumn("CR_BY_MAPNG_ID", lit(None).cast("long"))
                .withColumn("DW_CR_TMSP", current_timestamp())
                .withColumn("UPD_BY_MAPNG_ID", lit(None).cast("long"))
                .withColumn("DW_UPD_TMSP", current_timestamp())
                .withColumn("WRK_FLOW_RUN_ID", lit(None).cast("long"))
                .select(
                    "NISS_APRM_FINAL_SK",
                    "CLNDR_YR",
                    "CALL_YR",
                    "NISS_CMPNY_CD",
                    "ST_NM",
                    "ST_CD",
                    "ST_ABBR",
                    "NISS_ST_CD",
                    "LINE_CD",
                    "ACCDNT_YR",
                    "NISS_CVG_CD",
                    "NISS_TERR_CD",
                    "RTNG_ZNE_CD",
                    "TERM_ZNE_CD",
                    "GRGNG_ZIP_5",
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
                    "CVG_EXPS_VAL",
                    "TTL_WRITTN_PREM_AMT",
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
                    "WRK_FLOW_RUN_ID"
                )
            )
        except Exception:
            self.logger.exception("Failed applying EXP_PassThru_Tgt expressions")
            raise

        # assign final WRK dataframe variable for downstream lineage and rule compliance
        df_WRK_BIRP_NISS_APRM_FINAL_determined_hawking = df_EXP_PassThru_Tgt_calm_plato

        # FDR_LIB_WRK_BIRP_NISS_APRM_FINAL: write final WRK_ table as parquet to S3 (overwrite)
        try:
            self.logger.info("Writing WRK_BIRP_NISS_APRM_FINAL to S3 as parquet (overwrite)", table="WRK_BIRP_NISS_APRM_FINAL")
            self.parquet_writer.write(
                dataframe=df_WRK_BIRP_NISS_APRM_FINAL_determined_hawking,
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_FINAL/",
                mode="overwrite",
                partition_by=None,
                options=None,
            )
        except Exception:
            self.logger.exception("Failed writing WRK_BIRP_NISS_APRM_FINAL to S3")
            raise

        self.glue_job.commit()

    def finalize(self) -> None:
        self.logger.info("Finalizing m_NU0C_NISS_AUTO_115_ATPRM_FINAL_Load")

    @staticmethod
    def _require_env(env: Mapping[str, Any], key: str) -> str:

        value = env.get(key)

        if not value or str(value).startswith("REPLACE_WITH_"):
            raise ConfigurationValidationError(
                f"config.yaml 'env.{key}' must be set"
            )

        return str(value)
