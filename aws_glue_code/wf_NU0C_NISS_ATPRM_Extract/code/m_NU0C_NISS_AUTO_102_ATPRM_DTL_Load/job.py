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
    coalesce,
    length,
    substring,
    current_timestamp,
    row_number,
    broadcast,
)
from pyspark.sql.window import Window
from glue_lib.connector.snowflake import SnowflakeReader


# Informatica mapping m_NU0C_NISS_AUTO_102_ATPRM_DTL_Load.
# Every mapping node runs in order inside run().
class Nu0cNissAuto102AtprmDtlLoadJob(BaseJob):

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

        # mapping parameters / environment values referenced inside SQL overrides or expressions
        RUN_TYPE = self._require_env(self.env, "RUN_TYPE")
        GEO_ST_NM_BCKP = self._require_env(self.env, "GEO_ST_NM_BCKP")
        RPT_YEAR = self._require_env(self.env, "RPT_YEAR")
        BACKENDFIX_DT = self._require_env(self.env, "BACKENDFIX_DT")
        CLNDR_YR = self._require_env(self.env, "CLNDR_YR")

        SNOWFLAKE_CONNECTION_FDWS_FDRWRK = self._require_env(self.env, "SNOWFLAKE_CONNECTION_FDWS_FDRWRK")
        SNOWFLAKE_CONNECTION_DBCONNECTION_FDWS_REF_ODBC = self._require_env(self.env, "SNOWFLAKE_CONNECTION_DBCONNECTION_FDWS_REF_ODBC")

        # Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_DTL: try S3 parquet first, fall back to Glue Catalog
        try:
            self.logger.info("Attempting to read Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_DTL from S3 first", node="Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_DTL")
            df_WRK_BIRP_TA_NISS_NU0C_APRM_DTL_friendly_franklin = self.parquet_reader.read(
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_TA_NISS_NU0C_APRM_DTL/",
                options=None,
            )
            self.logger.info("Read Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_DTL from S3", node="Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_DTL")
        except Exception:
            # allowed fallback branch - try catalog read
            try:
                self.logger.info("S3 read failed; falling back to Glue Data Catalog for Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_DTL", node="Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_DTL")
                dyf = self.glue_context.create_dynamic_frame_from_catalog(
                    database=self.glue_database,
                    table_name="WRK_BIRP_TA_NISS_NU0C_APRM_DTL",
                    transformation_ctx="df_WRK_BIRP_TA_NISS_NU0C_APRM_DTL_friendly_franklin",
                )
                df_WRK_BIRP_TA_NISS_NU0C_APRM_DTL_friendly_franklin = dyf.toDF().select(
                    "FISC_PER_YR",
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
                    "PLCY_IND",
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
                    "GRGNG_ZIP",
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
                    "WRITTN_PREM_AMT",
                    "NJ_NO_LWST_LMT_IND",
                    "NJ_NMD_DRVR_EXCL_IND",
                    "COMP_DED",
                    "COLL_DED",
                    "PLCY_CNTRCT_NUM",
                    "UNIT_NUM",
                    "EFF_DT",
                    "NUM_OF_CARS_IN_HH",
                    "RDRVR_DT_OF_BRTH",
                    "TERM_STRT_DT",
                    "SRC_SYS_CD",
                    "PNI_AGE",
                    "MIS_LOB",
                    "PRINCIPAL_OPRT",
                    "SOURCE_IND_DERIVED",
                    "ANTI_THFT_CTGY_CD",
                )
            except Exception:
                self.logger.exception("Failed reading Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_DTL from Glue Catalog after S3 fallback", node="Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_DTL")
                raise

        # warn on empty source
        try:
            if df_WRK_BIRP_TA_NISS_NU0C_APRM_DTL_friendly_franklin.isEmpty():
                self.logger.warning("Source Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_DTL returned no rows", node="Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_DTL")
        except Exception:
            # isEmpty can occasionally fail if df is malformed - log and re-raise
            self.logger.exception("Failed checking emptiness for Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_DTL")
            raise

        # SQ_Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_DTL: read SQL override from Snowflake
        sql_query = f"""SELECT SQ.* , THEFT.ANTI_THFT_CTGY_CD FROM (SELECT CASE WHEN {RUN_TYPE}='Register' then REG_PER_YR else  FISC_PER_YR end CLNDR_YR,
NAIC_CMPNY_CD,
NISS_CMPNY_CD,
LTRIM(RTRIM(ST_NM)) ST_NM,
LTRIM(RTRIM(ST_CD)) ST_CD,
NISS_ST_CD,
LTRIM(RTRIM(ST_ABBR)) ST_ABBR,
LTRIM(RTRIM(ACCTNG_LOB)) ACCTNG_LOB,
LTRIM(RTRIM(CVG_TYP_CD)) CVG_TYP_CD,
LTRIM(RTRIM(CVG_AMT)) CVG_AMT,
LTRIM(RTRIM(BI_LMT)) BI_LMT,
GA_ADDED_AT_FAULT_IND,
FA2_PLCY_IND,
UM_UMI_STACKING,
PIP_WVR_WL_IND,
PIP_MED_SEC_IND,
PIP_LOSS_INCOME_IND,
MI_PPO_IND,
LTRIM(RTRIM(PRD_GRP_CD)) PRD_GRP_CD,
LTRIM(RTRIM(NJ_HLTH_INSR_PRIM)) NJ_HLTH_INSR_PRIM,
LTRIM(RTRIM(NJ_EXTR_PIP_PKG)) NJ_EXTR_PIP_PKG,
NJ_RESDNC_RLTNSHP_PIP_IND,
NY_SSL_IND,
NY_FULL_CVG_GLASS_COMP_IND,
LTRIM(RTRIM(GRGNG_ZIP_5)) GRGNG_ZIP_5,
NISS_TERR_CD,
LTRIM(RTRIM(RATNG_CMPY_CD)) RATNG_CMPY_CD,
LTRIM(RTRIM(MLT_CAR_IND)) MLT_CAR_IND,
LTRIM(RTRIM(RT_CLS)) RT_CLS,
LTRIM(RTRIM(AGE)) AGE,
LTRIM(RTRIM(GENDR)) GENDR,
LTRIM(RTRIM(MRTL_STAT)) MRTL_STAT,
LTRIM(RTRIM(AUTO_USE_CD)) AUTO_USE_CD,
LTRIM(RTRIM(MILES_TO_WRK)) MILES_TO_WRK,
LTRIM(RTRIM(GOOD_STDNT_IND)) GOOD_STDNT_IND,
LTRIM(RTRIM(DRVR_TRNG_IND)) DRVR_TRNG_IND,
CASE WHEN NISS_ST_CD='29' AND LTRIM(RTRIM(ACCTNG_LOB)) IN ('2110T','2110F') THEN '01' 
ELSE LTRIM(RTRIM(SOI_TYP)) END SOI_TYP ,
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
ANNL_STMNT_LOB_CD,
CVG_TYP_IND,
CVG_EXPS_VAL,
ROUND(TTL_WRITTN_PREM_AMT) AS TTL_WRITTN_PREM_AMT,
NJ_NO_LWST_LMT_IND,
NJ_NMD_DRVR_EXCL_IND,
COALESCE(LTRIM(RTRIM(COMP_DED)),'') AS COMP_DED,
COALESCE(LTRIM(RTRIM(COLL_DED)),'') AS COLL_DED,
LTRIM(RTRIM(PLCY_CNTRCT_NUM)) AS PLCY_CNTRCT_NUM,
UNIT_NUM,
EFF_DT,
NUM_OF_CARS_IN_HH,
COALESCE(RDRVR_DT_OF_BRTH,'0') AS RDRVR_DT_OF_BRTH,
TERM_STRT_DT,
COALESCE(LTRIM(RTRIM(SRC_SYS_CD)),'') AS SRC_SYS_CD,
PNI_AGE,
'' as LOB,
'' as PRINCIPAL_OPRT,
'FARMERS' AS SOURCE_IND_DERIVED
FROM FDR.WRK_BIRP_NISS_APRM_LND


UNION ALL


SELECT SUBSTR(REG_PER_YR,1,4) AS CLNDR_YR,
NAIC_CMPNY_CD,
NISS_CMPNY_CD,
LTRIM(RTRIM(ST_NM)) ST_NM,
LTRIM(RTRIM(ST_CD)) ST_CD,
NISS_ST_CD,
LTRIM(RTRIM(ST_ABBR)) ST_ABBR,
LTRIM(RTRIM(ACCTNG_LOB)) ACCTNG_LOB,
LTRIM(RTRIM(CVG_TYP_CD)) CVG_TYP_CD,
LTRIM(RTRIM(COALESCE(CVG_AMT,''))) CVG_AMT,
LTRIM(RTRIM(BI_LMT)) BI_LMT,
GA_ADDED_AT_FAULT_IND,
PLCY_IND,
UM_UMI_STACKING,
PIP_WVR_WL_IND,
PIP_MED_SEC_IND,
PIP_LOSS_INCOME_IND,
MI_PPO_IND,
LTRIM(RTRIM(PRD_GRP_CD)) PRD_GRP_CD,
LTRIM(RTRIM(NJ_HLTH_INSR_PRIM)) NJ_HLTH_INSR_PRIM,
LTRIM(RTRIM(NJ_EXTR_PIP_PKG)) NJ_EXTR_PIP_PKG,
NJ_RESDNC_RLTNSHP_PIP_IND,
NY_SSL_IND,
NY_FULL_CVG_GLASS_COMP_IND,
LTRIM(RTRIM(GRGNG_ZIP)) GRGNG_ZIP,
NISS_TERR_CD,
LTRIM(RTRIM(RATNG_CMPY_CD)) RATNG_CMPY_CD,
LTRIM(RTRIM(MLT_CAR_IND)) MLT_CAR_IND,
LTRIM(RTRIM(RT_CLS)) RT_CLS,
LTRIM(RTRIM(AGE)) AGE,
LTRIM(RTRIM(GENDR)) GENDR,
LTRIM(RTRIM(MRTL_STAT)) MRTL_STAT,
LTRIM(RTRIM(AUTO_USE_CD)) AUTO_USE_CD,
LTRIM(RTRIM(MILES_TO_WRK)) MILES_TO_WRK,
LTRIM(RTRIM(GOOD_STDNT_IND)) GOOD_STDNT_IND,
LTRIM(RTRIM(DRVR_TRNG_IND)) DRVR_TRNG_IND,
LTRIM(RTRIM(SOI_TYP)) SOI_TYP,
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
ANNL_STMNT_LOB_CD,
CVG_TYP_IND,
CVG_EXPS_VAL,
ROUND(WRITTN_PREM_AMT) WRITTN_PREM_AMT,
NJ_NO_LWST_LMT_IND,
NJ_NMD_DRVR_EXCL_IND,
COALESCE(LTRIM(RTRIM(COMP_DED)),'') AS COMP_DED,
COALESCE(LTRIM(RTRIM(COLL_DED)),'') AS COLL_DED,
LTRIM(RTRIM(PLCY_CNTRCT_NUM)) AS PLCY_CNTRCT_NUM,
UNIT_NUM,
EFF_DT,
NUM_OF_CARS_IN_HH,
COALESCE(RDRVR_DT_OF_BRTH,'0') AS RDRVR_DT_OF_BRTH,
TERM_STRT_DT,
COALESCE(LTRIM(RTRIM(SRC_SYS_CD)),'') AS SRC_SYS_CD,
PNI_AGE,
MIS_LOB,
PRINCIPAL_OPRT,
SOURCE_IND_DERIVED
FROM BIRP.WRK_BIRP_TA_NISS_NU0C_APRM_LND
) SQ
LEFT OUTER JOIN (SELECT DISTINCT A.ANTI_THFT_CTGY_CD AS ANTI_THFT_CTGY_CD,
A.PLCY_CNTRCT_NUM AS PLCY_CNTRCT_NUM,
A.UNIT_NUM AS UNIT_NUM,
A.TERM_STRT_DT AS TERM_STRT_DT
 FROM (
SELECT 
PLCY.PLCY_CNTRCT_NUM,
PLCY.TERM_STRT_DT, 
SOI.UNIT_NUM, ASOI.ANTI_THFT_CTGY_CD , 
ROW_NUMBER() OVER (PARTITION BY PLCY.PLCY_CNTRCT_NUM,PLCY.TERM_STRT_DT, SOI.UNIT_NUM ORDER BY ASOI.ANTI_THFT_CTGY_CD DESC ) AS CNT1
FROM AGDM.FACT_AG_WRITTN_PREM_CVG_LVL FACT
JOIN AGDM.DIM_AG_PLCY PLCY ON FACT.PLCY_SK=PLCY.PLCY_SK
JOIN AGDM.DIM_AG_TRANS_TYP_PLCY ON ( FACT.TRANS_TYP_PLCY_SK=AGDM.DIM_AG_TRANS_TYP_PLCY.TRANS_TYP_PLCY_SK )
JOIN AGDM.DIM_AG_SOI SOI ON FACT.SOI_SK = SOI.SOI_SK
JOIN AGDM.DIM_AG_FARMR_GEO_DISTR GEOD ON FACT.FARMR_GEO_DISTR_SK = GEOD.FARMR_GEO_DISTR_SK
JOIN AGDM.DIM_AG_FARMR_GEO_ST GEOS ON GEOD.FARMR_GEO_ST_SK = GEOS.FARMR_GEO_ST_SK
JOIN AGDM.DIM_AG_RATED_GEO RGEO ON FACT.RATED_GEO_SK = RGEO.RATED_GEO_SK
JOIN AGDM.DIM_DT  REGPER ON FACT.REGSTR_PER_SK=REGPER.DT_SK
JOIN AGDM.DIM_DT  FISCPER ON FACT.FISC_PER_SK =FISCPER.DT_SK
LEFT OUTER JOIN FDR.FDR_AUTO_SOI ASOI ON PLCY.PLCY_ID_SK = ASOI.PLCY_ID_SK AND SOI.UNIT_NUM = ASOI.UNIT_NUM AND PLCY.TERM_STRT_DT = ASOI.EFF_DT
WHERE 1 = 1 AND ASOI.ANTI_THFT_CTGY_CD IS NOT NULL 
AND TRIM(GEOS.ST_NM) IN ({GEO_ST_NM_BCKP})   
AND fiscper.clndr_yr = {RPT_YEAR}  
AND AGDM.DIM_AG_TRANS_TYP_PLCY.TRANS_TYP_PLCY_CD NOT IN ('WO'))A
    WHERE A.CNT1 = 1 )THEFT
ON 
TRIM(THEFT.PLCY_CNTRCT_NUM)=TRIM(SQ.PLCY_CNTRCT_NUM) AND THEFT.UNIT_NUM=SQ.UNIT_NUM AND SQ.TERM_STRT_DT=THEFT.TERM_STRT_DT
WHERE {BACKENDFIX_DT} = {CLNDR_YR} AND LTRIM(RTRIM(NISS_CMPNY_CD)) NOT IN ('180') --ADDED THIS FILTER FOR ONLY 2022 FILING.
"""

        try:
            self.logger.info("Reading SQ_Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_DTL from Snowflake via SQL override", node="SQ_Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_DTL")
            df_SQ_Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_DTL_awesome_faraday = SnowflakeReader(self.glue_context).read(
                connection_name=SNOWFLAKE_CONNECTION_FDWS_FDRWRK,
                query=sql_query,
            )
            if df_SQ_Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_DTL_awesome_faraday.isEmpty():
                self.logger.warning("Source SQ_Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_DTL returned no rows", node="SQ_Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_DTL")
        except Exception:
            self.logger.exception("Failed reading SQ_Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_DTL from Snowflake")
            raise

        # EXPTRANS: passthrough/project explicit ports
        try:
            self.logger.info("Applying EXPTRANS passthrough projection", node="EXPTRANS")
            df_EXPTRANS_blissful_pasteur = (
                df_SQ_Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_DTL_awesome_faraday
                .selectExpr(
                    "FISC_PER_YR",
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
                    "NJ_NO_LWST_LMT_IND",
                    "NJ_NMD_DRVR_EXCL_IND",
                    "COMP_DED",
                    "COLL_DED",
                    "PLCY_CNTRCT_NUM",
                    "UNIT_NUM",
                    "EFF_DT",
                    "NUM_OF_CARS_IN_HH",
                    "RDRVR_DT_OF_BRTH",
                    "TERM_STRT_DT",
                    "SRC_SYS_CD",
                    "PNI_AGE",
                    "MIS_LOB",
                    "PRINCIPAL_OPRT",
                    "SOURCE_IND_DERIVED",
                    "ANTI_THFT_CTGY_CD",
                )
            )
        except Exception:
            self.logger.exception("Failed applying EXPTRANS passthrough projection")
            raise

        # LKP_FDR_LIB_RBI_REF_AUTO_TERR_ByStZipLob: load small reference set from Snowflake (used as lookup)
        sql_query_lkp = f"""SELECT
REF.NISS_TERR_CD as NISS_TERR_CD,
REF.NISS_ST_CD as NISS_ST_CD,
REF.ST_ABBRV as ST_ABBRV,
REF.ZIP_CD as ZIP_CD
FROM BIRP.RBI_REF_AUTO_TERR REF
WHERE REF.END_EFF_DT='2999-12-31'
AND REF.PP_COMMRCL_CD IN ('BOTH','PP')
ORDER BY NISS_ST_CD, ST_ABBRV, ZIP_CD
"""
        try:
            self.logger.info("Reading lookup LKP_FDR_LIB_RBI_REF_AUTO_TERR_ByStZipLob from Snowflake", node="LKP_FDR_LIB_RBI_REF_AUTO_TERR_ByStZipLob")
            df_LKP_FDR_LIB_RBI_REF_AUTO_TERR_ByStZipLob_amazing_hilbert = SnowflakeReader(self.glue_context).read(
                connection_name=SNOWFLAKE_CONNECTION_DBCONNECTION_FDWS_REF_ODBC,
                query=sql_query_lkp,
            )
            if df_LKP_FDR_LIB_RBI_REF_AUTO_TERR_ByStZipLob_amazing_hilbert.isEmpty():
                self.logger.warning("Lookup LKP_FDR_LIB_RBI_REF_AUTO_TERR_ByStZipLob returned no rows", node="LKP_FDR_LIB_RBI_REF_AUTO_TERR_ByStZipLob")
        except Exception:
            self.logger.exception("Failed reading LKP_FDR_LIB_RBI_REF_AUTO_TERR_ByStZipLob from Snowflake")
            raise

        # EXP_PassThru: join main stream with lookup (left) to bring lookup columns inline
        try:
            self.logger.info("Applying EXP_PassThru join of EXPTRANS with lookup", node="EXP_PassThru")
            # rename lookup columns to avoid ambiguity
            df_lkp_ren = (
                df_LKP_FDR_LIB_RBI_REF_AUTO_TERR_ByStZipLob_amazing_hilbert
                .select(
                    col("NISS_TERR_CD").alias("LKP_NISS_TERR_CD"),
                    col("NISS_ST_CD").alias("LKP_NISS_ST_CD"),
                    col("ST_ABBRV").alias("LKP_ST_ABBRV"),
                    col("ZIP_CD").alias("LKP_ZIP_CD"),
                )
            )

            # join keys: NISS_ST_CD, ST_ABBR and GRGNG_ZIP_5 <-> ZIP_CD
            cond = (
                trim(col("NISS_ST_CD")) == trim(col("LKP_NISS_ST_CD"))
            ) & (
                trim(col("ST_ABBR")) == trim(col("LKP_ST_ABBRV"))
            ) & (
                trim(col("GRGNG_ZIP_5")) == trim(col("LKP_ZIP_CD"))
            )

            df_EXP_PassThru_elegant_descartes = (
                df_EXPTRANS_blissful_pasteur.join(broadcast(df_lkp_ren), cond, how="left")
            )
        except Exception:
            self.logger.exception("Failed applying EXP_PassThru join")
            raise

        # FDR_LIB_mplt_ABC_MAPPING_AUDIT: produce single-row audit dataframe (broadcastable)
        try:
            self.logger.info("Building mapplet FDR_LIB_mplt_ABC_MAPPING_AUDIT single-row audit payload", node="FDR_LIB_mplt_ABC_MAPPING_AUDIT")
            # Use incoming stream to derive a deterministic single row; fill mapping/workflow ids as NULL when unknown
            df_mplt_ABC_mapping_source = df_EXP_PassThru_elegant_descartes.limit(1)
            df_mplt_ABC_MAPPING_AUDIT_modest_kepler = (
                df_mplt_ABC_mapping_source
                .select(
                    lit(None).cast("long").alias("CR_BY_MAPNG_ID"),
                    current_timestamp().alias("DW_CR_TMSP"),
                    lit(None).cast("long").alias("UPD_BY_MAPNG_ID"),
                    current_timestamp().alias("DW_UPD_TMSP"),
                    lit(None).cast("long").alias("WRK_FLOW_RUN_ID"),
                )
                .distinct()
            )
            # cache since reused by downstream joins
            df_mplt_ABC_MAPPING_AUDIT_modest_kepler = df_mplt_ABC_MAPPING_AUDIT_modest_kepler.cache()
        except Exception:
            self.logger.exception("Failed building mapplet audit payload")
            raise

        # LKP_FDR_LIB_RBI_REF_AUTO_TERR_ByStZipLob1: second read of the same reference (mapping contains two lookup nodes)
        try:
            self.logger.info("Reading lookup LKP_FDR_LIB_RBI_REF_AUTO_TERR_ByStZipLob1 from Snowflake", node="LKP_FDR_LIB_RBI_REF_AUTO_TERR_ByStZipLob1")
            df_LKP_FDR_LIB_RBI_REF_AUTO_TERR_ByStZipLob_careful_einstein = SnowflakeReader(self.glue_context).read(
                connection_name=SNOWFLAKE_CONNECTION_DBCONNECTION_FDWS_REF_ODBC,
                query=sql_query_lkp,
            )
            if df_LKP_FDR_LIB_RBI_REF_AUTO_TERR_ByStZipLob_careful_einstein.isEmpty():
                self.logger.warning("Lookup LKP_FDR_LIB_RBI_REF_AUTO_TERR_ByStZipLob1 returned no rows", node="LKP_FDR_LIB_RBI_REF_AUTO_TERR_ByStZipLob1")
        except Exception:
            self.logger.exception("Failed reading LKP_FDR_LIB_RBI_REF_AUTO_TERR_ByStZipLob1 from Snowflake")
            raise

        # EXP_PassThru_Tgt: join main stream with second lookup and cross-attach audit mapplet, derive outputs including surrogate key
        try:
            self.logger.info("Applying EXP_PassThru_Tgt joins, audit attach, and derived columns", node="EXP_PassThru_Tgt")
            # prepare second lookup rename
            df_lkp2_ren = (
                df_LKP_FDR_LIB_RBI_REF_AUTO_TERR_ByStZipLob_careful_einstein
                .select(
                    col("NISS_TERR_CD").alias("L2_NISS_TERR_CD"),
                    col("NISS_ST_CD").alias("L2_NISS_ST_CD"),
                    col("ST_ABBRV").alias("L2_ST_ABBRV"),
                    col("ZIP_CD").alias("L2_ZIP_CD"),
                )
            )

            cond2 = (
                trim(col("NISS_ST_CD")) == trim(col("L2_NISS_ST_CD"))
            ) & (
                trim(col("ST_ABBR")) == trim(col("L2_ST_ABBRV"))
            ) & (
                trim(col("GRGNG_ZIP_5")) == trim(col("L2_ZIP_CD"))
            )

            df_after_lkp2 = (
                df_EXP_PassThru_elegant_descartes.join(broadcast(df_lkp2_ren), cond2, how="left")
            )

            # attach single-row audit mapplet by cross-joining (mapplet is single-row)
            # crossJoin is safe here because audit DF is tiny
            df_after_audit = df_after_lkp2.crossJoin(df_mplt_ABC_MAPPING_AUDIT_modest_kepler)

            # generate derived outputs: o_AUTO_USE_CD, CALL_YR (from CLNDR_YR mapping variable), and surrogate key
            win = Window.orderBy(lit(1))

            df_EXP_PassThru_Tgt_busy_nash = (
                df_after_audit
                .withColumn("o_AUTO_USE_CD", when(col("AUTO_USE_CD").isNull(), lit("")).otherwise(col("AUTO_USE_CD")))
                .withColumn("CALL_YR", lit(CLNDR_YR))
                .withColumn("NISS_APRM_DETL_SK", row_number().over(win))
            )
        except Exception:
            self.logger.exception("Failed applying EXP_PassThru_Tgt logic")
            raise

        # assign final target dataframe variable
        df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_elegant_galileo = df_EXP_PassThru_Tgt_busy_nash

        # write intermediate WRK_ table as parquet to S3 (overwrite)
        try:
            self.logger.info("Writing WRK_BIRP_NISS_APRM_DETL to S3 as parquet (overwrite)", table="WRK_BIRP_NISS_APRM_DETL")
            self.parquet_writer.write(
                dataframe=df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_elegant_galileo,
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                mode="overwrite",
            )
        except Exception:
            self.logger.exception("Failed writing WRK_BIRP_NISS_APRM_DETL to S3")
            raise

        self.glue_job.commit()

    def finalize(self) -> None:
        self.logger.info("Finalizing m_NU0C_NISS_AUTO_102_ATPRM_DTL_Load")

    @staticmethod
    def _require_env(env: Mapping[str, Any], key: str) -> str:

        value = env.get(key)

        if not value or str(value).startswith("REPLACE_WITH_"):
            raise ConfigurationValidationError(
                f"config.yaml 'env.{key}' must be set"
            )

        return str(value)
