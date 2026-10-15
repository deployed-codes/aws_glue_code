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
    when,
    lit,
    coalesce,
    current_timestamp,
    row_number,
    broadcast,
    substr
)
from pyspark.sql.window import Window
from glue_lib.connector.snowflake import SnowflakeReader


# Informatica mapping m_NU0C_NISS_AUTO_100_ATPRM_LND_Load.
# Every mapping node runs in order inside run().
class Nu0cNissAuto100AtprmLndLoadJob(BaseJob):

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

        # Environment / mapping parameters
        SNOWFLAKE_CONNECTION_FDWS_AGDM = self._require_env(self.env, "SNOWFLAKE_CONNECTION_FDWS_AGDM")
        LOOKUPFILE_FF_NISS_STATE = self._require_env(self.env, "LOOKUPFILE_FF_NISS_STATE")
        GEO_ST_NM = self._require_env(self.env, "GEO_ST_NM")
        RUN_TYPE = self._require_env(self.env, "RUN_TYPE")
        RPT_YEAR = self._require_env(self.env, "RPT_YEAR")
        SQ_FILTER_1 = self._require_env(self.env, "SQ_FILTER_1")

        # FDR_LIB_FACT_AG_WRITTN_PREM_CVG_LVL: read from Glue Data Catalog
        try:
            self.logger.info("Reading FDR_LIB_FACT_AG_WRITTN_PREM_CVG_LVL from Glue Data Catalog", node="FDR_LIB_FACT_AG_WRITTN_PREM_CVG_LVL")
            dyf = self.glue_context.create_dynamic_frame.from_catalog(
                database=self.glue_database,
                table_name="FACT_AG_WRITTN_PREM_CVG_LVL",
                transformation_ctx="df_FDR_LIB_FACT_AG_WRITTN_PREM_CVG_LVL_friendly_newton",
            )
            df_FDR_LIB_FACT_AG_WRITTN_PREM_CVG_LVL_friendly_newton = dyf.toDF()
            if df_FDR_LIB_FACT_AG_WRITTN_PREM_CVG_LVL_friendly_newton.isEmpty():
                self.logger.warning("Source FDR_LIB_FACT_AG_WRITTN_PREM_CVG_LVL returned no rows", node="FDR_LIB_FACT_AG_WRITTN_PREM_CVG_LVL")
        except Exception:
            self.logger.exception("Failed reading FDR_LIB_FACT_AG_WRITTN_PREM_CVG_LVL from Glue Data Catalog")
            raise

        # FDR_LIB_DIM_AG_SOI: read
        try:
            self.logger.info("Reading FDR_LIB_DIM_AG_SOI from Glue Data Catalog", node="FDR_LIB_DIM_AG_SOI")
            dyf = self.glue_context.create_dynamic_frame.from_catalog(
                database=self.glue_database,
                table_name="DIM_AG_SOI",
                transformation_ctx="df_DIM_AG_SOI_loving_pascal",
            )
            df_DIM_AG_SOI_loving_pascal = dyf.toDF()
            if df_DIM_AG_SOI_loving_pascal.isEmpty():
                self.logger.warning("Source DIM_AG_SOI returned no rows", node="FDR_LIB_DIM_AG_SOI")
        except Exception:
            self.logger.exception("Failed reading DIM_AG_SOI from Glue Data Catalog")
            raise

        # FDR_LIB_DIM_AG_PLCY_ENH: read
        try:
            self.logger.info("Reading FDR_LIB_DIM_AG_PLCY_ENH from Glue Data Catalog", node="FDR_LIB_DIM_AG_PLCY_ENH")
            dyf = self.glue_context.create_dynamic_frame.from_catalog(
                database=self.glue_database,
                table_name="DIM_AG_PLCY_ENH",
                transformation_ctx="df_FDR_LIB_DIM_AG_PLCY_ENH_blissful_noether",
            )
            df_FDR_LIB_DIM_AG_PLCY_ENH_blissful_noether = dyf.toDF()
            if df_FDR_LIB_DIM_AG_PLCY_ENH_blissful_noether.isEmpty():
                self.logger.warning("Source DIM_AG_PLCY_ENH returned no rows", node="FDR_LIB_DIM_AG_PLCY_ENH")
        except Exception:
            self.logger.exception("Failed reading DIM_AG_PLCY_ENH from Glue Data Catalog")
            raise

        # FDR_LIB_DIM_AG_PLCY: read
        try:
            self.logger.info("Reading FDR_LIB_DIM_AG_PLCY from Glue Data Catalog", node="FDR_LIB_DIM_AG_PLCY")
            dyf = self.glue_context.create_dynamic_frame.from_catalog(
                database=self.glue_database,
                table_name="DIM_AG_PLCY",
                transformation_ctx="df_FDR_LIB_DIM_AG_PLCY_keen_leibniz",
            )
            df_FDR_LIB_DIM_AG_PLCY_keen_leibniz = dyf.toDF()
            if df_FDR_LIB_DIM_AG_PLCY_keen_leibniz.isEmpty():
                self.logger.warning("Source DIM_AG_PLCY returned no rows", node="FDR_LIB_DIM_AG_PLCY")
        except Exception:
            self.logger.exception("Failed reading DIM_AG_PLCY from Glue Data Catalog")
            raise

        # FDR_LIB_DIM_AG_CVG: read
        try:
            self.logger.info("Reading FDR_LIB_DIM_AG_CVG from Glue Data Catalog", node="FDR_LIB_DIM_AG_CVG")
            dyf = self.glue_context.create_dynamic_frame.from_catalog(
                database=self.glue_database,
                table_name="DIM_AG_CVG",
                transformation_ctx="df_FDR_LIB_DIM_AG_CVG_blissful_darwin",
            )
            df_FDR_LIB_DIM_AG_CVG_blissful_darwin = dyf.toDF()
            if df_FDR_LIB_DIM_AG_CVG_blissful_darwin.isEmpty():
                self.logger.warning("Source DIM_AG_CVG returned no rows", node="FDR_LIB_DIM_AG_CVG")
        except Exception:
            self.logger.exception("Failed reading DIM_AG_CVG from Glue Data Catalog")
            raise

        # FDR_LIB_DIM_AG_RATED_GEO: read
        try:
            self.logger.info("Reading FDR_LIB_DIM_AG_RATED_GEO from Glue Data Catalog", node="FDR_LIB_DIM_AG_RATED_GEO")
            dyf = self.glue_context.create_dynamic_frame.from_catalog(
                database=self.glue_database,
                table_name="DIM_AG_RATED_GEO",
                transformation_ctx="df_FDR_LIB_DIM_AG_RATED_GEO_keen_rutherford",
            )
            df_FDR_LIB_DIM_AG_RATED_GEO_keen_rutherford = dyf.toDF()
            if df_FDR_LIB_DIM_AG_RATED_GEO_keen_rutherford.isEmpty():
                self.logger.warning("Source DIM_AG_RATED_GEO returned no rows", node="FDR_LIB_DIM_AG_RATED_GEO")
        except Exception:
            self.logger.exception("Failed reading DIM_AG_RATED_GEO from Glue Data Catalog")
            raise

        # FDR_LIB_DIM_AG_CVG_ENH: read
        try:
            self.logger.info("Reading FDR_LIB_DIM_AG_CVG_ENH from Glue Data Catalog", node="FDR_LIB_DIM_AG_CVG_ENH")
            dyf = self.glue_context.create_dynamic_frame.from_catalog(
                database=self.glue_database,
                table_name="DIM_AG_CVG_ENH",
                transformation_ctx="df_FDR_LIB_DIM_AG_CVG_ENH_modest_bohr",
            )
            df_FDR_LIB_DIM_AG_CVG_ENH_modest_bohr = dyf.toDF()
            if df_FDR_LIB_DIM_AG_CVG_ENH_modest_bohr.isEmpty():
                self.logger.warning("Source DIM_AG_CVG_ENH returned no rows", node="FDR_LIB_DIM_AG_CVG_ENH")
        except Exception:
            self.logger.exception("Failed reading DIM_AG_CVG_ENH from Glue Data Catalog")
            raise

        # FDR_LIB_DIM_AG_FARMR_GEO_ST: read
        try:
            self.logger.info("Reading FDR_LIB_DIM_AG_FARMR_GEO_ST from Glue Data Catalog", node="FDR_LIB_DIM_AG_FARMR_GEO_ST")
            dyf = self.glue_context.create_dynamic_frame.from_catalog(
                database=self.glue_database,
                table_name="DIM_AG_FARMR_GEO_ST",
                transformation_ctx="df_FDR_LIB_DIM_AG_FARMR_GEO_ST_reverent_hume",
            )
            df_FDR_LIB_DIM_AG_FARMR_GEO_ST_reverent_hume = dyf.toDF()
            if df_FDR_LIB_DIM_AG_FARMR_GEO_ST_reverent_hume.isEmpty():
                self.logger.warning("Source DIM_AG_FARMR_GEO_ST returned no rows", node="FDR_LIB_DIM_AG_FARMR_GEO_ST")
        except Exception:
            self.logger.exception("Failed reading DIM_AG_FARMR_GEO_ST from Glue Data Catalog")
            raise

        # FDR_LIB_DIM_AG_MINI_PLCY: read
        try:
            self.logger.info("Reading FDR_LIB_DIM_AG_MINI_PLCY from Glue Data Catalog", node="FDR_LIB_DIM_AG_MINI_PLCY")
            dyf = self.glue_context.create_dynamic_frame.from_catalog(
                database=self.glue_database,
                table_name="DIM_AG_MINI_PLCY",
                transformation_ctx="df_FDR_LIB_DIM_AG_MINI_PLCY_loving_planck",
            )
            df_FDR_LIB_DIM_AG_MINI_PLCY_loving_planck = dyf.toDF()
            if df_FDR_LIB_DIM_AG_MINI_PLCY_loving_planck.isEmpty():
                self.logger.warning("Source DIM_AG_MINI_PLCY returned no rows", node="FDR_LIB_DIM_AG_MINI_PLCY")
        except Exception:
            self.logger.exception("Failed reading DIM_AG_MINI_PLCY from Glue Data Catalog")
            raise

        # FDR_LIB_DIM_DT: read
        try:
            self.logger.info("Reading FDR_LIB_DIM_DT from Glue Data Catalog", node="FDR_LIB_DIM_DT")
            dyf = self.glue_context.create_dynamic_frame.from_catalog(
                database=self.glue_database,
                table_name="DIM_DT",
                transformation_ctx="df_FDR_LIB_DIM_DT_adoring_feynman",
            )
            df_FDR_LIB_DIM_DT_adoring_feynman = dyf.toDF()
            if df_FDR_LIB_DIM_DT_adoring_feynman.isEmpty():
                self.logger.warning("Source DIM_DT returned no rows", node="FDR_LIB_DIM_DT")
        except Exception:
            self.logger.exception("Failed reading DIM_DT from Glue Data Catalog")
            raise

        # FDR_LIB_DIM_AG_MINI_CVG: read
        try:
            self.logger.info("Reading FDR_LIB_DIM_AG_MINI_CVG from Glue Data Catalog", node="FDR_LIB_DIM_AG_MINI_CVG")
            dyf = self.glue_context.create_dynamic_frame.from_catalog(
                database=self.glue_database,
                table_name="DIM_AG_MINI_CVG",
                transformation_ctx="df_FDR_LIB_DIM_AG_MINI_CVG_jovial_bohr",
            )
            df_FDR_LIB_DIM_AG_MINI_CVG_jovial_bohr = dyf.toDF()
            if df_FDR_LIB_DIM_AG_MINI_CVG_jovial_bohr.isEmpty():
                self.logger.warning("Source DIM_AG_MINI_CVG returned no rows", node="FDR_LIB_DIM_AG_MINI_CVG")
        except Exception:
            self.logger.exception("Failed reading DIM_AG_MINI_CVG from Glue Data Catalog")
            raise

        # FDR_LIB_DIM_AG_SOI_ENH: read
        try:
            self.logger.info("Reading FDR_LIB_DIM_AG_SOI_ENH from Glue Data Catalog", node="FDR_LIB_DIM_AG_SOI_ENH")
            dyf = self.glue_context.create_dynamic_frame.from_catalog(
                database=self.glue_database,
                table_name="DIM_AG_SOI_ENH",
                transformation_ctx="df_FDR_LIB_DIM_AG_SOI_ENH_cool_hawking",
            )
            df_FDR_LIB_DIM_AG_SOI_ENH_cool_hawking = dyf.toDF()
            if df_FDR_LIB_DIM_AG_SOI_ENH_cool_hawking.isEmpty():
                self.logger.warning("Source DIM_AG_SOI_ENH returned no rows", node="FDR_LIB_DIM_AG_SOI_ENH")
        except Exception:
            self.logger.exception("Failed reading DIM_AG_SOI_ENH from Glue Data Catalog")
            raise

        # FDR_LIB_DIM_AG_MINI_SOI: read
        try:
            self.logger.info("Reading FDR_LIB_DIM_AG_MINI_SOI from Glue Data Catalog", node="FDR_LIB_DIM_AG_MINI_SOI")
            dyf = self.glue_context.create_dynamic_frame.from_catalog(
                database=self.glue_database,
                table_name="DIM_AG_MINI_SOI",
                transformation_ctx="df_FDR_LIB_DIM_AG_MINI_SOI_happy_hume",
            )
            df_FDR_LIB_DIM_AG_MINI_SOI_happy_hume = dyf.toDF()
            if df_FDR_LIB_DIM_AG_MINI_SOI_happy_hume.isEmpty():
                self.logger.warning("Source DIM_AG_MINI_SOI returned no rows", node="FDR_LIB_DIM_AG_MINI_SOI")
        except Exception:
            self.logger.exception("Failed reading DIM_AG_MINI_SOI from Glue Data Catalog")
            raise

        # SQ_FDR_LIB_FACT_AG_WRITTN_PREM_CVG_LVL: SQL override pushed to Snowflake
        sql_query = f"""SELECT * FROM (SELECT
REG_PER_YR,
FISC_PER_YR,
NAIC_CMPY_CD,
LTRIM(RTRIM(ST_NM)) AS ST_NM,
LTRIM(RTRIM(ST_CD)) AS ST_CD,
LTRIM(RTRIM(ACCTNG_LOB)) AS ACCTNG_LOB,
CVG_TYP_CD,
CVG_AMT,
BI_LMT,
GA_UMBI_PD_ADDED_IND,
FA2_PLCY_IND,
UM_UIM_STACKING,
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
NJ_NO_LWST_LMT_IND,
NJ_NMD_DRVR_EXCL_IND,
GRGNG_ZIP_5,
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
NJ_RTD_PNTS,
VEH_MDL_YR,
NJ_EXCPTION_CD,
NJ_FRGVN_PNTS,
PASSV_RESTRA_DISC,
SNR_DRVR_IND,
DEFNS_DRVR_DISC_IND,
ANTI_THFT_DISC,
DAY_TM_RUN_LIGHTS,
LMT_TORT,
CVG_TYP_IND,
CVG_EXPS_VAL,
TTL_WRITTN_PREM_AMT,
COMP_DED,
COLL_DED,
LTRIM(RTRIM(PLCY_CNTRCT_NUM)) AS PLCY_CNTRCT_NUM,
UNIT_NUM,
EFF_DT,
NUM_OF_CARS_IN_HH,
RDRVR_DT_OF_BRTH,
TERM_STRT_DT,
COALESCE(SRC_SYS_CD,'') as SRC_SYS_CD,
PNI_AGE
FROM (

SELECT
REGPER.CLNDR_YR REG_PER_YR,
FISCPER.CLNDR_YR FISC_PER_YR,
PLCY.PLCY_CNTRCT_NUM,
MPLCY.BI_LMT,
MPLCY.FA2_PLCY_IND,
MPLCY.RATNG_CMPY_CD,
EPLCY.NAIC_CMPY_CD,
EPLCY.PHY_DMG_IND,
EPLCY.NY_SSL_IND,
EPLCY.GA_UMBI_PD_ADDED_IND,
SOI.DRVR_TRNG_IND,
SOI.SOI_TYP,
SOI.VEH_MDL_YR,
SOI.SNR_DRVR_IND,
SOI.UNIT_NUM,
ESOI.UM_UIM_STACKING,
ESOI.PIP_WVR_WL_IND,
ESOI.PIP_MED_SEC_IND,
ESOI.PIP_LOSS_INCOME_IND,
ESOI.MI_PPO_IND,
ESOI.PRD_GRP_CD,
ESOI.NJ_HLTH_INSR_PRIM,
ESOI.NJ_EXTR_PIP_PKG,
ESOI.NJ_RESDNC_RLTNSHP_PIP_IND,
ESOI.NY_FULL_CVG_GLASS_COMP_IND,
ESOI.NJ_NO_LWST_LMT_IND,
ESOI.NJ_NMD_DRVR_EXCL_IND,
ESOI.RT_CLS,
ESOI.AGE,
ESOI.GENDR,
ESOI.MRTL_STAT,
ESOI.AUTO_USE_CD,
ESOI.MILES_TO_WRK,
ESOI.NJ_EXCPTION_CD,
ESOI.PASSV_RESTRA_DISC,
ESOI.DEFNS_DRVR_DISC_IND,
ESOI.ANTI_THFT_DISC,
ESOI.DAY_TM_RUN_LIGHTS,
ESOI.LMT_TORT,
ESOI.COMP_DED,
MSOI.MLT_CAR_IND,
MSOI.GOOD_STDNT_IND,
MSOI.COLL_DED,
CVG.ACCTNG_LOB,
CVG.CVG_TYP_CD,
ECVG.EFF_DT,
ECVG.NJ_RTD_PNTS,
ECVG.NJ_FRGVN_PNTS,
ECVG.CVG_TYP_IND,
MCVG.CVG_AMT,
GEOS.ST_NM,
GEOS.ST_CD,
RGEO.GRGNG_ZIP_5,
SUM(COALESCE(FACT.TTL_WRITTN_PREM_AMT,0)) TTL_WRITTN_PREM_AMT,
SUM(COALESCE(FACT.CVG_EXPS_VAL,0)) CVG_EXPS_VAL,
HH.NUM_OF_CARS_IN_HH,
ESOI.RDRVR_DT_OF_BRTH,
PLCY.TERM_STRT_DT,
PLCY.SRC_SYS_CD,
PLCY.PNI_AGE

FROM AGDM.FACT_AG_WRITTN_PREM_CVG_LVL FACT
JOIN AGDM.DIM_AG_PLCY PLCY
ON FACT.PLCY_SK=PLCY.PLCY_SK
JOIN AGDM.DIM_AG_MINI_PLCY MPLCY
ON FACT.MINI_PLCY_SK = MPLCY.MINI_PLCY_SK
JOIN AGDM.DIM_AG_PLCY_ENH EPLCY
ON FACT.PLCY_ENH_SK = EPLCY.PLCY_ENH_SK
JOIN AGDM.DIM_AG_TRANS_TYP_PLCY 
ON ( FACT.TRANS_TYP_PLCY_SK=AGDM.DIM_AG_TRANS_TYP_PLCY.TRANS_TYP_PLCY_SK )
JOIN AGDM.DIM_AG_SOI SOI
ON FACT.SOI_SK = SOI.SOI_SK
JOIN AGDM.DIM_AG_MINI_SOI MSOI
ON FACT.MINI_SOI_SK = MSOI.MINI_SOI_SK
JOIN AGDM.DIM_AG_SOI_ENH ESOI
ON FACT.SOI_ENH_SK = ESOI.SOI_ENH_SK
JOIN AGDM.DIM_AG_CVG CVG
ON FACT.CVG_SK = CVG.CVG_SK
JOIN AGDM.DIM_AG_MINI_CVG MCVG
ON FACT.MINI_CVG_SK = MCVG.MINI_CVG_SK
JOIN AGDM.DIM_AG_CVG_ENH ECVG
ON FACT.ON_OFF_PREM_SK = ECVG.ON_OFF_PREM_SK
JOIN AGDM.DIM_AG_FARMR_GEO_DISTR GEOD
ON FACT.FARMR_GEO_DISTR_SK = GEOD.FARMR_GEO_DISTR_SK
JOIN AGDM.DIM_AG_FARMR_GEO_ST GEOS
ON GEOD.FARMR_GEO_ST_SK = GEOS.FARMR_GEO_ST_SK
JOIN AGDM.DIM_AG_RATED_GEO RGEO
ON FACT.RATED_GEO_SK = RGEO.RATED_GEO_SK
JOIN AGDM.DIM_DT  REGPER
ON FACT.REGSTR_PER_SK=REGPER.DT_SK
JOIN AGDM.DIM_DT  FISCPER
ON FACT.FISC_PER_SK =FISCPER.DT_SK
JOIN AGDM.DIM_AG_HH HH
ON HH.HH_SK=FACT.HH_SK
WHERE 1 = 1
AND TRIM(GEOS.ST_NM) NOT IN ({GEO_ST_NM})
AND (case when '{RUN_TYPE}' ='Register' then REGPER.CLNDR_YR else fiscper.clndr_yr end ) = {RPT_YEAR}
AND AGDM.DIM_AG_TRANS_TYP_PLCY.TRANS_TYP_PLCY_CD NOT IN ('WO') and (TTL_WRITTN_PREM_AMT !=0 AND CVG_EXPS_VAL!=0)
GROUP BY 
REGPER.CLNDR_YR,
FISCPER.CLNDR_YR ,
PLCY.PLCY_CNTRCT_NUM,
MPLCY.BI_LMT,
MPLCY.FA2_PLCY_IND,
MPLCY.RATNG_CMPY_CD,
EPLCY.NAIC_CMPY_CD,
EPLCY.PHY_DMG_IND,
EPLCY.NY_SSL_IND,
EPLCY.GA_UMBI_PD_ADDED_IND,
SOI.DRVR_TRNG_IND,
SOI.SOI_TYP,
SOI.VEH_MDL_YR,
SOI.SNR_DRVR_IND,
SOI.UNIT_NUM,
ESOI.UM_UIM_STACKING,
ESOI.PIP_WVR_WL_IND,
ESOI.PIP_MED_SEC_IND,
ESOI.PIP_LOSS_INCOME_IND,
ESOI.MI_PPO_IND,
ESOI.PRD_GRP_CD,
ESOI.NJ_HLTH_INSR_PRIM,
ESOI.NJ_EXTR_PIP_PKG,
ESOI.NJ_RESDNC_RLTNSHP_PIP_IND,
ESOI.NY_FULL_CVG_GLASS_COMP_IND,
ESOI.NJ_NO_LWST_LMT_IND,
ESOI.NJ_NMD_DRVR_EXCL_IND,
ESOI.RT_CLS,
ESOI.AGE,
ESOI.GENDR,
ESOI.MRTL_STAT,
ESOI.AUTO_USE_CD,
ESOI.MILES_TO_WRK,
ESOI.NJ_EXCPTION_CD,
ESOI.PASSV_RESTRA_DISC,
ESOI.DEFNS_DRVR_DISC_IND,
ESOI.ANTI_THFT_DISC,
ESOI.DAY_TM_RUN_LIGHTS,
ESOI.LMT_TORT,
ESOI.COMP_DED,
MSOI.MLT_CAR_IND,
MSOI.GOOD_STDNT_IND,
MSOI.COLL_DED,
CVG.ACCTNG_LOB,
CVG.CVG_TYP_CD,
ECVG.EFF_DT,
ECVG.NJ_RTD_PNTS,
ECVG.NJ_FRGVN_PNTS,
ECVG.CVG_TYP_IND,
MCVG.CVG_AMT,
GEOS.ST_NM,
GEOS.ST_CD,
RGEO.GRGNG_ZIP_5,
HH.NUM_OF_CARS_IN_HH,
ESOI.RDRVR_DT_OF_BRTH,
PLCY.TERM_STRT_DT,
PLCY.SRC_SYS_CD,
PLCY.PNI_AGE
)
ORDER BY ST_CD, ST_NM)"""

        try:
            self.logger.info("Reading SQ_FDR_LIB_FACT_AG_WRITTN_PREM_CVG_LVL from Snowflake via SQL override", node="SQ_FDR_LIB_FACT_AG_WRITTN_PREM_CVG_LVL")
            df_SQ_FDR_LIB_FACT_AG_WRITTN_PREM_CVG_LVL_blissful_ramanujan = SnowflakeReader(self.glue_context).read(
                connection_name=SNOWFLAKE_CONNECTION_FDWS_AGDM,
                query=sql_query,
            )
            if df_SQ_FDR_LIB_FACT_AG_WRITTN_PREM_CVG_LVL_blissful_ramanujan.isEmpty():
                self.logger.warning("Source SQ_FDR_LIB_FACT_AG_WRITTN_PREM_CVG_LVL returned no rows", node="SQ_FDR_LIB_FACT_AG_WRITTN_PREM_CVG_LVL")
        except Exception:
            self.logger.exception("Failed reading SQ_FDR_LIB_FACT_AG_WRITTN_PREM_CVG_LVL from Snowflake")
            raise

        # EXP_Defaults: explicit passthrough select of SQ ports
        try:
            self.logger.info("Applying EXP_Defaults expressions", node="EXP_Defaults")
            df_EXP_Defaults_jovial_feynman = df_SQ_FDR_LIB_FACT_AG_WRITTN_PREM_CVG_LVL_blissful_ramanujan.selectExpr(
                "REG_PER_YR",
                "FISC_PER_YR",
                "NAIC_CMPY_CD",
                "ST_NM",
                "ST_CD",
                "ACCTNG_LOB",
                "CVG_TYP_CD",
                "CVG_AMT",
                "BI_LMT",
                "GA_UMBI_PD_ADDED_IND",
                "FA2_PLCY_IND",
                "UM_UIM_STACKING",
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
                "NJ_NO_LWST_LMT_IND",
                "NJ_NMD_DRVR_EXCL_IND",
                "GRGNG_ZIP_5",
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
                "NJ_RTD_PNTS",
                "VEH_MDL_YR",
                "NJ_EXCPTION_CD",
                "NJ_FRGVN_PNTS",
                "PASSV_RESTRA_DISC",
                "SNR_DRVR_IND",
                "DEFNS_DRVR_DISC_IND",
                "ANTI_THFT_DISC",
                "DAY_TM_RUN_LIGHTS",
                "LMT_TORT",
                "CVG_TYP_IND",
                "CVG_EXPS_VAL",
                "TTL_WRITTN_PREM_AMT",
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
            )
        except Exception:
            self.logger.exception("Failed applying EXP_Defaults expressions")
            raise

        # EXP_Passthru: compute v_FARMERS_STATE_CD, IFARMERS_STATE_CD and normalize GRGNG_ZIP_5
        try:
            self.logger.info("Applying EXP_Passthru expressions", node="EXP_Passthru")
            df_EXP_Passthru_nifty_schrodinger = (
                df_EXP_Defaults_jovial_feynman
                .withColumn("v_FARMERS_STATE_CD", when(trim(col("ST_CD")) == "#", lit("00")).otherwise(trim(col("ST_CD"))))
                .withColumn("IFARMERS_STATE_CD", col("v_FARMERS_STATE_CD").cast("int"))
                .withColumn(
                    "GRGNG_ZIP_5",
                    when(col("GRGNG_ZIP_5").isNull(), lit("00000"))
                    .when(trim(col("GRGNG_ZIP_5")) == "", lit("00000"))
                    .when(trim(col("GRGNG_ZIP_5")) == "0", lit("00000"))
                    .otherwise(trim(col("GRGNG_ZIP_5")))
                )
            )
        except Exception:
            self.logger.exception("Failed applying EXP_Passthru expressions")
            raise

        # EXP_To_Derive_Vals: trim NAIC, map to NISS company code, normalize state codes, derive flags and constants
        try:
            self.logger.info("Applying EXP_To_Derive_Vals expressions", node="EXP_To_Derive_Vals")
            df = df_EXP_Passthru_nifty_schrodinger
            # trimmed NAIC
            df = df.withColumn("v_NAIC_CMPY_CD", trim(col("NAIC_CMPY_CD"))).withColumn("o_NAIC_CMPY_CD", col("v_NAIC_CMPY_CD"))
            # decode v_NAIC_CMPY_CD -> o_NISS_CMPNY_CD
            df = df.withColumn(
                "o_NISS_CMPNY_CD",
                when(col("v_NAIC_CMPY_CD") == "10315", lit("172"))
                .when(col("v_NAIC_CMPY_CD") == "10317", lit("174"))
                .when(col("v_NAIC_CMPY_CD") == "10318", lit("173"))
                .when(col("v_NAIC_CMPY_CD") == "10806", lit("171"))
                .when(col("v_NAIC_CMPY_CD") == "10873", lit("170"))
                .when(col("v_NAIC_CMPY_CD") == "21598", lit("177"))
                .when(col("v_NAIC_CMPY_CD") == "21601", lit("079"))
                .when(col("v_NAIC_CMPY_CD") == "21628", lit("073"))
                .when(col("v_NAIC_CMPY_CD") == "21636", lit("178"))
                .when(col("v_NAIC_CMPY_CD") == "21644", lit("077"))
                .when(col("v_NAIC_CMPY_CD") == "21652", lit("070"))
                .when(col("v_NAIC_CMPY_CD") == "21660", lit("175"))
                .when(col("v_NAIC_CMPY_CD") == "21679", lit("072"))
                .when(col("v_NAIC_CMPY_CD") == "21687", lit("176"))
                .when(col("v_NAIC_CMPY_CD") == "21695", lit("179"))
                .when(col("v_NAIC_CMPY_CD") == "21709", lit("071"))
                .when(col("v_NAIC_CMPY_CD") == "24392", lit("169"))
                .when(col("v_NAIC_CMPY_CD") == "28673", lit("180"))
                .when(col("v_NAIC_CMPY_CD") == "36889", lit("074"))
                .otherwise(lit(""))
            )
            # STATE_ABBR: try i_STATE_ABBR, else ST_CD
            if "i_STATE_ABBR" in df.columns:
                df = df.withColumn("STATE_ABBR", when(col("i_STATE_ABBR").isNull() | (trim(col("i_STATE_ABBR")) == ""), lit("?")).otherwise(trim(col("i_STATE_ABBR"))))
            else:
                df = df.withColumn("STATE_ABBR", when(col("ST_CD").isNull() | (trim(col("ST_CD")) == ""), lit("?")).otherwise(trim(col("ST_CD"))))
            # NISS_STATE_CODE: prefer i_NISS_STATE_CODE or NISS_STATE_CODE, else '?'
            if "i_NISS_STATE_CODE" in df.columns:
                df = df.withColumn("NISS_STATE_CODE", when(col("i_NISS_STATE_CODE").isNull() | (trim(col("i_NISS_STATE_CODE")) == ""), lit("?")).otherwise(trim(col("i_NISS_STATE_CODE"))))
            elif "NISS_STATE_CODE" in df.columns:
                df = df.withColumn("NISS_STATE_CODE", when(col("NISS_STATE_CODE").isNull() | (trim(col("NISS_STATE_CODE")) == ""), lit("?")).otherwise(trim(col("NISS_STATE_CODE"))))
            else:
                df = df.withColumn("NISS_STATE_CODE", lit("?"))
            # GA_UMBI_PD_ADDED_IND normalization from i_GA_UMBI_PD_ADDED_IND if present
            if "i_GA_UMBI_PD_ADDED_IND" in df.columns:
                df = df.withColumn("GA_UMBI_PD_ADDED_IND", when(col("i_GA_UMBI_PD_ADDED_IND") == lit(1), lit("Y")).otherwise(lit("N")))
            elif "GA_UMBI_PD_ADDED_IND" in df.columns:
                df = df.withColumn("GA_UMBI_PD_ADDED_IND", when(col("GA_UMBI_PD_ADDED_IND") == lit(1), lit("Y")).otherwise(lit("N")))
            else:
                df = df.withColumn("GA_UMBI_PD_ADDED_IND", lit("N"))
            # o_ANNL_STMNT_LOB_CD from ACCTNG_LOB
            if "ACCTNG_LOB" in df.columns:
                df = df.withColumn("o_ANNL_STMNT_LOB_CD", substr(trim(col("ACCTNG_LOB")), 1, 3))
            else:
                df = df.withColumn("o_ANNL_STMNT_LOB_CD", lit(None).cast("string"))
            # EXPS_VAL_ROLLED constant
            df = df.withColumn("EXPS_VAL_ROLLED", lit(0))
            # Informatica PM... system vars -> NULLs
            df = df.withColumn("MAPPING_NAME", lit(None).cast("string")).withColumn("FOLDER_NAME", lit(None).cast("string")).withColumn("WORKFLOW_NAME", lit(None).cast("string"))
            df_EXP_To_Derive_Vals_kind_noether = df
        except Exception:
            self.logger.exception("Failed applying EXP_To_Derive_Vals expressions")
            raise

        # LKP_ff_REF_NISS_STATE_CD: read flat-file lookup and left-join to bring NISS_STATE_CODE and FARMERS_STATE_NAME
        try:
            self.logger.info("Reading lookup file for LKP_ff_REF_NISS_STATE_CD and joining", node="LKP_ff_REF_NISS_STATE_CD")
            df_lookup = self.spark.read.option("header", True).csv(LOOKUPFILE_FF_NISS_STATE)
            df_lookup.cache()
            df_LKP_ff_REF_NISS_STATE_CD_elated_turing = (
                df_EXP_Passthru_nifty_schrodinger.join(
                    broadcast(df_lookup),
                    df_EXP_Passthru_nifty_schrodinger["ST_NM"] == df_lookup["FARMERS_STATE_NAME"],
                    how="left",
                )
                .select(
                    df_EXP_Passthru_nifty_schrodinger["*"],
                    df_lookup["FARMERS_STATE_NAME"].alias("FARMERS_STATE_NAME"),
                    df_lookup["NISS_STATE_CODE"].alias("NISS_STATE_CODE")
                )
            )
        except Exception:
            self.logger.exception("Failed reading or joining lookup for LKP_ff_REF_NISS_STATE_CD")
            raise

        # EXP_Passtotgt: produce surrogate key NISS_ATPRM_LND_SK and derive NISS_TERR_CD
        try:
            self.logger.info("Applying EXP_Passtotgt expressions and generating surrogate key", node="EXP_Passtotgt")
            tmp = df_EXP_To_Derive_Vals_kind_noether
            if "i_NISS_TERR_CD" in tmp.columns:
                tmp = tmp.withColumn("NISS_TERR_CD", when(col("i_NISS_TERR_CD").isNull() | (trim(col("i_NISS_TERR_CD")) == ""), lit("?")).otherwise(trim(col("i_NISS_TERR_CD"))))
            else:
                tmp = tmp.withColumn("NISS_TERR_CD", lit("?"))
            window = Window.orderBy(lit(1))
            df_EXP_Passtotgt_zen_fermat = tmp.withColumn("NISS_ATPRM_LND_SK", row_number().over(window))
        except Exception:
            self.logger.exception("Failed applying EXP_Passtotgt expressions")
            raise

        # FDR_LIB_mplt_ABC_MAPPING_AUDIT: produce audit columns (mapplet-like)
        # TODO: unrecognized transformation_type 'Input Transformation' - no dedicated rule; verify against Informatica documentation
        try:
            self.logger.info("Applying FDR_LIB_mplt_ABC_MAPPING_AUDIT mapplet logic", node="FDR_LIB_mplt_ABC_MAPPING_AUDIT")
            # create audit outputs as columns; select them so the df name exists for downstream
            df_mplt_ABC_MAPPING_AUDIT_stoic_babbage = (
                df_EXP_To_Derive_Vals_kind_noether.select(
                    lit(None).cast("bigint").alias("CR_BY_MAPNG_ID"),
                    current_timestamp().alias("DW_CR_TMSP"),
                    lit(None).cast("bigint").alias("UPD_BY_MAPNG_ID"),
                    current_timestamp().alias("DW_UPD_TMSP"),
                    lit(None).cast("bigint").alias("WRK_FLOW_RUN_ID")
                )
            )
        except Exception:
            self.logger.exception("Failed applying FDR_LIB_mplt_ABC_MAPPING_AUDIT logic")
            raise

        # Prepare final target dataframe by adding audit columns and preserving pass-through fields
        try:
            self.logger.info("Preparing final target dataframe", node="Prepare target")
            df_FDR_LIB_WRK_BIRP_NISS_APRM_LND_laughing_faraday = (
                df_EXP_Passtotgt_zen_fermat
                .withColumn("CR_BY_MAPNG_ID", lit(None).cast("bigint"))
                .withColumn("DW_CR_TMSP", current_timestamp())
                .withColumn("UPD_BY_MAPNG_ID", lit(None).cast("bigint"))
                .withColumn("DW_UPD_TMSP", current_timestamp())
                .withColumn("WRK_FLOW_RUN_ID", lit(None).cast("bigint"))
            )
        except Exception:
            self.logger.exception("Failed preparing final target dataframe")
            raise

        # write final WRK_ table as parquet to S3 (overwrite)
        try:
            self.logger.info("Writing FDR_LIB_WRK_BIRP_NISS_APRM_LND to S3 as parquet (overwrite)", table="FDR_LIB_WRK_BIRP_NISS_APRM_LND")
            self.parquet_writer.write(
                dataframe=df_FDR_LIB_WRK_BIRP_NISS_APRM_LND_laughing_faraday,
                path=f"s3://{self.s3_output_bucket}/FDR_LIB_WRK_BIRP_NISS_APRM_LND/",
                mode="overwrite",
            )
        except Exception:
            self.logger.exception("Failed writing FDR_LIB_WRK_BIRP_NISS_APRM_LND to S3")
            raise

        self.glue_job.commit()

    def finalize(self) -> None:
        self.logger.info("Finalizing m_NU0C_NISS_AUTO_100_ATPRM_LND_Load")

    @staticmethod
    def _require_env(env: Mapping[str, Any], key: str) -> str:

        value = env.get(key)

        if not value or str(value).startswith("REPLACE_WITH_"):
            raise ConfigurationValidationError(
                f"config.yaml 'env.{key}' must be set"
            )

        return str(value)
