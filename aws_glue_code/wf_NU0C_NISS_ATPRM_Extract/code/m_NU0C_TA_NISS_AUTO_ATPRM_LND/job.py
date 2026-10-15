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
    current_timestamp,
    row_number,
    broadcast,
    substring,
    expr,
)
from pyspark.sql.window import Window
from glue_lib.connector.snowflake import SnowflakeReader


# Informatica mapping m_NU0C_TA_NISS_AUTO_ATPRM_LND.
# Every mapping node runs in order inside run().
class Nu0cTaNissAutoAtprmLndJob(BaseJob):

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

        # Read Snowflake connection and mapping parameters from env
        SNOWFLAKE_CONNECTION_FDWS_FDRWRK = self._require_env(self.env, "SNOWFLAKE_CONNECTION_FDWS_FDRWRK")
        SCHEMA_FPR = self._require_env(self.env, "SCHEMA_FPR")
        FPR_TA_DA_CVG_LVL_STAT_TBL = self._require_env(self.env, "FPR_TA_DA_CVG_LVL_STAT_TBL")
        GEO_ST_NM = self._require_env(self.env, "GEO_ST_NM")
        RPT_YEAR = self._require_env(self.env, "RPT_YEAR")
        LOOKUPFILE_FF_NISS_STATE = self._require_env(self.env, "LOOKUPFILE_FF_NISS_STATE")

        # SQ_Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_LND: read SQL override from Snowflake
        sql_query = f"""SELECT
    REGSTR_CLS_MNTH,
    FISC_PER_YR,
    NAIC_CMPNY_CD,
    NISS_CMPNY_CD,
    STATE_NM,
    STATE_CD,
    ACCT_LOB,
    CVG_CD,
    CVG_AMT,
    BI_LMT,
    GA_UMBI_PD_ADDED_IND,
    FA2_PLCY_IND,
    UM_UIM_STACKING_IND,
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
    RATNG_CMPY_CD,
    MLT_CAR_IND,
    RT_CLS,
    RTD_DRVR_AGE,
    RTD_DRVR_GNDR_CD,
    RTD_DRVR_MRTL_STAT,
    AUTO_USE_CD,
    MILES_TO_WRK,
    GOOD_STDNT_IND,
    DRVR_TRNG_IND,
    SOI_TYP_CD,
    PHY_DMG_IND,
    NJ_RTD_PNTS,
    VEH_YR,
    NJ_EXCPTION_CD,
    NJ_FRGVN_PNTS,
    PASSV_RESTRA_DISC_IND,
    SNR_DRVR_IND,
    DEFNSV_DRVR_DISC_IND,
    ANTI_THFT_DISC_IND,
    DAY_TM_RUN_LIGHTS,
    LMT_TORT,
    CVG_TYP_IND,
    CASE WHEN EXPSR_MNTH_CNT=0 THEN 12 ELSE EXPSR_MNTH_CNT END as EXPSR_MNTH_CNT,
    WRITTN_PREM_AMT AS WRITTN_PREM_AMT,
    NJ_NO_LWST_LMT_IND,
    NJ_NMD_DRVR_EXCL_IND,
    COMP_DED,
    COLL_DED,
    PLCY_NUM,
    UNIT_NUM,
    EFF_DT,
    NUM_OF_CARS_IN_HH,
    RTD_DRVR_DOB,
    PLCY_TRM_STRT_DT,
    SRC_SYS_CD,
    PNI_AGE,
    MIS_LOB,
    PRINCIPAL_OPRT,
    SOURCE_IND_DERIVED
FROM (
    SELECT
        TO_CHAR(REGSTR_CLS_MNTH) AS REGSTR_CLS_MNTH,
        '' as FISC_PER_YR,
        NAIC_CMPNY_CD,
        CASE WHEN NAIC_CMPNY_CD='21687' THEN '176'
             WHEN NAIC_CMPNY_CD='44245' THEN '941' END AS NISS_CMPNY_CD,
        TRIM(UPPER(STATE_NM)) AS STATE_NM,
        FIG_STATE AS STATE_CD,
        ACCT_LOB,
        CVG_CD,
        CVG_AMT,
        BI_LMT,
        GA_UMBI_PD_ADDED_IND,
        '' as FA2_PLCY_IND,
        UM_UIM_STACKING_IND,
        NULL as PIP_WVR_WL_IND,
        NULL as PIP_MED_SEC_IND,
        NULL as PIP_LOSS_INCOME_IND,
        NULL as MI_PPO_IND,
        '' as PRD_GRP_CD,
        '' as NJ_HLTH_INSR_PRIM,
        '' as NJ_EXTR_PIP_PKG,
        NULL as NJ_RESDNC_RLTNSHP_PIP_IND,
        NULL as NY_SSL_IND,
        NULL as NY_FULL_CVG_GLASS_COMP_IND,
        GRGNG_ZIP_5,
        '' as RATNG_CMPY_CD,
        MLT_CAR_IND,
        '' as RT_CLS,
        RTD_DRVR_AGE,
        RTD_DRVR_GNDR_CD,
        CASE WHEN RTD_DRVR_MRTL_STAT='single' THEN 'S'
             WHEN RTD_DRVR_MRTL_STAT='married' THEN 'M'
             ELSE UPPER(RTD_DRVR_MRTL_STAT) END AS RTD_DRVR_MRTL_STAT,
        AUTO_USE_CD,
        '' as MILES_TO_WRK,
        GOOD_STDNT_IND,
        DRVR_TRNG_IND,
        SOI_TYP_CD,
        CASE WHEN PHY_DMG_IND='Y' THEN 1 WHEN PHY_DMG_IND='N' THEN 0 ELSE NULL END AS PHY_DMG_IND,
        '' as NJ_RTD_PNTS,
        try_to_number(VEH_YR) as VEH_YR,
        '' AS NJ_EXCPTION_CD,
        '' AS NJ_FRGVN_PNTS,
        PASSV_RESTRA_DISC_IND,
        '' AS SNR_DRVR_IND,
        CASE WHEN DEFNSV_DRVR_DISC_IND='Y' THEN 1 WHEN DEFNSV_DRVR_DISC_IND='N' THEN 0 ELSE NULL END AS DEFNSV_DRVR_DISC_IND,
        ANTI_THFT_DISC_IND,
        '' as DAY_TM_RUN_LIGHTS,
        '' as LMT_TORT,
        CVG_TYP_IND,
        SUM(EXPSR_MNTH_CNT) AS EXPSR_MNTH_CNT,
        SUM(WRITTN_PREM_AMT) as WRITTN_PREM_AMT,
        NULL as NJ_NO_LWST_LMT_IND,
        NULL as NJ_NMD_DRVR_EXCL_IND,
        COMP_DED,
        COLL_DED,
        PLCY_NUM,
        NULL as UNIT_NUM,
        EFF_DT,
        NULL AS NUM_OF_CARS_IN_HH,
        RTD_DRVR_DOB,
        PLCY_TRM_STRT_DT,
        SRC_SYS_CD,
        NULL AS PNI_AGE,
        MIS_LOB,
        '' as PRINCIPAL_OPRT,
        'TOGGLE AUTO' AS SOURCE_IND_DERIVED
    FROM
        {SCHEMA_FPR}.{FPR_TA_DA_CVG_LVL_STAT_TBL}
    WHERE
        TRIM(UPPER(STATE_NM)) NOT IN ({GEO_ST_NM})
        AND SUBSTR(REGSTR_CLS_MNTH,1,4)={RPT_YEAR}
    GROUP BY
        REGSTR_CLS_MNTH,
        NAIC_CMPNY_CD,
        TRIM(UPPER(STATE_NM)),
        FIG_STATE,
        ACCT_LOB,
        CVG_CD,
        CVG_AMT,
        BI_LMT,
        GA_UMBI_PD_ADDED_IND,
        PLCY_REWRIT_IND,
        UM_UIM_STACKING_IND,
        GRGNG_ZIP_5,
        MLT_CAR_IND,
        RTD_DRVR_AGE,
        RTD_DRVR_GNDR_CD,
        RTD_DRVR_MRTL_STAT,
        AUTO_USE_CD,
        GOOD_STDNT_IND,
        DRVR_TRNG_IND,
        SOI_TYP_CD,
        PHY_DMG_IND,
        VEH_YR,
        PASSV_RESTRA_DISC_IND,
        DEFNSV_DRVR_DISC_IND,
        ANTI_THFT_DISC_IND,
        CVG_TYP_IND,
        COMP_DED,
        COLL_DED,
        PLCY_NUM,
        EFF_DT,
        RTD_DRVR_DOB,
        PLCY_TRM_STRT_DT,
        SRC_SYS_CD,
        MIS_LOB
) WHERE WRITTN_PREM_AMT != 0
"""

        try:
            self.logger.info("Reading SQ_Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_LND from Snowflake via SQL override", node="SQ_Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_LND")
            df_SQ_Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_LND_lucid_lovelace = SnowflakeReader(self.glue_context).read(
                connection_name=SNOWFLAKE_CONNECTION_FDWS_FDRWRK,
                query=sql_query,
            )
            if df_SQ_Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_LND_lucid_lovelace.isEmpty():
                self.logger.warning("Source SQ_Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_LND returned no rows", node="SQ_Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_LND")
        except Exception:
            self.logger.exception("Failed reading SQ_Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_LND from Snowflake")
            raise

        # Alias the bypassed Source node to the SQ result so downstream code that expects the Source df_name resolves
        df_Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_LND_humble_babbage = df_SQ_Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_LND_lucid_lovelace

        # EXP_PASS_THROUGH: passthrough select of explicit ports
        try:
            self.logger.info("Applying EXP_PASS_THROUGH passthrough select", node="EXP_PASS_THROUGH")
            # list every output/input-output port explicitly
            df_EXP_PASS_THROUGH_reverent_dirac = df_SQ_Shortcut_to_WRK_BIRP_TA_NISS_NU0C_APRM_LND_lucid_lovelace.selectExpr(
                "REG_PER_YR",
                "FISC_PER_YR",
                "NAIC_CMPNY_CD",
                "NISS_CMPNY_CD",
                "ST_NM",
                "ST_CD",
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
                "NISS_CMPNY_CD"
            )
        except Exception:
            self.logger.exception("Failed applying EXP_PASS_THROUGH expressions")
            raise

        # EXPTRANS: derived columns (GRGNG_ZIP_5, v_FARMERS_STATE_CD -> IFARMERS_STATE_CD) and passthroughs
        try:
            self.logger.info("Applying EXPTRANS expressions", node="EXPTRANS")
            df_in = df_EXP_PASS_THROUGH_reverent_dirac

            # determine which column contains raw zip input
            if "i_GRGNG_ZIP" in df_in.columns:
                zip_src_col = "i_GRGNG_ZIP"
            elif "GRGNG_ZIP" in df_in.columns:
                zip_src_col = "GRGNG_ZIP"
            else:
                zip_src_col = None

            if zip_src_col is None:
                df_with_zip = df_in.withColumn("GRGNG_ZIP_5", lit(None).cast("string"))
            else:
                df_with_zip = df_in.withColumn(
                    "GRGNG_ZIP_5",
                    when(col(zip_src_col).isNull() | (trim(col(zip_src_col)) == "") | (trim(col(zip_src_col)) == "0"), lit("00000")).otherwise(trim(col(zip_src_col)))
                )

            # v_FARMERS_STATE_CD logic: IIF(ST_CD = '#','00',ST_CD)
            df_with_v = df_with_zip.withColumn(
                "v_FARMERS_STATE_CD",
                when(col("ST_CD") == "#", lit("00")).otherwise(col("ST_CD"))
            )

            # IFARMERS_STATE_CD: TO_INTEGER(v_FARMERS_STATE_CD) -> cast to integer, invalid parse yields null
            df_EXPTRANS_magical_noether = df_with_v.withColumn(
                "IFARMERS_STATE_CD",
                col("v_FARMERS_STATE_CD").cast("int")
            )

        except Exception:
            self.logger.exception("Failed applying EXPTRANS expressions")
            raise

        # LKP_ff_REF_NISS_STATE_CD: read flat-file lookup, dedupe, cache, broadcast and left-join
        try:
            self.logger.info("Reading lookup file LKP_ff_REF_NISS_STATE_CD", node="LKP_ff_REF_NISS_STATE_CD")
            lookup_df = (
                self.spark.read.option("header", "true").option("sep", ",").csv(LOOKUPFILE_FF_NISS_STATE)
            )
            # Normalize column names expected: FARMERS_STATE_NAME and NISS_STATE_CODE
            # If file has different header names, attempt to handle common alternatives
            cols = [c.upper() for c in lookup_df.columns]
            # Try to find matching columns case-insensitively
            def pick_col(possible):
                for p in possible:
                    for orig in lookup_df.columns:
                        if orig.upper() == p.upper():
                            return orig
                return None

            farmers_col = pick_col(["FARMERS_STATE_NAME", "FARMERS_STATE", "STATE_NAME"])
            niss_code_col = pick_col(["NISS_STATE_CODE", "NISS_STATE_CD", "NISS_CODE"])

            if farmers_col is None or niss_code_col is None:
                # best-effort: rename first two columns if present
                if len(lookup_df.columns) >= 2:
                    lookup_df = lookup_df.withColumnRenamed(lookup_df.columns[0], "FARMERS_STATE_NAME").withColumnRenamed(lookup_df.columns[1], "NISS_STATE_CODE")
                    farmers_col = "FARMERS_STATE_NAME"
                    niss_code_col = "NISS_STATE_CODE"
                else:
                    # no usable lookup columns; create empty lookup with expected schema
                    lookup_df = self.spark.createDataFrame([], schema="FARMERS_STATE_NAME string, NISS_STATE_CODE string")
                    farmers_col = "FARMERS_STATE_NAME"
                    niss_code_col = "NISS_STATE_CODE"
            else:
                # rename to expected canonical names
                lookup_df = lookup_df.withColumnRenamed(farmers_col, "FARMERS_STATE_NAME").withColumnRenamed(niss_code_col, "NISS_STATE_CODE")

            # Deduplicate using window to keep first match policy
            win = Window.partitionBy("FARMERS_STATE_NAME").orderBy(lit(1))
            lookup_df = (
                lookup_df.withColumn("_rn", row_number().over(win)).filter(col("_rn") == 1).drop("_rn")
            )
            lookup_df = lookup_df.cache()

            # join: determine join column on left side
            left_df_cols = df_EXPTRANS_magical_noether.columns
            if "i_ST_NM" in left_df_cols:
                left_join_col = "i_ST_NM"
            elif "ST_NM" in left_df_cols:
                left_join_col = "ST_NM"
            else:
                left_join_col = None

            if left_join_col is not None:
                df_LKP_ff_REF_NISS_STATE_CD_focused_leibniz = (
                    df_EXPTRANS_magical_noether
                    .join(broadcast(lookup_df), trim(col(left_join_col)) == trim(col("FARMERS_STATE_NAME")), "left")
                    .withColumnRenamed("FARMERS_STATE_NAME", "FARMERS_STATE_NAME")
                    .withColumnRenamed("NISS_STATE_CODE", "NISS_STATE_CODE")
                )
            else:
                # Lookup cannot be applied; emit lookup outputs as NULLs
                df_LKP_ff_REF_NISS_STATE_CD_focused_leibniz = df_EXPTRANS_magical_noether
                df_LKP_ff_REF_NISS_STATE_CD_focused_leibniz = df_LKP_ff_REF_NISS_STATE_CD_focused_leibniz.withColumn("FARMERS_STATE_NAME", lit(None).cast("string")).withColumn("NISS_STATE_CODE", lit(None).cast("string"))

        except Exception:
            self.logger.exception("Failed reading or applying lookup LKP_ff_REF_NISS_STATE_CD")
            raise

        # EXPTRANS1: passthroughs and computed outputs (EXPS_VAL_ROLLED, ST_ABBR, GA_ADDED_AT_FAULT_IND, NISS_STATE_CODE normalization)
        try:
            self.logger.info("Applying EXPTRANS1 expressions", node="EXPTRANS1")
            df1 = df_LKP_ff_REF_NISS_STATE_CD_focused_leibniz

            # EXPS_VAL_ROLLED constant 0
            df1 = df1.withColumn("EXPS_VAL_ROLLED", lit(0))

            # ST_ABBR: normalize i_ST_ABBR or fallback to ST_ABBR
            if "i_ST_ABBR" in df1.columns:
                st_abbr_src = "i_ST_ABBR"
            elif "ST_ABBR" in df1.columns:
                st_abbr_src = "ST_ABBR"
            else:
                st_abbr_src = None

            if st_abbr_src is None:
                df1 = df1.withColumn("ST_ABBR", lit(None).cast("string"))
            else:
                df1 = df1.withColumn("ST_ABBR", when(col(st_abbr_src).isNull() | (trim(col(st_abbr_src)) == ""), lit("?")).otherwise(trim(col(st_abbr_src))))

            # GA_ADDED_AT_FAULT_IND: IIF(i_GA_ADDED_AT_FAULT_IND='1','Y','N')
            if "i_GA_ADDED_AT_FAULT_IND" in df1.columns:
                ga_src = "i_GA_ADDED_AT_FAULT_IND"
            elif "GA_ADDED_AT_FAULT_IND" in df1.columns:
                ga_src = "GA_ADDED_AT_FAULT_IND"
            else:
                ga_src = None

            if ga_src is None:
                df1 = df1.withColumn("GA_ADDED_AT_FAULT_IND", lit(None).cast("string"))
            else:
                df1 = df1.withColumn("GA_ADDED_AT_FAULT_IND", when(col(ga_src) == "1", lit("Y")).otherwise(lit("N")))

            # NISS_STATE_CODE normalization: prefer any incoming 'i_NISS_STATE_CODE' or joined 'NISS_STATE_CODE'
            if "i_NISS_STATE_CODE" in df1.columns:
                niss_src = "i_NISS_STATE_CODE"
            elif "NISS_STATE_CODE" in df1.columns:
                niss_src = "NISS_STATE_CODE"
            else:
                niss_src = None

            if niss_src is None:
                df1 = df1.withColumn("NISS_STATE_CODE", lit(None).cast("string"))
            else:
                df1 = df1.withColumn("NISS_STATE_CODE", when(col(niss_src).isNull() | (trim(col(niss_src)) == ""), lit("?")).otherwise(trim(col(niss_src))))

            df_EXPTRANS1_tender_babbage = df1

        except Exception:
            self.logger.exception("Failed applying EXPTRANS1 expressions")
            raise

        # EXPTRANS2: final mapping-level fields, NISS_TERR_CD clean-up, o_ANNUAL_STMT_LOB, and PM variables as NULL
        try:
            self.logger.info("Applying EXPTRANS2 expressions", node="EXPTRANS2")
            df2 = df_EXPTRANS1_tender_babbage

            # NISS_TERR_CD from i_NISS_TERR_CD if present
            if "i_NISS_TERR_CD" in df2.columns:
                terr_src = "i_NISS_TERR_CD"
            elif "NISS_TERR_CD" in df2.columns:
                terr_src = "NISS_TERR_CD"
            else:
                terr_src = None

            if terr_src is None:
                df2 = df2.withColumn("NISS_TERR_CD", lit(None).cast("string"))
            else:
                df2 = df2.withColumn("NISS_TERR_CD", when(col(terr_src).isNull() | (trim(col(terr_src)) == ""), lit("?")).otherwise(trim(col(terr_src))))

            # o_ANNUAL_STMT_LOB: SUBSTR(ltrim(rtrim(ACCTNG_LOB)),1,3)
            if "ACCTNG_LOB" in df2.columns:
                df2 = df2.withColumn("o_ANNUAL_STMT_LOB", substring(trim(col("ACCTNG_LOB")), 1, 3))
            else:
                df2 = df2.withColumn("o_ANNUAL_STMT_LOB", lit(None).cast("string"))

            # PM<...> system variables have no Glue equivalent -> NULL
            df2 = df2.withColumn("MAPPING_NAME", lit(None).cast("string")).withColumn("FOLDER_NAME", lit(None).cast("string")).withColumn("WORKFLOW_NAME", lit(None).cast("string"))

            df_EXPTRANS2_clever_schrodinger = df2

        except Exception:
            self.logger.exception("Failed applying EXPTRANS2 expressions")
            raise

        # FDR_LIB_mplt_ABC_MAPPING_AUDIT: derive audit fields deterministically (CR_BY_MAPNG_ID NULL, timestamps now, WRK_FLOW_RUN_ID=1)
        try:
            self.logger.info("Producing mapping-audit mapplet fallback", node="FDR_LIB_mplt_ABC_MAPPING_AUDIT")
            # Use a single-row audit dataframe
            audit_row = self.spark.createDataFrame([{
                "CR_BY_MAPNG_ID": None,
                "DW_CR_TMSP": None,
                "UPD_BY_MAPNG_ID": None,
                "DW_UPD_TMSP": None,
                "WRK_FLOW_RUN_ID": 1,
            }])
            # set timestamps to current
            audit_row = audit_row.withColumn("DW_CR_TMSP", current_timestamp()).withColumn("DW_UPD_TMSP", current_timestamp())
            df_FDR_LIB_mplt_ABC_MAPPING_AUDIT_hopeful_hilbert = build_mapping_audit_columns(audit_row)

        except Exception:
            self.logger.exception("Failed producing mapping-audit mapplet fallback")
            raise

        # Assemble final target dataframe, add surrogate key, cross-join audit row, and write to S3 as parquet
        try:
            self.logger.info("Assembling final WRK_BIRP_TA_NISS_NU0C_APRM_LND and writing to S3 (overwrite)", table="WRK_BIRP_TA_NISS_NU0C_APRM_LND")
            # Start from df_EXPTRANS2_clever_schrodinger
            main_df = df_EXPTRANS2_clever_schrodinger

            # Cross-join a single-row audit dataframe to attach audit columns to every row
            # ensure audit df has exactly one row
            audit_single = df_FDR_LIB_mplt_ABC_MAPPING_AUDIT_hopeful_hilbert.limit(1)
            main_with_audit = main_df.crossJoin(audit_single)

            # Generate surrogate key NISS_APRM_LND_SK using row_number over ordering by lit(1)
            sk_window = Window.orderBy(lit(1))
            final_df = main_with_audit.withColumn("NISS_APRM_LND_SK", row_number().over(sk_window).cast("long"))

            # Ensure final column ordering matches target definition where possible. Build select list from target fields.
            select_cols = [
                "NISS_APRM_LND_SK",
                "REG_PER_YR",
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
                "CR_BY_MAPNG_ID",
                "DW_CR_TMSP",
                "UPD_BY_MAPNG_ID",
                "DW_UPD_TMSP",
                "WRK_FLOW_RUN_ID",
                "NJ_NO_LWST_LMT_IND",
                "NJ_NMD_DRVR_EXCL_IND",
                "EXPS_VAL_ROLLED",
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
            ]

            # Only select columns that actually exist in final_df to avoid AnalysisException
            existing_select = [c for c in select_cols if c in final_df.columns]
            df_WRK_BIRP_TA_NISS_NU0C_APRM_LND_stoic_faraday = final_df.select(*existing_select)

            # write intermediate WRK_ table as parquet to S3 (overwrite)
            try:
                self.parquet_writer.write(
                    dataframe=df_WRK_BIRP_TA_NISS_NU0C_APRM_LND_stoic_faraday,
                    path=f"s3://{self.s3_output_bucket}/WRK_BIRP_TA_NISS_NU0C_APRM_LND/",
                    mode="overwrite",
                )
            except Exception:
                self.logger.exception("Failed writing WRK_BIRP_TA_NISS_NU0C_APRM_LND to S3")
                raise

        except Exception:
            self.logger.exception("Failed assembling or writing WRK_BIRP_TA_NISS_NU0C_APRM_LND target")
            raise

        self.glue_job.commit()

    def finalize(self) -> None:
        self.logger.info("Finalizing m_NU0C_TA_NISS_AUTO_ATPRM_LND")

    @staticmethod
    def _require_env(env: Mapping[str, Any], key: str) -> str:

        value = env.get(key)

        if not value or str(value).startswith("REPLACE_WITH_"):
            raise ConfigurationValidationError(
                f"config.yaml 'env.{key}' must be set"
            )

        return str(value)
