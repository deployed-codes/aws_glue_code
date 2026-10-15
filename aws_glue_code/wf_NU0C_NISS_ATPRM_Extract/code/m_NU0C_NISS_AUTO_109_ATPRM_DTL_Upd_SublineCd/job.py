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
    split,
    element_at,
    size,
    substring,
    coalesce,
)


# Informatica mapping m_NU0C_NISS_AUTO_109_ATPRM_DTL_Upd_SublineCd.
# Every mapping node runs in order inside run().
class Nu0cNissAuto109AtprmDtlUpdSublineCdJob(BaseJob):

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
            self.logger.info(
                "Attempting staged S3 parquet read for FDR_LIB_WRK_BIRP_NISS_APRM_DETL",
                node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL",
            )
            df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_romantic_hawking = self.parquet_reader.read(
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                options=None,
            )
            # project only the fields the Source node declared (keeps schema narrower and predictable)
            df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_romantic_hawking = df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_romantic_hawking.select(
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
            if df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_romantic_hawking.isEmpty():
                self.logger.warning(
                    "Source FDR_LIB_WRK_BIRP_NISS_APRM_DETL returned no rows from S3 read",
                    node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL",
                )
        except Exception:
            # S3-first branch failed; fall back to Glue Catalog read as allowed for staged/WRK_ tables
            self.logger.warning(
                "Staged S3 read failed for FDR_LIB_WRK_BIRP_NISS_APRM_DETL; falling back to Glue Catalog read",
                node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL",
            )
            try:
                dyf = self.glue_context.create_dynamic_frame.from_catalog(
                    database=self.glue_database,
                    table_name="WRK_BIRP_NISS_APRM_DETL",
                    transformation_ctx="df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_romantic_hawking",
                )
                df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_romantic_hawking = dyf.toDF().select(
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
                if df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_romantic_hawking.isEmpty():
                    self.logger.warning(
                        "Source FDR_LIB_WRK_BIRP_NISS_APRM_DETL returned no rows from Glue Catalog read",
                        node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL",
                    )
            except Exception:
                self.logger.exception("Failed reading FDR_LIB_WRK_BIRP_NISS_APRM_DETL from Glue Catalog")
                raise

        # SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL: Application Source Qualifier - staged SQL override
        try:
            # register the upstream staged dataframe as a temp view for the SQL override to reference
            self.logger.info(
                "Registering staged dataframe as temp view WRK_BIRP_NISS_APRM_DETL and running SQL override",
                node="SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL",
            )
            df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_romantic_hawking.createOrReplaceTempView(
                "WRK_BIRP_NISS_APRM_DETL"
            )

            sql_query = f"""SELECT
    NISS_APRM_DETL_SK,
    ST_NM,
    ST_ABBR,
    ACCTNG_LOB,
    CVG_TYP_CD,
    BI_LMT,
    MI_PPO_IND,
    LMT_TORT,
    TERM_STRT_DT,
    EFF_DT

FROM WRK_BIRP_NISS_APRM_DETL

WHERE ST_ABBR NOT IN ('NY','NJ')
"""

            df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_elated_euclid = self.spark.sql(sql_query)
            if df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_elated_euclid.isEmpty():
                self.logger.warning(
                    "SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL returned no rows",
                    node="SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL",
                )
        except Exception:
            self.logger.exception("Failed executing SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL SQL override")
            raise

        # EXP_BILimit_Split: parse and normalize BI_LMT into numeric parts
        try:
            self.logger.info("Applying EXP_BILimit_Split expressions", node="EXP_BILimit_Split")
            df_EXP_BILimit_Split_zen_bohr = (
                df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_elated_euclid
                .withColumn("v_BI_LMT", regexp_replace(trim(col("BI_LMT")), ",", ""))
                .withColumn("BI_LMT_NO_OF_PARTS", size(split(col("v_BI_LMT"), '/')))
                .withColumn("v_Limit_FIELD1", element_at(split(col("v_BI_LMT"), '/'), 1))
                .withColumn(
                    "v_Limit_FIELD2",
                    coalesce(element_at(split(col("v_BI_LMT"), '/'), 2), lit('0')),
                )
                .withColumn(
                    "v_Limit_FIELD3",
                    coalesce(element_at(split(col("v_BI_LMT"), '/'), 3), lit('0')),
                )
                .withColumn(
                    "BI_LMT_1_Decimal",
                    coalesce(col("v_Limit_FIELD1").cast("decimal(14,0)"), lit(0)).cast("decimal(14,0)"),
                )
                .withColumn(
                    "BI_LMT_2_Decimal",
                    coalesce(col("v_Limit_FIELD2").cast("decimal(14,0)"), lit(0)).cast("decimal(14,0)"),
                )
                .withColumn(
                    "BI_LMT_3_Decimal",
                    coalesce(col("v_Limit_FIELD3").cast("decimal(14,0)"), lit(0)).cast("decimal(14,0)"),
                )
                .withColumn("SRC_BI_LMT", col("v_BI_LMT"))
            )
        except Exception:
            self.logger.exception("Failed applying EXP_BILimit_Split expressions")
            raise

        # EXP_Derive_NISS_SUBLOB_CD_And_PassThru: join SQ and BI split, then derive NISS_SUBLOB_CD
        try:
            self.logger.info(
                "Applying EXP_Derive_NISS_SUBLOB_CD_And_PassThru expressions",
                node="EXP_Derive_NISS_SUBLOB_CD_And_PassThru",
            )

            df_joined = df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_elated_euclid.join(
                df_EXP_BILimit_Split_zen_bohr.select(
                    "NISS_APRM_DETL_SK",
                    "v_BI_LMT",
                    "BI_LMT_NO_OF_PARTS",
                    "BI_LMT_1_Decimal",
                    "BI_LMT_2_Decimal",
                    "BI_LMT_3_Decimal",
                    "SRC_BI_LMT",
                ),
                on="NISS_APRM_DETL_SK",
                how="inner",
            )

            # derive v_MI_PPO_IND
            df_step1 = df_joined.withColumn(
                "v_MI_PPO_IND",
                when(col("MI_PPO_IND") == 1, lit("Y")).when(col("MI_PPO_IND") == 0, lit("N")).otherwise(lit("")),
            )

            # helper for ACCTNG_LOB prefix
            acctng_lob_pref = substring(trim(col("ACCTNG_LOB")), 1, 3)
            cvg_type_in_130 = col("CVG_TYP_CD").isin("13000", "13006", "13007", "13008")
            cvg_type_in_350 = col("CVG_TYP_CD").isin("35000", "35082")

            # build vv_NISS_SUBLOB_CD using the long when/otherwise chain translated from the DECODE
            df_step2 = df_step1.withColumn(
                "vv_NISS_SUBLOB_CD",
                when(col("ST_ABBR") == "NH", lit("8"))
                .when(
                    (col("ST_ABBR") == "PA")
                    & acctng_lob_pref.isin("191", "192")
                    & (col("LMT_TORT") != "1")
                    & cvg_type_in_130,
                    lit("4"),
                )
                .when(
                    (col("ST_ABBR") == "PA")
                    & acctng_lob_pref.isin("191", "192")
                    & (col("LMT_TORT") != "1")
                    & (~cvg_type_in_130),
                    lit("5"),
                )
                .when(
                    (col("ST_ABBR") == "PA")
                    & acctng_lob_pref.isin("191", "192")
                    & (col("LMT_TORT") == "1")
                    & cvg_type_in_130,
                    lit("6"),
                )
                .when(
                    (col("ST_ABBR") == "PA")
                    & acctng_lob_pref.isin("191", "192")
                    & (col("LMT_TORT") == "1")
                    & (~cvg_type_in_130),
                    lit("7"),
                )
                # KY specific rules
                .when(
                    (col("ST_ABBR") == "KY")
                    & acctng_lob_pref.isin("191", "192")
                    & (col("CVG_TYP_CD") == "35028")
                    & (col("BI_LMT_NO_OF_PARTS") == 1)
                    & (col("BI_LMT_1_Decimal") > 0),
                    lit("3"),
                )
                .when(
                    (col("ST_ABBR") == "KY")
                    & acctng_lob_pref.isin("191", "192")
                    & (col("CVG_TYP_CD") == "35028")
                    & ((col("BI_LMT") == "0") | (col("BI_LMT_NO_OF_PARTS") > 1)),
                    lit("4"),
                )
                .when(
                    (col("ST_ABBR") == "KY")
                    & acctng_lob_pref.isin("191", "192")
                    & (col("LMT_TORT") != "#")
                    & (trim(coalesce(col("LMT_TORT"), lit(""))) != "")
                    & col("CVG_TYP_CD").isin("13000", "13006", "13007", "13008"),
                    lit("3"),
                )
                .when(
                    (col("ST_ABBR") == "KY")
                    & acctng_lob_pref.isin("191", "192")
                    & (col("LMT_TORT") != "#")
                    & (trim(coalesce(col("LMT_TORT"), lit(""))) != "")
                    & (~col("CVG_TYP_CD").isin("13000", "13006", "13007", "13008")),
                    lit("4"),
                )
                .when(
                    (col("ST_ABBR") == "KY")
                    & acctng_lob_pref.isin("191", "192")
                    & ((col("LMT_TORT") == "#") | (trim(coalesce(col("LMT_TORT"), lit(""))) == "") | col("LMT_TORT").isNull()),
                    when(col("CVG_TYP_CD").isin("13000", "13006", "13007", "13008"), lit("2")).otherwise(lit("1")),
                )
                # FL changes
                .when(
                    (col("ST_ABBR") == "FL")
                    & acctng_lob_pref.isin("191", "192")
                    & col("CVG_TYP_CD").isin("13000", "13006", "13007", "13008"),
                    lit("2"),
                )
                .when(
                    (col("ST_ABBR") == "FL")
                    & acctng_lob_pref.isin("191", "192"),
                    lit("1"),
                )
                # 9 no-fault states logic
                .when(
                    col("ST_ABBR").isin("CT", "KS", "MD", "MI", "MN", "ND", "OR", "UT", "WA")
                    & acctng_lob_pref.isin("191", "192")
                    & (col("v_MI_PPO_IND") == "Y")
                    & col("CVG_TYP_CD").isin("13000", "13006", "13007", "13008"),
                    lit("6"),
                )
                .when(
                    col("ST_ABBR").isin("CT", "KS", "MD", "MI", "MN", "ND", "OR", "UT", "WA")
                    & acctng_lob_pref.isin("191", "192")
                    & (col("v_MI_PPO_IND") == "Y")
                    & (~col("CVG_TYP_CD").isin("13000", "13006", "13007", "13008")),
                    lit("5"),
                )
                .when(
                    col("ST_ABBR").isin("CT", "KS", "MD", "MI", "MN", "ND", "OR", "UT", "WA")
                    & acctng_lob_pref.isin("191", "192")
                    & (col("v_MI_PPO_IND") != "Y")
                    & col("CVG_TYP_CD").isin("13000", "13006", "13007", "13008"),
                    lit("2"),
                )
                .when(
                    col("ST_ABBR").isin("CT", "KS", "MD", "MI", "MN", "ND", "OR", "UT", "WA")
                    & acctng_lob_pref.isin("191", "192")
                    & (col("v_MI_PPO_IND") != "Y")
                    & (~col("CVG_TYP_CD").isin("13000", "13006", "13007", "13008")),
                    lit("1"),
                )
                # 11 states and 211 prefix => '0'
                .when(
                    col("ST_ABBR").isin("KY", "PA", "CT", "KS", "MD", "MI", "MN", "ND", "OR", "UT", "WA", "FL")
                    & (acctng_lob_pref == "211"),
                    lit("0"),
                )
                # 12 states exceptions
                .when(
                    col("ST_ABBR").isin("NH", "KY", "PA", "CT", "KS", "MD", "MI", "MN", "ND", "OR", "UT", "WA"),
                    lit("?"),
                )
                .when(
                    col("ST_NM").isin("DISTRICT OF COLUMBIA", "DELAWARE", "HAWAII"),
                    lit("?"),
                )
                # other than above states default to '0'
                .when(
                    (~col("ST_ABBR").isin(
                        "NY",
                        "NJ",
                        "NC",
                        "NH",
                        "KY",
                        "PA",
                        "CT",
                        "KS",
                        "MD",
                        "MI",
                        "MN",
                        "ND",
                        "OR",
                        "UT",
                        "WA",
                        "FL",
                    ))
                    & (~col("ST_NM").isin("DISTRICT OF COLUMBIA", "DELAWARE", "HAWAII")),
                    lit("0"),
                )
                .otherwise(lit("?"))
            )

            # v_NISS_SUBLOB_CD applies MI-specific overrides based on date and CVG_TYP_CD/limits
            # If MI rules apply, compute according to documented Decode, otherwise keep vv_NISS_SUBLOB_CD
            df_step3 = df_step2.withColumn(
                "v_NISS_SUBLOB_CD",
                when(
                    (col("ST_ABBR") == "MI")
                    & (
                        (acctng_lob_pref == "191") | (trim(col("ACCTNG_LOB")) == "191")
                    )
                    & (col("EFF_DT").cast("date") >= lit("2020-07-01").cast("date")),
                    when(
                        cvg_type_in_350 & (col("BI_LMT_NO_OF_PARTS") == 1) & (col("BI_LMT_1_Decimal") > 0),
                        lit("2"),
                    )
                    .when(
                        cvg_type_in_350 & ((col("BI_LMT") == "0") | (col("BI_LMT_NO_OF_PARTS") > 1)),
                        lit("1"),
                    )
                    .when(
                        (col("BI_LMT_NO_OF_PARTS") == 1) & (col("BI_LMT_1_Decimal") > 0) & (~cvg_type_in_350),
                        lit("8"),
                    )
                    .when(
                        (~cvg_type_in_350) & ((col("BI_LMT") == "0") | (col("BI_LMT_NO_OF_PARTS") > 1)),
                        lit("7"),
                    )
                    .when(
                        (col("CVG_TYP_CD") == "") | (col("CVG_TYP_CD") == " ") & (col("BI_LMT_NO_OF_PARTS") == 1) & (col("BI_LMT_1_Decimal") > 0),
                        lit("?"),
                    )
                    .otherwise(col("vv_NISS_SUBLOB_CD")),
                ).otherwise(col("vv_NISS_SUBLOB_CD")),
            )

            # final output NISS_SUBLOB_CD: default '?' when empty or null
            df_EXP_Derive_NISS_SUBLOB_CD_And_PassThru_gifted_babbage = (
                df_step3.withColumn(
                    "NISS_SUBLOB_CD",
                    when((col("v_NISS_SUBLOB_CD") == "") | col("v_NISS_SUBLOB_CD").isNull(), lit("?")).otherwise(col("v_NISS_SUBLOB_CD")),
                )
                # keep passthrough key and EFF_DT as requested
                .select("NISS_APRM_DETL_SK", "EFF_DT", "NISS_SUBLOB_CD")
            )
        except Exception:
            self.logger.exception("Failed applying EXP_Derive_NISS_SUBLOB_CD_And_PassThru expressions")
            raise

        # UPD_NISS_SUBLOB_CD: Update Strategy - mark rows, write rejects (if any), and prepare combined full target dataframe
        try:
            self.logger.info("Applying UPD_NISS_SUBLOB_CD update-strategy marker", node="UPD_NISS_SUBLOB_CD")
            # all incoming rows are DD_UPDATE per mapping
            df_marked = df_EXP_Derive_NISS_SUBLOB_CD_And_PassThru_gifted_babbage.withColumn("dd_op", lit("UPDATE"))

            # write rejects (none expected for DD_UPDATE, but handle generically)
            df_rejects = df_marked.filter(col("dd_op") == "REJECT")
            try:
                if not df_rejects.isEmpty():
                    self.logger.info(
                        "Writing rejected rows from UPD_NISS_SUBLOB_CD to reject prefix",
                        node="UPD_NISS_SUBLOB_CD",
                    )
                    self.parquet_writer.write(
                        dataframe=df_rejects.withColumn("error_reason", lit("DD_REJECT")),
                        path=f"s3://{self.s3_output_bucket}/reject/WRK_BIRP_NISS_APRM_DETL/",
                        mode="overwrite",
                    )
            except Exception:
                self.logger.exception("Failed writing reject rows for UPD_NISS_SUBLOB_CD")
                raise

            # load current target full table from S3; if not present treat as empty
            try:
                self.logger.info(
                    "Reading existing target WRK_BIRP_NISS_APRM_DETL from S3 for load-modify-store-back",
                    node="UPD_NISS_SUBLOB_CD",
                )
                df_existing_target = self.parquet_reader.read(
                    path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                    options=None,
                )
            except Exception:
                self.logger.warning(
                    "Existing target WRK_BIRP_NISS_APRM_DETL not found on S3 - treating existing as empty",
                    node="UPD_NISS_SUBLOB_CD",
                )
                # create an empty DataFrame with no rows but no schema mismatch by using marked's schema as basis
                df_existing_target = df_marked.limit(0)

            # identify keys being changed
            changed_keys_df = df_marked.select("NISS_APRM_DETL_SK").distinct()

            # survivors from existing target (drop rows that will be updated/deleted)
            existing_survivors = df_existing_target.join(changed_keys_df, on="NISS_APRM_DETL_SK", how="left_anti")

            # rows in existing that must be updated: join existing rows to marked updates
            # Build updated rows by taking existing row columns and replacing NISS_SUBLOB_CD and EFF_DT when present in df_marked
            # If marked has only a subset of columns, unionByName with allowMissingColumns will be used below to combine
            marked_small = df_marked.select("NISS_APRM_DETL_SK", "NISS_SUBLOB_CD", "EFF_DT", "dd_op")

            updated_matches = (
                df_existing_target.alias("e")
                .join(marked_small.alias("m"), on="NISS_APRM_DETL_SK", how="inner")
                .select(
                    *[
                        # replace NISS_SUBLOB_CD and EFF_DT from marker when present, otherwise keep existing
                        coalesce(col(f"m.{c}"), col(f"e.{c}")).alias(c)
                        if c in ("NISS_SUBLOB_CD", "EFF_DT")
                        else col(f"e.{c}")
                        for c in df_existing_target.columns
                    ]
                )
            )

            # new inserts (marked rows with no match in existing)
            new_inserts = df_marked.join(df_existing_target, on="NISS_APRM_DETL_SK", how="left_anti")

            # combine: survivors + updated_matches + new_inserts
            # Use unionByName with allowMissingColumns=True to tolerate marked rows having fewer columns than the full target
            combined_df = existing_survivors
            if len(updated_matches.columns) > 0:
                combined_df = combined_df.unionByName(updated_matches, allowMissingColumns=True)
            if len(new_inserts.columns) > 0:
                combined_df = combined_df.unionByName(new_inserts.drop("dd_op"), allowMissingColumns=True)

            # note: full read-anti-join-overwrite is expensive on large targets but required to fully apply updates
            df_UPD_NISS_SUBLOB_CD_trusting_kepler = combined_df

        except Exception:
            self.logger.exception("Failed applying UPD_NISS_SUBLOB_CD update strategy")
            raise

        # assign the Output node's incoming dataframe variable name to the final combined dataframe
        df_WRK_BIRP_NISS_APRM_DETL_bold_noether = df_UPD_NISS_SUBLOB_CD_trusting_kepler

        # write intermediate WRK_ table as parquet to S3 (overwrite)
        try:
            self.logger.info("Writing WRK_BIRP_NISS_APRM_DETL to S3 as parquet (overwrite)", table="WRK_BIRP_NISS_APRM_DETL")
            self.parquet_writer.write(
                dataframe=df_WRK_BIRP_NISS_APRM_DETL_bold_noether,
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                mode="overwrite",
            )
        except Exception:
            self.logger.exception("Failed writing WRK_BIRP_NISS_APRM_DETL to S3")
            raise

        self.glue_job.commit()

    def finalize(self) -> None:
        self.logger.info("Finalizing m_NU0C_NISS_AUTO_109_ATPRM_DTL_Upd_SublineCd")

    @staticmethod
    def _require_env(env: Mapping[str, Any], key: str) -> str:

        value = env.get(key)

        if not value or str(value).startswith("REPLACE_WITH_"):
            raise ConfigurationValidationError(
                f"config.yaml 'env.{key}' must be set"
            )

        return str(value)
