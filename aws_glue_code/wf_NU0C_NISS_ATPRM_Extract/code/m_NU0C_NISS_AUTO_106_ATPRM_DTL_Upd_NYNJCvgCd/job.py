import sys
from typing import Any, Mapping

from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import DataFrame

from glue_lib.connector import ParquetReader, ParquetWriter
from glue_lib.exceptions import ConfigurationValidationError
from glue_lib.job.base import BaseJob
from pyspark.sql.functions import col, lit, trim, regexp_replace, split, size, when


# Informatica mapping m_NU0C_NISS_AUTO_106_ATPRM_DTL_Upd_NYNJCvgCd.
# Every mapping node runs in order inside run().
class Nu0cNissAuto106AtprmDtlUpdNYNJCvgCdJob(BaseJob):

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

        # Source: WRK_BIRP_NISS_APRM_DETL - try S3-first (WRK_ staging table), fall back to Glue Catalog
        try:
            self.logger.info("Attempting S3-first read for WRK_BIRP_NISS_APRM_DETL", node="WRK_BIRP_NISS_APRM_DETL")
            df_WRK_BIRP_NISS_APRM_DETL_vibrant_newton = self.parquet_reader.read(
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                options=None,
            )
            self.logger.info("Read WRK_BIRP_NISS_APRM_DETL from S3 (staged)", node="WRK_BIRP_NISS_APRM_DETL")
            df_WRK_BIRP_NISS_APRM_DETL_vibrant_newton = df_WRK_BIRP_NISS_APRM_DETL_vibrant_newton.select(
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
            if df_WRK_BIRP_NISS_APRM_DETL_vibrant_newton.isEmpty():
                self.logger.warning("Source WRK_BIRP_NISS_APRM_DETL returned no rows", node="WRK_BIRP_NISS_APRM_DETL")
        except Exception:
            # fallback to Glue Catalog read for WRK_BIRP_NISS_APRM_DETL
            try:
                self.logger.warning("S3 read failed for WRK_BIRP_NISS_APRM_DETL, falling back to Glue catalog read", node="WRK_BIRP_NISS_APRM_DETL")
                dyf = self.glue_context.create_dynamic_frame.from_catalog(
                    database=self.glue_database,
                    table_name="WRK_BIRP_NISS_APRM_DETL",
                    transformation_ctx="df_WRK_BIRP_NISS_APRM_DETL_vibrant_newton",
                )
                df_WRK_BIRP_NISS_APRM_DETL_vibrant_newton = dyf.toDF().select(
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
                if df_WRK_BIRP_NISS_APRM_DETL_vibrant_newton.isEmpty():
                    self.logger.warning("Catalog source WRK_BIRP_NISS_APRM_DETL returned no rows", node="WRK_BIRP_NISS_APRM_DETL")
            except Exception:
                self.logger.exception("Failed reading WRK_BIRP_NISS_APRM_DETL from Glue catalog")
                raise

        # SQ_WRK_BIRP_NISS_APRM_DETL: staged SQL override - register the upstream staged dataframe as a temp view and run the override via spark.sql()
        try:
            df_WRK_BIRP_NISS_APRM_DETL_vibrant_newton.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")

            sql_query = f"""SELECT
NISS_APRM_DETL_SK,
ST_NM,
ST_ABBR,
ACCTNG_LOB,
LTRIM(RTRIM(CVG_TYP_CD)) CVG_TYP_CD,
LTRIM(RTRIM(CVG_AMT)) CVG_AMT,
LTRIM(RTRIM(BI_LMT)) BI_LMT,
PIP_LOSS_INCOME_IND,
LTRIM(RTRIM(COALESCE(PRD_GRP_CD,''))) PRD_GRP_CD,
LTRIM(RTRIM(COALESCE(NJ_HLTH_INSR_PRIM,''))) NJ_HLTH_INSR_PRIM,
LTRIM(RTRIM(COALESCE(NJ_EXTR_PIP_PKG,''))) NJ_EXTR_PIP_PKG,
NJ_RESDNC_RLTNSHP_PIP_IND,
NY_FULL_CVG_GLASS_COMP_IND,
NY_SSL_IND,
LTRIM(RTRIM(COMP_DED)) COMP_DED,
LTRIM(RTRIM(COLL_DED)) COLL_DED,
REC_EXCPN_IND,
REC_EXCPN_RSN_DESC

FROM WRK_BIRP_NISS_APRM_DETL

WHERE ST_ABBR IN ('NY','NJ')"""

            self.logger.info("Executing staged SQL override for SQ_WRK_BIRP_NISS_APRM_DETL", node="SQ_WRK_BIRP_NISS_APRM_DETL")
            df_SQ_WRK_BIRP_NISS_APRM_DETL_mighty_kant = self.spark.sql(sql_query)
            if df_SQ_WRK_BIRP_NISS_APRM_DETL_mighty_kant.isEmpty():
                self.logger.warning("SQ_WRK_BIRP_NISS_APRM_DETL returned no rows", node="SQ_WRK_BIRP_NISS_APRM_DETL")
        except Exception:
            self.logger.exception("Failed executing SQ_WRK_BIRP_NISS_APRM_DETL SQL override")
            raise

        # EXP_BILimit_Split: split BI_LMT into up to 3 parts, convert to integer decimals, preserve REC_EXCPN fields
        try:
            self.logger.info("Applying EXP_BILimit_Split expressions", node="EXP_BILimit_Split")
            df_EXP_BILimit_Split_charming_babbage = (
                df_SQ_WRK_BIRP_NISS_APRM_DETL_mighty_kant
                .withColumn("v_BI_LMT", regexp_replace(trim(col("BI_LMT")), ",", ""))
                .withColumn("BI_LMT_NO_OF_PARTS", size(split(col("v_BI_LMT"), "/")))
                .withColumn("BI_LMT_1_String", when(col("BI_LMT_NO_OF_PARTS") >= 1, split(col("v_BI_LMT"), "/").getItem(0)).otherwise(lit('0')))
                .withColumn("BI_LMT_2_String", when(col("BI_LMT_NO_OF_PARTS") >= 2, split(col("v_BI_LMT"), "/").getItem(1)).otherwise(lit('0')))
                .withColumn("BI_LMT_3_String", when(col("BI_LMT_NO_OF_PARTS") >= 3, split(col("v_BI_LMT"), "/").getItem(2)).otherwise(lit('0')))
                .withColumn("BI_LMT_1_Decimal", trim(col("BI_LMT_1_String")).cast("int"))
                .withColumn("BI_LMT_2_Decimal", trim(col("BI_LMT_2_String")).cast("int"))
                .withColumn("BI_LMT_3_Decimal", trim(col("BI_LMT_3_String")).cast("int"))
                .withColumn("SRC_BI_LMT", col("v_BI_LMT"))
                .withColumn("REC_EXCPN_IND", col("REC_EXCPN_IND"))
                .withColumn("REC_EXCPN_RSN_DESC", col("REC_EXCPN_RSN_DESC"))
            )
        except Exception:
            self.logger.exception("Failed applying EXP_BILimit_Split expressions")
            raise

        # EXP_CvgAmount_Split: sanitize CVG_AMT and split into up to 3 numeric parts
        try:
            self.logger.info("Applying EXP_CvgAmount_Split expressions", node="EXP_CvgAmount_Split")
            df_EXP_CvgAmount_Split_trusting_aristotle = split_and_clean_amount(df_SQ_WRK_BIRP_NISS_APRM_DETL_mighty_kant, amt_col='CVG_AMT')
        except Exception:
            self.logger.exception("Failed applying EXP_CvgAmount_Split expressions")
            raise

        # EXP_Derive_NISS_CVG_CD_And_PassThru: join the splits onto the SQ and derive coverage codes
        try:
            self.logger.info("Applying EXP_Derive_NISS_CVG_CD_And_PassThru expressions", node="EXP_Derive_NISS_CVG_CD_And_PassThru")
            df_joined = (
                df_SQ_WRK_BIRP_NISS_APRM_DETL_mighty_kant.alias("sq")
                .join(df_EXP_BILimit_Split_charming_babbage.select("NISS_APRM_DETL_SK", "BI_LMT_1_Decimal", "BI_LMT_NO_OF_PARTS", "SRC_BI_LMT").alias("bi"), on=["NISS_APRM_DETL_SK"], how="left")
                .join(df_EXP_CvgAmount_Split_trusting_aristotle.select("NISS_APRM_DETL_SK", "CVG_AMT_1_Decimal", "CVG_AMT_2_Decimal", "CVG_AMT_3_Decimal", "CVG_AMT_NO_OF_PARTS", "SRC_CVG_AMT").alias("cvg"), on=["NISS_APRM_DETL_SK"], how="left")
            )

            df_derived = df_joined.withColumn("v_CVG_AMT", regexp_replace(trim(col("CVG_AMT")), ",", ""))

            df_derived = df_derived.withColumn(
                "v_PIP_LOSS_INCOME_IND",
                when(col("PIP_LOSS_INCOME_IND") == 1, lit('Y')).when(col("PIP_LOSS_INCOME_IND") == 0, lit('N')).otherwise(lit('')),
            ).withColumn(
                "v_NJ_RESDNC_RLTNSHP_PIP_IND",
                when(col("NJ_RESDNC_RLTNSHP_PIP_IND") == 1, lit('Y')).when(col("NJ_RESDNC_RLTNSHP_PIP_IND") == 0, lit('N')).otherwise(lit('')),
            ).withColumn(
                "v_NY_FULL_CVG_GLASS_COMP_IND",
                when(col("NY_FULL_CVG_GLASS_COMP_IND") == 1, lit('Y')).when(col("NY_FULL_CVG_GLASS_COMP_IND") == 0, lit('N')).otherwise(lit('')),
            )

            df_derived = df_derived.withColumn(
                "v_CVG_CD_NewJersey",
                when(
                    (col("ST_ABBR") == 'NJ') & (col("ACCTNG_LOB") == '191') & (col("NJ_HLTH_INSR_PRIM") == 'Y') & (col("CVG_TYP_CD").isin('35050','35058','35102')),
                    lit('691'),
                ).when(
                    (col("ST_ABBR") == 'NJ') & (col("ACCTNG_LOB") == '191') & (col("NJ_HLTH_INSR_PRIM") == 'Y') & (col("CVG_TYP_CD").isin('35051','35059','35103')),
                    lit('693'),
                ).when(
                    (col("ST_ABBR") == 'NJ') & (col("ACCTNG_LOB") == '191') & (col("NJ_HLTH_INSR_PRIM") == 'Y') & (col("CVG_TYP_CD").isin('35066','35067','35106')),
                    lit('699'),
                ).when(
                    (col("ST_ABBR") == 'NJ') & (col("ACCTNG_LOB") == '191') & (col("NJ_HLTH_INSR_PRIM") == 'Y') & (col("CVG_TYP_CD").isin('35069','35060','35104')) & (col("v_NJ_RESDNC_RLTNSHP_PIP_IND") != 'Y') & (col("NJ_EXTR_PIP_PKG") == '01'),
                    lit('601'),
                ).when(
                    (col("ST_ABBR") == 'NJ') & (col("ACCTNG_LOB") == '191') & (col("NJ_HLTH_INSR_PRIM") == 'Y') & (col("CVG_TYP_CD").isin('35069','35060','35104')) & (col("v_NJ_RESDNC_RLTNSHP_PIP_IND") != 'Y') & (col("NJ_EXTR_PIP_PKG") == '02'),
                    lit('602'),
                ).when(
                    (col("ST_ABBR") == 'NJ') & (col("ACCTNG_LOB") == '191') & (col("NJ_HLTH_INSR_PRIM") == 'Y') & (col("CVG_TYP_CD").isin('35069','35060','35104')) & ((col("NJ_EXTR_PIP_PKG") == '') | (col("NJ_EXTR_PIP_PKG") == '0') | (col("NJ_EXTR_PIP_PKG") == '00') | (col("NJ_EXTR_PIP_PKG") == '#') | col("NJ_EXTR_PIP_PKG").isNull()),
                    lit('616'),
                ).when(
                    (col("ST_ABBR") == 'NJ') & (col("ACCTNG_LOB") == '192BI'),
                    lit('001'),
                ).otherwise(lit('???')),
            )

            df_derived = df_derived.withColumn(
                "v_CVG_CD_NewYork",
                when(
                    (col("ST_ABBR") == 'NY') & (col("ACCTNG_LOB") == '191') & (col("v_PIP_LOSS_INCOME_IND") != 'Y') & (col("CVG_TYP_CD").isin('35030','35007')) & (col("v_CVG_AMT") == '50000'),
                    lit('0711'),
                ).when(
                    (col("ST_ABBR") == 'NY') & (col("ACCTNG_LOB") == '191') & (col("v_PIP_LOSS_INCOME_IND") != 'Y') & (col("CVG_TYP_CD").isin('35023','35024')) & (col("v_CVG_AMT").isin('25000/500','50000/1000','100000/2000')),
                    lit('0711'),
                ).when(
                    (col("ST_ABBR") == 'NY') & (col("ACCTNG_LOB") == '192UM') & (col("CVG_TYP_CD").isin('13100','13109')),
                    lit('2051'),
                ).when(
                    (col("ST_ABBR") == 'NY') & (col("ACCTNG_LOB") == '211CC') & (col("CVG_TYP_CD") == '21000') & (col("v_NY_FULL_CVG_GLASS_COMP_IND") == 'Y') & (col("v_CVG_AMT") == '0'),
                    lit('001 '),
                ).otherwise(lit('')),
            )

            df_derived = df_derived.withColumn(
                "v_NISS_CVG_CD",
                when(col("ST_ABBR") == 'NJ', col("v_CVG_CD_NewJersey")).when(col("ST_ABBR") == 'NY', col("v_CVG_CD_NewYork").substr(1, 3)).otherwise(lit('???')),
            ).withColumn(
                "v_NISS_LIAB_OR_NO_FAULT_CD",
                when(col("ST_ABBR") == 'NY', col("v_CVG_CD_NewYork").substr(4, 1)).otherwise(lit(' ')),
            ).withColumn(
                "v_NISS_SSL_LIAB_CD",
                when((col("ST_ABBR") == 'NY') & (col("v_NISS_CVG_CD").isin('001','006')) & (col("NY_SSL_IND") == 1), lit('1'))
                .when((col("ST_ABBR") == 'NY') & (col("v_NISS_CVG_CD").isin('001','006')) & (col("NY_SSL_IND") != 1), lit('9'))
                .when((col("ST_ABBR") == 'NY') & (~col("v_NISS_CVG_CD").isin('001','006')), lit(' ')).otherwise(lit(' ')),
            )

            df_derived = df_derived.withColumn(
                "v_REC_EXCPN_IND",
                when(
                    (
                        (col("ST_ABBR") == 'NJ') & (col("ACCTNG_LOB") == '191') & (col("CVG_TYP_CD").isin('35069','35060')) & (col("NJ_HLTH_INSR_PRIM") == 'Y') & ((col("NJ_EXTR_PIP_PKG") == '') | (col("NJ_EXTR_PIP_PKG") == '0') | (col("NJ_EXTR_PIP_PKG") == '00') | (col("NJ_EXTR_PIP_PKG") == '#') | col("NJ_EXTR_PIP_PKG").isNull()) & (col("v_NISS_CVG_CD") == '616')
                    ) | (
                        (col("ST_ABBR") == 'NY') & (col("ACCTNG_LOB") == '191') & (~col("CVG_TYP_CD").isin('35030','35007','35023','35024','30019','34008','35006','35013','35017')) & (col("v_NISS_CVG_CD") == '071') & (col("v_PIP_LOSS_INCOME_IND") != 'Y')
                    ),
                    lit('Y'),
                ).otherwise(lit('')),
            )

            df_EXP_Derive_NISS_CVG_CD_And_PassThru_calm_spinoza = (
                df_derived
                .withColumn("NISS_SSL_LIAB_CD", when(col("v_NISS_SSL_LIAB_CD") == '', lit(' ')).otherwise(col("v_NISS_SSL_LIAB_CD")))
                .withColumn("NISS_LIAB_OR_NO_FAULT_CD", when(col("v_NISS_LIAB_OR_NO_FAULT_CD") == '', lit(' ')).otherwise(col("v_NISS_LIAB_OR_NO_FAULT_CD")))
                .withColumn("NISS_CVG_CD", when(col("v_NISS_CVG_CD") == '', lit('???')).otherwise(col("v_NISS_CVG_CD")))
                .withColumn("REC_EXCPN_IND", when((col("v_REC_EXCPN_IND") == 'Y') | (col("REC_EXCPN_IND") == 'Y'), lit('Y')).otherwise(lit('')))
            )

        except Exception:
            self.logger.exception("Failed applying EXP_Derive_NISS_CVG_CD_And_PassThru expressions")
            raise

        # UPD_NISS_CVG_CD: derive dd_op marker, write REJECT rows to reject prefix, and pass surviving rows downstream
        try:
            self.logger.info("Applying UPD_NISS_CVG_CD update-strategy marker", node="UPD_NISS_CVG_CD")
            df_marked = df_EXP_Derive_NISS_CVG_CD_And_PassThru_calm_spinoza.withColumn("dd_op", lit('UPDATE'))

            df_rejects = df_marked.filter(col("dd_op") == 'REJECT').withColumn("error_reason", lit('DD_REJECT'))
            if not df_rejects.isEmpty():
                try:
                    self.logger.info("Writing rejected rows from UPD_NISS_CVG_CD to reject prefix", node="UPD_NISS_CVG_CD")
                    self.parquet_writer.write(
                        dataframe=df_rejects,
                        path=f"s3://{self.s3_output_bucket}/reject/WRK_BIRP_NISS_APRM_DETL/",
                        mode="overwrite",
                    )
                except Exception:
                    self.logger.exception("Failed writing rejects for UPD_NISS_CVG_CD")
                    raise

            df_UPD_NISS_CVG_CD_upbeat_kepler = df_marked.filter(col("dd_op") != 'REJECT')
        except Exception:
            self.logger.exception("Failed applying UPD_NISS_CVG_CD update-strategy logic")
            raise

        # FDR_LIB_WRK_BIRP_NISS_APRM_DETL1: target write using load-modify-store-back against s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/
        try:
            self.logger.info("Applying load-modify-store-back for WRK_BIRP_NISS_APRM_DETL (read existing target)", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL1")
            df_existing = self.parquet_reader.read(path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/", options=None)
        except Exception:
            self.logger.exception("Failed reading existing WRK_BIRP_NISS_APRM_DETL target from S3")
            raise

        try:
            self.logger.info("Computing changed keys and removing updated/deleted rows from existing target", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL1")
            changed_keys_df = (
                df_UPD_NISS_CVG_CD_upbeat_kepler
                .filter(col("dd_op").isin('INSERT', 'UPDATE', 'DELETE'))
                .select("NISS_APRM_DETL_SK")
                .distinct()
            )

            existing_surviving = df_existing.join(changed_keys_df, on=["NISS_APRM_DETL_SK"], how="left_anti")

            apply_rows = df_UPD_NISS_CVG_CD_upbeat_kepler.filter(col("dd_op").isin('INSERT', 'UPDATE')).drop("dd_op")

            combined_df = existing_surviving.unionByName(apply_rows, allowMissingColumns=True)

            # Note: full read-anti-join-overwrite of the whole target can be expensive on large targets
            self.logger.info("Writing combined target WRK_BIRP_NISS_APRM_DETL to S3 as parquet (overwrite)", table="WRK_BIRP_NISS_APRM_DETL")
            self.parquet_writer.write(
                dataframe=combined_df,
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                mode="overwrite",
            )

            # assign the output dataframe name expected by downstream callers
            df_WRK_BIRP_NISS_APRM_DETL_pensive_einstein = combined_df
        except Exception:
            self.logger.exception("Failed applying load-modify-store-back for WRK_BIRP_NISS_APRM_DETL")
            raise

        self.glue_job.commit()

    def finalize(self) -> None:
        self.logger.info("Finalizing m_NU0C_NISS_AUTO_106_ATPRM_DTL_Upd_NYNJCvgCd")

    @staticmethod
    def _require_env(env: Mapping[str, Any], key: str) -> str:

        value = env.get(key)

        if not value or str(value).startswith("REPLACE_WITH_"):
            raise ConfigurationValidationError(
                f"config.yaml 'env.{key}' must be set"
            )

        return str(value)
