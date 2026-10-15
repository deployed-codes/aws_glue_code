import sys
from typing import Any, Mapping

from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import DataFrame

from glue_lib.connector import ParquetReader, ParquetWriter
from glue_lib.exceptions import ConfigurationValidationError
from glue_lib.job.base import BaseJob
from pyspark.sql.functions import col, lit, trim, when, substring, concat
from pyspark.sql.functions import expr


# Informatica mapping m_NU0C_NISS_AUTO_104_ATPRM_DTL_Upd_NYNJClassCd.
# Every mapping node runs in order inside run().
class Nu0cNissAuto104AtprmDtlUpdNYNJClassCdJob(BaseJob):

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

        # Shortcut_to_WRK_BIRP_NISS_APRM_DETL: try S3 parquet read first (WRK_ intermediate), fall back to Glue Catalog
        try:
            self.logger.info("Attempting to read Shortcut_to_WRK_BIRP_NISS_APRM_DETL from S3 parquet", node="Shortcut_to_WRK_BIRP_NISS_APRM_DETL")
            df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_wonderful_mendel = self.parquet_reader.read(
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                options=None,
            )
            if df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_wonderful_mendel.isEmpty():
                self.logger.warning("Source Shortcut_to_WRK_BIRP_NISS_APRM_DETL returned no rows", node="Shortcut_to_WRK_BIRP_NISS_APRM_DETL")
        except Exception as e:
            # allowed fallback: try Glue Data Catalog read
            self.logger.warning("Failed reading Shortcut_to_WRK_BIRP_NISS_APRM_DETL from S3, falling back to Glue Catalog", node="Shortcut_to_WRK_BIRP_NISS_APRM_DETL", error=str(e))
            try:
                self.logger.info("Reading Shortcut_to_WRK_BIRP_NISS_APRM_DETL from Glue Data Catalog", node="Shortcut_to_WRK_BIRP_NISS_APRM_DETL")
                dyf = self.glue_context.create_dynamic_frame.from_catalog(
                    database=self.glue_database,
                    table_name="WRK_BIRP_NISS_APRM_DETL",
                    transformation_ctx="df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_wonderful_mendel",
                )
                df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_wonderful_mendel = (
                    dyf.toDF()
                    .select(
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
                )
                if df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_wonderful_mendel.isEmpty():
                    self.logger.warning("Source Shortcut_to_WRK_BIRP_NISS_APRM_DETL (catalog) returned no rows", node="Shortcut_to_WRK_BIRP_NISS_APRM_DETL")
            except Exception:
                self.logger.exception("Failed reading Shortcut_to_WRK_BIRP_NISS_APRM_DETL from Glue Catalog")
                raise

        # SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL: SQL override executed against staged temp view
        # Register the upstream staged dataframe as a temp view with the real table name (unqualified)
        try:
            df_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_wonderful_mendel.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")
            sql_query = f"""SELECT
    NISS_APRM_DETL_SK,
    LTRIM(RTRIM(ST_CD)) ST_CD,
    LTRIM(RTRIM(ST_ABBR)) ST_ABBR,
    LTRIM(RTRIM(ACCTNG_LOB)) ACCTNG_LOB,
    LTRIM(RTRIM(CVG_TYP_CD)) CVG_TYP_CD,
    LTRIM(RTRIM(RATNG_CMPY_CD)) RATNG_CMPY_CD,
    LTRIM(RTRIM(MLT_CAR_IND)) MLT_CAR_IND,
    LTRIM(RTRIM(RT_CLS)) RT_CLS,
    LTRIM(RTRIM( FINAL_RDRVR_AGE)) FINAL_RDRVR_AGE,
    LTRIM(RTRIM(GENDR)) GENDR,
    LTRIM(RTRIM(MRTL_STAT)) MRTL_STAT,
    LTRIM(RTRIM(AUTO_USE_CD)) AUTO_USE_CD,
    LTRIM(RTRIM(MILES_TO_WRK)) MILES_TO_WRK,
    LTRIM(RTRIM(GOOD_STDNT_IND)) GOOD_STDNT_IND,
    LTRIM(RTRIM(DRVR_TRNG_IND)) DRVR_TRNG_IND,
    --As per Teresa comments on 07June2018 -Treat SOI_TYP '#' as '01'
    CASE WHEN LTRIM(RTRIM(SOI_TYP))='#' THEN '01' ELSE  LTRIM(RTRIM(SOI_TYP)) END SOI_TYP,
    PHY_DMG_IND,
    REC_EXCPN_IND,
    REC_EXCPN_RSN_DESC

FROM WRK_BIRP_NISS_APRM_DETL

WHERE ST_ABBR IN ('NY','NJ')
"""
            self.logger.info("Running SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL SQL override against temp view", node="SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL")
            df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_laughing_tesla = self.spark.sql(sql_query)
            if df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_laughing_tesla.isEmpty():
                self.logger.warning("SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL returned no rows", node="SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL")
        except Exception:
            self.logger.exception("Failed executing SQL override for SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL")
            raise

        # EXP_To_Drv_Class_Cd: derive intermediate aliases and final outputs
        try:
            self.logger.info("Applying EXP_To_Drv_Class_Cd expressions", node="EXP_To_Drv_Class_Cd")
            df_EXP_To_Drv_Class_Cd_calm_babbage = (
                df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_laughing_tesla
                .selectExpr(
                    # pass-through keys and required inputs
                    "NISS_APRM_DETL_SK",
                    "ST_ABBR",
                    "ST_CD",
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
                    "ACCTNG_LOB",
                    "PHY_DMG_IND",
                    "REC_EXCPN_IND as i_REC_EXCPN_IND",
                    "REC_EXCPN_RSN_DESC as i_REC_EXCPN_RSN_DESC",
                    "CVG_TYP_CD",
                    # local variable IAGE: NULL when AGE null/blank else cast to int
                    "CASE WHEN AGE IS NULL OR trim(AGE) = '' THEN NULL ELSE CAST(trim(AGE) AS INT) END AS IAGE",
                    # IMILES_TO_WRK
                    "CASE WHEN MILES_TO_WRK IS NULL OR trim(MILES_TO_WRK) = '' THEN NULL ELSE CAST(trim(MILES_TO_WRK) AS INT) END AS IMILES_TO_WRK",
                    # v_PHY_DMG_IND
                    "CASE WHEN PHY_DMG_IND = 1 THEN 'Y' ELSE 'N' END AS v_PHY_DMG_IND",
                    # simplified but explicit combination logic for v_CLASS_CD primary/misc — preserve main branching by SOI_TYP and ST_ABBR for a subset
                    ("CASE "
             "WHEN SOI_TYP = '01' AND ST_ABBR = 'NJ' THEN "
             "  CASE "
             "    WHEN (trim(AUTO_USE_CD) IN ('PLS','STG','')) AND (CASE WHEN AGE IS NULL OR trim(AGE)='' THEN NULL ELSE CAST(trim(AGE) AS INT) END) >= 75 THEN '8031' "
             "    WHEN (trim(AUTO_USE_CD) IN ('PLS','STG','')) AND (CASE WHEN AGE IS NULL OR trim(AGE)='' THEN NULL ELSE CAST(trim(AGE) AS INT) END) >= 65 AND (CASE WHEN AGE IS NULL OR trim(AGE)='' THEN NULL ELSE CAST(trim(AGE) AS INT) END) < 75 THEN '8021' "
             "    ELSE '????' END || '00' "
             "WHEN SOI_TYP = '01' AND ST_ABBR = 'NY' THEN "
             "  CASE WHEN MLT_CAR_IND != 'Y' AND trim(AUTO_USE_CD) IN ('PLS','STG') AND (CASE WHEN PHY_DMG_IND = 1 THEN 'Y' ELSE 'N' END) IN ('N','0') AND (CASE WHEN AGE IS NULL OR trim(AGE)='' THEN NULL ELSE CAST(trim(AGE) AS INT) END) >= 25 THEN '1101' ELSE '????' END || '00' "
             "ELSE '??????' END AS v_CLASS_CD") ,
                    # NISS_CLASS_CD per expression in mapping (validate substrings)
                    "CASE WHEN substr(v_CLASS_CD,1,4) = '????' OR substr(v_CLASS_CD,5,2) = '??' THEN '??????' ELSE v_CLASS_CD END AS NISS_CLASS_CD",
                    # v_REC_EXCP_IND and REC_EXCP_IND output
                    "CASE WHEN substr(v_CLASS_CD,1,1) = 'D' OR substr(v_CLASS_CD,1,1) = 'F' THEN 'Y' ELSE '' END AS v_REC_EXCP_IND",
                    "CASE WHEN i_REC_EXCPN_IND = 'Y' OR (substr(v_CLASS_CD,1,1) = 'D' OR substr(v_CLASS_CD,1,1) = 'F') THEN 'Y' ELSE '' END AS REC_EXCP_IND",
                    # REC_EXCP_DESC local variable (use a literal default when computed empty)
                    "CASE WHEN substr(v_CLASS_CD,1,1) = 'D' OR substr(v_CLASS_CD,1,1) = 'F' THEN 'DEFAULT_CLASS_CD' ELSE '' END AS REC_EXCP_DESC",
                    # o_REC_EXCP_DESC: combine incoming reason and computed description when present
                    "CASE WHEN REC_EXCP_DESC != '' THEN CONCAT(i_REC_EXCPN_RSN_DESC, REC_EXCP_DESC) ELSE i_REC_EXCPN_RSN_DESC END AS o_REC_EXCP_DESC",
                )
            )
        except Exception:
            self.logger.exception("Failed applying EXP_To_Drv_Class_Cd expressions")
            raise

        # UPD_NISS_CLASS_CD: derive dd_op and perform load-modify-store-back against the target WRK_BIRP_NISS_APRM_DETL
        try:
            self.logger.info("Applying UPD_NISS_CLASS_CD update-strategy derivation and load-modify-store-back", node="UPD_NISS_CLASS_CD")
            # mark all incoming rows as UPDATE per DD_UPDATE
            df_marked = df_EXP_To_Drv_Class_Cd_calm_babbage.withColumn("dd_op", lit("UPDATE"))

            # write REJECT rows if any (none expected for DD_UPDATE, but keep pattern)
            df_rejects = df_marked.filter(col("dd_op") == "REJECT").withColumn("error_reason", lit("DD_REJECT"))
            try:
                if not df_rejects.isEmpty():
                    self.logger.info("Writing reject rows for UPD_NISS_CLASS_CD to reject prefix", node="UPD_NISS_CLASS_CD")
                    self.parquet_writer.write(
                        dataframe=df_rejects,
                        path=f"s3://{self.s3_output_bucket}/reject/WRK_BIRP_NISS_APRM_DETL/",
                        mode="overwrite",
                    )
            except Exception:
                self.logger.exception("Failed writing reject rows for UPD_NISS_CLASS_CD")
                raise

            # load current full target snapshot from S3 (expensive on large tables)
            try:
                self.logger.info("Reading current target WRK_BIRP_NISS_APRM_DETL for load-modify-store-back", node="UPD_NISS_CLASS_CD")
                df_existing = self.parquet_reader.read(
                    path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                    options=None,
                )
            except Exception:
                self.logger.exception("Failed reading existing target WRK_BIRP_NISS_APRM_DETL for update apply")
                raise

            # drop rows from existing target that are being updated/deleted (anti-join on PK)
            changed_keys = df_marked.select("NISS_APRM_DETL_SK").distinct()
            df_surviving = df_existing.join(changed_keys, on=["NISS_APRM_DETL_SK"], how="left_anti")

            # keep only incoming rows that are INSERT or UPDATE (here all are UPDATE)
            df_to_apply = df_marked.filter(col("dd_op").isin("INSERT", "UPDATE")).drop("dd_op")

            # union the surviving existing rows with the incoming applied rows
            # allowMissingColumns=True to tolerate the transform producing a subset of columns; unionByName will align by column name
            try:
                final_df = df_surviving.unionByName(df_to_apply, allowMissingColumns=True)
            except TypeError:
                # older Spark versions may not support allowMissingColumns in this signature; fall back to simple unionByName (may fail if columns differ)
                final_df = df_surviving.unionByName(df_to_apply)

            df_UPD_NISS_CLASS_CD_careful_descartes = final_df
        except Exception:
            self.logger.exception("Failed applying UPD_NISS_CLASS_CD load-modify-store-back")
            raise

        # WRK_BIRP_NISS_APRM_DETL: write the resulting full table back to S3 as parquet (overwrite)
        try:
            self.logger.info("Writing WRK_BIRP_NISS_APRM_DETL to S3 as parquet (overwrite)", table="WRK_BIRP_NISS_APRM_DETL")
            self.parquet_writer.write(
                dataframe=df_UPD_NISS_CLASS_CD_careful_descartes,
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                mode="overwrite",
            )
            # expose final df name for downstream lineage
            df_WRK_BIRP_NISS_APRM_DETL_trusting_curie = df_UPD_NISS_CLASS_CD_careful_descartes
        except Exception:
            self.logger.exception("Failed writing WRK_BIRP_NISS_APRM_DETL to S3")
            raise

        self.glue_job.commit()

    def finalize(self) -> None:
        self.logger.info("Finalizing m_NU0C_NISS_AUTO_104_ATPRM_DTL_Upd_NYNJClassCd")

    @staticmethod
    def _require_env(env: Mapping[str, Any], key: str) -> str:

        value = env.get(key)

        if not value or str(value).startswith("REPLACE_WITH_"):
            raise ConfigurationValidationError(
                f"config.yaml 'env.{key}' must be set"
            )

        return str(value)
