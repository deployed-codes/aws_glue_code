import sys
from typing import Any, Mapping

from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import DataFrame

from glue_lib.connector import ParquetReader, ParquetWriter
from glue_lib.exceptions import ConfigurationValidationError
from glue_lib.job.base import BaseJob
from pyspark.sql.functions import col, md5, lag, when, lit
from pyspark.sql.functions import sum as _sum
from pyspark.sql.window import Window


# Informatica mapping m_NU0C_NISS_AUTO_111_ATPRM_DTL_Upd_CVG_ATTR_SK.
# Every mapping node runs in order inside run().
class Nu0cNissAuto111AtprmDtlUpdCvgAttrSkJob(BaseJob):

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

        # FDR_LIB_WRK_BIRP_NISS_APRM_DETL: Source (WRK_ staging table) - try S3 parquet first, fall back to Glue Catalog
        try:
            self.logger.info("Attempting S3 parquet read for FDR_LIB_WRK_BIRP_NISS_APRM_DETL (WRK_BIRP_NISS_APRM_DETL)", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_happy_feynman = self.parquet_reader.read(
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                options=None,
            )
            self.logger.info("Read FDR_LIB_WRK_BIRP_NISS_APRM_DETL from S3 parquet (preferred)", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            if df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_happy_feynman.isEmpty():
                self.logger.warning("Source FDR_LIB_WRK_BIRP_NISS_APRM_DETL returned no rows from S3", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
        except Exception:
            self.logger.warning("S3 parquet read for FDR_LIB_WRK_BIRP_NISS_APRM_DETL failed; falling back to Glue Data Catalog read", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            try:
                dyf = self.glue_context.create_dynamic_frame_from_catalog(
                    database=self.glue_database,
                    table_name="WRK_BIRP_NISS_APRM_DETL",
                    transformation_ctx="df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_happy_feynman",
                )
                df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_happy_feynman = dyf.toDF()
                if df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_happy_feynman.isEmpty():
                    self.logger.warning("Source FDR_LIB_WRK_BIRP_NISS_APRM_DETL returned no rows from Glue Catalog", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            except Exception:
                self.logger.exception("Failed reading FDR_LIB_WRK_BIRP_NISS_APRM_DETL from Glue Catalog")
                raise

        # SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL: staged Application Source Qualifier - register upstream df as temp view and run the custom SQL override
        try:
            self.logger.info("Registering temp view for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL and running SQL override", node="SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            # register the upstream staged dataframe as the bare table name the SQL expects
            df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_happy_feynman.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")

            sql_query = f"""SELECT NISS_APRM_DETL_SK,
(CLNDR_YR || CALL_YR || NISS_CMPNY_CD || NISS_ST_CD || GRGNG_ZIP_5 || NISS_TERR_CD || LINE_CD || ACCDNT_YR || NISS_CVG_CD || RTNG_ZNE_CD || TERM_ZNE_CD || NISS_CLASS_CD || NISS_ELIG_PNTS_CD || NISS_AGE_GRP_CD || NISS_CMMCL_IND_CD || NISS_EXCPN_CD || NISS_FGVNS_CD || NISS_PASSV_RESTRA_CD || NISS_DEFNS_DRVR_CRD_CD || NISS_ANTI_THFT_DVC_CD || NISS_DAY_TM_RUN_LAMPS_DISC_CD || NISS_PLCY_LMT_CD || NISS_DEDUC_CD || NISS_SSL_LIAB_CD || NISS_SUBLOB_CD || NISS_TYP_LOSS_CD || NISS_LIAB_OR_NO_FAULT_CD || NISS_ANNL_STMNT_LOB_CD || NISS_PD_LOSS || NISS_PD_ALLOC_ADJUS_EXPNS || NISS_OUTSTNDG_LOSS || NISS_NO_PD_CLMS || NISS_NO_OUTSTND_CLMS || RSVD_NISS_USE || NISS_RSVD_CMPNY_USE || NISS_MNFCTRS_MDL_YR) as CVG_ATTR
FROM (

SELECT NISS_APRM_DETL_SK,
CASE WHEN (LTRIM(RTRIM(CLNDR_YR)) IS NULL OR  LTRIM(RTRIM(CLNDR_YR)) = '') THEN ' ' ELSE LTRIM(RTRIM(CLNDR_YR)) END AS CLNDR_YR,
CASE WHEN (LTRIM(RTRIM(CALL_YR)) IS NULL  OR LTRIM(RTRIM(CALL_YR)) = '') THEN ' ' ELSE LTRIM(RTRIM(CALL_YR)) END AS CALL_YR,
CASE WHEN (LTRIM(RTRIM(NISS_CMPNY_CD)) IS NULL OR LTRIM(RTRIM(NISS_CMPNY_CD)) = '') THEN ' ' ELSE LTRIM(RTRIM(NISS_CMPNY_CD)) END AS NISS_CMPNY_CD,
CASE WHEN (LTRIM(RTRIM(NISS_ST_CD)) IS NULL OR LTRIM(RTRIM(NISS_ST_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_ST_CD)) END AS NISS_ST_CD,
CASE WHEN (LTRIM(RTRIM(GRGNG_ZIP_5)) IS NULL OR LTRIM(RTRIM(GRGNG_ZIP_5)) ='') THEN ' ' ELSE LTRIM(RTRIM(GRGNG_ZIP_5)) END AS GRGNG_ZIP_5,
CASE WHEN (LTRIM(RTRIM(NISS_TERR_CD)) IS NULL OR LTRIM(RTRIM(NISS_TERR_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_TERR_CD)) END AS NISS_TERR_CD,
CASE WHEN (LTRIM(RTRIM(LINE_CD)) IS NULL OR LTRIM(RTRIM(LINE_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(LINE_CD)) END AS LINE_CD,
CASE WHEN (LTRIM(RTRIM(ACCDNT_YR)) IS NULL OR LTRIM(RTRIM(ACCDNT_YR)) ='') THEN ' ' ELSE LTRIM(RTRIM(ACCDNT_YR)) END AS ACCDNT_YR,
CASE WHEN (LTRIM(RTRIM(NISS_CVG_CD)) IS NULL  OR LTRIM(RTRIM(NISS_CVG_CD)) = '') THEN ' ' ELSE LTRIM(RTRIM(NISS_CVG_CD)) END AS NISS_CVG_CD,
CASE WHEN (LTRIM(RTRIM(RTNG_ZNE_CD)) IS NULL OR LTRIM(RTRIM(RTNG_ZNE_CD)) = '') THEN ' ' ELSE LTRIM(RTRIM(RTNG_ZNE_CD)) END AS RTNG_ZNE_CD,
CASE WHEN (LTRIM(RTRIM(TERM_ZNE_CD)) IS NULL OR LTRIM(RTRIM(TERM_ZNE_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(TERM_ZNE_CD)) END AS TERM_ZNE_CD,
CASE WHEN (LTRIM(RTRIM(NISS_CLASS_CD)) IS NULL OR LTRIM(RTRIM(NISS_CLASS_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_CLASS_CD)) END AS NISS_CLASS_CD,
CASE WHEN (LTRIM(RTRIM(NISS_ELIG_PNTS_CD)) IS NULL OR LTRIM(RTRIM(NISS_ELIG_PNTS_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_ELIG_PNTS_CD)) END AS NISS_ELIG_PNTS_CD,
TO_CHAR(CASE WHEN NISS_AGE_GRP_CD IS NULL THEN 0 ELSE NISS_AGE_GRP_CD END) AS NISS_AGE_GRP_CD,
CASE WHEN (LTRIM(RTRIM(NISS_CMMCL_IND_CD)) IS NULL OR LTRIM(RTRIM(NISS_CMMCL_IND_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_CMMCL_IND_CD)) END AS NISS_CMMCL_IND_CD,
CASE WHEN (LTRIM(RTRIM(NISS_EXCPN_CD)) IS NULL OR LTRIM(RTRIM(NISS_EXCPN_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_EXCPN_CD)) END AS NISS_EXCPN_CD,
CASE WHEN (LTRIM(RTRIM(NISS_FGVNS_CD)) IS NULL OR LTRIM(RTRIM(NISS_FGVNS_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_FGVNS_CD)) END AS NISS_FGVNS_CD,
CASE WHEN (LTRIM(RTRIM(NISS_PASSV_RESTRA_CD)) IS NULL OR LTRIM(RTRIM(NISS_PASSV_RESTRA_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_PASSV_RESTRA_CD)) END AS NISS_PASSV_RESTRA_CD,
CASE WHEN (LTRIM(RTRIM(NISS_DEFNS_DRVR_CRD_CD)) IS NULL OR LTRIM(RTRIM(NISS_DEFNS_DRVR_CRD_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_DEFNS_DRVR_CRD_CD)) END AS NISS_DEFNS_DRVR_CRD_CD,
CASE WHEN (LTRIM(RTRIM(NISS_ANTI_THFT_DVC_CD)) IS NULL OR LTRIM(RTRIM(NISS_ANTI_THFT_DVC_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_ANTI_THFT_DVC_CD)) END AS NISS_ANTI_THFT_DVC_CD,
CASE WHEN (LTRIM(RTRIM(NISS_DAY_TM_RUN_LAMPS_DISC_CD)) IS NULL OR LTRIM(RTRIM(NISS_DAY_TM_RUN_LAMPS_DISC_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_DAY_TM_RUN_LAMPS_DISC_CD)) END AS NISS_DAY_TM_RUN_LAMPS_DISC_CD,
CASE WHEN (LTRIM(RTRIM(NISS_PLCY_LMT_CD)) IS NULL OR LTRIM(RTRIM(NISS_PLCY_LMT_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_PLCY_LMT_CD)) END AS NISS_PLCY_LMT_CD,
CASE WHEN (LTRIM(RTRIM(NISS_DEDUC_CD)) IS NULL OR LTRIM(RTRIM(NISS_DEDUC_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_DEDUC_CD)) END AS NISS_DEDUC_CD,
CASE WHEN (LTRIM(RTRIM(NISS_SSL_LIAB_CD)) IS NULL OR LTRIM(RTRIM(NISS_SSL_LIAB_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_SSL_LIAB_CD)) END AS NISS_SSL_LIAB_CD,
CASE WHEN (LTRIM(RTRIM(NISS_SUBLOB_CD)) IS NULL OR LTRIM(RTRIM(NISS_SUBLOB_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_SUBLOB_CD)) END AS NISS_SUBLOB_CD,
CASE WHEN (LTRIM(RTRIM(NISS_TYP_LOSS_CD)) IS NULL OR LTRIM(RTRIM(NISS_TYP_LOSS_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_TYP_LOSS_CD)) END AS NISS_TYP_LOSS_CD,
CASE WHEN (LTRIM(RTRIM(NISS_LIAB_OR_NO_FAULT_CD)) IS NULL OR LTRIM(RTRIM(NISS_LIAB_OR_NO_FAULT_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_LIAB_OR_NO_FAULT_CD)) END AS NISS_LIAB_OR_NO_FAULT_CD,
CASE WHEN (LTRIM(RTRIM(NISS_ANNL_STMNT_LOB_CD)) IS NULL OR LTRIM(RTRIM(NISS_ANNL_STMNT_LOB_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_ANNL_STMNT_LOB_CD)) END AS NISS_ANNL_STMNT_LOB_CD,
TO_CHAR(CASE WHEN NISS_PD_LOSS IS NULL THEN 0 ELSE NISS_PD_LOSS END) AS NISS_PD_LOSS,
TO_CHAR(CASE WHEN NISS_PD_ALLOC_ADJUS_EXPNS IS NULL THEN 0 ELSE NISS_PD_ALLOC_ADJUS_EXPNS END) AS NISS_PD_ALLOC_ADJUS_EXPNS,
TO_CHAR(CASE WHEN NISS_OUTSTNDG_LOSS IS NULL THEN 0 ELSE NISS_OUTSTNDG_LOSS END) AS NISS_OUTSTNDG_LOSS,
TO_CHAR(CASE WHEN NISS_NO_PD_CLMS IS NULL THEN 0 ELSE NISS_NO_PD_CLMS END) AS NISS_NO_PD_CLMS,
TO_CHAR(CASE WHEN NISS_NO_OUTSTND_CLMS IS NULL THEN 0 ELSE NISS_NO_OUTSTND_CLMS END) AS NISS_NO_OUTSTND_CLMS,
CASE WHEN (LTRIM(RTRIM(RSVD_NISS_USE)) IS NULL OR LTRIM(RTRIM(RSVD_NISS_USE)) ='') THEN 0 ELSE LTRIM(RTRIM(RSVD_NISS_USE)) END AS RSVD_NISS_USE,
CASE WHEN (LTRIM(RTRIM(NISS_RSVD_CMPNY_USE)) IS NULL OR LTRIM(RTRIM(NISS_RSVD_CMPNY_USE)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_RSVD_CMPNY_USE)) END AS NISS_RSVD_CMPNY_USE,
TO_CHAR(CASE WHEN NISS_MNFCTRS_MDL_YR IS NULL THEN 0 ELSE NISS_MNFCTRS_MDL_YR END) AS NISS_MNFCTRS_MDL_YR

FROM WRK_BIRP_NISS_APRM_DETL
ORDER BY 
NISS_APRM_DETL_SK,
CLNDR_YR,
CALL_YR,
NISS_CMPNY_CD,
NISS_ST_CD,
GRGNG_ZIP_5,
NISS_TERR_CD,
LINE_CD,
ACCDNT_YR,
NISS_CVG_CD,
RTNG_ZNE_CD,
TERM_ZNE_CD,
NISS_CLASS_CD,
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
RSVD_NISS_USE,
NISS_RSVD_CMPNY_USE,
NISS_MNFCTRS_MDL_YR) A"""

            df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_loving_mendel = self.spark.sql(sql_query)
            if df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_loving_mendel.isEmpty():
                self.logger.warning("SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL returned no rows", node="SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
        except Exception:
            self.logger.exception("Failed running SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL SQL override")
            raise

        # EXP_Gen_CvgAttrCheckSum: compute MD5 checksum of CVG_ATTR and pass through NISS_APRM_DETL_SK
        try:
            self.logger.info("Applying EXP_Gen_CvgAttrCheckSum expressions", node="EXP_Gen_CvgAttrCheckSum")
            df_EXP_Gen_CvgAttrCheckSum_cool_heisenberg = (
                df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_loving_mendel
                .withColumn("CVG_ATTR_CHCKSUM", md5(col("CVG_ATTR")))
                .select(
                    col("NISS_APRM_DETL_SK"),
                    col("CVG_ATTR_CHCKSUM"),
                )
            )
        except Exception:
            self.logger.exception("Failed applying EXP_Gen_CvgAttrCheckSum expressions")
            raise

        # SRT_OrderByChkSum: global sort by CVG_ATTR_CHCKSUM asc, then NISS_APRM_DETL_SK asc
        try:
            self.logger.info("Applying SRT_OrderByChkSum sort", node="SRT_OrderByChkSum")
            df_SRT_OrderByChkSum_sharp_curie = df_EXP_Gen_CvgAttrCheckSum_cool_heisenberg.orderBy(
                col("CVG_ATTR_CHCKSUM").asc(),
                col("NISS_APRM_DETL_SK").asc(),
            )
        except Exception:
            self.logger.exception("Failed applying SRT_OrderByChkSum sort")
            raise

        # EXP_Gen_CvgAttrSK: detect checksum changes and generate cumulative surrogate CVG_ATTR_SK
        try:
            self.logger.info("Applying EXP_Gen_CvgAttrSK expressions", node="EXP_Gen_CvgAttrSK")
            w_lag = Window.orderBy(col("CVG_ATTR_CHCKSUM"), col("NISS_APRM_DETL_SK"))
            w_cum = Window.orderBy(col("CVG_ATTR_CHCKSUM"), col("NISS_APRM_DETL_SK")).rowsBetween(Window.unboundedPreceding, Window.currentRow)

            df_with_flags = (
                df_SRT_OrderByChkSum_sharp_curie
                .withColumn("prev_checksum", lag(col("CVG_ATTR_CHCKSUM")).over(w_lag))
                .withColumn(
                    "newval_flag",
                    when(col("prev_checksum").isNull() | (col("prev_checksum") != col("CVG_ATTR_CHCKSUM")), lit(1)).otherwise(lit(0)),
                )
            )

            df_EXP_Gen_CvgAttrSK_cool_babbage = (
                df_with_flags
                .withColumn("CVG_ATTR_SK", _sum(col("newval_flag")).over(w_cum).cast("bigint"))
                .select(
                    col("NISS_APRM_DETL_SK"),
                    col("CVG_ATTR_CHCKSUM"),
                    col("CVG_ATTR_SK"),
                )
            )
        except Exception:
            self.logger.exception("Failed applying EXP_Gen_CvgAttrSK expressions")
            raise

        # Upd_CVG_ATTR_SK: mark rows as UPDATE, write any REJECTs to reject prefix, drop rejects
        try:
            self.logger.info("Applying Upd_CVG_ATTR_SK update strategy (mark DD_UPDATE)", node="Upd_CVG_ATTR_SK")
            df_marked = df_EXP_Gen_CvgAttrSK_cool_babbage.withColumn("dd_op", lit("UPDATE"))

            df_rejects = df_marked.filter(col("dd_op") == "REJECT").withColumn("error_reason", lit("DD_REJECT"))
            try:
                if not df_rejects.isEmpty():
                    self.logger.info("Writing rejected rows for Upd_CVG_ATTR_SK to reject prefix", node="Upd_CVG_ATTR_SK")
                    self.parquet_writer.write(
                        dataframe=df_rejects,
                        path=f"s3://{self.s3_output_bucket}/reject/WRK_BIRP_NISS_APRM_DETL/",
                        mode="overwrite",
                    )
            except Exception:
                self.logger.exception("Failed writing reject rows for Upd_CVG_ATTR_SK")
                raise

            df_Upd_CVG_ATTR_SK_hopeful_einstein = df_marked.filter(col("dd_op") != "REJECT")
        except Exception:
            self.logger.exception("Failed applying Upd_CVG_ATTR_SK update strategy")
            raise

        # FDR_LIB_WRK_BIRP_NISS_APRM_DETL: Output - load-modify-store-back against the WRK target on S3
        try:
            self.logger.info("Applying load-modify-store-back for FDR_LIB_WRK_BIRP_NISS_APRM_DETL (full target rewrite)", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            # This full read-anti-join-overwrite can be expensive on large targets but is required to apply UPDATE/DELETE semantics.
            existing_df = self.parquet_reader.read(path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/", options=None)

            # keys of incoming changes
            changed_keys_df = df_Upd_CVG_ATTR_SK_hopeful_einstein.select("NISS_APRM_DETL_SK").distinct()

            # drop rows from existing that will be updated/deleted
            survivors_df = existing_df.join(changed_keys_df, on=["NISS_APRM_DETL_SK"], how="left_anti")

            # keep only INSERT/UPDATE rows from incoming (here marked UPDATE)
            incoming_apply_df = df_Upd_CVG_ATTR_SK_hopeful_einstein.filter(col("dd_op").isin("INSERT", "UPDATE"))

            # union survivors and incoming; allowMissingColumns so incoming may carry only changed columns
            combined_df = survivors_df.unionByName(incoming_apply_df, allowMissingColumns=True).drop("dd_op")

            # assign the Output node's dataframe name as required by the mapping
            df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_epic_pasteur = combined_df

            # write the final combined dataframe back to the WRK path (overwrite)
            self.parquet_writer.write(
                dataframe=df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_epic_pasteur,
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                mode="overwrite",
            )
        except Exception:
            self.logger.exception("Failed applying load-modify-store-back for FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            raise

        self.glue_job.commit()

    def finalize(self) -> None:
        self.logger.info("Finalizing m_NU0C_NISS_AUTO_111_ATPRM_DTL_Upd_CVG_ATTR_SK")

    @staticmethod
    def _require_env(env: Mapping[str, Any], key: str) -> str:

        value = env.get(key)

        if not value or str(value).startswith("REPLACE_WITH_"):
            raise ConfigurationValidationError(
                f"config.yaml 'env.{key}' must be set"
            )

        return str(value)
