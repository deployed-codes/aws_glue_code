import sys
from typing import Any, Mapping

from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import DataFrame

from glue_lib.connector import ParquetReader, ParquetWriter
from glue_lib.exceptions import ConfigurationValidationError
from glue_lib.job.base import BaseJob
from pyspark.sql.functions import col, lit


# Informatica mapping m_NU0C_NISS_AUTO_112_ATPRM_DTL_Upd_CVG_CD_SK.
# Every mapping node runs in order inside run().
class Nu0cNissAuto112AtprmDtlUpdCvgCdSkJob(BaseJob):

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

        # FDR_LIB_WRK_BIRP_NISS_APRM_DETL: try S3 parquet read first, fall back to Glue catalog if missing
        try:
            self.logger.info("Attempting to read FDR_LIB_WRK_BIRP_NISS_APRM_DETL from S3 parquet first", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_busy_galileo = self.parquet_reader.read(
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                options=None,
            ).select("CVG_ATTR_SK")
            self.logger.info("Read FDR_LIB_WRK_BIRP_NISS_APRM_DETL from S3 parquet", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            if df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_busy_galileo.isEmpty():
                self.logger.warning("Source FDR_LIB_WRK_BIRP_NISS_APRM_DETL returned no rows from S3", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
        except Exception as e:
            # fall back to Glue Data Catalog read
            self.logger.warning("S3 read for FDR_LIB_WRK_BIRP_NISS_APRM_DETL failed, falling back to Glue catalog", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL", error=str(e))
            try:
                self.logger.info("Reading FDR_LIB_WRK_BIRP_NISS_APRM_DETL from Glue Data Catalog", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
                dyf = self.glue_context.create_dynamic_frame_from_catalog(
                    database=self.glue_database,
                    table_name="WRK_BIRP_NISS_APRM_DETL",
                    transformation_ctx="df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_busy_galileo",
                )
                df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_busy_galileo = dyf.toDF().select("CVG_ATTR_SK")
                if df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_busy_galileo.isEmpty():
                    self.logger.warning("Source FDR_LIB_WRK_BIRP_NISS_APRM_DETL returned no rows from Glue catalog", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            except Exception:
                self.logger.exception("Failed reading FDR_LIB_WRK_BIRP_NISS_APRM_DETL from Glue catalog")
                raise

        # SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL: staged-case SQL override executed against temp view
        try:
            # register staged dataframe as a temp view for the override
            df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_busy_galileo.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")

            sql_query = f"""SELECT DISTINCT DETL.NISS_APRM_DETL_SK,DETL.CVG_ATTR_SK,Y.CVG_CD_ATTR_SK
FROM (
    SELECT DISTINCT CVG_ATTR_SK,CVG_TYP_CD,CVG_TYP_IND,CVG_CD_ATTR,ROW_NUMBER() OVER ( ORDER BY CVG_CD_ATTR) AS CVG_CD_ATTR_SK
    FROM (
        SELECT DISTINCT
            CVG_ATTR_SK,
            CASE WHEN (LTRIM(RTRIM(CLNDR_YR)) IS NULL OR  LTRIM(RTRIM(CLNDR_YR)) = '') THEN ' ' ELSE LTRIM(RTRIM(CLNDR_YR)) END AS CLNDR_YR,
            CASE WHEN (LTRIM(RTRIM(REG_PRD_YR)) IS NULL  OR LTRIM(RTRIM(REG_PRD_YR)) = '') THEN ' ' ELSE LTRIM(RTRIM(REG_PRD_YR)) END AS REG_PRD_YR,
            CASE WHEN (LTRIM(RTRIM(NISS_CMPNY_CD)) IS NULL OR LTRIM(RTRIM(NISS_CMPNY_CD)) = '') THEN ' ' ELSE LTRIM(RTRIM(NISS_CMPNY_CD)) END AS NISS_CMPNY_CD,
            CASE WHEN (LTRIM(RTRIM(NISS_ST_CD)) IS NULL OR LTRIM(RTRIM(NISS_ST_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_ST_CD)) END AS NISS_ST_CD,
            CASE WHEN (LTRIM(RTRIM(GRGNG_ZIP_5)) IS NULL OR LTRIM(RTRIM(GRGNG_ZIP_5)) ='') THEN ' ' ELSE LTRIM(RTRIM(GRGNG_ZIP_5)) END AS GRGNG_ZIP_5,
            CASE WHEN (LTRIM(RTRIM(NISS_TERR_CD)) IS NULL OR LTRIM(RTRIM(NISS_TERR_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_TERR_CD)) END AS NISS_TERR_CD,
            CASE WHEN (LTRIM(RTRIM(CVG_TYP_CD)) IS NULL OR LTRIM(RTRIM(CVG_TYP_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(CVG_TYP_CD)) END AS CVG_TYP_CD,
            CASE WHEN (LTRIM(RTRIM(CVG_TYP_IND)) IS NULL OR LTRIM(RTRIM(CVG_TYP_IND)) ='') THEN ' ' ELSE LTRIM(RTRIM(CVG_TYP_IND)) END AS CVG_TYP_IND,
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
            CASE WHEN NISS_PD_LOSS IS NULL THEN 0 ELSE NISS_PD_LOSS END AS NISS_PD_LOSS,
            CASE WHEN NISS_PD_ALLOC_ADJUS_EXPNS IS NULL THEN 0 ELSE NISS_PD_ALLOC_ADJUS_EXPNS END AS NISS_PD_ALLOC_ADJUS_EXPNS,
            CASE WHEN NISS_OUTSTNDG_LOSS IS NULL THEN 0 ELSE NISS_OUTSTNDG_LOSS END AS NISS_OUTSTNDG_LOSS,
            CASE WHEN NISS_NO_PD_CLMS IS NULL THEN 0 ELSE NISS_NO_PD_CLMS END AS NISS_NO_PD_CLMS,
            CASE WHEN NISS_NO_OUTSTND_CLMS IS NULL THEN 0 ELSE NISS_NO_OUTSTND_CLMS END AS NISS_NO_OUTSTND_CLMS,
            CASE WHEN (LTRIM(RTRIM(RSVD_NISS_USE)) IS NULL OR LTRIM(RTRIM(RSVD_NISS_USE)) ='') THEN 0 ELSE LTRIM(RTRIM(RSVD_NISS_USE)) END AS RSVD_NISS_USE,
            CASE WHEN (LTRIM(RTRIM(NISS_RSVD_CMPNY_USE)) IS NULL OR LTRIM(RTRIM(NISS_RSVD_CMPNY_USE)) ='') THEN ' ' ELSE LTRIM(RTRIM(NISS_RSVD_CMPNY_USE)) END AS NISS_RSVD_CMPNY_USE,
            CASE WHEN NISS_MNFCTRS_MDL_YR IS NULL THEN 0 ELSE NISS_MNFCTRS_MDL_YR END AS NISS_MNFCTRS_MDL_YR
        FROM WRK_BIRP_NISS_APRM_DETL
        ORDER BY
            CLNDR_YR,
            REG_PRD_YR,
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
            NISS_MNFCTRS_MDL_YR
    ) A
) X)Y,
(
    SELECT NISS_APRM_DETL_SK,CVG_ATTR_SK,
        CASE WHEN (LTRIM(RTRIM(CVG_TYP_CD)) IS NULL OR LTRIM(RTRIM(CVG_TYP_CD)) ='') THEN ' ' ELSE LTRIM(RTRIM(CVG_TYP_CD)) END AS CVG_TYP_CD,
        CASE WHEN (LTRIM(RTRIM(CVG_TYP_IND)) IS NULL OR LTRIM(RTRIM(CVG_TYP_IND)) ='') THEN ' ' ELSE LTRIM(RTRIM(CVG_TYP_IND)) END AS CVG_TYP_IND
    FROM WRK_BIRP_NISS_APRM_DETL
) DETL
WHERE Y.CVG_ATTR_SK = DETL.CVG_ATTR_SK
  AND LTRIM(RTRIM(Y.CVG_TYP_CD)) = LTRIM(RTRIM(DETL.CVG_TYP_CD))
  AND LTRIM(RTRIM(Y.CVG_TYP_IND)) = LTRIM(RTRIM(DETL.CVG_TYP_IND))
ORDER BY DETL.NISS_APRM_DETL_SK,DETL.CVG_ATTR_SK
"""

            try:
                self.logger.info("Executing SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL SQL override via spark.sql", node="SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
                df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_loving_heisenberg = self.spark.sql(sql_query)
                if df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_loving_heisenberg.isEmpty():
                    self.logger.warning("SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL returned no rows", node="SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            except Exception:
                self.logger.exception("Failed executing SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL SQL override")
                raise
        except Exception:
            self.logger.exception("Failed preparing or executing SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            raise

        # EXP_To_generate_CVG_CD_SK: pass-through select of the three pipeline columns
        try:
            self.logger.info("Applying EXP_To_generate_CVG_CD_SK pass-through select", node="EXP_To_generate_CVG_CD_SK")
            df_EXP_To_generate_CVG_CD_SK_admiring_hawking = df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_loving_heisenberg.selectExpr(
                "NISS_APRM_DETL_SK",
                "CVG_ATTR_SK",
                "CVG_CD_ATTR_SK"
            )
        except Exception:
            self.logger.exception("Failed applying EXP_To_generate_CVG_CD_SK expressions")
            raise

        # Upd_CVG_CD_SK: Update Strategy - mark dd_op, write rejects, and perform load-modify-store-back on the S3 WRK target
        try:
            self.logger.info("Applying Upd_CVG_CD_SK update-strategy marker derivation", node="Upd_CVG_CD_SK")
            # transformation configured as DD_UPDATE for all rows
            df_marked = df_EXP_To_generate_CVG_CD_SK_admiring_hawking.withColumn("dd_op", lit("UPDATE"))

            # write rejects (if any) to reject prefix
            try:
                df_rejects = df_marked.filter(col("dd_op") == "REJECT").withColumn("error_reason", lit("DD_REJECT"))
                if not df_rejects.isEmpty():
                    self.logger.info("Writing reject rows from Upd_CVG_CD_SK to reject prefix", node="Upd_CVG_CD_SK")
                    self.parquet_writer.write(
                        dataframe=df_rejects,
                        path=f"s3://{self.s3_output_bucket}/reject/WRK_BIRP_NISS_APRM_DETL/",
                        mode="overwrite",
                    )
            except Exception:
                self.logger.exception("Failed writing reject rows for Upd_CVG_CD_SK")
                raise

            # drop rejects from apply
            df_changed = df_marked.filter(col("dd_op") != "REJECT")

            # load existing target from S3
            try:
                self.logger.info("Reading existing WRK_BIRP_NISS_APRM_DETL target from S3 for load-modify-store-back", node="Upd_CVG_CD_SK")
                existing_df = self.parquet_reader.read(
                    path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                    options=None,
                )
            except Exception:
                self.logger.exception("Failed reading existing WRK_BIRP_NISS_APRM_DETL from S3")
                raise

            # compute keys being changed and build final dataframe
            try:
                changed_keys = df_changed.select("NISS_APRM_DETL_SK").distinct()

                # existing rows that are not updated/deleted
                existing_remaining = existing_df.join(
                    changed_keys,
                    on="NISS_APRM_DETL_SK",
                    how="left_anti",
                )

                # build updated_rows by joining existing rows to incoming changed rows and replacing CVG_CD_ATTR_SK
                changed_small = df_changed.select("NISS_APRM_DETL_SK", "CVG_CD_ATTR_SK")
                e = existing_df.alias("e")
                c = changed_small.alias("c")
                joined = e.join(c, on=(col("e.NISS_APRM_DETL_SK") == col("c.NISS_APRM_DETL_SK")), how="inner")

                # Reconstruct the full schema for updated rows replacing CVG_CD_ATTR_SK with incoming value
                updated_cols = []
                for cn in existing_df.columns:
                    if cn == "CVG_CD_ATTR_SK":
                        updated_cols.append(col("c.CVG_CD_ATTR_SK").alias("CVG_CD_ATTR_SK"))
                    else:
                        updated_cols.append(col(f"e.{cn}").alias(cn))

                updated_rows = joined.select(*updated_cols)

                # final table = existing_remaining union updated_rows
                final_df = existing_remaining.unionByName(updated_rows)

            except Exception:
                self.logger.exception("Failed building final dataframe for Upd_CVG_CD_SK apply")
                raise

            # write final table back to S3 (overwrite)
            try:
                self.logger.info("Writing updated WRK_BIRP_NISS_APRM_DETL back to S3 (overwrite)", node="Upd_CVG_CD_SK", table="WRK_BIRP_NISS_APRM_DETL")
                self.parquet_writer.write(
                    dataframe=final_df,
                    path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                    mode="overwrite",
                )
            except Exception:
                self.logger.exception("Failed writing updated WRK_BIRP_NISS_APRM_DETL to S3")
                raise

            # expose the update-strategy result for downstream
            df_Upd_CVG_CD_SK_nostalgic_lovelace = final_df

        except Exception:
            self.logger.exception("Failed applying Upd_CVG_CD_SK update-strategy")
            raise

        # FDR_LIB_WRK_BIRP_NISS_APRM_DETL1: Output - write final dataframe to S3 as parquet (overwrite)
        df_WRK_BIRP_NISS_APRM_DETL_brave_euclid = df_Upd_CVG_CD_SK_nostalgic_lovelace

        # write intermediate WRK_ table as parquet to S3 (overwrite)
        try:
            self.logger.info("Writing WRK_BIRP_NISS_APRM_DETL to S3 as parquet (overwrite)", table="WRK_BIRP_NISS_APRM_DETL")
            self.parquet_writer.write(
                dataframe=df_WRK_BIRP_NISS_APRM_DETL_brave_euclid,
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                mode="overwrite",
            )
        except Exception:
            self.logger.exception("Failed writing WRK_BIRP_NISS_APRM_DETL to S3")
            raise

        self.glue_job.commit()

    def finalize(self) -> None:
        self.logger.info("Finalizing m_NU0C_NISS_AUTO_112_ATPRM_DTL_Upd_CVG_CD_SK")

    @staticmethod
    def _require_env(env: Mapping[str, Any], key: str) -> str:

        value = env.get(key)

        if not value or str(value).startswith("REPLACE_WITH_"):
            raise ConfigurationValidationError(
                f"config.yaml 'env.{key}' must be set"
            )

        return str(value)
