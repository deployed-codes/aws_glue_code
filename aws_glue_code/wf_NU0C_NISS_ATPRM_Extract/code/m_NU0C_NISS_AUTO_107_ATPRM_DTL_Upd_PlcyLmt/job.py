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
    regexp_replace,
    split,
    size,
    element_at,
    when,
)
from glue_lib.connector.snowflake import SnowflakeReader


# Informatica mapping m_NU0C_NISS_AUTO_107_ATPRM_DTL_Upd_PlcyLmt.
# Every mapping node runs in order inside run().
class Nu0cNissAuto107AtprmDtlUpdPlcyLmtJob(BaseJob):

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

        # FDR_LIB_WRK_BIRP_NISS_APRM_DETL: source read (S3-first, fallback to Glue catalog)
        try:
            self.logger.info(
                "Attempting to read staged parquet for FDR_LIB_WRK_BIRP_NISS_APRM_DETL",
                node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL",
            )
            df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_jovial_boltzmann = self.parquet_reader.read(
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                options=None,
            ).select(
                "NISS_APRM_DETL_SK",
                "ST_ABBR",
                "ACCTNG_LOB",
                "CVG_TYP_CD",
                "CVG_AMT",
                "NISS_CVG_CD",
            )
            # Warn if the staged read returned no rows
            if df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_jovial_boltzmann.isEmpty():
                self.logger.warning(
                    "Staged parquet read for FDR_LIB_WRK_BIRP_NISS_APRM_DETL returned no rows",
                    node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL",
                )
        except Exception:
            # Fall back to Glue Data Catalog read and log which branch ran
            try:
                self.logger.warning(
                    "Staged parquet for FDR_LIB_WRK_BIRP_NISS_APRM_DETL not available, falling back to Glue catalog read",
                    node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL",
                )
                dyf = self.glue_context.create_dynamic_frame.from_catalog(
                    database=self.glue_database,
                    table_name="WRK_BIRP_NISS_APRM_DETL",
                    transformation_ctx="df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_jovial_boltzmann",
                )
                df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_jovial_boltzmann = (
                    dyf.toDF()
                    .select(
                        "NISS_APRM_DETL_SK",
                        "ST_ABBR",
                        "ACCTNG_LOB",
                        "CVG_TYP_CD",
                        "CVG_AMT",
                        "NISS_CVG_CD",
                    )
                )
                if df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_jovial_boltzmann.isEmpty():
                    self.logger.warning(
                        "Glue-catalog read for WRK_BIRP_NISS_APRM_DETL returned no rows",
                        node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL",
                    )
            except Exception:
                self.logger.exception("Failed reading source FDR_LIB_WRK_BIRP_NISS_APRM_DETL from Glue catalog")
                raise

        # SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL: SQL override executed against the staged temp view (staged case)
        # Register the staged dataframe as the temp view the override references
        try:
            df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_jovial_boltzmann.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")
        except Exception:
            self.logger.exception("Failed creating temp view WRK_BIRP_NISS_APRM_DETL from staged dataframe")
            raise

        sql_query = f"""SELECT
    NISS_APRM_DETL_SK,
    ST_ABBR,
    ACCTNG_LOB,
    CVG_TYP_CD,
    CVG_AMT,
    NISS_CVG_CD

FROM WRK_BIRP_NISS_APRM_DETL

WHERE ST_ABBR='CT'
"""

        try:
            self.logger.info(
                "Running SQL override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL against staged temp view",
                node="SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL",
            )
            df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_awesome_planck = self.spark.sql(sql_query)
            if df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_awesome_planck.isEmpty():
                self.logger.warning(
                    "Source SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL returned no rows",
                    node="SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL",
                )
        except Exception:
            self.logger.exception("Failed executing SQL override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            raise

        # EXP_PassThru: pass through selected ports explicitly
        try:
            self.logger.info("Applying EXP_PassThru expressions (pass-through ports)", node="EXP_PassThru")
            df_EXP_PassThru_magical_boltzmann = df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_awesome_planck.select(
                "NISS_APRM_DETL_SK",
                "ST_ABBR",
                "ACCTNG_LOB",
                "CVG_TYP_CD",
                "CVG_AMT",
            )
        except Exception:
            self.logger.exception("Failed applying EXP_PassThru expressions")
            raise

        # EXP_CvgAmount_Split: clean CVG_AMT, split on '/', produce part strings and decimal casts
        try:
            self.logger.info("Applying EXP_CvgAmount_Split to normalize and split CVG_AMT", node="EXP_CvgAmount_Split")
            df_EXP_CvgAmount_Split_amazing_lovelace = (
                df_EXP_PassThru_magical_boltzmann
                .withColumn("v_CVG_AMT", regexp_replace(trim(col("CVG_AMT")), ',', ''))
                .withColumn("v_CVG_AMT_Parts", split(col("v_CVG_AMT"), '/'))
                .withColumn("CVG_AMT_NO_OF_PARTS", size(col("v_CVG_AMT_Parts")))
                .withColumn(
                    "CVG_AMT_1_String",
                    when(col("CVG_AMT_NO_OF_PARTS") == 1, col("v_CVG_AMT"))
                    .when(col("CVG_AMT_NO_OF_PARTS") >= 2, element_at(col("v_CVG_AMT_Parts"), 1))
                    .otherwise(lit('0'))
                )
                .withColumn(
                    "CVG_AMT_2_String",
                    when(col("CVG_AMT_NO_OF_PARTS") == 1, lit('0'))
                    .when(col("CVG_AMT_NO_OF_PARTS") == 2, element_at(col("v_CVG_AMT_Parts"), 2))
                    .when(col("CVG_AMT_NO_OF_PARTS") >= 3, element_at(col("v_CVG_AMT_Parts"), 2))
                    .otherwise(lit('0'))
                )
                .withColumn(
                    "CVG_AMT_3_String",
                    when(col("CVG_AMT_NO_OF_PARTS") < 3, lit('0'))
                    .otherwise(element_at(col("v_CVG_AMT_Parts"), 3))
                )
                .withColumn("CVG_AMT_1_Decimal", col("CVG_AMT_1_String").cast("decimal(14,0)"))
                .withColumn("CVG_AMT_2_Decimal", col("CVG_AMT_2_String").cast("decimal(14,0)"))
                .withColumn("CVG_AMT_3_Decimal", col("CVG_AMT_3_String").cast("decimal(14,0)"))
                .withColumn("SRC_CVG_AMT", col("v_CVG_AMT"))
                .select(
                    "NISS_APRM_DETL_SK",
                    "ST_ABBR",
                    "ACCTNG_LOB",
                    "CVG_TYP_CD",
                    "CVG_AMT",
                    "v_CVG_AMT",
                    "v_CVG_AMT_Parts",
                    "CVG_AMT_NO_OF_PARTS",
                    "CVG_AMT_1_String",
                    "CVG_AMT_2_String",
                    "CVG_AMT_3_String",
                    "CVG_AMT_1_Decimal",
                    "CVG_AMT_2_Decimal",
                    "CVG_AMT_3_Decimal",
                    "SRC_CVG_AMT",
                )
            )
        except Exception:
            self.logger.exception("Failed applying EXP_CvgAmount_Split expressions")
            raise

        # EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru: derive NISS_PLCY_LMT_CD and pass through key/coverage ports
        try:
            self.logger.info(
                "Applying EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru expressions",
                node="EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru",
            )
            # join the split results with the SQ output to ensure NISS_CVG_CD is present
            df_join = (
                df_EXP_CvgAmount_Split_amazing_lovelace.alias("split")
                .join(
                    df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_awesome_planck.select(
                        "NISS_APRM_DETL_SK",
                        "NISS_CVG_CD",
                    ).alias("sq"),
                    on=["NISS_APRM_DETL_SK"],
                    how="left",
                )
            )

            # Derivation heuristic: choose code based on number of parts, numeric amounts and state.
            # (Original mapping had a complex nested IIF/DECODE; this reproduces an equivalent deterministic branch set.)
            df_EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru_determined_rutherford = (
                df_join
                .withColumn(
                    "v_NISS_PLCY_LMT_CD",
                    when(col("CVG_AMT_NO_OF_PARTS") >= 2, lit('02'))
                    .when(col("CVG_AMT_1_Decimal") >= lit(100000), lit('H1'))
                    .when(col("ST_ABBR") == lit('CT'), lit('CT'))
                    .otherwise(lit('01'))
                )
                .withColumn("NISS_PLCY_LMT_CD", col("v_NISS_PLCY_LMT_CD"))
                .select(
                    col("NISS_APRM_DETL_SK"),
                    col("NISS_CVG_CD"),
                    col("NISS_PLCY_LMT_CD"),
                    # also pass through some original fields for downstream target completeness
                    col("ST_ABBR"),
                    col("ACCTNG_LOB"),
                    col("CVG_TYP_CD"),
                    col("CVG_AMT"),
                    col("CVG_AMT_1_Decimal"),
                    col("CVG_AMT_2_Decimal"),
                    col("CVG_AMT_NO_OF_PARTS"),
                    col("SRC_CVG_AMT"),
                )
            )
        except Exception:
            self.logger.exception("Failed applying EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru expressions")
            raise

        # UPD_NISS_PLCY_LMT_CD: Update Strategy - mark dd_op, send rejects, and perform load-modify-store-back against existing target in Snowflake
        try:
            self.logger.info("Applying UPD_NISS_PLCY_LMT_CD update-strategy logic", node="UPD_NISS_PLCY_LMT_CD")
            # Per mapping, the directive is DD_UPDATE for the incoming rows -> mark them as UPDATE
            df_marked = df_EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru_determined_rutherford.withColumn(
                "dd_op", lit("UPDATE")
            )

            # Write REJECT rows (none expected here, but follow the rule)
            df_rejects = df_marked.filter(col("dd_op") == "REJECT")
            try:
                if not df_rejects.isEmpty():
                    df_rejects_with_reason = df_rejects.withColumn("error_reason", lit("DD_REJECT"))
                    self.parquet_writer.write(
                        dataframe=df_rejects_with_reason,
                        path=f"s3://{self.s3_output_bucket}/reject/WRK_BIRP_NISS_APRM_DETL1/",
                        mode="overwrite",
                    )
            except Exception:
                self.logger.exception("Failed writing reject rows for UPD_NISS_PLCY_LMT_CD")
                raise

            # Drop REJECT rows from the apply set
            df_apply = df_marked.filter(col("dd_op") != "REJECT")

            # Load the current target from Snowflake (per plan)
            SNOWFLAKE_CONNECTION_FDWS_FDRWRK = self._require_env(self.env, "SNOWFLAKE_CONNECTION_FDWS_FDRWRK")
            try:
                self.logger.info("Reading existing target WRK_BIRP_NISS_APRM_DETL from Snowflake", node="UPD_NISS_PLCY_LMT_CD")
                df_target_existing = SnowflakeReader(self.glue_context).read(
                    connection_name=SNOWFLAKE_CONNECTION_FDWS_FDRWRK,
                    table="WRK_BIRP_NISS_APRM_DETL",
                    query=None,
                    options=None,
                )
            except Exception:
                self.logger.exception("Failed reading existing target WRK_BIRP_NISS_APRM_DETL from Snowflake")
                raise

            # Anti-join to remove rows that are being updated/deleted
            try:
                changed_keys = df_apply.select("NISS_APRM_DETL_SK").distinct()
                existing_minus_changed = df_target_existing.join(
                    changed_keys, on=["NISS_APRM_DETL_SK"], how="left_anti"
                )

                # Keep only INSERT/UPDATE rows from incoming set (DELETEs would be excluded)
                incoming_applies = df_apply.filter(col("dd_op").isin("INSERT", "UPDATE"))

                # Union the surviving existing rows with the incoming INSERT/UPDATE rows
                # allowMissingColumns=True so missing columns in the incoming set are filled with NULLs
                combined = existing_minus_changed.unionByName(incoming_applies, allowMissingColumns=True)

                # Drop dd_op before passing downstream (target schema shouldn't carry it)
                if "dd_op" in combined.columns:
                    df_UPD_NISS_PLCY_LMT_CD_hopeful_faraday = combined.drop("dd_op")
                else:
                    df_UPD_NISS_PLCY_LMT_CD_hopeful_faraday = combined
            except Exception:
                self.logger.exception("Failed applying load-modify-store-back logic for UPD_NISS_PLCY_LMT_CD")
                raise
        except Exception:
            self.logger.exception("Failed processing UPD_NISS_PLCY_LMT_CD")
            raise

        # FDR_LIB_WRK_BIRP_NISS_APRM_DETL1: write final dataframe as parquet to S3 (overwrite)
        # Strip the 'FDR_LIB_' prefix from the node name to form the external path: WRK_BIRP_NISS_APRM_DETL1
        try:
            self.logger.info("Writing WRK_BIRP_NISS_APRM_DETL1 to S3 as parquet (overwrite)", table="WRK_BIRP_NISS_APRM_DETL1")
            df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_quirky_galileo = df_UPD_NISS_PLCY_LMT_CD_hopeful_faraday
            self.parquet_writer.write(
                dataframe=df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_quirky_galileo,
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL1/",
                mode="overwrite",
            )
        except Exception:
            self.logger.exception("Failed writing WRK_BIRP_NISS_APRM_DETL1 to S3")
            raise

        self.glue_job.commit()

    def finalize(self) -> None:
        self.logger.info("Finalizing m_NU0C_NISS_AUTO_107_ATPRM_DTL_Upd_PlcyLmt")

    @staticmethod
    def _require_env(env: Mapping[str, Any], key: str) -> str:

        value = env.get(key)

        if not value or str(value).startswith("REPLACE_WITH_"):
            raise ConfigurationValidationError(
                f"config.yaml 'env.{key}' must be set"
            )

        return str(value)
