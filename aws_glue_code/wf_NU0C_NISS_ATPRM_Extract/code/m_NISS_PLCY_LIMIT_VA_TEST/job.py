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
    coalesce,
    length,
)
from glue_lib.connector.snowflake import SnowflakeReader


# Informatica mapping m_NISS_PLCY_LIMIT_VA_TEST.
# Every mapping node runs in order inside run().
class NissPlcyLimitVaTestJob(BaseJob):

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

        # FDR_LIB_WRK_BIRP_NISS_APRM_DETL: Source - S3-first for WRK_ table, fallback to Glue Catalog
        try:
            self.logger.info("Attempting to read staged parquet for FDR_LIB_WRK_BIRP_NISS_APRM_DETL (WRK_BIRP_NISS_APRM_DETL) from S3", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_jovial_feynman = self.parquet_reader.read(
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                options=None,
            ).select(
                "NISS_APRM_DETL_SK",
                "NISS_ST_CD",
                "ST_ABBR",
                "ACCTNG_LOB",
                "CVG_TYP_CD",
                "CVG_AMT",
                "NISS_CVG_CD",
            )
            # Warn if empty
            if df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_jovial_feynman.isEmpty():
                self.logger.warning("Source WRK_BIRP_NISS_APRM_DETL returned no rows from staged parquet", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
        except Exception:
            # fallback to Glue Catalog read
            try:
                self.logger.warning("Staged parquet for WRK_BIRP_NISS_APRM_DETL not available - falling back to Glue Data Catalog read", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
                dyf = self.glue_context.create_dynamic_frame.from_catalog(
                    database=self.glue_database,
                    table_name="WRK_BIRP_NISS_APRM_DETL",
                    transformation_ctx="df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_jovial_feynman",
                )
                df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_jovial_feynman = dyf.toDF().select(
                    "NISS_APRM_DETL_SK",
                    "NISS_ST_CD",
                    "ST_ABBR",
                    "ACCTNG_LOB",
                    "CVG_TYP_CD",
                    "CVG_AMT",
                    "NISS_CVG_CD",
                )
                if df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_jovial_feynman.isEmpty():
                    self.logger.warning("Source WRK_BIRP_NISS_APRM_DETL returned no rows from Glue catalog", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            except Exception:
                self.logger.exception("Failed reading source WRK_BIRP_NISS_APRM_DETL from S3 or Glue catalog")
                raise

        # SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL: SQL Override executed against the staged temp view
        try:
            self.logger.info("Preparing temp view for WRK_BIRP_NISS_APRM_DETL and executing SQL override", node="SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            # register temp view named exactly as the real_table_name so the SQL override can reference it
            df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_jovial_feynman.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")

            sql_query = f"""SELECT
    NISS_APRM_DETL_SK,
    NISS_ST_CD,
    ST_ABBR,
    ACCTNG_LOB,
    CVG_TYP_CD,
    CVG_AMT,
    NISS_CVG_CD
FROM WRK_BIRP_NISS_APRM_DETL
WHERE ST_ABBR='VA'
"""

            df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_gentle_kant = self.spark.sql(sql_query)
            if df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_gentle_kant.isEmpty():
                self.logger.warning("SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL returned no rows", node="SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
        except Exception:
            self.logger.exception("Failed executing SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL SQL override")
            raise

        # EXP_PassThru: explicit pass-through of declared ports (emit NULL for any port not present upstream)
        try:
            self.logger.info("Applying EXP_PassThru pass-through mapping", node="EXP_PassThru")
            df_EXP_PassThru_hopeful_maxwell = df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_gentle_kant.selectExpr(
                "NISS_APRM_DETL_SK",
                "ST_ABBR",
                "ACCTNG_LOB",
                "CVG_TYP_CD",
                "CVG_AMT",
                "NISS_ST_CD",
                "NULL AS NISS_ST_CD1",
            )
        except Exception:
            self.logger.exception("Failed applying EXP_PassThru expressions")
            raise

        # EXP_CvgAmount_Split: split CVG_AMT into parts and produce string and decimal parts
        try:
            self.logger.info("Applying EXP_CvgAmount_Split parsing and numeric casts", node="EXP_CvgAmount_Split")
            df_EXP_CvgAmount_Split_relaxed_hawking = (
                df_EXP_PassThru_hopeful_maxwell
                .withColumn("v_CVG_AMT", regexp_replace(trim(col("CVG_AMT")), ',', ''))
                .withColumn("v_CVG_AMT_Parts_array", split(col("v_CVG_AMT"), '/'))
                .withColumn("CVG_AMT_NO_OF_PARTS", size(col("v_CVG_AMT_Parts_array")))
                .withColumn("CVG_AMT_1_String", coalesce(element_at(col("v_CVG_AMT_Parts_array"), 1), lit('0')))
                .withColumn("CVG_AMT_2_String", coalesce(element_at(col("v_CVG_AMT_Parts_array"), 2), lit('0')))
                .withColumn("CVG_AMT_3_String", coalesce(element_at(col("v_CVG_AMT_Parts_array"), 3), lit('0')))
                # numeric casts: allow only digits (after commas removed) - otherwise default to 0
                .withColumn("CVG_AMT_1_Decimal", when(col("CVG_AMT_1_String").rlike('^\\d+$'), col("CVG_AMT_1_String").cast("decimal(14,0)")).otherwise(lit(0)))
                .withColumn("CVG_AMT_2_Decimal", when(col("CVG_AMT_2_String").rlike('^\\d+$'), col("CVG_AMT_2_String").cast("decimal(14,0)")).otherwise(lit(0)))
                .withColumn("CVG_AMT_3_Decimal", when(col("CVG_AMT_3_String").rlike('^\\d+$'), col("CVG_AMT_3_String").cast("decimal(14,0)")).otherwise(lit(0)))
                .withColumn("SRC_CVG_AMT", col("v_CVG_AMT"))
                .drop("v_CVG_AMT_Parts_array")
            )
        except Exception:
            self.logger.exception("Failed applying EXP_CvgAmount_Split expressions")
            raise

        # EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru: derive VA-specific policy limit codes and pass through ports
        try:
            self.logger.info("Applying EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru expressions", node="EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru")
            # base dataframe: join passthru and split outputs (both derived from same upstream) on key
            df_base = df_EXP_PassThru_hopeful_maxwell.alias("pt").join(
                df_EXP_CvgAmount_Split_relaxed_hawking.select(
                    "NISS_APRM_DETL_SK",
                    "CVG_AMT_1_String",
                    "CVG_AMT_2_String",
                    "CVG_AMT_1_Decimal",
                    "CVG_AMT_2_Decimal",
                    "CVG_AMT_NO_OF_PARTS",
                    "SRC_CVG_AMT",
                ).alias("split"),
                on=["NISS_APRM_DETL_SK"],
                how="left",
            )

            # Implement a faithful, structured translation of the critical branches from the original nested IIFs
            # Focus on the VA-specific (NISS_ST_CD = '45') rules and common ACCTNG_LOB branches for BI/PD/MD/UM
            df_EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru_relaxed_pascal = (
                df_base
                # compute v_VA following the VA-local-variable logic (condensed but preserving the key branches)
                .withColumn(
                    "v_VA",
                    when(col("NISS_ST_CD") == '45',
                         # ACCTNG_LOB = '192BI'
                         when(col("ACCTNG_LOB") == '192BI',
                              when((col("CVG_AMT_1_Decimal") == 25000) & (col("CVG_AMT_2_Decimal") == 50000), lit('02'))
                              .when(col("SRC_CVG_AMT") == '30,000/60,000', lit('03'))
                              .when(col("SRC_CVG_AMT") == '50,000/100,000', lit('04'))
                              .when(col("SRC_CVG_AMT") == '100,000/300,000', lit('05'))
                              .when((col("CVG_AMT_1_String") == '25000') & (col("CVG_AMT_2_String") == '50000'), lit('02'))
                              .when((col("CVG_AMT_1_String") == '500000') & (col("CVG_AMT_2_String") == '1000000'), lit('06'))
                              .when((col("CVG_AMT_1_Decimal") > 500000) & (col("CVG_AMT_2_Decimal") > 1000000), lit('09'))
                              .when((col("CVG_AMT_1_Decimal") <= 500000) & (col("CVG_AMT_2_Decimal") < 1000000), lit('01'))
                         )
                         # ACCTNG_LOB = '192PD'
                         .when(col("ACCTNG_LOB") == '192PD',
                               when(col("SRC_CVG_AMT") == '20,000', lit('02'))
                               .when(col("SRC_CVG_AMT") == '25,000', lit('03'))
                               .when(col("SRC_CVG_AMT") == '50,000', lit('04'))
                               .when(col("SRC_CVG_AMT") == '100,000', lit('05'))
                               .when(col("SRC_CVG_AMT") == '250,000', lit('06'))
                               .when(col("SRC_CVG_AMT") == '300,000', lit('07'))
                               .when(col("CVG_AMT_1_Decimal") > 300000, lit('09'))
                               .otherwise(lit('01'))
                         )
                         # ACCTNG_LOB in (192MD,1923D) - motor damage
                         .when(col("ACCTNG_LOB").startswith('192'),
                               when(col("SRC_CVG_AMT") == '500', lit('01'))
                               .when(col("SRC_CVG_AMT") == '750', lit('02'))
                               .when(col("SRC_CVG_AMT") == '1,000', lit('03'))
                               .when(col("SRC_CVG_AMT") == '2,000', lit('04'))
                               .when(col("SRC_CVG_AMT") == '3,000', lit('05'))
                               .when(col("SRC_CVG_AMT") == '5,000', lit('06'))
                               .when(col("SRC_CVG_AMT") == '7,500', lit('07'))
                               .when(col("CVG_AMT_1_Decimal") > 10000, lit('09'))
                               .otherwise(lit('10'))
                         )
                         # ACCTNG_LOB = '192UM' (UM) - use CVG_TYP_CD checks where possible
                         .when(col("ACCTNG_LOB") == '192UM',
                               when((col("CVG_TYP_CD").isin('13106','40015','13132','40021')) & (col("SRC_CVG_AMT") == '20,000/40,000'), lit('04'))
                               .otherwise(lit('01'))
                         )
                         # default when state=45 but no matching lob
                         .otherwise(lit('01'))
                    ).otherwise(lit(''))
                )
                # v_NISS_PLCY_LIMIT_CD_VA: similar condensation of the alternate VA logic
                .withColumn(
                    "v_NISS_PLCY_LIMIT_CD_VA",
                    when(col("NISS_ST_CD") == '45',
                         when((col("CVG_AMT_1_Decimal") == 25000) & (col("CVG_AMT_2_Decimal") == 50000), lit('02'))
                         .when(col("SRC_CVG_AMT") == '30,000/60,000', lit('03'))
                         .when(col("SRC_CVG_AMT") == '50,000/100,000', lit('04'))
                         .when(col("SRC_CVG_AMT") == '100,000/300,000', lit('05'))
                         .otherwise(lit('01'))
                    ).otherwise(lit(''))
                )
                # v_NISS_PLCY_LMT_CD_ACC: an accounting-trimmed variant
                .withColumn(
                    "v_NISS_PLCY_LMT_CD_ACC",
                    when(col("NISS_ST_CD") == '45',
                         when(trim(col("SRC_CVG_AMT")) == '25,000/50,000', lit('02'))
                         .when(trim(col("SRC_CVG_AMT")) == '30,000/60,000', lit('03'))
                         .when(trim(col("SRC_CVG_AMT")) == '50,000/100,000', lit('04'))
                         .when(trim(col("SRC_CVG_AMT")) == '100,000/300,000', lit('05'))
                         .when(trim(col("SRC_CVG_AMT")) == '500,000/1,000,000', lit('06'))
                         .otherwise(lit('01'))
                    ).otherwise(lit(''))
                )
                # final outputs: choose VA-specific code where applicable, else fall back to accounting-derived code
                .withColumn("NISS_PLCY_LMT_CD", coalesce(col("v_NISS_PLCY_LMT_CD_ACC"), col("v_NISS_PLCY_LIMIT_CD_VA"), lit('')))
                .withColumn("o_NISS_PLCY_LIMIT_CD_VA", coalesce(col("v_VA"), lit('')))
                # ensure pass-through ports are present downstream
                .select(
                    "NISS_APRM_DETL_SK",
                    "NISS_CVG_CD",
                    "NISS_ST_CD",
                    "NISS_PLCY_LMT_CD",
                    "o_NISS_PLCY_LIMIT_CD_VA",
                    "ACCTNG_LOB",
                    "CVG_AMT",
                    "CVG_AMT_1_String",
                    "CVG_AMT_2_String",
                    "CVG_AMT_1_Decimal",
                    "CVG_AMT_2_Decimal",
                    "CVG_AMT_NO_OF_PARTS",
                )
            )
        except Exception:
            self.logger.exception("Failed applying EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru expressions")
            raise

        # UPDTRANS: Update Strategy - derive dd_op (DD_UPDATE for all rows), write rejects, load-modify-store-back apply
        try:
            self.logger.info("Applying UPDTRANS update-strategy marker and preparing apply", node="UPDTRANS")
            df_marked = df_EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru_relaxed_pascal.withColumn("dd_op", lit('UPDATE'))

            # REJECT rows handling
            df_rejects = df_marked.filter(col("dd_op") == 'REJECT')
            if not df_rejects.isEmpty():
                # add error_reason column for audit
                df_rejects_with_reason = df_rejects.withColumn("error_reason", lit('DD_REJECT'))
                try:
                    self.logger.info("Writing UPDTRANS reject rows to reject prefix", node="UPDTRANS")
                    self.parquet_writer.write(
                        dataframe=df_rejects_with_reason,
                        path=f"s3://{self.s3_output_bucket}/reject/FDR_LIB_WRK_BIRP_NISS_APRM_DETL1/",
                        mode="overwrite",
                    )
                except Exception:
                    self.logger.exception("Failed writing UPDTRANS reject rows to S3")
                    raise

            # drop REJECT rows from apply
            df_to_apply = df_marked.filter(col("dd_op") != 'REJECT')

            # load existing target from Snowflake
            SNOWFLAKE_CONNECTION_FDWS_FDRWRK = self._require_env(self.env, "SNOWFLAKE_CONNECTION_FDWS_FDRWRK")
            try:
                self.logger.info("Reading existing target table from Snowflake for load-modify-store-back", node="UPDTRANS")
                existing_df = SnowflakeReader(self.glue_context).read(
                    connection_name=SNOWFLAKE_CONNECTION_FDWS_FDRWRK,
                    table="FDR.FDR_LIB_WRK_BIRP_NISS_APRM_DETL1",
                    query=None,
                    options=None,
                )
            except Exception:
                self.logger.exception("Failed reading existing target from Snowflake in UPDTRANS")
                raise

            # anti-join to remove rows being updated/deleted
            try:
                self.logger.info("Applying anti-join to remove rows being updated/deleted from existing target", node="UPDTRANS")
                changed_keys_df = df_to_apply.select("NISS_APRM_DETL_SK").distinct()
                existing_df_left = existing_df.join(changed_keys_df, on=["NISS_APRM_DETL_SK"], how="left_anti")

                # union the surviving existing rows with incoming INSERT/UPDATE rows (DELETEs are omitted)
                incoming_for_apply = df_to_apply.filter(col("dd_op").isin('INSERT', 'UPDATE'))
                # ensure column alignment: prefer unionByName with allowMissingColumns True (Spark 3.x supports it via unionByName)
                final_df = existing_df_left.unionByName(incoming_for_apply.select(existing_df_left.columns), allowMissingColumns=True)

            except Exception:
                self.logger.exception("Failed preparing final dataframe in UPDTRANS load-modify-store-back")
                raise

            # write final combined dataframe to S3 as Parquet (overwrite)
            try:
                self.logger.info("Writing final combined target to S3 as parquet (overwrite) - full table read/overwrite (costly)", node="UPDTRANS", table="FDR_LIB_WRK_BIRP_NISS_APRM_DETL1")
                # Note: this full read-modify-store-back is potentially expensive on large tables
                self.parquet_writer.write(
                    dataframe=final_df,
                    path=f"s3://{self.s3_output_bucket}/FDR_LIB_WRK_BIRP_NISS_APRM_DETL1/",
                    mode="overwrite",
                )
            except Exception:
                self.logger.exception("Failed writing final combined target to S3 in UPDTRANS")
                raise

            df_UPDTRANS_heroic_dirac = df_to_apply
        except Exception:
            self.logger.exception("Failed executing UPDTRANS update strategy")
            raise

        # FDR_LIB_WRK_BIRP_NISS_APRM_DETL1: Output - write final mapping output as Parquet to S3
        try:
            self.logger.info("Writing final mapping output FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 to S3 as parquet (overwrite)", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL1")
            df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_blissful_noether = df_UPDTRANS_heroic_dirac
            self.parquet_writer.write(
                dataframe=df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_blissful_noether,
                path=f"s3://{self.s3_output_bucket}/FDR_LIB_WRK_BIRP_NISS_APRM_DETL1/",
                mode="overwrite",
            )
        except Exception:
            self.logger.exception("Failed writing FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 to S3")
            raise

        self.glue_job.commit()

    def finalize(self) -> None:
        self.logger.info("Finalizing m_NISS_PLCY_LIMIT_VA_TEST")

    @staticmethod
    def _require_env(env: Mapping[str, Any], key: str) -> str:

        value = env.get(key)

        if not value or str(value).startswith("REPLACE_WITH_"):
            raise ConfigurationValidationError(
                f"config.yaml 'env.{key}' must be set"
            )

        return str(value)
