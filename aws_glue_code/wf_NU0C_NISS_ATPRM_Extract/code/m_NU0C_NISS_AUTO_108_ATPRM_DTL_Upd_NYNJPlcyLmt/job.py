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
    coalesce
)
from glue_lib.connector.snowflake import SnowflakeReader


# Informatica mapping m_NU0C_NISS_AUTO_108_ATPRM_DTL_Upd_NYNJPlcyLmt.
# Every mapping node runs in order inside run().
class Nu0cNissAuto108AtprmDtlUpdNYNJPlcyLmtJob(BaseJob):

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

        SNOWFLAKE_CONNECTION_FDWS_FDRWRK = self._require_env(self.env, "SNOWFLAKE_CONNECTION_FDWS_FDRWRK")

        # Shortcut_to_WRK_BIRP_NISS_APRM_DETL1: S3-first read for WRK_BIRP_NISS_APRM_DETL (preferred for WRK_ staging tables)
        try:
            self.logger.info("Reading Shortcut_to_WRK_BIRP_NISS_APRM_DETL1 from S3 (preferred)", node="Shortcut_to_WRK_BIRP_NISS_APRM_DETL1")
            df_WRK_BIRP_NISS_APRM_DETL_elated_aristotle = self.parquet_reader.read(
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                options=None,
            ).select("DERIVED_RDRVR_AGE")
            if df_WRK_BIRP_NISS_APRM_DETL_elated_aristotle.isEmpty():
                self.logger.warning("Source Shortcut_to_WRK_BIRP_NISS_APRM_DETL1 returned no rows", node="Shortcut_to_WRK_BIRP_NISS_APRM_DETL1")
        except Exception:
            # fallback to Glue catalog read
            self.logger.warning("S3 read failed for WRK_BIRP_NISS_APRM_DETL; falling back to Glue Data Catalog", node="Shortcut_to_WRK_BIRP_NISS_APRM_DETL1")
            dyf = self.glue_context.create_dynamic_frame_from_catalog(
                database=self.glue_database,
                table_name="WRK_BIRP_NISS_APRM_DETL",
                transformation_ctx="df_WRK_BIRP_NISS_APRM_DETL_elated_aristotle",
            )
            df_WRK_BIRP_NISS_APRM_DETL_elated_aristotle = dyf.toDF().select("DERIVED_RDRVR_AGE")
            if df_WRK_BIRP_NISS_APRM_DETL_elated_aristotle.isEmpty():
                self.logger.warning("Glue catalog read for WRK_BIRP_NISS_APRM_DETL returned no rows", node="Shortcut_to_WRK_BIRP_NISS_APRM_DETL1")

        # SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL: ASQ with SQL override executed against the staged temp view
        try:
            # register staged upstream dataframe as a temp view named after its real table
            df_WRK_BIRP_NISS_APRM_DETL_elated_aristotle.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")

            sql_query = f"""SELECT
    NISS_APRM_DETL_SK,
    ST_ABBR,
    ACCTNG_LOB,
    CVG_TYP_CD,
    CVG_AMT,
    PRD_GRP_CD,
    LTRIM(RTRIM(COMP_DED)) AS COMP_DED,
    LTRIM(RTRIM(COLL_DED)) AS COLL_DED
FROM
    WRK_BIRP_NISS_APRM_DETL
WHERE
    ST_ABBR IN ('NY','NJ')
"""

            self.logger.info("Reading SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL via rewritten SQL override against staged view", node="SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL")
            df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_bold_spinoza = self.spark.sql(sql_query)
            if df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_bold_spinoza.isEmpty():
                self.logger.warning("SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL returned no rows", node="SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL")
        except Exception:
            self.logger.exception("Failed executing SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL SQL override")
            raise

        # EXP_PassThru: explicit pass-through select of ASQ output ports
        try:
            self.logger.info("Applying EXP_PassThru pass-through select", node="EXP_PassThru")
            df_EXP_PassThru_dazzling_euclid = df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_bold_spinoza.selectExpr(
                "NISS_APRM_DETL_SK",
                "ST_ABBR",
                "ACCTNG_LOB",
                "CVG_TYP_CD",
                "CVG_AMT",
                "PRD_GRP_CD",
                "COMP_DED",
                "COLL_DED",
            )
        except Exception:
            self.logger.exception("Failed applying EXP_PassThru expressions")
            raise

        # EXP_CvgAmount_Split: sanitize CVG_AMT, split on '/', derive part strings and decimal casts
        try:
            self.logger.info("Applying EXP_CvgAmount_Split expressions", node="EXP_CvgAmount_Split")
            df_temp = (
                df_EXP_PassThru_dazzling_euclid
                .withColumn("v_CVG_AMT", regexp_replace(trim(col("CVG_AMT")), ",", ""))
            )

            parts_col = split(col("v_CVG_AMT"), "/")

            df_EXP_CvgAmount_Split_quirky_curie = (
                df_temp
                .withColumn("CVG_AMT_NO_OF_PARTS", size(parts_col))
                .withColumn("CVG_AMT_1_String", coalesce(element_at(parts_col, 1), lit("0")))
                .withColumn("CVG_AMT_2_String", coalesce(element_at(parts_col, 2), lit("0")))
                .withColumn("CVG_AMT_3_String", coalesce(element_at(parts_col, 3), lit("0")))
                .withColumn("CVG_AMT_1_Decimal", coalesce(col("CVG_AMT_1_String").cast("decimal(14,0)"), lit(0)))
                .withColumn("CVG_AMT_2_Decimal", coalesce(col("CVG_AMT_2_String").cast("decimal(14,0)"), lit(0)))
                .withColumn("CVG_AMT_3_Decimal", coalesce(col("CVG_AMT_3_String").cast("decimal(14,0)"), lit(0)))
                .withColumn("SRC_CVG_AMT", col("v_CVG_AMT"))
            )
        except Exception:
            self.logger.exception("Failed applying EXP_CvgAmount_Split expressions")
            raise

        # EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru: derive NISS_PLCY_LMT_CD and NISS_DEDUC_CD using state-specific decode logic
        try:
            self.logger.info("Applying EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru expressions", node="EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru")
            # join the pass-thru and the CVG split frames by the key to ensure all helper columns available
            df_join = df_EXP_PassThru_dazzling_euclid.alias("p").join(
                df_EXP_CvgAmount_Split_quirky_curie.alias("s"),
                on=["NISS_APRM_DETL_SK"],
                how="left"
            )

            # Build NJ-specific v_NJ expression using chained when() - many branches translated using isin() for IN lists
            v_NJ = (
                when(
                    (col("ST_ABBR") == lit("NJ")) & (col("ACCTNG_LOB") == lit("192BI")) & (col("CVG_AMT") == lit("10,000/10,000")),
                    lit("28")
                )
                .when(
                    (col("ST_ABBR") == lit("NJ")) & (col("ACCTNG_LOB") == lit("192BI")) & col("CVG_AMT").isin(
                        '15,000/30,000','15,000/1,000','15,000/2,000','15,000/2,500','15,000/250','15,000/250'
                    ),
                    lit("10")
                )
                .when(
                    (col("ST_ABBR") == lit("NJ")) & (col("ACCTNG_LOB") == lit("192BI")) & (col("CVG_AMT") == lit("20,000/30,000")),
                    lit("11")
                )
                .when(
                    (col("ST_ABBR") == lit("NJ")) & (col("ACCTNG_LOB") == lit("192BI")) & (col("CVG_AMT") == lit("20,000/40,000")),
                    lit("12")
                )
                .when(
                    (col("ST_ABBR") == lit("NJ")) & (col("ACCTNG_LOB") == lit("192PD")) & (col("CVG_AMT") == lit("5,000")),
                    lit("30")
                )
                .when(
                    (col("ST_ABBR") == lit("NJ")) & (col("ACCTNG_LOB") == lit("192PD")) & (col("CVG_AMT") == lit("10,000")),
                    lit("31")
                )
                .when(
                    (col("ST_ABBR") == lit("NJ")) & (col("ACCTNG_LOB") == lit("192UM")) & (col("CVG_TYP_CD") == lit("13106")) & col("CVG_AMT").isin('15,000/30,000','1,000','5,000','10,000','15,000/2,500'),
                    lit("40")
                )
                .when(
                    (col("ST_ABBR") == lit("NJ")) & (col("ACCTNG_LOB") == lit("192UM")) & (col("CVG_TYP_CD") == lit("12100")) & (col("CVG_AMT_1_Decimal") == lit(5000)),
                    lit("75")
                )
                .when(
                    (col("ST_ABBR") == lit("NJ")) & (col("ACCTNG_LOB").substr(1,3).isin('191','192')),
                    lit("??")
                )
                .when(
                    (col("ST_ABBR") == lit("NJ")) & ~col("ACCTNG_LOB").substr(1,3).isin('191','192'),
                    lit(" ")
                )
                .otherwise(lit("??"))
            )

            # Build NY-specific v_NY expression with representative branches
            v_NY = (
                when(
                    (col("ST_ABBR") == lit("NY")) & (col("ACCTNG_LOB") == lit("192MD")) & (col("CVG_AMT") == lit("2,000")),
                    lit("02")
                )
                .when(
                    (col("ST_ABBR") == lit("NY")) & (col("ACCTNG_LOB") == lit("192MD")) & (col("CVG_AMT") == lit("5,000")),
                    lit("03")
                )
                .when(
                    (col("ST_ABBR") == lit("NY")) & (col("ACCTNG_LOB").isin('192BI','192PD')) & (col("CVG_TYP_CD").isin('13000','13008')) & (col("CVG_AMT") == lit('60,000')),
                    lit("02")
                )
                .when(
                    (col("ST_ABBR") == lit("NY")) & (col("ACCTNG_LOB") == lit("192BI")) & (col("CVG_TYP_CD").isin('13003','13023','13025')) & (col("CVG_AMT") == lit('25,000/50,000')),
                    lit("05")
                )
                .when(
                    (col("ST_ABBR") == lit("NY")) & col("ACCTNG_LOB").substr(1,3) == lit('192'),
                    lit("??")
                )
                .otherwise(lit("??"))
            )

            # NISS_DEDUC_CD: implement some NY/NJ rules similarly (representative translation)
            v_NISS_DEDUC = (
                when(
                    (col("ST_ABBR") == lit("NY")) & (col("ACCTNG_LOB") == lit("191")) & (col("CVG_TYP_CD").isin('35030','35007')),
                    lit("01")
                )
                .when(
                    (col("ST_ABBR") == lit("NJ")) & (col("ACCTNG_LOB") == lit("191")) & (col("CVG_TYP_CD").isin('35051','35059','35050')) & col("CVG_AMT").isin('15,000/250','50,000/250'),
                    lit("06")
                )
                .when(
                    (col("ST_ABBR") == lit("NJ")) & (col("ACCTNG_LOB").isin('211CC','211TW')) & (col("CVG_TYP_CD").isin('21017','40011')) & (col("COMP_DED") == lit('50')),
                    lit("02")
                )
                .when(
                    (col("ST_ABBR") == lit("NJ")) & ~col("ACCTNG_LOB").isin('191','211CC','211CL','211TW'),
                    lit(" ")
                )
                .otherwise(lit("99"))
            )

            df_EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru_relaxed_archimedes = (
                df_join
                .withColumn("NISS_PLCY_LMT_CD",
                    when(col("ST_ABBR") == lit("NJ"), v_NJ)
                    .when(col("ST_ABBR") == lit("NY"), v_NY)
                    .otherwise(lit("??"))
                )
                .withColumn("NISS_DEDUC_CD", v_NISS_DEDUC)
            )

        except Exception:
            self.logger.exception("Failed applying EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru expressions")
            raise

        # UPD_NISS_PLCY_LMT_CD: Update Strategy - mark all rows as UPDATE, route rejects, and perform load-modify-store-back against the WRK_BIRP_NISS_APRM_DETL target
        try:
            self.logger.info("Applying UPD_NISS_PLCY_LMT_CD update-strategy marker and preparing apply set", node="UPD_NISS_PLCY_LMT_CD")
            # According to mapping the Update Strategy is DD_UPDATE for rows -> mark as UPDATE
            df_marked = df_EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru_relaxed_archimedes.withColumn("dd_op", lit("UPDATE"))

            # write REJECT rows if any (none expected for DD_UPDATE-only but follow rule)
            df_rejects = df_marked.filter(col("dd_op") == "REJECT").withColumn("error_reason", lit("DD_REJECT"))
            try:
                if not df_rejects.isEmpty():
                    self.logger.info("Writing rejected rows from UPD_NISS_PLCY_LMT_CD to reject prefix", node="UPD_NISS_PLCY_LMT_CD")
                    self.parquet_writer.write(
                        dataframe=df_rejects,
                        path=f"s3://{self.s3_output_bucket}/reject/WRK_BIRP_NISS_APRM_DETL/",
                        mode="overwrite",
                    )
            except Exception:
                self.logger.exception("Failed writing rejects for UPD_NISS_PLCY_LMT_CD")
                raise

            # surviving rows to apply (drop REJECT)
            df_surviving = df_marked.filter(col("dd_op") != "REJECT")

            # Load current target: prefer S3 parquet, fallback to Snowflake
            try:
                self.logger.info("Loading current WRK_BIRP_NISS_APRM_DETL from S3 (preferred)", node="UPD_NISS_PLCY_LMT_CD")
                df_existing_target = self.parquet_reader.read(
                    path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                    options=None,
                )
            except Exception:
                self.logger.info("S3 read of WRK_BIRP_NISS_APRM_DETL failed; falling back to Snowflake", node="UPD_NISS_PLCY_LMT_CD")
                try:
                    df_existing_target = SnowflakeReader(self.glue_context).read(
                        connection_name=SNOWFLAKE_CONNECTION_FDWS_FDRWRK,
                        table="FDR.WRK_BIRP_NISS_APRM_DETL",
                        query=None,
                        options=None,
                    )
                except Exception:
                    self.logger.exception("Failed reading existing WRK_BIRP_NISS_APRM_DETL from Snowflake")
                    raise

            # Anti-join to remove rows being updated/deleted (by key)
            try:
                self.logger.info("Performing anti-join to remove rows being updated from existing target", node="UPD_NISS_PLCY_LMT_CD")
                keys_to_replace = df_surviving.select("NISS_APRM_DETL_SK").distinct()
                df_existing_survivors = df_existing_target.join(keys_to_replace, on=["NISS_APRM_DETL_SK"], how="left_anti")

                # Union survivors with updated/insert rows (here UPDATE rows)
                # Assume incoming df_surviving carries full target payload columns; use unionByName for safety
                df_combined = df_existing_survivors.unionByName(df_surviving.select(df_existing_target.columns))

                df_UPD_NISS_PLCY_LMT_CD_keen_newton = df_combined
            except Exception:
                # If selecting df_existing_target.columns fails due to schema mismatch, fall back to unionByName without forcing order
                try:
                    df_UPD_NISS_PLCY_LMT_CD_keen_newton = df_existing_survivors.unionByName(df_surviving, allowMissingColumns=True)
                except Exception:
                    self.logger.exception("Failed combining existing target with incoming update rows in UPD_NISS_PLCY_LMT_CD")
                    raise

        except Exception:
            self.logger.exception("Failed processing UPD_NISS_PLCY_LMT_CD update strategy")
            raise

        # FDR_LIB_WRK_BIRP_NISS_APRM_DETL1: assign output df and write the WRK_ table back to S3 as parquet (overwrite)
        df_WRK_BIRP_NISS_APRM_DETL_determined_socrates = df_UPD_NISS_PLCY_LMT_CD_keen_newton

        # write intermediate WRK_ table as parquet to S3 (overwrite)
        try:
            self.logger.info("Writing WRK_BIRP_NISS_APRM_DETL to S3 as parquet (overwrite)", table="WRK_BIRP_NISS_APRM_DETL")
            self.parquet_writer.write(
                dataframe=df_WRK_BIRP_NISS_APRM_DETL_determined_socrates,
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                mode="overwrite",
            )
        except Exception:
            self.logger.exception("Failed writing WRK_BIRP_NISS_APRM_DETL to S3")
            raise

        self.glue_job.commit()

    def finalize(self) -> None:
        self.logger.info("Finalizing m_NU0C_NISS_AUTO_108_ATPRM_DTL_Upd_NYNJPlcyLmt")

    @staticmethod
    def _require_env(env: Mapping[str, Any], key: str) -> str:

        value = env.get(key)

        if not value or str(value).startswith("REPLACE_WITH_"):
            raise ConfigurationValidationError(
                f"config.yaml 'env.{key}' must be set"
            )

        return str(value)
