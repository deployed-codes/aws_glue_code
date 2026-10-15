import sys
from typing import Any, Mapping

from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import DataFrame

from glue_lib.connector import ParquetReader, ParquetWriter
from glue_lib.exceptions import ConfigurationValidationError
from glue_lib.job.base import BaseJob
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit
from glue_lib.connector.snowflake import SnowflakeReader


# Informatica mapping m_NU0C_NISS_AUTO_113_ATPRM_DTL_Upd_EXPS_VAL_ROLLED.
# Every mapping node runs in order inside run().
class Nu0cNissAuto113AtprmDtlUpdExpsValRolledJob(BaseJob):

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

        # FDR_LIB_WRK_BIRP_NISS_APRM_DETL: Source (WRK_ staged table) - try S3 parquet first, fall back to Glue catalog
        try:
            self.logger.info("Attempting to read staged parquet for FDR_LIB_WRK_BIRP_NISS_APRM_DETL (S3-first)", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            df_WRK_BIRP_NISS_APRM_DETL_eager_ramanujan = self.parquet_reader.read(
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                options=None,
            ).select("CVG_ATTR_SK")
            # note: S3 branch taken
            self.logger.info("Read FDR_LIB_WRK_BIRP_NISS_APRM_DETL from S3 staged path", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            if df_WRK_BIRP_NISS_APRM_DETL_eager_ramanujan.isEmpty():
                self.logger.warning("Source FDR_LIB_WRK_BIRP_NISS_APRM_DETL returned no rows", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
        except Exception:
            # fallback to Glue Data Catalog
            self.logger.warning("Falling back to Glue Data Catalog read for FDR_LIB_WRK_BIRP_NISS_APRM_DETL", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            try:
                dyf = self.glue_context.create_dynamic_frame.from_catalog(
                    database=self.glue_database,
                    table_name="WRK_BIRP_NISS_APRM_DETL",
                    transformation_ctx="df_WRK_BIRP_NISS_APRM_DETL_eager_ramanujan",
                )
                df_WRK_BIRP_NISS_APRM_DETL_eager_ramanujan = dyf.toDF().select("CVG_ATTR_SK")
                if df_WRK_BIRP_NISS_APRM_DETL_eager_ramanujan.isEmpty():
                    self.logger.warning("Source FDR_LIB_WRK_BIRP_NISS_APRM_DETL (catalog) returned no rows", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            except Exception:
                self.logger.exception("Failed reading FDR_LIB_WRK_BIRP_NISS_APRM_DETL from Glue catalog")
                raise

        # SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL: Application Source Qualifier with SQL override against staged temp view
        # register upstream staged dataframe as a temp view named exactly WRK_BIRP_NISS_APRM_DETL
        try:
            self.logger.info("Registering staged source dataframe as temp view for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL", node="SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            df_WRK_BIRP_NISS_APRM_DETL_eager_ramanujan.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")
        except Exception:
            self.logger.exception("Failed registering temp view for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            raise

        sql_query = f"""SELECT
CVG_ATTR_SK,
CASE WHEN CVG_TYP_IND='B' THEN '?-?-?' ELSE CVG_TYP_CD END CVG_TYP_CD,
CVG_TYP_IND,
CASE WHEN CVG_TYP_IND='B' THEN 1 ELSE 99 END CVG_TYP_Priority,
SUM(CVG_EXPS_VAL) CVG_EXPS_VAL

FROM WRK_BIRP_NISS_APRM_DETL
WHERE 1=1
--AND CVG_ATTR_SK IN (580,581,582,583)
AND SOURCE_IND_DERIVED!='TOGGLE AUTO'

GROUP BY
CVG_ATTR_SK,
CASE WHEN CVG_TYP_IND='B' THEN '?-?-?' ELSE CVG_TYP_CD END,
CVG_TYP_IND,
CASE WHEN CVG_TYP_IND='B' THEN 1 ELSE 99 END

ORDER BY CVG_ATTR_SK,CVG_TYP_Priority,CVG_EXPS_VAL DESC"""

        try:
            self.logger.info("Executing SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL SQL override via spark.sql", node="SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_pensive_pascal = self.spark.sql(sql_query).select(
                "CVG_ATTR_SK",
                "CVG_TYP_CD",
                "CVG_TYP_IND",
                "CVG_TYP_Priority",
                "CVG_EXPS_VAL",
            )
            if df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_pensive_pascal.isEmpty():
                self.logger.warning("Source SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL returned no rows", node="SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
        except Exception:
            self.logger.exception("Failed executing SQL override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            raise

        # EXP_Passthru: pass through declared ports explicitly
        try:
            self.logger.info("Applying EXP_Passthru pass-through projection", node="EXP_Passthru")
            df_EXP_Passthru_peaceful_kepler = df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_pensive_pascal.selectExpr(
                "CVG_ATTR_SK",
                "CVG_TYP_CD",
                "CVG_TYP_IND",
                "CVG_TYP_Priority",
                "CVG_EXPS_VAL",
            )
        except Exception:
            self.logger.exception("Failed applying EXP_Passthru expressions")
            raise

        # AGG_RetainFirstValueOnly: group by CVG_ATTR_SK, take FIRST(CVG_EXPS_VAL)
        try:
            self.logger.info("Applying AGG_RetainFirstValueOnly aggregation (FIRST CVG_EXPS_VAL)", node="AGG_RetainFirstValueOnly")
            df_AGG_RetainFirstValueOnly_eager_planck = (
                df_EXP_Passthru_peaceful_kepler
                .select("CVG_ATTR_SK", "CVG_EXPS_VAL")
                .groupBy("CVG_ATTR_SK")
                .agg(F.first("CVG_EXPS_VAL").alias("CVG_EXPS_VAL"))
            )
        except Exception:
            self.logger.exception("Failed performing AGG_RetainFirstValueOnly aggregation")
            raise

        # EXP_PassThrough: compute v_CVG_EXPS_VAL = ROUND(CVG_EXPS_VAL * 12) then EXPS_VAL_ROLLED = v_CVG_EXPS_VAL
        try:
            self.logger.info("Applying EXP_PassThrough computations (roll exposure)", node="EXP_PassThrough")
            df_EXP_PassThrough_gentle_heisenberg = (
                df_AGG_RetainFirstValueOnly_eager_planck
                .withColumn("v_CVG_EXPS_VAL", F.round(F.col("CVG_EXPS_VAL") * F.lit(12)))
                .withColumn("EXPS_VAL_ROLLED", F.col("v_CVG_EXPS_VAL"))
                .select("CVG_ATTR_SK", "EXPS_VAL_ROLLED")
            )
        except Exception:
            self.logger.exception("Failed applying EXP_PassThrough expressions")
            raise

        # EXP_Passthru_Tgt: pass to target ports; lookup input missing => NISS_APRM_DETL_SK replaced with NULL
        try:
            self.logger.info("Applying EXP_Passthru_Tgt to prepare target-ready columns (missing lookup replaced by NULL)", node="EXP_Passthru_Tgt")
            df_EXP_Passthru_Tgt_amazing_leibniz = (
                df_EXP_PassThrough_gentle_heisenberg
                .withColumn("NISS_APRM_DETL_SK", lit(None).cast("long"))
                .withColumn("EXPS_VAL_ROLLED", col("EXPS_VAL_ROLLED"))
                .select("NISS_APRM_DETL_SK", "EXPS_VAL_ROLLED")
            )
            self.logger.info("Note: lookup input df_LKP_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_ByCvgAttrSk was missing in mapping export; NISS_APRM_DETL_SK set to NULL", node="EXP_Passthru_Tgt")
        except Exception:
            self.logger.exception("Failed applying EXP_Passthru_Tgt expressions")
            raise

        # Upd_CVG_ATTR_SK: Update Strategy - mark rows as UPDATE, write rejects, load-modify-store-back against WRK_BIRP_NISS_APRM_DETL
        try:
            self.logger.info("Applying Upd_CVG_ATTR_SK update-strategy marker (DD_UPDATE)", node="Upd_CVG_ATTR_SK")
            df_marked = df_EXP_Passthru_Tgt_amazing_leibniz.withColumn("dd_op", lit("UPDATE"))

            # write REJECT rows if any (none expected for DD_UPDATE, but handle generically)
            df_rejects = df_marked.filter(col("dd_op") == "REJECT")
            try:
                if not df_rejects.isEmpty():
                    df_rejects_out = df_rejects.withColumn("error_reason", lit("DD_REJECT"))
                    self.logger.info("Writing reject rows for Upd_CVG_ATTR_SK to reject prefix", node="Upd_CVG_ATTR_SK")
                    self.parquet_writer.write(
                        dataframe=df_rejects_out,
                        path=f"s3://{self.s3_output_bucket}/reject/WRK_BIRP_NISS_APRM_DETL/",
                        mode="overwrite",
                    )
            except Exception:
                self.logger.exception("Failed writing reject rows for Upd_CVG_ATTR_SK")
                raise

            # drop rejects from apply
            df_to_apply = df_marked.filter(col("dd_op") != "REJECT")

            # load current full target (S3-first), fall back to Snowflake
            try:
                self.logger.info("Loading existing target WRK_BIRP_NISS_APRM_DETL from S3 (parquet)", node="Upd_CVG_ATTR_SK")
                existing_target_df = self.parquet_reader.read(
                    path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                    options=None,
                )
                self.logger.info("Loaded existing target from S3 for Upd_CVG_ATTR_SK", node="Upd_CVG_ATTR_SK")
            except Exception:
                self.logger.warning("S3 parquet read for existing target failed; falling back to Snowflake for WRK_BIRP_NISS_APRM_DETL", node="Upd_CVG_ATTR_SK")
                try:
                    SNOWFLAKE_CONNECTION_FDWS_FDRWRK = self._require_env(self.env, "SNOWFLAKE_CONNECTION_FDWS_FDRWRK")
                    existing_target_df = SnowflakeReader(self.glue_context).read(
                        connection_name=SNOWFLAKE_CONNECTION_FDWS_FDRWRK,
                        table="FDR.WRK_BIRP_NISS_APRM_DETL",
                        query=None,
                        options=None,
                    )
                    self.logger.info("Loaded existing target from Snowflake for Upd_CVG_ATTR_SK", node="Upd_CVG_ATTR_SK")
                except Exception:
                    self.logger.exception("Failed loading existing target for Upd_CVG_ATTR_SK from Snowflake")
                    raise

            # apply changes: for UPDATE/INSERT, build changed rows and replace matching existing rows
            try:
                # changed rows (INSERT or UPDATE)
                changed = df_to_apply.filter(col("dd_op").isin("INSERT", "UPDATE")).select("NISS_APRM_DETL_SK", "EXPS_VAL_ROLLED")

                # surviving existing rows (exclude rows with keys being changed)
                surviving_existing = existing_target_df.join(changed.select("NISS_APRM_DETL_SK"), on="NISS_APRM_DETL_SK", how="left_anti")

                # build updated rows by joining existing rows to changed to get full schema then overwrite EXPS_VAL_ROLLED
                # inner join ensures we only update rows that exist in the current target
                updated_rows_join = existing_target_df.alias("t").join(changed.alias("c"), on="NISS_APRM_DETL_SK", how="inner")
                # construct column list: take all columns from existing target, but replace EXPS_VAL_ROLLED with the value from changed
                existing_cols = existing_target_df.columns
                select_exprs = []
                for c_name in existing_cols:
                    if c_name == "EXPS_VAL_ROLLED":
                        select_exprs.append(F.col("c.EXPS_VAL_ROLLED").alias("EXPS_VAL_ROLLED"))
                    else:
                        select_exprs.append(F.col(f"t.{c_name}").alias(c_name))
                updated_rows = updated_rows_join.select(*select_exprs)

                # Combined dataframe = surviving_existing UNION updated_rows
                df_Upd_CVG_ATTR_SK_modest_curie = surviving_existing.unionByName(updated_rows)

            except Exception:
                self.logger.exception("Failed applying load-modify-store-back logic in Upd_CVG_ATTR_SK")
                raise

        except Exception:
            self.logger.exception("Failed applying Upd_CVG_ATTR_SK update-strategy")
            raise

        # FDR_LIB_WRK_BIRP_NISS_APRM_DETL: write final WRK_BIRP_NISS_APRM_DETL to S3 as parquet (overwrite)
        try:
            self.logger.info("Writing WRK_BIRP_NISS_APRM_DETL to S3 as parquet (overwrite)", table="WRK_BIRP_NISS_APRM_DETL")
            self.parquet_writer.write(
                dataframe=df_Upd_CVG_ATTR_SK_modest_curie,
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                mode="overwrite",
                partition_by=None,
                options=None,
            )
        except Exception:
            self.logger.exception("Failed writing WRK_BIRP_NISS_APRM_DETL to S3")
            raise

        # expose the final output dataframe variable name expected by downstream consumers
        df_WRK_BIRP_NISS_APRM_DETL_modest_bohr = df_Upd_CVG_ATTR_SK_modest_curie

        self.glue_job.commit()

    def finalize(self) -> None:
        self.logger.info("Finalizing m_NU0C_NISS_AUTO_113_ATPRM_DTL_Upd_EXPS_VAL_ROLLED")

    @staticmethod
    def _require_env(env: Mapping[str, Any], key: str) -> str:

        value = env.get(key)

        if not value or str(value).startswith("REPLACE_WITH_"):
            raise ConfigurationValidationError(
                f"config.yaml 'env.{key}' must be set"
            )

        return str(value)
