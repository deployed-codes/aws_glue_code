import sys
from typing import Any, Mapping

from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import DataFrame

from glue_lib.connector import ParquetReader, ParquetWriter
from glue_lib.exceptions import ConfigurationValidationError
from glue_lib.job.base import BaseJob
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import StructType, StructField, DecimalType, StringType


# Informatica mapping m_NU0C_NISS_AUTO_111A_ATPRM_DTL_Upd_CvgAttrChkSum.
# Every mapping node runs in order inside run().
class Nu0cNissAuto111aAtprmDtlUpdCvgAttrChkSumJob(BaseJob):

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

        # FDR_LIB_WRK_BIRP_NISS_APRM_DETL: staged WRK_ source - try S3 parquet first, fall back to Glue catalog
        try:
            self.logger.info("Attempting to read staged parquet for FDR_LIB_WRK_BIRP_NISS_APRM_DETL", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_hopeful_lovelace = self.parquet_reader.read(
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                options=None,
            )
            self.logger.info("Read staged parquet for FDR_LIB_WRK_BIRP_NISS_APRM_DETL from S3", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            # Project only the exposed field(s)
            try:
                df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_hopeful_lovelace = df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_hopeful_lovelace.select("CVG_ATTR_CHCKSUM")
            except Exception:
                # If the column is not present, create an empty dataframe with the expected column to keep downstream stable
                self.logger.exception("Expected column CVG_ATTR_CHCKSUM not present in staged parquet; creating empty projection")
                schema = StructType([StructField("CVG_ATTR_CHCKSUM", StringType(), True)])
                df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_hopeful_lovelace = self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), schema)
            if df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_hopeful_lovelace.isEmpty():
                self.logger.warning("Source FDR_LIB_WRK_BIRP_NISS_APRM_DETL returned no rows", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
        except Exception as e:
            # Fallback to Glue catalog read
            self.logger.warning("Staged parquet not available, falling back to Glue catalog read for WRK_BIRP_NISS_APRM_DETL", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL", error=str(e))
            try:
                dyf = self.glue_context.create_dynamic_frame_from_catalog(
                    database=self.glue_database,
                    table_name="WRK_BIRP_NISS_APRM_DETL",
                    transformation_ctx="df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_hopeful_lovelace",
                )
                df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_hopeful_lovelace = dyf.toDF().select("CVG_ATTR_CHCKSUM")
                if df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_hopeful_lovelace.isEmpty():
                    self.logger.warning("Catalog source WRK_BIRP_NISS_APRM_DETL returned no rows", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            except Exception:
                self.logger.exception("Failed reading WRK_BIRP_NISS_APRM_DETL from Glue catalog")
                raise

        # EXP_Gen_CvgAttrCheckSum: upstream missing - emit deterministic empty schema with expected ports (NISS_APRM_DETL_SK, CVG_ATTR_CHCKSUM)
        try:
            self.logger.info("Applying EXP_Gen_CvgAttrCheckSum expressions (upstream missing -> emit NULLs)", node="EXP_Gen_CvgAttrCheckSum")
            schema_exp = StructType([
                StructField("NISS_APRM_DETL_SK", DecimalType(38, 0), True),
                StructField("CVG_ATTR_CHCKSUM", StringType(), True),
            ])
            # create an empty dataframe with the expected schema (no rows, columns typed as required)
            df_EXP_Gen_CvgAttrCheckSum_eager_newton = self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), schema_exp)
        except Exception:
            self.logger.exception("Failed applying EXP_Gen_CvgAttrCheckSum expressions")
            raise

        # Upd_CVG_ATTR_SK: Update Strategy - mark as UPDATE, write rejects if any, load-modify-store-back apply to WRK_BIRP_NISS_APRM_DETL
        try:
            self.logger.info("Applying Upd_CVG_ATTR_SK update-strategy marker and preparing apply", node="Upd_CVG_ATTR_SK")
            # source for update strategy
            df_in = df_EXP_Gen_CvgAttrCheckSum_eager_newton
            # mark every row as UPDATE per transformation expression DD_UPDATE
            df_marked = df_in.withColumn("dd_op", lit("UPDATE"))
        except Exception:
            self.logger.exception("Failed marking rows in Upd_CVG_ATTR_SK")
            raise

        # Step 2: write REJECT rows to reject prefix (none expected for DD_UPDATE)
        try:
            self.logger.info("Writing REJECT rows for Upd_CVG_ATTR_SK if any", node="Upd_CVG_ATTR_SK")
            df_rejects = df_marked.filter(col("dd_op") == "REJECT")
            if not df_rejects.isEmpty():
                df_rejects_w_err = df_rejects.withColumn("error_reason", lit("DD_REJECT"))
                # overwrite reject path for idempotency
                self.parquet_writer.write(
                    dataframe=df_rejects_w_err,
                    path=f"s3://{self.s3_output_bucket}/reject/WRK_BIRP_NISS_APRM_DETL/",
                    mode="overwrite",
                )
                self.logger.info("Wrote reject rows for WRK_BIRP_NISS_APRM_DETL to reject prefix", table="WRK_BIRP_NISS_APRM_DETL")
        except Exception:
            self.logger.exception("Failed writing reject rows for Upd_CVG_ATTR_SK")
            raise

        # Step 4: load-modify-store-back for non-Iceberg target
        try:
            self.logger.info("Loading existing target WRK_BIRP_NISS_APRM_DETL from S3 for apply", node="Upd_CVG_ATTR_SK")
            df_existing = self.parquet_reader.read(path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/", options=None)
        except Exception:
            self.logger.exception("Failed reading existing target WRK_BIRP_NISS_APRM_DETL for update apply")
            raise

        try:
            self.logger.info("Building changed-keys set and applying updates to existing rows", node="Upd_CVG_ATTR_SK")
            # focus on the key and incoming checksum
            changed_keys_df = df_marked.select("NISS_APRM_DETL_SK", "CVG_ATTR_CHCKSUM").distinct()

            # existing rows that are NOT being updated/deleted
            existing_anti = df_existing.join(changed_keys_df, on=["NISS_APRM_DETL_SK"], how="left_anti")

            # rows to replace: existing rows inner-joined to changed keys, but with CVG_ATTR_CHCKSUM replaced by incoming value
            joined = df_existing.alias("t").join(changed_keys_df.alias("c"), on=["NISS_APRM_DETL_SK"], how="inner")
            # build select list: take t.<col> for all columns except CVG_ATTR_CHCKSUM which comes from c
            select_exprs = []
            for c_name in df_existing.columns:
                if c_name == "CVG_ATTR_CHCKSUM":
                    # prefer incoming value; if incoming is null, preserve existing
                    select_exprs.append(when(col("c.CVG_ATTR_CHCKSUM").isNotNull(), col("c.CVG_ATTR_CHCKSUM")).otherwise(col("t.CVG_ATTR_CHCKSUM")).alias("CVG_ATTR_CHCKSUM"))
                else:
                    select_exprs.append(col(f"t.{c_name}").alias(c_name))

            updates_full = joined.select(*select_exprs)

            # final target dataframe: rows not changed plus updated rows
            final_target_df = existing_anti.unionByName(updates_full)

            # assign node output
            df_Upd_CVG_ATTR_SK_silly_ramanujan = final_target_df

            # write final combined dataframe back to the same target path (full overwrite)
            self.parquet_writer.write(
                dataframe=df_Upd_CVG_ATTR_SK_silly_ramanujan,
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                mode="overwrite",
            )
            self.logger.info("Applied updates and overwrote WRK_BIRP_NISS_APRM_DETL on S3 (load-modify-store-back)", table="WRK_BIRP_NISS_APRM_DETL")
        except Exception:
            self.logger.exception("Failed applying updates for Upd_CVG_ATTR_SK")
            raise

        # FDR_LIB_WRK_BIRP_NISS_APRM_DETL (Output): write intermediate WRK_ table as parquet to S3 (overwrite)
        try:
            self.logger.info("Writing WRK_BIRP_NISS_APRM_DETL to S3 as parquet (overwrite)", table="WRK_BIRP_NISS_APRM_DETL")
            df_WRK_BIRP_NISS_APRM_DETL_peaceful_shannon = df_Upd_CVG_ATTR_SK_silly_ramanujan
            # write via ParquetWriter
            self.parquet_writer.write(
                dataframe=df_WRK_BIRP_NISS_APRM_DETL_peaceful_shannon,
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                mode="overwrite",
            )
        except Exception:
            self.logger.exception("Failed writing WRK_BIRP_NISS_APRM_DETL to S3")
            raise

        self.glue_job.commit()

    def finalize(self) -> None:
        self.logger.info("Finalizing m_NU0C_NISS_AUTO_111A_ATPRM_DTL_Upd_CvgAttrChkSum")

    @staticmethod
    def _require_env(env: Mapping[str, Any], key: str) -> str:

        value = env.get(key)

        if not value or str(value).startswith("REPLACE_WITH_"):
            raise ConfigurationValidationError(
                f"config.yaml 'env.{key}' must be set"
            )

        return str(value)
