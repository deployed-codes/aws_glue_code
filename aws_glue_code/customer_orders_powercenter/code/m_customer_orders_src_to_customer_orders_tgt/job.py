import sys
from typing import Any, Mapping

from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import DataFrame

from glue_lib.connector import ParquetReader, ParquetWriter
from glue_lib.exceptions import ConfigurationValidationError
from glue_lib.job.base import BaseJob
from pyspark.sql.functions import col, trim, upper, lit, length


# Informatica mapping m_customer_orders_src_to_customer_orders_tgt.
# Every mapping node runs in order inside run().
class CustomerOrdersSrcToCustomerOrdersTgtJob(BaseJob):

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

        # SQ_customer_orders_src: read from Glue Data Catalog (project only declared fields)
        try:
            self.logger.info("Reading SQ_customer_orders_src from Glue Data Catalog", node="SQ_customer_orders_src")
            dyf_SQ_customer_orders_src_modest_shannon = self.glue_context.create_dynamic_frame.from_catalog(
                database=self.glue_database,
                table_name="customer_orders_src",
                transformation_ctx="df_SQ_customer_orders_src_modest_shannon",
            )
            df_SQ_customer_orders_src_modest_shannon = (
                dyf_SQ_customer_orders_src_modest_shannon
                .toDF()
                .select(
                    "order_id",
                    "customer_id",
                    "order_date",
                    "product_id",
                    "product_name",
                    "quantity",
                    "unit_price",
                    "order_status",
                    "country",
                )
            )
            if df_SQ_customer_orders_src_modest_shannon.isEmpty():
                self.logger.warning("Source SQ_customer_orders_src returned no rows", node="SQ_customer_orders_src")
        except Exception:
            self.logger.exception("Failed reading SQ_customer_orders_src from Glue Catalog")
            raise

        # customer_orders_src: logical source placeholder (bypassed in this mapping) - alias to the SQ dataframe for lineage
        # (Assigned after the SQ read which provides the actual data at runtime)
        df_customer_orders_src_fierce_heisenberg = df_SQ_customer_orders_src_modest_shannon

        # EXP_customer_orders: derived columns and passthroughs
        try:
            self.logger.info("Applying EXP_customer_orders expressions", node="EXP_customer_orders")
            df_EXP_customer_orders_mighty_spinoza = (
                df_SQ_customer_orders_src_modest_shannon
                .withColumn("customer_id_clean", trim(col("customer_id")))
                .withColumn("order_status_norm", upper(trim(col("order_status"))))
                .withColumn("order_total", (col("quantity") * col("unit_price")))
                # $$PMSourceFileName is an Informatica PM system variable with no Glue equivalent
                .withColumn("source_file_name", lit(None).cast("string"))
            )
        except Exception:
            self.logger.exception("Failed applying EXP_customer_orders expressions")
            raise

        # FIL_customer_orders: apply filter condition
        try:
            self.logger.info("Applying FIL_customer_orders filter", node="FIL_customer_orders")
            df_FIL_customer_orders_hopeful_socrates = df_EXP_customer_orders_mighty_spinoza.filter(
                (col("order_id").isNotNull())
                & (col("customer_id").isNotNull())
                & (length(trim(col("customer_id"))) > 0)
                & (col("quantity") > 0)
                & (col("unit_price") >= 0)
            )
        except Exception:
            self.logger.exception("Failed applying FIL_customer_orders filter")
            raise

        # write final target customer_orders_tgt as parquet to S3 (overwrite)
        try:
            self.logger.info("Writing customer_orders_tgt to S3 as parquet (overwrite)", table="customer_orders_tgt")
            self.parquet_writer.write(
                dataframe=df_FIL_customer_orders_hopeful_socrates,
                path=f"s3://{self.s3_output_bucket}/customer_orders_tgt/",
                mode="overwrite",
            )
        except Exception:
            self.logger.exception("Failed writing customer_orders_tgt to S3")
            raise

        # final assignment for target df_name expected by downstream/validation
        # (This variable name marks the Output node's df_name_out; it references the DF we just wrote)
        df_customer_orders_tgt_elegant_euclid = df_FIL_customer_orders_hopeful_socrates

        self.glue_job.commit()

    def finalize(self) -> None:
        self.logger.info("Finalizing m_customer_orders_src_to_customer_orders_tgt")

    @staticmethod
    def _require_env(env: Mapping[str, Any], key: str) -> str:

        value = env.get(key)

        if not value or str(value).startswith("REPLACE_WITH_"):
            raise ConfigurationValidationError(
                f"config.yaml 'env.{key}' must be set"
            )

        return str(value)
