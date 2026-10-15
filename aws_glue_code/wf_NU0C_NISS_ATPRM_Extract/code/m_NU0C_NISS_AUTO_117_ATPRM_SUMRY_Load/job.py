import sys
from typing import Any, Mapping

from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import DataFrame

from glue_lib.connector import ParquetReader, ParquetWriter
from glue_lib.exceptions import ConfigurationValidationError
from glue_lib.job.base import BaseJob
from pyspark.sql.functions import col, lit, current_timestamp, row_number, broadcast
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType, LongType


# Informatica mapping m_NU0C_NISS_AUTO_117_ATPRM_SUMRY_Load.
# Every mapping node runs in order inside run().
class Nu0cNissAuto117AtprmSumryLoadJob(BaseJob):

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

        # FDR_LIB_WRK_BIRP_NISS_APRM_FINAL: Source with S3-first preference, project NISS_NO_PD_CLMS
        try:
            self.logger.info("Attempting to read FDR_LIB_WRK_BIRP_NISS_APRM_FINAL from S3 first", node="FDR_LIB_WRK_BIRP_NISS_APRM_FINAL")
            df_WRK_BIRP_NISS_APRM_FINAL_heroic_nash = self.parquet_reader.read(
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_FINAL/",
                options=None,
            ).select("NISS_NO_PD_CLMS")
            self.logger.info("Read FDR_LIB_WRK_BIRP_NISS_APRM_FINAL from S3", node="FDR_LIB_WRK_BIRP_NISS_APRM_FINAL")
        except Exception:
            # S3-first read allowed to fall back to Glue Catalog
            self.logger.warning("S3 read for FDR_LIB_WRK_BIRP_NISS_APRM_FINAL failed, falling back to Glue Data Catalog", node="FDR_LIB_WRK_BIRP_NISS_APRM_FINAL")
            try:
                dyf = self.glue_context.create_dynamic_frame_from_catalog(
                    database=self.glue_database,
                    table_name="WRK_BIRP_NISS_APRM_FINAL",
                    transformation_ctx="df_WRK_BIRP_NISS_APRM_FINAL_heroic_nash",
                )
                df_WRK_BIRP_NISS_APRM_FINAL_heroic_nash = dyf.toDF().select("NISS_NO_PD_CLMS")
                self.logger.info("Read FDR_LIB_WRK_BIRP_NISS_APRM_FINAL from Glue Catalog", node="FDR_LIB_WRK_BIRP_NISS_APRM_FINAL")
            except Exception:
                self.logger.exception("Failed reading FDR_LIB_WRK_BIRP_NISS_APRM_FINAL from Glue Catalog")
                raise

        if df_WRK_BIRP_NISS_APRM_FINAL_heroic_nash.isEmpty():
            self.logger.warning("Source FDR_LIB_WRK_BIRP_NISS_APRM_FINAL returned no rows", node="FDR_LIB_WRK_BIRP_NISS_APRM_FINAL")

        # EXP_Passthru: upstream input missing -> emit a dataframe with every passthrough/output port as NULL
        try:
            self.logger.info("Building EXP_Passthru passthrough dataframe (upstream missing)", node="EXP_Passthru")
            schema_exp_passthru = StructType([
                StructField("NISS_CMPNY_CD", StringType()),
                StructField("CLNDR_YR", StringType()),
                StructField("ST_ABBR", StringType()),
                StructField("NISS_ST_CD", StringType()),
                StructField("ACCDNT_YR", StringType()),
                StructField("NISS_CVG_CD", StringType()),
                StructField("NISS_CLASS_CD", StringType()),
                StructField("NISS_SUBLOB_CD", StringType()),
                StructField("NISS_TYP_LOSS_CD", StringType()),
                StructField("NISS_ANNL_STMNT_LOB_CD", StringType()),
                StructField("CVG_EXPS_VAL", DecimalType(9, 2)),
                StructField("TTL_WRITTN_PREM_AMT", DecimalType(15, 2)),
                StructField("NISS_PD_LOSS", IntegerType()),
                StructField("NISS_PD_ALLOC_ADJUS_EXPNS", IntegerType()),
                StructField("NISS_OUTSTNDG_LOSS", IntegerType()),
                StructField("NISS_NO_PD_CLMS", IntegerType()),
                StructField("NISS_NO_OUTSTND_CLMS", IntegerType()),
                StructField("NISS_TERR_CD", StringType()),
                # PM<...> system variables have no Glue equivalent - emit NULLs
                StructField("MAPPING_NAME", StringType()),
                StructField("FOLDER_NAME", StringType()),
                StructField("WORKFLOW_NAME", StringType()),
            ])
            # Create an empty DataFrame with the required schema (no rows)
            df_EXP_Passthru_determined_turing = self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), schema_exp_passthru)
        except Exception:
            self.logger.exception("Failed building EXP_Passthru dataframe")
            raise

        # mplt_FDR_LIB_ABC_MAPPING_AUDIT: mapplet instance - reviewer TODO and produce audit single-row outputs
        # TODO: unrecognized transformation_type 'Input'/'Mapplet' - no dedicated rule; verify against Informatica documentation
        try:
            self.logger.info("Applying mplt_FDR_LIB_ABC_MAPPING_AUDIT mapplet logic", node="mplt_FDR_LIB_ABC_MAPPING_AUDIT")
            # Create a single-row audit dataframe with current timestamps and NULL ids (lookups unavailable in export)
            df_mplt_FDR_LIB_ABC_MAPPING_AUDIT_amazing_hopper = (
                self.spark.sql("SELECT current_timestamp() AS DW_CR_TMSP, current_timestamp() AS DW_UPD_TMSP")
                .withColumn("CR_BY_MAPNG_ID", lit(None).cast(LongType()))
                .withColumn("UPD_BY_MAPNG_ID", lit(None).cast(LongType()))
                .withColumn("WRK_FLOW_RUN_ID", lit(None).cast(LongType()))
                # Keep input mapping/folder/workflow name columns if needed downstream (pass through as NULL since input may be empty)
                .withColumn("MAPPING_NAME", lit(None).cast(StringType()))
                .withColumn("FOLDER_NAME", lit(None).cast(StringType()))
                .withColumn("WORKFLOW_NAME", lit(None).cast(StringType()))
                .select(
                    "CR_BY_MAPNG_ID",
                    "DW_CR_TMSP",
                    "UPD_BY_MAPNG_ID",
                    "DW_UPD_TMSP",
                    "WRK_FLOW_RUN_ID",
                    "MAPPING_NAME",
                    "FOLDER_NAME",
                    "WORKFLOW_NAME",
                )
            )
        except Exception:
            self.logger.exception("Failed applying mplt_FDR_LIB_ABC_MAPPING_AUDIT mapplet logic")
            raise

        # EXP_Pass_Tgt: generate surrogate key and cross-join broadcasted mapplet audit values
        try:
            self.logger.info("Applying EXP_Pass_Tgt expressions and generating surrogate key", node="EXP_Pass_Tgt")
            w = Window.orderBy(lit(1))

            # Ensure the passthrough dataframe has the expected columns; they were created as NULLs earlier if upstream was missing
            passthru_cols = [
                "NISS_CMPNY_CD",
                "CLNDR_YR",
                "ST_ABBR",
                "NISS_ST_CD",
                "ACCDNT_YR",
                "NISS_CVG_CD",
                "NISS_CLASS_CD",
                "NISS_SUBLOB_CD",
                "NISS_TYP_LOSS_CD",
                "NISS_ANNL_STMNT_LOB_CD",
                "CVG_EXPS_VAL",
                "TTL_WRITTN_PREM_AMT",
                "NISS_PD_LOSS",
                "NISS_PD_ALLOC_ADJUS_EXPNS",
                "NISS_OUTSTNDG_LOSS",
                "NISS_NO_PD_CLMS",
                "NISS_NO_OUTSTND_CLMS",
                "NISS_TERR_CD",
            ]

            # Select passthrough columns explicitly (they exist in df_EXP_Passthru_determined_turing's schema)
            df_passthru = df_EXP_Passthru_determined_turing.select(*[col(c) for c in passthru_cols])

            # Cross-join the single-row audit mapplet (broadcast) so every passthrough row receives audit values
            df_joined = df_passthru.crossJoin(broadcast(df_mplt_FDR_LIB_ABC_MAPPING_AUDIT_amazing_hopper.select(
                "CR_BY_MAPNG_ID",
                "DW_CR_TMSP",
                "UPD_BY_MAPNG_ID",
                "DW_UPD_TMSP",
                "WRK_FLOW_RUN_ID",
            )))

            # Add surrogate key as a gap-free sequence
            df_with_sk = df_joined.withColumn("NISS_APRM_SUMRY_SK", row_number().over(w))

            # Reorder columns to place the surrogate key first, then passthroughs, then audit columns
            final_columns = [
                "NISS_APRM_SUMRY_SK",
                *passthru_cols,
                "CR_BY_MAPNG_ID",
                "DW_CR_TMSP",
                "UPD_BY_MAPNG_ID",
                "DW_UPD_TMSP",
                "WRK_FLOW_RUN_ID",
            ]

            df_EXP_Pass_Tgt_nice_einstein = df_with_sk.select(*final_columns)
        except Exception:
            self.logger.exception("Failed applying EXP_Pass_Tgt expressions")
            raise

        # FDR_LIB_WRK_BIRP_NISS_APRM_SUMRY: write the WRK_ target as parquet to S3 (overwrite)
        # write intermediate WRK_ table as parquet to S3 (overwrite)
        try:
            self.logger.info("Writing WRK_BIRP_NISS_APRM_SUMRY to S3 as parquet (overwrite)", table="WRK_BIRP_NISS_APRM_SUMRY")
            df_WRK_BIRP_NISS_APRM_SUMRY_loving_descartes = df_EXP_Pass_Tgt_nice_einstein
            self.parquet_writer.write(
                dataframe=df_WRK_BIRP_NISS_APRM_SUMRY_loving_descartes,
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_SUMRY/",
                mode="overwrite",
                partition_by=None,
                options=None,
            )
        except Exception:
            self.logger.exception("Failed writing WRK_BIRP_NISS_APRM_SUMRY to S3")
            raise

        self.glue_job.commit()

    def finalize(self) -> None:
        self.logger.info("Finalizing m_NU0C_NISS_AUTO_117_ATPRM_SUMRY_Load")

    @staticmethod
    def _require_env(env: Mapping[str, Any], key: str) -> str:

        value = env.get(key)

        if not value or str(value).startswith("REPLACE_WITH_"):
            raise ConfigurationValidationError(
                f"config.yaml 'env.{key}' must be set"
            )

        return str(value)
