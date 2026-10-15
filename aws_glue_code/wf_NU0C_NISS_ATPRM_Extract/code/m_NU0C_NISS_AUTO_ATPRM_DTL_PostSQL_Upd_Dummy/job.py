import sys
from typing import Any, Mapping

from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import DataFrame

from glue_lib.connector import ParquetReader, ParquetWriter
from glue_lib.exceptions import ConfigurationValidationError
from glue_lib.job.base import BaseJob
from pyspark.sql.functions import col, lit
from glue_lib.connector.snowflake import SnowflakeReader


# Informatica mapping m_NU0C_NISS_AUTO_ATPRM_DTL_PostSQL_Upd_Dummy.
# Every mapping node runs in order inside run().
class Nu0cNissAutoAtprmDtlPostSQLUpdDummyJob(BaseJob):

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

        # SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1: SQL override read from Snowflake (session-level override, returns no rows)
        sql_query = f"""--This is a dummy session that does not read any rows
SELECT 
NISS_APRM_DETL_SK,
ST_ABBR,
ACCTNG_LOB,
BI_LMT,
PRD_GRP_CD,
NJ_NO_LWST_LMT_IND,
NJ_NMD_DRVR_EXCL_IND,
CVG_TYP_CD

FROM FDR.WRK_BIRP_NISS_APRM_DETL
WHERE 1=2"""

        try:
            self.logger.info("Reading SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1 from Snowflake via SQL override", node="SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1")
            df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1_gentle_euclid = SnowflakeReader(self.glue_context).read(
                connection_name=SNOWFLAKE_CONNECTION_FDWS_FDRWRK,
                query=sql_query,
                options={
                    "pre_sql": "UPDATE FDR.WRK_BIRP_NISS_APRM_DETL\r\nSET NISS_CLASS_CD ='861129' WHERE SUBSTR(NISS_CLASS_CD,1,1)='D' AND MLT_CAR_IND='Y'",
                    "post_sql": "UPDATE FDR.WRK_BIRP_NISS_APRM_DETL\r\nSET NISS_CLASS_CD ='123200' WHERE SUBSTR(NISS_CLASS_CD,1,1)='F'",
                },
            )
            # guard: log if source returned no rows
            try:
                is_empty = df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1_gentle_euclid.isEmpty()
            except Exception:
                # fall back to rdd.isEmpty() if isEmpty() is not available
                is_empty = df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1_gentle_euclid.rdd.isEmpty()
            if is_empty:
                self.logger.warning("Source SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1 returned no rows", node="SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1")
        except Exception:
            self.logger.exception("Failed reading SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1 from Snowflake")
            raise

        # Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1: Source bypassed in the default (non-staged) SQL-override case - alias it to the ASQ result for lineage
        df_WRK_BIRP_NISS_APRM_DETL_1_clever_franklin = df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1_gentle_euclid

        # EXPTRANS: passthrough projection of all INPUT/OUTPUT ports
        try:
            self.logger.info("Applying EXPTRANS expressions", node="EXPTRANS")
            df_EXPTRANS_humble_socrates = df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1_gentle_euclid.selectExpr(
                "NISS_APRM_DETL_SK",
                "ST_ABBR",
                "ACCTNG_LOB",
                "BI_LMT",
                "PRD_GRP_CD",
                "NJ_NO_LWST_LMT_IND",
                "NJ_NMD_DRVR_EXCL_IND",
                "CVG_TYP_CD",
            )
        except Exception:
            self.logger.exception("Failed applying EXPTRANS expressions")
            raise

        # pass the Expression output to the Output node's dataframe name
        df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_admiring_pascal = df_EXPTRANS_humble_socrates

        # write intermediate WRK_ table as parquet to S3 (overwrite)
        try:
            self.logger.info("Writing WRK_BIRP_NISS_APRM_DETL to S3 as parquet (overwrite)", table="WRK_BIRP_NISS_APRM_DETL")
            self.parquet_writer.write(
                dataframe=df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_admiring_pascal,
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                mode="overwrite",
                partition_by=None,
                options=None,
            )
        except Exception:
            self.logger.exception("Failed writing WRK_BIRP_NISS_APRM_DETL to S3")
            raise

        self.glue_job.commit()

    def finalize(self) -> None:
        self.logger.info("Finalizing m_NU0C_NISS_AUTO_ATPRM_DTL_PostSQL_Upd_Dummy")

    @staticmethod
    def _require_env(env: Mapping[str, Any], key: str) -> str:

        value = env.get(key)

        if not value or str(value).startswith("REPLACE_WITH_"):
            raise ConfigurationValidationError(
                f"config.yaml 'env.{key}' must be set"
            )

        return str(value)
