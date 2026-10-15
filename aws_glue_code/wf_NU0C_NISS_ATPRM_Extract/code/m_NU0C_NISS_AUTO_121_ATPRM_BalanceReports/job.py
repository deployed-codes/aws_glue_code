import sys
from typing import Any, Mapping

from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import DataFrame

from glue_lib.connector import ParquetReader, ParquetWriter
from glue_lib.exceptions import ConfigurationValidationError
from glue_lib.job.base import BaseJob
from pyspark.sql.functions import col, trim, substring, when, lit, coalesce
import tempfile
import uuid
import glob
import shutil
import os


# Informatica mapping m_NU0C_NISS_AUTO_121_ATPRM_BalanceReports.
# Every mapping node runs in order inside run().
class Nu0cNissAuto121AtprmBalanceReportsJob(BaseJob):

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

        # FDR_LIB_WRK_BIRP_NISS_APRM_DETL: try S3 parquet first, fall back to Glue Catalog
        try:
            self.logger.info("Attempting S3-first read for FDR_LIB_WRK_BIRP_NISS_APRM_DETL", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_keen_heisenberg = self.parquet_reader.read(
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                options=None,
            ).select("CLNDR_YR", "NISS_CMPNY_CD", "ST_NM", "TTL_WRITTN_PREM_AMT", "REC_DROP_IND")
            if df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_keen_heisenberg.isEmpty():
                self.logger.warning("Source FDR_LIB_WRK_BIRP_NISS_APRM_DETL returned no rows", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
        except Exception:
            # allowed fallback branch - read from Glue Catalog instead
            self.logger.info("Falling back to Glue Catalog read for FDR_LIB_WRK_BIRP_NISS_APRM_DETL", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            dyf = self.glue_context.create_dynamic_frame.from_catalog(
                database=self.glue_database,
                table_name="WRK_BIRP_NISS_APRM_DETL",
                transformation_ctx="df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_keen_heisenberg",
            )
            df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_keen_heisenberg = (
                dyf.toDF()
                .select("CLNDR_YR", "NISS_CMPNY_CD", "ST_NM", "TTL_WRITTN_PREM_AMT", "REC_DROP_IND")
            )
            if df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_keen_heisenberg.isEmpty():
                self.logger.warning("Source FDR_LIB_WRK_BIRP_NISS_APRM_DETL returned no rows from Glue Catalog", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")

        # FDR_LIB_WRK_BIRP_NISS_APRM_FINAL: try S3 parquet first, fall back to Glue Catalog
        try:
            self.logger.info("Attempting S3-first read for FDR_LIB_WRK_BIRP_NISS_APRM_FINAL", node="FDR_LIB_WRK_BIRP_NISS_APRM_FINAL")
            df_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL_magical_maxwell = self.parquet_reader.read(
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_FINAL/",
                options=None,
            ).select("CLNDR_YR", "NISS_CMPNY_CD", "ST_NM", "TTL_WRITTN_PREM_AMT")
            if df_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL_magical_maxwell.isEmpty():
                self.logger.warning("Source FDR_LIB_WRK_BIRP_NISS_APRM_FINAL returned no rows", node="FDR_LIB_WRK_BIRP_NISS_APRM_FINAL")
        except Exception:
            # allowed fallback branch - read from Glue Catalog instead
            self.logger.info("Falling back to Glue Catalog read for FDR_LIB_WRK_BIRP_NISS_APRM_FINAL", node="FDR_LIB_WRK_BIRP_NISS_APRM_FINAL")
            dyf = self.glue_context.create_dynamic_frame.from_catalog(
                database=self.glue_database,
                table_name="WRK_BIRP_NISS_APRM_FINAL",
                transformation_ctx="df_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL_magical_maxwell",
            )
            df_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL_magical_maxwell = (
                dyf.toDF()
                .select("CLNDR_YR", "NISS_CMPNY_CD", "ST_NM", "TTL_WRITTN_PREM_AMT")
            )
            if df_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL_magical_maxwell.isEmpty():
                self.logger.warning("Source FDR_LIB_WRK_BIRP_NISS_APRM_FINAL returned no rows from Glue Catalog", node="FDR_LIB_WRK_BIRP_NISS_APRM_FINAL")

        # SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL: Application Source Qualifier using SQL override against the staged temp views
        # register the staged source DataFrames as temp views so the SQL override can run against them
        try:
            self.logger.info("Registering temp views for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL", node="SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_keen_heisenberg.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")
            df_FDR_LIB_WRK_BIRP_NISS_APRM_FINAL_magical_maxwell.createOrReplaceTempView("WRK_BIRP_NISS_APRM_FINAL")
        except Exception:
            self.logger.exception("Failed registering temp views for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            raise

        # rewritten SQL override referencing the bare temp view names instead of the original FDR-qualified names
        sql_query = f"""SELECT
COALESCE(DTL.CLNDR_YR, FNL.CLNDR_YR) CLNDR_YR,
COALESCE(DTL.NISS_CMPNY_CD, FNL.NISS_CMPNY_CD) NISS_CMPNY_CD,
COALESCE(DTL.ST_NM, FNL.ST_NM) ST_NM,
COALESCE(DTL_PREM_AMT,0) DTL_PREM_AMT,
COALESCE(DROP_PREM_AMT,0) DROP_PREM_AMT,
COALESCE(FNL_PREM_AMT,0) FNL_PREM_AMT

FROM
(SELECT
COALESCE(D_ALL.CLNDR_YR, D_DROP.CLNDR_YR) CLNDR_YR,
COALESCE(D_ALL.NISS_CMPNY_CD, D_DROP.NISS_CMPNY_CD) NISS_CMPNY_CD,
COALESCE(D_ALL.ST_NM, D_DROP.ST_NM) ST_NM,
COALESCE(DTL_PREM_AMT,0) DTL_PREM_AMT,
COALESCE(DROP_PREM_AMT,0) DROP_PREM_AMT

FROM
(SELECT CLNDR_YR, NISS_CMPNY_CD, ST_NM,
SUM(TTL_WRITTN_PREM_AMT) DTL_PREM_AMT
FROM WRK_BIRP_NISS_APRM_DETL
GROUP BY CLNDR_YR, NISS_CMPNY_CD, ST_NM) D_ALL

FULL OUTER JOIN

(SELECT CLNDR_YR, NISS_CMPNY_CD, ST_NM,
SUM(TTL_WRITTN_PREM_AMT) DROP_PREM_AMT
FROM WRK_BIRP_NISS_APRM_DETL
WHERE REC_DROP_IND='Y'
GROUP BY CLNDR_YR, NISS_CMPNY_CD, ST_NM) D_DROP

ON D_ALL.CLNDR_YR = D_DROP.CLNDR_YR
AND D_ALL.NISS_CMPNY_CD = D_DROP.NISS_CMPNY_CD
AND D_ALL.ST_NM = D_DROP.ST_NM
) DTL

FULL OUTER JOIN

(SELECT CLNDR_YR, NISS_CMPNY_CD, ST_NM,
SUM(TTL_WRITTN_PREM_AMT) FNL_PREM_AMT
FROM WRK_BIRP_NISS_APRM_FINAL
GROUP BY CLNDR_YR, NISS_CMPNY_CD, ST_NM) FNL

ON DTL.CLNDR_YR = FNL.CLNDR_YR
AND DTL.NISS_CMPNY_CD = FNL.NISS_CMPNY_CD
AND DTL.ST_NM = FNL.ST_NM
"""

        try:
            self.logger.info("Running SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL SQL override via Spark SQL", node="SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_romantic_hopper = self.spark.sql(sql_query)
            # normalize column names to what downstream expression expects (DET_PREM_AMT)
            df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_romantic_hopper = df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_romantic_hopper.selectExpr(
                "CLNDR_YR",
                "NISS_CMPNY_CD",
                "ST_NM",
                "COALESCE(DTL_PREM_AMT, 0) AS DET_PREM_AMT",
                "COALESCE(DROP_PREM_AMT, 0) AS DROP_PREM_AMT",
                "COALESCE(FNL_PREM_AMT, 0) AS FNL_PREM_AMT",
            )
            if df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_romantic_hopper.isEmpty():
                self.logger.warning("SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL produced no rows", node="SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
        except Exception:
            self.logger.exception("Failed executing SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL SQL override")
            raise

        # EXP_DeriveBalanaceIndicator: derive o_CLNDR_YR, BAL_DIFF, BAL_IND and map to final flat-file column names
        try:
            self.logger.info("Applying EXP_DeriveBalanaceIndicator expressions", node="EXP_DeriveBalanaceIndicator")
            df_EXP_DeriveBalanaceIndicator_calm_heisenberg = (
                df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_romantic_hopper
                .withColumn("Clndr_Year", substring(trim(col("CLNDR_YR")), 3, 2))
                .withColumn("Company_Number", col("NISS_CMPNY_CD"))
                .withColumn("State_Name", col("ST_NM"))
                .withColumn("Detail_Premium_Amount", col("DET_PREM_AMT"))
                .withColumn("Detail_Dropped_Premium_Amount", col("DROP_PREM_AMT"))
                .withColumn("Final_Premium_Amount", col("FNL_PREM_AMT"))
                .withColumn(
                    "Premium_Amount_Difference",
                    (col("DET_PREM_AMT") - col("DROP_PREM_AMT") - col("FNL_PREM_AMT"))
                )
                .withColumn(
                    "Balance_Indicator",
                    when((col("DET_PREM_AMT") - col("DROP_PREM_AMT") - col("FNL_PREM_AMT")) == 0, lit("Y")).otherwise(lit("N"))
                )
                .select(
                    "Clndr_Year",
                    "Company_Number",
                    "State_Name",
                    "Detail_Premium_Amount",
                    "Detail_Dropped_Premium_Amount",
                    "Final_Premium_Amount",
                    "Premium_Amount_Difference",
                    "Balance_Indicator",
                )
            )
        except Exception:
            self.logger.exception("Failed applying EXP_DeriveBalanaceIndicator expressions")
            raise

        # FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptBal2: flat-file output
        # read mapping parameters from env
        OUTPUTFILE_PREM_RPT_BAL2 = self._require_env(self.env, "OUTPUTFILE_PREM_RPT_BAL2")
        PM_TARGET_FILE_DIR = self._require_env(self.env, "PM_TARGET_FILE_DIR")

        # assign outgoing df_name as alias to the expression result
        df_FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptBal2_stoic_mendel = df_EXP_DeriveBalanaceIndicator_calm_heisenberg

        try:
            self.logger.info("Writing flat-file target FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptBal2 as single delimited file (staged + move)", table="FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptBal2")
            # stage to a temporary directory under the target dir
            tmp_dir = f"{PM_TARGET_FILE_DIR.rstrip('/')}/_tmp_FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptBal2_{uuid.uuid4().hex}"
            # write as a single-part CSV via Spark
            df_FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptBal2_stoic_mendel.coalesce(1).write.mode("overwrite").option("header", True).option("sep", ",").csv(tmp_dir)
            # locate the single part file and move it to the exact filename expected
            part_files = glob.glob(f"{tmp_dir}/part-*.csv")
            if not part_files:
                raise RuntimeError(f"No part file found in staged temp directory {tmp_dir}")
            staged_part = part_files[0]
            target_path = f"{PM_TARGET_FILE_DIR.rstrip('/')}/{OUTPUTFILE_PREM_RPT_BAL2}"
            # ensure target directory exists for local filesystem moves
            target_dir = os.path.dirname(target_path)
            if target_dir and not os.path.exists(target_dir):
                os.makedirs(target_dir, exist_ok=True)
            shutil.move(staged_part, target_path)
            # cleanup temp staging directory
            try:
                shutil.rmtree(tmp_dir)
            except Exception:
                # non-fatal cleanup failure - log and continue
                self.logger.warning("Failed removing temporary directory after flat file staging", tmp_dir=tmp_dir)
        except Exception:
            self.logger.exception("Failed writing flat-file target FDR_LIB_ff_BIRP_NU0C_NISS_ATPRM_RptBal2")
            raise

        self.glue_job.commit()

    def finalize(self) -> None:
        self.logger.info("Finalizing m_NU0C_NISS_AUTO_121_ATPRM_BalanceReports")

    @staticmethod
    def _require_env(env: Mapping[str, Any], key: str) -> str:

        value = env.get(key)

        if not value or str(value).startswith("REPLACE_WITH_"):
            raise ConfigurationValidationError(
                f"config.yaml 'env.{key}' must be set"
            )

        return str(value)
