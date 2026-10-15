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
    ltrim,
    rtrim,
    when,
    regexp_replace,
    split,
    size,
    expr,
    substring,
)
from glue_lib.connector.snowflake import SnowflakeReader


# Informatica mapping m_NU0C_NISS_AUTO_110_ATPRM_DTL_Upd_NYNJSublineCd.
# Every mapping node runs in order inside run().
class Nu0cNissAuto110AtprmDtlUpdNYNJSublineCdJob(BaseJob):

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

        # SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1: read the SQL-override result from Snowflake
        sql_query = f"""SELECT
NISS_APRM_DETL_SK,
ST_ABBR,
ACCTNG_LOB,
BI_LMT,
PRD_GRP_CD,
NJ_NO_LWST_LMT_IND,
NJ_NMD_DRVR_EXCL_IND,
CVG_TYP_CD

FROM FDR.WRK_BIRP_NISS_APRM_DETL

WHERE ST_ABBR IN ('NY','NJ')"""

        try:
            self.logger.info("Reading SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1 from Snowflake via SQL override", node="SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1")
            df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1_focused_pasteur = SnowflakeReader(self.glue_context).read(
                connection_name=SNOWFLAKE_CONNECTION_FDWS_FDRWRK,
                query=sql_query,
            )
            if df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1_focused_pasteur.isEmpty():
                self.logger.warning("Source SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1 returned no rows", node="SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1")
        except Exception:
            self.logger.exception("Failed reading SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1 from Snowflake")
            raise

        # Alias the bypassed Source node to the SQ result so the Source df_name is present for lineage
        df_WRK_BIRP_NISS_APRM_DETL_1_awesome_fermat = df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1_focused_pasteur

        # EXP_BILimit_Split: split BI_LMT into parts, convert to decimals, and expose count and cleaned source
        try:
            self.logger.info("Applying EXP_BILimit_Split expressions", node="EXP_BILimit_Split")
            df_EXP_BILimit_Split_brave_leibniz = (
                df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1_focused_pasteur
                .withColumn("v_BI_LMT", regexp_replace(trim(col("BI_LMT")), ",", ""))
                .withColumn("parts", split(col("v_BI_LMT"), "/"))
                .withColumn("BI_LMT_NO_OF_PARTS", size(col("parts")))
                .withColumn("BI_LMT_1_Decimal", expr("try_cast(element_at(parts, 1) as decimal(14,0))"))
                .withColumn("BI_LMT_2_Decimal", expr("try_cast(element_at(parts, 2) as decimal(14,0))"))
                .withColumn("BI_LMT_3_Decimal", expr("try_cast(element_at(parts, 3) as decimal(14,0))"))
                .withColumn("SRC_BI_LMT", col("v_BI_LMT"))
                # keep the key so downstream joins can re-associate rows
                .select(
                    col("NISS_APRM_DETL_SK"),
                    col("BI_LMT_1_Decimal"),
                    col("BI_LMT_2_Decimal"),
                    col("BI_LMT_3_Decimal"),
                    col("BI_LMT_NO_OF_PARTS"),
                    col("SRC_BI_LMT"),
                )
            )
        except Exception:
            self.logger.exception("Failed applying EXP_BILimit_Split expressions")
            raise

        # EXP_Derive_NISS_SUBLOB_CD_And_PassThru: derive NISS_SUBLOB_CD using NJ/NY logic and pass through source columns
        try:
            self.logger.info("Applying EXP_Derive_NISS_SUBLOB_CD_And_PassThru expressions", node="EXP_Derive_NISS_SUBLOB_CD_And_PassThru")
            # join split BI_LMT results back to the SQ rows by key to ensure all passthrough columns are available
            df_joined = df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1_focused_pasteur.alias("s").join(
                df_EXP_BILimit_Split_brave_leibniz.alias("p"),
                on=[col("s.NISS_APRM_DETL_SK") == col("p.NISS_APRM_DETL_SK")],
                how="left",
            )
            # after join, drop the duplicate join key column from the right side and normalize column references
            # create a base dataframe that exposes all original source columns (from s) and the BI_LMT-derived cols from p
            # select s.* explicitly to preserve all upstream fields
            s_cols = [col(f"s.{c}") for c in df_SQ_Shortcut_to_WRK_BIRP_NISS_APRM_DETL_1_focused_pasteur.columns]
            df_base = df_joined.select(*(s_cols + [col("p.BI_LMT_1_Decimal"), col("p.BI_LMT_2_Decimal"), col("p.BI_LMT_3_Decimal"), col("p.BI_LMT_NO_OF_PARTS"), col("p.SRC_BI_LMT")]))

            # derive helper flags
            df_derived = (
                df_base
                .withColumn("v_NJ_NO_LWST_LMT_IND", when(col("NJ_NO_LWST_LMT_IND") == 1, lit("Y")).otherwise(lit("N")))
                .withColumn("v_NJ_NMD_DRVR_EXCL_IND", when(col("NJ_NMD_DRVR_EXCL_IND") == 1, lit("Y")).otherwise(lit("N")))
                .withColumn("_acctng_lob_trim", trim(col("ACCTNG_LOB")))
                .withColumn("_acctng_lob_3", substring(col("_acctng_lob_trim"), 1, 3))
            )

            # New Jersey logic (ordered WHENs mirror the DECODE priority)
            nj_case = (
                when(
                    (col("ST_ABBR") == "NJ") & col("_acctng_lob_3").isin("191", "192") & (col("PRD_GRP_CD") != "BA") & (col("v_NJ_NO_LWST_LMT_IND") == "Y"),
                    lit("7"),
                )
                .when(
                    (col("ST_ABBR") == "NJ") & col("_acctng_lob_3").isin("191", "192") & (col("PRD_GRP_CD") != "BA") & (col("v_NJ_NO_LWST_LMT_IND") != "Y"),
                    lit("8"),
                )
                .when(
                    (col("ST_ABBR") == "NJ") & col("_acctng_lob_3").isin("191", "192") & (col("PRD_GRP_CD") == "BA"),
                    lit("9"),
                )
                .when(
                    (col("ST_ABBR") == "NJ") & (col("_acctng_lob_3") == "211") & (col("PRD_GRP_CD") != "BA") & (col("v_NJ_NMD_DRVR_EXCL_IND") != "Y"),
                    lit("1"),
                )
                .when(
                    (col("ST_ABBR") == "NJ") & (col("_acctng_lob_3") == "211") & (col("PRD_GRP_CD") != "BA") & (col("v_NJ_NMD_DRVR_EXCL_IND") == "Y"),
                    lit("2"),
                )
                .when(
                    (col("ST_ABBR") == "NJ") & (col("_acctng_lob_3") == "211") & (col("PRD_GRP_CD") == "BA") & (col("v_NJ_NMD_DRVR_EXCL_IND") != "Y"),
                    lit("3"),
                )
                .when(
                    (col("ST_ABBR") == "NJ") & (col("_acctng_lob_3") == "211") & (col("PRD_GRP_CD") == "BA") & (col("v_NJ_NMD_DRVR_EXCL_IND") == "Y"),
                    lit("4"),
                )
                .when(
                    col("ST_ABBR") == "NJ",
                    lit("?"),
                )
                .otherwise(lit("?"))
            )

            # New York logic
            ny_case = (
                when(
                    (col("ST_ABBR") == "NY") & col("_acctng_lob_3").isin("191", "192") & (~col("CVG_TYP_CD").isin("13000", "13006", "13007", "13008")),
                    lit("1"),
                )
                .when(
                    (col("ST_ABBR") == "NY") & col("_acctng_lob_3").isin("191", "192") & (col("CVG_TYP_CD").isin("13000", "13006", "13007", "13008")),
                    lit("2"),
                )
                .when(
                    (col("ST_ABBR") == "NY") & (col("_acctng_lob_3") == "211"),
                    lit("0"),
                )
                .when(col("ST_ABBR") == "NY", lit("?"))
                .otherwise(lit("?"))
            )

            df_with_v = (
                df_derived
                .withColumn("v_NISS_SUBLOB_CD_NewJersey", nj_case)
                .withColumn("v_NISS_SUBLOB_CD_NewYork", ny_case)
                .withColumn(
                    "v_NISS_SUBLOB_CD",
                    when(col("ST_ABBR") == "NY", col("v_NISS_SUBLOB_CD_NewYork")).when(col("ST_ABBR") == "NJ", col("v_NISS_SUBLOB_CD_NewJersey")).otherwise(lit("?")),
                )
                .withColumn(
                    "NISS_SUBLOB_CD",
                    when((col("v_NISS_SUBLOB_CD") == "") | col("v_NISS_SUBLOB_CD").isNull(), lit("?")).otherwise(col("v_NISS_SUBLOB_CD")),
                )
            )

            # final dataframe preserves all original source columns plus the derived NISS_SUBLOB_CD
            # drop helper cols used only for derivation
            drop_cols = ["v_NJ_NO_LWST_LMT_IND", "v_NJ_NMD_DRVR_EXCL_IND", "_acctng_lob_trim", "_acctng_lob_3", "v_NISS_SUBLOB_CD_NewJersey", "v_NISS_SUBLOB_CD_NewYork", "v_NISS_SUBLOB_CD"]
            select_cols = [c for c in df_with_v.columns if c not in drop_cols]
            df_EXP_Derive_NISS_SUBLOB_CD_And_PassThru_magical_hilbert = df_with_v.select(*select_cols)
        except Exception:
            self.logger.exception("Failed applying EXP_Derive_NISS_SUBLOB_CD_And_PassThru expressions")
            raise

        # UPD_NISS_SUBLOB_CD: mark rows as UPDATE, write rejects (if any), and perform load-modify-store-back against S3 WRK_BIRP_NISS_APRM_DETL
        try:
            self.logger.info("Applying UPD_NISS_SUBLOB_CD update-strategy marking and REJECT handling", node="UPD_NISS_SUBLOB_CD")
            df_UPD_marked = df_EXP_Derive_NISS_SUBLOB_CD_And_PassThru_magical_hilbert.withColumn("dd_op", lit("UPDATE"))

            # write rejects (none expected for DD_UPDATE, but handle generically)
            df_rejects = df_UPD_marked.filter(col("dd_op") == "REJECT").withColumn("error_reason", lit("DD_REJECT"))
            try:
                if not df_rejects.isEmpty():
                    self.logger.info("Writing UPD_NISS_SUBLOB_CD reject rows to S3 reject prefix", table="WRK_BIRP_NISS_APRM_DETL")
                    self.parquet_writer.write(
                        dataframe=df_rejects,
                        path=f"s3://{self.s3_output_bucket}/reject/WRK_BIRP_NISS_APRM_DETL/",
                        mode="overwrite",
                    )
            except Exception:
                self.logger.exception("Failed writing UPD_NISS_SUBLOB_CD reject rows to S3")
                raise

            # drop rejects from the apply set
            df_apply = df_UPD_marked.filter(col("dd_op") != "REJECT")

            # load current target from S3
            try:
                self.logger.info("Reading existing WRK_BIRP_NISS_APRM_DETL target from S3 for load-modify-store-back", table="WRK_BIRP_NISS_APRM_DETL")
                df_existing_target = self.parquet_reader.read(path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/")
            except Exception:
                self.logger.exception("Failed reading existing WRK_BIRP_NISS_APRM_DETL from S3")
                raise

            # determine changed keys
            try:
                changed_keys = df_apply.select("NISS_APRM_DETL_SK").distinct()

                # drop rows in the existing target that will be updated/deleted
                survivors = df_existing_target.join(changed_keys, on="NISS_APRM_DETL_SK", how="left_anti")

                # only INSERT/UPDATE rows are applied back in (DELETE rows would be omitted)
                rows_to_upsert = df_apply.filter(col("dd_op").isin("INSERT", "UPDATE")).drop("dd_op")

                # union survivors with the upsert rows to form the new full target
                # ensure unionByName to align columns by name
                apply_df = survivors.unionByName(rows_to_upsert, allowMissingColumns=True)
            except Exception:
                self.logger.exception("Failed preparing apply dataframe for WRK_BIRP_NISS_APRM_DETL")
                raise

            # write the combined dataframe back to the same S3 target (overwrite)
            try:
                self.logger.info("Writing applied WRK_BIRP_NISS_APRM_DETL to S3 as parquet (overwrite)", table="WRK_BIRP_NISS_APRM_DETL")
                self.parquet_writer.write(
                    dataframe=apply_df,
                    path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                    mode="overwrite",
                )
            except Exception:
                self.logger.exception("Failed writing applied WRK_BIRP_NISS_APRM_DETL to S3")
                raise

            # assign output df name for downstream/lineage
            df_UPD_NISS_SUBLOB_CD_eager_hilbert = df_apply
        except Exception:
            self.logger.exception("Failed executing UPD_NISS_SUBLOB_CD update strategy")
            raise

        # FDR_LIB_WRK_BIRP_NISS_APRM_DETL1: final write of WRK_BIRP_NISS_APRM_DETL as parquet to S3 (mapping target)
        df_WRK_BIRP_NISS_APRM_DETL_amazing_mendel = df_UPD_NISS_SUBLOB_CD_eager_hilbert

        try:
            self.logger.info("Writing WRK_BIRP_NISS_APRM_DETL to S3 as parquet (overwrite)", table="WRK_BIRP_NISS_APRM_DETL")
            self.parquet_writer.write(
                dataframe=df_WRK_BIRP_NISS_APRM_DETL_amazing_mendel,
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                mode="overwrite",
            )
        except Exception:
            self.logger.exception("Failed writing WRK_BIRP_NISS_APRM_DETL to S3")
            raise

        self.glue_job.commit()

    def finalize(self) -> None:
        self.logger.info("Finalizing m_NU0C_NISS_AUTO_110_ATPRM_DTL_Upd_NYNJSublineCd")

    @staticmethod
    def _require_env(env: Mapping[str, Any], key: str) -> str:

        value = env.get(key)

        if not value or str(value).startswith("REPLACE_WITH_"):
            raise ConfigurationValidationError(
                f"config.yaml 'env.{key}' must be set"
            )

        return str(value)
