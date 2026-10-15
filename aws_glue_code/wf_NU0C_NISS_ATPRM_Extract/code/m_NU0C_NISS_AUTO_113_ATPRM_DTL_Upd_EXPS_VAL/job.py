import sys
from typing import Any, Mapping

from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import DataFrame

from glue_lib.connector import ParquetReader, ParquetWriter
from glue_lib.exceptions import ConfigurationValidationError
from glue_lib.job.base import BaseJob
from pyspark.sql.functions import col, lit, round as spark_round


# Informatica mapping m_NU0C_NISS_AUTO_113_ATPRM_DTL_Upd_EXPS_VAL.
# Every mapping node runs in order inside run().
class Nu0cNissAuto113AtprmDtlUpdExpsValJob(BaseJob):

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

        # Read source WRK_BIRP_NISS_APRM_DETL from S3 first, fall back to Glue Catalog
        try:
            self.logger.info("Attempting S3 parquet read for FDR_LIB_WRK_BIRP_NISS_APRM_DETL", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_silly_franklin = self.parquet_reader.read(
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                options=None,
            ).select("ACCTNG_LOB")
            # log which branch ran
            self.logger.info("Read FDR_LIB_WRK_BIRP_NISS_APRM_DETL from S3", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            if df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_silly_franklin.isEmpty():
                self.logger.warning("Source FDR_LIB_WRK_BIRP_NISS_APRM_DETL returned no rows", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
        except Exception:
            # fallback to Glue Catalog
            try:
                self.logger.warning("S3 read failed for FDR_LIB_WRK_BIRP_NISS_APRM_DETL; falling back to Glue Catalog", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
                dyf = self.glue_context.create_dynamic_frame.from_catalog(
                    database=self.glue_database,
                    table_name="WRK_BIRP_NISS_APRM_DETL",
                    transformation_ctx="df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_silly_franklin",
                )
                df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_silly_franklin = dyf.toDF().select("ACCTNG_LOB")
                if df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_silly_franklin.isEmpty():
                    self.logger.warning("Catalog source WRK_BIRP_NISS_APRM_DETL is empty", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            except Exception:
                self.logger.exception("Failed reading FDR_LIB_WRK_BIRP_NISS_APRM_DETL from S3 and Glue Catalog")
                raise

        # Second Source instance (same underlying table) - S3-first read with same fallback
        try:
            self.logger.info("Attempting S3 parquet read for FDR_LIB_WRK_BIRP_NISS_APRM_DETL2", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL2")
            df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL2_humble_maxwell = self.parquet_reader.read(
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                options=None,
            ).select("TTL_WRITTN_PREM_AMT")
            self.logger.info("Read FDR_LIB_WRK_BIRP_NISS_APRM_DETL (instance 2) from S3", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL2")
            if df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL2_humble_maxwell.isEmpty():
                self.logger.warning("Source FDR_LIB_WRK_BIRP_NISS_APRM_DETL (instance 2) returned no rows", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL2")
        except Exception:
            try:
                self.logger.warning("S3 read failed for FDR_LIB_WRK_BIRP_NISS_APRM_DETL2; falling back to Glue Catalog", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL2")
                dyf = self.glue_context.create_dynamic_frame.from_catalog(
                    database=self.glue_database,
                    table_name="WRK_BIRP_NISS_APRM_DETL",
                    transformation_ctx="df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL2_humble_maxwell",
                )
                df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL2_humble_maxwell = dyf.toDF().select("TTL_WRITTN_PREM_AMT")
                if df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL2_humble_maxwell.isEmpty():
                    self.logger.warning("Catalog source WRK_BIRP_NISS_APRM_DETL (instance 2) is empty", node="FDR_LIB_WRK_BIRP_NISS_APRM_DETL2")
            except Exception:
                self.logger.exception("Failed reading FDR_LIB_WRK_BIRP_NISS_APRM_DETL (instance 2) from S3 and Glue Catalog")
                raise

        # SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL: staged-case SQL override against the temp view of earlier source
        sql_query = f"""SELECT
NISS_APRM_DETL_SK,
ROUND((CVG_EXPS_VAL*12)) AS CVG_EXPS_VAL,
PLCY_CNTRCT_NUM,
UNIT_NUM,
EFF_DT

FROM WRK_BIRP_NISS_APRM_DETL

WHERE CVG_TYP_IND ='B'"""

        try:
            # register the upstream staged dataframe as the bare table name the override expects
            self.logger.info("Registering temp view WRK_BIRP_NISS_APRM_DETL for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL", node="SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            # use the full dataframe that contains all columns - ensure view exists (we may have only selected ACCTNG_LOB earlier for brevity)
            # Re-read full staged table to register full view for the SQL override
            try:
                df_view_src = self.parquet_reader.read(path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/", options=None)
                self.logger.info("Registered temp view from S3 parquet for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            except Exception:
                dyf_temp = self.glue_context.create_dynamic_frame.from_catalog(
                    database=self.glue_database,
                    table_name="WRK_BIRP_NISS_APRM_DETL",
                    transformation_ctx="df_for_sq_temp_view_FDR_LIB_WRK_BIRP_NISS_APRM_DETL",
                )
                df_view_src = dyf_temp.toDF()
                self.logger.info("Registered temp view from Glue Catalog for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL")

            df_view_src.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")

            self.logger.info("Executing SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL SQL override via spark.sql", node="SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
            df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_optimistic_kant = self.spark.sql(sql_query).select(
                "NISS_APRM_DETL_SK",
                "CVG_EXPS_VAL",
                "PLCY_CNTRCT_NUM",
                "UNIT_NUM",
                "EFF_DT",
            )
            if df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_optimistic_kant.isEmpty():
                self.logger.warning("SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL returned no rows", node="SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL")
        except Exception:
            self.logger.exception("Failed executing SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL SQL override")
            raise

        # SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1: staged-case CustomSQLQuery executed against the temp view
        sql_query = f"""SELECT
C.NISS_APRM_DETL_SK,
C.CVG_EXPS_VAL,
C.PLCY_CNTRCT_NUM,
C.UNIT_NUM,
C.EFF_DT
--CVG_TYP_IND
from 
(SELECT 
B.NISS_APRM_DETL_SK,
B.CVG_EXPS_VAL,
B.PLCY_CNTRCT_NUM,
B.UNIT_NUM,
B.EFF_DT,
ROW_NUMBER() OVER  ( PARTITION BY 
B.PLCY_CNTRCT_NUM,
B.UNIT_NUM,
B.EFF_DT
ORDER BY 
B.CVG_EXPS_VAL DESC
) AS ROW_RANK

FROM FDR.WRK_BIRP_NISS_APRM_DETL B  where not exists
(SELECT 1 FROM FDR.WRK_BIRP_NISS_APRM_DETL A WHERE A.CVG_TYP_IND ='B' and A.PLCY_CNTRCT_NUM=B.PLCY_CNTRCT_NUM)  ) C

WHERE C.ROW_RANK=1"""

        try:
            # ensure temp view for the ASQ's associated source is registered
            self.logger.info("Registering temp view WRK_BIRP_NISS_APRM_DETL for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1", node="SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1")
            try:
                df_view_src2 = self.parquet_reader.read(path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/", options=None)
            except Exception:
                dyf_temp2 = self.glue_context.create_dynamic_frame.from_catalog(
                    database=self.glue_database,
                    table_name="WRK_BIRP_NISS_APRM_DETL",
                    transformation_ctx="df_for_sq_temp_view_FDR_LIB_WRK_BIRP_NISS_APRM_DETL2",
                )
                df_view_src2 = dyf_temp2.toDF()
            df_view_src2.createOrReplaceTempView("WRK_BIRP_NISS_APRM_DETL")

            self.logger.info("Executing SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 CustomSQLQuery via spark.sql", node="SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1")
            df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_daring_euclid = self.spark.sql(sql_query).select(
                "NISS_APRM_DETL_SK",
                "CVG_EXPS_VAL",
                "PLCY_CNTRCT_NUM",
                "UNIT_NUM",
                "EFF_DT",
            )
            if df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_daring_euclid.isEmpty():
                self.logger.warning("SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 returned no rows", node="SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1")
        except Exception:
            self.logger.exception("Failed executing SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 SQL override")
            raise

        # Exp_before_tgt: passthrough select from first SQ
        try:
            self.logger.info("Applying Exp_before_tgt passthrough select", node="Exp_before_tgt")
            df_Exp_before_tgt_reverent_babbage = df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_optimistic_kant.select(
                "NISS_APRM_DETL_SK",
                "CVG_EXPS_VAL",
                "PLCY_CNTRCT_NUM",
                "UNIT_NUM",
                "EFF_DT",
            )
        except Exception:
            self.logger.exception("Failed applying Exp_before_tgt passthrough select")
            raise

        # Exp_before_tgt1: compute EXP_VAL_ROLLED = ROUND(CVG_EXPS_VAL * 12) and pass through other ports
        try:
            self.logger.info("Applying Exp_before_tgt1 expression to compute EXP_VAL_ROLLED", node="Exp_before_tgt1")
            df_Exp_before_tgt1_awesome_shannon = (
                df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_daring_euclid
                .withColumn("EXP_VAL_ROLLED", spark_round(col("CVG_EXPS_VAL") * 12))
                .select(
                    "NISS_APRM_DETL_SK",
                    "CVG_EXPS_VAL",
                    "PLCY_CNTRCT_NUM",
                    "UNIT_NUM",
                    "EFF_DT",
                    "EXP_VAL_ROLLED",
                )
            )
        except Exception:
            self.logger.exception("Failed applying Exp_before_tgt1 expression")
            raise

        # Upd_EXP_UPD: mark dd_op='UPDATE', write rejects (none expected), then load-modify-store-back against WRK_BIRP_NISS_APRM_DETL
        try:
            self.logger.info("Applying Upd_EXP_UPD update-strategy logic (mark rows and apply to target)", node="Upd_EXP_UPD")
            # mark every incoming row as UPDATE per DD_UPDATE
            df_marked = df_Exp_before_tgt_reverent_babbage.withColumn("dd_op", lit("UPDATE"))

            # write rejects (if any) to reject prefix
            try:
                df_rejects = df_marked.filter(col("dd_op") == "REJECT").withColumn("error_reason", lit("DD_REJECT"))
                if not df_rejects.isEmpty():
                    self.logger.info("Writing REJECT rows for Upd_EXP_UPD to reject prefix", node="Upd_EXP_UPD")
                    self.parquet_writer.write(
                        dataframe=df_rejects,
                        path=f"s3://{self.s3_output_bucket}/reject/WRK_BIRP_NISS_APRM_DETL/",
                        mode="overwrite",
                    )
            except Exception:
                self.logger.exception("Failed writing REJECT rows for Upd_EXP_UPD")
                raise

            # drop rejects from apply
            df_apply = df_marked.filter(col("dd_op") != "REJECT").drop("dd_op")

            # load current full target (expensive on large targets)
            try:
                self.logger.info("Loading existing WRK_BIRP_NISS_APRM_DETL for load-modify-store-back", node="Upd_EXP_UPD")
                existing_df = self.parquet_reader.read(path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/", options=None)
            except Exception:
                self.logger.exception("Failed reading existing WRK_BIRP_NISS_APRM_DETL for Upd_EXP_UPD")
                raise

            # remove existing rows that will be replaced (by primary key NISS_APRM_DETL_SK)
            try:
                changed_keys = df_apply.select("NISS_APRM_DETL_SK").distinct()
                existing_minus_changed = existing_df.join(changed_keys, on="NISS_APRM_DETL_SK", how="left_anti")
            except Exception:
                self.logger.exception("Failed computing existing_minus_changed for Upd_EXP_UPD")
                raise

            # build updated_rows by joining existing rows to incoming changes and overwriting the columns present in the incoming df
            try:
                changed_cols = {"CVG_EXPS_VAL", "PLCY_CNTRCT_NUM", "UNIT_NUM", "EFF_DT"}
                joined = existing_df.alias("e").join(df_apply.alias("c"), col("e.NISS_APRM_DETL_SK") == col("c.NISS_APRM_DETL_SK"), "inner")
                select_exprs = [
                    (col("c." + coln).alias(coln) if coln in changed_cols else col("e." + coln).alias(coln))
                    for coln in existing_df.columns
                ]
                updated_rows = joined.select(*select_exprs)
            except Exception:
                self.logger.exception("Failed building updated_rows for Upd_EXP_UPD")
                raise

            # union remaining existing rows with updated rows to create final table
            try:
                final_df = existing_minus_changed.unionByName(updated_rows)
            except Exception:
                self.logger.exception("Failed unioning final dataframe for Upd_EXP_UPD")
                raise

            # persist the combined result back to the same target path (overwrite)
            try:
                self.logger.info("Writing updated WRK_BIRP_NISS_APRM_DETL (overwrite) for Upd_EXP_UPD", node="Upd_EXP_UPD")
                self.parquet_writer.write(
                    dataframe=final_df,
                    path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                    mode="overwrite",
                )
            except Exception:
                self.logger.exception("Failed writing updated WRK_BIRP_NISS_APRM_DETL for Upd_EXP_UPD")
                raise

            # assign output df for downstream
            df_Upd_EXP_UPD_modest_ramanujan = final_df

        except Exception:
            self.logger.exception("Failed processing Upd_EXP_UPD update strategy")
            raise

        # Upd_EXP_UPD_CVG_IND_NOT_B: identical pattern but applying EXP_VAL_ROLLED updates (and other ports) from df_Exp_before_tgt1_awesome_shannon
        try:
            self.logger.info("Applying Upd_EXP_UPD_CVG_IND_NOT_B update-strategy logic (mark rows and apply to target)", node="Upd_EXP_UPD_CVG_IND_NOT_B")
            df_marked2 = df_Exp_before_tgt1_awesome_shannon.withColumn("dd_op", lit("UPDATE"))

            # write rejects if any
            try:
                df_rejects2 = df_marked2.filter(col("dd_op") == "REJECT").withColumn("error_reason", lit("DD_REJECT"))
                if not df_rejects2.isEmpty():
                    self.logger.info("Writing REJECT rows for Upd_EXP_UPD_CVG_IND_NOT_B to reject prefix", node="Upd_EXP_UPD_CVG_IND_NOT_B")
                    self.parquet_writer.write(
                        dataframe=df_rejects2,
                        path=f"s3://{self.s3_output_bucket}/reject/WRK_BIRP_NISS_APRM_DETL/",
                        mode="overwrite",
                    )
            except Exception:
                self.logger.exception("Failed writing REJECT rows for Upd_EXP_UPD_CVG_IND_NOT_B")
                raise

            df_apply2 = df_marked2.filter(col("dd_op") != "REJECT").drop("dd_op")

            # load existing target
            try:
                self.logger.info("Loading existing WRK_BIRP_NISS_APRM_DETL for load-modify-store-back (CVG_IND_NOT_B)", node="Upd_EXP_UPD_CVG_IND_NOT_B")
                existing_df2 = self.parquet_reader.read(path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/", options=None)
            except Exception:
                self.logger.exception("Failed reading existing WRK_BIRP_NISS_APRM_DETL for Upd_EXP_UPD_CVG_IND_NOT_B")
                raise

            try:
                changed_keys2 = df_apply2.select("NISS_APRM_DETL_SK").distinct()
                existing_minus_changed2 = existing_df2.join(changed_keys2, on="NISS_APRM_DETL_SK", how="left_anti")
            except Exception:
                self.logger.exception("Failed computing existing_minus_changed for Upd_EXP_UPD_CVG_IND_NOT_B")
                raise

            try:
                # columns updated by this update-strategy include EXP_VAL_ROLLED (plus passthrough ports)
                changed_cols2 = {"EXP_VAL_ROLLED", "CVG_EXPS_VAL", "PLCY_CNTRCT_NUM", "UNIT_NUM", "EFF_DT"}
                joined2 = existing_df2.alias("e").join(df_apply2.alias("c"), col("e.NISS_APRM_DETL_SK") == col("c.NISS_APRM_DETL_SK"), "inner")
                select_exprs2 = [
                    (col("c." + coln).alias(coln) if coln in changed_cols2 else col("e." + coln).alias(coln))
                    for coln in existing_df2.columns
                ]
                updated_rows2 = joined2.select(*select_exprs2)
            except Exception:
                self.logger.exception("Failed building updated_rows for Upd_EXP_UPD_CVG_IND_NOT_B")
                raise

            try:
                final_df2 = existing_minus_changed2.unionByName(updated_rows2)
            except Exception:
                self.logger.exception("Failed unioning final dataframe for Upd_EXP_UPD_CVG_IND_NOT_B")
                raise

            try:
                self.logger.info("Writing updated WRK_BIRP_NISS_APRM_DETL (overwrite) for Upd_EXP_UPD_CVG_IND_NOT_B", node="Upd_EXP_UPD_CVG_IND_NOT_B")
                self.parquet_writer.write(
                    dataframe=final_df2,
                    path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                    mode="overwrite",
                )
            except Exception:
                self.logger.exception("Failed writing updated WRK_BIRP_NISS_APRM_DETL for Upd_EXP_UPD_CVG_IND_NOT_B")
                raise

            df_Upd_EXP_UPD_CVG_IND_NOT_B_zen_galileo = final_df2

        except Exception:
            self.logger.exception("Failed processing Upd_EXP_UPD_CVG_IND_NOT_B update strategy")
            raise

        # FDR_LIB_WRK_BIRP_NISS_APRM_DETL1: write the upstream Update Strategy result to S3 as parquet (overwrite)
        # write intermediate WRK_ table as parquet to S3 (overwrite)
        try:
            self.logger.info("Writing WRK_BIRP_NISS_APRM_DETL to S3 as parquet (overwrite) from Upd_EXP_UPD output", table="WRK_BIRP_NISS_APRM_DETL")
            df_WRK_BIRP_NISS_APRM_DETL_loving_gauss = df_Upd_EXP_UPD_modest_ramanujan
            self.parquet_writer.write(
                dataframe=df_WRK_BIRP_NISS_APRM_DETL_loving_gauss,
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                mode="overwrite",
            )
        except Exception:
            self.logger.exception("Failed writing WRK_BIRP_NISS_APRM_DETL from Upd_EXP_UPD to S3")
            raise

        # FDR_LIB_WRK_BIRP_NISS_APRM_DETL3: write the other Update Strategy result to S3 as parquet (overwrite)
        # write intermediate WRK_ table as parquet to S3 (overwrite)
        try:
            self.logger.info("Writing WRK_BIRP_NISS_APRM_DETL to S3 as parquet (overwrite) from Upd_EXP_UPD_CVG_IND_NOT_B output", table="WRK_BIRP_NISS_APRM_DETL")
            df_WRK_BIRP_NISS_APRM_DETL_heroic_descartes = df_Upd_EXP_UPD_CVG_IND_NOT_B_zen_galileo
            self.parquet_writer.write(
                dataframe=df_WRK_BIRP_NISS_APRM_DETL_heroic_descartes,
                path=f"s3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/",
                mode="overwrite",
            )
        except Exception:
            self.logger.exception("Failed writing WRK_BIRP_NISS_APRM_DETL from Upd_EXP_UPD_CVG_IND_NOT_B to S3")
            raise

        self.glue_job.commit()

    def finalize(self) -> None:
        self.logger.info("Finalizing m_NU0C_NISS_AUTO_113_ATPRM_DTL_Upd_EXPS_VAL")

    @staticmethod
    def _require_env(env: Mapping[str, Any], key: str) -> str:

        value = env.get(key)

        if not value or str(value).startswith("REPLACE_WITH_"):
            raise ConfigurationValidationError(
                f"config.yaml 'env.{key}' must be set"
            )

        return str(value)
