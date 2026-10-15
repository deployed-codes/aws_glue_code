import sys
from typing import Any, Mapping

from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import DataFrame

from glue_lib.connector import ParquetReader, ParquetWriter
from glue_lib.exceptions import ConfigurationValidationError
from glue_lib.job.base import BaseJob
from pyspark.sql.functions import col, lit, row_number
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# Informatica mapping m_NU0C_NISS_AUTO_111B_ATPRM_DTL_Upd_CvgAttrSk.
# Every mapping node runs in order inside run().
class Nu0cNissAuto111bAtprmDtlUpdCvgAttrSkJob(BaseJob):

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

        # FDR_LIB_WRK_BIRP_NISS_APRM_DETL1: Source - S3-first read of staged WRK_BIRP_NISS_APRM_DETL, project CVG_ATTR_CHCKSUM
        try:
            self.logger.info('Attempting S3 parquet read for FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 (staged)', node='FDR_LIB_WRK_BIRP_NISS_APRM_DETL1')
            df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_careful_babbage = self.parquet_reader.read(
                path=f's3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/',
                options=None,
            ).select('CVG_ATTR_CHCKSUM')
            if df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_careful_babbage.isEmpty():
                self.logger.warning('Source FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 returned no rows from S3', node='FDR_LIB_WRK_BIRP_NISS_APRM_DETL1')
        except Exception:
            self.logger.warning('S3 read for FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 failed; falling back to Glue Catalog read', node='FDR_LIB_WRK_BIRP_NISS_APRM_DETL1')
            try:
                dyf = self.glue_context.create_dynamic_frame.from_catalog(
                    database=self.glue_database,
                    table_name='WRK_BIRP_NISS_APRM_DETL',
                    transformation_ctx='df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_careful_babbage',
                )
                df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_careful_babbage = dyf.toDF().select('CVG_ATTR_CHCKSUM')
                if df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_careful_babbage.isEmpty():
                    self.logger.warning('Source FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 returned no rows from Glue Catalog', node='FDR_LIB_WRK_BIRP_NISS_APRM_DETL1')
            except Exception:
                self.logger.exception('Failed reading FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 from Glue Catalog after S3 fallback')
                raise

        # FDR_LIB_WRK_BIRP_NISS_APRM_DETL: Source - S3-first read of staged WRK_BIRP_NISS_APRM_DETL, project CVG_ATTR_CHCKSUM
        try:
            self.logger.info('Attempting S3 parquet read for FDR_LIB_WRK_BIRP_NISS_APRM_DETL (staged)', node='FDR_LIB_WRK_BIRP_NISS_APRM_DETL')
            df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_trusting_noether = self.parquet_reader.read(
                path=f's3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/',
                options=None,
            ).select('CVG_ATTR_CHCKSUM')
            if df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_trusting_noether.isEmpty():
                self.logger.warning('Source FDR_LIB_WRK_BIRP_NISS_APRM_DETL returned no rows from S3', node='FDR_LIB_WRK_BIRP_NISS_APRM_DETL')
        except Exception:
            self.logger.warning('S3 read for FDR_LIB_WRK_BIRP_NISS_APRM_DETL failed; falling back to Glue Catalog read', node='FDR_LIB_WRK_BIRP_NISS_APRM_DETL')
            try:
                dyf = self.glue_context.create_dynamic_frame.from_catalog(
                    database=self.glue_database,
                    table_name='WRK_BIRP_NISS_APRM_DETL',
                    transformation_ctx='df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_trusting_noether',
                )
                df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_trusting_noether = dyf.toDF().select('CVG_ATTR_CHCKSUM')
                if df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_trusting_noether.isEmpty():
                    self.logger.warning('Source FDR_LIB_WRK_BIRP_NISS_APRM_DETL returned no rows from Glue Catalog', node='FDR_LIB_WRK_BIRP_NISS_APRM_DETL')
            except Exception:
                self.logger.exception('Failed reading FDR_LIB_WRK_BIRP_NISS_APRM_DETL from Glue Catalog after S3 fallback')
                raise

        # SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1: SQL Override rewritten to use staged temp view
        try:
            self.logger.info('Registering temp view for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 and executing SQL override', node='SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1')
            # register the upstream staged dataframe as the real table name so the override can run against it
            df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_careful_babbage.createOrReplaceTempView('WRK_BIRP_NISS_APRM_DETL')
            sql_query = f'''SELECT DISTINCT CVG_ATTR_CHCKSUM
FROM WRK_BIRP_NISS_APRM_DETL
'''
            df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_affectionate_franklin = self.spark.sql(sql_query).select('CVG_ATTR_CHCKSUM')
            if df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_affectionate_franklin.isEmpty():
                self.logger.warning('SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 returned no rows from SQL override', node='SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1')
        except Exception:
            self.logger.exception('Failed executing SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1 SQL override')
            raise

        # SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL: Application Source Qualifier rewritten to use staged temp view
        try:
            self.logger.info('Registering temp view for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL and executing SQL override', node='SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL')
            df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_trusting_noether.createOrReplaceTempView('WRK_BIRP_NISS_APRM_DETL')
            sql_query = f'''SELECT
D.NISS_APRM_DETL_SK,
D.CVG_ATTR_CHCKSUM

FROM WRK_BIRP_NISS_APRM_DETL D
'''
            df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_admiring_bohr = self.spark.sql(sql_query).select('NISS_APRM_DETL_SK', 'CVG_ATTR_CHCKSUM')
            if df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_admiring_bohr.isEmpty():
                self.logger.warning('SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL returned no rows from SQL override', node='SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL')
        except Exception:
            self.logger.exception('Failed executing SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL SQL override')
            raise

        # EXPTRANS: derive gap-free surrogate CVG_ATTR_SK and passthrough CVG_ATTR_CHCKSUM
        try:
            self.logger.info('Applying EXPTRANS expressions (derive CVG_ATTR_SK)', node='EXPTRANS')
            w = Window.orderBy(lit(1))
            df_EXPTRANS_charming_hopper = (
                df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_affectionate_franklin
                .withColumn('CVG_ATTR_SK', row_number().over(w).cast('bigint'))
                # preserve passthrough CVG_ATTR_CHCKSUM
                .select('CVG_ATTR_CHCKSUM', 'CVG_ATTR_SK')
            )
        except Exception:
            self.logger.exception('Failed applying EXPTRANS expressions')
            raise

        # EXPTRANS1: pass through NISS_APRM_DETL_SK and CVG_ATTR_CHCKSUM
        try:
            self.logger.info('Applying EXPTRANS1 passthroughs', node='EXPTRANS1')
            df_EXPTRANS1_gentle_hawking = df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_admiring_bohr.select('NISS_APRM_DETL_SK', 'CVG_ATTR_CHCKSUM')
        except Exception:
            self.logger.exception('Failed applying EXPTRANS1 passthroughs')
            raise

        # JNRTRANS: inner join detail (df_EXPTRANS_charming_hopper) with master (df_EXPTRANS1_gentle_hawking) on CVG_ATTR_CHCKSUM
        try:
            self.logger.info('Performing JNRTRANS join between detail and master', node='JNRTRANS')
            d = df_EXPTRANS_charming_hopper.alias('d')
            m = df_EXPTRANS1_gentle_hawking.alias('m')
            join_cond = (col('d.CVG_ATTR_CHCKSUM') == col('m.CVG_ATTR_CHCKSUM'))
            df_JNRTRANS_adoring_shannon = (
                d.join(m, join_cond, how='inner')
                # select the declared output ports: NISS_APRM_DETL_SK (from master side), CVG_ATTR_SK (from detail side)
                .select(col('m.NISS_APRM_DETL_SK').alias('NISS_APRM_DETL_SK'), col('d.CVG_ATTR_SK').alias('CVG_ATTR_SK'))
            )
        except Exception:
            self.logger.exception('Failed performing JNRTRANS join')
            raise

        # EXP_PassThrough: pass through NISS_APRM_DETL_SK and CVG_ATTR_SK unchanged
        try:
            self.logger.info('Applying EXP_PassThrough passthroughs', node='EXP_PassThrough')
            df_EXP_PassThrough_trusting_euclid = df_JNRTRANS_adoring_shannon.select('NISS_APRM_DETL_SK', 'CVG_ATTR_SK')
        except Exception:
            self.logger.exception('Failed applying EXP_PassThrough passthroughs')
            raise

        # Upd_CVG_ATTR_SK: Update Strategy - mark every row as UPDATE, write rejects (if any), drop REJECTs and pass through survivors
        try:
            self.logger.info('Applying Upd_CVG_ATTR_SK update-strategy marking', node='Upd_CVG_ATTR_SK')
            df_marked = df_EXP_PassThrough_trusting_euclid.withColumn('dd_op', lit('UPDATE'))
            # write rejects (dd_op == 'REJECT') if any - in this mapping there are none, but implement the sink per rule
            try:
                df_rejects = df_marked.filter(col('dd_op') == 'REJECT').withColumn('error_reason', lit('DD_REJECT'))
                if not df_rejects.isEmpty():
                    self.logger.info('Writing rejected rows from Upd_CVG_ATTR_SK to reject prefix', node='Upd_CVG_ATTR_SK')
                    self.parquet_writer.write(
                        dataframe=df_rejects,
                        path=f's3://{self.s3_output_bucket}/reject/WRK_BIRP_NISS_APRM_DETL/',
                        mode='overwrite',
                    )
            except Exception:
                self.logger.exception('Failed writing rejects for Upd_CVG_ATTR_SK')
                raise

            # drop rejects from the apply
            df_Upd_CVG_ATTR_SK_dazzling_kepler = df_marked.filter(col('dd_op') != 'REJECT')
        except Exception:
            self.logger.exception('Failed applying Upd_CVG_ATTR_SK update-strategy')
            raise

        # FDR_LIB_WRK_BIRP_NISS_APRM_DETL: Output - load-modify-store-back apply to s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/
        try:
            self.logger.info('Applying load-modify-store-back for target WRK_BIRP_NISS_APRM_DETL', node='FDR_LIB_WRK_BIRP_NISS_APRM_DETL')
            # read existing target (treat missing path as empty)
            try:
                existing_df = self.parquet_reader.read(path=f's3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/', options=None)
            except Exception:
                self.logger.warning('Existing target WRK_BIRP_NISS_APRM_DETL not found on S3; treating as empty', node='FDR_LIB_WRK_BIRP_NISS_APRM_DETL')
                # create an empty dataframe with the same schema as incoming apply rows where possible
                existing_df = self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), schema=df_Upd_CVG_ATTR_SK_dazzling_kepler.schema)

            # determine changed keys (INSERT/UPDATE/DELETE). The mapping marks all as UPDATE
            changed_keys = df_Upd_CVG_ATTR_SK_dazzling_kepler.select('NISS_APRM_DETL_SK').distinct()

            # anti-join to remove rows being updated/deleted from existing
            existing_anti = existing_df.join(changed_keys, on=['NISS_APRM_DETL_SK'], how='left_anti')

            # rows to apply: keep INSERT and UPDATE (DELETE would be omitted)
            to_apply = df_Upd_CVG_ATTR_SK_dazzling_kepler.filter(col('dd_op').isin('INSERT', 'UPDATE')).drop('dd_op')

            # union the surviving existing rows with the incoming apply rows; allowMissingColumns=True to accommodate differing schemas
            try:
                final_df = existing_anti.unionByName(to_apply, allowMissingColumns=True)
            except TypeError:
                # older Spark variant may not accept allowMissingColumns named arg without keyword compatibility - fallback to positional
                final_df = existing_anti.unionByName(to_apply, True)

            # assign the node's output df_name to the resulting full-table dataframe so downstream code (and validation) sees it
            df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_mystifying_gauss = final_df

            # note: this operation reads and overwrites the entire target - potentially expensive on large tables
            try:
                self.logger.info('Writing final WRK_BIRP_NISS_APRM_DETL to S3 as parquet (overwrite)', table='WRK_BIRP_NISS_APRM_DETL')
                self.parquet_writer.write(
                    dataframe=final_df,
                    path=f's3://{self.s3_output_bucket}/WRK_BIRP_NISS_APRM_DETL/',
                    mode='overwrite',
                )
            except Exception:
                self.logger.exception('Failed writing WRK_BIRP_NISS_APRM_DETL to S3')
                raise
        except Exception:
            self.logger.exception('Failed applying FDR_LIB_WRK_BIRP_NISS_APRM_DETL output logic')
            raise

        self.glue_job.commit()

    def finalize(self) -> None:
        self.logger.info("Finalizing m_NU0C_NISS_AUTO_111B_ATPRM_DTL_Upd_CvgAttrSk")

    @staticmethod
    def _require_env(env: Mapping[str, Any], key: str) -> str:

        value = env.get(key)

        if not value or str(value).startswith("REPLACE_WITH_"):
            raise ConfigurationValidationError(
                f"config.yaml 'env.{key}' must be set"
            )

        return str(value)
