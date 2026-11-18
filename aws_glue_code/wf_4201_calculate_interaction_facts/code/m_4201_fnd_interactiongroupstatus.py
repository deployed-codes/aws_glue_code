import sys
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from utils import *

args = getResolvedOptions(sys.argv, ["JOB_NAME", "ENV", "GLUE_DATABASE", "S3_OUTPUT_BUCKET", "SNOWFLAKE_SECRET_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()

# Resolved once here so every mapping script stops inventing its own differently-spelled
# placeholder for the same handful of values (six spellings of "the Glue database" alone,
# across scripts, before this). Node-level code must reference these directly, never
# redeclare them.
ENV = args["ENV"]
GLUE_DATABASE = args["GLUE_DATABASE"]
S3_OUTPUT_BUCKET = args["S3_OUTPUT_BUCKET"]

SNOWFLAKE_SECRET_NAME = args["SNOWFLAKE_SECRET_NAME"]
SNOWFLAKE_URL, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD = get_snowflake_connection(SNOWFLAKE_SECRET_NAME)


# Top-of-script placeholders for Informatica mapping parameters used in the SQ override
INTERACTION_WORK_DB = "REPLACE_WITH_INTERACTION_WORK_DB_VALUE"
INTERACTION_FND_DB = "REPLACE_WITH_INTERACTION_FND_DB_VALUE"
EXCLUDED_SOURCES = "REPLACE_WITH_EXCLUDED_SOURCES_VALUE"  # expected to be a SQL-list literal like "'SRC1','SRC2'"
IGS_EXCLUDED_SOURCE = "REPLACE_WITH_IGS_EXCLUDED_SOURCE_VALUE"  # expected to be a SQL-list literal
JOB_CD = "REPLACE_WITH_JOB_CD_VALUE"

from pyspark.sql.functions import trim, upper, lpad, col, concat, lit, when, broadcast
from pyspark.sql import types as T

# Source: InteractionEvent
try:
    logger.info("Reading Source InteractionEvent from Glue Data Catalog into df_InteractionEvent_loving_hawking")
    dyf_InteractionEvent_loving_hawking = glueContext.create_dynamic_frame_from_catalog(
        database=GLUE_DATABASE,
        table_name="InteractionEvent",
        transformation_ctx="df_InteractionEvent_loving_hawking",
    )
    df_InteractionEvent_loving_hawking = (
        dyf_InteractionEvent_loving_hawking
        .toDF()
        .select(
            # fields selected based on those referenced by downstream SQ/expressions
            col("InteractionEvent_Id"),
            col("InteractionGroup_Id"),
            col("BusinessStatus_Cd"),
            col("Status_Cd"),
            col("StatusReason_Cd"),
            col("ActionType_Tp"),
            col("Action_Tp"),
            col("ActionResult_Tp"),
            col("ActionResultDetail_Tp"),
            col("ActionResultReason_Tp"),
            col("TransactionType_Tp"),
            col("Transaction_Tp"),
            col("TransactionEffective_Dt"),
            col("TransactionExpiration_Dt"),
            col("Transaction_Dt"),
            col("Transaction_Ts"),
            col("Sequence_It"),
            col("Source_Cd"),
            col("LoadEvent_Id"),
            col("AgentKey_Cd"),
            col("Claim_Id"),
            col("ClaimKey_Cd"),
            col("InteractionGroupKey_Cd"),
            col("InteractionGroupBusinessKey_Id"),
            col("Party_Id"),
            col("PartyKey_Cd"),
            col("Policy_Id"),
            col("PolicyKey_Cd"),
            col("ActivityCreatedBy_Nb"),
            col("SystemResult_Cd"),
            col("SystemResultReason_Cd"),
            col("ActivityRemarks_Tt"),
            col("TimeofDay_Cd"),
            col("TimeofDay_Tp"),
            col("InfaMd5_NB")
        )
    )
    if df_InteractionEvent_loving_hawking.rdd.isEmpty():
        logger.warning("Source InteractionEvent returned no rows (df_InteractionEvent_loving_hawking is empty)")
except Exception as e:
    logger.error(f"Failed reading InteractionEvent from Glue catalog: {e}", exc_info=True)
    raise

# SQ: SQ_InteractionEvent (SQL Override executed via JDBC pushdown)
sql_query = f"""
SELECT ROW_NUMBER() OVER (
		ORDER BY P.InteractionGroup_Id
			,P.Sequence_It
		) NUMBER_SEQ
	,P.InteractionGroup_Id
	,P.BusinessStatus_Cd
	,P.Status_Cd
	,P.StatusReason_Cd
	,P.TransactionEffective_Dt
	,P.Transaction_Ts
	,P.Sequence_It
	,P.Source_Cd
	,P.LoadEvent_Id
FROM (
	SELECT VIR_TBL.InteractionGroup_Id AS InteractionGroup_Id
		,VIR_TBL.BusinessStatus_Cd AS BusinessStatus_Cd
		,VIR_TBL.Status_Cd AS Status_Cd
		,VIR_TBL.StatusReason_Cd AS StatusReason_Cd
		,VIR_TBL.Action_Tp AS Action_Tp
		,VIR_TBL.TransactionEffective_Dt AS TransactionEffective_Dt
		,VIR_TBL.Transaction_Ts AS Transaction_Ts
		,VIR_TBL.Sequence_It AS Sequence_It
		,VIR_TBL.Source_Cd AS Source_Cd
		,VIR_TBL.LoadEvent_Id AS LoadEvent_Id
		,ROW_NUMBER() OVER (
			PARTITION BY InteractionGroup_Id ORDER BY InteractionGroup_Id
				,Sequence_It
			) AS Row_Num
		,TO_CHAR(VIR_TBL.InteractionGroup_Id) || VIR_TBL.BusinessStatus_Cd || VIR_TBL.Status_Cd || VIR_TBL.StatusReason_Cd AS Curr_Row
		,COALESCE(MIN(TO_CHAR(VIR_TBL.InteractionGroup_Id) || VIR_TBL.BusinessStatus_Cd || VIR_TBL.Status_Cd || VIR_TBL.StatusReason_Cd) OVER (
				PARTITION BY InteractionGroup_Id 
				ORDER BY InteractionGroup_Id
					     ,Sequence_It 
				ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 0) AS Prev_Row
	FROM (
		SELECT CASE 
		        WHEN IE.source_cd IN ({IGS_EXCLUDED_SOURCE})
                     AND  IE.TransactionType_Tp = 9 
                     THEN 'PHONE'
				WHEN IE.source_cd IN ({EXCLUDED_SOURCES})
					THEN 'Intct'
				WHEN (IE.source_cd NOT IN ( {EXCLUDED_SOURCES} ))AND(IE.source_cd NOT IN ({IGS_EXCLUDED_SOURCE}))
				    THEN CASE
						WHEN IE.TransactionType_Tp = 10
                        THEN 'EMAIL'
						WHEN IE.TransactionType_Tp = 11 
						THEN 'MOBILE'
                        WHEN IE.TransactionType_Tp = 3
							THEN CASE 
								WHEN IE.Action_Tp IN (
										9
										,20
										,22
										)
									OR IE.ActionResultReason_Tp IN (
										62
										,63
										,64
										,65
										)
									THEN 'Trgt'
									WHEN IE.Action_Tp IN (
										5
										,21
										)
									THEN 'Dstrb'
									WHEN IE.ActionType_Tp IN (
										4
										,8
										,9
										,10
										,11
										,12
										)
									THEN 'Intct'
									WHEN IE.ActionType_Tp = 5
										AND IE.Action_Tp IN (
											26
											,27
											,28
											,29
										)
									THEN 'Ident'
									ELSE 'Notdef'
							END
					END AS BusinessStatus_Cd
					,CASE 
					WHEN  IE.source_cd IN ({IGS_EXCLUDED_SOURCE})  AND  IE.TransactionType_Tp = 9 
                    THEN 'A' || TRIM(IE.Action_Tp)		
					WHEN IE.Source_Cd IN  ({EXCLUDED_SOURCES})
						THEN CASE 
								WHEN IE.ActionResultDetail_Tp IN (
									6
									,10
									,50
									,51
									,52
									,58
									)
									THEN 'Closed'
									WHEN IE.ActionResultDetail_Tp IN (
										7
										,8
										,35
										,41
										,53
										,54
										,55
										,56
										,57
										,59
										,60
										,61
										,62
										)
									THEN 'WIP'
									ELSE 'Notdef'
									END
					WHEN (IE.source_cd NOT IN ({EXCLUDED_SOURCES}) )AND (IE.source_cd NOT IN ({IGS_EXCLUDED_SOURCE}))
					THEN CASE
					    WHEN IE.TransactionType_Tp = 10 
					    THEN 'A' || TRIM(IE.Action_Tp)	
						WHEN IE.TransactionType_Tp = 11 
						THEN 'AR' || TRIM(IE.ActionResult_Tp)
						WHEN IE.TransactionType_Tp = 3
						THEN CASE 
							WHEN IE.Action_Tp = 5
							THEN 'Recvd'
							WHEN IE.ActionResult_Tp IN (
								16
								,17
								)
							OR IE.ActionResultReason_Tp = 62
							THEN 'Crtd'
							WHEN IE.Action_Tp IN (
								20
								,22
								)
							OR IE.ActionResultReason_Tp IN (
								63
								,64
								,65
								)
							THEN 'Asgd'
							WHEN IE.Action_Tp = 21
							THEN 'Sent'
							WHEN IE.Action_Tp = 6
							OR IE.ActionType_Tp = 9
							THEN 'Syscld'
							WHEN IE.ActionResultDetail_Tp IN (
								3
								,6
								,10
								,33
								)
							THEN 'Closed'
                                            WHEN IE.ActionResultDetail_Tp IN (32,50)
                                                         AND IE.Transaction_Tp IN (40,41)
                                                         THEN 'Closed'
							WHEN IE.Action_Tp IN (
								3
								,4
								,14
								)
							OR IE.ActionType_Tp IN (
								10
								,11
								)
							THEN 'WIP'
							WHEN IE.ActionType_Tp = 12
							THEN 'View'
							WHEN IE.ActionType_Tp = 5
							AND IE.Action_Tp = 26
							THEN 'Qcrtd'
							WHEN IE.ActionType_Tp = 5
							AND IE.Action_Tp IN (
								27
								,28
								,29
								)
							THEN 'Qabdn'
							ELSE 'Notdef'
							END
					END AS Status_Cd
					,CASE 
					WHEN IE.Source_Cd IN ({IGS_EXCLUDED_SOURCE})
					     AND IE.TransactionType_Tp = 9 
					                    THEN 'AR' || TRIM( IE.ActionResult_Tp)
						WHEN IE.Source_Cd IN ({EXCLUDED_SOURCES})
							THEN TRIM(XREF.source_cd) || CAST(CAST(ActionResultDetail_Tp AS INTEGER FORMAT '9(3)') AS CHAR(3))
						WHEN (XREF.source_cd NOT IN ({EXCLUDED_SOURCES} ))AND (XREF.source_cd NOT IN ({IGS_EXCLUDED_SOURCE}))
					    THEN CASE
								WHEN IE.TransactionType_Tp = 10 
									THEN '@' 
								WHEN IE.TransactionType_Tp = 11 
									THEN '@' 
								WHEN IE.TransactionType_Tp = 3
									THEN CASE 
										WHEN IE.ActionResult_Tp IN (
											16
											,17
											)
											OR IE.ActionResultReason_Tp = 62
											THEN 'Crtd'
											WHEN IE.ActionResultReason_Tp = 5
											OR IE.ActionResult_Tp = 28
											THEN 'CCNW'
											WHEN IE.ActionResultDetail_Tp = 3
											THEN 'AgntRm'
											WHEN IE.ActionResultDetail_Tp = 6
											THEN 'Intrsd'
											WHEN IE.ActionResultDetail_Tp = 10
											THEN 'NotInt'
                                            WHEN IE.ActionResultDetail_Tp = 50
                                                    AND IE.ActionType_Tp = 8
                                                    THEN 'Complt'
											WHEN IE.ActionResultDetail_Tp = 12
											THEN 'CallBk'
											WHEN IE.ActionResult_Tp = 6
											THEN 'Qlfd'
											WHEN IE.ActionResult_Tp = 7
											THEN 'Err'
											WHEN IE.ActionResult_Tp = 10
											OR IE.ActionResultReason_Tp = 54
											THEN 'Expire'
											WHEN IE.ActionResult_Tp = 12
											THEN 'MnRmvd'
											WHEN IE.ActionResult_Tp = 13
											THEN 'Suprs'
											WHEN IE.ActionResult_Tp = 11
											THEN 'KO'
											WHEN IE.ActionResult_Tp IN (
											8
											,9
											)
											OR IE.ActionType_Tp = 9
											THEN 'Gnrl'
											WHEN IE.Action_Tp IN (
											20
											,22
											)
											OR IE.ActionResultReason_Tp IN (
											63
											,64
											,65
											)
											THEN CASE 
												WHEN IE.TransactionReason_Tp = 3
												THEN 'Ctrl'
												ELSE 'Trtbl'
											END
										WHEN IE.Action_Tp = 21
										THEN 'Sent'
										WHEN IE.ActionType_Tp IN (
											4
											,10
											,11
											)
											AND NOT IE.ActionResultDetail_Tp IN (
											3
											,6
											,10
											)
											THEN 'Active'
											WHEN IE.ActionResultDetail_Tp IN (
											32
											,33
											)
											THEN 'AgntMl'
										WHEN IE.ActionResultDetail_Tp = 37
											THEN 'MltSt'
										WHEN IE.ActionResultDetail_Tp = 34
											THEN 'DelPx'
										WHEN IE.ActionResultDetail_Tp = 38
											THEN 'PDRmd'
										WHEN IE.ActionResultDetail_Tp = 40
											THEN 'Sysrmv'
										WHEN IE.ActionResultReason_Tp IN (
											49
											,50
											,55
											,56
											,57
											,58
											,60
											,69
											,70
											)
											THEN 'NoQlf'
										WHEN IE.ActionResult_Tp = 24
											THEN 'Delvd'
										WHEN IE.ActionResult_Tp = 25
											THEN 'Undelv'
										WHEN IE.ActionType_Tp = 12
											THEN 'View'
										WHEN IE.ActionType_Tp = 5
											AND IE.Action_Tp = 26
											THEN 'Qcrtd'
										WHEN IE.ActionType_Tp = 5
											AND IE.Action_Tp IN (
											27
											,28
											,29
											)
											THEN 'Qabdn'
											ELSE 'Notdef'
										END
						END AS StatusReason_Cd
					,IE.InteractionGroup_Id
					,IE.ActionType_Tp
					,IE.Action_Tp
					,IE.ActionResult_Tp
					,IE.ActionResultDetail_Tp
					,IE.ActionResultReason_Tp
					,IE.TransactionEffective_Dt
					,IE.Transaction_Ts
					,IE.Sequence_It
					,IE.Source_Cd
					,IE.LoadEvent_Id
					,CASE
WHEN IE.Transaction_Tp = 40 
THEN 'Include'
when (IE.Action_Tp in ( 15, 57, 58, 59, 60, 61, 62, 63 ) AND (IE.TransactionType_Tp = 10 ))
then 'Include'
when IE.TransactionType_Tp = 11 
then 'Include'

WHEN IE.Action_Tp IN (7, 8, 11, 56)
THEN 'Exclude'
WHEN IE.ActionType_Tp = 3
THEN 'Exclude'
WHEN (IE.Action_Tp = 12 
AND IE.ActionResult_Tp IN (33, 34, 35, 36))
THEN 'Exclude'
WHEN (IE.Action_Tp = 19 
AND IE.ActionResult_Tp IN (33, 34, 35, 36))
THEN 'Exclude'
WHEN IE.ActionResultDetail_Tp = 35 
THEN 'Exclude'
WHEN IE.Action_Tp IN (10, 13) 
THEN 'Exclude'
WHEN ( IE.ActionType_Tp = 5
AND IE.Action_Tp = 10
AND IE.ActionResult_Tp = 7) 
THEN 'Exclude'
WHEN ( IE.ActionType_Tp = 6 
AND IE.Action_Tp = 29) 
THEN 'Exclude'
WHEN ( IE.ActionType_Tp = 6 
AND IE.Action_Tp = 21) 
THEN 'Exclude'
WHEN (ActionResultDetail_Tp = 0 
AND Transaction_Tp = 41 ) 
THEN 'Exclude'
ELSE 'Include'
end AS StatusIncludeExclude_Cd
				FROM {INTERACTION_WORK_DB}.INTERACTIONEVENT IE
				INNER JOIN {INTERACTION_WORK_DB}.LOADEVENT_ID_XREF XREF
				ON TRIM(IE.SOURCE_CD) = TRIM(XREF.Source_Cd)
				AND IE.LOADEVENT_ID = XREF.LOADEVENT_ID
				AND XREF.JOB_CD = {JOB_CD} 
				WHERE (
					XREF.SOURCE_CD NOT IN ({EXCLUDED_SOURCES}) AND XREF.SOURCE_CD NOT IN ({IGS_EXCLUDED_SOURCE})
				     AND StatusIncludeExclude_Cd = 'Include'

					AND NOT EXISTS (
						SELECT ''
						FROM {INTERACTION_WORK_DB}.INTERACTIONEVENT IE2
						WHERE IE.InteractionGroup_Id = IE2.InteractionGroup_Id
							AND XREF.Source_cd = IE2.Source_Cd
							AND IE2.LoadEvent_Id = XREF.LoadEvent_Id
							AND IE.Sequence_It > IE2.Sequence_it
							AND IE.Action_Tp = 22
							AND IE2.Transaction_Tp = 14
							AND (
								IE2.Action_Tp = 6
								OR IE2.ActionType_Tp = 9
								OR IE2.ActionResultDetail_Tp IN (
									3
									,6
									,10
									)
								)
						)
					)
				OR( XREF.SOURCE_CD IN( {EXCLUDED_SOURCES} , {IGS_EXCLUDED_SOURCE} )	
					AND SystemResult_Cd <> 'Planned Event')          	
			) VIR_TBL
	
	) P
WHERE P.CURR_ROW <> P.PREV_ROW
	AND NOT EXISTS (
		SELECT ''
		FROM {INTERACTION_FND_DB}.INTERACTIONGROUPSTATUS G
		WHERE (
				G.EXPIRATION_DT = '3500-01-01'
				AND P.Row_Num = 1
				AND G.InteractionGroup_Id = P.InteractionGroup_Id
				AND G.BusinessStatus_Cd = P.BusinessStatus_Cd
				AND G.Status_Cd = P.Status_Cd
				AND G.StatusReason_Cd = P.StatusReason_Cd
				)
			OR (
				G.InteractionGroup_Id = P.InteractionGroup_Id
				AND G.BusinessStatus_Cd = P.BusinessStatus_Cd
				AND G.Status_Cd = P.Status_Cd
				AND G.StatusReason_Cd = P.StatusReason_Cd
				AND G.SequenceStart_It = P.Sequence_It
			)
			OR (
				P.InteractionGroup_Id = G.InteractionGroup_Id
				AND P.Source_cd = G.Source_cd
				AND P.Source_cd NOT IN ({EXCLUDED_SOURCES}) AND P.Source_cd NOT IN ({IGS_EXCLUDED_SOURCE})
				AND P.Sequence_It >= G.SequenceStart_It
				AND P.Sequence_It < G.SequenceEnd_It
				AND P.Action_Tp = 22
				AND G.Status_Tp IN (
					4
					,9
					)
				)
		)
"""

try:
    logger.info("Reading SQ_InteractionEvent from Snowflake via JDBC override query")
    df_SQ_InteractionEvent_calm_kant = (
        spark.read.format("jdbc")
        .option("url", SNOWFLAKE_URL)
        .option("user", SNOWFLAKE_USER)
        .option("password", SNOWFLAKE_PASSWORD)
        .option("query", sql_query)
        .load()
    )
except Exception as e:
    logger.error(f"Failed reading SQ_InteractionEvent via JDBC override: {e}", exc_info=True)
    raise

# Expression: exp_get_tp_values (lookup-derived type columns + padded composite id)
# utils_candidate: this implements a generic code-to-type lookup + padded id builder
try:
    logger.info("Computing lookup-derived type columns and o_InteractionGroupStatus_Id in df_exp_get_tp_values_zen_maxwell")
    # prepare upper-trimmed code columns
    df_base_for_lkp = df_SQ_InteractionEvent_calm_kant.withColumn(
        "BusinessStatus_Cd_u", upper(trim(col("BusinessStatus_Cd")))
    ).withColumn(
        "Status_Cd_u", upper(trim(col("Status_Cd")))
    ).withColumn(
        "StatusReason_Cd_u", upper(trim(col("StatusReason_Cd")))
    )

    # read lookup reference table expected to contain mapping of (lookup_id, code) -> type_id
    # This mapping is implementation-dependent; we'll attempt to read a catalog table named 'lkp_xref_xt'.
    # If your environment stores lookups under a different table, replace 'lkp_xref_xt' with the correct name.
    try:
        dyf_lkp_xref_xt = glueContext.create_dynamic_frame_from_catalog(
            database=GLUE_DATABASE,
            table_name="lkp_xref_xt",
            transformation_ctx="df_lkp_xref_xt",
        )
        df_lkp_xref_xt = dyf_lkp_xref_xt.toDF().select(
            col("lookup_id").cast("int").alias("lookup_id"),
            upper(trim(col("code"))).alias("code"),
            col("type_id").alias("type_id")
        )
    except Exception:
        # If lookup table is not available in the Glue Catalog, create an empty lookup frame so the join runs and nulls surface
        df_lkp_xref_xt = spark.createDataFrame([], schema=T.StructType([
            T.StructField("lookup_id", T.IntegerType()),
            T.StructField("code", T.StringType()),
            T.StructField("type_id", T.LongType())
        ]))

    # prepare broadcasted subsets for the three lookup IDs
    df_lkp_5021 = broadcast(df_lkp_xref_xt.filter(col("lookup_id") == 5021).select(col("code").alias("code_5021"), col("type_id").alias("type_id_5021")))
    df_lkp_5022 = broadcast(df_lkp_xref_xt.filter(col("lookup_id") == 5022).select(col("code").alias("code_5022"), col("type_id").alias("type_id_5022")))
    df_lkp_5023 = broadcast(df_lkp_xref_xt.filter(col("lookup_id") == 5023).select(col("code").alias("code_5023"), col("type_id").alias("type_id_5023")))

    # join the lookups
    df_join_lkp = (
        df_base_for_lkp
        .join(df_lkp_5021, df_base_for_lkp.BusinessStatus_Cd_u == df_lkp_5021.code_5021, how="left")
        .join(df_lkp_5022, df_base_for_lkp.Status_Cd_u == df_lkp_5022.code_5022, how="left")
        .join(df_lkp_5023, df_base_for_lkp.StatusReason_Cd_u == df_lkp_5023.code_5023, how="left")
    )

    # extract the numeric type ids and compute the padded composite id
    df_exp_get_tp_values_zen_maxwell = (
        df_join_lkp.withColumn("o_BusinessStatus_Tp", col("type_id_5021").cast("smallint"))
        .withColumn("o_Status_Tp", col("type_id_5022").cast("smallint"))
        .withColumn("o_StatusReason_Tp", col("type_id_5023").cast("smallint"))
        # build padded LoadEvent_Id to 18 characters then add NUMBER_SEQ numerically
        .withColumn("LoadEvent_Id_str", lpad(col("LoadEvent_Id").cast("string"), 18, "0"))
        .withColumn("o_InteractionGroupStatus_Id", (col("LoadEvent_Id_str").cast("decimal(38,0)") + col("NUMBER_SEQ").cast("decimal(38,0)"))).drop("LoadEvent_Id_str")
        # keep key columns for downstream joins
        .select(
            "LoadEvent_Id",
            "NUMBER_SEQ",
            "InteractionGroup_Id",
            "BusinessStatus_Cd",
            "Status_Cd",
            "StatusReason_Cd",
            "o_BusinessStatus_Tp",
            "o_Status_Tp",
            "o_StatusReason_Tp",
            "o_InteractionGroupStatus_Id"
        )
    )

    # enforce default-on-error behavior from the original expression: raise if any lookup returned null
    missing_lkp_count = (
        df_exp_get_tp_values_zen_maxwell
        .filter(
            col("o_BusinessStatus_Tp").isNull() | col("o_Status_Tp").isNull() | col("o_StatusReason_Tp").isNull()
        )
        .limit(1)
        .count()
    )
    if missing_lkp_count > 0:
        raise ValueError("exp_get_tp_values failed: one or more lookups returned NULL (missing reference data in lkp_xref_xt for lookup ids 5021/5022/5023)")

except Exception as e:
    logger.error(f"Failed computing exp_get_tp_values logic: {e}", exc_info=True)
    raise

# Expression: exp_set_default_Tp_values (replace NULLs with sentinel -2)
try:
    logger.info("Applying default sentinel (-2) for NULL type values in df_exp_set_default_Tp_values_heroic_faraday")
    df_exp_set_default_Tp_values_heroic_faraday = (
        df_exp_get_tp_values_zen_maxwell
        .withColumn(
            "o_BusinessStatus_Tp",
            when(col("o_BusinessStatus_Tp").isNotNull(), col("o_BusinessStatus_Tp").cast("smallint")).otherwise(lit(-2).cast("smallint"))
        )
        .withColumn(
            "o_Status_Tp",
            when(col("o_Status_Tp").isNotNull(), col("o_Status_Tp").cast("smallint")).otherwise(lit(-2).cast("smallint"))
        )
        .withColumn(
            "o_StatusReason_Tp",
            when(col("o_StatusReason_Tp").isNotNull(), col("o_StatusReason_Tp").cast("smallint")).otherwise(lit(-2).cast("smallint"))
        )
        # preserve keys for downstream join
        .select(
            "LoadEvent_Id",
            "NUMBER_SEQ",
            "InteractionGroup_Id",
            "BusinessStatus_Cd",
            "Status_Cd",
            "StatusReason_Cd",
            "o_BusinessStatus_Tp",
            "o_Status_Tp",
            "o_StatusReason_Tp",
            "o_InteractionGroupStatus_Id"
        )
    )
except Exception as e:
    logger.error(f"Failed applying defaults in exp_set_default_Tp_values: {e}", exc_info=True)
    raise

# Output: InteractionGroupStatus (assemble and write to S3 as parquet)
try:
    logger.info("Assembling InteractionGroupStatus target dataframe as df_InteractionGroupStatus_heroic_shannon")
    # join SQ with the expression outputs on LoadEvent_Id and NUMBER_SEQ
    df_joined = (
        df_SQ_InteractionEvent_calm_kant.alias("sq")
        .join(df_exp_get_tp_values_zen_maxwell.alias("g"), on=["LoadEvent_Id", "NUMBER_SEQ"], how="left")
        .join(df_exp_set_default_Tp_values_heroic_faraday.alias("d"), on=["LoadEvent_Id", "NUMBER_SEQ"], how="left")
    )

    df_InteractionGroupStatus_heroic_shannon = df_joined.select(
        col("g.o_InteractionGroupStatus_Id").alias("InteractionGroupStatus_Id"),
        col("sq.InteractionGroup_Id").alias("InteractionGroup_Id"),
        col("sq.BusinessStatus_Cd").alias("BusinessStatus_Cd"),
        col("d.o_BusinessStatus_Tp").alias("BusinessStatus_Tp"),
        col("sq.Status_Cd").alias("Status_Cd"),
        col("d.o_Status_Tp").alias("Status_Tp"),
        col("sq.StatusReason_Cd").alias("StatusReason_Cd"),
        col("d.o_StatusReason_Tp").alias("StatusReason_Tp"),
        col("sq.TransactionEffective_Dt").alias("Effective_Dt"),
        # expiration is a target-specific static value in many mappings; preserve as NULL here unless upstream provides it
        lit(None).cast("timestamp").alias("Expiration_Dt"),
        col("sq.Transaction_Ts").alias("Transaction_Ts"),
        col("sq.Transaction_Ts").alias("Revision_Ts"),
        col("sq.Sequence_It").alias("SequenceStart_It"),
        col("sq.Sequence_It").alias("SequenceEnd_It"),
        col("sq.Source_Cd").alias("Source_Cd"),
        col("sq.LoadEvent_Id").alias("LoadEvent_Id")
    )

    # write intermediate WRK / target as parquet to S3 (overwrite)
    try:
        logger.info("Writing InteractionGroupStatus to S3 as parquet (overwrite)")
        df_InteractionGroupStatus_heroic_shannon.write.mode("overwrite").parquet(
            f"s3://{S3_OUTPUT_BUCKET}/InteractionGroupStatus/"
        )
    except Exception as e:
        logger.error(f"Failed writing InteractionGroupStatus to S3: {e}", exc_info=True)
        raise

except Exception as e:
    logger.error(f"Failed assembling or writing InteractionGroupStatus target: {e}", exc_info=True)
    raise



job.commit()
