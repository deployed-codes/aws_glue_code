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


# Top-of-script placeholders for Informatica mapping parameters and JDBC connection info
INTERACTION_FND_DB = "REPLACE_WITH_INTERACTION_FND_DB_VALUE"
MAX_SEQUENCE_IT = "REPLACE_WITH_MaxSequenceIt_VALUE"
MIN_LOAD_EVENT_ID = "REPLACE_WITH_MinLoadEventId_VALUE"
LOAD_EVENT_ID = "REPLACE_WITH_Load_Event_Id_VALUE"
RL_EDW_EINTERACTION_JDBC_URL = "REPLACE_WITH_RL_EDW_EINTERACTION_JDBC_URL"
RL_EDW_EINTERACTION_USER = "REPLACE_WITH_RL_EDW_EINTERACTION_USER"
RL_EDW_EINTERACTION_PASSWORD = "REPLACE_WITH_RL_EDW_EINTERACTION_PASSWORD"

from pyspark.sql import functions as F

# Source: InteractionEvent
try:
    logger.info("Reading Source node InteractionEvent from Glue Data Catalog into df_InteractionEvent_calm_turing")
    dyf_InteractionEvent = glueContext.create_dynamic_frame_from_catalog(
        database=GLUE_DATABASE,
        table_name="InteractionEvent",
        transformation_ctx="df_InteractionEvent_calm_turing",
    )
    df_InteractionEvent_calm_turing = dyf_InteractionEvent.toDF()
    # Project only declared ports where reasonable (many ports exist in source metadata; keep the full set passed along in this mapping)
    # Select a conservative set used downstream; add others if needed by later nodes
    df_InteractionEvent_calm_turing = df_InteractionEvent_calm_turing.select(
        "InteractionGroup_Id",
        "InteractionGroupKey_Cd",
        "InteractionGroupBusinessKey_Id",
        "InteractionGroupBusinessKey_Cd",
        "Sequence_It",
        "Source_Cd",
        "Transaction_Cd",
        "Transaction_Ts",
        "InfaMd5_Nb",
        "OriginatingSource_Cd",
        "Interaction_Id",
    )
    if df_InteractionEvent_calm_turing.isEmpty():
        logger.warning("Source InteractionEvent returned no rows")
except Exception as e:
    logger.error(f"Failed reading Source InteractionEvent from Glue Catalog: {e}", exc_info=True)
    raise

# Source: FBL_Campaign_Lkp (first instance)
try:
    logger.info("Reading Source node FBL_Campaign_Lkp from Glue Data Catalog into df_FBL_Campaign_Lkp_peaceful_hopper")
    dyf_FBL_Campaign_Lkp = glueContext.create_dynamic_frame_from_catalog(
        database=GLUE_DATABASE,
        table_name="FBL_Campaign_Lkp",
        transformation_ctx="df_FBL_Campaign_Lkp_peaceful_hopper",
    )
    df_FBL_Campaign_Lkp_peaceful_hopper = dyf_FBL_Campaign_Lkp.toDF().select(
        "CampaignKey_Cd",
        "CMADisplay_Nm",
        "ReferenceKey_Cd",
    )
    if df_FBL_Campaign_Lkp_peaceful_hopper.isEmpty():
        logger.warning("Source FBL_Campaign_Lkp (peaceful_hopper) returned no rows")
except Exception as e:
    logger.error(f"Failed reading Source FBL_Campaign_Lkp (peaceful_hopper) from Glue Catalog: {e}", exc_info=True)
    raise

# Source: FBL_Campaign_Lkp (second instance)
try:
    logger.info("Reading Source node FBL_Campaign_Lkp (second instance) from Glue Data Catalog into df_FBL_Campaign_Lkp_lucid_hume")
    dyf_FBL_Campaign_Lkp1 = glueContext.create_dynamic_frame_from_catalog(
        database=GLUE_DATABASE,
        table_name="FBL_Campaign_Lkp",
        transformation_ctx="df_FBL_Campaign_Lkp_lucid_hume",
    )
    df_FBL_Campaign_Lkp_lucid_hume = dyf_FBL_Campaign_Lkp1.toDF().select(
        "CampaignKey_Cd",
        "CMADisplay_Nm",
        "ReferenceKey_Cd",
    )
    if df_FBL_Campaign_Lkp_lucid_hume.isEmpty():
        logger.warning("Source FBL_Campaign_Lkp (lucid_hume) returned no rows")
except Exception as e:
    logger.error(f"Failed reading Source FBL_Campaign_Lkp (lucid_hume) from Glue Catalog: {e}", exc_info=True)
    raise

# SQ: SQ_InteractionEvent (SQL override pushdown to relational source - default non-staged case)
# Source: InteractionEvent (bypassed — SQL Override reads it directly)
sql_query = f"""SELECT
IE.InteractionGroup_Id,
IE.InteractionGroupKey_Cd,
IE.InteractionGroupBusinessKey_Id,
IE.InteractionGroupBusinessKey_Cd,
COALESCE(IE1.InteractionGrpLogiclBusnKey_Cd,IE.InteractionGroupBusinessKey_Cd) AS InteractionGrpLogiclBusnKey_Cd,
IE.Sequence_It,
IE.Source_Cd,
IE.Transaction_Cd,
COALESCE(IE1.Source_Cd, IE.Source_Cd)   AS OriginatingSource_Cd
FROM
{INTERACTION_FND_DB}.InteractionEvent IE
LEFT OUTER JOIN
(SELECT
 InteractionGroupBusinessKey_Id
      ,InteractionGroupBusinessKey_Cd
      ,InteractionGrpLogiclBusnKey_Id
      ,InteractionGrpLogiclBusnKey_Cd
      ,SequenceStart_It
      ,COALESCE(MIN(SequenceStart_It) OVER
                         (PARTITION BY InteractionGroupBusinessKey_Id
                              ORDER BY SequenceStart_It ASC
                          ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)
                      ,350036535003659999) AS NewSequenceEnd_It
      ,Source_Cd
FROM
  (SELECT
 InteractionGroupBusinessKey_Id
      ,InteractionGroupBusinessKey_Cd
      ,InteractionGrpLogiclBusnKey_Id
      ,InteractionGrpLogiclBusnKey_Cd
      ,MIN(Sequence_It) AS SequenceStart_It
      ,Source_Cd
FROM {INTERACTION_FND_DB}.InteractionEvent
WHERE Source_Cd = 'IAQ'
GROUP BY 1,2,3,4,6) TMP) IE1
    ON (ie.InteractionGroupBusinessKey_Id  = IE1.InteractionGroupBusinessKey_Id
     AND  ie.Sequence_It                    >= IE1.SequenceStart_It
     AND  ie.Sequence_It                     < IE1.NewSequenceEnd_It
     )
CROSS JOIN

(SELECT COALESCE(MAX(SequenceStart_It),0) AS  SequenceStart_It FROM  {INTERACTION_FND_DB}.LeadInteraction WHERE SOURCE_CD='FBL') FBLSEQ

     
 WHERE (IE.Source_Cd = 'IAQ' OR IE.Source_Cd = 'LMT')
 AND ((IE.Sequence_It > FBLSEQ.SequenceStart_It AND  IE.Sequence_It<={MAX_SEQUENCE_IT})
 OR (IE.loadevent_id >= {MIN_LOAD_EVENT_ID} AND IE.Sequence_It <  FBLSEQ.SequenceStart_It))"""

try:
    logger.info("Reading SQ_InteractionEvent_jovial_darwin from relational source via JDBC override query")
    df_SQ_InteractionEvent_jovial_darwin = (
        spark.read.format("jdbc")
        .option("url", RL_EDW_EINTERACTION_JDBC_URL)
        .option("user", RL_EDW_EINTERACTION_USER)
        .option("password", RL_EDW_EINTERACTION_PASSWORD)
        .option("query", sql_query)
        .load()
    )
except Exception as e:
    logger.error(f"Failed reading SQ_InteractionEvent_jovial_darwin via JDBC override: {e}", exc_info=True)
    raise

# SQ: SQ_Campaign_FBL_Lkp (no SQL override) - passthrough/projection
try:
    logger.info("Projecting fields for SQ_Campaign_FBL_Lkp into df_SQ_Campaign_FBL_Lkp_sharp_heisenberg")
    df_SQ_Campaign_FBL_Lkp_sharp_heisenberg = df_FBL_Campaign_Lkp_peaceful_hopper.select(
        "CampaignKey_Cd",
        "CMADisplay_Nm",
        "ReferenceKey_Cd",
    )
except Exception as e:
    logger.error(f"Failed projecting SQ_Campaign_FBL_Lkp: {e}", exc_info=True)
    raise

# SQ: SQ_Campaign_FBL_Lkp1 (no SQL override) - passthrough/projection
try:
    logger.info("Projecting fields for SQ_Campaign_FBL_Lkp1 into df_SQ_Campaign_FBL_Lkp1_vibrant_mendel")
    df_SQ_Campaign_FBL_Lkp1_vibrant_mendel = df_FBL_Campaign_Lkp_lucid_hume.select(
        "CampaignKey_Cd",
        "CMADisplay_Nm",
        "ReferenceKey_Cd",
    )
except Exception as e:
    logger.error(f"Failed projecting SQ_Campaign_FBL_Lkp1: {e}", exc_info=True)
    raise

# Lookup: lkp_Lead_Interaction - build lookup set via JDBC and left-join (broadcast) to incoming stream
try:
    logger.info("Building LeadInteraction lookup set via JDBC for lkp_Lead_Interaction")
    sql_lookup_li = f"SELECT Campaign_Id, InteractionGroup_Id, CMADisplay_Nm, SequenceStart_It, SequenceEnd_It, Source_Cd FROM {INTERACTION_FND_DB}.LeadInteraction WHERE Source_Cd = 'IAQ' OR (Source_Cd = 'LMT' AND LeadOrigin_Cd IN ('LMA','ADHOC') AND LeadDistribution_Cd = 'Sales') GROUP BY Campaign_Id, InteractionGroup_Id, CMADisplay_Nm, SequenceStart_It, SequenceEnd_It, Source_Cd"
    df_lookup_LeadInteraction = (
        spark.read.format("jdbc")
        .option("url", RL_EDW_EINTERACTION_JDBC_URL)
        .option("user", RL_EDW_EINTERACTION_USER)
        .option("password", RL_EDW_EINTERACTION_PASSWORD)
        .option("query", sql_lookup_li)
        .load()
    )
except Exception as e:
    logger.error(f"Failed building LeadInteraction lookup set: {e}", exc_info=True)
    raise

try:
    logger.info("Performing lookup join for lkp_Lead_Interaction into df_lkp_Lead_Interaction_awesome_maxwell")
    i = df_SQ_InteractionEvent_jovial_darwin.alias("i")
    lk = F.broadcast(df_lookup_LeadInteraction).alias("lk")
    cond = (
        (F.col("i.InteractionGroup_Id") == F.col("lk.InteractionGroup_Id"))
        & (F.col("i.Source_Cd") == F.col("lk.Source_Cd"))
        & (F.col("lk.SequenceStart_It") <= F.col("i.Sequence_It"))
        & (F.col("lk.SequenceEnd_It") > F.col("i.Sequence_It"))
    )
    df_lkp_Lead_Interaction_awesome_maxwell = (
        i.join(lk, cond, how="left")
        .select(
            F.col("lk.Campaign_Id").alias("Campaign_Id"),
            F.col("lk.InteractionGroup_Id").alias("InteractionGroup_Id"),
            F.col("lk.CMADisplay_Nm").alias("CMADisplay_Nm"),
            F.col("lk.SequenceStart_It").alias("SequenceStart_It"),
            F.col("lk.SequenceEnd_It").alias("SequenceEnd_It"),
            F.col("lk.Source_Cd").alias("Source_Cd"),
            F.col("i.InteractionGroup_Id").alias("ie_InteractionGroup_Id"),
            F.col("i.Sequence_It").alias("ie_Sequence_It"),
            F.col("i.Source_Cd").alias("ie_Source_Cd"),
            F.col("i.InteractionGroupKey_Cd").alias("ie_InteractionGroupKey_Cd"),
            F.col("i.InteractionGroupBusinessKey_Id").alias("ie_InteractionGroupBusinessKey_Id"),
            F.col("i.InteractionGroupBusinessKey_Cd").alias("ie_InteractionGroupBusinessKey_Cd"),
            F.col("i.InteractionGrpLogiclBusnKey_Cd").alias("ie_InteractionGrpLogiclBusnKey_Cd"),
            F.col("i.Transaction_Cd").alias("ie_Transaction_Cd"),
            F.col("i.OriginatingSource_Cd").alias("OriginatingSource_Cd"),
        )
    )
except Exception as e:
    logger.error(f"Failed performing lookup join for lkp_Lead_Interaction: {e}", exc_info=True)
    raise

# Filter: fil_Non_Default_CampaingKey_Cd (CampaignKey_Cd <> '@')
try:
    logger.info("Applying filter fil_Non_Default_CampaingKey_Cd (CampaignKey_Cd<>'@')")
    df_fil_Non_Default_CampaingKey_Cd_clever_curie = df_SQ_Campaign_FBL_Lkp_sharp_heisenberg.filter(
        expr := F.expr("CampaignKey_Cd <> '@'")
    ).select("CampaignKey_Cd", "CMADisplay_Nm", "ReferenceKey_Cd")
except Exception as e:
    logger.error(f"Failed applying filter fil_Non_Default_CampaingKey_Cd: {e}", exc_info=True)
    raise

# Filter: fil_Default_CampaingKey_Cd (CampaignKey_Cd = '@')
try:
    logger.info("Applying filter fil_Default_CampaingKey_Cd (CampaignKey_Cd='@')")
    df_fil_Default_CampaingKey_Cd_calm_shannon = df_SQ_Campaign_FBL_Lkp1_vibrant_mendel.filter(
        F.expr("CampaignKey_Cd = '@'")
    ).select("CampaignKey_Cd", "CMADisplay_Nm", "ReferenceKey_Cd")
except Exception as e:
    logger.error(f"Failed applying filter fil_Default_CampaingKey_Cd: {e}", exc_info=True)
    raise

# Generic transformation (filter by Campaign_Id not null): fil_LeadInteracion_LMT
try:
    logger.info("Applying filter fil_LeadInteracion_LMT (Campaign_Id IS NOT NULL)")
    df_fil_LeadInteracion_LMT_awesome_pasteur = df_lkp_Lead_Interaction_awesome_maxwell.filter(
        F.col("Campaign_Id").isNotNull()
    ).select(
        "Campaign_Id",
        "CMADisplay_Nm",
        "ie_InteractionGroup_Id",
        "ie_Sequence_It",
        "ie_Source_Cd",
        "ie_InteractionGroupKey_Cd",
        "ie_InteractionGroupBusinessKey_Id",
        "ie_InteractionGroupBusinessKey_Cd",
        "ie_InteractionGrpLogiclBusnKey_Cd",
        "ie_Transaction_Cd",
        "OriginatingSource_Cd",
    )
except Exception as e:
    logger.error(f"Failed applying fil_LeadInteracion_LMT: {e}", exc_info=True)
    raise

# Lookup: lkp_BaseCampaign_KeyCd_SameSource - read BaseCampaign and left-join
try:
    logger.info("Reading BaseCampaign lookup table via JDBC for lkp_BaseCampaign_KeyCd_SameSource")
    df_lookup_BaseCampaign = (
        spark.read.format("jdbc")
        .option("url", RL_EDW_EINTERACTION_JDBC_URL)
        .option("user", RL_EDW_EINTERACTION_USER)
        .option("password", RL_EDW_EINTERACTION_PASSWORD)
        .option("dbtable", f"{INTERACTION_FND_DB}.BaseCampaign")
        .load()
        .select(
            "Campaign_Id",
            "CampaignKey_Cd",
            "SequenceStart_It",
            "SequenceEnd_It",
            "Source_Cd",
        )
    )
except Exception as e:
    logger.error(f"Failed reading BaseCampaign lookup table: {e}", exc_info=True)
    raise

try:
    logger.info("Performing lookup join for lkp_BaseCampaign_KeyCd_SameSource into df_lkp_BaseCampaign_KeyCd_SameSource_mystifying_babbage")
    i = df_fil_LeadInteracion_LMT_awesome_pasteur.alias("i")
    lk = F.broadcast(df_lookup_BaseCampaign).alias("lk")
    cond = (
        (F.col("lk.Campaign_Id") == F.col("i.Campaign_Id"))
        & (F.col("lk.Source_Cd") == F.col("i.ie_Source_Cd"))
        & (F.col("lk.SequenceStart_It") <= F.col("i.ie_Sequence_It"))
        & (F.col("lk.SequenceEnd_It") > F.col("i.ie_Sequence_It"))
    )
    df_lkp_BaseCampaign_KeyCd_SameSource_mystifying_babbage = (
        i.join(lk, cond, how="left")
        .select(
            F.col("lk.Campaign_Id").alias("Campaign_Id"),
            F.col("lk.CampaignKey_Cd").alias("CampaignKey_Cd"),
            F.col("lk.SequenceStart_It").alias("SequenceStart_It"),
            F.col("lk.SequenceEnd_It").alias("SequenceEnd_It"),
            F.col("lk.Source_Cd").alias("Source_Cd"),
            F.col("i.Campaign_Id").alias("li_Campaign_Id"),
            F.col("i.CMADisplay_Nm").alias("li_CMADisplay_Nm"),
            F.col("i.ie_InteractionGroup_Id").alias("ie_InteractionGroup_Id"),
            F.col("i.ie_Sequence_It").alias("ie_Sequence_It"),
            F.col("i.ie_Source_Cd").alias("ie_Source_Cd"),
            F.col("i.ie_InteractionGroupKey_Cd").alias("ie_InteractionGroupKey_Cd"),
            F.col("i.ie_InteractionGroupBusinessKey_Id").alias("ie_InteractionGroupBusinessKey_Id"),
            F.col("i.ie_InteractionGroupBusinessKey_Cd").alias("ie_InteractionGroupBusinessKey_Cd"),
            F.col("i.ie_InteractionGrpLogiclBusnKey_Cd").alias("ie_InteractionGrpLogiclBusnKey_Cd"),
            F.col("i.ie_Transaction_Cd").alias("ie_Transaction_Cd"),
            F.col("i.OriginatingSource_Cd").alias("OriginatingSource_Cd"),
        )
    )
except Exception as e:
    logger.error(f"Failed performing lookup join for lkp_BaseCampaign_KeyCd_SameSource: {e}", exc_info=True)
    raise

# Joiner: jnr_NonDefault_Campaign_Key_Cd_toPull_ReferenceKey_Cd (Master Outer semantics)
try:
    logger.info("Joining non-default campaign stream to BaseCampaign master (jnr_NonDefault_Campaign_Key_Cd_toPull_ReferenceKey_Cd)")
    d = df_fil_Non_Default_CampaingKey_Cd_clever_curie.alias("d")
    m = df_lkp_BaseCampaign_KeyCd_SameSource_mystifying_babbage.alias("m")
    # Join condition: detail.CampaignKey_Cd = master.CampaignKey_Cd (master port named CampaignKey_Cd1 in metadata)
    join_cond = F.col("d.CampaignKey_Cd") == F.col("m.CampaignKey_Cd")
    df_jnr_NonDefault_Campaign_Key_Cd_toPull_ReferenceKey_Cd_zen_babbage = (
        d.join(m, join_cond, how="left")
        .select(
            F.col("d.CampaignKey_Cd").alias("CampaignKey_Cd"),
            F.col("m.Campaign_Id").alias("li_Campaign_Id"),
            F.col("m.CMADisplay_Nm").alias("li_CMADisplay_Nm"),
            F.col("m.ie_InteractionGroup_Id").alias("ie_InteractionGroup_Id"),
            F.col("m.ie_Sequence_It").alias("ie_Sequence_It"),
            F.col("m.ie_Source_Cd").alias("ie_Source_Cd"),
            F.col("m.ie_InteractionGroupKey_Cd").alias("ie_InteractionGroupKey_Cd"),
            F.col("m.ie_InteractionGroupBusinessKey_Id").alias("ie_InteractionGroupBusinessKey_Id"),
            F.col("m.ie_InteractionGroupBusinessKey_Cd").alias("ie_InteractionGroupBusinessKey_Cd"),
            F.col("m.ie_InteractionGrpLogiclBusnKey_Cd").alias("ie_InteractionGrpLogiclBusnKey_Cd"),
            F.col("m.ie_Transaction_Cd").alias("ie_Transaction_Cd"),
            F.col("m.OriginatingSource_Cd").alias("OriginatingSource_Cd"),
            F.col("m.CampaignKey_Cd").alias("CampaignKey_Cd1"),
            F.col("m.CMADisplay_Nm").alias("CMADisplay_Nm"),
            F.col("m.ReferenceKey_Cd").alias("ReferenceKey_Cd"),
        )
    )
except Exception as e:
    logger.error(f"Failed executing joiner jnr_NonDefault_Campaign_Key_Cd_toPull_ReferenceKey_Cd: {e}", exc_info=True)
    raise

# Joiner: jnr_Default_Campaign_Key_Cd_toPull_ReferenceKey_Cd (Master Outer semantics)
try:
    logger.info("Joining default campaign stream to non-default join output (jnr_Default_Campaign_Key_Cd_toPull_ReferenceKey_Cd)")
    d = df_fil_Default_CampaingKey_Cd_calm_shannon.alias("d")
    m = df_jnr_NonDefault_Campaign_Key_Cd_toPull_ReferenceKey_Cd_zen_babbage.alias("m")
    # Join condition: CampaignKey_Cd2 = CampaignKey_Cd AND CMADisplay_Nm1 = li_CMADisplay_Nm
    # align names: assume master provides CampaignKey_Cd1/CMADisplay_Nm
    join_cond = (
        (F.col("d.CampaignKey_Cd") == F.col("m.CampaignKey_Cd1"))
        & (F.col("d.CMADisplay_Nm") == F.col("m.li_CMADisplay_Nm"))
    )
    df_jnr_Default_Campaign_Key_Cd_toPull_ReferenceKey_Cd_quirky_tesla = (
        d.join(m, join_cond, how="left")
        .select(
            F.col("d.CampaignKey_Cd").alias("CampaignKey_Cd"),
            F.col("m.li_Campaign_Id").alias("li_Campaign_Id"),
            F.col("m.li_CMADisplay_Nm").alias("li_CMADisplay_Nm"),
            F.col("m.ie_InteractionGroup_Id").alias("ie_InteractionGroup_Id"),
            F.col("m.ie_Sequence_It").alias("ie_Sequence_It"),
            F.col("m.ie_Source_Cd").alias("ie_Source_Cd"),
            F.col("m.ie_InteractionGroupKey_Cd").alias("ie_InteractionGroupKey_Cd"),
            F.col("m.ie_InteractionGroupBusinessKey_Id").alias("ie_InteractionGroupBusinessKey_Id"),
            F.col("m.ie_InteractionGroupBusinessKey_Cd").alias("ie_InteractionGroupBusinessKey_Cd"),
            F.col("m.ie_InteractionGrpLogiclBusnKey_Cd").alias("ie_InteractionGrpLogiclBusnKey_Cd"),
            F.col("m.ie_Transaction_Cd").alias("ie_Transaction_Cd"),
            F.col("m.OriginatingSource_Cd").alias("OriginatingSource_Cd"),
            F.col("m.CampaignKey_Cd1").alias("CampaignKey_Cd1"),
            F.col("m.CMADisplay_Nm").alias("CMADisplay_Nm"),
            F.col("m.ReferenceKey_Cd").alias("ReferenceKey_Cd"),
            F.col("d.CampaignKey_Cd").alias("CampaignKey_Cd2"),
            F.col("d.CMADisplay_Nm").alias("CMADisplay_Nm1"),
            F.col("m.ReferenceKey_Cd").alias("ReferenceKey_Cd1"),
        )
    )
except Exception as e:
    logger.error(f"Failed executing joiner jnr_Default_Campaign_Key_Cd_toPull_ReferenceKey_Cd: {e}", exc_info=True)
    raise

# Expression: exp_toPass_LMT_SourceCd (passthrough + literal o_LMT_Source_Cd = 'LMT')
try:
    logger.info("Applying expression exp_toPass_LMT_SourceCd (passthrough + o_LMT_Source_Cd = 'LMT')")
    df_exp_toPass_LMT_SourceCd_humble_plato = (
        df_jnr_Default_Campaign_Key_Cd_toPull_ReferenceKey_Cd_quirky_tesla
        .withColumn("o_LMT_Source_Cd", F.lit("LMT"))
        .select(
            "CampaignKey_Cd",
            "li_Campaign_Id",
            "li_CMADisplay_Nm",
            "ie_InteractionGroup_Id",
            "ie_Sequence_It",
            "ie_Source_Cd",
            "ie_InteractionGroupKey_Cd",
            "ie_InteractionGroupBusinessKey_Id",
            "ie_InteractionGroupBusinessKey_Cd",
            "ie_InteractionGrpLogiclBusnKey_Cd",
            "ie_Transaction_Cd",
            "OriginatingSource_Cd",
            "CampaignKey_Cd1",
            "CMADisplay_Nm",
            "ReferenceKey_Cd",
            "CampaignKey_Cd2",
            "CMADisplay_Nm1",
            "ReferenceKey_Cd1",
            "o_LMT_Source_Cd",
        )
    )
except Exception as e:
    logger.error(f"Failed applying exp_toPass_LMT_SourceCd: {e}", exc_info=True)
    raise

# Lookup: lkp_BaseCampaign_KeyCd_FBLSource - read BaseCampaign and left-join using CampaignKey_Cd = CampaignKey_Cd1 AND Source_Cd = o_LMT_Source_Cd AND sequence range
try:
    logger.info("Preparing BaseCampaign lookup (FBL source variant) for lkp_BaseCampaign_KeyCd_FBLSource")
    # Reuse df_lookup_BaseCampaign if available; otherwise read
    try:
        df_lookup_BaseCampaign_fbl = df_lookup_BaseCampaign
    except NameError:
        df_lookup_BaseCampaign_fbl = (
            spark.read.format("jdbc")
            .option("url", RL_EDW_EINTERACTION_JDBC_URL)
            .option("user", RL_EDW_EINTERACTION_USER)
            .option("password", RL_EDW_EINTERACTION_PASSWORD)
            .option("dbtable", f"{INTERACTION_FND_DB}.BaseCampaign")
            .load()
            .select("Campaign_Id", "CampaignKey_Cd", "SequenceStart_It", "SequenceEnd_It", "Source_Cd")
        )
except Exception as e:
    logger.error(f"Failed preparing BaseCampaign lookup for FBLSource: {e}", exc_info=True)
    raise

try:
    logger.info("Performing lookup join for lkp_BaseCampaign_KeyCd_FBLSource into df_lkp_BaseCampaign_KeyCd_FBLSource_blissful_fermat")
    i = df_exp_toPass_LMT_SourceCd_humble_plato.alias("i")
    lk = F.broadcast(df_lookup_BaseCampaign_fbl).alias("lk")
    cond = (
        (F.col("lk.CampaignKey_Cd") == F.col("i.CampaignKey_Cd1"))
        & (F.col("lk.Source_Cd") == F.col("i.o_LMT_Source_Cd"))
        & (F.col("lk.SequenceStart_It") <= F.col("i.ie_Sequence_It"))
        & (F.col("lk.SequenceEnd_It") > F.col("i.ie_Sequence_It"))
    )
    df_lkp_BaseCampaign_KeyCd_FBLSource_blissful_fermat = (
        i.join(lk, cond, how="left")
        .select(
            F.col("lk.Campaign_Id").alias("Campaign_Id"),
            F.col("lk.CampaignKey_Cd").alias("CampaignKey_Cd"),
            F.col("lk.SequenceStart_It").alias("SequenceStart_It"),
            F.col("lk.SequenceEnd_It").alias("SequenceEnd_It"),
            F.col("lk.Source_Cd").alias("Source_Cd"),
            F.col("i.CampaignKey_Cd1").alias("CampaignKey_Cd1"),
            F.col("i.li_Campaign_Id").alias("li_Campaign_Id"),
            F.col("i.li_CMADisplay_Nm").alias("li_CMADisplay_Nm"),
            F.col("i.ie_InteractionGroup_Id").alias("ie_InteractionGroup_Id"),
            F.col("i.ie_Sequence_It").alias("ie_Sequence_It"),
            F.col("i.ie_Source_Cd").alias("ie_Source_Cd"),
            F.col("i.ie_InteractionGroupKey_Cd").alias("ie_InteractionGroupKey_Cd"),
            F.col("i.ie_InteractionGroupBusinessKey_Id").alias("ie_InteractionGroupBusinessKey_Id"),
            F.col("i.ie_InteractionGroupBusinessKey_Cd").alias("ie_InteractionGroupBusinessKey_Cd"),
            F.col("i.ie_InteractionGrpLogiclBusnKey_Cd").alias("ie_InteractionGrpLogiclBusnKey_Cd"),
            F.col("i.ie_Transaction_Cd").alias("ie_Transaction_Cd"),
            F.col("i.OriginatingSource_Cd").alias("OriginatingSource_Cd"),
            F.col("i.CampaignKey_Cd11").alias("CampaignKey_Cd11") if "CampaignKey_Cd11" in df_exp_toPass_LMT_SourceCd_humble_plato.columns else F.lit(None).alias("CampaignKey_Cd11"),
            F.col("i.CMADisplay_Nm").alias("CMADisplay_Nm"),
            F.col("i.ReferenceKey_Cd").alias("ReferenceKey_Cd"),
            F.col("i.CampaignKey_Cd2").alias("CampaignKey_Cd2"),
            F.col("i.CMADisplay_Nm1").alias("CMADisplay_Nm1"),
            F.col("i.ReferenceKey_Cd1").alias("ReferenceKey_Cd1"),
            F.col("i.o_LMT_Source_Cd").alias("o_LMT_Source_Cd"),
        )
    )
except Exception as e:
    logger.error(f"Failed performing lookup join for lkp_BaseCampaign_KeyCd_FBLSource: {e}", exc_info=True)
    raise

# Expression: exp_Derived_Attributes - compute o_ReferenceKey_Cd, o_LeadError_In, o_Load_Event_Id and passthroughs
try:
    logger.info("Computing derived attributes in exp_Derived_Attributes (o_ReferenceKey_Cd, o_LeadError_In, o_Load_Event_Id)")
    df_inter = df_lkp_BaseCampaign_KeyCd_FBLSource_blissful_fermat
    df_exp_Derived_Attributes_fancy_rutherford = (
        df_inter
        .withColumn("o_Originating_Source_Cd", F.col("OriginatingSource_Cd"))
        .withColumn(
            "o_ReferenceKey_Cd",
            F.coalesce(
                F.col("ReferenceKey_Cd"),
                F.col("ReferenceKey_Cd1"),
                F.when(F.col("CampaignKey_Cd").isNull(), F.lit("@")).otherwise(F.col("CampaignKey_Cd")),
            ),
        )
        .withColumn("o_LeadError_In", F.when(F.trim(F.col("ie_Transaction_Cd")) == "LMT2", F.lit("Y")).otherwise(F.lit("N")))
        .withColumn("o_Load_Event_Id", F.lit(LOAD_EVENT_ID))
        .select(
            "CampaignKey_Cd1",
            "ie_InteractionGroup_Id",
            "ie_Sequence_It",
            "ie_Source_Cd",
            "ie_InteractionGroupKey_Cd",
            "ie_InteractionGroupBusinessKey_Id",
            "ie_InteractionGroupBusinessKey_Cd",
            "ie_InteractionGrpLogiclBusnKey_Cd",
            "li_Campaign_Id",
            "li_CMADisplay_Nm",
            "CampaignKey_Cd11",
            "CMADisplay_Nm",
            "ReferenceKey_Cd",
            "CampaignKey_Cd2",
            "CMADisplay_Nm1",
            "ReferenceKey_Cd1",
            "o_LMT_Source_Cd",
            "CampaignKey_Cd",
            "o_ReferenceKey_Cd",
            "OriginatingSource_Cd",
            "ie_Transaction_Cd",
            "o_LeadError_In",
            "o_Load_Event_Id",
            "o_Originating_Source_Cd",
        )
    )
except Exception as e:
    logger.error(f"Failed computing exp_Derived_Attributes: {e}", exc_info=True)
    raise

# Aggregator: Agg_to_pick_MinSequenceIt_MaxLeaderror_ind
try:
    logger.info("Aggregating to pick MIN(ie_Sequence_It) and MAX(ie_LeadError_In) in Agg_to_pick_MinSequenceIt_MaxLeaderror_ind")
    gb_cols = [
        "ie_InteractionGroup_Id",
        "ie_Source_Cd",
        "o_Originating_Source_Cd",
        "ie_InteractionGroupKey_Cd",
        "ie_InteractionGroupBusinessKey_Id",
        "ie_InteractionGroupBusinessKey_Cd",
        "ie_InteractionGrpLogiclBusnKey_Cd",
        "o_ReferenceKey_Cd",
        "o_Load_Event_Id",
    ]
    df_Agg_to_pick_MinSequenceIt_MaxLeaderror_ind_mighty_pasteur = (
        df_exp_Derived_Attributes_fancy_rutherford.select(*(gb_cols + ["ie_Sequence_It", "o_LeadError_In"]))
        .groupBy(*gb_cols)
        .agg(
            F.min(F.col("ie_Sequence_It")).alias("o_Sequence_It"),
            F.max(F.col("o_LeadError_In")).alias("o_LeadError_In"),
        )
        .select(*(gb_cols + ["o_Sequence_It", "o_LeadError_In"]))
    )
except Exception as e:
    logger.error(f"Failed executing aggregator Agg_to_pick_MinSequenceIt_MaxLeaderror_ind: {e}", exc_info=True)
    raise

# Output: LBK_XREF_Preprocess -> write as parquet to S3 (overwrite)
try:
    logger.info("Writing LBK_XREF_Preprocess to S3 as parquet (overwrite)")
    df_LBK_XREF_Preprocess_lucid_gauss = df_Agg_to_pick_MinSequenceIt_MaxLeaderror_ind_mighty_pasteur
    df_LBK_XREF_Preprocess_lucid_gauss.write.mode("overwrite").parquet(f"s3://{S3_OUTPUT_BUCKET}/LBK_XREF_Preprocess/")
except Exception as e:
    logger.error(f"Failed writing LBK_XREF_Preprocess to S3: {e}", exc_info=True)
    raise



job.commit()
