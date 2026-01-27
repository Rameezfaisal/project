#!/usr/bin/env python
# coding: utf-8

# ## nb_omat_insert_explode_nha
# 
# New notebook

# ##### **Pyspark_Notebook :** nb_omat_insert_explode_nha
# ##### 📌 **Author:** Rameez Ansari
# ##### 📅 **Last Updated:** 2025-12-08
# ###### 🔢 **Notebook Version:** 1.0
# ###### 🔄 **Change Log:** v1.0 | 2025-12-03 |
# ###### Rameez Ansari   → Work in Progress
# 📖 **Notebook Overview:**
# 
# 🔹This Notebook identifies all the NHAs of a Component  part and insert it into Table for OMAT Application.
# 
# **Includes:**
# - NHA explosion up to 15 levels
# - CMPQPA calculation
# - Inserts into OMAT tables
# - Buy NHA logic
# - Approval matrix updates
# - Logging and cleanup
# 
# 1. **rpt_omat_buynha_suppliers_plants:** Details suppliers and associated plants for Buy NHAs, supporting obsolescence management and inventory updates.
# 
# Load Type: Full refresh.
# 
# 2. **rpt_omat_log_dtls:** Logs OMAT system activities like job executions and email refresh requests for tracking and troubleshooting.
# 
# Load Type: Incremental append

# In[1]:


from silketl import SqlDatabaseConnector, get_workspace_name, preload, load_data, postload,json,current_workspace_name


# In[2]:


# project_code = 'p2f_ome' 
# ##FTR meaning feauture workspace
# exec_env = 'FTR'
# job_id = '' 
# task_id = '' 
# env = ''


# ##### Step-1  --Initial imports

# In[3]:


from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ##### Helper Function for Union

# In[4]:


from pyspark.sql.utils import AnalysisException

def read_union_fast(table_key: str):
    ecc_path = paths["ecc"].get(table_key)
    s4h_path = paths["s4h"].get(table_key)

    # Read ECC if available
    try:
        df_ecc = spark.read.table(ecc_path)
    except Exception:
        df_ecc = None

    # Always try S4H
    try:
        df_s4h = spark.read.table(s4h_path)
    except Exception:
        df_s4h = None

    # If only one exists, just return it
    if df_ecc is None and df_s4h is None:
        raise ValueError(f"No data found for {table_key}")
    if df_ecc is None:
        return df_s4h
    if df_s4h is None:
        return df_ecc

    # Get all columns across both
    all_cols = sorted(set(df_ecc.columns) | set(df_s4h.columns))

    # Align schemas without casting (fastest)
    df_ecc_aligned = df_ecc.select([F.col(c) if c in df_ecc.columns else F.lit(None).alias(c) for c in all_cols])
    df_s4h_aligned = df_s4h.select([F.col(c) if c in df_s4h.columns else F.lit(None).alias(c) for c in all_cols])

    return df_ecc_aligned.unionByName(df_s4h_aligned)


# #### Step -2  Parameters Source/target

# In[5]:


# ===== control parameters =====
i_prno = "DEFAULT_PRNO"
explode_level = 15


# Initialize paths dictionary once
paths = {
    "ecc": {
        "mara": "wsf_silk_glb_da_dev.lhs_glb.ecc.mara"
    },
    "s4h": {
        "mara": "wsf_silk_glb_da_dev.lhs_glb.s4h.mara"
    }
}


# ===== target tables =====
tgt_nha_dtls        = "lhs_glb.eng.rpt_omat_obpn_nha_dtls"
tgt_buy_nha_dtls   = "lhs_glb.eng.rpt_omat_obpn_buy_nha_dtls"
tgt_obprt_dtls     = "lhs_glb.eng.rpt_omat_obprtno_dtls"
tgt_log_dtls       = "lhs_glb.eng.rpt_omat_log_dtls"



# #### Step-3 Read Source Tables

# In[6]:


# Read union of ECC + S4H for MARA
df_mara = read_union_fast("mara")

# All other tables are NOT ECC/S4H paired → read directly
df_obprt = spark.table("lhs_glb.omat_test.rpt_omat_obprtno_dtls")
df_bom   = spark.table("wsf_silk_glb_dt_qa.lhg_glb.eng.rpt_matbom_flat")
df_nha_v = spark.table("lhs_glb.omat_test.rpt_omat_obpn_nha_dtls")
df_buy_v = spark.table("lhs_glb.omat_test.rpt_omat_obpn_buy_nha_dtls")


# #### Step-4 OB Parts already Obsolte (Direct Insert)
# - For the given i_prno:
# Find component parts that are already obsolete
# For each such part:
# Treat the part as its own NHA
# Insert one row into rpt_omat_obpn_nha_dtls
# 
# - Set:
# level = 1
# cmpprtno = nha = childpn = part
# bomqpa = cmpqpa = 1
# Insert only if it does not already exist in NHA table
# 

# In[7]:


# Filter OB parts that are already obsolete
df_obsolete = (
    df_obprt
    .filter(
        (F.col("obprno") == i_prno) &
        (F.col("part_status").isin("ob","os","op")) &
        (F.col("is_obprt_processed") != "Yes")
    )
    .select(
        F.col("obprtno").alias("obprtno"),
        F.col("part_status").alias("part_status")
    )
    .distinct()
)

# Level-1 self-NHA rows
df_lvl1 = (
    df_obsolete.alias("c")
    .join(df_mara.alias("a"), F.col("c.obprtno") == F.col("a.matnr"), "inner")
    .join(df_nha_v.alias("b"), F.col("a.matnr") == F.col("b.cmpprtno"), "left")
    .filter(F.col("b.cmpprtno").isNull())
    .select(
        F.col("c.obprtno").alias("cmpprtno"),
        F.col("c.obprtno").alias("nha"),
        F.lit(1).alias("level"),
        F.col("c.obprtno").alias("childpn"),
        F.lit(None).cast("string").alias("bomitem"),
        F.lit(1).alias("bomqpa"),
        F.lit(1).alias("cmpqpa"),
        F.col("a.matkl").alias("matkl"),
        F.col("c.part_status").alias("mstae"),
        F.lit("N").alias("exploded"),
        F.current_date().alias("last_modified_on"),
        F.lit("A").alias("nha_status_inactive")
    )
)

# Register temp view
df_lvl1.createOrReplaceTempView("df_lvl1")

# Explicit-column INSERT (Delta-safe, ANSI-safe)
spark.sql(f"""
INSERT INTO {tgt_nha_dtls}
(
    cmpprtno,
    nha,
    level,
    childpn,
    bomitem,
    bomqpa,
    cmpqpa,
    matkl,
    mstae,
    exploded,
    last_modified_on,
    nha_status_inactive
)
SELECT
    cmpprtno,
    nha,
    level,
    childpn,
    bomitem,
    bomqpa,
    cmpqpa,
    matkl,
    mstae,
    exploded,
    last_modified_on,
    nha_status_inactive
FROM df_lvl1
""")


# #### Step-5   -- OB Parts that Require Explosion
# 
# find all component parts for the PR that are not yet obsolete —
# these are the parts that must be exploded through the BOM to find their NHAs.
# We stored them in df_active_ob and registered it as df_active_ob.

# In[8]:


# OB parts that still need explosion (not obsolete)
df_active_ob = (
    df_obprt
    .filter(
        (F.col("obprno") == i_prno) &
        (~F.col("part_status").isin("ob", "os", "op"))
    )
    .select(
        F.col("obprtno").alias("obprtno")
    )
    .distinct()
)

# Safety check – no nulls (Delta NOT NULL guard)
df_active_ob = df_active_ob.filter(F.col("obprtno").isNotNull())

# Register for later looping
df_active_ob.createOrReplaceTempView("df_active_ob")


# #### Step-6 Initialize nhalist
# For every active OB part, insert the part itself as the first NHA (Level = 1), and mark it as not exploded.

# In[9]:


# Build Level-1 NHALIST rows (one per active OB part)
df_nhalist = (
    df_active_ob.alias("o")
    .join(df_mara.alias("m"), F.col("o.obprtno") == F.col("m.matnr"), "inner")
    .join(df_nha_v.alias("v"), F.col("o.obprtno") == F.col("v.cmpprtno"), "left")
    .filter(F.col("v.cmpprtno").isNull())   # same as SAP: B.CMPPRTNO IS NULL
    .select(
        F.lit(0).alias("rowno"),
        F.col("o.obprtno").alias("cmpprtno"),
        F.col("o.obprtno").alias("nha"),
        F.lit(1).alias("level"),
        F.col("o.obprtno").alias("childpn"),
        F.lit(None).cast("string").alias("bomitem"),
        F.lit(1).alias("bomqpa"),
        F.lit(0).alias("cmpqpa"),
        F.col("m.matkl").alias("nhamatkl"),
        F.col("m.mstae").alias("nhamstae"),
        F.lit("N").alias("exploded")
    )
)

# Register as temp view (SAP #NHALIST replacement)
df_nhalist.createOrReplaceTempView("nhalist")


# #### Step-7  BOM Explosion - 15 level
# For every part in the current level that is not obsolete and not yet exploded,
# find its parent assemblies from BOMREC and add them as the next level NHAs.
# 
# CMP Part:
# Level 1 → itself
# Level 2 → its immediate parents (NHAs)
# Level 3 → parents of those NHAs
# ...up to 15 levels
# 
# 
# 

# In[10]:


current_level = 1

while current_level <= explode_level:

    # Parts in current level that must be exploded
    df_to_explode = (
        df_nhalist
        .filter(
            (F.col("level") == current_level) &
            (F.col("exploded") == "N") &
            (~F.col("nhamstae").isin("ob","os","op"))
        )
        .select(
            F.col("cmpprtno").alias("cmpprtno"),
            F.col("nha").alias("parent_nha")
        )
        .distinct()
    )

    if df_to_explode.count() == 0:
        break

    # Get parent NHAs from BOM
    df_children = (
        df_to_explode.alias("p")
        .join(df_bom.alias("b"), F.col("b.bomcpno") == F.col("p.parent_nha"), "inner")
        .join(df_mara.alias("m"), F.col("b.prtno") == F.col("m.matnr"), "inner")
        .filter(
            (F.col("b.postp") == "L") &
            (F.col("b.bomedat") <= F.current_date()) &
            (F.col("b.bomidat") > F.current_date())
        )
        .select(
            F.lit(0).alias("rowno"),
            F.col("p.cmpprtno").alias("cmpprtno"),
            F.col("b.prtno").alias("nha"),
            F.lit(current_level + 1).alias("level"),
            F.col("b.bomcpno").alias("childpn"),
            F.col("b.bomitem").alias("bomitem"),
            F.col("b.bomqpa").alias("bomqpa"),
            F.lit(0).alias("cmpqpa"),
            F.col("m.matkl").alias("nhamatkl"),
            F.col("m.mstae").alias("nhamstae"),
            F.lit("N").alias("exploded")
        )
    )

    # Append children
    df_nhalist = df_nhalist.unionByName(df_children)

    # Mark current level as exploded
    df_nhalist = (
        df_nhalist
        .withColumn(
            "exploded",
            F.when(
                (F.col("level") == current_level) & (F.col("exploded") == "N"),
                "Y"
            ).otherwise(F.col("exploded"))
        )
    )


# #### Step-8 CMPQPA Calculation
# 
# - computed cmpqpa (Component Quantity Per Assembly) for every NHA.
# - CMPQPA = how many units of the component exist inside each higher-level NHA.
# - Logic:
# Level 1 → cmpqpa = bomqpa
# Level 2+ →
# cmpqpa = bomqpa × sum(parent cmpqpa)
# This propagates the quantity up the BOM hierarchy
# Now every NHA row in nhalist has the correct rolled-up quantity.

# In[11]:


# Assign row numbers per level (SAP ROWN0)
w = Window.partitionBy("level").orderBy("nha", "childpn")
df_nhalist = df_nhalist.withColumn("rowno", F.row_number().over(w))

# Level 1 CMPQPA = BOMQPA
df_nhalist = df_nhalist.withColumn(
    "cmpqpa",
    F.when(F.col("level") == 1, F.col("bomqpa")).otherwise(F.col("cmpqpa"))
)

# Get max level safely
row = df_nhalist.select(F.max("level").alias("maxlvl")).collect()[0]
max_level = row["maxlvl"]

# Only run propagation if levels exist
if max_level is not None and max_level >= 2:

    for lvl in range(2, max_level + 1):

        parent = (
            df_nhalist
            .filter(F.col("level") == lvl - 1)
            .select(
                F.col("nha").alias("parent_nha"),
                F.col("cmpqpa").alias("parent_cmpqpa")
            )
        )

        df_nhalist = (
            df_nhalist.alias("x")
            .join(
                parent.alias("y"),
                (F.col("x.childpn") == F.col("y.parent_nha")) &
                (F.col("x.level") == lvl),
                "left"
            )
            .withColumn(
                "cmpqpa",
                F.when(
                    F.col("x.level") == lvl,
                    F.col("x.bomqpa") * F.coalesce(F.col("y.parent_cmpqpa"), F.lit(0))
                ).otherwise(F.col("x.cmpqpa"))
            )
            .select("x.*")
        )

# Refresh temp view
df_nhalist.createOrReplaceTempView("nhalist")


# #### Step-9 -- Insert NHALIST into rpt_omat_obpn_nha_dtls
# 
# - took the fully-exploded and quantity-calculated nhalist and loaded it into the official OMAT table.
# - It Removed obsolete NHAs (OB / OS / OP)
# - Renamed SAP temp columns → OMAT columns
# - Added last_modified_on and nha_status_inactive
# - Inserted using explicit column mapping

# In[12]:


# Prepare final NHA rows
df_final_nha = (
    df_nhalist
    .filter(~F.col("nhamstae").isin("ob","os","op"))
    .select(
        F.col("cmpprtno").alias("cmpprtno"),
        F.col("nha").alias("nha"),
        F.col("level").alias("level"),
        F.col("childpn").alias("childpn"),
        F.col("bomitem").alias("bomitem"),
        F.col("bomqpa").alias("bomqpa"),
        F.col("cmpqpa").alias("cmpqpa"),
        F.col("nhamatkl").alias("matkl"),
        F.col("nhamstae").alias("mstae"),
        F.col("exploded").alias("exploded"),
        F.current_date().alias("last_modified_on"),
        F.lit("A").alias("nha_status_inactive")
    )
)

df_final_nha.createOrReplaceTempView("df_final_nha")

# Explicit-column INSERT (Delta-safe)
spark.sql(f"""
INSERT INTO {tgt_nha_dtls}
(
    cmpprtno,
    nha,
    level,
    childpn,
    bomitem,
    bomqpa,
    cmpqpa,
    matkl,
    mstae,
    exploded,
    last_modified_on,
    nha_status_inactive
)
SELECT
    cmpprtno,
    nha,
    level,
    childpn,
    bomitem,
    bomqpa,
    cmpqpa,
    matkl,
    mstae,
    exploded,
    last_modified_on,
    nha_status_inactive
FROM df_final_nha
""")


# #### Step-10  -- Populate: rpt_omat_buy_nha_dtls
# 
# - I believe the table used by business to decide what should be bought.
# - It did two things :
# 
# 1. **Self-NHA rows**
# 
# If a component is itself a valid buyable part (MATKL = F, E, etc):
# Insert (CMPPRTNO = NHA), Aggregate CMPQPA, Avoid duplicates.
# 
# 2.  **True (New) Buy NHAs**
# 
# For higher-level assemblies:
# Only MATKL like 'P%'
# Exclude DELTA parts
# Exclude obsolete (OB/OS/OP), Avoid duplicates, Aggregate quantities

# In[13]:


# Self-NHA rows (CMPPRTNO = NHA)

df_self_buy = (
    df_nha_v.alias("a")
    .join(df_buy_v.alias("b"),
          (F.col("a.cmpprtno") == F.col("b.cmpprtno")) &
          (F.col("a.nha") == F.col("b.nha")),
          "left")
    .filter(
        (F.col("a.cmpprtno") == F.col("a.nha")) &
        (F.col("b.cmpprtno").isNull())
    )
)

df_self_buy = (
    df_self_buy
    .groupBy("a.cmpprtno","a.nha","a.level","a.matkl","a.mstae")
    .agg(F.sum("a.cmpqpa").alias("cmpqpa"))
    .withColumn("last_modified_on", F.current_date())
    .withColumn("isprocessed", F.lit("N"))
)

df_self_buy.createOrReplaceTempView("df_self_buy")

spark.sql(f"""
INSERT INTO {tgt_buy_nha_dtls}
(
    cmpprtno,
    nha,
    level,
    cmpqpa,
    matkl,
    mstae,
    last_modified_on,
    isprocessed
)
SELECT
    cmpprtno,
    nha,
    level,
    cmpqpa,
    matkl,
    mstae,
    last_modified_on,
    isprocessed
FROM df_self_buy
""")


#  BUY NHAs (P-type only)

df_buy = (
    df_nha_v.alias("a")
    .join(df_buy_v.alias("b"),
          (F.col("a.cmpprtno") == F.col("b.cmpprtno")) &
          (F.col("a.nha") == F.col("b.nha")),
          "left")
    .filter(
        (F.col("a.cmpprtno") != F.col("a.nha")) &
        (~F.col("a.nha").like("%DELTA%")) &
        (F.col("a.matkl").like("P%")) &
        (F.length(F.trim(F.col("a.matkl"))) == 2) &
        (~F.col("a.mstae").isin("ob","os","op")) &
        (F.col("b.cmpprtno").isNull())
    )
)

df_buy = (
    df_buy
    .groupBy("a.cmpprtno","a.nha","a.level","a.matkl","a.mstae")
    .agg(F.sum("a.cmpqpa").alias("cmpqpa"))
    .withColumn("last_modified_on", F.current_date())
    .withColumn("isprocessed", F.lit("N"))
)

df_buy.createOrReplaceTempView("df_buy")

spark.sql(f"""
INSERT INTO {tgt_buy_nha_dtls}
(
    cmpprtno,
    nha,
    level,
    cmpqpa,
    matkl,
    mstae,
    last_modified_on,
    isprocessed
)
SELECT
    cmpprtno,
    nha,
    level,
    cmpqpa,
    matkl,
    mstae,
    last_modified_on,
    isprocessed
FROM df_buy
""")


# #### Step-11  -- End Item (ei) enrichment.
# 
# - Populate the End Item (ei-*) list for each OB part in rpt_omat_obprtno_dtls.
# - This tells which finish products are impacted if its components become obsolete.
# 

# In[14]:


# 11.1 – Build End Item list (EI-*)

df_ei = (
    df_nha_v
    .filter(
        (F.col("cmpprtno") == i_prno) &
        (F.col("nha").like("ei-%"))
    )
    .groupBy("cmpprtno")
    .agg(F.concat_ws(",", F.collect_set("nha")).alias("enditem"))
)

df_ei.createOrReplaceTempView("df_ei")


# 11.2 – Update OMAT_OBPRTNO_DTLS

spark.sql(f"""
MERGE INTO {tgt_obprt_dtls} t
USING df_ei s
ON t.obprtno = s.cmpprtno
WHEN MATCHED THEN UPDATE SET
    t.enditem = s.enditem
""")


# #### Step-12 -- Finalization & Logging
# ###### it performs two final action:
# 
# 1. Mark the PR as Processed and prevents the same PR from being exploded again.
# 2. Insert a run record into rpt_omat_log_dtls. This creates an audit trail for the execution.
# 
# ###### Log table details.
# - EXECUTED_ON - When the explosion ran - For audit, troubleshooting, SLA
# - PROGRAM_NAME - Which PR was processed - Identifies the run (NB_RPT_OMAT_INSERT_EXPLODE_NHA - PR***)
# - OBPR_CNT - How many PRs were processed - Usually 1 (one PR per run)
# - OBPRTNO_CNT - How many component parts were in the PR - Shows the size of the obsolescence event
# - OBPRTNO_NHA_CNT - How many NHAs were found - Shows the blast radius of the obsolescence

# In[15]:


# 12.1 – Mark PR as processed

spark.sql(f"""
UPDATE {tgt_obprt_dtls}
SET is_obprt_processed = 'Yes'
WHERE obprno = '{i_prno}'
""")


# 12.2 – Insert into OMAT_LOG_DTLS

spark.sql(f"""
INSERT INTO {tgt_log_dtls}
(
    executed_on,
    obprtno_cnt,
    obprtno_nha_cnt,
    obpr_cnt,
    program_name
)
SELECT
    current_timestamp()                              AS executed_on,
    COUNT(DISTINCT cmpprtno)                          AS obprtno_cnt,
    COUNT(DISTINCT nha)                               AS obprtno_nha_cnt,
    1                                                 AS obpr_cnt,
    concat('NB_RPT_OMAT_INSERT_EXPLODE_NHA - ', '{i_prno}') AS program_name
FROM {tgt_nha_dtls}
WHERE cmpprtno = '{i_prno}'
""")


# In[16]:


# SELECT * FROM lhs_glb.eng_test.rpt_omat_log_dtls order by executed_on desc LIMIT 100

