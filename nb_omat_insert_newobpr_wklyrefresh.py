#!/usr/bin/env python
# coding: utf-8

# ## nb_omat_insert_newobpr_wklyrefresh
# 
# New notebook

# ##### **Pyspark_Notebook :** nb_omat_insert_newobpr_wklyrefresh
# ##### 📌 **Author:** Rameez Ansari
# ##### 📅 **Last Updated:** 2025-12-08
# ###### 🔢 **Notebook Version:** 1.0
# ###### 🔄 **Change Log:** v1.0 | 2025-12-03 |
# ###### Rameez Ansari   → Work in Progress
# 📖 **Notebook Overview:**
# 
# 🔹This Notebook INSERTS the new OBPR added to iPLM over the week into OMAT database with its State, demand and consumption details. This executes every week as a Job.
# 
# **It Includes:**    
# 1. **rpt_omat_obprtno_dtls:** 
#     - Lists OBPR numbers and associated details for weekly refresh and insertion into OMAT tables.
#         - Load Type: Append & Upsert
# 
# 2. **rpt_omat_log_dtls:** 
#     - Logs OMAT system activities like job executions and email refresh requests for tracking and troubleshooting.
#         - Load Type: Incremental append

# In[42]:


from pyspark.sql import Row
from pyspark.sql import functions as F
from delta.tables import DeltaTable
 
import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants

from silketl import SqlDatabaseConnector, get_workspace_name, preload, load_data, postload,json,current_workspace_name, get_workspaceid_by_name
 


# In[43]:


project_code = 'eng_omat'
job_id = '123'
task_id = '14701'
env = 'FTR'
 


# In[44]:


# sql_db = SqlDatabaseConnector(env)
# sql_query = f"select * from config.vw_transform_egress_ingress_mapping where transform_egress_id = ? and project_code = ?"
# object_df = sql_db.execute(sql_query,(task_id,project_code))
# display(object_df)
 


# In[45]:


# tgt_system_properties = json.loads(object_df['TGT_SYSTEM_PROPERTIES'][0])
# # tgt_dt_workspacename = get_workspace_name(workspace_name = tgt_system_properties['workspaceName'])
# tgt_dt_workspacename = tgt_system_properties['workspaceName']
# src_dt_workspace_id = get_workspaceid_by_name(tgt_dt_workspacename)
# print(tgt_dt_workspacename)
# print(src_dt_workspace_id)
 


# In[46]:


# schema_name = str(object_df['TGT_SCHEMA_NAME'][0])
# delta_table_name = str(object_df['TGT_TABLE_NAME'][0])

workspace = current_workspace_name
s_workspace = current_workspace_name
t_workspace = workspace.replace("_da_", "_dt_")
 
# cat_mdm = workspace.replace("_da_", "_dt_")
 
 
t_lakehouse = "lhg_glb.eng"
s_lakehouse = "lhg_glb.eng"

# lh_sch_mdm = "lhg_glb.mdm"
# lh_sch_ecc = "lhs_glb.ecc"
# lh_sch_s4h = "lhs_glb.s4h"
# lh_sch_s4u = "lhs_glb.s4u"

# lakehousename = str(object_df['TGT_SYSTEM_CODE'][0])

# target_path = (
# f"abfss://{dt_workspace}@onelake.dfs.fabric.microsoft.com/"
# f"{lakehousename}.Lakehouse/Tables/{schema_name}/{delta_table_name}")

# stg_target_path = (
# f"abfss://{dt_workspace}@onelake.dfs.fabric.microsoft.com/"
# f"{lakehousename}.Lakehouse/Tables/{schema_name}/{delta_table_name}_stg")

# fqn_target_table = f"{dt_workspace}.{lakehousename}.{schema_name}.{delta_table_name}"


# ##### **Table Creation if Not Exist**
# - rpt_omat_obpn_buy_nha_dtls
# - rpt_omat_obpn_nha_dtls
# - rpt_omat_obprtno_dtls
# - rpt_omat_log_dtls

# In[47]:


spark.sql(f" create schema if not exists {t_workspace}.{t_lakehouse}")


# In[48]:


spark.sql(f""" CREATE TABLE IF NOT EXISTS {t_workspace}.{t_lakehouse}.rpt_omat_obpn_buy_nha_dtls (
  cmpprtno            STRING,
  cmpqpa              INT,
  isprocessed         STRING,
  last_modified_on    DATE,
  level               INT,
  matkl               STRING,
  mstae               STRING,
  nha                 STRING,
  nha_status_inactive STRING
)
USING DELTA """)




spark.sql(f""" CREATE TABLE IF NOT EXISTS {t_workspace}.{t_lakehouse}.rpt_omat_obpn_nha_dtls (
  bomitem             STRING,
  bomqpa              INT,
  childpn             STRING      NOT NULL,
  cmpprtno            STRING      NOT NULL,
  cmpqpa              INT,
  exploded            STRING,
  last_modified_on    DATE,
  level               INT         NOT NULL,
  matkl               STRING,
  mstae               STRING,
  nha                 STRING      NOT NULL,
  nha_status_inactive STRING
)
USING DELTA """)



spark.sql(f"""  CREATE TABLE IF NOT EXISTS {t_workspace}.{t_lakehouse}.rpt_omat_obprtno_dtls (
  business_unit           STRING,
  created_on              TIMESTAMP,   -- SECONDDATE
  duplicate_pr_flag       STRING,
  enditem                 STRING,
  is_ltb_avail            STRING,
  is_obprt_processed      STRING,
  is_weekly_processed     STRING,
  last_modified_on        TIMESTAMP,   -- SECONDDATE
  ltb_date                DATE,
  ltb_qty                 INT,
  obprno                  STRING       NOT NULL,
  obprtno                 STRING       NOT NULL,
  obpr_state              STRING,
  part_status             STRING,
  pg_pm                   STRING,
  reltd_ob_pr             STRING,
  supplier_date_to_zero   DATE
)
USING DELTA """)




spark.sql(f""" CREATE TABLE IF NOT EXISTS {t_workspace}.{t_lakehouse}.rpt_omat_log_dtls (
  executed_on        TIMESTAMP,  -- SECONDDATE
  obprtno_cnt        INT,
  obprtno_nha_cnt    INT,
  obpr_cnt           INT,
  program_name       STRING
)
USING DELTA """)


# #### Step-1 — Imports

# In[49]:


from pyspark.sql import functions as F
from pyspark.sql.window import Window


# #### Step-2 — Parameters
# - Define source and target table paths and runtime controls.
# - Only this cell should be edited when moving across environments.

# In[50]:


# Source tables (FULL paths)
tbl_iplm_prp        = f"{t_workspace}.{t_lakehouse}.stg_omat_iplm_problem_report_part"
tbl_omat_obprt_src  = f"{t_workspace}.{t_lakehouse}.rpt_omat_obprtno_dtls"

# Target tables (FULL paths)
tbl_omat_obprt_tgt  = f"{t_workspace}.{t_lakehouse}.rpt_omat_obprtno_dtls"
tbl_omat_log_tgt    = f"{t_workspace}.{t_lakehouse}.rpt_omat_log_dtls"

# Control
# weekly_limit = 10

# spark.conf.set("spark.native.enabled", "false")


# ####  Step-3 — Read Source Tables
# - Load source tables from Fabric Lakehouse into DataFrames.

# In[51]:


df_iplm = spark.read.table(tbl_iplm_prp)
df_omat_src = spark.read.table(tbl_omat_obprt_src)


# #### Step-4 — Identify New OBPRs
# - Identify new OB Problem Reports added during the week that are not yet present in OMAT.
# 
# This replicates the OBPR list logic from the SAP stored procedure.

# In[52]:


obprlst_df = (
    df_iplm
    .filter(F.col("part_name") != "NA")
    .filter(F.upper("state") != "CLOSED")
    .filter(
        (F.upper("state").isin("CONFIRMED","IN REVIEW","IN WORK")) |
        (F.upper("disposition").isin("CONFIRMED","DEFER"))
    )
    .filter(F.upper("reason") == "OBSOLETE COMPONENT")
    .join(
        df_omat_src,
        (df_iplm.name == df_omat_src.obprno) &
        (df_iplm.part_name == df_omat_src.obprtno),
        "left"
    )
    .filter(df_omat_src.obprno.isNull())
    .select("name")
    .distinct()
    .withColumn("id", F.dense_rank().over(Window.orderBy("name")))
    # .limit(weekly_limit)
    .withColumnRenamed("name", "ob_prno")
)

obprlst_df.createOrReplaceTempView("obprlst")


# #### Step-5 — Build OBPR Part Details
# Create the core OBPR–Part dataset with status, LTB,
# supplier date, and processing flags.
# 
# This replaces the SAP temporary table #OBPRDTLS.

# In[53]:


base_df = (
    df_iplm
    .select(
        F.col("name").alias("ob_prno"),
        F.col("part_name").alias("ob_prtno"),
        F.col("mstae").alias("part_status_cd"),   #  renamed here
        F.col("state").alias("obpr_state_cd"),    #  renamed here
        F.col("disposition"),
        F.col("reason"),
        F.col("is_the_last_time_buy_available").alias("is_ltb_avail_cd"),    #  renamed here
        F.col("ob_last_buy_date").alias("ltb_date_cd"),     #  renamed here
        F.col("lrc_suppliers_date_to_zero_inventory").alias("supplier_date_to_zero_cd")   #  renamed here
    )
    .filter(F.col("ob_prtno") != "NA")
    .filter(F.upper("obpr_state_cd") != "CLOSED")
    .filter(
        (F.upper("obpr_state_cd").isin("CONFIRMED","IN REVIEW","IN WORK")) |
        (F.upper("disposition").isin("CONFIRMED","DEFER"))
    )
    .filter(F.upper("reason") == "OBSOLETE COMPONENT")
)

b = base_df.alias("b")
l = obprlst_df.alias("l")
o = df_omat_src.alias("o")

obprdtls_df = (
    b
    # join OBPR list
    .join(l, F.col("b.ob_prno") == F.col("l.ob_prno"), "inner")
    .drop(F.col("l.ob_prno"))          # FIX: remove duplicate key here

    # left join OMAT to avoid duplicates
    .join(
        o,
        (F.col("b.ob_prno") == F.col("o.obprno")) &
        (F.col("b.ob_prtno") == F.col("o.obprtno")),
        "left"
    )
    .filter(F.col("o.obprno").isNull())
    .drop(F.col("o.obprno"))
    .drop(F.col("o.obprtno"))

    # windowing
    .withColumn(
        "id",
        F.row_number().over(
            Window.partitionBy("b.ob_prno").orderBy("b.ob_prtno")
        )
    )
    .withColumn("ltb_qty", F.lit(0))
    .withColumn("reltd_ob_prs", F.lit(None).cast("string"))
    .withColumn("duplicate_pr_flag", F.lit("No"))
    .withColumn("is_obprt_processed", F.lit("No"))
)

obprdtls_df.createOrReplaceTempView("obprdtls")


# ### Step-6 — Resolve Related PRs
# Derive related open PRs for each OBPR–Part combination
# using IPLM and existing OMAT data.
# 
# This implements the SAP RLTDPRS logic.

# In[54]:


# Step 1: clean base (already correct from Cell 5)
base_rel = (
    obprdtls_df
    .select("ob_prno", "ob_prtno")
    .distinct()
)

# Step 2: join OMAT with explicit projection (CRITICAL FIX)
omat_rel = (
    base_rel
    .join(
        df_omat_src.select(
            F.col("obprtno").alias("omat_obprtno"),
            F.col("reltd_ob_pr").alias("omat_reltd_ob_pr"),
            F.col("obpr_state").alias("omat_obpr_state")
        ),
        base_rel.ob_prtno == F.col("omat_obprtno"),
        "left"
    )
    .filter(
        F.col("omat_obpr_state").isNull() |
        (~F.upper(F.col("omat_obpr_state")).isin("CLOSED", "CANCELLED"))
    )
)

# Step 3: join IPLM (state column is different name, no conflict)
iplm_rel = (
    omat_rel
    .join(
        df_iplm.select(
            F.col("name").alias("iplm_prno"),
            "part_name",
            "state",
            "disposition",
            "reason"
        ),
        (F.col("part_name") == F.col("ob_prtno")) &
        (F.col("iplm_prno") != F.col("ob_prno")) &
        (F.upper(F.col("state")) != "CLOSED") &
        (
            (F.upper(F.col("state")).isin("CONFIRMED","IN REVIEW","IN WORK")) |
            (F.upper(F.col("disposition")).isin("CONFIRMED","DEFER"))
        ) &
        (F.upper(F.col("reason")) == "OBSOLETE COMPONENT"),
        "left"
    )
)

# Step 4: aggregate (NO ambiguous columns remain)
related_prs_df = (
    iplm_rel
    .groupBy("ob_prno", "ob_prtno")
    .agg(
        F.when(
            F.count("omat_obprtno") == 0,
            F.concat_ws(",", F.collect_set("iplm_prno"))
        )
        .otherwise(F.max("omat_reltd_ob_pr"))
        .alias("reltd_ob_prs")
    )
)

related_prs_df.createOrReplaceTempView("related_prs")


# In[55]:


display(related_prs_df.limit(10))


# ### Cell 7 — Insert into OMAT Target Table
# Insert processed OBPR–Part records into the OMAT target table.
# 
# Column order and data types are controlled to ensure Delta safety.

# In[56]:


spark.sql(f"""
INSERT INTO {tbl_omat_obprt_tgt} (
    business_unit,
    created_on,
    duplicate_pr_flag,
    enditem,
    is_ltb_avail,
    is_obprt_processed,
    is_weekly_processed,
    last_modified_on,
    ltb_date,
    ltb_qty,
    obprno,
    obprtno,
    obpr_state,
    part_status,
    pg_pm,
    reltd_ob_pr,
    supplier_date_to_zero
)
SELECT
    CAST(NULL AS STRING)        AS business_unit,
    current_timestamp()        AS created_on,
    CASE
        WHEN r.reltd_ob_prs IS NOT NULL THEN 'Yes'
        ELSE 'No'
    END                         AS duplicate_pr_flag,
    CAST(NULL AS STRING)        AS enditem,
    d.is_ltb_avail_cd           AS is_ltb_avail,
    d.is_obprt_processed        AS is_obprt_processed,
    'A'                         AS is_weekly_processed,
    current_timestamp()        AS last_modified_on,
    d.ltb_date                  AS ltb_date,                 --  FIXED
    d.ltb_qty                   AS ltb_qty,
    d.ob_prno                   AS obprno,
    d.ob_prtno                  AS obprtno,
    d.obpr_state_cd             AS obpr_state,
    d.part_status_cd            AS part_status,
    CAST(NULL AS STRING)        AS pg_pm,
    r.reltd_ob_prs              AS reltd_ob_pr,
    d.supplier_date_to_zero     AS supplier_date_to_zero     --  FIXED
FROM (
    SELECT
        ob_prno,
        ob_prtno,
        part_status_cd,
        obpr_state_cd,
        is_ltb_avail_cd,
        ltb_date,
        ltb_qty,
        is_obprt_processed,
        supplier_date_to_zero
    FROM obprdtls
    WHERE ob_prno IS NOT NULL
      AND ob_prtno IS NOT NULL
) d
LEFT JOIN related_prs r
  ON d.ob_prno  = r.ob_prno
 AND d.ob_prtno = r.ob_prtno
""")


# 
# ### Step-8 — Insert Execution Log
# Insert execution metrics into OMAT log table,
# 
# including OBPR count, part count, and execution timestamp.

# In[59]:


spark.sql(f"""
INSERT INTO {tbl_omat_log_tgt} (
    executed_on,
    obprtno_cnt,
    obprtno_nha_cnt,
    obpr_cnt,
    program_name
)
SELECT
    current_timestamp()                       AS executed_on,
    COUNT(DISTINCT d.ob_prtno)                AS obprtno_cnt,
    0                                         AS obprtno_nha_cnt,
    (SELECT COUNT(*) FROM obprlst)             AS obpr_cnt,
    concat(
        'nb_omat_insert_newobpr_wklyrefresh - ',
        d.ob_prno
    )                                         AS program_name
FROM obprdtls d
GROUP BY d.ob_prno
""").show()


# In[58]:


# select * from wsf_silk_glb_dt_i2r_f.lhg_glb.eng.rpt_omat_log_dtls order by executed_on desc limit 2

