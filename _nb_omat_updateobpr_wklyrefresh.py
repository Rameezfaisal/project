#!/usr/bin/env python
# coding: utf-8

# ## _nb_omat_updateobpr_wklyrefresh
# 
# New notebook

# ##### **Pyspark_Notebook :** nb_omat_updateobpr_wklyrefresh
# ##### 📌 **Author:** Rameez Ansari
# ##### 📅 **Last Updated:** 2026-1-29
# ###### 🔢 **Notebook Version:** 1.0
# ###### 🔄 **Change Log:** v1.0 | 2026-1-29 |   
# ###### Varshitha K S   → Work in Progress
# 📖 **Notebook Overview:**
# 
# 🔹This Notebook Updates the existing OBPR in OMAT database with its State, demand and consumption details Refresh everything except Explosion.
# 
# **It Includes:**    
# 1. **rpt_omat_obprtno_dtls:**
#     - Lists OBPR numbers and associated details for weekly refresh and insertion into OMAT tables.
#         - Load Type: Update and Insert
# 
# 2. **rpt_omat_log_dtls:** 
#     - Logs OMAT system activities like job executions and email refresh requests for tracking and troubleshooting.
#         - Load Type: Incremental append.

# In[1]:


from silketl import SqlDatabaseConnector, get_workspace_name, preload, load_data, postload,json,current_workspace_name


# In[2]:


# project_code = 'p2f_ome' 
# ##FTR meaning feauture workspace
# exec_env = 'FTR'
# job_id = '' 
# task_id = '' 
# env = ''


# #### Step-1 -- Imports

# In[3]:


from pyspark.sql import functions as F
from pyspark.sql import Window
from delta.tables import DeltaTable


# #### Step-2 -- Parameterize (source/target)
# 

# In[4]:


# Source Tables

src_iplm_pr_part_tbl = "eng.stg_omat_iplm_problem_report_part"   
src_omat_obpr_view    = "eng.rpt_omat_obprtno_dtls"    


# Target Tables

tgt_obprtno_dtls_tbl = "eng.rpt_omat_obprtno_dtls"
tgt_log_dtls_tbl     = "eng.rpt_omat_log_dtls"


# Control Flags

active_states = ["CONFIRMED", "IN REVIEW", "IN WORK"]
active_dispositions = ["CONFIRMED", "DEFER"]
obsolete_reason = "OBSOLETE COMPONENT"
closed_state = "CLOSED"

run_timestamp = F.current_timestamp()


# #### Step-3 Reading Sources as Tables

# In[5]:


# iPLM Problem Report Part data
iplm_df_raw = spark.table(src_iplm_pr_part_tbl)

# OMAT OBPR-Part current snapshot (used for duplicate detection logic)
omat_view_df_raw = spark.table(src_omat_obpr_view)

# Target table reference (Delta)
obprtno_delta = DeltaTable.forName(spark, tgt_obprtno_dtls_tbl)


# #### Step-4 -- Log Start of Weekly OBPR Refresh
# - Record that the OBPR weekly refresh process has started.

# In[6]:


log_start_df = spark.createDataFrame(
    [("nb_omat_updateobpr_wklyrefresh - started",)],
    ["program_name"]
).withColumn("executed_on", F.current_timestamp()) \
 .withColumn("obpr_cnt", F.lit(0).cast("int")) \
 .withColumn("obprtno_cnt", F.lit(0).cast("int")) \
 .withColumn("obprtno_nha_cnt", F.lit(0).cast("int"))

(
    log_start_df
    .select("executed_on", "obprtno_cnt", "obprtno_nha_cnt", "obpr_cnt", "program_name")
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(tgt_log_dtls_tbl)
)


# #### Step-5 -- Prepare Filtered iPLM Obsolete PR-Part Dataset
# - From all iPLM PR-Part records, keep only those related to Obsolete Components and standardize fields needed for downstream OBPR updates.

# In[7]:


iplm_obsolete_df = (
    iplm_df_raw
    # Keep only Obsolete Component records
    .filter(F.upper(F.col("reason")) == obsolete_reason)

    # Standardize state & disposition for rule checks
    .withColumn("state_cd", F.upper(F.col("state")))
    .withColumn("disposition_cd", F.upper(F.col("disposition")))

    # Business keys (renamed early to avoid ambiguity later)
    .withColumn("obprno_key", F.col("name"))
    .withColumn("obprtno_key", F.col("part_name"))

    # Select only required columns for downstream logic
    .select(
        "obprno_key",
        "obprtno_key",
        "state_cd",
        "disposition_cd",
        F.col("mstae").alias("part_status_cd"),
        F.col("is_the_last_time_buy_available").alias("is_ltb_avail_cd"),
        F.col("ob_last_buy_date").alias("ltb_date_cd"),
        F.col("lrc_suppliers_date_to_zero_inventory").alias("supplier_date_to_zero_cd"),
        "submitted_date"
    )
    .dropDuplicates()
    .cache()
)

iplm_obsolete_df.createOrReplaceTempView("vw_iplm_obsolete_pr_parts")


# #### Step-6 -- Update OBPR State for Terminal/Non-Active PRs
# - If iPLM shows a PR-Part is obsolete but its state is no longer in active workflow states (CONFIRMED, IN REVIEW, IN WORK), then update OMAT to reflect the latest OBPR state.
# 

# In[8]:


# Source dataset for terminal-state updates
terminal_state_df = (
    iplm_obsolete_df
    .filter(~F.col("state_cd").isin(active_states))  # NOT in active workflow states
    .select(
        F.col("obprno_key").alias("obprno_upd"),
        F.col("obprtno_key").alias("obprtno_upd"),
        F.col("state_cd").alias("new_obpr_state")
    )
    .dropDuplicates()
)

# Perform Delta MERGE update
(
    obprtno_delta.alias("t")
    .merge(
        terminal_state_df.alias("s"),
        "t.obprno = s.obprno_upd AND t.obprtno = s.obprtno_upd"
    )
    .whenMatchedUpdate(set={
        "obpr_state": "s.new_obpr_state",
        "last_modified_on": "current_timestamp()"
    })
    .execute()
)


# #### Step-7 -- Close OBPR-Part Records Removed from iPLM
# - If an OBPR–Part combination no longer exists in the iPLM obsolete component list, it means the part was removed from that PR.
# - Such records must be marked as CLOSED in OMAT.

# In[9]:


active_iplm_keys_df = (
    iplm_obsolete_df
    .select(
        F.col("obprno_key").alias("obprno_key_chk"),
        F.col("obprtno_key").alias("obprtno_key_chk")
    )
    .dropDuplicates()
)


# In[10]:


target_df = spark.table(tgt_obprtno_dtls_tbl) \
    .select(
        F.col("obprno").alias("t_obprno"),
        F.col("obprtno").alias("t_obprtno")
    )

removed_pairs_df = (
    target_df.alias("t")
    .join(
        active_iplm_keys_df.alias("s"),
        (F.col("t.t_obprno") == F.col("s.obprno_key_chk")) &
        (F.col("t.t_obprtno") == F.col("s.obprtno_key_chk")),
        "left_anti"
    )
    .dropDuplicates(["t_obprno", "t_obprtno"])
)


# In[11]:


(
    obprtno_delta.alias("t")
    .merge(
        removed_pairs_df.alias("r"),
        "t.obprno = r.t_obprno AND t.obprtno = r.t_obprtno"
    )
    .whenMatchedUpdate(set={
        "obpr_state": f"'{closed_state}'",
        "last_modified_on": "current_timestamp()"
    })
    .execute()
)


# #### Step-8 -- Build RLTDPRS (Detect Parts in Multiple Active OBPRs)
# - Identify parts that appear in more than one active OB Problem Report.
# - This helps flag duplicate investigations and links related PR numbers.

# In[13]:


# Active PRs in iPLM that are Obsolete Components and NOT CLOSED
active_pr_parts_df = (
    iplm_df_raw
    .filter(F.upper(F.col("reason")) == obsolete_reason)
    .withColumn("state_cd", F.upper(F.col("state")))
    .withColumn("disposition_cd", F.upper(F.col("disposition")))
    .filter(
        (F.col("state_cd") != closed_state) &
        (
            F.col("state_cd").isin(active_states) |
            F.col("disposition_cd").isin(active_dispositions)
        )
    )
    .select(
        F.col("name").alias("obprno_key"),
        F.col("part_name").alias("obprtno_key")
    )
    .dropDuplicates()
)

# OMAT existing PR-Part combinations
omat_pairs_df = (
    omat_view_df_raw
    .select(
        F.col("obprno").alias("omat_obprno"),
        F.col("obprtno").alias("omat_obprtno")
    )
    .dropDuplicates()
)

# Find same part in other PRs
duplicate_pairs_df = (
    active_pr_parts_df.alias("a")
    .join(
        omat_pairs_df.alias("b"),
        (F.col("a.obprtno_key") == F.col("b.omat_obprtno")) &
        (F.col("a.obprno_key") != F.col("b.omat_obprno")),
        "inner"
    )
    .select(
        F.col("a.obprtno_key").alias("part_name_dup"),
        F.col("a.obprno_key").alias("related_pr")
    )
)

rltdprs_df = (
    duplicate_pairs_df
    .groupBy("part_name_dup")
    .agg(
        F.concat_ws(",", F.collect_list("related_pr")).alias("openprs"),
        F.lit("Yes").alias("duplicate_pr_flag")
    )
)

rltdprs_df.createOrReplaceTempView("vw_rltdprs")


# #### Cell 9 — Main OBPR Enrichment Update
# - Update OMAT OBPR-Part records with the latest information from iPLM, including:
#     - Current PR state
#     - Part lifecycle status
#     - LTB availability & date
#     - Supplier depletion date
#     - Related PR list (duplicates)
#     - Duplicate PR flag

# In[14]:


enrichment_src_df = (
    iplm_df_raw
    .filter(F.upper(F.col("reason")) == obsolete_reason)
    .withColumn("state_cd", F.upper(F.col("state")))
    .withColumn("disposition_cd", F.upper(F.col("disposition")))
    .filter(
        (F.col("state_cd").isin(active_states)) |
        (F.col("disposition_cd").isin(active_dispositions))
    )
    .select(
        F.col("name").alias("obprno_upd"),
        F.col("part_name").alias("obprtno_upd"),
        F.col("mstae").alias("part_status_new"),
        F.col("state_cd").alias("obpr_state_new"),
        F.col("is_the_last_time_buy_available").alias("is_ltb_avail_new"),
        F.col("ob_last_buy_date").alias("ltb_date_new"),
        F.col("lrc_suppliers_date_to_zero_inventory").alias("supplier_date_to_zero_new"),
        "submitted_date"
    )
    .join(
        rltdprs_df.alias("r"),
        F.col("obprtno_upd") == F.col("r.part_name_dup"),
        "left"
    )
    .select(
        "obprno_upd",
        "obprtno_upd",
        "part_status_new",
        "obpr_state_new",
        "is_ltb_avail_new",
        "ltb_date_new",
        "supplier_date_to_zero_new",
        F.col("openprs").alias("reltd_ob_pr_new"),
        F.col("duplicate_pr_flag").alias("duplicate_pr_flag_new")
    )
    .dropDuplicates(["obprno_upd", "obprtno_upd"])
)

(
    obprtno_delta.alias("t")
    .merge(
        enrichment_src_df.alias("s"),
        "t.obprno = s.obprno_upd AND t.obprtno = s.obprtno_upd"
    )
    .whenMatchedUpdate(set={
        "part_status": "s.part_status_new",
        "obpr_state": "s.obpr_state_new",
        "is_ltb_avail": "s.is_ltb_avail_new",
        "ltb_date": "s.ltb_date_new",
        "supplier_date_to_zero": "s.supplier_date_to_zero_new",
        "reltd_ob_pr": "s.reltd_ob_pr_new",
        "duplicate_pr_flag": "s.duplicate_pr_flag_new",
        "last_modified_on": "current_timestamp()"
    })
    .execute()
)


# 
# 

# #### Step-10 -- Build DUPLICATEPR (Latest PR per Part)
# - For parts that appear in multiple PRs, we must determine which PR is the most recent (by submitted date).
# - Later, we will pull LTB and supplier depletion info only from that latest PR.
# - This ensures OMAT reflects the most up-to-date commercial decision.

# In[15]:


duplicate_parts_df = (
    spark.table(tgt_obprtno_dtls_tbl)
    .filter(F.col("reltd_ob_pr").isNotNull() & (F.length(F.col("reltd_ob_pr")) > 0))
    .filter(F.col("obprtno") != "NA")
    .select(F.col("obprtno").alias("dup_part_key"))
    .dropDuplicates()
)

active_iplm_for_dup_df = (
    iplm_df_raw
    .filter(F.upper(F.col("reason")) == obsolete_reason)
    .withColumn("state_cd", F.upper(F.col("state")))
    .withColumn("disposition_cd", F.upper(F.col("disposition")))
    .filter(
        (F.col("state_cd") != closed_state) &
        (
            F.col("state_cd").isin(active_states) |
            F.col("disposition_cd").isin(active_dispositions)
        )
    )
    .select(
        F.col("part_name").alias("dup_part_key"),
        "submitted_date"
    )
)

duplicatepr_df = (
    duplicate_parts_df.alias("d")
    .join(active_iplm_for_dup_df.alias("i"), "dup_part_key")
    .groupBy("dup_part_key")
    .agg(F.max("submitted_date").alias("latest_submitted_date"))
)

duplicatepr_df.createOrReplaceTempView("vw_duplicatepr")




# #### Step-11 — Build LTBINFO (Latest LTB Data per Duplicate Part)
# - For parts that exist in multiple PRs, use only the most recently submitted PR to determine:
#     - Last Time Buy availability
#     - Last Time Buy date
#     - Supplier inventory depletion date

# In[16]:


ltbinfo_df = (
    iplm_df_raw
    .filter(F.upper(F.col("reason")) == obsolete_reason)
    .withColumn("state_cd", F.upper(F.col("state")))
    .withColumn("disposition_cd", F.upper(F.col("disposition")))
    .filter(
        (F.col("part_name") != "NA") &
        (F.col("state_cd") != closed_state) &
        (
            F.col("state_cd").isin(active_states) |
            F.col("disposition_cd").isin(active_dispositions)
        )
    )
    .select(
        F.col("part_name").alias("ltb_part_key"),
        "submitted_date",
        F.col("is_the_last_time_buy_available").alias("is_ltb_avail_new"),
        F.col("ob_last_buy_date").alias("ltb_date_new"),
        F.col("lrc_suppliers_date_to_zero_inventory").alias("supplier_date_to_zero_new")
    )
    .join(
        duplicatepr_df.alias("d"),
        (F.col("ltb_part_key") == F.col("d.dup_part_key")) &
        (F.col("submitted_date") == F.col("latest_submitted_date")),
        "inner"
    )
    .select(
        "ltb_part_key",
        "is_ltb_avail_new",
        "ltb_date_new",
        "supplier_date_to_zero_new"
    )
    .dropDuplicates(["ltb_part_key"])
)

ltbinfo_df.createOrReplaceTempView("vw_ltbinfo")


# #### Step-12 — Update OMAT with Latest LTB & Supplier Dates
# Business Purpose:
# For parts appearing in multiple PRs, ensure OMAT stores LTB details from the most recently submitted PR only.

# In[17]:


ltb_update_src_df = (
    ltbinfo_df
    .select(
        F.col("ltb_part_key").alias("obprtno_upd"),
        "is_ltb_avail_new",
        "ltb_date_new",
        "supplier_date_to_zero_new"
    )
    .dropDuplicates(["obprtno_upd"])
)


(
    obprtno_delta.alias("t")
    .merge(
        ltb_update_src_df.alias("s"),
        "t.obprtno = s.obprtno_upd"
    )
    .whenMatchedUpdate(set={
        "is_ltb_avail": "s.is_ltb_avail_new",
        "ltb_date": "s.ltb_date_new",
        "supplier_date_to_zero": "s.supplier_date_to_zero_new",
        "last_modified_on": "current_timestamp()"
    })
    .execute()
)


# #### Step-13 — Log Completion of Weekly OBPR Refresh
# Record that the OBPR weekly refresh has successfully finished.
# This mirrors the final INSERT into OMAT_LOG_DTLS in the SAP procedure.

# In[18]:


log_finish_df = spark.createDataFrame(
    [("SP_OMAT_UPDATEOBPR_WklyRefresh - Finished",)],
    ["program_name"]
).withColumn("executed_on", F.current_timestamp()) \
 .withColumn("obpr_cnt", F.lit(0).cast("int")) \
 .withColumn("obprtno_cnt", F.lit(0).cast("int")) \
 .withColumn("obprtno_nha_cnt", F.lit(0).cast("int"))

(
    log_finish_df
    .select("executed_on", "obprtno_cnt", "obprtno_nha_cnt", "obpr_cnt", "program_name")
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(tgt_log_dtls_tbl)
)

