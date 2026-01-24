#!/usr/bin/env python
# coding: utf-8

# ## nb_omat_updatedmd_cons_mknha_wklyrefresh
# 
# New notebook

# ##### **Pyspark_Notebook :** nb_omat_updatedmd_cons_mknha_wklyrefresh
# ##### 📌 **Author:** Rameez Ansari
# ##### 📅 **Last Updated:** 2026-01-18
# ###### 🔢 **Notebook Version:** 1.0
# ###### 🔄 **Change Log:** v1.0 | 2026-01-18 |
# ###### Rameez Ansari   → Work in Progress
# 📖 **Notebook Overview:**
# 
# This Notebook identifies the change in demand, consumption for Make NHAs in OB PR on weekly basis and insert it into Tables for OMAT Application.
# 
# **Includes:**    
# - Target Tables This Notebook Populates:
# 
#     1. **rpt_omat_makenha_dmd_consumption_dtls:** Provides demand and consumption details for Make NHAs, including manufacturing and spares forecasts.
#         - Load type: Full Refresh.
#     2. **rpt_omat_log_dtls:** Logs OMAT system activities like job executions and email refresh requests for tracking and troubleshooting.
#         - Load type: Append-only (audit logging).
# 

# In[96]:


from silketl import SqlDatabaseConnector, get_workspace_name, preload, load_data, postload,json,current_workspace_name


# In[97]:


# project_code = 'p2f_ome' 
# ##FTR meaning feauture workspace
# exec_env = 'FTR'
# job_id = '' 
# task_id = '' 
# env = ''


# #### Step-1 -- Imports

# In[98]:


from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *


# #### Step-2 -- Parameterize (Source/Target) path and Date Calculations

# In[99]:


# ---------- SOURCE TABLES ----------
cv_omat_obpn_make_nha_dtls = "lhs_glb.omat_test.rpt_omat_obpn_make_nha_dtls"
cv_omat_obprtno_dtls = "lhs_glb.omat_test.rpt_omat_obprtno_dtls"
cv_mseg = "wsf_silk_glb_da_dev.lhs_glb.ecc.mseg"
cv_makt = "wsf_silk_glb_da_dev.lhs_glb.ecc.makt"
cv_mkpf = "wsf_silk_glb_da_dev.lhs_glb.ecc.mkpf"
cv_mdkp = "wsf_silk_glb_da_dev.lhs_glb.ecc.mdkp"
cv_mdtb = "wsf_silk_glb_da_dev.lhs_glb.ecc.mdtb"
cv_sql_so_2 = "lhs_glb.eng_test.sales_order_optimised"
zt_csbg_spares_deliveries = "wsf_silk_glb_dt_qa.lhg_glb.eng.rpt_csbg_spares_deliveries"

# ---------- TARGET TABLES ----------
tgt_dmd_cons = "lhs_glb.eng_test.rpt_omat_makenha_dmd_consumption_dtls"
tgt_log = "lhs_glb.eng_test.rpt_omat_log_dtls"

# ---------- CONTROL DATES ----------
from_date = F.add_months(F.current_date(), -12)
to_date = F.add_months(F.current_date(), 12)

dt1wk = F.expr("date_add(current_date(), - (dayofweek(current_date()) - 1))")
dt26wk = F.expr("""
    date_add(
        date_add(
            date_add(current_date(), 182),
            - (dayofweek(date_add(current_date(), 182)) - 1)
        ),
        6
    )
""")

_SYS_BIC."prd.csbg/CVNS_CSBG_SPARES_DELIVERIES"
_SYS_BIC."prd.global.ecc/CV_MAKT"
_SYS_BIC."prd.global.ecc/CV_MDKP"
_SYS_BIC."prd.global.ecc/CV_MDTB"
_SYS_BIC."prd.global.ecc/CV_MKPF"
_SYS_BIC."prd.global.ecc/CV_MSEG"
_SYS_BIC."prd.gops.GLIS/CV_SQL_SO_2"
_SYS_BIC."prd.gops.OMAT/CV_OMAT_OBPN_MAKE_NHA_DTLS"
_SYS_BIC."prd.gops.OMAT/CV_OMAT_OBPRTNO_DTLS"
ALEX_CUSTOM."ZT_CSBG_SPARES_DELIVERIES"



# #### Step-3 -- Reading Source tables

# In[100]:


df_obpn = spark.read.format("delta").table(cv_omat_obpn_make_nha_dtls) \
    .selectExpr(
        "lower(cmpprtno) as cmpprtno_cd",
        "lower(nha) as nha_cd"
    )

df_obprt = spark.read.format("delta").table(cv_omat_obprtno_dtls) \
    .selectExpr(
        "lower(obprtno) as obprtno_cd",
        "upper(obpr_state) as obpr_state_cd"
    )

df_mseg = spark.read.format("delta").table(cv_mseg) \
    .selectExpr(
        "lower(matnr) as matnr_cd",
        "lower(shkzg) as shkzg_cd",
        "menge",
        "bwart",
        "werks",
        "cpudt_mkpf",
        "mblnr"
    )

df_makt = spark.read.format("delta").table(cv_makt) \
    .selectExpr("lower(matnr) as matnr_cd")

df_mkpf = spark.read.format("delta").table(cv_mkpf) \
    .selectExpr(
        "mblnr",
        "bldat"
    )

df_mdkp = spark.read.format("delta").table(cv_mdkp) \
    .selectExpr(
        "lower(matnr) as matnr_cd",
        "plwrk",
        "dtnum"
    )

df_mdtb = spark.read.format("delta").table(cv_mdtb) \
    .selectExpr(
        "dtnum",
        "plumi",
        "delkz",
        "dat00",
        "mng01"
    )

df_so = spark.read.format("delta").table(cv_sql_so_2) \
    .selectExpr(
        "lower(part) as matnr_cd",
        "lower(orderstatus) as orderstatus_cd",
        "ordergroup",
        "deliveryduedate",
        "ordertype",
        "openqty"
    )

df_spares = spark.read.format("delta").table(zt_csbg_spares_deliveries) \
    .selectExpr(
        "lower(material) as material_cd",
        "qty_shipped",
        "source_doc",
        "order_type",
        "actual_gi"
    )


# #### Step-4 -- Logging Start.

# In[101]:


spark.sql("""
INSERT INTO eng_test.rpt_omat_log_dtls (
  executed_on,
  obprtno_cnt,
  obprtno_nha_cnt,
  obpr_cnt,
  program_name
)
VALUES (
  current_timestamp(),
  NULL,
  NULL,
  1,
  'SP_OMAT_UPDATEDMD_CONS_MKNHA_WklyRefresh - Started'
)
""")


# #### Step-5 -- NHALIST, NHAMAINLIST (Core population of interest)
# - Extracts the unique list of NHA materials from NHAMAINLIST.
# - This is the master NHA population on which:
#     - Manufacturing consumption
#     - Manufacturing demand
#     - Spares demand
#     - Spares consumption
#     - will be calculated.
# - This cell identifies “Make NHA parts that are currently active in OB PR”.

# In[102]:


df_nhamainlist = (
    df_obpn.alias("a")
    .join(df_obprt.alias("b"),
          F.col("b.obprtno_cd") == F.col("a.cmpprtno_cd"),
          "inner")
    .filter(F.col("b.obpr_state_cd").isin("CONFIRMED","IN REVIEW","IN WORK"))
    .select(
        F.col("a.cmpprtno_cd"),
        F.col("a.nha_cd")
    )
    .distinct()
)

df_nhamainlist.createOrReplaceTempView("nhamainlist_vw")


df_nhalist = df_nhamainlist.select("nha_cd").distinct()
df_nhalist.createOrReplaceTempView("nhalist_vw")


# #### Step-6 -- Manufacturing 5 Years Consumption (df_mfg_5yrs)
# - Compute total manufacturing consumption of each NHA over the last 5 years, considering:
#     - Goods issues/receipts (261/262)
#     - Only relevant plants (<2001 or 3120)
#     - Adjusted for debit/credit (SHKZG logic)

# In[104]:


df_mfg_5yrs = (
    df_nhalist.alias("a")
    .join(df_mseg.alias("b"), F.col("a.nha_cd") == F.col("b.matnr_cd"), "inner")
    .join(df_makt.alias("c"), F.col("c.matnr_cd") == F.col("b.matnr_cd"), "inner")
    .join(df_mkpf.alias("d"), F.col("b.mblnr") == F.col("d.mblnr"), "inner")
    .filter(
        F.col("b.bwart").isin("261","262") &
        (F.col("d.bldat") > F.date_sub(F.current_date(), 365*5)) &
        (F.col("d.bldat") < F.current_date()) &
        ((F.col("b.werks") < 2001) | (F.col("b.werks") == 3120)) &
        (F.col("b.cpudt_mkpf") > F.date_sub(F.current_date(), 365*5))
    )
    .groupBy(F.col("b.matnr_cd"))
    .agg(
        F.sum(
            F.when(F.col("b.shkzg_cd") == "s", -1 * F.col("b.menge"))
             .otherwise(F.col("b.menge"))
        ).alias("mfg_5yrs_consumption_cd")
    )
)

df_nhalist_mfgcons = (
    df_nhamainlist.alias("a")
    .join(df_mfg_5yrs.alias("b"),
          F.col("b.matnr_cd") == F.col("a.nha_cd"),
          "left")
    .select(
        F.col("a.cmpprtno_cd"),
        F.col("a.nha_cd"),
        F.col("b.mfg_5yrs_consumption_cd")
    )
)

df_nhalist_mfgcons.createOrReplaceTempView("nhalist_mfgcons_vw")


# #### Step-7 -- Manufacturing Weekly Demand (26 Weeks)
# - Calculates planned manufacturing demand for each NHA over the next 26 weeks, based on:
#     - MDKP + MDTB planning data
#     - Only relevant plants
#     - Only relevant demand categories (AR, SB)
#     - Only within the 26-week window (DT1WK to DT26WK)
#     - This is your forward-looking manufacturing demand signal.

# In[107]:


df_mfg_dmd = (
    df_nhalist.alias("m")
    .join(df_mdkp.alias("d"), F.col("m.nha_cd") == F.col("d.matnr_cd"), "inner")
    .join(df_mdtb.alias("f"), F.col("f.dtnum") == F.col("d.dtnum"), "inner")
    .filter(
        F.col("f.plumi").isin("+","-") &
        ((F.col("d.plwrk") < 2001) | (F.col("d.plwrk") == 3120)) &
        F.col("f.delkz").isin("AR","SB") &
        (F.col("f.dat00") >= dt1wk) &
        (F.col("f.dat00") <= dt26wk)
    )
    .groupBy(F.col("d.matnr_cd"))
    .agg(
        F.sum(
            F.when(F.col("f.plumi") == "-", F.col("f.mng01"))
             .otherwise(-1 * F.col("f.mng01"))
        ).alias("mfg_dmd_cd")
    )
)

df_nhalist_mfgdmd = (
    df_nhalist_mfgcons.alias("a")
    .join(df_mfg_dmd.alias("b"),
          F.col("b.matnr_cd") == F.col("a.nha_cd"),
          "left")
    .select(
        "a.cmpprtno_cd",
        "a.nha_cd",
        "a.mfg_5yrs_consumption_cd",
        F.col("b.mfg_dmd_cd")
    )
)

df_nhalist_mfgdmd.createOrReplaceTempView("nhalist_mfgdmd_vw")


# #### Step-8 -- Spares Weekly Demand (26 Weeks)
# - Calculates open spares sales order demand for each NHA over the next 26 weeks, considering:
#     - Only Sales Orders (OrderGroup = SO)
#     - Excluding closed orders
#     - Only relevant order types (TA, ZCON, ZUP, etc.)
#     - Only deliveries due within the 26-week window
#     - This is your forward-looking spares demand signal.

# In[110]:


df_sprs_dmd = (
    df_nhalist.alias("a")
    .join(df_so.alias("b"),
          F.col("a.nha_cd") == F.col("b.matnr_cd"),
          "inner")
    .filter(
        (F.col("b.ordergroup") == "SO") &
        (F.col("b.orderstatus_cd") != "closed") &
        (F.col("b.deliveryduedate") >= dt1wk) &
        (F.col("b.deliveryduedate") <= dt26wk) &
        F.col("b.ordertype").isin("TA","ZCON","ZO09","ZO04","ZSD","ZSO","ZFO","ZUP")
    )
    .groupBy(F.col("b.matnr_cd"))
    .agg(
        F.sum(F.col("b.openqty")).alias("sprs_dmd_cd")
    )
)

df_nhalist_sprsdmd = (
    df_nhalist_mfgdmd.alias("a")
    .join(df_sprs_dmd.alias("b"),
          F.col("b.matnr_cd") == F.col("a.nha_cd"),
          "left")
    .select(
        "a.cmpprtno_cd",
        "a.nha_cd",
        "a.mfg_5yrs_consumption_cd",
        "a.mfg_dmd_cd",
        F.col("b.sprs_dmd_cd")
    )
)

df_nhalist_sprsdmd.createOrReplaceTempView("nhalist_sprsdmd_vw")


# #### Step-9 -- Spares 5 Years Consumption (Fixed)
# - This checks How much of each NHA was actually consumed as spare parts in the last 5 years.
# - It uses actual goods issue from ZT_CSBG_SPARES_DELIVERIES, keeping only:
#     - Relevant order types (TA, ZCON, ZUP, etc.)
#     - Non-STO deliveries
#     - Shipments in the last 5 years
#     - Positive shipped quantities
#     - This is your historical spares consumption signal.

# In[113]:


df_sprs_cons = (
    df_spares.alias("a")
    .join(df_nhalist.alias("b"),
          F.col("b.nha_cd") == F.col("a.material_cd"),
          "inner")
    .filter(
        (F.col("a.source_doc") != "STO") &
        (F.col("a.order_type").isin("TA","ZCON","ZO09","ZO04","ZSD","ZSO","ZFO","ZUP")) &
        (F.col("a.actual_gi") > F.date_sub(F.current_date(), 365*5))
    )
    .groupBy(F.col("a.material_cd"))
    .agg(
        F.sum(F.col("a.qty_shipped")).alias("sprs_5yrs_consumption_cd")
    )
    .filter(F.col("sprs_5yrs_consumption_cd") > 0)
)

df_nhalist_sprscons = (
    df_nhalist_sprsdmd.alias("a")
    .join(df_sprs_cons.alias("b"),
          F.col("b.material_cd") == F.col("a.nha_cd"),
          "left")
    .select(
        "a.cmpprtno_cd",
        "a.nha_cd",
        "a.mfg_5yrs_consumption_cd",
        "a.mfg_dmd_cd",
        "a.sprs_dmd_cd",
        F.col("b.sprs_5yrs_consumption_cd")
    )
)

df_nhalist_sprscons.createOrReplaceTempView("nhalist_sprscons_vw")


# #### Step-10 -- Manufacturing 12 Months Consumption + Final Candidate Dataset
# - Part A — Calculate recent (last 12 months) manufacturing consumption
#     - It checks For each NHA, how much was consumed in manufacturing in the last 12 months.
#     - It uses the same movement logic as your 5-year consumption (261/262, SHKZG sign handling), but only for the last 12 months. This gives you a recent trend signal rather than long-term history.
# - Part B — Build the final working dataset (SAP equivalent of NHALIST_MFG12MNTHSCONS)
#     - Then it keeps only those NHAs where at least one of these is non-zero:
#         - 5-year manufacturing consumption
#         - 26-week manufacturing demand
#         - 26-week spares demand
#         - 5-year spares consumption
# - This filter means Keep only NHAs that actually show some meaningful activity or demand.

# In[116]:


df_mfg_12m = (
    df_mseg.alias("b")
    .join(df_makt.alias("c"), F.col("c.matnr_cd") == F.col("b.matnr_cd"), "inner")
    .join(df_mkpf.alias("d"), F.col("b.mblnr") == F.col("d.mblnr"), "inner")
    .join(df_nhalist.alias("a"), F.col("b.matnr_cd") == F.col("a.nha_cd"), "inner")
    .filter(
        F.col("b.bwart").isin("261","262") &
        (F.col("d.bldat") > F.date_sub(F.current_date(), 365)) &
        (F.col("d.bldat") < F.current_date()) &
        (F.col("b.cpudt_mkpf") > F.date_sub(F.current_date(), 365))
    )
    .groupBy(F.col("b.matnr_cd"))
    .agg(
        F.sum(
            F.when(F.col("b.shkzg_cd") == "s", -1 * F.col("b.menge"))
             .otherwise(F.col("b.menge"))
        ).alias("mfg_12mnths_consumption_cd")
    )
)

df_final_base = (
    df_nhalist_sprscons.alias("a")
    .join(df_mfg_12m.alias("b"),
          F.col("b.matnr_cd") == F.col("a.nha_cd"),
          "left")
    .select(
        "a.cmpprtno_cd",
        "a.nha_cd",
        "a.mfg_5yrs_consumption_cd",
        "a.mfg_dmd_cd",
        "a.sprs_dmd_cd",
        "a.sprs_5yrs_consumption_cd",
        F.col("b.mfg_12mnths_consumption_cd")
    )
    .filter(
        (F.abs(F.coalesce("mfg_5yrs_consumption_cd", F.lit(0))) +
         F.abs(F.coalesce("mfg_dmd_cd", F.lit(0))) +
         F.abs(F.coalesce("sprs_dmd_cd", F.lit(0))) +
         F.abs(F.coalesce("sprs_5yrs_consumption_cd", F.lit(0)))) > 0
    )
)

df_final_base.createOrReplaceTempView("nhalist_final_vw")


# #### Step-11 -- Cell 11 — Truncate Target + Final INSERT into Delta
# - This cell performs your official weekly refresh for OMAT:
# Clears the previous snapshot of: rpt_omat_makenha_dmd_consumption_dtls
# - Loads only the new “active” records from df_final_base into the target table, with:
#     - Explicit column list (no positional inserts)
#     - Lowercase column mapping
#     - Load date stamped as last_modified_on

# In[119]:


# --- Step 1: Truncate target (SAP DELETE equivalent) ---
spark.sql(f"TRUNCATE TABLE {tgt_dmd_cons}")

# --- Step 2: Final explicit-column insert ---
spark.sql(f"""
INSERT INTO {tgt_dmd_cons} (
    cmpprtno,
    nha,
    mfg_5yrs_consumption,
    mfg_dmd,
    sprs_dmd,
    sprs_5yrs_consumption,
    mfg_12mnths_consumption,
    last_modified_on
)
SELECT
    cmpprtno_cd,
    nha_cd,
    mfg_5yrs_consumption_cd,
    mfg_dmd_cd,
    sprs_dmd_cd,
    sprs_5yrs_consumption_cd,
    mfg_12mnths_consumption_cd,
    current_date()
FROM nhalist_final_vw
""")


# #### Step-12 -- Logging / Audit (SAP-equivalent completion step)
# - This cell replicates the start/end audit trail of your SAP procedure in Fabric:
#     - Captures how many OB component parts (CMPPRTNO) were in scope
#     - Captures how many NHAs were in scope
#     - Writes two records to OMAT_LOG_DTLS: “Procedure started”, “Procedure ended”

# In[122]:


# ---- Calculate counts (SAP OBPRTCnt / NHACnt equivalents) ----
obprtno_cnt = df_nhamainlist.select("cmpprtno_cd").distinct().count()
obpr_cnt = df_nhamainlist.select("nha_cd").distinct().count()
obprtno_nha_cnt = df_nhamainlist.count()   # total CMPPRTNO–NHA pairs

# ---- Define schema explicitly to avoid Delta merge issues ----
log_schema = StructType([
    StructField("executed_on", TimestampType(), True),
    StructField("obprtno_cnt", IntegerType(), True),
    StructField("obprtno_nha_cnt", IntegerType(), True),
    StructField("obpr_cnt", IntegerType(), True),
    StructField("program_name", StringType(), True)
])

# ---- Create END log row using Spark current_timestamp ----
df_log_end = spark.createDataFrame(
    [(None, obprtno_cnt, obprtno_nha_cnt, obpr_cnt,
      "SP_OMAT_UPDATEDMD_CONS_MKNHA_WklyRefresh - End")],
    schema=log_schema
).withColumn("executed_on", current_timestamp())

# ---- Append both records to Delta log table ----
df_log_end.write.format("delta").mode("append").saveAsTable("eng_test.rpt_omat_log_dtls")


# In[123]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql

# SELECT *
# FROM eng_test.rpt_omat_log_dtls
# ORDER BY executed_on DESC
# LIMIT 4;

