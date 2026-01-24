#!/usr/bin/env python
# coding: utf-8

# ## nb_omat_insert_makenha_dmd_cons
# 
# New notebook

# ##### **Pyspark_Notebook :** nb_omat_insert_makenha_dmd_cons
# ##### 📌 **Author:** Rameez Ansari
# ##### 📅 **Last Updated:** 2026-01-14
# ###### 🔢 **Notebook Version:** 1.0
# ###### 🔄 **Change Log:** v1.0 | 2026-01-14 |
# ###### Rameez Ansari   → Work in Progress
# 📖 **Notebook Overview:**
# 
# This Notebook identifies the demand, consumption of MAKE NHAs in OB PR and insert it into OMAT Tables for OMAT Application.
# 
# **Includes:**    
# - Target Tables This Notebook Populates:
# 
#     1. **rpt_omat_make_nha_dtls:** Similar to Buy NHAs but for Make NHAs, showing forecast and historical demand for assemblies.
#         - Load type: Incremental (only new combinations are inserted; existing ones are skipped).
#     2. **rpt_omat_makenha_dmd_consumption_dtls:** Provides demand and consumption details for Make NHAs, including manufacturing and spares forecasts.
#         - Load type: Incremental append (adds rows for NHAs marked as unprocessed and having demand/consumption > 0).
#     3. **rpt_omat_log_dtls:** Logs OMAT system activities like job executions and email refresh requests for tracking and troubleshooting.
#         - Load type: Append-only (audit logging).
# 

# In[2]:


from silketl import SqlDatabaseConnector, get_workspace_name, preload, load_data, postload,json,current_workspace_name


# In[3]:


# project_code = 'p2f_ome' 
# ##FTR meaning feauture workspace
# exec_env = 'FTR'
# job_id = '' 
# task_id = '' 
# env = ''


# #### Step-1 -- Imports

# In[4]:


from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException


# ##### Helper Function: align columns and union w/ de-dup

# In[5]:


def _read_if_exists(path: str):
    try:
        if not path:
            return None
        df = spark.read.table(path)
        return df if df.head(1) else None
    except AnalysisException:
        return None

def read_union_distinct(table_key: str):
    ecc_path = paths["ecc"].get(table_key)
    s4h_path = paths["s4h"].get(table_key)

    df_ecc = _read_if_exists(ecc_path)
    df_s4h = _read_if_exists(s4h_path)

    if df_ecc is None and df_s4h is None:
        raise ValueError(f"No data found for '{table_key}'")

    if df_ecc is None:
        return df_s4h.distinct()
    if df_s4h is None:
        return df_ecc.distinct()

    df_ecc = df_ecc.select([F.col(c).alias(c.lower()) for c in df_ecc.columns])
    df_s4h = df_s4h.select([F.col(c).alias(c.lower()) for c in df_s4h.columns])

    all_cols = sorted(set(df_ecc.columns) | set(df_s4h.columns))

    ecc_aligned = df_ecc.select([F.col(c) if c in df_ecc.columns else F.lit(None).alias(c) 
                                 for c in all_cols])
    s4h_aligned = df_s4h.select([F.col(c) if c in df_s4h.columns else F.lit(None).alias(c) 
                                 for c in all_cols])

    return ecc_aligned.unionByName(s4h_aligned).distinct()


# #### Step-2 -- Parameterize (Source/Target) path and Date Calculations

# In[6]:


# ---------- Source tables ----------
cv_nha_dtls_path          = "lhs_glb.omat_test.rpt_omat_obpn_nha_dtls"
cv_make_nha_dtls_path     = "lhs_glb.omat_test.rpt_omat_obpn_make_nha_dtls"
cv_mdkp_path              = "wsf_silk_glb_da_dev.lhs_glb.ecc.mdkp"
cv_mdtb_path              = "wsf_silk_glb_da_dev.lhs_glb.ecc.mdtb"
cv_mseg_path              = "wsf_silk_glb_da_dev.lhs_glb.ecc.mseg"
cv_mkpf_path              = "wsf_silk_glb_da_dev.lhs_glb.ecc.mkpf"
zt_spares_path            = "wsf_silk_glb_dt_qa.lhg_glb.eng.rpt_csbg_spares_deliveries"
cv_sql_so2_path           = "lhs_glb.eng_test.sales_order_optimised"

# ---------- Target tables ----------
make_nha_target_path     = "lhs_glb.eng_test.rpt_omat_obpn_make_nha_dtls"
dmd_cons_target_path     = "lhs_glb.eng_test.rpt_omat_makenha_dmd_consumption_dtls"
log_table_path           = "lhs_glb.eng_test.rpt_omat_log_dtls"


# ---------- Date Calculations ----------

today = F.current_date()

from_date = F.add_months(today, -12)
to_date   = F.add_months(today, 12)

sap_weekday = (F.dayofweek(today) + 5) % 7
dt1wk = F.date_sub(today, sap_weekday)

dt26_base = F.date_add(dt1wk, 182)
sap_weekday_26 = (F.dayofweek(dt26_base) + 5) % 7
dt26wk = F.date_add(F.date_sub(dt26_base, sap_weekday_26), 6)



# #### Step-3 -- Reading Source tables

# In[7]:


nha = spark.read.table(cv_nha_dtls_path)
make_nha = spark.read.table(cv_make_nha_dtls_path)
mdkp = spark.read.table(cv_mdkp_path)
mdtb = spark.read.table(cv_mdtb_path)
mseg = spark.read.table(cv_mseg_path)
mkpf = spark.read.table(cv_mkpf_path)
spares = spark.read.table(zt_spares_path)
so2 = spark.read.table(cv_sql_so2_path)


# #### Step-4 -- Insert New MAKE-NHAs
# - Register newly detected MAKE-NHAs for OB parent parts so that their demand & consumption can be calculated downstream.
# - It Finds new NHAs • Aggregates CMPQPA • Inserts into rpt_omat_obpn_make_nha_dtls

# In[8]:


new_make_nha = (
    nha.alias("a")
    .join(
        make_nha
            .select(
                F.col("cmpprtno").alias("b_cmpprtno"),
                F.col("nha").alias("b_nha")
            )
            .alias("b"),
        (F.col("a.cmpprtno") == F.col("b.b_cmpprtno")) &
        (F.col("a.nha") == F.col("b.b_nha")),
        "left"
    )
    .where(
        (F.col("a.cmpprtno") != F.col("a.nha")) &
        F.col("b.b_cmpprtno").isNull() &
        (~F.col("a.nha").like("%DELTA%")) &
        (F.col("a.nha_status_inactive") != "X") &
        (F.col("a.matkl").isin("M","MN","X")) &
        (~F.col("a.mstae").isin("OB","OS","OP"))
    )
    .groupBy(
        F.col("a.cmpprtno").alias("cmpprtno"),
        F.col("a.nha").alias("nha"),
        F.col("a.level").alias("level"),
        F.col("a.matkl").alias("matkl"),
        F.col("a.mstae").alias("mstae")
    )
    .agg(
        F.sum("a.cmpqpa").cast("int").alias("cmpqpa")
    )
    .withColumn("last_modified_on", F.current_date())
    .withColumn("isprocessed", F.lit("N"))
    .select(
        "cmpprtno",
        "cmpqpa",
        "isprocessed",
        "last_modified_on",
        "level",
        "matkl",
        "mstae",
        "nha"
    )
)

new_make_nha.write.mode("append").saveAsTable(make_nha_target_path)


# #### Step-5 -- Build MKNHA (Unprocessed MAKE-NHAs)
# - This is the driving dataset of the entire procedure.
# - It represents: Which OB Parent → NHA combinations are new and still need demand & consumption to be calculated.

# In[9]:


mknha = (
    spark.read.table(make_nha_target_path)
    .filter(F.col("isprocessed") == "N")
    .select(
        F.col("cmpprtno").alias("cmpprtno"),
        F.col("nha").alias("nha")
    )
    .dropDuplicates()
)

mknha.cache()


# #### Step-6 -- Manufacturing 26-Week Demand (MFGDMD26WK)
# - For every OB Parent → NHA, calculate: How much manufacturing demand exists for this NHA in the next 26 weeks.
# - This uses: • Planned orders (MDKP)
# • Order lines (MDTB)
# • Only manufacturing plants
# • Only active demand
# • Only the next 26-week horizon

# In[10]:


mfg_dmd26wk = (
    mdkp
    .select(
        F.col("dtnum").alias("d_dtnum"),
        F.col("matnr").alias("d_matnr"),
        F.col("plwrk").cast("int").alias("plwrk")
    )
    .alias("d")
    .join(
        mknha
        .select(
            F.col("cmpprtno").alias("m_cmpprtno"),
            F.col("nha").alias("m_nha")
        )
        .alias("m"),
        F.col("d.d_matnr") == F.col("m.m_nha"),
        "inner"
    )
    .join(
        mdtb
        .select(
            F.col("dtnum").alias("f_dtnum"),
            F.to_date("dat00", "yyyyMMdd").alias("dat00_dt"),
            F.col("plumi").alias("plumi"),
            F.col("mng01").cast("int").alias("mng01"),
            F.col("delkz").alias("delkz")
        )
        .alias("f"),
        F.col("d.d_dtnum") == F.col("f.f_dtnum"),
        "inner"
    )
    .where(
        F.col("f.plumi").isin("+", "-") &
        ((F.col("d.plwrk") < 2001) | (F.col("d.plwrk") == 3120)) &
        F.col("f.delkz").isin("AR", "SB") &
        (F.col("f.dat00_dt") >= dt1wk) &
        (F.col("f.dat00_dt") <= dt26wk)
    )
    .groupBy(
        F.col("m.m_cmpprtno").alias("cmpprtno"),
        F.col("d.d_matnr").alias("nha")
    )
    .agg(
        F.sum(
            F.when(F.col("f.plumi") == "-", F.col("f.mng01"))
             .otherwise(-F.col("f.mng01"))
        ).cast("int").alias("mfg_dmd")
    )
)

mfg_dmd26wk.cache()


# #### Step-7 -- Manufacturing 5-Year Consumption (MFGCONS5YRS)
# - For each OB Parent → NHA, calculate: How much of this NHA has been consumed in manufacturing in the last 5 years
# - This measures historical usage from:
# Goods issue movements (261, 262)
# Only manufacturing plants
# Last 5 years only
# This is the OB risk driver.

# In[11]:


mfg_5yrs = (
    mseg
    .select(
        F.col("mblnr").alias("mblnr"),
        F.col("matnr").alias("b_matnr"),
        F.col("werks").cast("int").alias("werks"),
        F.col("bwart").alias("bwart"),
        F.col("shkzg").alias("shkzg"),
        F.col("menge").cast("int").alias("menge"),
        F.col("cpudt_mkpf").alias("cpudt_mkpf")
    )
    .alias("b")
    .join(
        mkpf
        .select(
            F.col("mblnr").alias("d_mblnr"),
            F.to_date("bldat", "yyyyMMdd").alias("bldat_dt")
        )
        .alias("d"),
        F.col("b.mblnr") == F.col("d.d_mblnr"),
        "inner"
    )
    .join(
        mknha
        .select(
            F.col("cmpprtno").alias("m_cmpprtno"),
            F.col("nha").alias("m_nha")
        )
        .alias("m"),
        F.col("b.b_matnr") == F.col("m.m_nha"),
        "inner"
    )
    .where(
        F.col("b.bwart").isin("261", "262") &
        (F.col("d.bldat_dt") > F.date_sub(F.current_date(), 365 * 5)) &
        (F.col("d.bldat_dt") < F.current_date()) &
        ((F.col("b.werks") < 2001) | (F.col("b.werks") == 3120)) &
        (F.to_date("b.cpudt_mkpf", "yyyyMMdd") > F.date_sub(F.current_date(), 365 * 5))
    )
    .groupBy(
        F.col("m.m_cmpprtno").alias("cmpprtno"),
        F.col("b.b_matnr").alias("nha")
    )
    .agg(
        F.sum(
            F.when(F.col("b.shkzg") == "S", -F.col("b.menge"))
             .otherwise(F.col("b.menge"))
        ).cast("int").alias("mfg_5yrs_consumption")
    )
)

mfg_5yrs.cache()


# #### Step-8 -- Spares 5-Year Consumption (SPRSCONS)
# - For each OB Parent → NHA, calculate: How many of these NHAs were shipped as spares in the last 5 years.
# - This measures after-market demand from:
#     - Customer shipments
#     - Only valid order types
#     - Excluding stock transfers.
#     - Last 5 years only.
#     - This is one of the strongest OB signals.

# In[12]:


sprs_5yrs = (
    spares
    .select(
        F.col("material").alias("a_material"),
        F.col("qty_shipped").cast("int").alias("qty_shipped"),
        F.col("source_doc").alias("source_doc"),
        F.col("order_type").alias("order_type"),
        F.to_date("actual_gi", "yyyyMMdd").alias("actual_gi_dt")
    )
    .alias("a")
    .join(
        mknha
        .select(
            F.col("cmpprtno").alias("m_cmpprtno"),
            F.col("nha").alias("m_nha")
        )
        .alias("m"),
        F.col("a.a_material") == F.col("m.m_nha"),
        "inner"
    )
    .where(
        (F.col("a.source_doc") != "STO") &
        F.col("a.order_type").isin("TA","ZCON","ZO09","ZO04","ZSD","ZSO","ZFO","ZUP") &
        (F.col("a.actual_gi_dt") > F.date_sub(F.current_date(), 365 * 5))
    )
    .groupBy(
        F.col("m.m_cmpprtno").alias("cmpprtno"),
        F.col("a.a_material").alias("nha")
    )
    .agg(
        F.sum("a.qty_shipped").cast("int").alias("sprs_5yrs_consumption")
    )
    .filter(F.col("sprs_5yrs_consumption") > 0)
)

sprs_5yrs.cache()


# #### Step-9 -- Spares 26-Week Demand (SPRSDMD26WK)
# - It Calculate, for each OB Parent → NHA, how much open spares (sales) demand exists in the next 26 weeks based on open sales orders.
# - This represents forward-looking after-market demand.

# In[47]:


sprs_dmd26wk = (
    so2
    .select(
        F.col("part").alias("s_part"),
        F.col("ordergroup").alias("ordergroup"),
        F.col("orderstatus").alias("orderstatus"),
        F.col("ordertype").alias("ordertype"),
        # use the same “best available date” logic, but still filter on the 26-week window
        F.coalesce(
            F.col("deliveryduedate"),
            F.col("requestdate"),
            F.col("firmcommitdate")
        ).alias("demand_date"),
        F.col("openqty").alias("openqty")
    )
    .alias("s")
    .join(
        mknha
        .select(
            F.col("cmpprtno").alias("m_cmpprtno"),
            F.col("nha").alias("m_nha")
        )
        .alias("m"),
        F.col("s.s_part") == F.col("m.m_nha"),
        "inner"
    )
    .where(
        (F.col("s.ordergroup") == "SO") &
        (F.lower(F.col("s.orderstatus")) != "closed") &
        (F.col("s.demand_date") >= dt1wk) &
        (F.col("s.demand_date") <= dt26wk) &
        F.col("s.ordertype").isin(
            "TA","ZCON","ZO09","ZO04","ZSD","ZSO","ZFO","ZUP"
        )
    )
    .groupBy(
        F.col("m.m_cmpprtno").alias("cmpprtno"),
        F.col("s.s_part").alias("nha")
    )
    .agg(
        F.sum("s.openqty").cast("int").alias("sprs_dmd")
    )
)

sprs_dmd26wk.cache()


# #### Step-10 -- Manufacturing 12-Month Consumption (MKMFG12MNCONS)
# - Calculate, for each OB Parent → NHA, how much of that NHA was consumed in manufacturing over the last 12 months (goods movements 261/262).
# - This represents recent manufacturing usage, complementing the 5-year history in Cell 7.

# In[49]:


mfg_12m = (
    mseg
    .select(
        F.col("mblnr").alias("b_mblnr"),
        F.col("matnr").alias("b_matnr"),
        F.col("bwart").alias("bwart"),
        F.col("shkzg").alias("shkzg"),
        F.col("menge").cast("int").alias("menge"),
        F.to_date("cpudt_mkpf", "yyyyMMdd").alias("cpudt_dt")
    )
    .alias("b")
    .join(
        mkpf
        .select(
            F.col("mblnr").alias("d_mblnr"),
            F.to_date("bldat", "yyyyMMdd").alias("bldat_dt")
        )
        .alias("d"),
        F.col("b.b_mblnr") == F.col("d.d_mblnr"),
        "inner"
    )
    .join(
        mknha
        .select(
            F.col("cmpprtno").alias("m_cmpprtno"),
            F.col("nha").alias("m_nha")
        )
        .alias("m"),
        F.col("b.b_matnr") == F.col("m.m_nha"),
        "inner"
    )
    .where(
        F.col("b.bwart").isin("261", "262") &
        (F.col("d.bldat_dt") > F.date_sub(F.current_date(), 365)) &
        (F.col("d.bldat_dt") < F.current_date()) &
        (F.col("b.cpudt_dt") > F.date_sub(F.current_date(), 365))
    )
    .groupBy(
        F.col("m.m_cmpprtno").alias("cmpprtno"),
        F.col("b.b_matnr").alias("nha")
    )
    .agg(
        F.sum(
            F.when(F.col("b.shkzg") == "S", -F.col("b.menge"))
             .otherwise(F.col("b.menge"))
        )
        .cast("int")
        .alias("mfg_12mnths_consumption")
    )
)

mfg_12m.cache()


# #### Step-11 -- Final INSERT into Demand & Consumption Delta Table
# - Create the consolidated MAKE-NHA demand & consumption record by bringing together:
#     - Manufacturing 5-year consumption
#     - Manufacturing 26-week demand
#     - Spares 5-year consumption
#     - Spares 26-week demand
#     - Manufacturing 12-month consumption
#     - Only parts with at least one non-zero metric are inserted, exactly as in SAP.

# In[51]:


final_df = (
    mknha.alias("a")
    .join(mfg_5yrs.alias("b"),  ["cmpprtno","nha"], "left")
    .join(mfg_dmd26wk.alias("c"), ["cmpprtno","nha"], "left")
    .join(sprs_5yrs.alias("d"),  ["cmpprtno","nha"], "left")
    .join(sprs_dmd26wk.alias("e"), ["cmpprtno","nha"], "left")
    .join(mfg_12m.alias("f"), ["cmpprtno","nha"], "left")
    .select(
        F.col("a.cmpprtno").alias("cmpprtno"),
        F.col("a.nha").alias("nha"),
        F.col("b.mfg_5yrs_consumption").alias("mfg_5yrs_consumption"),
        F.col("c.mfg_dmd").alias("mfg_dmd"),
        F.col("d.sprs_5yrs_consumption").alias("sprs_5yrs_consumption"),
        F.col("e.sprs_dmd").alias("sprs_dmd"),
        F.col("f.mfg_12mnths_consumption").alias("mfg_12mnths_consumption"),
        F.current_date().alias("last_modified_on")
    )
    .filter(
        (F.col("mfg_5yrs_consumption") > 0) |
        (F.col("mfg_dmd") > 0) |
        (F.col("sprs_5yrs_consumption") > 0) |
        (F.col("sprs_dmd") > 0) |
        (F.col("mfg_12mnths_consumption") > 0)
    )
)

final_df.createOrReplaceTempView("vw_makenha_dmd_cons")


# In[53]:


spark.sql(f"""
INSERT INTO {dmd_cons_target_path} (
    cmpprtno,
    last_modified_on,
    mfg_12mnths_consumption,
    mfg_5yrs_consumption,
    mfg_dmd,
    nha,
    sprs_5yrs_consumption,
    sprs_dmd
)
SELECT
    cmpprtno,
    last_modified_on,
    mfg_12mnths_consumption,
    mfg_5yrs_consumption,
    mfg_dmd,
    nha,
    sprs_5yrs_consumption,
    sprs_dmd
FROM vw_makenha_dmd_cons
""")


# #### Step-12 -- Mark MAKE-NHAs as Processed
# - Once demand and consumption have been calculated and written, all new MAKE-NHAs used in this run must be marked as processed so that:
#     - They are not reprocessed in the next run
#     - The next execution only picks up truly new NHAs.

# In[54]:


spark.sql(f"""
UPDATE {make_nha_target_path}
SET isprocessed = 'Y'
WHERE isprocessed = 'N'
""")


# #### Step-13 -- Logging / Audit INSERT (rpt_OMAT_LOG_DTLS)
# - Create a single audit record for this execution capturing:
#     - When the job ran:
#     - How many OB parent parts were processed
#     - How many NHAs were processed
#     - A run indicator (always 1 for a successful execution)
#     - The program/notebook name

# In[55]:


# Compute counts (same as SAP variables)
obprtno_cnt = mknha.select("cmpprtno").distinct().count()
obprtno_nha_cnt = mknha.select("nha").distinct().count()

# Insert audit record with explicit column list (Delta-safe)
spark.sql(f"""
INSERT INTO {log_table_path} (
    executed_on,
    obprtno_cnt,
    obprtno_nha_cnt,
    obpr_cnt,
    program_name
)
VALUES (
    current_timestamp(),
    {obprtno_cnt},
    {obprtno_nha_cnt},
    1,
    'SP_OMAT_INSERT_MAKENHA_DMD_CONS'
)
""")

