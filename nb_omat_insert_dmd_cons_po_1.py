#!/usr/bin/env python
# coding: utf-8

# ## nb_omat_insert_dmd_cons_po_1
# 
# New notebook

# ##### **Pyspark_Notebook :** nb_omat_insert_dmd_cons_po_1
# ##### 📌 **Author:** Rameez Ansari
# ##### 📅 **Last Updated:** 2025-12-31
# ###### 🔢 **Notebook Version:** 1.0
# ###### 🔄 **Change Log:** v1.0 | 2025-12-31 |
# ###### Rameez Ansari   → Work in Progress
# 📖 **Notebook Overview:**
# 
# This Notebook identifies the demand, consumption, open order quantity, Lam quantity On Hand, Supplier Info and product details of BUY NHAs in OB PR and insert it into OMAT Tables for OMAT Application.	
# 
# **Includes:**    
# - Target Tables This Notebook Populates:
#     1. **rpt_omat_dmd_consumption_dtls :** Captures historical consumption and demand forecast for obsolete parts and NHAs, used for run-out and planning.
#         - Load Type : Incremental append based on NHAs where isprocessed = 'N' on (cmpprtno, nha).
# 
#     3. **rpt_omat_log_dtls :** Logs OMAT system activities like job executions and email refresh requests for tracking and troubleshooting.
#         - Incremental append (new log entry per run); no key-based current timestamp insert.
# 

# In[2]:


from pyspark.sql import Row
from pyspark.sql import functions as F
from delta.tables import DeltaTable
 
import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants
 
from silketl import SqlDatabaseConnector, get_workspace_name, preload, load_data, postload,json,current_workspace_name, get_workspaceid_by_name
 


# In[3]:


project_code = 'eng_omat'
job_id = '123'
task_id = '14703'
env = 'FTR'
 


# In[4]:


# sql_db = SqlDatabaseConnector(env)
# sql_query = f"select * from config.vw_transform_egress_ingress_mapping where transform_egress_id = ? and project_code = ?"
# object_df = sql_db.execute(sql_query,(task_id,project_code))
# display(object_df)
 


# In[5]:


# tgt_system_properties = json.loads(object_df['TGT_SYSTEM_PROPERTIES'][0])
# # tgt_dt_workspacename = get_workspace_name(workspace_name = tgt_system_properties['workspaceName'])
# tgt_dt_workspacename = tgt_system_properties['workspaceName']
# src_dt_workspace_id = get_workspaceid_by_name(tgt_dt_workspacename)
# print(tgt_dt_workspacename)
# print(src_dt_workspace_id)
 


# In[6]:


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
# lh_sch_eng = "lhg_glb.eng"
 
 
 
# lakehousename = str(object_df['TGT_SYSTEM_CODE'][0])
 
# target_path = (
# f"abfss://{dt_workspace}@onelake.dfs.fabric.microsoft.com/"
# f"{lakehousename}.Lakehouse/Tables/{schema_name}/{delta_table_name}")
 
# stg_target_path = (
# f"abfss://{dt_workspace}@onelake.dfs.fabric.microsoft.com/"
# f"{lakehousename}.Lakehouse/Tables/{schema_name}/{delta_table_name}_stg")
 
# fqn_target_table = f"{dt_workspace}.{lakehousename}.{schema_name}.{delta_table_name}"
 


# #### Step-1 -- Imports

# In[8]:


from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ##### Helper Function for Union

# In[9]:


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


# #### Step-2 -- Parameterize (Source/Target) path

# In[10]:


# Initialize once
paths = {
    "ecc": {
        "mseg":        "wsf_silk_glb_da_dev.lhs_glb.ecc.mseg",
        "mkpf":        "wsf_silk_glb_da_dev.lhs_glb.ecc.mkpf",
        "makt":        "wsf_silk_glb_da_dev.lhs_glb.ecc.makt",
        "zmmforecast": "wsf_silk_glb_da_dev.lhs_glb.ecc.zmmforecast",
        "mard":        "wsf_silk_glb_da_dev.lhs_glb.ecc.mard",
        "zpo_hstry":   "wsf_silk_glb_da_dev.lhs_glb.ecc.zpo_hstry",
        "ekpo":        "wsf_silk_glb_da_dev.lhs_glb.ecc.ekpo",
    },
    "s4h": {
        "mseg":        "wsf_silk_glb_da_dev.lhs_glb.s4h.mseg",
        "mkpf":        "wsf_silk_glb_da_dev.lhs_glb.s4h.mkpf",
        "makt":        "wsf_silk_glb_da_dev.lhs_glb.s4h.makt",
        "zmmforecast": "wsf_silk_glb_da_dev.lhs_glb.s4h.zmmforecast",
        "mard":        "wsf_silk_glb_da_dev.lhs_glb.s4h.mard",
        "zpo_hstry":   "wsf_silk_glb_da_dev.lhs_glb.s4h.zpo_hstry",
        "ekpo":        "wsf_silk_glb_da_dev.lhs_glb.s4h.ekpo",
    }
}


# ===== Targets =====
tgt_dmd  = f"{t_workspace}.{t_lakehouse}.rpt_omat_dmd_consumption_dtls"
tgt_log  = f"{t_workspace}.{t_lakehouse}.rpt_omat_log_dtls"

# ===== Control =====
today = F.current_date()



# #### Step-3 -- Reading Source tables

# In[11]:


# mseg      = read_union_fast("mseg")
# mkpf      = read_union_fast("mkpf")
# makt      = read_union_fast("makt")
# forecast  = read_union_fast("zmmforecast")
# mard      = read_union_fast("mard")
# po        = read_union_fast("zpo_hstry")
# ekpo      = read_union_fast("ekpo")

# spares = spark.table("wsf_silk_glb_dt_qa.lhg_glb.eng.rpt_csbg_spares_deliveries")
# suppliers_by_plant = spark.table(f"{t_workspace}.{t_lakehouse}.rpt_omat_buynha_suppliers_plants")

# nha = (
#     spark.table(f"{t_workspace}.{t_lakehouse}.rpt_omat_obpn_buy_nha_dtls")
#          .select("cmpprtno", "nha", "isprocessed")
#          .filter(F.col("isprocessed") == "N")
#          .distinct()
# )



# In[12]:


# #### Step-3 -- Reading Source tables (Fixed Type Mismatch)

from pyspark.sql import functions as F
from datetime import date

# 1. Define Date/Year Cutoffs
today = F.current_date()
five_years_ago = F.date_sub(today, 365*6)

# CRITICAL FIX: Convert start_year to STRING.
# SAP stores MJAHR as string. Comparing Int (2020) >= String ('2020') kills performance.
# By making this a string, we enable Partition Pruning.
start_year_str = str(date.today().year - 6)

print(f"Filtering MSEG for MJAHR >= '{start_year_str}' (String Match)")

# --- 1. Read MKPF (Header) ---
try:
    mkpf_ecc = spark.read.table(paths["ecc"]["mkpf"]) \
                    .select("mblnr", "mjahr", "bldat") \
                    .filter(F.col("bldat") >= five_years_ago)
except:
    mkpf_ecc = None

try:
    mkpf_s4h = spark.read.table(paths["s4h"]["mkpf"]) \
                    .select("mblnr", "mjahr", "bldat") \
                    .filter(F.col("bldat") >= five_years_ago)
except:
    mkpf_s4h = None

# Safe Union MKPF
if mkpf_ecc and mkpf_s4h:
    mkpf = mkpf_ecc.unionByName(mkpf_s4h)
elif mkpf_ecc:
    mkpf = mkpf_ecc
else:
    mkpf = mkpf_s4h

# --- 2. Read MSEG (Item) ---
# FIX: Filter applied using STRING variable (start_year_str)
try:
    mseg_ecc = spark.read.table(paths["ecc"]["mseg"]) \
                    .select("mblnr", "mjahr", "zeile", "matnr", "menge", "shkzg", "bwart") \
                    .filter(F.col("mjahr") >= start_year_str) 
except:
    mseg_ecc = None

try:
    mseg_s4h = spark.read.table(paths["s4h"]["mseg"]) \
                    .select("mblnr", "mjahr", "zeile", "matnr", "menge", "shkzg", "bwart") \
                    .filter(F.col("mjahr") >= start_year_str)
except:
    mseg_s4h = None

# Safe Union MSEG
if mseg_ecc and mseg_s4h:
    mseg = mseg_ecc.unionByName(mseg_s4h)
elif mseg_ecc:
    mseg = mseg_ecc
else:
    mseg = mseg_s4h

# --- 3. Read Other Tables ---
def simple_union(key):
    try: df1 = spark.read.table(paths["ecc"][key])
    except: df1 = None
    try: df2 = spark.read.table(paths["s4h"][key])
    except: df2 = None
    if df1 and df2: return df1.unionByName(df2, allowMissingColumns=True)
    return df1 if df1 else df2


makt      = read_union_fast("makt")
forecast  = read_union_fast("zmmforecast")
mard      = read_union_fast("mard")
po        = read_union_fast("zpo_hstry")
ekpo      = read_union_fast("ekpo")

spares = spark.table("wsf_silk_glb_dt_qa.lhg_glb.eng.rpt_csbg_spares_deliveries")
suppliers_by_plant = spark.table(f"{t_workspace}.{t_lakehouse}.rpt_omat_buynha_suppliers_plants")

nha = (
    spark.table(f"{t_workspace}.{t_lakehouse}.rpt_omat_obpn_buy_nha_dtls")
         .select("cmpprtno", "nha", "isprocessed")
         .filter(F.col("isprocessed") == "N")
         .distinct()
)


# #### Step-4 -- Creating NHALIST Temp table

# In[13]:


# 1. Increase Broadcast Threshold to 200MB (Default is 10MB)
# Since nhalist has ~625k rows, it is likely around 50-70MB. This config enables Broadcast join.
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "209715200") 

# 2. Cache the list. 
# It is used 9 times. Caching prevents 9 separate reads/computations.
nhalist = nha.select("cmpprtno", "nha").distinct().cache()


# #### Step-5 -- Calculating Manufacturing Consumption

# In[14]:


# #### Step-5 (Corrected & 100% Faithful) -- Manufacturing Consumption
# Fixes:
# 1. Join Key: Uses 'mblnr' ONLY (Removes 'mjahr'). Matches SAP exactly.
#    Note: This allows cross-year matches (Cartesian product) if doc IDs recycle.
# 2. MAKT Logic: No 'SPRAS' filter. Groups by 'maktx' to replicate SAP's 
#    grain (summing per description language), preserving the exact aggregation behavior.

mfg_all = (
    mseg.join(mkpf, "mblnr", "inner") # <--- FAITHFUL: Join on MBLNR only
        # 1. Scope Filter
        .join(F.broadcast(nhalist), mseg.matnr == nhalist.nha) 
        # 2. MAKT Join (Unfiltered, matches SAP)
        .join(makt, mseg.matnr == makt.matnr, "inner")
        .filter(
            (mseg.bwart.isin("261","262")) &
            # 3. Global Time Window (Matches SAP's implicit bounds for the data range)
            (mkpf.bldat > F.date_sub(today, 365*5)) &
            (mkpf.bldat < today) 
        )
        # 4. Group by MAKTX: Mirrors SAP's handling of multi-language rows
        .groupBy(
            nhalist.cmpprtno.alias("cmpprtno_cd"), 
            mseg.matnr.alias("nha_cd"), 
            makt.maktx 
        )
        .agg(
            # 12 Month Logic
            F.sum(
                F.when((mkpf.bldat > F.date_sub(today, 365)) & (mseg.shkzg=="S"), -mseg.menge)
                 .when((mkpf.bldat > F.date_sub(today, 365)), mseg.menge)
                 .otherwise(0)
            ).alias("mfg_12mnths_consumption"),
            
            # 3 Year Logic
            F.sum(
                F.when((mkpf.bldat > F.date_sub(today, 365*3)) & (mseg.shkzg=="S"), -mseg.menge)
                 .when((mkpf.bldat > F.date_sub(today, 365*3)), mseg.menge)
                 .otherwise(0)
            ).alias("mfg_3yrs_consumption"),
            
            # 5 Year Logic
            F.sum(
                F.when(mseg.shkzg=="S", -mseg.menge).otherwise(mseg.menge)
            ).alias("mfg_5yrs_consumption")
        )
        # 5. Distinct: Collapses the multi-language rows (Matches SAP Insert DISTINCT)
        .drop("maktx")
        .distinct()
)


# #### Step-6 -- Calculating Manufacturing Demand (26 weeks)

# In[15]:


# dmd_cols = ["dmdpd"] + [f"dmd{i}" for i in range(1,27)]

# mfgdmd = (
#     forecast
#     .join(F.broadcast(nhalist), forecast.matnr == nhalist.nha, "inner")
#     .filter(
#         (forecast.rowtype == "06") &
#         (forecast.werks == "COMB")
#     )
#     .groupBy(
#         nhalist.cmpprtno.alias("cmpprtno_cd"),
#         forecast.matnr.alias("nha_cd")
#     )
#     .agg(
#         sum(F.sum(F.col(c)) for c in dmd_cols).alias("mfg_dmd")
#     )
# )


# #### Step-7 -- Spares Demand (26 weeks)

# In[16]:


# dmd_cols = ["dmdpd"] + [f"dmd{i}" for i in range(1,27)]

# sprsdmd = (
#     forecast
#     .join(F.broadcast(nhalist), forecast.matnr == nhalist.nha, "inner")
#     .filter(
#         (forecast.rowtype == "07") &
#         (forecast.werks == "COMB")
#     )
#     .groupBy(
#         nhalist.cmpprtno.alias("cmpprtno_cd"),
#         forecast.matnr.alias("nha_cd")
#     )
#     .agg(
#         sum(F.sum(F.col(c)) for c in dmd_cols).alias("sprs_dmd")
#     )
# )


# In[17]:


# #### Step-6 & 7 (Consolidated) -- Mfg & Spares Demand
# OPTIMIZATION: Reads Forecast table only once.

dmd_cols = ["dmdpd"] + [f"dmd{i}" for i in range(1,27)]
# Create a single expression to sum all dmd columns
total_dmd_expr = sum(F.col(c) for c in dmd_cols)

demand_all = (
    forecast
    .join(F.broadcast(nhalist), forecast.matnr == nhalist.nha, "inner")
    .filter(
        (forecast.werks == "COMB") &
        (forecast.rowtype.isin("06", "07")) # Filter for both types
    )
    .groupBy(
        nhalist.cmpprtno.alias("cmpprtno_cd"),
        forecast.matnr.alias("nha_cd")
    )
    .agg(
        # Conditional Sum based on Row Type
        F.sum(F.when(forecast.rowtype == "06", total_dmd_expr).otherwise(0)).alias("mfg_dmd"),
        F.sum(F.when(forecast.rowtype == "07", total_dmd_expr).otherwise(0)).alias("sprs_dmd")
    )
)


# #### Step-8 Spares Consumption
# - Calculates total spares consumption over the last 3 years from
# CSBG_SPARES_DELIVERIES, filtered by order type, source doc, and GI date, grouped by CMPPRTNO + NHA.

# In[18]:


# # #### Step-8a -- Spares 3-Year Consumption

# sprs3 = (
#     spares
#     .join(F.broadcast(nhalist), spares.material == nhalist.nha, "inner")
#     .filter(
#         (spares.source_doc != "STO") &
#         (spares.order_type.isin("TA","ZCON","ZO09","ZO04","ZSD","ZSO","ZFO","ZUP")) &
#         (spares.actual_gi > F.date_sub(F.current_date(), 365*3))
#     )
#     .groupBy(
#         nhalist.cmpprtno.alias("cmpprtno_cd"),
#         spares.material.alias("nha_cd")
#     )
#     .agg(
#         F.sum(spares.qty_shipped).alias("sprs_3yrs_consumption")
#     )
# )


# # #### Step-8b -- Spares 5-Year Consumption

# sprs5y = (
#     spares
#     .join(F.broadcast(nhalist), spares.material == nhalist.nha, "inner")
#     .filter(
#         (spares.source_doc != "STO") &
#         (spares.order_type.isin("TA","ZCON","ZO09","ZO04","ZSD","ZSO","ZFO","ZUP")) &
#         (spares.actual_gi > F.date_sub(F.current_date(), 365*5)) # Last 5 years
#     )
#     .groupBy(
#         nhalist.cmpprtno.alias("cmpprtno_cd"),
#         spares.material.alias("nha_cd")
#     )
#     .agg(
#         F.sum(spares.qty_shipped).alias("sprs_5yrs_consumption")
#     )
# )


# In[19]:


# #### Step-8 (Consolidated) -- Spares Consumption (3yr, 5yr)
# OPTIMIZATION: Reads Spares table only once.

sprs_all = (
    spares
    .join(F.broadcast(nhalist), spares.material == nhalist.nha, "inner")
    .filter(
        (spares.source_doc != "STO") &
        (spares.order_type.isin("TA","ZCON","ZO09","ZO04","ZSD","ZSO","ZFO","ZUP")) &
        # Filter for widest range (5 years)
        (spares.actual_gi > F.date_sub(F.current_date(), 365*5))
    )
    .groupBy(
        nhalist.cmpprtno.alias("cmpprtno_cd"),
        spares.material.alias("nha_cd")
    )
    .agg(
        F.sum(
            F.when(spares.actual_gi > F.date_sub(F.current_date(), 365*3), spares.qty_shipped)
             .otherwise(0)
        ).alias("sprs_3yrs_consumption"),
        
        F.sum(spares.qty_shipped).alias("sprs_5yrs_consumption")
    )
)


# #### Step-9 -- LAM Quantity On Hand & Restricted Stock
# Calculates LAMQOH and RESTRICTED_ALL_STOCK from "ecc.mard", excluding storage location V014, grouped by CMPPRTNO + NHA.

# In[20]:


lamqoh = (
    mard
    .join(F.broadcast(nhalist), mard.matnr == nhalist.nha, "inner")
    .filter(~mard.lgort.isin("V014"))
    .groupBy(
        nhalist.cmpprtno.alias("cmpprtno_cd"),
        mard.matnr.alias("nha_cd")
    )
    .agg(
        F.sum(mard.labst + mard.klabs).alias("lamqoh"),
        F.sum(mard.insme + mard.umlme + mard.einme + mard.speme).alias("restricted_all_stock")
    )
)


# #### Step-10 -- Open Purchase Order Quantity
# It Calculates OPEN_PO_QTY per CMPPRTNO + NHA using PO history and "EKPO"

# In[21]:


openpo = (
    po
    .join(ekpo, ["ebeln", "ebelp"], "inner")
    .join(F.broadcast(nhalist), po.matnr == nhalist.nha, "inner")
    .filter(
        (~F.substring(po.due_dt, -5, 5).isin("04-01", "12-31")) &
        (po.status != "INACT") &
        (F.trim(po.matnr) != "") &
        (F.trim(po.loekz) == "") &
        (po.aussl != "U3") &
        (po.bsart != "UB") &
        (po.elikz != "X") &
        (~ekpo.pstyp.isin("7", "9"))
    )
    .groupBy(
        nhalist.cmpprtno.alias("cmpprtno_cd"),
        po.matnr.alias("nha_cd")
    )
    .agg(
        F.sum(po.menge - po.wemng).alias("open_po_qty")
    )
)


# #### Step-11 -- Final UNION + Insert into "rpt_omat_dmd_consumption_dtls"
# 
# - Always insert rows where CMPPRTNO = NHA
# - Insert all other rows only if at least one metric > 0
# 

# In[22]:


# #### Step-11 -- Final Insert (Faithful Logic)

base = (
    nhalist.alias("a")
    .join(mfg_all.alias("b"), nhalist.nha == mfg_all.nha_cd, "left")
    .join(demand_all.alias("c"), nhalist.nha == demand_all.nha_cd, "left")
    .join(sprs_all.alias("d"), nhalist.nha == sprs_all.nha_cd, "left")
    .join(lamqoh.alias("g"), nhalist.nha == lamqoh.nha_cd, "left")
    .join(openpo.alias("h"), nhalist.nha == openpo.nha_cd, "left")
    .select(
        nhalist.cmpprtno.alias("cmpprtno"),
        nhalist.nha.alias("nha"),
        F.col("b.mfg_3yrs_consumption").cast("int"),
        F.col("b.mfg_12mnths_consumption").cast("int"),
        F.col("b.mfg_5yrs_consumption").cast("int"),
        F.col("c.mfg_dmd").cast("int"),
        F.col("c.sprs_dmd").cast("int"),
        F.col("d.sprs_3yrs_consumption").cast("int"),
        F.col("d.sprs_5yrs_consumption").cast("int"),
        F.col("g.lamqoh").cast("int"),
        F.col("g.restricted_all_stock").cast("int"),
        F.col("h.open_po_qty").cast("int")
    )
)

# --- INCLUSION LOGIC (UNION DISTINCT EQUIVALENT) ---

# Branch 1: Self-Reference (CMPPRTNO = NHA)
cond_self = (F.col("cmpprtno") == F.col("nha"))

# Branch 2: Activity (Any metric > 0)
# Note: Uses explicit '|' for logical OR between all metrics
cond_activity = (
    (F.coalesce(F.col("mfg_3yrs_consumption"), F.lit(0)) > 0) |
    (F.coalesce(F.col("mfg_dmd"), F.lit(0)) > 0) |
    (F.coalesce(F.col("sprs_dmd"), F.lit(0)) > 0) |
    (F.coalesce(F.col("sprs_3yrs_consumption"), F.lit(0)) > 0) |
    (F.coalesce(F.col("mfg_12mnths_consumption"), F.lit(0)) > 0) |
    (F.coalesce(F.col("open_po_qty"), F.lit(0)) > 0) |
    (F.coalesce(F.col("mfg_5yrs_consumption"), F.lit(0)) > 0) |
    (F.coalesce(F.col("sprs_5yrs_consumption"), F.lit(0)) > 0)
)

# Logic: (Branch 1) OR (Branch 2) -> Matches SAP UNION DISTINCT
final = (
    base
    .filter(cond_self | cond_activity) 
    .distinct()
    .withColumn("last_modified_on", F.current_date())
)

final.createOrReplaceTempView("final_dmd")

spark.sql(f"""
INSERT INTO {tgt_dmd} 
(cmpprtno, nha, mfg_3yrs_consumption, mfg_dmd, sprs_dmd, sprs_3yrs_consumption,
 mfg_12mnths_consumption, lamqoh, restricted_all_stock, open_po_qty, 
 last_modified_on, mfg_5yrs_consumption, sprs_5yrs_consumption)
SELECT DISTINCT
 cmpprtno, nha, mfg_3yrs_consumption, mfg_dmd, sprs_dmd, sprs_3yrs_consumption,
 mfg_12mnths_consumption, lamqoh, restricted_all_stock, open_po_qty, 
 last_modified_on, mfg_5yrs_consumption, sprs_5yrs_consumption
FROM final_dmd
""")


# #### Step-12 -- Logging
# 
# - This records: how many OB parts were processed, how many NHAs were processed, when the procedure ran.

# In[23]:


obprt_cnt = nhalist.select("cmpprtno").distinct().count()
nha_cnt = nhalist.select("nha").distinct().count()

spark.sql(f"""
INSERT INTO {tgt_log}
(program_name, obpr_cnt, obprtno_cnt, obprtno_nha_cnt, executed_on)
VALUES
('nb_omat_insert_dmd_cons_po_1', 1, {obprt_cnt}, {nha_cnt}, current_timestamp())
""")

# Clean up
nhalist.unpersist()

