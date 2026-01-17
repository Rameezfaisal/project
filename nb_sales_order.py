#!/usr/bin/env python
# coding: utf-8

# ## nb_sales_order
# 
# New notebook

# ##### **Pyspark_Notebook :** nb_sales_order
# ##### 📌 **Author:** Rameez Ansari
# ##### 📅 **Last Updated:** 2026-01-12
# ###### 🔢 **Notebook Version:** 1.0
# ###### 🔄 **Change Log:** v1.0 | 2026-01-12 |
# ###### Rameez Ansari   → Work in Progress
# 📖 **Notebook Overview:**
# 
# This Notebook is used for creating SAP table zt_m_sales_order_status_v2 in fabric as sales_order
# 
# **Includes:**    
# - Target Tables This Notebook Populates:
#     1. **cdm.sales_order:** Provides ......

# In[2]:


from silketl import SqlDatabaseConnector, get_workspace_name, preload, load_data, postload,json,current_workspace_name


# In[78]:


# project_code = 'eng_omat' 
# ##FTR meaning feauture workspace
# exec_env = 'FTR'
# job_id = '' 
# task_id = '' 
# env = ''


# ##### Step-0 -- Imports and (Helper Function: Union ECC + S4H with schema alignment + Distinct)

# In[79]:


from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException

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


# #### Step-1 -- Config and Parameterize (Source/Target)
# - we also declare some required constants.
# - Creating tables.

# In[80]:


# --- Spark Configs for legacy time parser ---

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.avro.datetimeRebaseModeInWrite", "LEGACY")

# --- TABLE PATH PARAMETERS (ECC + S4H Sources) ---
paths = {
    "ecc": {
        "vbak":  "wsf_silk_glb_da_dev.lhs_glb.ecc.vbak",
        "vbap":  "wsf_silk_glb_da_dev.lhs_glb.ecc.vbap",
        "vbep":  "wsf_silk_glb_da_dev.lhs_glb.ecc.vbep",
        "vbuk":  "wsf_silk_glb_da_dev.lhs_glb.ecc.vbuk",
        "vbup":  "wsf_silk_glb_da_dev.lhs_glb.ecc.vbup",
        "likp":  "wsf_silk_glb_da_dev.lhs_glb.ecc.likp",
        "lips":  "wsf_silk_glb_da_dev.lhs_glb.ecc.lips",
        "vbfa":  "wsf_silk_glb_da_dev.lhs_glb.ecc.vbfa",
        "vbpa":  "wsf_silk_glb_da_dev.lhs_glb.ecc.vbpa",
        "kna1":  "wsf_silk_glb_da_dev.lhs_glb.ecc.kna1",
        "jest":  "wsf_silk_glb_da_dev.lhs_glb.ecc.jest",
        "tj30t": "wsf_silk_glb_da_dev.lhs_glb.ecc.tj30t",
        "mard":  "wsf_silk_glb_da_dev.lhs_glb.ecc.mard"
    },
    "s4h": {
        "vbak":  "wsf_silk_glb_da_dev.lhs_glb.s4h.vbak",
        "vbap":  "wsf_silk_glb_da_dev.lhs_glb.s4h.vbap",
        "vbep":  "wsf_silk_glb_da_dev.lhs_glb.s4h.vbep",
        "vbuk":  "wsf_silk_glb_da_dev.lhs_glb.s4h.vbuk",
        "vbup":  "wsf_silk_glb_da_dev.lhs_glb.s4h.vbup",
        "likp":  "wsf_silk_glb_da_dev.lhs_glb.s4h.likp",
        "lips":  "wsf_silk_glb_da_dev.lhs_glb.s4h.lips",
        "vbfa":  "wsf_silk_glb_da_dev.lhs_glb.s4h.vbfa",
        "vbpa":  "wsf_silk_glb_da_dev.lhs_glb.s4h.vbpa",
        "kna1":  "wsf_silk_glb_da_dev.lhs_glb.s4h.kna1",
        "jest":  "wsf_silk_glb_da_dev.lhs_glb.s4h.jest",
        "tj30t": "wsf_silk_glb_da_dev.lhs_glb.s4h.tj30t",
        "mard":  "wsf_silk_glb_da_dev.lhs_glb.s4h.mard"
    },
    "csbg":   "wsf_silk_glb_dt_qa.lhg_glb.eng.rpt_csbg_spares_deliveries",
}

# --- TARGET TABLE ---
T_SO_STATUS = "lhs_glb.eng_test.sales_order"

# --- BUSINESS CONSTANTS ---
CONST_LANG_KEY   = 'E'
CONST_STATUS_PRF = 'Z0000003'
CONST_WERKS_MIN  = '1900'

# --- EXECUTION RANGE ---
PARAM_LOW_VBELN  = "0000000000"
PARAM_HIGH_VBELN = "9999999999"


# --- Read Sources with Union ---
df_vbak = read_union_distinct("vbak").alias("vbak")
df_vbap = read_union_distinct("vbap").alias("vbap")
df_vbep = read_union_distinct("vbep").alias("vbep")
df_vbuk = read_union_distinct("vbuk").alias("vbuk")
df_vbup = read_union_distinct("vbup").alias("vbup")
df_likp = read_union_distinct("likp").alias("likp")
df_lips = read_union_distinct("lips").alias("lips")
df_vbfa = read_union_distinct("vbfa").alias("vbfa")
df_vbpa = read_union_distinct("vbpa").alias("vbpa")
df_kna1 = read_union_distinct("kna1").alias("kna1")
df_jest = read_union_distinct("jest").alias("jest")
df_tj30t = read_union_distinct("tj30t").alias("tj30t")
df_mard = read_union_distinct("mard").alias("mard")

# Single-source tables
df_csbg = spark.read.table(paths["csbg"]).alias("csbg")


# In[81]:


spark.sql(f"""CREATE TABLE IF NOT EXISTS {T_SO_STATUS}
(
  booking STRING,
  carrier STRING,
  carrier_name STRING,
  dlvstatus STRING,
  etenr STRING NOT NULL,
  expectedqtycumulative DECIMAL(13,3),
  f1_vbeln STRING,
  frontload STRING,
  last_changed TIMESTAMP,
  lastpgidoc STRING,
  mard_kinsm DECIMAL(13,3),
  mard_klabs DECIMAL(13,3),
  mard_labst DECIMAL(13,3),
  mard_lgort STRING,
  mard_matnr STRING,
  mard_werks STRING,
  order_status STRING,
  opendlvdoc STRING,
  opendlvqty DECIMAL(13,3),
  pgidate DATE,
  pgiqty DECIMAL(15,3),
  poreceived STRING,
  posnr STRING NOT NULL,
  qtyshippedttline DECIMAL(13,3),
  shipto STRING,
  shiptoname STRING,
  vbak_auart STRING,
  vbak_aufnr STRING,
  vbak_autlf STRING,
  vbak_bstnk STRING,
  vbak_erdat DATE,
  vbak_ernam STRING,
  vbak_faksk STRING,
  vbak_ihrez STRING,
  vbak_kunnr STRING,
  vbak_lifsk STRING,
  vbak_netwr DECIMAL(15,2),
  vbak_objnr STRING,
  vbak_vkorg STRING,
  vbak_vsnmr_v STRING,
  vbak_waerk STRING,
  vbap_abgru STRING,
  vbap_aedat DATE,
  vbap_arktx STRING,
  vbap_charg STRING,
  vbap_erdat DATE,
  vbap_ernam STRING,
  vbap_erzet STRING,
  vbap_faksp STRING,
  vbap_grkor STRING,
  vbap_kdmat STRING,
  vbap_kwmeng DECIMAL(15,3),
  vbap_lgort STRING,
  vbap_lprio STRING,
  vbap_matnr STRING,
  vbap_netpr DECIMAL(11,2),
  vbap_posex STRING,
  vbap_prodh STRING,
  vbap_vstel STRING,
  vbap_werks STRING,
  vbeln STRING NOT NULL,
  vbep_bmeng DECIMAL(13,3),
  vbep_edatu DATE,
  vbep_h_edatu DATE,
  vbep_lifsp STRING,
  vbep_mbdat DATE,
  vbuk_lfgsk STRING,
  vbuk_wbstk STRING,
  vbup_lfsta STRING,
  delvd_qty DOUBLE
)
USING DELTA """)


# #### Step-2 -- Read Sources & Apply Scope Filters
# - ​We load the raw data. Crucially, we apply "Predicate Pushdown" immediately. Instead of loading the entire 10+ years of SAP history, we only load the Sales Orders (vbeln) and their related Deliveries/Flows (vgbel/vbelv) that fall within the parameter range.

# In[82]:


# --- 1. CORE SALES DOCUMENTS (Filtered by Range) ---
# VBEP: Schedule Lines (The primary driver)
df_vbep = spark.read.table(S_VBEP).filter(
    (F.col("vbeln") >= PARAM_LOW_VBELN) & (F.col("vbeln") <= PARAM_HIGH_VBELN)
)

# VBAK: Header
df_vbak = spark.read.table(S_VBAK).filter(
    (F.col("vbeln") >= PARAM_LOW_VBELN) & (F.col("vbeln") <= PARAM_HIGH_VBELN)
)

# VBAP: Item
df_vbap = spark.read.table(S_VBAP).filter(
    (F.col("vbeln") >= PARAM_LOW_VBELN) & (F.col("vbeln") <= PARAM_HIGH_VBELN)
)

# VBUK: Header Status
df_vbuk = spark.read.table(S_VBUK).filter(
    (F.col("vbeln") >= PARAM_LOW_VBELN) & (F.col("vbeln") <= PARAM_HIGH_VBELN)
)

# VBUP: Item Status
df_vbup = spark.read.table(S_VBUP).filter(
    (F.col("vbeln") >= PARAM_LOW_VBELN) & (F.col("vbeln") <= PARAM_HIGH_VBELN)
)

# --- 2. DOCUMENT FLOW & DELIVERIES (Filtered by Preceding Doc) ---
# VBFA: Document Flow (Filter on Preceding Doc VBELV)
df_vbfa = spark.read.table(S_VBFA).filter(
    (F.col("vbelv") >= PARAM_LOW_VBELN) & (F.col("vbelv") <= PARAM_HIGH_VBELN)
)

# LIPS: Delivery Item (Filter on Reference Doc VGBEL = Sales Order)
df_lips = spark.read.table(S_LIPS).filter(
    (F.col("vgbel") >= PARAM_LOW_VBELN) & (F.col("vgbel") <= PARAM_HIGH_VBELN)
)

# LIKP: Delivery Header (Read full; joined later via LIPS)
df_likp = spark.read.table(S_LIKP) 

# CSBG: Spares Deliveries View (Filter on ORDER_NO)
df_csbg = spark.read.table(S_CSBG).filter(
    (F.col("order_no") >= PARAM_LOW_VBELN) & (F.col("order_no") <= PARAM_HIGH_VBELN)
)

# --- 3. MASTER DATA & STATUS ---
# These are dimensional tables loaded lazily
df_jest  = spark.read.table(S_JEST)
df_tj30t = spark.read.table(S_TJ30T)
df_mard  = spark.read.table(S_MARD)
df_kna1  = spark.read.table(S_KNA1)
df_vbpa  = spark.read.table(S_VBPA)


# #### Step-3 -- Carrier & Ship-To Party Logic
# - This cell calculates two isolated datasets:
#     - ​Carrier Info (CSBG): Determines the carrier for each order line.
#     - ​Ship-To Party (SHIPTO): Identifies the customer receiving the goods (Partner Function WE) from the header partners.
# - ​Technical Note:
#     - ​We use groupBy on CSBG to handle potential duplicates, matching the SAP MAX() logic.
#     - ​We filter VBPA for POSNR = '000000' (Header level) and PARVW = 'WE' (Ship-To) before joining to the Customer Master (KNA1).
# 
# 

# In[83]:


# --- 1. CSBG LOGIC (Carrier Info) ---
# SAP: GROUP BY "ORDER_NO","ORDER_LINE_NO" taking MAX of Carrier fields
ds_csbg = df_csbg.groupBy("order_no", "order_line_no").agg(
    F.max("carrier").alias("carrier_cd"),
    F.max("carrier_name").alias("carrier_name_cd")
)

# --- 2. SHIPTO LOGIC (Partner Function 'WE') ---
# SAP: Join VBPA with KNA1 where POSNR='000000' and PARVW='WE'
ds_shipto = df_vbpa.alias("v").join(
    df_kna1.alias("k"), 
    F.col("v.kunnr") == F.col("k.kunnr")
).filter(
    (F.col("v.posnr") == "000000") & 
    (F.col("v.parvw") == "WE")
).select(
    F.col("v.vbeln").alias("shipto_vbeln"),  # Renamed for join key safety
    F.col("v.kunnr").alias("shipto_cd"),
    F.col("k.name1").alias("shiptoname_cd")
)


# #### Step-4 -- Delivery & PGI Status Logic (LIKP/LIPS)
# - This cell analyzes the Deliveries associated with the Sales Orders. It calculates:
#     - ​Open Delivery Quantity: The quantity currently processing in deliveries.
#     - ​Shipped Quantity: The quantity that has actually been PGI'd (Post Goods Issue).
#     - ​Delivery Status: Whether the line is 'Created' or 'Shipped'.
# - ​Technical Note:
#     - ​We replicate the SAP WHERE clause strictness (wadat_ist <> ''), which implies this specific logic block focuses on processed/PGI'd deliveries.
#     - ​We exclude Returns (3%) and Internal/Service Orders (4%) based on the reference document logic.

# In[84]:


# --- LIKP_LIPS LOGIC ---
# Join Delivery Item (s) to Header (k)
ds_likp_lips = df_lips.alias("s").join(
    df_likp.alias("k"), 
    F.col("s.vbeln") == F.col("k.vbeln")
).filter(
    (F.col("s.vgbel") != "") & 
    (~F.col("s.vgbel").like("4%")) & 
    (~F.col("s.vgbel").like("3%")) &
    # Replicating SAP Logic: Filter for rows where PGI Date exists
    # Note: If your Lakehouse converts SAP empty strings to NULL, change to .isNotNull()
    (F.col("k.wadat_ist") != "") 
).groupBy(
    "s.vgbel", "s.vgpos"  # Group by Sales Order Number & Item
).agg(
    F.max("s.vbeln").alias("opendlvdoc_cd"),
    
    # OpenDlvQty: Sum of Delivery Quantity (LGMNG)
    F.sum("s.lgmng").alias("opendlvqty_cd"),
    
    # QtyShippedTtLine: If PGI Date is present, sum Actual Delivered Qty (LFIMG)
    # Since we filtered wadat_ist != '' in WHERE, this ELSE condition is the primary path
    F.when(F.max("k.wadat_ist") == "", 0)
     .otherwise(F.sum("s.lfimg")).alias("qtyshippedttline_cd"),
    
    # PGIDate: Max PGI Date
    F.max("k.wadat_ist").alias("pgidate_cd"),
    
    # DlvStatus: Derived Status
    F.when(F.max("k.wadat_ist") == "", "Created")
     .otherwise("Shipped").alias("dlvstatus_cd")
)


# #### Step-5 -- VBEP Cumulative Logic & First Schedule Line
# - This cell handles the "Schedule Lines" (VBEP), which define when and how much quantity is confirmed.
# - ​Expected Qty Cumulative: Calculates the running total of confirmed quantities (BMENG) for each line item.
# - ​Optimization:
#     - The SAP code used a slow "Self-Join" (t1.ETENR >= t2.ETENR). In Spark, we replace this with a highly efficient Window Function (Running Sum).
# - ​First Date (H): Captures the EDATU (Schedule Line Date) from the very first schedule line (ETENR = '0001') to establish the baseline date.
# 

# In[85]:


# --- 1. VBEP CUMULATIVE LOGIC ---
# Filter VBEP first as per SAP requirements (BMENG > 0)
df_vbep_filtered = df_vbep.filter(F.col("bmeng") > 0)

# Define Window: Partition by Order/Item, Order by Schedule Line Number (ETENR)
w_cum = Window.partitionBy("vbeln", "posnr").orderBy("etenr").rowsBetween(Window.unboundedPreceding, Window.currentRow)

ds_vbep_cum = df_vbep_filtered.select(
    "vbeln", 
    "posnr", 
    "etenr", 
    "bmeng",
    "edatu",
    "lifsp",
    "mbdat"
).withColumn(
    "expectedqtycumulative_cd", 
    F.sum("bmeng").over(w_cum) # Running Sum replacing the SAP Self-Join
)

# --- 2. H LOGIC (First Schedule Line Date) ---
# SAP: SELECT ... WHERE ETENR = '0001'
ds_h = df_vbep.filter(F.col("etenr") == "0001").select(
    F.col("vbeln").alias("h_vbeln"),
    F.col("posnr").alias("h_posnr"),
    F.col("edatu").alias("vbep_h_edatu_cd")
)


# #### Step-6 -- Cell 6: Document Flow Logic (F1 & F2)
# - This cell reconstructs the "Document Flow" (VBFA) to track the lifecycle of the order. We are looking for a two-step chain:
#     - ​Sales Order \rightarrow Delivery (Table A)
#     - ​Delivery \rightarrow Goods Issue / Invoice (Table B)
# - ​We calculate:
#     - ​F1 (Delivered Qty): How much quantity has successfully moved through the chain.
#     - ​F2 (PGI Qty): Specifically tracking quantities that have been "Posted Goods Issue" (finalized), often filtered by specific document types (like '49%' series).
# - ​Technical Note:
#     - ​df_vbfa (from Cell 2) contains flow starting with the Sales Order. This serves as "Table A".
#     - ​We need a new read of VBFA for "Table B" because the preceding document for Table B is a Delivery, not a Sales Order, so it wasn't captured in our initial Cell 2 filter.

# In[86]:


# --- PREPARE DATASETS ---

# Table A: Flow starting from the Sales Order (Already filtered in Cell 2)
# SAP Filter: STUFE='00' AND VBTYP_N IN ('T','J') (T=Returns, J=Delivery)
df_vbfa_a = df_vbfa.filter(
    (F.col("stufe") == "00") & 
    (F.col("vbtyp_n").isin("T", "J"))
).alias("a")

# Table B: Flow starting from the Delivery (Subsequent steps)
# We need to read VBFA again without the Sales Order Range filter
# Optimization: Filter for relevant child types to reduce scan size
df_vbfa_b = spark.read.table(S_VBFA).filter(
    F.col("vbtyp_n").isin("R", "h") # R=Invoice/PGI, h=Returns
).alias("b")

# --- BUILD THE JOIN ---
# Join A (Order->Dlv) to B (Dlv->PGI)
# SAP Join: a.VBELN (Delivery) = b.VBELV (Preceding Doc of PGI)
#           a.POSNN (Dlv Item) = b.POSNV (Preceding Item of PGI)
df_flow_joined = df_vbfa_a.join(
    df_vbfa_b,
    (F.col("a.vbeln") == F.col("b.vbelv")) & 
    (F.col("a.posnn") == F.col("b.posnv"))
)

# --- F1 LOGIC (Delivered Quantity) ---
ds_f1 = df_flow_joined.groupBy(
    F.col("a.vbelv").alias("vbelv"), # Original Sales Order
    F.col("a.posnv").alias("posnv")  # Original Sales Item
).agg(
    F.max("a.vbeln").alias("f1_vbeln_cd"), # Latest Delivery Doc
    
    # Conditional Sum matching SAP CASE statement
    F.sum(
        F.when((F.col("b.vbtyp_n") == "R") & (F.col("b.plmin") == "+"), F.coalesce(F.col("b.rfmng_flo"), F.lit(0)))
         .when(F.col("b.vbtyp_n") == "h", F.coalesce(F.col("b.rfmng_flo"), F.lit(0)) * -1)
         .otherwise(0)
    ).alias("delvd_qty_cd")
)

# --- F2 LOGIC (PGI Quantity & Last Doc) ---
# Same join, but extra filter on 'b' (Target Doc starts with 49, e.g., Pro Forma)
ds_f2 = df_flow_joined.filter(
    F.col("b.vbeln").like("49%")
).groupBy(
    F.col("a.vbelv"), 
    F.col("a.posnv")
).agg(
    F.max("b.vbeln").alias("lastpgidoc_cd"),
    
    # Conditional Sum for PGI
    F.sum(
        F.when(F.col("b.vbtyp_n") == "R", F.coalesce(F.col("b.rfmng"), F.lit(0)))
         .when(F.col("b.vbtyp_n") == "h", F.coalesce(F.col("b.rfmng"), F.lit(0)) * -1)
         .otherwise(0)
    ).alias("pgiqty_cd")
)


# #### Step-7 -- Status (JEST) & Inventory (MARD) Logic
# - This cell handles two critical lookups:
#     - ​Status Flags (JEST): Checks the system status for specific "blocks" or "progress markers" (like 'Front Load', 'PO Received'). We filter this by the OBJNR (Object Number) from the Sales Orders in our scope.
#     - ​Inventory Snapshot (MARD): Fetches current stock levels (LABST) for the material at the specific plant/storage location.
#     - ​Optimization: We implement a "Calculated Join Key" for the Storage Location (LGORT) to handle the complex business rule (swapping '0010' to '0030' for Plant '3000') efficiently without slowing down the join.
# 

# In[87]:


# --- 1. JEST STATUS LOGIC ---
# Filter JEST to only relevant Object Numbers from our Sales Order Header (VBAK)
# This prevents scanning the entire billion-row JEST table
target_objnrs = df_vbak.select("objnr").distinct()

ds_jest = df_jest.alias("j").join(
    target_objnrs, "objnr" # Inner join acts as a filter
).join(
    df_tj30t.alias("t"), 
    F.col("j.stat") == F.col("t.estat"), 
    "left"
).filter(
    (F.col("j.stat").isin('E0001', 'E0004', 'E0005', 'E0008', 'E0009')) & 
    (F.col("j.inact") != "X") & 
    (F.col("t.stsma") == CONST_STATUS_PRF) & # Z0000003
    (F.col("t.spras") == CONST_LANG_KEY)     # E
).groupBy("j.objnr").agg(
    F.max(F.when(F.col("j.stat") == "E0008", "X").otherwise("")).alias("frontload_cd"),
    F.max(F.when(F.col("j.stat") == "E0009", "X").otherwise("")).alias("poreceived_cd"),
    # Booking: If status is E0001/4/5, capture the text description
    F.max(F.when(F.col("j.stat").isin('E0001', 'E0004', 'E0005'), F.col("t.txt04")).otherwise("")).alias("booking_cd")
)

# --- 2. MARD (INVENTORY) LOGIC ---
# Pre-calculate the "Effective Storage Location" on the VBAP side to enable a fast Equi-Join
# Rule: If LGORT is empty or '0010' AND Plant is '3000', use '0030'. Else use LGORT (defaulting '0010' if empty).
df_vbap_calc = df_vbap.withColumn(
    "calc_lgort",
    F.when(
        ((F.coalesce(F.col("lgort"), F.lit("")) == "") | (F.col("lgort") == "0010")) & 
        (F.col("werks") == "3000"), 
        "0030"
    ).when(
        F.coalesce(F.col("lgort"), F.lit("")) != "", 
        F.col("lgort")
    ).otherwise("0010")
)

# We define this lookup to be joined in the final step
# We select only needed columns to keep the plan light
ds_mard = df_mard.select(
    F.col("matnr"), 
    F.col("werks"), 
    F.col("lgort"), 
    F.col("labst").alias("mard_labst"),
    F.col("klabs").alias("mard_klabs"),
    F.col("kinsm").alias("mard_kinsm")
)


# #### Step-8 -- Cell 8: Master Join & Final Transformation
# - This is the heart of the logic. We bring all the components together:
#     - ​Base: Schedule Lines (VBEP) driven by our scope.
#     - ​Enrichment: We join Header/Item details, Statuses, Document Flow (F1/F2), Deliveries, and Inventory.
#     - ​Calculation: We determine the final ORDER_STATUS ('Open'/'Closed') using the complex cumulative logic provided.
#     - ​Projection: We explicitly select and cast all columns to match the ZT_M_SALES_ORDER_STATUS_V2 target schema strictly.
# - OPtimization:
#     - We will force "Broadcast Joins" for the smaller lookups (SHIPTO, MARD, CSBG, etc) so they don't trigger a massive shuffle.

# In[88]:


# --- CELL 8: OPTIMIZED MASTER JOIN ---

# Optimization: Broadcast small/lookup tables to avoid shuffling huge fact tables
# We use .hint("broadcast") on datasets that are definitely smaller than the main VBEP/VBAK facts

df_master = ds_vbep_cum.alias("ep") \
    .join(df_vbuk.alias("uk"), "vbeln") \
    .join(df_vbak.alias("ak"), "vbeln") \
    .join(df_vbap_calc.alias("ap"), ["vbeln", "posnr"]) \
    .join(df_vbup.alias("up"), ["vbeln", "posnr"]) \
    .join(ds_h.alias("h").hint("broadcast"), (F.col("ep.vbeln") == F.col("h.h_vbeln")) & (F.col("ep.posnr") == F.col("h.h_posnr"))) \
    .join(ds_shipto.alias("sh").hint("broadcast"), F.col("ep.vbeln") == F.col("sh.shipto_vbeln")) \
    .join(ds_f1.alias("f1"), (F.col("ep.vbeln") == F.col("f1.f1_vbeln_cd")) & (F.col("ep.posnr") == F.col("f1.posnv")), "left") \
    .join(ds_f2.alias("f2"), (F.col("ep.vbeln") == F.col("f2.lastpgidoc_cd")) & (F.col("ep.posnr") == F.col("f2.posnv")), "left") \
    .join(ds_likp_lips.alias("ll"), (F.col("ep.vbeln") == F.col("ll.opendlvdoc_cd")) & (F.col("ep.posnr") == F.col("ll.vgpos")), "left") \
    .join(ds_jest.alias("je"), F.col("ak.objnr") == F.col("je.objnr"), "left") \
    .join(ds_csbg.alias("cs").hint("broadcast"), (F.col("ep.vbeln") == F.col("cs.order_no")) & (F.col("ep.posnr") == F.col("cs.order_line_no")), "left") \
    .join(ds_mard.alias("md"), 
          (F.col("ap.matnr") == F.col("md.matnr")) & 
          (F.col("ap.werks") == F.col("md.werks")) & 
          (F.col("ap.calc_lgort") == F.col("md.lgort")), 
          "left") \
    .filter(
        (F.col("ap.werks") >= CONST_WERKS_MIN) & 
        (~F.col("ak.auart").isin("ZVC", "ZR07"))
    )

# --- 2. ORDER STATUS CALCULATION (Unchanged) ---
c_wbstk  = F.coalesce(F.col("uk.wbstk"), F.lit(""))
c_abgru  = F.coalesce(F.col("ap.abgru"), F.lit(""))
c_lfsta  = F.coalesce(F.col("up.lfsta"), F.lit(""))
n_kwmeng = F.coalesce(F.col("ap.kwmeng"), F.lit(0))
n_delvd  = F.coalesce(F.col("f1.delvd_qty_cd"), F.lit(0))
n_bmeng  = F.coalesce(F.col("ep.bmeng"), F.lit(0))
n_expcum = F.coalesce(F.col("ep.expectedqtycumulative_cd"), F.lit(0))

calc_remaining = F.when(n_expcum <= n_delvd, 0) \
                  .when((n_expcum - n_delvd) <= n_bmeng, (n_expcum - n_delvd)) \
                  .otherwise(n_bmeng)

df_calculated = df_master.withColumn(
    "calc_order_status",
    F.when(
        (c_wbstk != "C") & 
        (c_abgru == "") & 
        (c_lfsta != "") & 
        (n_kwmeng > n_delvd) & 
        (n_bmeng > 0) & 
        (calc_remaining > 0), 
        "Open"
    ).otherwise("Closed")
)

# --- 3. FINAL PROJECTION (Unchanged) ---
final_df = df_calculated.select(
    # ... (Keep your select list exactly as before)
    F.col("je.booking_cd").alias("Booking"),
    F.col("cs.carrier_cd").alias("CARRIER"),
    F.col("cs.carrier_name_cd").alias("CARRIER_NAME"),
    F.col("ll.dlvstatus_cd").alias("DlvStatus"),
    F.col("ep.etenr").alias("ETENR"),
    F.col("ep.expectedqtycumulative_cd").cast("decimal(13,3)").alias("ExpectedQtyCumulative"),
    F.col("f1.f1_vbeln_cd").alias("F1_VBELN"),
    F.col("je.frontload_cd").alias("FrontLoad"),
    F.current_timestamp().alias("LAST_CHANGED"),
    F.col("f2.lastpgidoc_cd").alias("LastPGIDoc"),
    F.col("md.mard_kinsm").cast("decimal(13,3)").alias("MARD_KINSM"),
    F.col("md.mard_klabs").cast("decimal(13,3)").alias("MARD_KLABS"),
    F.col("md.mard_labst").cast("decimal(13,3)").alias("MARD_LABST"),
    F.col("md.lgort").alias("MARD_LGORT"),
    F.col("md.matnr").alias("MARD_MATNR"),
    F.col("md.werks").alias("MARD_WERKS"),
    F.col("calc_order_status").alias("ORDER_STATUS"),
    F.col("ll.opendlvdoc_cd").alias("OpenDlvDoc"),
    F.col("ll.opendlvqty_cd").cast("decimal(13,3)").alias("OpenDlvQty"),
    F.col("ll.pgidate_cd").cast("date").alias("PGIDate"),
    F.col("f2.pgiqty_cd").cast("decimal(15,3)").alias("PGIQty"),
    F.col("je.poreceived_cd").alias("POReceived"),
    F.col("ep.posnr").alias("POSNR"),
    F.col("ll.qtyshippedttline_cd").cast("decimal(13,3)").alias("QtyShippedTtLine"),
    F.col("sh.shipto_cd").alias("ShipTo"),
    F.col("sh.shiptoname_cd").alias("ShipToName"),
    F.col("ak.auart").alias("VBAK_AUART"),
    F.col("ak.aufnr").alias("VBAK_AUFNR"),
    F.col("ak.autlf").alias("VBAK_AUTLF"),
    F.col("ak.bstnk").alias("VBAK_BSTNK"),
    F.col("ak.erdat").alias("VBAK_ERDAT"),
    F.col("ak.ernam").alias("VBAK_ERNAM"),
    F.col("ak.faksk").alias("VBAK_FAKSK"),
    F.col("ak.ihrez").alias("VBAK_IHREZ"),
    F.col("ak.kunnr").alias("VBAK_KUNNR"),
    F.col("ak.lifsk").alias("VBAK_LIFSK"),
    F.col("ak.netwr").cast("decimal(15,2)").alias("VBAK_NETWR"),
    F.col("ak.objnr").alias("VBAK_OBJNR"),
    F.col("ak.vkorg").alias("VBAK_VKORG"),
    F.col("ak.vsnmr_v").alias("VBAK_VSNMR_V"),
    F.col("ak.waerk").alias("VBAK_WAERK"),
    F.col("ap.abgru").alias("VBAP_ABGRU"),
    F.col("ap.aedat").alias("VBAP_AEDAT"),
    F.col("ap.arktx").alias("VBAP_ARKTX"),
    F.col("ap.charg").alias("VBAP_CHARG"),
    F.col("ap.erdat").alias("VBAP_ERDAT"),
    F.col("ap.ernam").alias("VBAP_ERNAM"),
    F.col("ap.erzet").alias("VBAP_ERZET"),
    F.col("ap.faksp").alias("VBAP_FAKSP"),
    F.col("ap.grkor").alias("VBAP_GRKOR"),
    F.col("ap.kdmat").alias("VBAP_KDMAT"),
    F.col("ap.kwmeng").cast("decimal(15,3)").alias("VBAP_KWMENG"),
    F.col("ap.lgort").alias("VBAP_LGORT"),
    F.col("ap.lprio").alias("VBAP_LPRIO"),
    F.col("ap.matnr").alias("VBAP_MATNR"),
    F.col("ap.netpr").cast("decimal(11,2)").alias("VBAP_NETPR"),
    F.col("ap.posex").alias("VBAP_POSEX"),
    F.col("ap.prodh").alias("VBAP_PRODH"),
    F.col("ap.vstel").alias("VBAP_VSTEL"),
    F.col("ap.werks").alias("VBAP_WERKS"),
    F.col("ep.vbeln").alias("VBELN"),
    F.col("ep.bmeng").cast("decimal(13,3)").alias("VBEP_BMENG"),
    F.col("ep.edatu").alias("VBEP_EDATU"),
    F.col("h.vbep_h_edatu_cd").alias("VBEP_H_EDATU"),
    F.col("ep.lifsp").alias("VBEP_LIFSP"),
    F.col("ep.mbdat").alias("VBEP_MBDAT"),
    F.col("uk.lfgsk").alias("VBUK_LFGSK"),
    F.col("uk.wbstk").alias("VBUK_WBSTK"),
    F.col("up.lfsta").alias("VBUP_LFSTA"),
    F.col("f1.delvd_qty_cd").cast("double").alias("delvd_qty")
)


# #### Step-9 -- Write to Delta Lake (MERGE / Upsert)
# - This final step persists the results. We use a Delta MERGE operation (equivalent to SAP UPSERT).
#     - ​Match Logic: If a record exists with the same Sales Order (VBELN), Item (POSNR), and Schedule Line (ETENR), we update it with the new calculations.
#     - ​New Records: If it doesn't exist, we insert it.
# - ​Schema Safety: Since we carefully renamed every column in final_df (Cell 8) to match the target table exactly, we can use the UPDATE SET * and INSERT * shorthand, which makes the code cleaner and easier to maintain.
# - ​We explicitly break the skew by reshuffling the data on the Key (VBELN) into 200 (or more) clean partitions before the MERGE starts.

# In[89]:


# --- CELL 9: ROBUST MERGE WITH DEDUPLICATION ---

# 1. Enforce Uniqueness (Crucial Fix)
# We drop duplicates on the Primary Key. 
# If a join caused an explosion, this arbitrarily picks one (safe for status updates).
final_df_clean = final_df.dropDuplicates(["VBELN", "POSNR", "ETENR"])

# 2. Filter Null Keys (Safety Net)
# Delta will fail if the Join Keys are Null.
final_df_clean = final_df_clean.filter(
    F.col("VBELN").isNotNull() & 
    F.col("POSNR").isNotNull() & 
    F.col("ETENR").isNotNull()
)

# 3. Repartition for Write Stability
# We keep the repartitioning to prevent the Skew error you saw earlier.
final_df_safe = final_df_clean.repartition(200, "VBELN")

# 4. Register View
final_df_safe.createOrReplaceTempView("source_updates")

print("Starting Merge...")

# 5. Execute Merge
spark.sql(f"""
MERGE INTO {T_SO_STATUS} AS target
USING source_updates AS source
ON target.VBELN = source.VBELN 
   AND target.POSNR = source.POSNR 
   AND target.ETENR = source.ETENR

WHEN MATCHED THEN
  UPDATE SET *

WHEN NOT MATCHED THEN
  INSERT *
""")

print(f"Merge completed successfully into {T_SO_STATUS}")


# In[91]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql

# SELECT count(*) FROM lhs_glb.eng_test.sales_order

