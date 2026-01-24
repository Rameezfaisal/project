#!/usr/bin/env python
# coding: utf-8

# ## nb_omat_update_suppliers_byplant
# 
# New notebook

# ##### **Pyspark_Notebook :** nb_omat_update_suppliers_byplant
# ##### 📌 **Author:** Rameez Ansari
# ##### 📅 **Last Updated:** 2025-12-26
# ###### 🔢 **Notebook Version:** 1.0
# ###### 🔄 **Change Log:** v1.0 | 2025-12-26 |
# ###### Rameez Ansari   → Work in Progress
# 
# 📖 **Notebook Overview:**
# The purpose of this notebook is to identifies the MRP Preffered and Active suppliers in various MFG WHS in LAM. This notebook also identifies the supplier with whom we have the OPEN PO and the supplier who created the OB PR in iplm. This is to collect the inventory information from the supplier for the BUY NHA & OB Part No. 
# 
# **It Includes:**    
# 1. **rpt_omat_buynha_suppliers_plants:** 
#     - Details suppliers and associated plants for Buy NHAs, supporting obsolescence management and inventory updates
#         - Load Type: Full refresh.
# 2. **rpt_omat_log_dtls:** 
#     - Logs OMAT system activities like job executions and email refresh requests for tracking and troubleshooting.
#         - Load Type: Incremental append.
# 

# In[1]:


from silketl import SqlDatabaseConnector, get_workspace_name, preload, load_data, postload,json,current_workspace_name


# In[29]:


# project_code = 'p2f_ome' 
# ##FTR meaning feauture workspace
# exec_env = 'FTR'
# job_id = '' 
# task_id = '' 
# env = ''


# #### Step-1 -- Imports

# In[2]:


from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, StringType


# ##### Helper Union Function

# In[3]:


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


# #### Step-2 -- Parameterize (Source/Target)

# In[4]:


# ---- Source tables ---
src_omat_dmd_tbl          = "lhs_glb.omat_test.rpt_omat_dmd_consumption_dtls"
src_inforec_tbl           = "wsf_silk_glb_da_dev.lhs_glb.alex.zt_inforec_data_sched"
src_iplm_tbl              = "lhs_glb.eng.stg_omat_iplm_problem_report_part"



paths = {
    "ecc": {
        "eord":      "wsf_silk_glb_da_dev.lhs_glb.ecc.eord",
        "lfa1":      "wsf_silk_glb_da_dev.lhs_glb.ecc.lfa1",
        "zpo_hstry": "wsf_silk_glb_da_dev.lhs_glb.ecc.zpo_hstry",
        "ekpo":      "wsf_silk_glb_da_dev.lhs_glb.ecc.ekpo",
    },
    "s4h": {
        "eord":      "wsf_silk_glb_da_dev.lhs_glb.s4h.eord",
        "lfa1":      "wsf_silk_glb_da_dev.lhs_glb.s4h.lfa1",
        "zpo_hstry": "wsf_silk_glb_da_dev.lhs_glb.s4h.zpo_hstry",
        "ekpo":      "wsf_silk_glb_da_dev.lhs_glb.s4h.ekpo",
    }
}


# ---- Target tables ----
tgt_suppliers_tbl         = "lhs_glb.eng.rpt_omat_buynha_suppliers_plants"
tgt_log_tbl               = "lhs_glb.eng.rpt_omat_log_dtls"

# ---- Control values ----
procurement_plants = ["1900", "2000", "3120", "1050", "1060", "1090"]
fallback_plants    = ["1000", "1010", "1020"]

valid_mrp          = "1"
valid_autet        = "1"
preferred_flag    = "X"

# supplier categories to exclude
excluded_supcats   = ["A05", "A08", "A15"]

# Program name for logging
program_name = "nb_omat_update_suppliers_byplant"



# #### Step-3 -- Reading Source tables
# load tables and rename columns that can clash in joins and cause ambiguity error.

# In[5]:


# ===============================
# OMAT demand / consumption
# ===============================
df_omat = (
    spark.read.table(src_omat_dmd_tbl)
    .select(
        F.col("nha"),
        F.col("open_po_qty").cast("int").alias("open_po_qty")
    )
    .groupBy("nha")
    .agg(
        F.max("open_po_qty").alias("open_po_qty")  # SAP semantics
    )
)

# ===============================
# INFOREC
# ===============================
df_inforec = (
    spark.read.table(src_inforec_tbl)
         .select(
             F.col("material").alias("material_cd"),
             F.col("plant").alias("plant_cd"),
             F.col("mrp").alias("mrp_cd"),
             F.col("supcode").alias("supcode_cd"),
             F.col("suppliername").alias("suppliername_cd"),
             F.col("supcat").alias("supcat_cd")
         )
         .filter(F.col("supcode_cd").isNotNull())
         .filter(F.length(F.trim(F.col("supcode_cd"))) > 0)  
         .filter(~F.col("supcat_cd").isin(excluded_supcats))
)

# ===============================
# EORD
# ===============================
df_eord = (
    read_union_fast("eord")
         .select(
             F.col("matnr").alias("matnr_cd"),
             F.col("werks").alias("werks_cd"),
             F.col("lifnr").alias("lifnr_cd"),
             F.col("autet").alias("autet_cd"),
             F.col("flifn").alias("flifn_cd")
         )
)

# ===============================
# LFA1 (Supplier master)
# ===============================
df_lfa1 = (
    read_union_fast("lfa1")
         .select(
             F.col("lifnr").alias("lifnr_cd"),
             F.col("name1").alias("name1_cd")
         )
)

# ===============================
# ZPO history (Open PO logic)
# ===============================
df_zpo = (
    read_union_fast("zpo_hstry")
         .select(
             F.col("matnr").alias("matnr_cd"),
             F.col("lifnr").alias("lifnr_cd"),
             F.col("ebeln").alias("ebeln_cd"),
             F.col("ebelp").alias("ebelp_cd"),
             F.col("due_dt").alias("due_dt_cd"),
             F.col("status").alias("status_cd"),
             F.col("loekz").alias("loekz_cd"),
             F.col("discrd").alias("discrd_cd"),
             F.col("aussl").alias("aussl_cd"),
             F.col("bsart").alias("bsart_cd"),
             F.col("elikz").alias("elikz_cd")
         )
)

# ===============================
# EKPO
# ===============================
df_ekpo = (
    read_union_fast("ekpo")
         .select(
             F.col("ebeln").alias("ebeln_cd"),
             F.col("ebelp").alias("ebelp_cd"),
             F.col("pstyp").alias("pstyp_cd")
         )
)

# ===============================
# IPLM OB PR
# ===============================
df_iplm = (
    spark.read.table(src_iplm_tbl)
         .select(
             F.col("part_name").alias("part_name_cd"),
             F.col("supplier_code").alias("supplier_code_cd"),
             F.upper(F.col("state")).alias("state_cd"),
             F.upper(F.col("disposition")).alias("disposition_cd"),
             F.upper(F.col("reason")).alias("reason_cd")
         )
)


# #### Step-4 — Build MRP-Preferred Supplier Matrix
# 
# - This builds the first and most important supplier matrix for every Buy-NHA part.
# It checks For each NHA, which supplier is officially preferred by SAP MRP in each manufacturing plant.
# - SAP stores this information in Info Records (INFOREC).
# It identifies all 571-series NHAs that drive the entire BOM demand & consumption logic.

# In[6]:


# Filter only MRP preferred rows
df_inforec_mrp = df_inforec.filter(F.col("mrp_cd") == valid_mrp)

# Safe padding (only when value exists)
def pad10(c):
    return F.when(
        F.length(F.trim(c)) > 0,
        F.lpad(c, 10, "0")
    )

# Create plant-specific INFOREC datasets
def plant_inforec(plant):
    return (
        df_inforec_mrp
        .filter(F.col("plant_cd") == plant)
        .select(
            F.col("material_cd").alias("nha"),
            F.col("supcode_cd").alias(f"raw_suppcd_{plant}"),   # raw SUPCODE (SAP check)
            pad10(F.col("supcode_cd")).alias(f"suppcd_{plant}"),
            F.col("suppliername_cd").alias(f"suppname_{plant}")
        )
    )

# Procurement plants
inf_1900 = plant_inforec("1900")
inf_2000 = plant_inforec("2000")
inf_3120 = plant_inforec("3120")
inf_1050 = plant_inforec("1050")
inf_1060 = plant_inforec("1060")
inf_1090 = plant_inforec("1090")

# Fallback plants
inf_1000 = plant_inforec("1000")
inf_1010 = plant_inforec("1010")
inf_1020 = plant_inforec("1020")

# DISTINCT NHA (SAP SELECT DISTINCT)
df_nha = df_omat.select("nha").distinct()

# Join all plant datasets
df_stage1 = (
    df_nha
    .join(inf_1900, "nha", "left")
    .join(inf_2000, "nha", "left")
    .join(inf_3120, "nha", "left")
    .join(inf_1050, "nha", "left")
    .join(inf_1060, "nha", "left")
    .join(inf_1090, "nha", "left")
    .join(inf_1000, "nha", "left")
    .join(inf_1010, "nha", "left")
    .join(inf_1020, "nha", "left")
)

# ------------------------------------------------
# SAP CASE-WHEN fallback logic (RAW SUPCODE based)
# ------------------------------------------------

df_stage1 = (
    df_stage1
    # 1900 ← 1020
    .withColumn(
        "suppcd_1900",
        F.when(F.length(F.trim("raw_suppcd_1900")) > 0, F.col("suppcd_1900"))
         .otherwise(F.col("suppcd_1020"))
    )
    .withColumn(
        "suppname_1900",
        F.when(F.length(F.trim("raw_suppcd_1900")) > 0, F.col("suppname_1900"))
         .otherwise(F.col("suppname_1020"))
    )

    # 2000 ← 1000
    .withColumn(
        "suppcd_2000",
        F.when(F.length(F.trim("raw_suppcd_2000")) > 0, F.col("suppcd_2000"))
         .otherwise(F.col("suppcd_1000"))
    )
    .withColumn(
        "suppname_2000",
        F.when(F.length(F.trim("raw_suppcd_2000")) > 0, F.col("suppname_2000"))
         .otherwise(F.col("suppname_1000"))
    )

    # 3120 ← 1010
    .withColumn(
        "suppcd_3120",
        F.when(F.length(F.trim("raw_suppcd_3120")) > 0, F.col("suppcd_3120"))
         .otherwise(F.col("suppcd_1010"))
    )
    .withColumn(
        "suppname_3120",
        F.when(F.length(F.trim("raw_suppcd_3120")) > 0, F.col("suppname_3120"))
         .otherwise(F.col("suppname_1010"))
    )
)

# Final projection (drop raw columns)
df_stage1 = df_stage1.select(
    "nha",
    "suppcd_1900", "suppname_1900",
    "suppcd_2000", "suppname_2000",
    "suppcd_3120", "suppname_3120",
    "suppcd_1050", "suppname_1050",
    "suppcd_1060", "suppname_1060",
    "suppcd_1090", "suppname_1090"
)

df_stage1.createOrReplaceTempView("stage1_mrp_suppliers")


# #### Step-5 -- Fill Missing Suppliers from SAP EORD (Purchasing Source List)
# - Even after checking Info Records (MRP-preferred suppliers), many parts still have no supplier.
# SAP allows this because sometimes the official supplier is maintained in EORD (the Purchasing Source List) instead of Info Records.
# - It Tells If MRP did not define a supplier, who is the officially approved supplier for this part in SAP.

# In[8]:


# 1. Filter valid fixed EORD suppliers (AUTET = '1' AND FLIFN = 'X')
df_eord_valid = (
    df_eord
    .filter(
        (F.col("autet_cd") == valid_autet) &
        (F.col("flifn_cd") == preferred_flag)
    )
)

# 2. Join supplier names
df_eord_named = df_eord_valid.join(df_lfa1, "lifnr_cd", "left")


# 3. Extract EORD per plant
def eord_by_plant(plant):
    return (
        df_eord_named
        .filter(F.col("werks_cd") == plant)
        .select(
            F.col("matnr_cd").alias("nha"),
            pad10(F.col("lifnr_cd")).alias(f"eord_suppcd_{plant}"),
            F.col("name1_cd").alias(f"eord_suppname_{plant}")
        )
    )

eord_1900 = eord_by_plant("1900")
eord_2000 = eord_by_plant("2000")
eord_3120 = eord_by_plant("3120")
eord_1050 = eord_by_plant("1050")
eord_1060 = eord_by_plant("1060")
eord_1090 = eord_by_plant("1090")

# fallback plants
eord_1000 = eord_by_plant("1000")
eord_1010 = eord_by_plant("1010")
eord_1020 = eord_by_plant("1020")

# 5. Join EORD to Stage-1
df_stage2 = (
    spark.table("stage1_mrp_suppliers")
    .join(eord_1900, "nha", "left")
    .join(eord_2000, "nha", "left")
    .join(eord_3120, "nha", "left")
    .join(eord_1050, "nha", "left")
    .join(eord_1060, "nha", "left")
    .join(eord_1090, "nha", "left")
    .join(eord_1000, "nha", "left")
    .join(eord_1010, "nha", "left")
    .join(eord_1020, "nha", "left")
)

# 6. SAP WHERE condition:
# Apply EORD ONLY when ALL supplier columns are empty
no_supplier_cond = (
    (F.length(F.trim("suppcd_1900")) == 0) &
    (F.length(F.trim("suppcd_2000")) == 0) &
    (F.length(F.trim("suppcd_3120")) == 0) &
    (F.length(F.trim("suppcd_1050")) == 0) &
    (F.length(F.trim("suppcd_1060")) == 0) &
    (F.length(F.trim("suppcd_1090")) == 0)
)

# 7. SAP CASE-WHEN logic (not COALESCE)
df_stage2 = (
    df_stage2
    .withColumn(
        "suppcd_1900",
        F.when(
            no_supplier_cond,
            F.when(F.length(F.trim("eord_suppcd_1900")) > 0, F.col("eord_suppcd_1900"))
             .otherwise(F.col("eord_suppcd_1020"))
        ).otherwise(F.col("suppcd_1900"))
    )
    .withColumn(
        "suppname_1900",
        F.when(
            no_supplier_cond,
            F.when(F.length(F.trim("eord_suppcd_1900")) > 0, F.col("eord_suppname_1900"))
             .otherwise(F.col("eord_suppname_1020"))
        ).otherwise(F.col("suppname_1900"))
    )
    .withColumn(
        "suppcd_2000",
        F.when(
            no_supplier_cond,
            F.when(F.length(F.trim("eord_suppcd_2000")) > 0, F.col("eord_suppcd_2000"))
             .otherwise(F.col("eord_suppcd_1000"))
        ).otherwise(F.col("suppcd_2000"))
    )
    .withColumn(
        "suppname_2000",
        F.when(
            no_supplier_cond,
            F.when(F.length(F.trim("eord_suppcd_2000")) > 0, F.col("eord_suppname_2000"))
             .otherwise(F.col("eord_suppname_1000"))
        ).otherwise(F.col("suppname_2000"))
    )
    .withColumn(
        "suppcd_3120",
        F.when(
            no_supplier_cond,
            F.when(F.length(F.trim("eord_suppcd_3120")) > 0, F.col("eord_suppcd_3120"))
             .otherwise(F.col("eord_suppcd_1010"))
        ).otherwise(F.col("suppcd_3120"))
    )
    .withColumn(
        "suppname_3120",
        F.when(
            no_supplier_cond,
            F.when(F.length(F.trim("eord_suppcd_3120")) > 0, F.col("eord_suppname_3120"))
             .otherwise(F.col("eord_suppname_1010"))
        ).otherwise(F.col("suppname_3120"))
    )
    .withColumn(
        "suppcd_1050",
        F.when(no_supplier_cond, F.col("eord_suppcd_1050"))
         .otherwise(F.col("suppcd_1050"))
    )
    .withColumn(
        "suppname_1050",
        F.when(no_supplier_cond, F.col("eord_suppname_1050"))
         .otherwise(F.col("suppname_1050"))
    )
    .withColumn(
        "suppcd_1060",
        F.when(no_supplier_cond, F.col("eord_suppcd_1060"))
         .otherwise(F.col("suppcd_1060"))
    )
    .withColumn(
        "suppname_1060",
        F.when(no_supplier_cond, F.col("eord_suppname_1060"))
         .otherwise(F.col("suppname_1060"))
    )
    .withColumn(
        "suppcd_1090",
        F.when(no_supplier_cond, F.col("eord_suppcd_1090"))
         .otherwise(F.col("suppcd_1090"))
    )
    .withColumn(
        "suppname_1090",
        F.when(no_supplier_cond, F.col("eord_suppname_1090"))
         .otherwise(F.col("suppname_1090"))
    )
)

# 8. Final projection
df_stage2 = df_stage2.select(
    "nha",
    "suppcd_1900","suppname_1900",
    "suppcd_2000","suppname_2000",
    "suppcd_3120","suppname_3120",
    "suppcd_1050","suppname_1050",
    "suppcd_1060","suppname_1060",
    "suppcd_1090","suppname_1090"
)

df_stage2.createOrReplaceTempView("stage2_eord_filled")


# #### Step-6 -- Remove Non-Preferred Suppliers when Duplicate NHAs Exist
# 
# - After combining Info Records + EORD, some NHAs may still appear with multiple supplier rows
# (because SAP allows more than one valid source per part).
# However, OMAT must keep only the “best” supplier per NHA per plant.
# - It checks if multiple suppliers exist for the same NHA, which one is NOT the official fixed vendor.

# In[10]:


df_base = spark.table("stage2_eord_filled")

# 1. SAP ROW_NUMBER logic (not simple count)
w = Window.partitionBy("nha").orderBy("nha")

df_with_rn = df_base.withColumn("rn", F.row_number().over(w))

df_dups = (
    df_with_rn
    .filter(F.col("rn") > 1)
    .select("nha")
    .distinct()
)

# 2. Prepare EORD flags
df_eord_flag = df_eord.select(
    F.col("matnr_cd").alias("eord_nha"),
    F.col("werks_cd").alias("eord_plant"),
    F.col("lifnr_cd").alias("eord_lifnr"),
    F.col("flifn_cd").alias("eord_flifn"),
    F.col("autet_cd").alias("eord_autet")
)

# 3. SAP delete logic per plant
def dedup_plant(df, plant):
    return (
        df
        .join(df_dups, "nha", "left")
        .join(
            df_eord_flag,
            (df.nha == df_eord_flag.eord_nha) &
            (df_eord_flag.eord_plant == plant) &
            (df_eord_flag.eord_lifnr == df[f"suppcd_{plant}"]),
            "left"
        )
        .filter(
            ~(
                F.col("nha").isNotNull() &           # duplicate NHA
                (F.col("eord_flifn") != preferred_flag) &
                F.col("eord_autet").isNotNull()      # forces SAP join semantics
            )
        )
        .drop("rn","eord_nha","eord_plant","eord_lifnr","eord_flifn","eord_autet")
    )

df_stage3 = df_with_rn.drop("rn")
for p in procurement_plants:
    df_stage3 = dedup_plant(df_stage3, p)

df_stage3.createOrReplaceTempView("stage3_deduped")


# #### Step-7 — Base-Part Supplier Inheritance for R / S / C / X / D Parts
# - The variant parts (R, S, C, X, D) often do not have their own supplier but the base part DOES
# So “If a variant part has no supplier, inherit the supplier of the base part.”
# - SAP does this using two different pattern rules:
# Replace R/S/C/X/D with -  (3-6R3 → 3-6-3)
# Remove R/S/C/X/D completely (R2-6-2 → 2-6-2)
# We reproduce both patterns.

# In[16]:


# ===============================
# Base table (ONE copy only)
# ===============================
base = spark.table("stage3_deduped")

# SAP WHERE condition: all suppliers missing
no_supplier = (
    (F.length(F.trim(F.col("suppcd_1900"))) == 0) &
    (F.length(F.trim(F.col("suppcd_2000"))) == 0) &
    (F.length(F.trim(F.col("suppcd_3120"))) == 0) &
    (F.length(F.trim(F.col("suppcd_1050"))) == 0) &
    (F.length(F.trim(F.col("suppcd_1060"))) == 0) &
    (F.length(F.trim(F.col("suppcd_1090"))) == 0)
)

# ===============================
# Base lookup (correlated subquery equivalent)
# ===============================
lookup = base.select(
    F.col("nha").alias("base_nha"),
    "suppcd_1900","suppname_1900",
    "suppcd_2000","suppname_2000",
    "suppcd_3120","suppname_3120",
    "suppcd_1050","suppname_1050",
    "suppcd_1060","suppname_1060",
    "suppcd_1090","suppname_1090"
)

# ===============================
# Pattern-1: R,S,C,X,D → '-'
# ===============================
p1 = (
    base
    .select("nha")
    .withColumn("base_nha", F.regexp_replace("nha", "[RSCXD]", "-"))
    .join(
        lookup.selectExpr(
            "base_nha",
            "suppcd_1900 as p1_suppcd_1900", "suppname_1900 as p1_suppname_1900",
            "suppcd_2000 as p1_suppcd_2000", "suppname_2000 as p1_suppname_2000",
            "suppcd_3120 as p1_suppcd_3120", "suppname_3120 as p1_suppname_3120",
            "suppcd_1050 as p1_suppcd_1050", "suppname_1050 as p1_suppname_1050",
            "suppcd_1060 as p1_suppcd_1060", "suppname_1060 as p1_suppname_1060",
            "suppcd_1090 as p1_suppcd_1090", "suppname_1090 as p1_suppname_1090"
        ),
        "base_nha",
        "left"
    )
    .drop("base_nha")
)

# ===============================
# Pattern-2: remove R,S,C,X,D
# ===============================
p2 = (
    base
    .select("nha")
    .withColumn("base_nha", F.regexp_replace("nha", "[RSCXD]", ""))
    .join(
        lookup.selectExpr(
            "base_nha",
            "suppcd_1900 as p2_suppcd_1900", "suppname_1900 as p2_suppname_1900",
            "suppcd_2000 as p2_suppcd_2000", "suppname_2000 as p2_suppname_2000",
            "suppcd_3120 as p2_suppcd_3120", "suppname_3120 as p2_suppname_3120",
            "suppcd_1050 as p2_suppcd_1050", "suppname_1050 as p2_suppname_1050",
            "suppcd_1060 as p2_suppcd_1060", "suppname_1060 as p2_suppname_1060",
            "suppcd_1090 as p2_suppcd_1090", "suppname_1090 as p2_suppname_1090"
        ),
        "base_nha",
        "left"
    )
    .drop("base_nha")
)

# ===============================
# SAP CASE logic
# ===============================
def sap_update(main, p1c, p2c):
    return (
        F.when(
            no_supplier,
            F.when(F.length(F.trim(F.col(p1c))) > 0, F.col(p1c))
             .otherwise(F.col(p2c))
        ).otherwise(F.col(main))
    )

# ===============================
# Final UPDATE-style result
# ===============================
df_stage4 = (
    base
    .join(p1, "nha", "left")
    .join(p2, "nha", "left")
    .select(
        "nha",
        sap_update("suppcd_1900","p1_suppcd_1900","p2_suppcd_1900").alias("suppcd_1900"),
        sap_update("suppname_1900","p1_suppname_1900","p2_suppname_1900").alias("suppname_1900"),
        sap_update("suppcd_2000","p1_suppcd_2000","p2_suppcd_2000").alias("suppcd_2000"),
        sap_update("suppname_2000","p1_suppname_2000","p2_suppname_2000").alias("suppname_2000"),
        sap_update("suppcd_3120","p1_suppcd_3120","p2_suppcd_3120").alias("suppcd_3120"),
        sap_update("suppname_3120","p1_suppname_3120","p2_suppname_3120").alias("suppname_3120"),
        sap_update("suppcd_1050","p1_suppcd_1050","p2_suppcd_1050").alias("suppcd_1050"),
        sap_update("suppname_1050","p1_suppname_1050","p2_suppname_1050").alias("suppname_1050"),
        sap_update("suppcd_1060","p1_suppcd_1060","p2_suppcd_1060").alias("suppcd_1060"),
        sap_update("suppname_1060","p1_suppname_1060","p2_suppname_1060").alias("suppname_1060"),
        sap_update("suppcd_1090","p1_suppcd_1090","p2_suppcd_1090").alias("suppcd_1090"),
        sap_update("suppname_1090","p1_suppname_1090","p2_suppname_1090").alias("suppname_1090")
    )
)

df_stage4.createOrReplaceTempView("stage4_basepart_filled")


# #### Step-8 — Open Purchase Order (PO) Supplier Identification
# - Even if SAP master data does not show a supplier, open purchase orders tell us who we are actively buying from.
# - It checks For this NHA, which suppliers currently have open purchase orders.
# - These suppliers are extremely important because:
#     - They are currently delivering
#     - They can provide inventory & lead-time
#     - OMAT uses them when master data is missing

# In[38]:


# A = OMAT demand (OPEN_PO_QTY > 0)
df_omat_po = (
    df_omat
    .filter(F.col("open_po_qty") > 0)
    .select("nha")
)

df_po_base = (
    df_omat_po
    .join(df_zpo, df_zpo.matnr_cd == df_omat_po.nha, "inner")
    .join(
        df_ekpo,
        (df_zpo.ebeln_cd == df_ekpo.ebeln_cd) &
        (df_zpo.ebelp_cd == df_ekpo.ebelp_cd),
        "inner"
    )
    .join(df_lfa1, df_zpo.lifnr_cd == df_lfa1.lifnr_cd, "inner")

    # ---- SAP WHERE conditions ----
    .filter(~F.substring(df_zpo.due_dt_cd, -5, 5).isin("04-01", "12-31"))
    .filter(df_zpo.status_cd != "INACT")
    .filter(F.length(F.trim(df_zpo.matnr_cd)) > 0)
    .filter(F.trim(df_zpo.loekz_cd) == "")
    .filter(df_zpo.discrd_cd != "X")
    .filter(df_zpo.aussl_cd != "U3")
    .filter(df_zpo.bsart_cd != "UB")
    .filter(df_zpo.elikz_cd != "X")
    .filter(~df_ekpo.pstyp_cd.isin("7", "9"))
    .filter(F.length(df_zpo.lifnr_cd) == 10)

    # SAP inner SELECT DISTINCT
    .select(
        df_omat_po.nha.alias("nha"),
        df_zpo.lifnr_cd.alias("lifnr"),
        df_lfa1.name1_cd.alias("name1")
    )
    .distinct()
)

df_posupp = (
    df_po_base
    .groupBy("nha")
    .agg(
        F.concat_ws(",", F.collect_set("lifnr")).alias("posuppcode"),
        F.concat_ws(",", F.collect_set("name1")).alias("posuppname")
    )
    .orderBy("nha")
)

df_posupp.createOrReplaceTempView("posupp")


# #### Step-9 — Attach Open-PO Suppliers (PO1 / PO2) to Each NHA
# - At this point we know:
# Who we prefers (MRP + EORD), 
# Who we is actually buying from (Open POs)
# - OMAT must use Open PO suppliers as a fallback when:
# Master data is missing Or the part is newly sourced.
# - It checks Who are the first two active suppliers we are currently purchasing this NHA from.
# SAP logic: First supplier in PO list → PO1, Second supplier in PO list → PO2

# In[39]:


df_stage5 = (
    spark.table("stage4_basepart_filled")
    .join(spark.table("posupp"), "nha", "left")

    # ---------- PO SUPPLIER 1 ----------
    .withColumn(
        "suppcd_po1",
        F.when(
            F.instr(F.col("posuppcode"), ",") > 0,
            F.substring_index("posuppcode", ",", 1)
        ).otherwise(
            F.substring("posuppcode", 1, 10)
        )
    )

    # ---------- PO SUPPLIER 2 ----------
    .withColumn(
        "suppcd_po2",
        F.when(
            (F.instr(F.col("posuppcode"), ",") > 0) &
            (F.length(F.col("posuppcode")) <= 21),
            F.substring_index("posuppcode", ",", -1)
        ).otherwise(
            F.substring("posuppcode", 12, 10)
        )
    )

    # ---------- PO SUPPLIER 1 NAME ----------
    .withColumn(
        "suppname_po1",
        F.when(
            F.instr(F.col("posuppname"), ",") > 0,
            F.substring_index("posuppname", ",", 1)
        ).otherwise(
            F.col("posuppname")
        )
    )

    # ---------- PO SUPPLIER 2 NAME ----------
    .withColumn(
        "suppname_po2",
        F.when(
            (F.instr(F.col("posuppname"), ",") > 0) &
            (F.length(F.col("posuppname")) <= 21),
            F.substring_index("posuppname", ",", -1)
        )
    )
)

df_stage5.createOrReplaceTempView("stage5_with_po")


# #### Step-10 — OB PR Supplier (Supplier Who Raised the Obsolescence Request)
# - Some parts become obsolete because a supplier officially reported the problem in IPLM (Problem Reports).
# - That supplier is directly involved Usually holds last-time-buy stock Is often the best contact for OMAT.
# - It Checks Which supplier raised the Obsolete Component PR for this NHA we uses this supplier as a high-priority fallback.

# In[41]:


df_pr = (
    df_iplm
    .filter(F.col("part_name_cd") != "NA")          
    .filter(F.col("state_cd") != "CLOSED")
    .filter(F.col("reason_cd") == "OBSOLETE COMPONENT")
    .filter(
        (F.col("state_cd").isin("CONFIRMED", "IN REVIEW", "IN WORK")) |
        (F.col("disposition_cd").isin("CONFIRMED", "DEFER"))
    )
    .withColumn(
        "suppcd_pr",
        F.when(
            F.length(F.trim("supplier_code_cd")) > 0,
            F.lpad("supplier_code_cd", 10, "0")
        )
    )
    .join(df_lfa1, F.col("suppcd_pr") == df_lfa1.lifnr_cd, "left")
    .select(
        F.col("part_name_cd"),
        F.col("suppcd_pr"),
        F.col("name1_cd").alias("suppname_pr")
    )
)

df_stage6 = (
    spark.table("stage5_with_po")
    .join(df_pr, df_pr.part_name_cd == F.col("nha"), "left")
    .drop("part_name_cd")
)

df_stage6.createOrReplaceTempView("stage6_with_pr")


# #### Step-11 — Final OMAT Supplier & Plant Selection
# - Now OMAT has multiple possible suppliers for every NHA:
#     - From MRP / Info Records
#     - From EORD
#     - From Base-part inheritance
#     - From Open POs
#     - From Obsolescence PR
# - OMAT must choose one authoritative supplier to:
#     - Contact for inventory
#     - Track exposure
#     - Drive risk decisions
# - Buisness defines a strict priority order (Supplier Priority) :
#     - Plant 2000 MRP
#     - Plant 1900 MRP
#     - Plant 3120 MRP
#     - Plant 1050 MRP
#     - Plant 1060 MRP
#     - Plant 1090 MRP
#     - Open PO Supplier (PO1)
#     - Second PO Supplier (PO2)
#     - OB PR Supplier.

# In[42]:


df = spark.table("stage6_with_pr")

def has_val(c):
    return F.length(F.trim(F.col(c))) > 0

df_final = (
    df

    # -------------------------------
    # Selected Supplier Code
    # -------------------------------
    .withColumn(
        "omat_selected_suppliercd",
        F.when(has_val("suppcd_2000"), F.col("suppcd_2000"))
         .when(has_val("suppcd_1900"), F.col("suppcd_1900"))
         .when(has_val("suppcd_3120"), F.col("suppcd_3120"))
         .when(has_val("suppcd_1050"), F.col("suppcd_1050"))
         .when(has_val("suppcd_1060"), F.col("suppcd_1060"))
         .when(has_val("suppcd_1090"), F.col("suppcd_1090"))
         .when(F.length(F.trim("suppcd_po1")) == 10, F.col("suppcd_po1"))
         .when(F.length(F.trim("suppcd_po2")) == 10, F.col("suppcd_po2"))
         .when(has_val("suppcd_pr"), F.col("suppcd_pr"))
    )

    # -------------------------------
    # Selected Supplier Name
    # -------------------------------
    .withColumn(
        "omat_selected_supplier",
        F.when(has_val("suppcd_2000"), F.col("suppname_2000"))
         .when(has_val("suppcd_1900"), F.col("suppname_1900"))
         .when(has_val("suppcd_3120"), F.col("suppname_3120"))
         .when(has_val("suppcd_1050"), F.col("suppname_1050"))
         .when(has_val("suppcd_1060"), F.col("suppname_1060"))
         .when(has_val("suppcd_1090"), F.col("suppname_1090"))
         .when(F.length(F.trim("suppcd_po1")) == 10, F.col("suppname_po1"))
         .when(F.length(F.trim("suppcd_po2")) == 10, F.col("suppname_po2"))
         .when(has_val("suppcd_pr"), F.col("suppname_pr"))
    )

    # -------------------------------
    # Selected Plant
    # -------------------------------
    .withColumn(
        "omat_selected_plant",
        F.when(has_val("suppcd_2000"), F.lit("2000"))
         .when(has_val("suppcd_1900"), F.lit("1900"))
         .when(has_val("suppcd_3120"), F.lit("3120"))
         .when(has_val("suppcd_1050"), F.lit("1050"))
         .when(has_val("suppcd_1060"), F.lit("1060"))
         .when(has_val("suppcd_1090"), F.lit("1090"))
         .when(F.length(F.trim("suppcd_po1")) == 10, F.lit("2000"))
         .when(has_val("suppcd_pr"), F.lit("2000"))
    )
)

df_final.createOrReplaceTempView("stage7_final_suppliers")


# #### Step-12 — Write to OMAT Tables + Log Execution
# - This is the commit step:
# Everything we calculated in memory is now 
# Written into the OMAT reporting table, 
# Audited in the OMAT log table

# In[21]:


# 1. Final dataset

df_final = spark.table("stage7_final_suppliers")


# 2. Replace target table (full refresh)

(
    df_final
    .select(
        "nha",
        "omat_selected_plant",
        "omat_selected_supplier",
        "omat_selected_suppliercd",
        "suppcd_1050",
        "suppcd_1060",
        "suppcd_1090",
        "suppcd_1900",
        "suppcd_2000",
        "suppcd_3120",
        "suppcd_po1",
        "suppcd_po2",
        "suppcd_pr",
        "suppname_1050",
        "suppname_1060",
        "suppname_1090",
        "suppname_1900",
        "suppname_2000",
        "suppname_3120",
        "suppname_po1",
        "suppname_po2",
        "suppname_pr"
    )
    .write
    .mode("overwrite")
    .format("delta")
    .option("overwriteSchema", "true")
    .saveAsTable(tgt_suppliers_tbl)
)



# 3. Insert execution log

log_df = spark.createDataFrame(
    [
        (
            None,   # executed_on
            0,      # obprtno_cnt
            0,      # obprtno_nha_cnt
            0,      # obpr_cnt
            program_name
        )
    ],
    schema=StructType([
        StructField("executed_on", TimestampType(), True),
        StructField("obprtno_cnt", IntegerType(), True),
        StructField("obprtno_nha_cnt", IntegerType(), True),
        StructField("obpr_cnt", IntegerType(), True),
        StructField("program_name", StringType(), True)
    ])
).withColumn("executed_on", F.current_timestamp())

(
    log_df
    .write
    .mode("append")
    .format("delta")
    .saveAsTable(tgt_log_tbl)
)

