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


# In[18]:


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
from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, StringType


# #### Step-2 -- Parameterize (Source/Target)

# In[4]:


# ---- Source tables ---
src_omat_dmd_tbl          = "lhs_glb.eng_test.rpt_omat_dmd_consumption_dtls"
src_inforec_tbl           = "wsf_silk_glb_da_dev.lhs_glb.alex.zt_inforec_data_sched"
src_eord_tbl              = "wsf_silk_glb_da_dev.lhs_glb.ecc.eord"
src_lfa1_tbl              = "wsf_silk_glb_da_dev.lhs_glb.ecc.lfa1"
src_zpo_hist_tbl          = "wsf_silk_glb_da_dev.lhs_glb.ecc.zpo_hstry"
src_ekpo_tbl              = "wsf_silk_glb_da_dev.lhs_glb.ecc.ekpo"
src_iplm_tbl              = "lhs_glb.eng_test.stg_omat_iplm_problem_report_part"

# ---- Target tables ----
tgt_suppliers_tbl         = "lhs_glb.eng_test.rpt_omat_buynha_suppliers_plants"
tgt_log_tbl               = "lhs_glb.eng_test.rpt_omat_log_dtls"

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

# In[21]:


# ===============================
# OMAT demand / consumption
# ===============================
df_omat = (
    spark.read.table(src_omat_dmd_tbl)
         .select(
             F.col("nha").alias("nha"),
             F.col("open_po_qty").cast("int").alias("open_po_qty")
         )
         .dropDuplicates(["nha"])
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
         .filter(~F.col("supcat_cd").isin(excluded_supcats))
)

# ===============================
# EORD
# ===============================
df_eord = (
    spark.read.table(src_eord_tbl)
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
    spark.read.table(src_lfa1_tbl)
         .select(
             F.col("lifnr").alias("lifnr_cd"),
             F.col("name1").alias("name1_cd")
         )
)

# ===============================
# ZPO history (Open PO logic)
# ===============================
df_zpo = (
    spark.read.table(src_zpo_hist_tbl)
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
    spark.read.table(src_ekpo_tbl)
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

# In[24]:


# filter only MRP preferred rows
df_inforec_mrp = df_inforec.filter(F.col("mrp_cd") == valid_mrp)

# helper to pad supplier codes to 10 chars
def pad10(col):
    return F.lpad(col, 10, "0")

# create plant specific filtered datasets
def plant_inforec(plant):
    return (
        df_inforec_mrp
        .filter(F.col("plant_cd") == plant)
        .select(
            F.col("material_cd").alias("nha"),
            pad10(F.col("supcode_cd")).alias(f"suppcd_{plant}"),
            F.col("suppliername_cd").alias(f"suppname_{plant}")
        )
    )

inf_1900 = plant_inforec("1900")
inf_2000 = plant_inforec("2000")
inf_3120 = plant_inforec("3120")
inf_1050 = plant_inforec("1050")
inf_1060 = plant_inforec("1060")
inf_1090 = plant_inforec("1090")

# fallback plants
inf_1000 = plant_inforec("1000")
inf_1010 = plant_inforec("1010")
inf_1020 = plant_inforec("1020")

# join OMAT NHA with all plants
df_nha = df_omat.select("nha")

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

# apply SAP fallback logic
df_stage1 = (
    df_stage1
    .withColumn(
        "suppcd_1900",
        F.coalesce(F.col("suppcd_1900"), F.col("suppcd_1020"))
    )
    .withColumn(
        "suppname_1900",
        F.coalesce(F.col("suppname_1900"), F.col("suppname_1020"))
    )
    .withColumn(
        "suppcd_2000",
        F.coalesce(F.col("suppcd_2000"), F.col("suppcd_1000"))
    )
    .withColumn(
        "suppname_2000",
        F.coalesce(F.col("suppname_2000"), F.col("suppname_1000"))
    )
    .withColumn(
        "suppcd_3120",
        F.coalesce(F.col("suppcd_3120"), F.col("suppcd_1010"))
    )
    .withColumn(
        "suppname_3120",
        F.coalesce(F.col("suppname_3120"), F.col("suppname_1010"))
    )
)

# keep only required columns
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

# In[23]:


# filter valid fixed EORD suppliers
df_eord_valid = (
    df_eord
    .filter((F.col("autet_cd") == valid_autet) & (F.col("flifn_cd") == preferred_flag))
)

# join supplier names
df_eord_named = (
    df_eord_valid
    .join(df_lfa1, "lifnr_cd", "left")
)

# helper to extract EORD supplier per plant
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

# join to stage1
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

# apply fallback and fill missing MRP values from EORD
df_stage2 = (
    df_stage2
    .withColumn("suppcd_1900", F.coalesce("suppcd_1900", "eord_suppcd_1900", "eord_suppcd_1020"))
    .withColumn("suppname_1900", F.coalesce("suppname_1900", "eord_suppname_1900", "eord_suppname_1020"))
    .withColumn("suppcd_2000", F.coalesce("suppcd_2000", "eord_suppcd_2000", "eord_suppcd_1000"))
    .withColumn("suppname_2000", F.coalesce("suppname_2000", "eord_suppname_2000", "eord_suppname_1000"))
    .withColumn("suppcd_3120", F.coalesce("suppcd_3120", "eord_suppcd_3120", "eord_suppcd_1010"))
    .withColumn("suppname_3120", F.coalesce("suppname_3120", "eord_suppname_3120", "eord_suppname_1010"))
    .withColumn("suppcd_1050", F.coalesce("suppcd_1050", "eord_suppcd_1050"))
    .withColumn("suppname_1050", F.coalesce("suppname_1050", "eord_suppname_1050"))
    .withColumn("suppcd_1060", F.coalesce("suppcd_1060", "eord_suppcd_1060"))
    .withColumn("suppname_1060", F.coalesce("suppname_1060", "eord_suppname_1060"))
    .withColumn("suppcd_1090", F.coalesce("suppcd_1090", "eord_suppcd_1090"))
    .withColumn("suppname_1090", F.coalesce("suppname_1090", "eord_suppname_1090"))
)

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


# #### Step-6 -- Cell 6 — Remove Non-Preferred Suppliers when Duplicate NHAs Exist
# 
# - After combining Info Records + EORD, some NHAs may still appear with multiple supplier rows
# (because SAP allows more than one valid source per part).
# However, OMAT must keep only the “best” supplier per NHA per plant.
# - It checks if multiple suppliers exist for the same NHA, which one is NOT the official fixed vendor.

# In[ ]:


# ===============================
# Identify NHAs that have duplicates
# ===============================
df_dups = (
    spark.table("stage2_eord_filled")
    .groupBy("nha")
    .count()
    .filter(F.col("count") > 1)
    .select(F.col("nha").alias("dup_nha"))
)

# ===============================
# Prepare EORD flags
# ===============================
df_eord_flag = (
    df_eord
    .select(
        F.col("matnr_cd").alias("eord_nha"),
        F.col("werks_cd").alias("eord_plant"),
        F.col("lifnr_cd").alias("eord_lifnr"),
        F.col("flifn_cd").alias("eord_flifn"),
        F.col("autet_cd").alias("eord_autet")
    )
)

base = spark.table("stage2_eord_filled")

# ===============================
# Apply SAP delete logic per plant
# ===============================
def dedup_plant(df, plant):
    return (
        df
        .join(df_dups, df.nha == df_dups.dup_nha, "left")
        .join(
            df_eord_flag,
            (df.nha == df_eord_flag.eord_nha) &
            (df_eord_flag.eord_plant == plant) &
            (df_eord_flag.eord_lifnr == df[f"suppcd_{plant}"]),
            "left"
        )
        .filter(
            ~(
                F.col("dup_nha").isNotNull() &
                F.col("eord_autet").isNotNull() &
                (F.col("eord_flifn") != preferred_flag)
            )
        )
        .drop("dup_nha","eord_nha","eord_plant","eord_lifnr","eord_flifn","eord_autet")
    )

df_stage3 = base
for p in procurement_plants:
    df_stage3 = dedup_plant(df_stage3, p)

df_stage3.createOrReplaceTempView("stage3_deduped")


# #### Cell 7 — Base-Part Supplier Inheritance for R / S / C / X / D Parts
# - The variant parts (R, S, C, X, D) often do not have their own supplier but the base part DOES
# So “If a variant part has no supplier, inherit the supplier of the base part.”
# - SAP does this using two different pattern rules:
# Replace R/S/C/X/D with -  (3-6R3 → 3-6-3)
# Remove R/S/C/X/D completely (R2-6-2 → 2-6-2)
# We reproduce both patterns.

# In[ ]:


df_base = spark.table("stage3_deduped")

# -------------------------------
# Create base lookup with renamed columns
# -------------------------------
df_lookup = df_base.select(
    F.col("nha").alias("base_nha"),
    F.col("suppcd_1900").alias("base_suppcd_1900"),
    F.col("suppname_1900").alias("base_suppname_1900"),
    F.col("suppcd_2000").alias("base_suppcd_2000"),
    F.col("suppname_2000").alias("base_suppname_2000"),
    F.col("suppcd_3120").alias("base_suppcd_3120"),
    F.col("suppname_3120").alias("base_suppname_3120"),
    F.col("suppcd_1050").alias("base_suppcd_1050"),
    F.col("suppname_1050").alias("base_suppname_1050"),
    F.col("suppcd_1060").alias("base_suppcd_1060"),
    F.col("suppname_1060").alias("base_suppname_1060"),
    F.col("suppcd_1090").alias("base_suppcd_1090"),
    F.col("suppname_1090").alias("base_suppname_1090")
)

# -------------------------------
# Pattern 1: replace R,S,C,X,D with "-"
# -------------------------------
df_p1 = (
    df_base
    .withColumn("base_nha", F.regexp_replace("nha", "[RSCXD]", "-"))
    .join(df_lookup, "base_nha", "left")
)

# -------------------------------
# Pattern 2: remove R,S,C,X,D
# -------------------------------
df_p2 = (
    df_base
    .withColumn("base_nha", F.regexp_replace("nha", "[RSCXD]", ""))
    .join(df_lookup, "base_nha", "left")
)

# -------------------------------
# Fill missing suppliers from base part
# -------------------------------
def fill_from_base(df):
    return (
        df
        .withColumn("suppcd_1900", F.coalesce("suppcd_1900", "base_suppcd_1900"))
        .withColumn("suppname_1900", F.coalesce("suppname_1900", "base_suppname_1900"))
        .withColumn("suppcd_2000", F.coalesce("suppcd_2000", "base_suppcd_2000"))
        .withColumn("suppname_2000", F.coalesce("suppname_2000", "base_suppname_2000"))
        .withColumn("suppcd_3120", F.coalesce("suppcd_3120", "base_suppcd_3120"))
        .withColumn("suppname_3120", F.coalesce("suppname_3120", "base_suppname_3120"))
        .withColumn("suppcd_1050", F.coalesce("suppcd_1050", "base_suppcd_1050"))
        .withColumn("suppname_1050", F.coalesce("suppname_1050", "base_suppname_1050"))
        .withColumn("suppcd_1060", F.coalesce("suppcd_1060", "base_suppcd_1060"))
        .withColumn("suppname_1060", F.coalesce("suppname_1060", "base_suppname_1060"))
        .withColumn("suppcd_1090", F.coalesce("suppcd_1090", "base_suppcd_1090"))
        .withColumn("suppname_1090", F.coalesce("suppname_1090", "base_suppname_1090"))
    )

df_stage4 = fill_from_base(df_p1)
df_stage4 = fill_from_base(df_stage4.join(df_p2.select("nha"), "nha", "left"))

df_stage4 = df_stage4.select(
    "nha",
    "suppcd_1900","suppname_1900",
    "suppcd_2000","suppname_2000",
    "suppcd_3120","suppname_3120",
    "suppcd_1050","suppname_1050",
    "suppcd_1060","suppname_1060",
    "suppcd_1090","suppname_1090"
)

df_stage4.createOrReplaceTempView("stage4_basepart_filled")


# #### Cell 8 — Open Purchase Order (PO) Supplier Identification
# - Even if SAP master data does not show a supplier, open purchase orders tell us who we are actively buying from.
# - It checks For this NHA, which suppliers currently have open purchase orders.
# - These suppliers are extremely important because:
#     - They are currently delivering
#     - They can provide inventory & lead-time
#     - OMAT uses them when master data is missing

# In[ ]:


df_po_valid = (
    df_zpo
    .join(df_ekpo, ["ebeln_cd","ebelp_cd"])
    .filter(F.col("status_cd") != "INACT")
    .filter(F.col("loekz_cd") == "")
    .filter(F.col("discrd_cd") != "X")
    .filter(F.col("aussl_cd") != "U3")
    .filter(F.col("bsart_cd") != "UB")
    .filter(F.col("elikz_cd") != "X")
    .filter(~F.col("pstyp_cd").isin("7","9"))
    .filter(F.length("lifnr_cd") == 10)
    .filter(~F.substring("due_dt_cd",-5,5).isin("04-01","12-31"))
)

df_posupp = (
    df_po_valid
    .join(df_lfa1,"lifnr_cd","left")
    .groupBy(F.col("matnr_cd").alias("nha"))
    .agg(
        F.expr("concat_ws(',', array_sort(collect_set(lifnr_cd)))").alias("posuppcode"),
        F.expr("concat_ws(',', array_sort(collect_set(name1_cd)))").alias("posuppname")
    )
)

df_posupp.createOrReplaceTempView("posupp")


# #### Cell 9 — Attach Open-PO Suppliers (PO1 / PO2) to Each NHA
# - At this point we know:
# Who we prefers (MRP + EORD), 
# Who we is actually buying from (Open POs)
# - OMAT must use Open PO suppliers as a fallback when:
# Master data is missing Or the part is newly sourced.
# - It checks Who are the first two active suppliers we are currently purchasing this NHA from.
# SAP logic: First supplier in PO list → PO1, Second supplier in PO list → PO2

# In[ ]:


df_stage5 = (
    spark.table("stage4_basepart_filled")
    .join(spark.table("posupp"), "nha", "left")
    .withColumn("suppcd_po1",F.split("posuppcode",",")[0])
    .withColumn("suppcd_po2",F.split("posuppcode",",")[1])
    .withColumn("suppname_po1",F.split("posuppname",",")[0])
    .withColumn("suppname_po2",F.split("posuppname",",")[1])
)

df_stage5.createOrReplaceTempView("stage5_with_po")


# #### Cell 10 — OB PR Supplier (Supplier Who Raised the Obsolescence Request)
# - Some parts become obsolete because a supplier officially reported the problem in IPLM (Problem Reports).
# - That supplier is directly involved Usually holds last-time-buy stock Is often the best contact for OMAT.
# - It Checks Which supplier raised the Obsolete Component PR for this NHA we uses this supplier as a high-priority fallback.

# In[ ]:


df_pr = (
    df_iplm
    .filter(F.col("state_cd") != "CLOSED")
    .filter(F.col("reason_cd") == "OBSOLETE COMPONENT")
    .filter(
        (F.col("state_cd").isin("CONFIRMED","IN REVIEW","IN WORK")) |
        (F.col("disposition_cd").isin("CONFIRMED","DEFER"))
    )
    .withColumn("suppcd_pr",F.lpad("supplier_code_cd",10,"0"))
    .join(df_lfa1, F.col("suppcd_pr")==df_lfa1.lifnr_cd, "left")
    .select("part_name_cd","suppcd_pr","name1_cd")
)

df_stage6 = (
    spark.table("stage5_with_po")
    .join(df_pr, df_pr.part_name_cd==F.col("nha"), "left")
    .drop("part_name_cd")
    .withColumnRenamed("name1_cd","suppname_pr")
)

df_stage6.createOrReplaceTempView("stage6_with_pr")


# In[ ]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# select * from stage6_with_pr limit 5


# #### Cell 11 — Final OMAT Supplier & Plant Selection
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

# In[ ]:


def nz(c):
    return F.when(F.trim(F.col(c)) == "", None).otherwise(F.col(c))

df = spark.table("stage6_with_pr")

df_final = (
    df
    .withColumn(
        "omat_selected_suppliercd",
        F.coalesce(
            nz("suppcd_2000"),
            nz("suppcd_1900"),
            nz("suppcd_3120"),
            nz("suppcd_1050"),
            nz("suppcd_1060"),
            nz("suppcd_1090"),
            nz("suppcd_po1"),
            nz("suppcd_po2"),
            nz("suppcd_pr")
        )
    )
    .withColumn(
        "omat_selected_supplier",
        F.coalesce(
            nz("suppname_2000"),
            nz("suppname_1900"),
            nz("suppname_3120"),
            nz("suppname_1050"),
            nz("suppname_1060"),
            nz("suppname_1090"),
            nz("suppname_po1"),
            nz("suppname_po2"),
            nz("suppname_pr")
        )
    )
    .withColumn(
        "omat_selected_plant",
        F.when(nz("suppcd_2000").isNotNull(),"2000")
         .when(nz("suppcd_1900").isNotNull(),"1900")
         .when(nz("suppcd_3120").isNotNull(),"3120")
         .when(nz("suppcd_1050").isNotNull(),"1050")
         .when(nz("suppcd_1060").isNotNull(),"1060")
         .when(nz("suppcd_1090").isNotNull(),"1090")
         .when(nz("suppcd_po1").isNotNull(),"2000")
         .when(nz("suppcd_pr").isNotNull(),"2000")
    )
)

df_final.createOrReplaceTempView("stage7_final_suppliers")


# #### Cell 12 — Write to OMAT Tables + Log Execution
# - This is the commit step:
# Everything we calculated in memory is now 
# Written into the OMAT reporting table, 
# Audited in the OMAT log table

# In[ ]:


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


log_schema = StructType([
    StructField("executed_on", TimestampType(), True),
    StructField("obprtno_cnt", IntegerType(), True),
    StructField("obprtno_nha_cnt", IntegerType(), True),
    StructField("obpr_cnt", IntegerType(), True),
    StructField("program_name", StringType(), True)
])

log_df = spark.createDataFrame(
    [(None, None, None, None, program_name)],
    schema=log_schema
).withColumn("executed_on", F.current_timestamp())

(
    log_df
    .write
    .mode("append")
    .format("delta")
    .saveAsTable(tgt_log_tbl)
)


# In[ ]:


# display(df_final.limit(10))

