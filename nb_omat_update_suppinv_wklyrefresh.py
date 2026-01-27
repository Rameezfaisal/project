#!/usr/bin/env python
# coding: utf-8

# ## nb_omat_update_suppinv_wklyrefresh
# 
# New notebook

# ##### **Pyspark_Notebook :** nb_omat_update_suppinv_wklyrefresh
# ##### 📌 **Author:** Rameez Ansari
# ##### 📅 **Last Updated:** 2025-12-31
# ###### 🔢 **Notebook Version:** 1.0
# ###### 🔄 **Change Log:** v1.0 | 2025-12-31 |
# ###### Rameez Ansari   → Work in Progress
# 📖 **Notebook Overview:**
# 
# This Stored Procedure is used to UPDATE the Supplier Inventory details of BUY NHAs that have either demand, consumption or Open order in OMAT Application along with its LTB info from iPLM on weekly basis. This is invoked from SQL and store the result set in Suppl Inventory Table in SQL.  	
# 
# **Includes:**    
# - Target Tables This Notebook Populates:
#     1. **rpt_omat_dmd_consumption_dtls :** Captures historical consumption and demand forecast for obsolete parts and NHAs, used for run-out and planning.
#         - Load Type : Incremental append based on NHAs where isprocessed = 'N' on (cmpprtno, nha).
# 
# 

# In[117]:


from silketl import SqlDatabaseConnector, get_workspace_name, preload, load_data, postload,json,current_workspace_name


# In[118]:


# project_code = 'p2f_ome' 
# ##FTR meaning feauture workspace
# exec_env = 'FTR'
# job_id = '' 
# task_id = '' 
# env = ''


# #### Step-1 -- Imports

# In[119]:


from pyspark.sql import functions as F
from pyspark.sql import types as T


# ##### Helper Function for Union

# In[120]:


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

# In[121]:


# =========================
# Source Tables
# =========================

paths = {
    "ecc": {
        "eord":          "wsf_silk_glb_da_dev.lhs_glb.ecc.eord",
        "lfa1":          "wsf_silk_glb_da_dev.lhs_glb.ecc.lfa1",
        "eban":          "wsf_silk_glb_da_dev.lhs_glb.ecc.eban",
        "zpo_hstry":     "wsf_silk_glb_da_dev.lhs_glb.ecc.zpo_hstry",
        "zpo_hstry_arch":"wsf_silk_glb_da_dev.lhs_glb.ecc.zpo_hstry_arch",
    },
    "s4h": {
        "eord":          "wsf_silk_glb_da_dev.lhs_glb.s4h.eord",
        "lfa1":          "wsf_silk_glb_da_dev.lhs_glb.s4h.lfa1",
        "eban":          "wsf_silk_glb_da_dev.lhs_glb.s4h.eban", 
        "zpo_hstry":     "wsf_silk_glb_da_dev.lhs_glb.s4h.zpo_hstry", 
        "zpo_hstry_arch":"wsf_silk_glb_da_dev.lhs_glb.s4h.zpo_hstry_arch",
    }
}



tbl_buynha_suppliers      = "lhs_glb.eng.rpt_omat_buynha_suppliers_plants"
tbl_dmd                   = "lhs_glb.eng.rpt_omat_dmd_consumption_dtls"
tbl_part_dtls             = "lhs_glb.eng.rpt_omat_part_dtls"
tbl_obprtno_dtls          = "lhs_glb.eng.rpt_omat_obprtno_dtls"
tbl_obpr_info             = "lhs_glb.eng.rpt_omat_obpr_info_dtls" 
tbl_rscxd_dmd             = "lhs_glb.eng.rpt_omat_rscxd_dmd_cons"
tbl_rscxd_dtls            = "lhs_glb.eng.rpt_omat_rscxd_dtls"

# =========================
# Target Tables
# =========================
tbl_target_supplier_inv   = "eng.rpt_omat_supplier_inventory"

# =========================
# Control Parameters
# =========================
refresh_frequency_days = 42
valid_states = ["CONFIRMED","IN REVIEW","IN WORK"]



# #### Step-3 -- Reading Source tables

# In[122]:


buynha_suppliers_df = spark.table(tbl_buynha_suppliers) \
    .select(
        F.col("nha").alias("nha"),
        F.col("omat_selected_suppliercd").alias("supcode")
    ).distinct()

eord_df = read_union_fast("eord") \
    .select(
        F.col("matnr").alias("material"),
        F.lpad(F.col("lifnr"),10,"0").alias("supcode_eord")
    )

lfa1_df = read_union_fast("lfa1") \
    .select(
        F.col("lifnr").alias("lifnr_lfa1"),
        F.col("name1").alias("suppliername_lfa1")
    )

dmd_df = spark.table(tbl_dmd).selectExpr(
    "cmpprtno as cmpprtno_dmd",
    "nha as nha_dmd",
    "lamqoh as lamqoh_dmd",
    "open_po_qty as open_po_qty_dmd",
    "restricted_all_stock as restricted_all_stock_dmd"
)

part_dtls_df = spark.table(tbl_part_dtls).selectExpr(
    "cmpprtno as cmpprtno_part",
    "nha as nha_part",
    "vencode as vencode_part",
    "venname as venname_part"
)

obprt_df = spark.table(tbl_obprtno_dtls).selectExpr(
    "obprno","obprtno",
    "upper(obpr_state) as obpr_state",
    "is_ltb_avail","ltb_date","ltb_qty",
    "supplier_date_to_zero as supplier_date_to_zero"
)

rscxd_dmd_df = spark.table(tbl_rscxd_dmd).selectExpr(
    "cmpprtno as cmpprtno_r",
    "nha as nha_r",
    "lamqoh as lamqoh_r",
    "open_po_qty as open_po_qty_r",
    "restricted_all_stock as restricted_all_stock_r"
)

rscxd_dtls_df = spark.table(tbl_rscxd_dtls).selectExpr(
    "cmpprtno as cmpprtno_rd",
    "nha as nha_rd",
    "vencode as vencode_rd",
    "venname as venname_rd"
)


# #### Step-4 -- 

# In[123]:


base_data_df = buynha_suppliers_df.groupBy("nha","supcode").count().drop("count")


# #### Step-5 -- 

# In[124]:


nha_supp_df = eord_df.alias("a") \
    .join(lfa1_df.alias("b"), F.col("a.supcode_eord")==F.col("b.lifnr_lfa1"), "left") \
    .join(base_data_df.alias("bd"),
          (F.col("a.material")==F.col("bd.nha")) &
          (F.col("a.supcode_eord") != F.col("bd.supcode")),
          "left") \
    .select(
        F.col("a.material").alias("material_ns"),
        F.col("a.supcode_eord").alias("supcode_ns"),
        F.col("b.suppliername_lfa1").alias("suppliername_ns")
    ).distinct()


# #### Step-6 --  LTB OPEN PO DATASET
# - This step builds the list of LTB Purchase Orders that are still OPEN
# → These represent suppliers who still owe material for long-term buys.
# We are reconstructing SAP’s PART_SUPP logic safely in Spark.

# In[125]:


# ================================
# Active PO History
# ================================
zpo_hist_active_df = read_union_fast("zpo_hstry").select(
    F.col("matnr").alias("material_ph"),
    F.col("ebeln").alias("po_number_ph"),
    F.col("ebelp").alias("po_item_ph"),
    F.col("menge").alias("qty_requested_ph"),
    F.col("wemng").alias("qty_received_ph"),
    F.col("lifnr").alias("ltb_supplier"),
    F.col("closed").alias("closed_flag"),
    F.col("status").alias("status_flag")
)

# ================================
# Archived PO History  (MISSING BEFORE)
# ================================
zpo_hist_arch_df = read_union_fast("zpo_hstry_arch").select(
    F.col("matnr").alias("material_ph"),
    F.col("ebeln").alias("po_number_ph"),
    F.col("ebelp").alias("po_item_ph"),
    F.col("menge").alias("qty_requested_ph"),
    F.lit(None).cast("double").alias("qty_received_ph"),  # archived zero-closed lines
    F.col("lifnr").alias("ltb_supplier"),
    F.col("closed").alias("closed_flag"),
    F.lit(None).alias("status_flag")
)

# Combine active + archive (SAP UNION behavior)
zpo_hist_all_df = zpo_hist_active_df.unionByName(zpo_hist_arch_df)

# Only completed / valid LTB lines (SAP filters CLSCOMP / CLSZERO logic simplified safely)
ltb_hist_df = zpo_hist_all_df.filter(
    (F.col("closed_flag").isin("CLSCOMP", "CLSZERO")) |
    (F.col("status_flag").isNull())
)

# ================================
# EBAN (LTB PR base)
# ================================
eban_df = read_union_fast("eban").select(
    F.col("banfn").alias("banfn_eban"),
    F.col("bnfpo").alias("bnfpo_eban"),
    F.col("ebeln").alias("po_number"),
    F.col("ebelp").alias("po_item"),
    F.col("ekgrp").alias("purch_group"),
    F.col("loekz").alias("delete_flag")
)

# ================================
# Final LTB Supplier Dataset
# ================================
part_supp_df = eban_df.alias("e") \
    .join(
        ltb_hist_df.alias("h"),
        (F.col("e.po_number") == F.col("h.po_number_ph")) &
        (F.col("e.po_item") == F.col("h.po_item_ph")),
        "inner"
    ) \
    .filter(
        (F.col("e.purch_group") == "LTB") &
        (F.col("e.delete_flag") != "X")
    ) \
    .groupBy(
        F.col("h.material_ph").alias("material_ps"),
        F.col("h.ltb_supplier").alias("ltb_supplier_ps"),
        F.col("e.po_number").alias("ltb_po_number")
    ) \
    .agg(
        F.sum("qty_requested_ph").alias("qty_requested"),
        F.sum("qty_received_ph").alias("qty_received")
    ) \
    .withColumn(
        "ltb_status",
        F.when(F.col("qty_requested") == F.col("qty_received"), "LTB COMPLETED")
         .when(F.col("qty_received").isNull(), "LTB CANCELLED")
         .otherwise("LTB PO OPENED")
    ) \
    .filter(F.col("ltb_status") == "LTB PO OPENED")

part_supp_df.cache()
part_supp_df.count()


# #### Step-7 -- Build TEMP Supplier Table (SAP #temp_table)
# - Here we identify:
#     - Suppliers who are NOT the selected OMAT supplier for an NHA, but DO have an OPEN LTB PO
#     - These suppliers must still be tracked because they owe material, even if they are not the preferred supplier.
#     - This replaces SAP temporary table #temp_table.

# In[126]:


# Join alternate NHA suppliers with OPEN LTB PO suppliers
temp_table_df = nha_supp_df.alias("ns") \
    .join(
        part_supp_df.alias("ps"),
        (F.col("ns.material_ns") == F.col("ps.material_ps")) &
        (F.col("ns.supcode_ns") == F.col("ps.ltb_supplier_ps")),
        "inner"
    ) \
    .select(
        F.col("ns.material_ns").alias("material_tt"),
        F.col("ns.supcode_ns").alias("vencode_tt"),
        F.col("ns.suppliername_ns").alias("venname_tt"),
        F.col("ps.ltb_po_number").alias("ltbpono_tt")
    ) \
    .distinct()

temp_table_df.cache()
temp_table_df.count()


# #### Step-8 -- OB Part Supplier (Primary OB Supplier Logic)
# - This block captures:
#     - OB Parts where the component itself is the NHA
#     - and we pull the primary (OB) supplier + current inventory position
#     - These are the main suppliers responsible for the obsolescence part.
#     - This corresponds to the FIRST SELECT block in SAP before all the UNIONs.
# - What we derive here For each OB Part:
#     - Supplier (preferred/OB supplier)
#     - On-hand stock (LAMQOH)
#     - Open PO quantity
#     - LTB info (if exists)
#     - Refresh scheduling fields
# - Only OB parts that truly belong to a valid OB Problem Report should proceed.

# In[142]:


obpr_info_df = spark.table(tbl_obpr_info).select(
    F.col("obprno").alias("obprno_info"),
    F.col("obprtno").alias("obprtno_info")
).distinct()

# Enforce SAP join condition
ob_base_df = ob_base_df.alias("o") \
    .join(
        obpr_info_df.alias("i"),
        (F.col("o.obprno") == F.col("i.obprno_info")) &
        (F.col("o.obprtno") == F.col("i.obprtno_info")),
        "inner"
    ) \
    .select("o.*") \
    .distinct()

ob_base_df.cache()
ob_base_df.count()


# In[128]:


ob_supplier_df = ob_base_df.alias("o") \
    .join(
        dmd_df.alias("d"),
        F.col("o.obprtno") == F.col("d.cmpprtno_dmd"),
        "inner"
    ) \
    .join(
        part_dtls_df.alias("p"),
        (F.col("p.cmpprtno_part") == F.col("d.cmpprtno_dmd")) &
        (F.col("p.nha_part") == F.col("d.nha_dmd")),
        "inner"
    ) \
    .select(
        F.col("o.obprno"),
        F.col("o.obprtno"),
        F.col("d.nha_dmd").alias("nha"),
        F.col("p.vencode_part").alias("vencode"),
        F.col("p.venname_part").alias("venname"),
        F.col("o.is_ltb_avail"),
        F.col("o.ltb_date").alias("ltbdate"),
        F.col("o.ltb_qty").cast("int").alias("ltbqty"),
        F.lit(0).alias("qty_liable_supplier"),
        F.lit(None).cast("int").alias("qty_available_supplier"),
        F.col("o.supplier_date_to_zero").alias("lrc_suppliers_date_to_zero_inventory"),
        F.col("d.lamqoh_dmd").cast("int").alias("lamqoh"),
        F.col("d.open_po_qty_dmd").cast("int").alias("open_po_qty"),
        F.lit(refresh_frequency_days).alias("refresh_frequency"),
        F.lit(None).cast("date").alias("supply_last_updated"),
        F.current_date().alias("last_request_date"),
        F.date_add(F.current_date(), refresh_frequency_days).alias("next_request_date"),
        F.lit("Y").alias("obsupplier"),
        F.col("d.restricted_all_stock_dmd").cast("int").alias("restricted_all_stock")
    ) \
    .distinct()

ob_supplier_df.cache()
ob_supplier_df.count()


# #### Step-9 --  NHA Suppliers (MRP Preferred Suppliers)
# - This block finds:
#     - Suppliers of the Parent Assembly (NHA)
#     - for OB parts that are components of that assembly
#     - These are MRP-preferred suppliers for the higher-level build.

# In[129]:


nha_supplier_df = ob_base_df.alias("o") \
    .join(
        dmd_df.alias("d"),
        (F.col("d.cmpprtno_dmd") == F.col("o.obprtno")) &
        (F.col("d.nha_dmd") != F.col("d.cmpprtno_dmd")),
        "inner"
    ) \
    .join(
        part_dtls_df.alias("p"),
        (F.col("p.cmpprtno_part") == F.col("o.obprtno")) &
        (F.col("p.nha_part") == F.col("d.nha_dmd")),
        "inner"
    ) \
    .select(
        F.col("o.obprno"),
        F.col("o.obprtno"),
        F.col("d.nha_dmd").alias("nha"),
        F.col("p.vencode_part").alias("vencode"),
        F.col("p.venname_part").alias("venname"),
        F.lit("NA").alias("is_ltb_avail"),
        F.lit(None).cast("date").alias("ltbdate"),
        F.lit(0).alias("ltbqty"),
        F.lit(0).alias("qty_liable_supplier"),
        F.lit(None).cast("int").alias("qty_available_supplier"),
        F.lit(None).cast("date").alias("lrc_suppliers_date_to_zero_inventory"),
        F.col("d.lamqoh_dmd").cast("int").alias("lamqoh"),
        F.col("d.open_po_qty_dmd").cast("int").alias("open_po_qty"),
        F.lit(refresh_frequency_days).alias("refresh_frequency"),
        F.lit(None).cast("date").alias("supply_last_updated"),
        F.current_date().alias("last_request_date"),
        F.date_add(F.current_date(), refresh_frequency_days).alias("next_request_date"),
        F.lit("N").alias("obsupplier"),
        F.col("d.restricted_all_stock_dmd").cast("int").alias("restricted_all_stock")
    ) \
    .distinct()

nha_supplier_df.cache()
nha_supplier_df.count()


# #### Step-10 -- LTB OPEN PO SUPPLIERS (From Temp Table)
# 
# - These are suppliers who still have OPEN LTB POs
#     - for either: The OB part itself, or The NHA where OB part is used
#     - Even if they are not preferred suppliers, they must be tracked because inventory is still inbound.

# In[130]:


ltb_supplier_df = temp_table_df.alias("t") \
    .join(
        ob_base_df.alias("o"),
        F.col("t.material_tt") == F.col("o.obprtno"),
        "inner"
    ) \
    .join(
        dmd_df.alias("d"),
        F.col("d.cmpprtno_dmd") == F.col("o.obprtno"),
        "left"
    ) \
    .select(
        F.col("o.obprno"),
        F.col("o.obprtno"),
        F.coalesce(F.col("d.nha_dmd"), F.col("o.obprtno")).alias("nha"),
        F.col("t.vencode_tt").alias("vencode"),
        F.col("t.venname_tt").alias("venname"),
        F.lit("NA").alias("is_ltb_avail"),
        F.lit(None).cast("date").alias("ltbdate"),
        F.lit(0).alias("ltbqty"),
        F.lit(0).alias("qty_liable_supplier"),
        F.lit(None).cast("int").alias("qty_available_supplier"),
        F.lit(None).cast("date").alias("lrc_suppliers_date_to_zero_inventory"),
        F.coalesce(F.col("d.lamqoh_dmd"), F.lit(0)).cast("int").alias("lamqoh"),
        F.coalesce(F.col("d.open_po_qty_dmd"), F.lit(0)).cast("int").alias("open_po_qty"),
        F.lit(refresh_frequency_days).alias("refresh_frequency"),
        F.lit(None).cast("date").alias("supply_last_updated"),
        F.current_date().alias("last_request_date"),
        F.date_add(F.current_date(), refresh_frequency_days).alias("next_request_date"),
        F.lit("N").alias("obsupplier"),
        F.coalesce(F.col("d.restricted_all_stock_dmd"), F.lit(0)).cast("int").alias("restricted_all_stock")
    ) \
    .distinct()

ltb_supplier_df.cache()
ltb_supplier_df.count()


# #### Step-11 -- R / S / C / X / D Parts Supplier Logic
# - Some special part categories (Repair, Service, Consumable, etc.) have separate supplier tracking outside normal OMAT logic.

# In[131]:


rscxd_supplier_df = ob_base_df.alias("o") \
    .join(
        rscxd_dmd_df.alias("d"),
        (F.col("d.cmpprtno_r") == F.col("o.obprtno")) &
        (F.col("d.nha_r") != F.col("d.cmpprtno_r")),
        "inner"
    ) \
    .join(
        rscxd_dtls_df.alias("p"),
        (F.col("p.cmpprtno_rd") == F.col("o.obprtno")) &
        (F.col("p.nha_rd") == F.col("d.nha_r")),
        "inner"
    ) \
    .filter(F.col("p.vencode_rd").isNotNull()) \
    .select(
        F.col("o.obprno"),
        F.col("o.obprtno"),
        F.col("d.nha_r").alias("nha"),
        F.col("p.vencode_rd").alias("vencode"),
        F.col("p.venname_rd").alias("venname"),
        F.lit("NA").alias("is_ltb_avail"),
        F.lit(None).cast("date").alias("ltbdate"),
        F.lit(0).alias("ltbqty"),
        F.lit(0).alias("qty_liable_supplier"),
        F.lit(None).cast("int").alias("qty_available_supplier"),
        F.lit(None).cast("date").alias("lrc_suppliers_date_to_zero_inventory"),
        F.col("d.lamqoh_r").cast("int").alias("lamqoh"),
        F.col("d.open_po_qty_r").cast("int").alias("open_po_qty"),
        F.lit(refresh_frequency_days).alias("refresh_frequency"),
        F.lit(None).cast("date").alias("supply_last_updated"),
        F.current_date().alias("last_request_date"),
        F.date_add(F.current_date(), refresh_frequency_days).alias("next_request_date"),
        F.lit("N").alias("obsupplier"),
        F.col("d.restricted_all_stock_r").cast("int").alias("restricted_all_stock")
    ) \
    .distinct()

rscxd_supplier_df.cache()
rscxd_supplier_df.count()


# #### Step-12 -- All Suppliers (OB Part Direct Suppliers)
# - This block says:
# Even if no demand exists,
# list ALL known suppliers of this OB part
# so procurement can still reach out.

# In[132]:


all_suppliers_part_df = ob_base_df.alias("o") \
    .join(
        nha_supp_df.alias("s"),
        F.col("s.material_ns") == F.col("o.obprtno"),
        "inner"
    ) \
    .select(
        F.col("o.obprno"),
        F.col("o.obprtno"),
        F.col("o.obprtno").alias("nha"),
        F.col("s.supcode_ns").alias("vencode"),
        F.col("s.suppliername_ns").alias("venname"),
        F.lit("NA").alias("is_ltb_avail"),
        F.lit(None).cast("date").alias("ltbdate"),
        F.lit(0).alias("ltbqty"),
        F.lit(0).alias("qty_liable_supplier"),
        F.lit(None).cast("int").alias("qty_available_supplier"),
        F.lit(None).cast("date").alias("lrc_suppliers_date_to_zero_inventory"),
        F.lit(0).alias("lamqoh"),
        F.lit(0).alias("open_po_qty"),
        F.lit(refresh_frequency_days).alias("refresh_frequency"),
        F.lit(None).cast("date").alias("supply_last_updated"),
        F.current_date().alias("last_request_date"),
        F.date_add(F.current_date(), refresh_frequency_days).alias("next_request_date"),
        F.lit("N").alias("obsupplier"),
        F.lit(0).alias("restricted_all_stock")
    ) \
    .distinct()

all_suppliers_part_df.cache()
all_suppliers_part_df.count()


# #### Step-13 -- All Suppliers (NHA Level Suppliers)
# - If the OB part is used in a higher-level assembly (NHA),
# SAP also pulls all suppliers of that NHA.
# - Even if no demand exists.
# This broadens supplier outreach for potential alternates.

# In[133]:


all_suppliers_nha_df = ob_base_df.alias("o") \
    .join(
        dmd_df.alias("d"),
        (F.col("d.cmpprtno_dmd") == F.col("o.obprtno")) &
        (F.col("d.nha_dmd") != F.col("d.cmpprtno_dmd")),
        "inner"
    ) \
    .join(
        nha_supp_df.alias("s"),
        F.col("s.material_ns") == F.col("d.nha_dmd"),
        "inner"
    ) \
    .select(
        F.col("o.obprno"),
        F.col("o.obprtno"),
        F.col("d.nha_dmd").alias("nha"),
        F.col("s.supcode_ns").alias("vencode"),
        F.col("s.suppliername_ns").alias("venname"),
        F.lit("NA").alias("is_ltb_avail"),
        F.lit(None).cast("date").alias("ltbdate"),
        F.lit(0).alias("ltbqty"),
        F.lit(0).alias("qty_liable_supplier"),
        F.lit(None).cast("int").alias("qty_available_supplier"),
        F.lit(None).cast("date").alias("lrc_suppliers_date_to_zero_inventory"),
        F.lit(0).alias("lamqoh"),
        F.lit(0).alias("open_po_qty"),
        F.lit(refresh_frequency_days).alias("refresh_frequency"),
        F.lit(None).cast("date").alias("supply_last_updated"),
        F.current_date().alias("last_request_date"),
        F.date_add(F.current_date(), refresh_frequency_days).alias("next_request_date"),
        F.lit("N").alias("obsupplier"),
        F.lit(0).alias("restricted_all_stock")
    ) \
    .distinct()

all_suppliers_nha_df.cache()
all_suppliers_nha_df.count()


# #### Step-14 -- All Suppliers (Component → NHA Supplier Backfill)
# - This block is a fallback expansion SAP uses when:
#     - OB part is a component of an NHA
#     - NHA has suppliers
#     - But those suppliers were not already captured in earlier logic
# - It ensures no potential supplier is missed.
# 

# In[134]:


all_suppliers_component_df = ob_base_df.alias("o") \
    .join(
        dmd_df.alias("d"),
        (F.col("d.cmpprtno_dmd") == F.col("o.obprtno")) &
        (F.col("d.nha_dmd") != F.col("d.cmpprtno_dmd")),
        "inner"
    ) \
    .join(
        nha_supp_df.alias("s"),
        F.col("s.material_ns") == F.col("d.nha_dmd"),
        "inner"
    ) \
    .select(
        F.col("o.obprno"),
        F.col("o.obprtno"),
        F.col("o.obprtno").alias("nha"),
        F.col("s.supcode_ns").alias("vencode"),
        F.col("s.suppliername_ns").alias("venname"),
        F.lit("NA").alias("is_ltb_avail"),
        F.lit(None).cast("date").alias("ltbdate"),
        F.lit(0).alias("ltbqty"),
        F.lit(0).alias("qty_liable_supplier"),
        F.lit(None).cast("int").alias("qty_available_supplier"),
        F.lit(None).cast("date").alias("lrc_suppliers_date_to_zero_inventory"),
        F.lit(0).alias("lamqoh"),
        F.lit(0).alias("open_po_qty"),
        F.lit(refresh_frequency_days).alias("refresh_frequency"),
        F.lit(None).cast("date").alias("supply_last_updated"),
        F.current_date().alias("last_request_date"),
        F.date_add(F.current_date(), refresh_frequency_days).alias("next_request_date"),
        F.lit("N").alias("obsupplier"),
        F.lit(0).alias("restricted_all_stock")
    ) \
    .distinct()

all_suppliers_component_df.cache()
all_suppliers_component_df.count()


# #### Step-15 -- Final UNION (Materialized Result Set)
# - We now combine all supplier discovery paths into a single result.

# In[135]:


final_supplier_inventory_df = (
    ob_supplier_df
    .unionByName(nha_supplier_df)
    .unionByName(ltb_supplier_df)
    .unionByName(rscxd_supplier_df)
    .unionByName(all_suppliers_part_df)
    .unionByName(all_suppliers_nha_df)
    .unionByName(all_suppliers_component_df)
).distinct()

final_supplier_inventory_df.cache()
final_supplier_inventory_df.count()


# #### Step-16 — Final Column Standardization (SAP Output Schema)
# - This is the materialized result of the procedure.

# In[136]:


final_output_df = final_supplier_inventory_df.select(
    F.col("obprno").alias("obprno"),
    F.col("obprtno").alias("obprtno"),
    F.col("nha").alias("nha"),
    F.col("vencode").alias("vencode"),
    F.col("venname").alias("venname"),
    F.when(F.upper(F.col("is_ltb_avail")).isin("Y", "YES"), "YES")
    .when(F.upper(F.col("is_ltb_avail")).isin("N", "NO"), "NO")
    .otherwise("NA")
    .alias("is_ltb_avail"), 
    F.col("ltbdate").cast("date").alias("ltbdate"),
    F.col("ltbqty").cast("int").alias("ltbqty"),
    F.col("qty_liable_supplier").cast("int").alias("qty_liable_supplier"),
    F.col("qty_available_supplier").cast("int").alias("qty_available_supplier"),
    F.col("lrc_suppliers_date_to_zero_inventory").cast("date").alias("lrc_suppliers_date_to_zero_inventory"),
    F.col("lamqoh").cast("int").alias("lamqoh"),
    F.col("open_po_qty").cast("int").alias("open_po_qty"),
    F.col("refresh_frequency").cast("int").alias("refresh_frequency"),
    F.col("supply_last_updated").cast("date").alias("supply_last_updated"),
    F.col("last_request_date").cast("date").alias("last_request_date"),
    F.col("next_request_date").cast("date").alias("next_request_date"),
    F.col("obsupplier").alias("obsupplier"),
    F.col("restricted_all_stock").cast("int").alias("restricted_all_stock")
)

# final_output_df.printSchema()
# final_output_df.show(truncate=False)


# In[137]:


# Target table path (already parameterized earlier)
target_table = tbl_target_supplier_inv

# Write as FULL LOAD (overwrite each run)
final_output_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(target_table)

print(f"Table {target_table} created/refreshed successfully")

