#!/usr/bin/env python
# coding: utf-8

# ## nb_omat_rscxd_dmd_cons
# 
# New notebook

# ##### **Pyspark_Notebook :** nb_omat_rscxd_dmd_cons
# ##### 📌 **Author:** Rameez Ansari
# ##### 📅 **Last Updated:** 2026-01-07
# ###### 🔢 **Notebook Version:** 1.0
# ###### 🔄 **Change Log:** v1.0 | 2026-01-07 |
# ###### Rameez Ansari   → Work in Progress
# 📖 **Notebook Overview:**
# 
# This Notebook identifies the demand, consumption of RSCXD parts.	
# 
# **Includes:**    
# - Target Tables This Notebook Populates:
#     1. **rpt_omat_rscxd_dtls:** Provides RSCXD parts details of NHAs linked to obsolete part numbers
#         - Load type: Full refresh (truncate/delete + reload)
#     2. **rpt_omat_rscxd_dmd_cons:** Provides RSCXD parts Demand and Consumption of NHAs linked to obsolete part numbers
# 
#         - Load type: Full refresh (truncate/delete + reload)
# 

# In[49]:


from silketl import SqlDatabaseConnector, get_workspace_name, preload, load_data, postload,json,current_workspace_name


# In[50]:


# project_code = 'p2f_ome' 
# ##FTR meaning feauture workspace
# exec_env = 'FTR'
# job_id = '' 
# task_id = '' 
# env = ''


# #### Step-1 -- Imports

# In[51]:


from pyspark.sql import functions as F
from pyspark.sql import Window


# ##### Helper Union Function

# In[52]:


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

# In[53]:


# Source tables


paths = {
    "ecc": {
        "makt":            "wsf_silk_glb_da_dev.lhs_glb.ecc.makt",
        "mara":            "wsf_silk_glb_da_dev.lhs_glb.ecc.mara",
        "marc":            "wsf_silk_glb_da_dev.lhs_glb.ecc.marc",
        "mbew":            "wsf_silk_glb_da_dev.lhs_glb.ecc.mbew",
        "ausp":            "wsf_silk_glb_da_dev.lhs_glb.ecc.ausp",
        "zsupplier_xref":  "wsf_silk_glb_da_dev.lhs_glb.ecc.zsupplier_xref",
        "zsupp_code_mstr": "wsf_silk_glb_da_dev.lhs_glb.ecc.zsupp_code_mstr",
        "zspm_dmd_detail": "wsf_silk_glb_da_dev.lhs_glb.ecc.zspm_dmd_detail",
        "zspm_sales_order":"wsf_silk_glb_da_dev.lhs_glb.ecc.zspm_sales_order",
        "zpo_hstry":       "wsf_silk_glb_da_dev.lhs_glb.ecc.zpo_hstry",
        "ekpo":            "wsf_silk_glb_da_dev.lhs_glb.ecc.ekpo",
        "mard":            "wsf_silk_glb_da_dev.lhs_glb.ecc.mard",
    },
    "s4h": {
        "makt":            "wsf_silk_glb_da_dev.lhs_glb.s4h.makt",
        "mara":            "wsf_silk_glb_da_dev.lhs_glb.s4h.mara",
        "marc":            "wsf_silk_glb_da_dev.lhs_glb.s4h.marc",
        "mbew":            "wsf_silk_glb_da_dev.lhs_glb.s4h.mbew",
        "ausp":            "wsf_silk_glb_da_dev.lhs_glb.s4h.ausp",
        "zsupplier_xref":  "wsf_silk_glb_da_dev.lhs_glb.s4h.zsupplier_xref",
        "zsupp_code_mstr": "wsf_silk_glb_da_dev.lhs_glb.s4h.zsupp_code_mstr",
        "zspm_dmd_detail": "wsf_silk_glb_da_dev.lhs_glb.s4h.zspm_dmd_detail",
        "zspm_sales_order":"wsf_silk_glb_da_dev.lhs_glb.s4h.zspm_sales_order",
        "zpo_hstry":       "wsf_silk_glb_da_dev.lhs_glb.s4h.zpo_hstry",
        "ekpo":            "wsf_silk_glb_da_dev.lhs_glb.s4h.ekpo",
        "mard":            "wsf_silk_glb_da_dev.lhs_glb.s4h.mard",
    }
}



# Target tables

tgt_dtls     = "eng.rpt_omat_rscxd_dtls"
tgt_dmd_cons = "eng.rpt_omat_rscxd_dmd_cons"


# In[54]:


# # Source tables

# cv_rcsm_material        = "lhs_glb.eng.stg_omat_csbg_rcsm_material"
# cv_buy_nha_dtls         = "lhs_glb.omat_test.rpt_omat_obpn_buy_nha_dtls"
# cv_obprtno_dtls         = "lhs_glb.omat_test.rpt_omat_obprtno_dtls"
# cv_buynha_suppliers     = "lhs_glb.omat_test.rpt_omat_buynha_suppliers_plants"
# cv_part_ecosystem       = "wsf_silk_glb_dt_qa.lhg_glb.eng.rpt_part_ecosystem"
# cv_rscxd                = "lhs_glb.omat_test.rpt_omat_rscxd_dtls"
# cv_makt                 = "wsf_silk_glb_da_dev.lhs_glb.ecc.makt"
# cv_mara                 = "wsf_silk_glb_da_dev.lhs_glb.ecc.mara"
# cv_marc                 = "wsf_silk_glb_da_dev.lhs_glb.ecc.marc"
# cv_mbew                 = "wsf_silk_glb_da_dev.lhs_glb.ecc.mbew"
# cv_ausp                 = "wsf_silk_glb_da_dev.lhs_glb.ecc.ausp"
# cv_supplier_xref        = "wsf_silk_glb_da_dev.lhs_glb.ecc.zsupplier_xref"
# cv_supp_code            = "wsf_silk_glb_da_dev.lhs_glb.ecc.zsupp_code_mstr"
# cv_dmd_detail           = "wsf_silk_glb_da_dev.lhs_glb.ecc.zspm_dmd_detail"
# cv_sales_order         = "wsf_silk_glb_da_dev.lhs_glb.ecc.zspm_sales_order"
# cv_po_hist             = "wsf_silk_glb_da_dev.lhs_glb.ecc.zpo_hstry"
# cv_ekpo                = "wsf_silk_glb_da_dev.lhs_glb.ecc.ekpo"
# cv_mard                = "wsf_silk_glb_da_dev.lhs_glb.ecc.mard"







# #### Step-3 -- Reading Source tables

# In[55]:


# mat_df      = spark.table(cv_rcsm_material)
# buy_df      = spark.table(cv_buy_nha_dtls)
# ob_df       = spark.table(cv_obprtno_dtls)
# makt_df     = spark.table(cv_makt)
# sup_df      = spark.table(cv_buynha_suppliers)
# eco_df      = spark.table(cv_part_ecosystem)
# mara_df     = spark.table(cv_mara)
# marc_df     = spark.table(cv_marc)
# mbew_df     = spark.table(cv_mbew)
# ausp_df     = spark.table(cv_ausp)
# xref_df     = spark.table(cv_supplier_xref)
# code_df     = spark.table(cv_supp_code)
# rscxd_df    = spark.table(cv_rscxd)
# dmd_df      = spark.table(cv_dmd_detail)
# so_df       = spark.table(cv_sales_order)
# po_df       = spark.table(cv_po_hist)
# ekpo_df     = spark.table(cv_ekpo)
# mard_df     = spark.table(cv_mard)

makt_df = read_union_fast("makt")
mara_df = read_union_fast("mara")
marc_df = read_union_fast("marc")
mbew_df = read_union_fast("mbew")
ausp_df = read_union_fast("ausp")
xref_df = read_union_fast("zsupplier_xref")
code_df = read_union_fast("zsupp_code_mstr")
dmd_df  = read_union_fast("zspm_dmd_detail")
so_df   = read_union_fast("zspm_sales_order")
po_df   = read_union_fast("zpo_hstry")
ekpo_df = read_union_fast("ekpo")
mard_df = read_union_fast("mard")


mat_df   = spark.table("lhs_glb.eng.stg_omat_csbg_rcsm_material")
buy_df   = spark.table("lhs_glb.omat_test.rpt_omat_obpn_buy_nha_dtls")
ob_df    = spark.table("lhs_glb.omat_test.rpt_omat_obprtno_dtls")
sup_df   = spark.table("lhs_glb.omat_test.rpt_omat_buynha_suppliers_plants")
eco_df   = spark.table("wsf_silk_glb_dt_qa.lhg_glb.eng.rpt_part_ecosystem")
rscxd_df = spark.table("lhs_glb.omat_test.rpt_omat_rscxd_dtls")


# #### Step-4 -- Date window calculation (DT1WK, DT26WK)
# - DT1WK = Start of the current week
# - DT26WK = End of the 26-week window (~6 months)
# - This creates the time window in which open sales orders are considered as demand for RSCXD parts.
# - Look at all sales demand for the next 6 months, starting from this week

# In[56]:


dt1wk = F.date_sub(F.current_date(), F.weekday(F.current_date()))
dt26wk = F.date_add(
            F.date_sub(F.date_add(dt1wk, 182), F.weekday(F.date_add(dt1wk, 182))), 6
         )


# #### Step-5 -- NHALIST (Part Ecosystem Mapping)
# - Identifies all RSCXD component parts and maps them to their parent assemblies (NHA/DASH) that are in the obsolescence program.
# - Which components belong to which assembly, and are actively being tracked for obsolescence

# In[57]:


nhalist = (
    mat_df.alias("a")
    .join(buy_df.alias("b"), F.col("b.nha") == F.col("a.material"))
    .join(ob_df.alias("c"), F.col("c.obprtno") == F.col("b.cmpprtno"))
    .filter(F.upper("c.obpr_state").isin("CONFIRMED","IN REVIEW","IN WORK"))
    .select(
        F.col("b.cmpprtno").alias("cmpprtno"),
        F.col("b.nha").alias("dash"),
        "a.r_part","a.core_part","a.s_part","a.x_part","a.d_part"
    ).distinct()
)


# #### Step-6 -- LSTDATA (Explode part types)
# - Creates one row per real physical part from the part ecosystem.
# Each NHA has multiple types of parts:
# R = Replaceable, C = Core, S = Spares, X = Exchange, D = Depot
# - List every sellable, usable, and stocked part that belongs to each RSCXD assembly.

# In[58]:


lstdata = (
    nhalist.select("cmpprtno","dash",F.col("r_part").alias("nha"),F.lit("R").alias("type")).filter("nha is not null")
    .unionByName(nhalist.select("cmpprtno","dash",F.col("core_part").alias("nha"),F.lit("C").alias("type")).filter("nha is not null"))
    .unionByName(nhalist.select("cmpprtno","dash",F.col("s_part").alias("nha"),F.lit("S").alias("type")).filter("nha is not null"))
    .unionByName(nhalist.select("cmpprtno","dash",F.col("x_part").alias("nha"),F.lit("X").alias("type")).filter("nha is not null"))
    .unionByName(nhalist.select("cmpprtno","dash",F.col("d_part").alias("nha"),F.lit("D").alias("type")).filter("nha is not null"))
)


# #### Step-7 -- Build OMAT_RSCXD_DTLS (Master data table)
# - Creates the master RSCXD part table used by planners.
#     - **It enriches every part with:**
# Description, Supplier, Lead time, Material status, Commodity hierarchy, Criticality & CE flags
#     - **And removes:**
# Obsolete or out-of-scope parts (OB, OS, OP)
# - This is the golden list of all valid RSCXD parts with procurement and planning attributes.
#     - **This table is the foundation for:**
# Demand, Consumption, Inventory, Open POs and Everything downstream depends on this.

# In[59]:


spark.sql(f"DELETE FROM {tgt_dtls}")


# In[60]:


final_dtls = (
    lstdata.alias("a")

    # Material description
    .join(makt_df.alias("m"), F.col("m.matnr") == F.col("a.nha"), "left")

    # Supplier per NHA
    .join(sup_df.alias("s"), F.col("s.nha") == F.col("a.nha"), "left")

    # Part ecosystem (CMPQPA)
    .join(
        eco_df.alias("e"),
        (F.col("e.nha") == F.col("a.dash")) &
        (F.col("e.obprtno") == F.col("a.cmpprtno")),
        "left"
    )

    # Material master (status + commodity)
    .join(mara_df.alias("ma"), F.col("ma.matnr") == F.col("a.nha"), "left")

    # Plant-specific planning (LEAD TIME)
    .join(
        marc_df.alias("mc"),
        (F.col("mc.matnr") == F.col("a.nha")) &
        (F.col("mc.werks") == F.col("s.omat_selected_plant")),
        "left"
    )

    # Exclude obsolete & out-of-scope parts
    .filter(~F.col("ma.mstae").isin("OB", "OS", "OP"))

    # Final column set (lineage safe)
    .select(
        F.col("a.cmpprtno"),
        F.col("a.nha"),
        F.col("a.type"),
        F.col("a.dash"),
        F.col("m.maktx").alias("description"),
        F.col("e.cmpqpa"),
        F.col("ma.matkl"),
        F.col("ma.mstae"),
        F.col("s.omat_selected_suppliercd").alias("vencode"),
        F.col("s.omat_selected_supplier").alias("venname"),
        F.col("mc.plifz").alias("lead_time")
    )
)


# In[61]:


final_dtls_insert = (
    final_dtls
    .select(
        F.lit(None).cast("string").alias("ce"),
        F.col("cmpprtno"),
        F.col("cmpqpa"),
        F.lit(None).cast("string").alias("commodity"),
        F.lit(None).cast("string").alias("critical"),
        F.lit(None).cast("string").alias("curr_cost"),
        F.col("dash"),
        F.col("description"),
        F.col("lead_time"),
        F.col("matkl"),
        F.col("mstae"),
        F.col("nha"),
        F.lit(None).cast("string").alias("po_supplier"),
        F.lit(None).cast("string").alias("po_supplier_code"),
        F.lit(None).cast("string").alias("sbm"),
        F.lit(None).cast("string").alias("smg_group"),
        F.lit(None).cast("string").alias("smg_head"),
        F.lit(None).cast("string").alias("std_cost"),
        F.col("type"),
        F.col("vencode"),
        F.col("venname")
    )
)

final_dtls_insert.write.insertInto(tgt_dtls)


# #### Step-8 -- Historical Consumption
# This step Calculates how much of each RSCXD part has been used in the past from SAP demand history.
# This shows part criticality based on real usage.

# In[62]:


partcons = (
    rscxd_df.alias("a")
    .join(dmd_df.alias("b"), F.col("a.nha") == F.col("b.hostpartid"))
    .filter(F.col("b.ordertype").isin("TA","ZCON","ZO09","ZO04","ZSD","ZSO","ZFO","ZUP"))
    .groupBy("a.nha")
    .agg(F.sum("b.historyamount").cast("int").alias("sprs_cons"))
)


# #### Step-9 -- Future Demand (SPRS_DMD)
# - It Calculates how much of each RSCXD part is needed in the next 6 months based on open sales orders.
# This represents forward-looking demand.
# 

# In[63]:


partdmd = (
    spark.table(tgt_dtls).alias("a")
    .join(so_df.alias("b"), F.col("a.nha") == F.col("b.hostpartid"))
    .filter(F.col("b.auart").isin("TA","ZCON","ZO09","ZO04","ZSD","ZSO","ZFO","ZUP"))
    .filter(
        (F.col("b.salesorderdtdue") >= dt1wk) &
        (F.col("b.salesorderdtdue") <= dt26wk)
    )
    .filter(F.col("a.type").isin("R","S","D","X"))
    .groupBy("a.nha")
    .agg(F.sum("b.openqty").cast("int").alias("sprs_dmd"))
)


# #### Step-10 -- Open Purchase Order Quantity
# - This Step Calculates how many units are already on order from suppliers for each RSCXD part and what supply is coming in.
# 

# In[64]:


openpo = (
    po_df.alias("p")
    .join(ekpo_df.alias("e"), ["ebeln", "ebelp"])
    .join(spark.table(tgt_dtls).alias("d"), F.col("p.matnr") == F.col("d.nha"))
    .filter(~F.right(F.col("p.due_dt"), F.lit(5)).isin("04-01", "12-31"))
    .filter(F.col("p.status") != "INACT")
    .filter(F.trim(F.col("p.matnr")) != "")
    .filter(F.trim(F.col("p.loekz")) == "")
    .filter(F.col("p.aussl") != "U3")
    .filter(F.col("p.bsart") != "UB")
    .filter(F.col("p.elikz") != "X")
    .filter(~F.col("e.pstyp").isin("7", "9"))
    .groupBy("d.cmpprtno", "p.matnr")
    .agg(F.sum(F.col("p.menge") - F.col("p.wemng")).cast("int").alias("open_po_qty"))
)


# #### Step-11 -- LAMQOH & Restricted Stock
# - This Step calculates how much inventory is available and how much is restricted for each RSCXD part.
# - This shows what is physically in stock right now.

# In[65]:


lamqoh = (
    mard_df.alias("m")
    .join(spark.table(tgt_dtls).alias("d"), F.col("m.matnr") == F.col("d.nha"))
    .groupBy("d.cmpprtno", "m.matnr")
    .agg(
        F.sum(F.col("m.labst") + F.col("m.klabs")).cast("int").alias("lamqoh"),
        F.sum(
            F.col("m.insme") +
            F.col("m.umlme") +
            F.col("m.einme") +
            F.col("m.speme")
        ).cast("int").alias("restricted_all_stock")
    )
)


# #### Step-12 -- Build OMAT_RSCXD_DMD_CONS
# - It Combines usage, future demand, open POs, and inventory into one table so planners can see
# supply vs demand risk per RSCXD part.

# In[66]:


spark.sql(f"DELETE FROM {tgt_dmd_cons}")


# In[67]:


final_dmd_cons = (
    spark.table(tgt_dtls).alias("a")
    .join(partcons.alias("b"), "nha", "left")
    .join(partdmd.alias("c"), "nha", "left")
    .join(
        openpo.alias("d"),
        (F.col("a.nha") == F.col("d.matnr")) &
        (F.col("a.cmpprtno") == F.col("d.cmpprtno")),
        "left"
    )
    .join(
        lamqoh.alias("e"),
        (F.col("a.nha") == F.col("e.matnr")) &
        (F.col("a.cmpprtno") == F.col("e.cmpprtno")),
        "left"
    )
    .select(
        F.col("a.cmpprtno"),
        F.col("a.nha"),
        F.col("b.sprs_cons"),
        F.col("c.sprs_dmd"),
        F.col("d.open_po_qty"),
        F.current_date().alias("last_modified_on"),
        F.col("e.lamqoh"),
        F.col("e.restricted_all_stock")
    )
)


# In[68]:


final_dmd_cons_insert = (
    final_dmd_cons
    .select(
        F.col("cmpprtno"),
        F.col("lamqoh"),
        F.col("last_modified_on"),
        F.col("nha"),
        F.col("open_po_qty"),
        F.col("restricted_all_stock"),
        F.col("sprs_cons"),
        F.col("sprs_dmd")
    )
)

final_dmd_cons_insert.write.insertInto(tgt_dmd_cons)

