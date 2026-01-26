#!/usr/bin/env python
# coding: utf-8

# ## nb_omat_insert_rs_supplier
# 
# New notebook

# ##### **Pyspark_Notebook :** nb_omat_insert_rs_supplier
# ##### 📌 **Author:** Rameez Ansari
# ##### 📅 **Last Updated:** 2025-12-28
# ###### 🔢 **Notebook Version:** 1.0
# ###### 🔄 **Change Log:** v1.0 | 2025-12-28 |
# ###### Rameez Ansari   → Work in Progress
# 📖 **Notebook Overview:**
# 
# 🔹 This Notebook identify, prioritize, and store supplier information for all R and S type NHA (Next Higher Assembly) parts used in the OMAT obsolescence program.
# 
# 
# **It Includes:**    
# 1. **rpt_omat_buynha_suppliers_plants:** 
#     - Details suppliers and associated plants for Buy NHAs, supporting obsolescence management and inventory updates
#         - Load Type: Full refresh
# 
# 2. **rpt_omat_log_dtls:** 
#     - Logs OMAT system activities like job executions and email refresh requests for tracking and troubleshooting.
#         - Load Type: Incremental append.

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
from pyspark.sql.window import Window


# ##### Helper Union Function

# In[5]:


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


# #### Step-2 -- Parameterize path (Source/Target)

# In[6]:


# ---- Source tables ----


paths = {
    "ecc": {
        "zpo_hstry": "wsf_silk_glb_da_dev.lhs_glb.ecc.zpo_hstry",
        "ekpo":      "wsf_silk_glb_da_dev.lhs_glb.ecc.ekpo",
        "lfa1":      "wsf_silk_glb_da_dev.lhs_glb.ecc.lfa1",
    },
    "s4h": {
        "zpo_hstry": "wsf_silk_glb_da_dev.lhs_glb.s4h.zpo_hstry",   # update if needed
        "ekpo":      "wsf_silk_glb_da_dev.lhs_glb.s4h.ekpo",
        "lfa1":      "wsf_silk_glb_da_dev.lhs_glb.s4h.lfa1",
    }
}



src_rscxd_dtls          = "lhs_glb.omat_test.rpt_omat_rscxd_dtls"
src_rscxd_dmd_cons      = "lhs_glb.omat_test.rpt_omat_rscxd_dmd_cons"
src_ml_part             = "wsf_silk_glb_dt_qa.lhg_glb.mdm.mlv_material_plant_supplier"
src_problem_part        = "lhs_glb.eng.stg_omat_iplm_problem_report_part"

# ---- Targets ----
tgt_supplier_plants = "eng.rpt_omat_buynha_suppliers_plants"
tgt_rscxd_dtls      = "eng.rpt_omat_rscxd_dtls"



# #### Step-3 -- Reading Source tables

# In[7]:


rscxd_dtls   = spark.read.table(src_rscxd_dtls)
rscxd_cons   = spark.read.table(src_rscxd_dmd_cons)
ml_part     = spark.read.table(src_ml_part)
problem_pt  = spark.read.table(src_problem_part)

po_hstry    = read_union_fast("zpo_hstry")
ekpo        = read_union_fast("ekpo")
lfa1        = read_union_fast("lfa1")


# #### Step-4 -- Creating NHALIST Temp table

# In[8]:


nhalist = (
    rscxd_dtls
    .select(
        F.col("nha").alias("nha_cd"),
        F.col("type").alias("type_cd")
    )
    .filter(F.col("type_cd").isin("R", "S"))
    .select("nha_cd")
    .distinct()
)


# #### Step-5 — SUPPLINFO (Master Supplier Mapping)
# - This step attaches GLIS master supplier information to every R/S NHA.
# - It checks Does this NHA already have a known buy supplier in the "mlv_material_plant_supplier"

# In[9]:


supplinfo = (
    nhalist
    .join(
        ml_part.select(
            F.col("material").alias("nha_cd"),
            F.col("supplier_code").alias("supplierid_cd"),
            F.col("supplier_name").alias("supplier_nm")
        ),
        "nha_cd",
        "left"
    )
)


# #### Step-6 — POSUPP (Open PO Supplier Discovery)
# - This step finds all valid open purchase order suppliers for each R/S NHA from ECC and aggregates them.
# - It checks Which suppliers are currently supplying this NHA on open PO.

# In[10]:


po_base = (
    nhalist
    .join(rscxd_cons.select("nha", "open_po_qty"), nhalist.nha_cd == rscxd_cons.nha)
    .join(po_hstry, po_hstry.matnr == nhalist.nha_cd)
    .join(ekpo, (po_hstry.ebeln == ekpo.ebeln) & (po_hstry.ebelp == ekpo.ebelp))
    .join(lfa1, lfa1.lifnr == po_hstry.lifnr)
    .filter(
        (rscxd_cons.open_po_qty > 0) &
        (~F.substring(po_hstry.due_dt, -5, 5).isin("04-01", "12-31")) &
        (po_hstry.status != "INACT") &
        (F.trim(po_hstry.matnr) != "") &
        (F.trim(po_hstry.loekz) == "") &
        (po_hstry.discrd != "X") &
        (po_hstry.aussl != "U3") &
        (po_hstry.bsart != "UB") &
        (po_hstry.elikz != "X") &
        (~ekpo.pstyp.isin("7", "9"))
    )
    .select(
        nhalist.nha_cd.alias("nha_cd"),
        po_hstry.lifnr.alias("lifnr_cd"),
        lfa1.name1.alias("name1_cd")
    )
    .distinct()
)

posupp = (
    po_base
    .groupBy("nha_cd")
    .agg(
        F.concat_ws(",", F.collect_list("lifnr_cd")).alias("posuppcode_cd"),
        F.concat_ws(",", F.collect_list("name1_cd")).alias("posuppname_cd")
    )
)


# #### Step-7 — OPENPOSUPPL (Derive PO1 & PO2 suppliers)
# - From the comma-separated PO supplier list, this step extracts:
# First open PO supplier (PO1),
# Second open PO supplier (PO2).
# - It then looks up their supplier names from the vendor master (LFA1).

# In[11]:


openposuppl = (
    posupp
    .withColumn(
        "suppcd_po1",
        F.when(
            F.instr("posuppcode_cd", ",") > 0,
            F.substring_index("posuppcode_cd", ",", 1)
        ).otherwise(F.substring("posuppcode_cd", 1, 10))
    )
    .withColumn(
        "suppcd_po2",
        F.when(
            (F.instr("posuppcode_cd", ",") > 0) & (F.length("posuppcode_cd") <= 21),
            F.substring_index("posuppcode_cd", ",", -1)
        ).otherwise(F.substring("posuppcode_cd", 12, 10))
    )
    .join(
        lfa1.select(
            F.col("lifnr").alias("suppcd_po1"),
            F.col("name1").alias("suppname_po1")
        ),
        "suppcd_po1",
        "left"
    )
    .join(
        lfa1.select(
            F.col("lifnr").alias("suppcd_po2"),
            F.col("name1").alias("suppname_po2")
        ),
        "suppcd_po2",
        "left"
    )
)


# #### Step-8 — OBPRSUPPL (IPLM Problem-Report Suppliers)
# - This step finds suppliers coming from IPLM Obsolescence Problem Reports.
# - It checks if this part is obsolete and under investigation and which supplier is responsible.

# In[12]:


obpr = (
    nhalist
    .join(problem_pt, problem_pt.part_name == nhalist.nha_cd)
    .filter(
        (problem_pt.part_name != "NA") &
        (F.upper(problem_pt.state) != "CLOSED") &
        (
            F.upper(problem_pt.state).isin("CONFIRMED", "IN REVIEW", "IN WORK") |
            F.upper(problem_pt.disposition).isin("CONFIRMED", "DEFER")
        ) &
        (F.upper(problem_pt.reason) == "OBSOLETE COMPONENT")
    )
    .withColumn("suppcd_pr", F.lpad(problem_pt.supplier_code, 10, "0"))
    .join(
        lfa1.select(
            F.col("lifnr").alias("suppcd_pr"),
            F.col("name1").alias("suppname_pr")
        ),
        "suppcd_pr",
        "left"
    )
    .select(
        nhalist.nha_cd.alias("nha_cd"),
        "suppcd_pr",
        "suppname_pr"
    )
)


# #### Step-9 — Final Supplier Selection (OMAT Logic)
# - This step combines:
# mlv_material_plant_supplier and Open PO suppliers
# IPLM problem-report suppliers
# and applies the priority logic to decide:
# 1. The OMAT selected plant
# 2. The OMAT selected supplier code
# 3. The OMAT selected supplier name

# In[13]:


final_df = (
    supplinfo
    .join(openposuppl, "nha_cd", "left")
    .join(obpr, "nha_cd", "left")
    .withColumn(
        "omat_selected_plant",
        F.when(F.length("supplierid_cd") > 0, F.lit("2000"))
         .when(F.length("suppcd_po1") == 10, F.lit("2000"))
         .when(F.length("suppcd_pr") > 0, F.lit("2000"))
    )
    .withColumn(
        "omat_selected_suppliercd",
        F.when(F.length("supplierid_cd") > 0, F.col("supplierid_cd"))
         .when(F.length("suppcd_po1") == 10, F.col("suppcd_po1"))
         .when(F.length("suppcd_po2") == 10, F.substring("suppcd_po2", 1, 10))
         .when(F.length("suppcd_pr") > 0, F.col("suppcd_pr"))
    )
    .withColumn(
        "omat_selected_supplier",
        F.when(F.length("supplierid_cd") > 0, F.col("supplier_nm"))
         .when(F.length("suppcd_po1") == 10, F.col("suppname_po1"))
         .when(F.length("suppcd_po2") == 10, F.col("suppname_po2"))
         .when(F.length("suppcd_pr") > 0, F.col("suppname_pr"))
    )
)


# #### Step-10 — Insert into rpt_omat_buynha_suppliers_plants
# - It stores:
# All supplier sources
# And the final OMAT-selected supplier per NHA.

# In[14]:


final_df.select(
    F.col("nha_cd").alias("nha"),
    "omat_selected_plant",
    "omat_selected_supplier",
    "omat_selected_suppliercd",

    F.lit(None).cast("string").alias("suppcd_1050"),
    F.lit(None).cast("string").alias("suppcd_1060"),
    F.lit(None).cast("string").alias("suppcd_1090"),
    F.lit(None).cast("string").alias("suppcd_1900"),
    F.col("supplierid_cd").alias("suppcd_2000"),
    F.lit(None).cast("string").alias("suppcd_3120"),

    F.col("suppcd_po1"),
    F.col("suppcd_po2"),
    F.col("suppcd_pr"),

    F.lit(None).cast("string").alias("suppname_1050"),
    F.lit(None).cast("string").alias("suppname_1060"),
    F.lit(None).cast("string").alias("suppname_1090"),
    F.lit(None).cast("string").alias("suppname_1900"),
    F.col("supplier_nm").alias("suppname_2000"),
    F.lit(None).cast("string").alias("suppname_3120"),

    F.col("suppname_po1"),
    F.col("suppname_po2"),
    F.col("suppname_pr")
).write.mode("append").saveAsTable(tgt_supplier_plants)


# #### Step-11 — Update rpt_omat_rscxd_dtls with selected supplier
# - It updates the main OMAT part table (rpt_omat_rscxd_details) so that each NHA now shows the final chosen supplier, supplier code, and the first open-PO supplier, ensuring all reports and users see the correct vendor information.

# In[15]:


spark.sql(f"""
MERGE INTO {tgt_rscxd_dtls} AS a
USING {tgt_supplier_plants} AS b
ON a.nha = b.nha
WHEN MATCHED THEN
  UPDATE SET
    a.vencode = b.omat_selected_suppliercd,
    a.venname = b.omat_selected_supplier,
    a.po_supplier_code = b.suppcd_po1,
    a.po_supplier = b.suppname_po1
""")

