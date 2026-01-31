#!/usr/bin/env python
# coding: utf-8

# ## nb_omat_insert_dmd_cons_po_2
# 
# New notebook

# ##### **Pyspark_Notebook :** nb_omat_insert_dmd_cons_po_2
# ##### 📌 **Author:** Rameez Ansari
# ##### 📅 **Last Updated:** 2025-12-31
# ###### 🔢 **Notebook Version:** 1.0
# ###### 🔄 **Change Log:** v1.0 | 2025-12-31 |
# ###### Rameez Ansari   → Work in Progress
# 📖 **Notebook Overview:**
# 
# It builds the omat_part_dtls by combining demand-driven NHAs with their MRP-selected suppliers, costs and buisness classifications so OMAT can analyze sourcing, risk and obsolescence.
# 
# **Includes:**    
#     1. **rpt_omat_part_dtls :**
# - Provides comprehensive part-level details including status, supplier, and obsolescence indicators.
#     - Load Type: Insert

# In[1]:


from pyspark.sql import Row
from pyspark.sql import functions as F
from delta.tables import DeltaTable
 
import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants
 
from silketl import SqlDatabaseConnector, get_workspace_name, preload, load_data, postload,json,current_workspace_name, get_workspaceid_by_name
 


# In[2]:


project_code = 'eng_omat'
job_id = '123'
task_id = '14704'
env = 'FTR'
 


# In[ ]:


# sql_db = SqlDatabaseConnector(env)
# sql_query = f"select * from config.vw_transform_egress_ingress_mapping where transform_egress_id = ? and project_code = ?"
# object_df = sql_db.execute(sql_query,(task_id,project_code))
# display(object_df)
 


# In[ ]:


# tgt_system_properties = json.loads(object_df['TGT_SYSTEM_PROPERTIES'][0])
# # tgt_dt_workspacename = get_workspace_name(workspace_name = tgt_system_properties['workspaceName'])
# tgt_dt_workspacename = tgt_system_properties['workspaceName']
# src_dt_workspace_id = get_workspaceid_by_name(tgt_dt_workspacename)
# print(tgt_dt_workspacename)
# print(src_dt_workspace_id)
 


# In[ ]:


# schema_name = str(object_df['TGT_SCHEMA_NAME'][0])
# delta_table_name = str(object_df['TGT_TABLE_NAME'][0])
 
workspace = current_workspace_name
s_workspace = current_workspace_name
t_workspace = workspace.replace("_da_", "_dt_")
 
# cat_mdm = workspace.replace("_da_", "_dt_")
 
 
t_lakehouse = "lhg_glb.eng"
s_lakehouse = "lhg_glb.eng"
 
 
lh_sch_mdm = "lhg_glb.mdm"
lh_sch_ecc = "lhs_glb.ecc"
lh_sch_s4h = "lhs_glb.s4h"
lh_sch_eng = "lhg_glb.eng"
 
 
 
# lakehousename = str(object_df['TGT_SYSTEM_CODE'][0])
 
# target_path = (
# f"abfss://{dt_workspace}@onelake.dfs.fabric.microsoft.com/"
# f"{lakehousename}.Lakehouse/Tables/{schema_name}/{delta_table_name}")
 
# stg_target_path = (
# f"abfss://{dt_workspace}@onelake.dfs.fabric.microsoft.com/"
# f"{lakehousename}.Lakehouse/Tables/{schema_name}/{delta_table_name}_stg")
 
# fqn_target_table = f"{dt_workspace}.{lakehousename}.{schema_name}.{delta_table_name}"
 


# #### Step-1 -- Imports

# In[3]:


from pyspark.sql import functions as F


# ##### Helper Union Function

# In[4]:


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

# In[5]:


# Initialize union mapping
paths = {
    "ecc": {
        "makt":           "wsf_silk_glb_da_dev.lhs_glb.ecc.makt",
        "marc":           "wsf_silk_glb_da_dev.lhs_glb.ecc.marc",
        "mbew":           "wsf_silk_glb_da_dev.lhs_glb.ecc.mbew",
        "ausp":           "wsf_silk_glb_da_dev.lhs_glb.ecc.ausp",
        "zsupplier_xref": "wsf_silk_glb_da_dev.lhs_glb.ecc.zsupplier_xref",
        "zsupp_code_mstr":"wsf_silk_glb_da_dev.lhs_glb.ecc.zsupp_code_mstr",
    },
    "s4h": {
        "makt":           "wsf_silk_glb_da_dev.lhs_glb.s4h.makt",
        "marc":           "wsf_silk_glb_da_dev.lhs_glb.s4h.marc",
        "mbew":           "wsf_silk_glb_da_dev.lhs_glb.s4h.mbew",
        "ausp":           "wsf_silk_glb_da_dev.lhs_glb.s4h.ausp",
        "zsupplier_xref": "wsf_silk_glb_da_dev.lhs_glb.s4h.zsupplier_xref",
        "zsupp_code_mstr":"wsf_silk_glb_da_dev.lhs_glb.s4h.zsupp_code_mstr",
    }
}


# === Target ===
tgt_part  = "eng.rpt_omat_part_dtls"


# #### Step-3 -- Reading Source tables

# In[6]:


# ---- Direct non-ECC/S4H tables ----
dmd = spark.table("lhs_glb.omat_test.rpt_omat_dmd_consumption_dtls")
sup = spark.table("lhs_glb.omat_test.rpt_omat_buynha_suppliers_plants")

# ---- Union-managed ECC + S4H tables ----
makt  = read_union_fast("makt")
marc  = read_union_fast("marc")
mbew  = read_union_fast("mbew")
ausp  = read_union_fast("ausp")
xref  = read_union_fast("zsupplier_xref")
code  = read_union_fast("zsupp_code_mstr")


# #### Step-4 -- AUSP attributes (Critical & CE)
# - This cell extracts two material characteristics from the SAP AUSP table and prepares them for joining to NHA parts.

# In[7]:


critical = (
    ausp
    .filter(ausp.atinn == "0000006193")
    .select(ausp.objek.alias("nha_cd"), ausp.atwrt.alias("critical"))
)

ce = (
    ausp
    .filter((ausp.atinn == "0000015276") & (ausp.atwrt == "Y"))
    .select(ausp.objek.alias("nha_cd"), ausp.atwrt.alias("ce"))
)


# #### Step-5 -- Base join graph
# This cell joins together all master and reference data needed to build OMAT part records.

# In[8]:


base = (
    dmd.alias("a")
    .join(sup.alias("b"), dmd.nha == sup.nha, "left")
    .join(makt, dmd.nha == makt.matnr, "left")
    .join(marc, (dmd.nha == marc.matnr) & (sup.omat_selected_plant == marc.werks), "left")
    .join(mbew, (dmd.nha == mbew.matnr) & (sup.omat_selected_plant == mbew.bwkey), "left")
    .join(critical, dmd.nha == critical.nha_cd, "left")
    .join(ce, dmd.nha == ce.nha_cd, "left")
    .join(xref, sup.omat_selected_suppliercd == xref.lifnr, "left")
    .join(code.alias("g"), xref.zcommcod == F.col("g.zcode"), "left")
    .join(code.alias("h"), xref.zgrpsmg == F.col("h.zcode"), "left")
    .join(code.alias("i"), xref.zsmgcd == F.col("i.zcode"), "left")
    .join(code.alias("j"), xref.zprimmcd == F.col("j.zcode"), "left")
)


# #### Step-6 -- Identify MRP-preferred parts
# - This step checks if the part has at least one valid MRP-approved supplier in the LAM manufacturing plants.
# If yes → it is an MRP-managed part
# If no → it is non-MRP (riskier, manual sourcing)
# 
# - Tell OMAT which parts are officially supported by the MRP supply chain.

# In[9]:


mrp_flag = (
    (F.length(sup.suppcd_2000) > 0) |
    (F.length(sup.suppcd_1900) > 0) |
    (F.length(sup.suppcd_3120) > 0) |
    (F.length(sup.suppcd_1050) > 0) |
    (F.length(sup.suppcd_1060) > 0) |
    (F.length(sup.suppcd_1090) > 0)
)


# #### Step-7 -- Final Projection
# 
# Convert all raw SAP data into a clean OMAT-ready part master record.

# In[10]:


final_part = base.select(
    dmd.cmpprtno,
    dmd.nha,
    makt.maktx.alias("description"),
    sup.omat_selected_suppliercd.alias("vencode"),
    sup.omat_selected_supplier.alias("venname"),
    marc.plifz.cast("int").alias("lead_time"),
    mbew.stprs.cast("double").alias("std_cost"),
    mbew.zplp2.cast("double").alias("curr_cost"),
    critical.critical,
    ce.ce,
    F.col("g.zdesc").alias("commodity"),
    F.col("h.zdesc").alias("smg_group"),
    F.col("i.zdesc").alias("smg_head"),
    F.col("j.zdesc").alias("sbm"),
    F.when(mrp_flag, F.lit(1)).otherwise(F.lit(0)).alias("mrp_supplier"),
    sup.suppcd_po1.alias("po_supp_code"),
    sup.suppname_po1.alias("po_supp_name")
)


# #### Step-8 -- Insert to "rpt_omat_part_dtls"

# In[11]:


final_part.createOrReplaceTempView("final_part")

spark.sql(f"""
INSERT INTO {tgt_part}
(ce,cmpprtno,commodity,critical,curr_cost,description,lead_time,mrp_supplier,
 nha,po_supp_code,po_supp_name,sbm,smg_group,smg_head,std_cost,vencode,venname)
SELECT
 ce,cmpprtno,commodity,critical,curr_cost,description,lead_time,mrp_supplier,
 nha,po_supp_code,po_supp_name,sbm,smg_group,smg_head,std_cost,vencode,venname
FROM final_part
""")

