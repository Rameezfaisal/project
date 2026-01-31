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
#     2. **rpt_omat_part_dtls :** Provides comprehensive part-level details including status, supplier, and obsolescence indicators.
#         - Load Type : Incremental append and Update for the same NHAs
# 
#     3. **rpt_omat_log_dtls :** Logs OMAT system activities like job executions and email refresh requests for tracking and troubleshooting.
#         - Incremental append (new log entry per run); no key-based current timestamp insert.
# 

# In[1]:


from pyspark.sql import Row
from pyspark.sql import functions as F
from delta.tables import DeltaTable
 
import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants
 
from silketl import SqlDatabaseConnector, get_workspace_name, preload, load_data, postload,json,current_workspace_name, get_workspaceid_by_name
 


# In[ ]:


project_code = 'eng_omat'
job_id = '123'
task_id = '14703'
env = 'FTR'
 


# In[ ]:


# sql_db = SqlDatabaseConnector(env)
# sql_query = f"select * from config.vw_transform_egress_ingress_mapping where transform_egress_id = ? and project_code = ?"
# object_df = sql_db.execute(sql_query,(task_id,project_code))
# display(object_df)
 


# In[2]:


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
 


# #### **Table Creation if Not Exist**
# 1. rpt_omat_buynha_suppliers_plants
# 2. rpt_omat_dmd_consumption_dtls
# 3. rpt_omat_part_dtls
# 4. rpt_omat_obpr_info_dtls
# 5. rpt_omat_makenha_dmd_consumption_dtls
# 6. rpt_omat_rscxd_dmd_cons
# 7. rpt_omat_rscxd_dtls
# 8. rpt_omat_odrbom_cons_dtls
# 9. rpt_omat_odrbom_dmd_dtls
# 

# In[3]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql

# -- Set the schema name once

# SET schema_name = eng;


# In[4]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql

# -- Table Creation

# CREATE TABLE IF NOT EXISTS ${schema_name}.rpt_omat_buynha_suppliers_plants (
#   nha                      STRING,
#   omat_selected_plant      STRING,
#   omat_selected_supplier   STRING,
#   omat_selected_suppliercd STRING,
#   suppcd_1050              STRING,
#   suppcd_1060              STRING,
#   suppcd_1090              STRING,
#   suppcd_1900              STRING,
#   suppcd_2000              STRING,
#   suppcd_3120              STRING,
#   suppcd_po1               STRING,
#   suppcd_po2               STRING,
#   suppcd_pr                STRING,
#   suppname_1050            STRING,
#   suppname_1060            STRING,
#   suppname_1090            STRING,
#   suppname_1900            STRING,
#   suppname_2000            STRING,
#   suppname_3120            STRING,
#   suppname_po1             STRING,
#   suppname_po2             STRING,
#   suppname_pr              STRING
# )
# USING DELTA;



# CREATE TABLE IF NOT EXISTS ${schema_name}.rpt_omat_dmd_consumption_dtls (
#   cmpprtno                STRING,
#   lamqoh                  INT,
#   last_modified_on        DATE,
#   mfg_12mnths_consumption INT,
#   mfg_3yrs_consumption    INT,
#   mfg_5yrs_consumption    INT,
#   mfg_dmd                 INT,
#   nha                     STRING,
#   open_po_qty             INT,
#   restricted_all_stock    INT,
#   sprs_3yrs_consumption   INT,
#   sprs_5yrs_consumption   INT,
#   sprs_dmd                INT
# )
# USING DELTA;



# CREATE TABLE IF NOT EXISTS ${schema_name}.rpt_omat_part_dtls (
#   ce           STRING,
#   cmpprtno     STRING,
#   commodity    STRING,
#   critical     STRING,
#   curr_cost    DOUBLE,
#   description  STRING,
#   lead_time    INT,
#   mrp_supplier INT,
#   nha          STRING,
#   po_supp_code STRING,
#   po_supp_name STRING,
#   sbm          STRING,
#   smg_group    STRING,
#   smg_head     STRING,
#   std_cost     DOUBLE,
#   vencode      STRING,
#   venname      STRING
# )
# USING DELTA;



# CREATE TABLE IF NOT EXISTS ${schema_name}.rpt_omat_obpr_info_dtls (
#   active_co                   STRING,
#   active_co_nha               STRING,
#   active_co_status            STRING,
#   active_co_status_nha        STRING,
#   bu                          STRING,
#   ce                          STRING,
#   co_effected_bu              STRING,
#   co_effected_bu_nha          STRING,
#   critical                    STRING,
#   description                 STRING,
#   deviation_expiry            STRING,
#   deviation_name              STRING,
#   deviation_state             STRING,
#   deviation_exists            STRING,
#   disposition                 STRING,
#   end_items                   STRING,
#   epl_trigger_lead_time       STRING,
#   is_last_time_buy_available  STRING,
#   ltb_available_date          DATE,
#   ltb_decision                STRING,
#   mbu_flag                    STRING,
#   mpn                         STRING,
#   obprno                      STRING NOT NULL,
#   obprtno                     STRING NOT NULL,
#   open_pos                    INT,
#   primary_product_affected    STRING,
#   primary_product_pgpm        STRING,
#   priority                    STRING,
#   product_group               STRING,
#   pr_originator               STRING,
#   pr_planned_completion_date  DATE,
#   pr_submitted_date           DATE,
#   requestor_org               STRING,
#   reviewer                    STRING,
#   review_start_date           DATE,
#   secondary_products_affected STRING,
#   solution_owner              STRING,
#   state                       STRING,
#   status                      STRING,
#   supplier_code               STRING,
#   supplier_date_to_zero       DATE,
#   supplier_name               STRING,
#   vendor_code                 STRING,
#   vendor_name                 STRING
# )
# USING DELTA;




# CREATE TABLE IF NOT EXISTS ${schema_name}.rpt_omat_makenha_dmd_consumption_dtls (
#   cmpprtno                STRING,
#   last_modified_on        DATE,
#   mfg_12mnths_consumption INT,
#   mfg_5yrs_consumption    INT,
#   mfg_dmd                 INT,
#   nha                     STRING,
#   sprs_5yrs_consumption   INT,
#   sprs_dmd                INT
# )
# USING DELTA;



# CREATE TABLE IF NOT EXISTS ${schema_name}.rpt_omat_rscxd_dmd_cons (
#   cmpprtno             STRING,
#   lamqoh               INT,
#   last_modified_on     DATE,
#   nha                  STRING,
#   open_po_qty          INT,
#   restricted_all_stock INT,
#   sprs_cons            INT,
#   sprs_dmd             INT
# )
# USING DELTA;



# CREATE TABLE IF NOT EXISTS ${schema_name}.rpt_omat_rscxd_dtls (
#   ce               STRING,
#   cmpprtno         STRING,
#   cmpqpa           STRING,
#   commodity        STRING,
#   critical         STRING,
#   curr_cost        STRING,
#   dash             STRING,
#   description      STRING,
#   lead_time        INT,
#   matkl            STRING,
#   mstae            STRING,
#   nha              STRING,
#   po_supplier      STRING,
#   po_supplier_code STRING,
#   sbm              STRING,
#   smg_group        STRING,
#   smg_head         STRING,
#   std_cost         STRING,
#   type             STRING,
#   vencode          STRING,
#   venname          STRING
# )
# USING DELTA;



# CREATE TABLE IF NOT EXISTS ${schema_name}.rpt_omat_odrbom_cons_dtls (
#   cons_date DATE,
#   menge     DECIMAL(13,3) NOT NULL,
#   nha       STRING,
#   odrbom    STRING
# )
# USING DELTA;



# CREATE TABLE IF NOT EXISTS ${schema_name}.rpt_omat_odrbom_dmd_dtls (
#   dmd1     DECIMAL(13,3),
#   dmd10    DECIMAL(13,3),
#   dmd11    DECIMAL(13,3),
#   dmd12    DECIMAL(13,3),
#   dmd13    DECIMAL(13,3),
#   dmd14    DECIMAL(13,3),
#   dmd15    DECIMAL(13,3),
#   dmd16    DECIMAL(13,3),
#   dmd17    DECIMAL(13,3),
#   dmd18    DECIMAL(13,3),
#   dmd19    DECIMAL(13,3),
#   dmd2     DECIMAL(13,3),
#   dmd20    DECIMAL(13,3),
#   dmd21    DECIMAL(13,3),
#   dmd22    DECIMAL(13,3),
#   dmd23    DECIMAL(13,3),
#   dmd24    DECIMAL(13,3),
#   dmd25    DECIMAL(13,3),
#   dmd26    DECIMAL(13,3),
#   dmd3     DECIMAL(13,3),
#   dmd4     DECIMAL(13,3),
#   dmd5     DECIMAL(13,3),
#   dmd6     DECIMAL(13,3),
#   dmd7     DECIMAL(13,3),
#   dmd8     DECIMAL(13,3),
#   dmd9     DECIMAL(13,3),
#   dmdpd    DECIMAL(13,3),
#   nha      STRING,
#   odrbom   STRING,
#   rowtype  STRING
# )
# USING DELTA;


# #### Step-1 -- Imports

# In[5]:


from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ##### Helper Function for Union

# In[6]:


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

# In[7]:


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
tgt_dmd = "eng.rpt_omat_dmd_consumption_dtls"
tgt_part = "eng.rpt_omat_part_dtls"
tgt_log = "eng.rpt_omat_log_dtls"

# ===== Control =====
today = F.current_date()



# #### Step-3 -- Reading Source tables

# In[8]:


mseg      = read_union_fast("mseg")
mkpf      = read_union_fast("mkpf")
makt      = read_union_fast("makt")
forecast  = read_union_fast("zmmforecast")
mard      = read_union_fast("mard")
po        = read_union_fast("zpo_hstry")
ekpo      = read_union_fast("ekpo")

spares = spark.table("wsf_silk_glb_dt_qa.lhg_glb.eng.rpt_csbg_spares_deliveries")
suppliers_by_plant = spark.table("lhs_glb.omat_test.rpt_omat_buynha_suppliers_plants")

nha = (
    spark.table("lhs_glb.omat_test.rpt_omat_obpn_buy_nha_dtls")
         .select("cmpprtno", "nha", "isprocessed")
         .filter(F.col("isprocessed") == "N")
         .distinct()
)



# #### Step-4 -- Creating NHALIST Temp table

# In[9]:


nhalist = nha.select("cmpprtno", "nha").distinct()


# #### Step-5 -- Calculating Manufacturing 3-year Consumption

# In[10]:


mfg3 = (
    mseg.join(mkpf, "mblnr")
        .join(nhalist, mseg.matnr == nhalist.nha)
        .filter(
            (mseg.bwart.isin("261","262")) &
            (mkpf.bldat > F.date_sub(today, 365*3))
        )
        .groupBy(nhalist.cmpprtno.alias("cmpprtno_cd"), mseg.matnr.alias("nha_cd"))
        .agg(F.sum(F.when(mseg.shkzg=="S",-mseg.menge).otherwise(mseg.menge)).alias("mfg_3yrs_consumption"))
)


# #### Step-6 -- Calculating Manufacturing Demand (26 weeks)

# In[11]:


dmd_cols = ["dmdpd"] + [f"dmd{i}" for i in range(1,27)]

mfgdmd = (
    forecast
    .join(nhalist, forecast.matnr == nhalist.nha, "inner")
    .filter(
        (forecast.rowtype == "06") &
        (forecast.werks == "COMB")
    )
    .groupBy(
        nhalist.cmpprtno.alias("cmpprtno_cd"),
        forecast.matnr.alias("nha_cd")
    )
    .agg(
        sum(F.sum(F.col(c)) for c in dmd_cols).alias("mfg_dmd")
    )
)


# #### Step-7 -- Spares Demand (26 weeks)

# In[12]:


dmd_cols = ["dmdpd"] + [f"dmd{i}" for i in range(1,27)]

sprsdmd = (
    forecast
    .join(nhalist, forecast.matnr == nhalist.nha, "inner")
    .filter(
        (forecast.rowtype == "07") &
        (forecast.werks == "COMB")
    )
    .groupBy(
        nhalist.cmpprtno.alias("cmpprtno_cd"),
        forecast.matnr.alias("nha_cd")
    )
    .agg(
        sum(F.sum(F.col(c)) for c in dmd_cols).alias("sprs_dmd")
    )
)


# #### Step-8 Spares 3-Year Consumption
# 
# - Calculates total spares consumption over the last 3 years from
# CSBG_SPARES_DELIVERIES, filtered by order type, source doc, and GI date, grouped by CMPPRTNO + NHA.

# In[13]:


sprs3 = (
    spares
    .join(nhalist, spares.material == nhalist.nha, "inner")
    .filter(
        (spares.source_doc != "STO") &
        (spares.order_type.isin("TA","ZCON","ZO09","ZO04","ZSD","ZSO","ZFO","ZUP")) &
        (spares.actual_gi > F.date_sub(F.current_date(), 365*3))
    )
    .groupBy(
        nhalist.cmpprtno.alias("cmpprtno_cd"),
        spares.material.alias("nha_cd")
    )
    .agg(
        F.sum(spares.qty_shipped).alias("sprs_3yrs_consumption")
    )
)


# #### Step-9 -- LAM Quantity On Hand & Restricted Stock
# Calculates LAMQOH and RESTRICTED_ALL_STOCK from "ecc.mard", excluding storage location V014, grouped by CMPPRTNO + NHA.

# In[14]:


lamqoh = (
    mard
    .join(nhalist, mard.matnr == nhalist.nha, "inner")
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

# In[15]:


openpo = (
    po
    .join(ekpo, ["ebeln", "ebelp"], "inner")
    .join(nhalist, po.matnr == nhalist.nha, "inner")
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

# In[16]:


base = (
    nhalist.alias("a")
    .join(mfg3.alias("b"), nhalist.nha == mfg3.nha_cd, "left")
    .join(mfgdmd.alias("c"), nhalist.nha == mfgdmd.nha_cd, "left")
    .join(sprsdmd.alias("d"), nhalist.nha == sprsdmd.nha_cd, "left")
    .join(sprs3.alias("e"), nhalist.nha == sprs3.nha_cd, "left")
    .join(lamqoh.alias("g"), nhalist.nha == lamqoh.nha_cd, "left")
    .join(openpo.alias("h"), nhalist.nha == openpo.nha_cd, "left")
    .select(
        nhalist.cmpprtno.alias("cmpprtno"),
        nhalist.nha.alias("nha"),
        F.col("b.mfg_3yrs_consumption"),
        F.col("c.mfg_dmd"),
        F.col("d.sprs_dmd"),
        F.col("e.sprs_3yrs_consumption"),
        F.col("g.lamqoh"),
        F.col("g.restricted_all_stock"),
        F.col("h.open_po_qty")
    )
)

part1 = base.filter(F.col("cmpprtno") == F.col("nha"))

part2 = base.filter(
    (F.coalesce("mfg_3yrs_consumption",F.lit(0)) > 0) |
    (F.coalesce("mfg_dmd",F.lit(0)) > 0) |
    (F.coalesce("sprs_dmd",F.lit(0)) > 0) |
    (F.coalesce("sprs_3yrs_consumption",F.lit(0)) > 0) |
    (F.coalesce("open_po_qty",F.lit(0)) > 0)
)

final = part1.unionByName(part2).withColumn("last_modified_on", F.current_date())

final.createOrReplaceTempView("final_dmd")


spark.sql(f"""
INSERT INTO {tgt_dmd}
(cmpprtno,nha,mfg_3yrs_consumption,mfg_dmd,sprs_dmd,
 sprs_3yrs_consumption,lamqoh,restricted_all_stock,open_po_qty,last_modified_on)
SELECT cmpprtno,nha,mfg_3yrs_consumption,mfg_dmd,sprs_dmd,
       sprs_3yrs_consumption,lamqoh,restricted_all_stock,open_po_qty,last_modified_on
FROM final_dmd
""")


# #### Step-12 -- Logging (SAP equivalent)
# 
# - This records: how many OB parts were processed, how many NHAs were processed, when the procedure ran.

# In[17]:


obprt_cnt = nhalist.select("cmpprtno").distinct().count()
nha_cnt = nhalist.select("nha").distinct().count()

spark.sql(f"""
INSERT INTO {tgt_log}
(program_name, obpr_cnt, obprtno_cnt, obprtno_nha_cnt, executed_on)
VALUES
('nb_omat_insert_dmd_cons_po_1', 1, {obprt_cnt}, {nha_cnt}, current_timestamp())
""")

