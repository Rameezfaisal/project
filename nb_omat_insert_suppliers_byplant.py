#!/usr/bin/env python
# coding: utf-8

# ## nb_omat_insert_suppliers_byplant
# 
# New notebook

# ##### **Pyspark_Notebook :** nb_omat_insert_suppliers_byplant
# ##### 📌 **Author:** Rameez Ansari
# ##### 📅 **Last Updated:** 2025-12-24
# ###### 🔢 **Notebook Version:** 1.0
# ###### 🔄 **Change Log:** v1.0 | 2025-12-24 |
# ###### Rameez Ansari   → Work in Progress
# 📖 **Notebook Overview:**
# The purpose of this notebook is to identifies the MRP Preffered and Active suppliers in various MFG WHS in LAM. This notebook also identifies the supplier with whom we have the OPEN PO and the supplier who created the OB PR in iplm. This is to collect the inventory information from the supplier for the BUY NHA & OB Part No. 
# 
# **It Includes:**    
# 1. **rpt_omat_buynha_suppliers_plants:**  
#     - Details suppliers and associated plants for Buy NHAs, supporting obsolescence management and inventory updates
#         - Load type: Insert and Update.
# 
# 2. **rpt_omat_obpn_buy_nha_dtls:** 
#     - Contains details of Buy NHAs linked to obsolete part numbers, including supplier and demand data.
#         - Load Type: Update
# 
# 3. **rpt_omat_log_dtls:** 
#     - Logs OMAT system activities like job executions and email refresh requests for tracking and troubleshooting.
#        - Load Type: Incremental append. Logs OMAT system activities.
# 

# In[19]:


from pyspark.sql import Row
from pyspark.sql import functions as F
from delta.tables import DeltaTable
 
import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants
 
from silketl import SqlDatabaseConnector, get_workspace_name, preload, load_data, postload,json,current_workspace_name, get_workspaceid_by_name


# In[20]:


project_code = 'eng_omat'
job_id = '123'
task_id = '14704'
env = 'FTR'
 


# In[21]:


# sql_db = SqlDatabaseConnector(env)
# sql_query = f"select * from config.vw_transform_egress_ingress_mapping where transform_egress_id = ? and project_code = ?"
# object_df = sql_db.execute(sql_query,(task_id,project_code))
# display(object_df)
 


# In[22]:


# tgt_system_properties = json.loads(object_df['TGT_SYSTEM_PROPERTIES'][0])
# # tgt_dt_workspacename = get_workspace_name(workspace_name = tgt_system_properties['workspaceName'])
# tgt_dt_workspacename = tgt_system_properties['workspaceName']
# src_dt_workspace_id = get_workspaceid_by_name(tgt_dt_workspacename)
# print(tgt_dt_workspacename)
# print(src_dt_workspace_id)
 


# In[23]:


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

# In[24]:


from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.functions import broadcast
from delta.tables import DeltaTable


# ##### Helper Functionn for Union

# In[25]:


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


# #### Step-2 -- Parameterize (source/target)
# 

# In[26]:


# Initialize union mapping
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


# Targets
tgt_suppliers   = f"{t_workspace}.{t_lakehouse}.rpt_omat_buynha_suppliers_plants"
tgt_nha         = f"{t_workspace}.{t_lakehouse}.rpt_omat_obpn_buy_nha_dtls"
tgt_log         = f"{t_workspace}.{t_lakehouse}.rpt_omat_log_dtls"


# In[27]:


# # Source tables
# src_nha          = "lhs_glb.eng_test.rpt_omat_obpn_buy_nha_dtls"
# src_inforec     = "wsf_silk_glb_dt_qa.lhg_glb.eng.rpt_inforec_data_sched"
# src_eord        = "wsf_silk_glb_da_dev.lhs_glb.ecc.eord"
# src_lfa1        = "wsf_silk_glb_da_dev.lhs_glb.ecc.lfa1"
# src_po_hist     = "wsf_silk_glb_da_dev.lhs_glb.ecc.zpo_hstry"
# src_ekpo        = "wsf_silk_glb_da_dev.lhs_glb.ecc.ekpo"
# src_dmd         = "lhs_glb.eng_test.rpt_omat_dmd_consumption_dtls"
# src_pr          = "lhs_glb.eng_test.stg_omat_iplm_problem_report_part"




# #### Step-3  -- Read Sources at DataFrames

# In[28]:


# df_nha_src   = spark.read.table(src_nha)
# df_inforec  = spark.read.table(src_inforec)
# df_eord     = spark.read.table(src_eord)
# df_lfa1     = spark.read.table(src_lfa1)
# df_po_hist  = spark.read.table(src_po_hist)
# df_ekpo     = spark.read.table(src_ekpo)
# df_dmd      = spark.read.table(src_dmd)
# df_pr       = spark.read.table(src_pr)

# ---- Direct non-ECC/S4H tables ----
df_nha_src  = spark.table(f"{t_workspace}.{t_lakehouse}.rpt_omat_obpn_buy_nha_dtls")
df_dmd      = spark.table(f"{t_workspace}.{t_lakehouse}.rpt_omat_dmd_consumption_dtls")
df_pr       = spark.table(f"{t_workspace}.{t_lakehouse}.stg_omat_iplm_problem_report_part")
df_inforec  = spark.table("wsf_silk_glb_da_dev.lhs_glb.alex.zt_inforec_data_sched")

# ---- Union-managed ECC + S4H tables ----
df_eord     = read_union_fast("eord")
df_lfa1     = read_union_fast("lfa1")
df_po_hist  = read_union_fast("zpo_hstry")
df_ekpo     = read_union_fast("ekpo")


# _SYS_BIC."prd.gops.OMAT/CV_OMAT_BUYNHA_SUPPLIERS_PLANT"


# #### Step-4 -- NHALIST - Temp Table
# 
# Reads rpt_omat_obpn_buy_nha_dtls and creates the working list of unprocessed NHA + component part numbers.
# 

# In[29]:


df_nha = (
    df_nha_src
    .filter(F.col("isprocessed") == "N")
    .select(
        F.col("cmpprtno").alias("cmpprtno_cd"),
        F.col("nha").alias("nha_cd")
    )
    .distinct()
)

df_nha.createOrReplaceTempView("nha_list")


# ####  Step-5 -- Filter INFOREC preferred suppliers per plant.
# 
# - Load & filter INFOREC suppliers by plant.
# - Reads INFOREC and filters only valid, MRP-preferred suppliers per plant (1900, 2000, 3120, 1050, 1060, 1090 + fallback plants).
# - Fallback plants are the backup plants used when the plant has no direct supplier in INFOREC.

# In[30]:


def inforec_df(plant):
    return (
        df_inforec
        .filter(
            (F.col("mrp") == "1") &
            (F.col("plant") == plant) &
            (~F.col("supcat").isin("A05","A08","A15")) &
            (F.length(F.trim(F.col("supcode"))) > 0)
        )
        .select(
            F.col("material").alias("nha_cd"),
            F.lpad("supcode",10,"0").alias(f"suppcd_{plant}"),
            F.col("suppliername").alias(f"suppname_{plant}")
        )
    )

i1900 = inforec_df("1900")
i2000 = inforec_df("2000")
i3120 = inforec_df("3120")
i1050 = inforec_df("1050")
i1060 = inforec_df("1060")
i1090 = inforec_df("1090")
i1000 = inforec_df("1000")
i1010 = inforec_df("1010")
i1020 = inforec_df("1020")





# #### Step-6 -- Build initial supplier rows
# - Resolve preferred suppliers per NHA (broadcast joins)
# - Joins NHALIST to all INFOREC plant datasets using broadcast to determine the best supplier per plant.

# In[31]:


df_sup = (
    df_nha.alias("a")

    .join(broadcast(i1900).alias("b"), "nha_cd", "left")
    .join(broadcast(i2000).alias("c"), "nha_cd", "left")
    .join(broadcast(i3120).alias("d"), "nha_cd", "left")
    .join(broadcast(i1050).alias("e"), "nha_cd", "left")
    .join(broadcast(i1060).alias("f"), "nha_cd", "left")
    .join(broadcast(i1090).alias("g"), "nha_cd", "left")
    .join(broadcast(i1000).alias("h"), "nha_cd", "left")
    .join(broadcast(i1010).alias("i"), "nha_cd", "left")
    .join(broadcast(i1020).alias("j"), "nha_cd", "left")

    .select(
        "nha_cd",

        F.coalesce("b.suppcd_1900","j.suppcd_1020").alias("suppcd_1900"),
        F.coalesce("b.suppname_1900","j.suppname_1020").alias("suppname_1900"),

        F.coalesce("c.suppcd_2000","h.suppcd_1000").alias("suppcd_2000"),
        F.coalesce("c.suppname_2000","h.suppname_1000").alias("suppname_2000"),

        F.coalesce("d.suppcd_3120","i.suppcd_1010").alias("suppcd_3120"),
        F.coalesce("d.suppname_3120","i.suppname_1010").alias("suppname_3120"),

        "e.suppcd_1050","e.suppname_1050",
        "f.suppcd_1060","f.suppname_1060",
        "g.suppcd_1090","g.suppname_1090"
    )
)


# #### Step-7 -- Insert new NHA's
# 
# - Insert new NHA supplier rows into Delta
# - Writes the resolved supplier-per-plant data into rpt_omat_buynha_suppliers_plants using skew-safe Delta insert (works if table is empty or not).

# In[35]:


# Read existing target NHAs
df_existing_nha = (
    spark.read.table(tgt_suppliers)
    .select("nha")
    .distinct()
)

# Anti-join: NHAs not yet present in target
df_sup_new = (
    df_sup.alias("s")
    .join(
        df_existing_nha.alias("t"),
        F.col("s.nha_cd") == F.col("t.nha"),
        "left_anti"
    )
)

# Prepare insert dataset
df_insert = (
    df_sup_new
    .select(
        F.col("nha_cd").alias("nha"),

        "suppcd_1900","suppname_1900",
        "suppcd_2000","suppname_2000",
        "suppcd_3120","suppname_3120",
        "suppcd_1050","suppname_1050",
        "suppcd_1060","suppname_1060",
        "suppcd_1090","suppname_1090"
    )
    .filter(F.col("nha").isNotNull())
)

# Optional but good for Fabric scale
df_insert_balanced = df_insert.repartitionByRange(300, "nha")

# Insert
(
    df_insert_balanced
    .write
    .mode("append")
    .format("delta")
    .saveAsTable(tgt_suppliers)
)



# #### Step-8 -- EORD Supplier Back-fill
# 
# - Some NHAs do not get suppliers from INFOREC. (Cell-6), 
# This Cell then looks in EORD (MRP preferred supplier) and fills the missing ones.
# 
# - Finds NHAs where all plant suppliers are blank
# Reads EORD + LFA1 tables
# Applies SAP fallback rules and 
# Updates zt_omat_buynha_suppliers_plants.

# In[34]:


# 8.1 -- NHAs that need EORD backfill

df_missing = (
    spark.read.table(tgt_suppliers).alias("t")
    .join(
        df_nha.select(F.col("nha_cd").alias("nha")).alias("n"),
        "nha",
        "inner"
    )
    .filter(
        (F.coalesce(F.length("suppcd_1900"),F.lit(0)) == 0) &
        (F.coalesce(F.length("suppcd_2000"),F.lit(0)) == 0) &
        (F.coalesce(F.length("suppcd_3120"),F.lit(0)) == 0) &
        (F.coalesce(F.length("suppcd_1050"),F.lit(0)) == 0) &
        (F.coalesce(F.length("suppcd_1060"),F.lit(0)) == 0) &
        (F.coalesce(F.length("suppcd_1090"),F.lit(0)) == 0)
    )
    .select("nha")
)

# 8.2 Preferred EORD suppliers with names

df_eord_pref = (
    df_eord
    .filter((F.col("autet")=="1") & (F.col("flifn")=="X"))
    .select(
        F.trim("matnr").alias("nha"),
        F.col("werks").alias("plant"),
        F.lpad("lifnr",10,"0").alias("suppcd")
    )
    .join(
        df_lfa1.select(F.lpad("lifnr",10,"0").alias("suppcd"), F.col("name1").alias("suppname")),
        "suppcd",
        "left"
    )
)


# 8.3 Pivot plants into columns (as flat strings)

df_eord_pivot = (
    df_eord_pref
    .groupBy("nha")
    .pivot("plant", ["1900","2000","3120","1050","1060","1090","1000","1010","1020"])
    .agg(
        F.first("suppcd").alias("suppcd"),
        F.first("suppname").alias("suppname")
    )
)


# 8.4 Flatten pivot + SAP fallback logic

df_eord_flat = (
    df_eord_pivot
    .select(
        "nha",

        F.coalesce(F.col("1900_suppcd"), F.col("1020_suppcd")).alias("suppcd_1900"),
        F.coalesce(F.col("1900_suppname"), F.col("1020_suppname")).alias("suppname_1900"),

        F.coalesce(F.col("2000_suppcd"), F.col("1000_suppcd")).alias("suppcd_2000"),
        F.coalesce(F.col("2000_suppname"), F.col("1000_suppname")).alias("suppname_2000"),

        F.coalesce(F.col("3120_suppcd"), F.col("1010_suppcd")).alias("suppcd_3120"),
        F.coalesce(F.col("3120_suppname"), F.col("1010_suppname")).alias("suppname_3120"),

        F.col("1050_suppcd").alias("suppcd_1050"),
        F.col("1050_suppname").alias("suppname_1050"),

        F.col("1060_suppcd").alias("suppcd_1060"),
        F.col("1060_suppname").alias("suppname_1060"),

        F.col("1090_suppcd").alias("suppcd_1090"),
        F.col("1090_suppname").alias("suppname_1090")
    )
)


# 8.5 Only update NHAs that were missing

df_update = df_missing.join(df_eord_flat, "nha")


# 8.6 Delta MERGE (UPDATE)

delta_tbl = DeltaTable.forName(spark, tgt_suppliers)

(
    delta_tbl.alias("t")
    .merge(df_update.alias("s"), "t.nha = s.nha")
    .whenMatchedUpdate(set={
        "suppcd_1900":"s.suppcd_1900",
        "suppname_1900":"s.suppname_1900",
        "suppcd_2000":"s.suppcd_2000",
        "suppname_2000":"s.suppname_2000",
        "suppcd_3120":"s.suppcd_3120",
        "suppname_3120":"s.suppname_3120",
        "suppcd_1050":"s.suppcd_1050",
        "suppname_1050":"s.suppname_1050",
        "suppcd_1060":"s.suppcd_1060",
        "suppname_1060":"s.suppname_1060",
        "suppcd_1090":"s.suppcd_1090",
        "suppname_1090":"s.suppname_1090"
    })
    .execute()
)


# #### Step-9 -- POSUPP (Open PO Supplier Aggregation)
# - Find all NHA's that have open purchase orders, Applies PO validity rules.
# - Collects all suppliers per NHA, Produces one row per NHA with comma seperated value: 
# e.g. - posupp_cd = "0000001234, 0000009876"
# 

# In[ ]:


df_posupp = (
    df_nha.alias("a")

    .join(df_dmd.alias("d"), F.col("a.nha_cd") == F.col("d.nha"))

    .join(df_po_hist.alias("p"), F.col("p.matnr") == F.col("a.nha_cd"))

    .join(
        df_ekpo.alias("e"),
        (F.col("p.ebeln") == F.col("e.ebeln")) &
        (F.col("p.ebelp") == F.col("e.ebelp"))
    )

    .join(
        df_lfa1.alias("l"),
        F.lpad(F.col("p.lifnr"),10,"0") == F.lpad(F.col("l.lifnr"),10,"0"),
        "left"
    )

    .filter(
        (F.col("d.open_po_qty") > 0) &
        (~F.substring(F.col("p.due_dt"), -5, 5).isin("04-01", "12-31")) &
        (F.col("p.status") != "INACT") &
        (F.trim("p.matnr") != "") &
        (F.trim("p.loekz") == "") &
        (F.col("p.discrd") != "X") &
        (F.col("p.aussl") != "U3") &
        (F.col("p.bsart") != "UB") &
        (F.col("p.elikz") != "X") &
        (~F.trim(F.col("e.pstyp")).isin("7", "9"))
    )

    .select(
        F.col("a.nha_cd").alias("nha"),
        F.lpad(F.col("p.lifnr"),10,"0").alias("suppcd"),
        F.col("l.name1").alias("suppname")
    )
    .distinct()
    .groupBy("nha")
    .agg(
        F.concat_ws(",", F.collect_list("suppcd")).alias("posupp_cd"),
        F.concat_ws(",", F.collect_list("suppname")).alias("posupp_name")
    )
)

df_posupp.createOrReplaceTempView("posupp")


# #### Step-10 -- Update PO1, PO2 & PR supplier
# - Splits the aggregated POSUPP string into PO1 and PO2.
# - Pulls PR supplier from IPLM and Adds supplier names from LFA1.
# - Updates the rpt_omat_buynha_suppliers_plants table using MERGE
# Now every NHA has: Plant suppliers, Open PO suppliers, PR (problem report) supplier.

# In[ ]:


# 10.1 -- Split PO suppliers into PO1 and PO2

df_po_split = (
    df_posupp
    .withColumn("suppcd_po1", F.split("posupp_cd", ",")[0])
    .withColumn("suppcd_po2", F.split("posupp_cd", ",")[1])
    .withColumn("suppname_po1", F.split("posupp_name", ",")[0])
    .withColumn("suppname_po2", F.split("posupp_name", ",")[1])
    .select(
        "nha",
        "suppcd_po1","suppname_po1",
        "suppcd_po2","suppname_po2"
    )
)


# 10.2 -- PR (IPLM) supplier

df_pr_supp = (
    df_pr
    .filter(
        (F.col("part_name") != "NA") &
        (~F.upper("state").isin("CLOSED")) &
        (
            F.upper("state").isin("CONFIRMED","IN REVIEW","IN WORK") |
            F.upper("disposition").isin("CONFIRMED","DEFER")
        ) &
        (F.upper("reason") == "OBSOLETE COMPONENT")
    )
    .select(
        F.col("part_name").alias("nha"),
        F.lpad("supplier_code",10,"0").alias("suppcd_pr")
    )
    .join(
        df_lfa1.select(F.lpad("lifnr",10,"0").alias("suppcd_pr"), F.col("name1").alias("suppname_pr")),
        "suppcd_pr",
        "left"
    )
)


# 10.3 -- Combine PO and PR suppliers

df_po_pr = (
    df_po_split
    .join(df_pr_supp, "nha", "left")
)


# 10.4 -- Delta MERGE into target

delta_tbl = DeltaTable.forName(spark, tgt_suppliers)

(
    delta_tbl.alias("t")
    .merge(df_po_pr.alias("s"), "t.nha = s.nha")
    .whenMatchedUpdate(set={
        "suppcd_po1":"s.suppcd_po1",
        "suppname_po1":"s.suppname_po1",
        "suppcd_po2":"s.suppcd_po2",
        "suppname_po2":"s.suppname_po2",
        "suppcd_pr":"s.suppcd_pr",
        "suppname_pr":"s.suppname_pr"
    })
    .execute()
)


# #### Step-11 -- OMAT selected supplier logic
# 
# This gives OMAT one final supplier per NHA.
# 
# 

# In[ ]:


df_sel = (
    spark.read.table(tgt_suppliers)
    .select(
        "nha",

        "suppcd_2000","suppname_2000",
        "suppcd_1900","suppname_1900",
        "suppcd_3120","suppname_3120",
        "suppcd_1050","suppname_1050",
        "suppcd_1060","suppname_1060",
        "suppcd_1090","suppname_1090",
        "suppcd_po1","suppname_po1",
        "suppcd_po2","suppname_po2",
        "suppcd_pr","suppname_pr"
    )
)

df_choice = (
    df_sel
    .withColumn(
        "omat_selected_plant",
        F.when(F.length("suppcd_2000")>0,"2000")
         .when(F.length("suppcd_1900")>0,"1900")
         .when(F.length("suppcd_3120")>0,"3120")
         .when(F.length("suppcd_1050")>0,"1050")
         .when(F.length("suppcd_1060")>0,"1060")
         .when(F.length("suppcd_1090")>0,"1090")
         .when(F.length("suppcd_po1")==10,"2000")
         .when(F.length("suppcd_pr")>0,"2000")
    )
    .withColumn(
        "omat_selected_suppliercd",
        F.when(F.length("suppcd_2000")>0, F.col("suppcd_2000"))
         .when(F.length("suppcd_1900")>0, F.col("suppcd_1900"))
         .when(F.length("suppcd_3120")>0, F.col("suppcd_3120"))
         .when(F.length("suppcd_1050")>0, F.col("suppcd_1050"))
         .when(F.length("suppcd_1060")>0, F.col("suppcd_1060"))
         .when(F.length("suppcd_1090")>0, F.col("suppcd_1090"))
         .when(F.length("suppcd_po1")==10, F.col("suppcd_po1"))
         .when(F.length("suppcd_po2")==10, F.substring("suppcd_po2",1,10))
         .when(F.length("suppcd_pr")>0, F.col("suppcd_pr"))
    )
    .withColumn(
        "omat_selected_supplier",
        F.when(F.length("suppcd_2000")>0, F.col("suppname_2000"))
         .when(F.length("suppcd_1900")>0, F.col("suppname_1900"))
         .when(F.length("suppcd_3120")>0, F.col("suppname_3120"))
         .when(F.length("suppcd_1050")>0, F.col("suppname_1050"))
         .when(F.length("suppcd_1060")>0, F.col("suppname_1060"))
         .when(F.length("suppcd_1090")>0, F.col("suppname_1090"))
         .when(F.length("suppcd_po1")==10, F.col("suppname_po1"))
         .when(F.length("suppcd_po2")==10, F.col("suppname_po2"))
         .when(F.length("suppcd_pr")>0, F.col("suppname_pr"))
    )
)

delta_tbl = DeltaTable.forName(spark, tgt_suppliers)

(
    delta_tbl.alias("t")
    .merge(df_choice.alias("s"), "t.nha = s.nha")
    .whenMatchedUpdate(set={
        "omat_selected_plant":"s.omat_selected_plant",
        "omat_selected_suppliercd":"s.omat_selected_suppliercd",
        "omat_selected_supplier":"s.omat_selected_supplier"
    })
    .execute()
)


# #### Step-12 -- Mark processed & write OMAT log
# Marks all NHAs from the current run as processed using a Delta MERGE and then writes a log record to rpt_omat_log_dtls with the OB PR count, NHA count, and execution timestamp for audit.

# In[ ]:


# 12.1 -- Mark NHAs as processed using MERGE (Delta-safe)

df_to_mark = df_nha.select(F.col("nha_cd").alias("nha")).distinct()

delta_nha = DeltaTable.forName(spark, tgt_nha)

(
    delta_nha.alias("t")
    .merge(df_to_mark.alias("s"), "t.nha = s.nha")
    .whenMatchedUpdate(set={"isprocessed": F.lit("Y")})
    .execute()
)


# 12.2 -- Compute counts for logging

obprt_cnt = df_nha.select("cmpprtno_cd").distinct().count()
nha_cnt   = df_nha.select("nha_cd").distinct().count()


# 12.3 -- Insert into OMAT_LOG_DTLS

spark.sql(f"""
INSERT INTO {tgt_log}
(
  program_name,
  obprtno_cnt,
  obprtno_nha_cnt,
  obpr_cnt,
  executed_on
)
VALUES
(
  'nb_omat_insert_suppliers_byplant - finished',
  1,
  {obprt_cnt},
  {nha_cnt},
  current_timestamp()
)
""")


# 
# 
