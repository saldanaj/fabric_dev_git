# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5872e3b9-1ba0-4b08-a254-d62ad9be6be4",
# META       "default_lakehouse_name": "healthcare_data_solutions_demo_msft_gold_omop",
# META       "default_lakehouse_workspace_id": "1487c09d-5112-4a8b-b246-eb796192bb85"
# META     },
# META     "environment": {
# META       "environmentId": "764953bd-4033-8909-4a06-2e935d07dfb6",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# ##### WARNING
# The following notebook is intended to be read only. Please do not modify the contents of this notebook.


# MARKDOWN ********************

# # Overview
# This notebook demonstrates the process of generating `drug_era` table records in OMOP using PySpark in an Azure Synapse Analytics notebook, primarily for exploratory purposes. The `drug_era` table records generation follows the [OHDSI Drug Era sample script](https://github.com/OHDSI/ETL-CMS/blob/master/SQL/create_CDMv5_drug_era_non_stockpile.sql), which is adapted to work with PySpark in Azure Synapse Analytics. The drug era generator code is part of our custom Python library that is packaged as a wheel file and uploaded to a Spark pool for easy access. This should be a good starting point to generate OMOP `drug_era` derived table. 
# 
# ## Prerequisites
# 1. Ensure that the OMOP database has valid data in the following tables:
# 
#     - `drug_exposure`
#     - `concept`
#     - `concept_ancestor`
# 
#     This data can be generated using our sample data or BYOD (Bring Your Own Data) by running the fhir-to-omop pipeline.
# 2. Ensure our custom library wheel package is attached to the spark pool to be used when running this notebook
# 
# Notebook parameters:
# 
# - **omop_database_name** : The OMOP database name with data for generating the Drug Era table. Update this only if your OMOP database differs from the default.
# 
# If the OMOP `drug_exposure` table is populated with valid data, this notebook invokes the DrugEraGenerator module that strings together periods of time that a person is exposed to an active drug ingredient, allowing for 30 gap days.
# The DrugEraGenerator module deletes all the existing `drug_era` records and generates new records based on the latest OMOP data.
# 
# _For more information and detailed steps see the Healthcare data solutions Documentation_

# MARKDOWN ********************

# ##### Configuration management and setup
# To setup and manage configurations for the Healthcare data solutions, please execute the following cell:

# CELL ********************

%run healthcare_data_solutions_demo_msft_config_notebook

# METADATA ********************

# META {
# META   "frozen": false,
# META   "editable": false
# META }

# CELL ********************

%run healthcare_data_solutions_demo_msft_config_notebook {"enable_spark_setup" : true, "enable_packages_mount" : false} 

# METADATA ********************

# META {
# META   "frozen": false,
# META   "editable": false
# META }

# MARKDOWN ********************

# Retrieve OMOP Lakehouse ID from Parameter Service

# CELL ********************

from microsoft.fabric.hls.hds.utils.parameter_service import ParameterService
from microsoft.fabric.hls.hds.global_constants.global_constants import GlobalConstants as GC

parameter_service = ParameterService(
    spark,
    workspace_name=workspace_name,
    admin_lakehouse_name=administration_database_name,
    one_lake_endpoint=one_lake_endpoint
)

omop_database_name = parameter_service.get_foundation_config_value(GC.OMOP_LAKEHOUSE_ID_KEY)

# METADATA ********************

# META {
# META   "frozen": false,
# META   "editable": false
# META }

# CELL ********************

from microsoft.fabric.hls.hds.omop.derived_tables.drug_era_generator import DrugEraGenerator

drug_era_table_name = "drug_era"

drug_era_generator = DrugEraGenerator(spark_session=spark, database_name=omop_database_name)
is_successful, message = drug_era_generator.generate_drug_era_table()

if is_successful:
    drug_era_df = spark.sql(f"SELECT * FROM `{omop_database_name}`.`{drug_era_table_name}`")
    display(drug_era_df)
else:
    print(message)

# METADATA ********************

# META {
# META   "frozen": false,
# META   "editable": false
# META }

# CELL ********************

mssparkutils.fs.unmount(packages_mount_name)

# METADATA ********************

# META {
# META   "frozen": false,
# META   "editable": false
# META }
