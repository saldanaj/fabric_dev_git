# Fabric notebook source

# METADATA ********************

# META {
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
# This notebook demonstrates an exploratory analysis on the `drug_era` table using PySpark in an Azure Synapse Analytics notebook. The analysis generates a histogram displaying patients secondary drug exposures to active ingredients, stratified by gender and age for a specific year. The `drug_era` table is generated using a custom library(`drug_era_generator`) invoked in the previous notebook(omopgeneratedrugera.ipynb). This analysis extends the [Drug Exposure Query (DEX03: Distribution of age, stratified by drug)](https://github.com/OHDSI/OMOP-Queries/blob/master/md/Drug_Exposure.md#dex03-distribution-of-age-stratified-by-drug) by incorporating stratification based on both gender and age.
# 
# ## Prerequisites
# - If you would like to make changes to this notebook, please make a copy of the notebook vs. updating this notebook directly.
# - Ensure the `drug-era` table contains data by executing the `omopgeneratedrugera` notebook located at 'OHDSI Drug Era Generation/omopgeneratedrugera'. Running this notebook will replace any existing `drug_era` records with new ones, based on the latest OMOP data.
# - Use this notebook as is for the exploratory analysis or create a copy to perform custom analysis.
# 
# Notebook Parameters: 
# - **primary_drug_concept_id**: The primary active ingredient exposure for patients.
# - **secondary_drug_concept_id**: The secondary active ingredient exposure for patients.
# - **year**: The target year during which patients were actively exposed to both primary and secondary drugs.
# 
# Modify these parameters for alternative exploratory analysis on patient drug exposures.
# 
# _For more information and detailed steps see the Healthcare data solutions Documentation_

# MARKDOWN ********************

# ##### Configuration management and setup
# To setup and manage configurations for the Healthcare data solutions, please execute the following cell:

# CELL ********************

%run healthcare_data_solutions_demo_msft_config_notebook {"enable_packages_mount" : false}

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

# PARAMETERS CELL ********************

primary_drug_concept_id = 1596977 # The ingredient drug concept id of insulin, regular, human. # Set the actual primary ingredient drug concept ID for your analysis
secondary_drug_concept_id = 1308216 # The ingredient drug concept id of lisinopril. Set the actual secondary ingredient drug concept ID for your analysis
year = 2022 # Set the desired year for analysis

# METADATA ********************

# META {
# META   "frozen": false,
# META   "editable": false
# META }

# CELL ********************

import plotly.express as px
import pandas as pd

# Table names
drug_era_table_name = "drug_era"
person_table_name = "person"

if not spark.catalog.databaseExists(dbName=omop_database_name):
    raise ValueError(f"The `{omop_database_name}` database does not exist.")

# Check if the tables exist and are not empty
for table_name in [drug_era_table_name, person_table_name]:
    if not spark.sql(f"SHOW TABLES IN `{omop_database_name}` LIKE '{table_name}'").first():
        raise ValueError(f"The table `{table_name}` does not exist in `{omop_database_name}`.")
        
    if spark.sql(f"SELECT COUNT(*) as count FROM `{omop_database_name}`.`{table_name}`").first().count == 0:
        raise ValueError(f"The `{omop_database_name}`.`{table_name}` table is empty.")
        
# SQL query to extract data for age, gender, and patient_id
query = f"""
    SELECT DISTINCT
        EXTRACT(YEAR FROM (MIN(t.drug_era_start_date) OVER (PARTITION BY t.person_id, t.drug_concept_id))) - p.year_of_birth AS age,
        p.gender_source_value AS gender,
        t.person_id AS patient_id
    FROM
        `{omop_database_name}`.`{drug_era_table_name}` t
    JOIN
        `{omop_database_name}`.`{person_table_name}` p ON t.person_id = p.person_id
    WHERE
        t.person_id IN (
            SELECT
                person_id
            FROM
                `{omop_database_name}`.`{drug_era_table_name}`
            WHERE
                drug_concept_id = {primary_drug_concept_id}
        )
        AND t.drug_concept_id = {secondary_drug_concept_id}
        AND YEAR(t.drug_era_start_date) = {year}
"""

# Create a Spark dataframe with age, gender, and count
age_gender_count_spark_df = spark.sql(query).groupBy('age', 'gender').count()

# Convert Spark dataframe to Pandas dataframe if not empty
if age_gender_count_spark_df.head(1):
    age_gender_count_df = age_gender_count_spark_df.toPandas()
    
    # Create a histogram plot using Plotly Express
    fig = px.histogram(age_gender_count_df, x="age", y="count", color="gender")

    # Update plot layout with appropriate settings
    fig.update_layout(
        autosize=False,
        width=700,
        height=700,
        margin=dict(
            l=50,
            r=50,
            b=100,
            t=100,
            pad=4
        ),
        paper_bgcolor="LightSteelBlue",
        yaxis_title="Exposure Count",
        title=dict(
            text=f"Gender & Age Distribution: Secondary Drug Exposure ({year})",  # Update the title with the specific year
            font=dict(
                family="Arial",
                size=24,
                color="black"
            ),
            x=0.5,
            y=0.95,
            xanchor="center",
            yanchor="top"
        ),
        xaxis=dict(
            title="Age"
        ),
        yaxis=dict(
            title="Sum of Count"
        ),
        legend=dict(
            title="Gender",
            itemsizing="constant"
        ),
    )
    
    # Display the plot
    fig.show()
else:
    print("No data found for the specified criteria.")

# METADATA ********************

# META {
# META   "frozen": false,
# META   "editable": false
# META }
