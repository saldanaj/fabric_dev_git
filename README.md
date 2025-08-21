# Microsoft Fabric demo artifacts (healthcare + mslearn)

This repository contains Microsoft Fabric artifacts synced via Git integration. It includes sample Lakehouses, Data Pipelines, Notebooks, and Semantic Models for:

- Healthcare Data Solutions (end-to-end demo across FHIR, imaging/DICOM, SDOH, OMOP, and medallion layers)
- Microsoft Learn samples (medallion architecture and sales/orders demos)

Last updated: 2025-08-21

## What this repo does

Provides a working set of Fabric items you can deploy to a Fabric workspace to explore:

- Ingestion (FHIR NDJSON, DICOM imaging, SDOH and sales/orders data)
- Medallion architecture (Bronze → Silver → Gold) in Lakehouse
- Transformations with Notebooks and Data Pipelines
- Analytics with a Semantic Model (e.g., OMOP gold)

Items are organized as Fabric artifact folders and are intended to be opened/run inside Microsoft Fabric.

## Repository structure

- `healthcare_data_solutions/`
	- `*.Environment/` — environment settings and shared libraries for the demo
	- `*.Lakehouse/` — Lakehouse definitions (metadata) for admin/bronze/silver/gold areas
	- `*.Notebook/` — notebooks for FHIR flattening, OMOP transforms, SDOH, DICOM conversion, and utilities
	- `*.DataPipeline/` — ingestion and processing pipelines (clinical foundation, imaging, OMOP analytics, SDOH)
	- `*.SemanticModel/` — OMOP semantic model for analytics

- `mslearn/`
	- `*.Lakehouse/` — sample Lakehouse used in MS Learn modules
	- `*.Dataflow/` — Dataflow Gen2 example (Orders)
	- `*.Notebook/` — medallion architecture demo and product/order notebooks
	- `*.SemanticModel/` — sales gold semantic model

Note: Artifact folder names map 1:1 to Fabric items. They are not meant to be executed directly outside Fabric.

## Get started (Fabric)

1) Prerequisites
- Access to Microsoft Fabric with Lakehouse, Data Engineering, and Data Factory experiences enabled
- A Fabric workspace you can connect to Git

2) Connect a Fabric workspace to this repo
- In Fabric: Open your workspace → Git integration → Connect to Git → point to this repository and `main` branch
- Pull to sync items into the workspace

3) Validate and wire dependencies
- Ensure Lakehouses exist/bind correctly for pipelines and notebooks (admin/bronze/silver/gold where applicable)
- Update any workspace-specific connections or parameters if prompted

4) Run examples
- Healthcare: run ingestion pipelines (clinical foundation, SDOH, imaging) → execute notebooks for flattening and medallion moves → build gold/OMOP semantic model
- MS Learn: open the medallion architecture and sales/orders notebooks; use the sample Lakehouse and Dataflow

## Notes

- This repo is for demo/learning scenarios; adapt before production use
- Data sources (e.g., sample NDJSON/DICOM) are expected to be configured per your environment
- If you don’t use Fabric Git integration, you can import individual notebooks/pipelines manually into a workspace
