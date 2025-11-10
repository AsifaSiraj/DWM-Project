# Real-Estate Management Data Warehouse End-to-End Pipeline

## Table of Contents
1. [Project Overview](#project-overview)
2. [Repository Structure](#repository-structure)
3. [Data Generation](#data-generation)
4. [ETL Process](#etl-process)
5. [Data Warehouse](#data-warehouse)
6. [Dashboard and Visualization](#dashboard-and-visualization)
7. [Future Updates](#future-updates)

## Project Overview
This project involves building a Data Warehouse (DWH) for a Real-Estate Management Company which is then specifically used to evaluate agents' performance based on the transactions managed and carried out by them. The company facilitates property transactions by connecting property owners with potential clients for purchase or rental. The company allocates agents to market properties, arrange viewings, negotiate terms, and carry out the transaction process. It also handles maintenance and repairs of properties on behalf of their owners.

**Key Objectives:**
- Design and implement a relational database schema.
- Design and implement a data warehouse based on the star schema model.
- Create an ETL pipeline to populate the data warehouse.
- Answer analytical queries through Star Schema.
- Visualize the data using Power BI and automate dashboard updates.
<br>

**Project Stages:**

![image](https://github.com/hase3b/End-to-End-DWH-Pipeline/assets/52492910/ca4506f8-ceb8-4813-9aee-5c3134e90946)

<br>

**Project Flow:**

![image](https://github.com/hase3b/End-to-End-DWH-Pipeline/assets/52492910/d390dcd3-5a8d-4356-bcb4-d0e965fa161d "Project Flow")

## Repository Structure
The repository contains the following folders:

1. **Database**
   - `Datasets` - Contains CSV files for each entity.
   - `DDL Queries.sql` - SQL script to create database tables.
   - `Documentation.pdf` - Documentation for database schema.
   - `ERD.png` - Entity-Relationship Diagram (ERD).

2. **DWH Dimensional Modelling**
   - `Star Schema Blueprint.xlsx` - Blueprint for the star schema.

3. **E2E DWH Pipeline**
   - `E2EPipelineExec.ipynb` - Jupyter Notebook demonstrating the complete pipeline execution.
   - `Dashboard Snips` - Snapshots of the Power BI dashboard.
   - `Pipeline_Support` - Contains Python scripts for Data Generation, Dimensional Queries, ETL Support Functions, ETL Master Function, Fact Snapshot Creation, and Fact Snapshot Uploading.



## Data Generation
The data generation script uses Mockaroo to create synthetic data for the database.

1. Run the data generation script:
    ```sh
    python %run Pipeline_Support/DataGen.py
    ```

2. This will create CSV files in the `Database/Datasets` folder.

## ETL Process
The ETL process extracts data from the OLTP database, transforms it, and loads it into the data warehouse.

The ETL pipeline will:
    - Fetch datasets from the `Datasets` folder.
    - Treat missing values.
    - Correct data types.
    - Create the star schema dimensions and fact table.
    - Upload the fact table snapshot to GitHub.

## Data Warehouse
The star schema includes the following dimensions and fact table:

- **Dimensions**:
  - Date
  - Location
  - Agent
  - PropertyDetails
  - Listing

- **Fact Table**:
  - Transaction facts such as MaintenanceExp, AskedAmount, TransactionValue, CommissionRate, CommissionValue, NegotiationDays, ClosingDays.

## Dashboard and Visualization
The Power BI dashboard visualizes the data from the fact table snapshot.

1. Snippet

## Analytics and Machine Learning
After loading and cleaning data into the star schema, the project includes an analytics module that computes KPIs, generates trend aggregates, produces property segments, detects anomalies, and trains a simple price model. These artifacts are exported as CSVs so Power BI can consume them directly.

Files and location
- `E2E_DWH_Pipeline/Pipeline_Support/analytics.py` — runs the analytics pipeline and writes outputs.
- Outputs (CSV) are written to `E2E_DWH_Pipeline/Fact Table Snapshot/` and model artifacts are saved to `E2E_DWH_Pipeline/Pipeline_Support/models/`.

Provided analytics
- KPIs: totals, averages, medians, maintenance totals, negotiation and closing averages, stored as `kpis_snapshot_*.csv`.
- Top agents and locations: `top_agents_by_value.csv`, `top_locations_by_transactions.csv`.
- Trends by period: `trends_by_period.csv` (monthly by default).
- Property segmentation (KMeans): `property_segments.csv` and saved KMeans model/scaler.
- Anomaly detection (IsolationForest): `anomalies_transactions.csv` and model artifact.
- Price model (RandomForestRegressor): trained model saved to `models/` with an R2 score summary.

How to run (local)
1. Install dependencies into a Python environment:

```powershell
pip install pandas scikit-learn joblib
```

2. Ensure the fact snapshot `FactSnapshot.csv` exists at `E2E_DWH_Pipeline/Fact Table Snapshot/`.

3. Run the analytics script:

```powershell
python .\E2E_DWH_Pipeline\Pipeline_Support\analytics.py
```

Power BI integration options
- OneDrive/SharePoint: copy the generated CSVs to OneDrive for Business or SharePoint and point your Power BI dataset to those files. Power BI will auto-refresh when files change.
- Power BI REST API (push dataset): for near-real-time updates use the Power BI REST API to push rows or trigger dataset refreshes (requires Azure AD app and credentials).
- Triggered refresh: upload CSVs to a cloud location supported by Power BI and call the Power BI REST API to trigger a dataset refresh.

Next steps
- Wire the ETL to call `analytics.run_all()` after the fact snapshot is created so analytics artifacts are produced automatically.
- Choose an integration option above and I can add upload/refresh code to automate pushing CSVs to Power BI or OneDrive.


## Future Updates
For updating the data warehouse with new data:

1. Generate new data for the desired year.
2. Append the new data to the existing datasets.
3. Modify the start and end dates in the ETL master function script.
4. Re-run the pipeline from start.

By following these steps, the fact table snapshot and the Power BI dashboard will be updated with the new data.

---
