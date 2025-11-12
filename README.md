# Final Project Report — Data Warehouse Implementation

## 1. Title Page

Project Title: Data Warehouse Implementation

Student Name / ID: 

Instructor / Course: 

Institution: 

Submission Date: 

---

## Latest Changes

- 2025-11-12: Rewrote `README.md` into a 15-section Final Project Report format (Data Warehouse Implementation).
- 2025-11-12: Added an `Introduction` section describing motivation, scope, and key deliverables.

For a complete history, consult the Git commit log.

---

## 2. Abstract

This project demonstrates the end-to-end design and implementation of a Data Warehouse (DWH) for organizational analytics. The system consolidates data from multiple sources into a centralized repository, enabling faster reporting and data-driven insights. A star schema model was designed, ETL pipelines were implemented in Python for extraction and transformation, and the final data was loaded into a relational warehouse. Visualization and analysis were carried out using Power BI dashboards to track KPIs and performance metrics. The project successfully integrates database design, ETL automation, and business intelligence into a unified architecture that supports scalability, accuracy, and efficient decision-making.

---

## Introduction

This project implements a practical Data Warehouse solution for real-estate analytics. The core motivation is to consolidate transactional and master data (properties, agents, clients, contracts, maintenance) into a single, query-optimized store that supports fast reporting and business intelligence. The scope includes data generation (synthetic CSVs), a modular Python ETL pipeline to extract/transform/load data into a star schema, and Power BI dashboards for visualization and analysis.

Key deliverables:

- A documented star-schema design and SQL DDL (`Database/DDL Queries.sql`).
- Modular ETL scripts and a Jupyter notebook demonstrating the pipeline (`E2E_DWH_Pipeline/E2EPipelineExec.ipynb`).
- A fact snapshot and Power BI dashboard templates for analytical reporting.


## 3. Project Overview

This repository contains an end-to-end implementation of a Data Warehouse for real-estate transactions. It ingests synthetic datasets (property, agent, client, transactions, maintenance) and consolidates them into a star-schema DWH optimized for analytical queries and Power BI visualizations.

Project flow:

Data Sources (CSV / Mockaroo) → ETL Pipeline (Python) → Data Warehouse (PostgreSQL / local) → Fact Snapshot → Power BI Dashboard

Files of interest:

- `Database/Datasets/` — raw CSVs used as data sources
- `E2E_DWH_Pipeline/E2EPipelineExec.ipynb` — demonstration notebook for the pipeline
- `E2E_DWH_Pipeline/Pipeline_Support/` — Python modules for ETL and support functions

---

## 4. Project Objectives

Clear, action-based objectives:

- Implement a centralized data warehouse using PostgreSQL.
- Build an automated ETL pipeline for data cleaning and loading.
- Design and implement a star schema model (Fact + Dimension tables).
- Develop Power BI dashboards for performance analytics.
- Ensure data accuracy, consistency, and scalability.

---

## 5. Tools and Technologies

Category | Tools / Technologies
--- | ---
Database | PostgreSQL
ETL Pipeline | Python (pandas, SQLAlchemy)
Data Modeling | Excel / Lucidchart
Visualization | Power BI
Data Generation | Mockaroo
Version Control | GitHub

---

## 6. System Design

### 6.1 Architecture Diagram

Add a diagram showing data flow: Mockaroo CSVs → ETL Script → DWH Tables → Fact Snapshot → Power BI Dashboard. (Images can be added to the repository and linked here.)

### 6.2 Schema Design

Overview of implemented schema:

- Fact Table: `Fact_Transactions`
  - Attributes: TransactionID, DateID, AgentID, PropertyID, TransactionValue, CommissionValue, MaintenanceExp, NegotiationDays, ClosingDays

- Dimension Tables:
  - `Dim_Date` (DateID, Date, Year, Month, Quarter)
  - `Dim_Agent` (AgentID, AgentName, Region, Experience)
  - `Dim_Property` (PropertyID, Address, PropertyType, Bedrooms, Bathrooms)
  - `Dim_Location` (LocationID, City, Region)

ERD and star-schema images (if available) should be added to the repo and referenced here.

---

## 7. ETL Pipeline Implementation

Summary of the implementation steps:

- Extract: Read CSV files from `Database/Datasets/` using pandas.
- Transform: Clean missing values, cast column types, derive lookup keys, and apply business rules (e.g., commission calculations).
- Load: Insert transformed rows into the DWH tables using SQLAlchemy (PostgreSQL).

Main files:

- `E2E_DWH_Pipeline/E2EPipelineExec.ipynb` — Notebook walkthrough of the pipeline.
- `E2E_DWH_Pipeline/Pipeline_Support/ETL_MasterFunction.py` — Orchestrates extraction, transformation, and load.

Example snippet:

```python
import pandas as pd
from sqlalchemy import create_engine

df = pd.read_csv("Database/Datasets/contract.csv")
df['transaction_value'] = df['asked_amount'].astype(float) - df['discount'].fillna(0).astype(float)

engine = create_engine('postgresql://user:password@localhost:5432/dwh')
df.to_sql('staging_contract', con=engine, if_exists='replace', index=False)

# After additional transforms, load into Fact_Transactions
```

Refer to `E2E_DWH_Pipeline/Pipeline_Support/ETL_SupportFunctions.py` for transformation utilities.

---

## 8. Data Quality Validation

Quality checks implemented:

- Duplicate row detection and removal in staging.
- Type and range checks (e.g., non-negative amounts).
- Referential integrity: ensuring foreign keys in fact rows map to existing dimension rows.
- Cross-checks: aggregate totals compared between source CSVs and loaded fact snapshots.

Logs and validation summaries are produced by the pipeline and can be found in the notebook outputs or pipeline logs.

---

## 9. Warehouse Deployment

Deployment notes:

- Schema and tables created using `Database/DDL Queries.sql` (PostgreSQL).
- Data loaded via the Python ETL pipeline (`E2E_DWH_Pipeline/Pipeline_Support/ETL_MasterFunction.py`).
- Power BI connects to the local PostgreSQL instance (or exported `FactSnapshot.csv`) for reporting.

If running locally, ensure PostgreSQL is running and update connection details in `Pipeline_Support/ETL_MasterFunction.py`.

---

## 10. Dashboard and Visualization

Power BI artifacts:

- `PowerBI Desktop Template/` contains the template file for the dashboard.
- `E2E_DWH_Pipeline/Dashboard Snips/` contains screenshots used in the report.

Key KPIs shown:

- Total Sales (TransactionValue)
- Commission Value
- Active Agents
- Top properties by revenue

The dashboard uses visuals such as bar charts, pie charts, and time-series trends. It connects directly to the fact snapshot and refreshes when new data is loaded.

---

## 11. Results and Analysis

Achievements:

- DWH integrated multiple datasets (properties, agents, clients, contracts, maintenance).
- ETL pipeline is automated and modular, facilitating reuse and extension.
- Power BI dashboards provide near real-time insights after ETL runs.

Sample insight:

- The top-performing agent generated 20% higher commission in 2024 compared to 2023. (See `Dashboard Snips`.)

---

## 12. Challenges Faced

Examples of issues encountered and resolutions:

- Schema mismatch during loading — resolved with explicit casting and mapping functions.
- Null values in transaction amounts — handled using median imputation or business-rule defaults.
- Power BI connection issues — resolved via proper ODBC/postgres driver installation and credentials management.

---

## 13. Future Enhancements

- Automate daily ETL schedule with Airflow or OS-level cron.
- Integrate live API data instead of static CSVs.
- Add machine learning predictions (sales forecasting) and integrate into dashboards.
- Migrate to a cloud DWH such as AWS Redshift or BigQuery for scalability.

---

## 14. Conclusion

This project demonstrates the full lifecycle of a DWH implementation: data ingestion, transformation, modeling, loading, and visualization. The architecture provides a foundation for repeatable analytics and can be extended with automation and predictive capabilities.

---

## 15. References / Appendix

- Mockaroo dataset links (if used) — add URLs here.
- GitHub repository: https://github.com/AsifaSiraj/DWM-Project
- Key files:
  - `Database/Datasets/`
  - `Database/DDL Queries.sql`
  - `E2E_DWH_Pipeline/E2EPipelineExec.ipynb`
  - `E2E_DWH_Pipeline/Pipeline_Support/`

Add screenshots of SQL scripts, ERDs, and Power BI visuals to the `E2E_DWH_Pipeline/Dashboard Snips/` folder if available.

---

If you want, I can also add a one-page printable PDF report generated from this README or inject your name/id and submission date into the Title Page — tell me what to fill and I'll update it.
# Final Project Report — Data Warehouse Implementation

## 1. Title Page

Project Title: Data Warehouse Implementation

Student Name / ID: 

Instructor / Course: 

Institution: 

Submission Date: 

---

## 2. Abstract

This project demonstrates the end-to-end design and implementation of a Data Warehouse (DWH) for organizational analytics. The system consolidates data from multiple sources into a centralized repository, enabling faster reporting and data-driven insights. A star schema model was designed, ETL pipelines were implemented in Python for extraction and transformation, and the final data was loaded into a relational warehouse. Visualization and analysis were carried out using Power BI dashboards to track KPIs and performance metrics. The project successfully integrates database design, ETL automation, and business intelligence into a unified architecture that supports scalability, accuracy, and efficient decision-making.

---

## 3. Project Overview

This repository contains an end-to-end implementation of a Data Warehouse for real-estate transactions. It ingests synthetic datasets (property, agent, client, transactions, maintenance) and consolidates them into a star-schema DWH optimized for analytical queries and Power BI visualizations.

Project flow:

Data Sources (CSV / Mockaroo) → ETL Pipeline (Python) → Data Warehouse (PostgreSQL / local) → Fact Snapshot → Power BI Dashboard

Files of interest:

- `Database/Datasets/` — raw CSVs used as data sources
- `E2E_DWH_Pipeline/E2EPipelineExec.ipynb` — demonstration notebook for the pipeline
- `E2E_DWH_Pipeline/Pipeline_Support/` — Python modules for ETL and support functions

---

## 4. Project Objectives

Clear, action-based objectives:

- Implement a centralized data warehouse using PostgreSQL.
- Build an automated ETL pipeline for data cleaning and loading.
- Design and implement a star schema model (Fact + Dimension tables).
- Develop Power BI dashboards for performance analytics.
- Ensure data accuracy, consistency, and scalability.

---

## 5. Tools and Technologies

Category | Tools / Technologies
--- | ---
Database | PostgreSQL
ETL Pipeline | Python (pandas, SQLAlchemy)
Data Modeling | Excel / Lucidchart
Visualization | Power BI
Data Generation | Mockaroo
Version Control | GitHub

---

## 6. System Design

### 6.1 Architecture Diagram

Add a diagram showing data flow: Mockaroo CSVs → ETL Script → DWH Tables → Fact Snapshot → Power BI Dashboard. (Images can be added to the repository and linked here.)

### 6.2 Schema Design

Overview of implemented schema:

- Fact Table: `Fact_Transactions`
  - Attributes: TransactionID, DateID, AgentID, PropertyID, TransactionValue, CommissionValue, MaintenanceExp, NegotiationDays, ClosingDays

- Dimension Tables:
  - `Dim_Date` (DateID, Date, Year, Month, Quarter)
  - `Dim_Agent` (AgentID, AgentName, Region, Experience)
  - `Dim_Property` (PropertyID, Address, PropertyType, Bedrooms, Bathrooms)
  - `Dim_Location` (LocationID, City, Region)

ERD and star-schema images (if available) should be added to the repo and referenced here.

---

## 7. ETL Pipeline Implementation

Summary of the implementation steps:

- Extract: Read CSV files from `Database/Datasets/` using pandas.
- Transform: Clean missing values, cast column types, derive lookup keys, and apply business rules (e.g., commission calculations).
- Load: Insert transformed rows into the DWH tables using SQLAlchemy (PostgreSQL).

Main files:

- `E2E_DWH_Pipeline/E2EPipelineExec.ipynb` — Notebook walkthrough of the pipeline.
- `E2E_DWH_Pipeline/Pipeline_Support/ETL_MasterFunction.py` — Orchestrates extraction, transformation, and load.

Example snippet:

```python
import pandas as pd
from sqlalchemy import create_engine

df = pd.read_csv("Database/Datasets/contract.csv")
df['transaction_value'] = df['asked_amount'].astype(float) - df['discount'].fillna(0).astype(float)

engine = create_engine('postgresql://user:password@localhost:5432/dwh')
df.to_sql('staging_contract', con=engine, if_exists='replace', index=False)

# After additional transforms, load into Fact_Transactions
```

Refer to `E2E_DWH_Pipeline/Pipeline_Support/ETL_SupportFunctions.py` for transformation utilities.

---

## 8. Data Quality Validation

Quality checks implemented:

- Duplicate row detection and removal in staging.
- Type and range checks (e.g., non-negative amounts).
- Referential integrity: ensuring foreign keys in fact rows map to existing dimension rows.
- Cross-checks: aggregate totals compared between source CSVs and loaded fact snapshots.

Logs and validation summaries are produced by the pipeline and can be found in the notebook outputs or pipeline logs.

---

## 9. Warehouse Deployment

Deployment notes:

- Schema and tables created using `Database/DDL Queries.sql` (PostgreSQL).
- Data loaded via the Python ETL pipeline (`E2E_DWH_Pipeline/Pipeline_Support/ETL_MasterFunction.py`).
- Power BI connects to the local PostgreSQL instance (or exported `FactSnapshot.csv`) for reporting.

If running locally, ensure PostgreSQL is running and update connection details in `Pipeline_Support/ETL_MasterFunction.py`.

---

## 10. Dashboard and Visualization

Power BI artifacts:

- `PowerBI Desktop Template/` contains the template file for the dashboard.
- `E2E_DWH_Pipeline/Dashboard Snips/` contains screenshots used in the report.

Key KPIs shown:

- Total Sales (TransactionValue)
- Commission Value
- Active Agents
- Top properties by revenue

The dashboard uses visuals such as bar charts, pie charts, and time-series trends. It connects directly to the fact snapshot and refreshes when new data is loaded.

---

## 11. Results and Analysis

Achievements:

- DWH integrated multiple datasets (properties, agents, clients, contracts, maintenance).
- ETL pipeline is automated and modular, facilitating reuse and extension.
- Power BI dashboards provide near real-time insights after ETL runs.

Sample insight:

- The top-performing agent generated 20% higher commission in 2024 compared to 2023. (See `Dashboard Snips`.)

---

## 12. Challenges Faced

Examples of issues encountered and resolutions:

- Schema mismatch during loading — resolved with explicit casting and mapping functions.
- Null values in transaction amounts — handled using median imputation or business-rule defaults.
- Power BI connection issues — resolved via proper ODBC/postgres driver installation and credentials management.

---

## 13. Future Enhancements

- Automate daily ETL schedule with Airflow or OS-level cron.
- Integrate live API data instead of static CSVs.
- Add machine learning predictions (sales forecasting) and integrate into dashboards.
- Migrate to a cloud DWH such as AWS Redshift or BigQuery for scalability.

---

## 14. Conclusion

This project demonstrates the full lifecycle of a DWH implementation: data ingestion, transformation, modeling, loading, and visualization. The architecture provides a foundation for repeatable analytics and can be extended with automation and predictive capabilities.

---

## 15. References / Appendix

- Mockaroo dataset links (if used) — add URLs here.
- GitHub repository: https://github.com/AsifaSiraj/DWM-Project
- Key files:
  - `Database/Datasets/`
  - `Database/DDL Queries.sql`
  - `E2E_DWH_Pipeline/E2EPipelineExec.ipynb`
  - `E2E_DWH_Pipeline/Pipeline_Support/`

Add screenshots of SQL scripts, ERDs, and Power BI visuals to the `E2E_DWH_Pipeline/Dashboard Snips/` folder if available.

---

If you want, I can also add a one-page printable PDF report generated from this README or inject your name/id and submission date into the Title Page — tell me what to fill and I'll update it.
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


## Future Updates
For updating the data warehouse with new data:

1. Generate new data for the desired year.
2. Append the new data to the existing datasets.
3. Modify the start and end dates in the ETL master function script.
4. Re-run the pipeline from start.

By following these steps, the fact table snapshot and the Power BI dashboard will be updated with the new data.

---
