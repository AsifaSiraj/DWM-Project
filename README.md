# Final Project Report — Data Warehouse Implementation

## 1. Abstract

This project demonstrates the end-to-end design and implementation of a Data Warehouse (DWH) for real-estate analytics. The system consolidates data from multiple sources into a centralized repository, enabling faster reporting and data-driven insights. A star schema model was designed, ETL pipelines were implemented in Python for extraction and transformation, and the final data was loaded into a relational warehouse. Visualization and analysis were carried out using Power BI dashboards to track KPIs and performance metrics. The project successfully integrates database design, ETL automation, and business intelligence into a unified architecture that supports scalability, accuracy, and efficient decision-making.

---

## 2. Introduction

This project implements a practical Data Warehouse solution for real-estate analytics. The core motivation is to consolidate transactional and master data (properties, agents, clients, contracts, maintenance) into a single, query-optimized store that supports fast reporting and business intelligence.

**Scope and Key Deliverables:**
- Documented star-schema design and SQL DDL (`Database/DDL Queries.sql`)
- Modular ETL scripts and Jupyter notebook demonstration (`E2E_DWH_Pipeline/E2EPipelineExec.ipynb`)
- Fact snapshot generation and Power BI dashboard templates for analytical reporting

---

## 3. Project Overview

This repository contains an end-to-end implementation of a Data Warehouse for real-estate transactions. It ingests data from two primary sources: synthetic CSV datasets (generated via Mockaroo API) and an existing PostgreSQL OLTP database. These are consolidated into a star-schema DWH optimized for analytical queries and Power BI visualizations.

**Primary Data Sources:**
- Local CSV files (stored in `Database/Datasets/`)
- PostgreSQL OLTP database (existing transactional system)
- JSON data from Mockaroo API (when available, combined with CSV and PostgreSQL)

**Project Flow:**
```
Data Sources (Local CSV + PostgreSQL + JSON API) 
    ↓
CombineSources.py → ETL_MasterFunction.py (Hybrid Data Combination)
    ↓
ETL_SupportFunctions.py (Data Processing & Transformations)
    ↓
Star Schema Creation (Dimensions + Fact Transaction)
    ↓
Data Loading → PostgreSQL DWH + CSV Files
    ↓
FactSnapshot.py → Fact_Snapshot.csv → PostgreSQL + CSV Export
    ↓
Power BI Dashboard (imports Fact_Snapshot.csv)
```

**Key Repository Files:**
- `Database/Datasets/` — Local CSV files (static data sources)
- `Database/DDL Queries.sql` — SQL scripts for PostgreSQL database schema creation
- `E2E_DWH_Pipeline/E2EPipelineExec.ipynb` — Complete pipeline demonstration notebook
- `E2E_DWH_Pipeline/Pipeline_Support/CombineSources.py` — Combines CSV, PostgreSQL, and JSON data
- `E2E_DWH_Pipeline/Pipeline_Support/ETL_MasterFunction.py` — Main ETL orchestration and data combination
- `E2E_DWH_Pipeline/Pipeline_Support/ETL_SupportFunctions.py` — Data processing and transformation functions
- `E2E_DWH_Pipeline/Pipeline_Support/FactSnapshot.py` — Fact snapshot generation
- `E2E_DWH_Pipeline/DimTable Snapshot/` — Generated dimension table CSVs
- `E2E_DWH_Pipeline/Fact Table Snapshot/Fact_Snapshot.csv` — Generated fact snapshot
- `PowerBI Desktop Template/` — Power BI dashboard templates

---

## 4. Project Objectives

**Clear, action-based objectives:**
- Implement a centralized data warehouse using PostgreSQL
- Build an automated ETL pipeline for data cleaning and loading
- Design and implement a star schema model (Fact + Dimension tables)
- Develop Power BI dashboards for performance analytics
- Ensure data accuracy, consistency, and scalability

---

## 5. Tools and Technologies

| Category | Tools / Technologies |
|----------|---------------------|
| Database | PostgreSQL (OLTP Source + DWH Target) |
| ETL Pipeline | Python (pandas, SQLAlchemy, requests) |
| Data Processing | CombineSources.py, ETL_MasterFunction.py |
| Data Modeling | Excel / Lucidchart |
| Visualization | Power BI |
| Data Sources | Local CSV files, PostgreSQL, JSON API (optional) |
| Version Control | GitHub |

---

## 6. System Design

### 6.1 Architecture Diagram

The system follows this data flow: **Local CSV + PostgreSQL + JSON API → CombineSources.py → ETL_MasterFunction.py → ETL_SupportFunctions.py → PostgreSQL DWH + CSV Export → FactSnapshot.py → Fact_Snapshot.csv (PostgreSQL + CSV) → Power BI Dashboard**

Data flows from three sources (local CSV files, PostgreSQL OLTP, and optional JSON API) through Python ETL processing, creates star schema in PostgreSQL, exports to CSV files, and generates fact snapshot for Power BI consumption.

*(Architecture diagrams can be added to the repository and referenced here)*

### 6.2 Schema Design

**Star Schema Overview:**

**Fact Table:** `Fact_Transactions`
- Attributes: TransactionID, DateID, AgentID, PropertyID, TransactionValue, CommissionValue, MaintenanceExp, NegotiationDays, ClosingDays

**Dimension Tables:**
- `Dim_Date` (DateID, Date, Year, Month, Quarter, Week, Day)
- `Dim_Agent` (AgentID, AgentName, Gender, AgeCat, AgentSince, Position)
- `Dim_PropertyDetails` (PropertyDetailsID, LotArea, Bedrooms, Bathrooms, Kitchens, Floors, ParkingArea, BuiltSince, Condition)
- `Dim_Location` (LocationID, State, City, ZipCode)
- `Dim_Listing` (ListingID, ListingType, NumVisits)

---

## 7. ETL Pipeline Implementation

### 7.1 Implementation Steps

**Extract:**
- Local CSV files are loaded from `Database/Datasets/` folder
- PostgreSQL OLTP data is fetched using connection parameters
- Optional JSON data is fetched from Mockaroo API (when API key is available)
- `CombineSources.py` combines all three data sources using union of columns and deduplication

**Transform:**
- **Data Cleaning:** Handle missing values using median imputation for numerical fields and default values for categorical fields
- **Type Conversion:** Cast column data types (dates to datetime, amounts to float, IDs to integers)
- **Deduplication Process:** 
  - Identify duplicate records between Mockaroo CSV and PostgreSQL sources
  - Create hybrid staging rows that merge overlapping data from both sources
  - Apply business rules for conflict resolution (e.g., use most recent transaction data)
  - Remove complete duplicates and reconcile partial matches
- **Business Rule Application:** Calculate derived fields (commission values, transaction totals, duration calculations)
- **Key Generation:** Create surrogate keys for dimension tables and establish foreign key relationships

**Load:**
- Dimension tables and Fact Transaction table are loaded into PostgreSQL using SQLAlchemy
- Same data is also exported as CSV files to `E2E_DWH_Pipeline/DimTable Snapshot/` and `E2E_DWH_Pipeline/Fact Table Snapshot/`
- `FactSnapshot.py` merges all dimensions with fact table and creates `Fact_Snapshot.csv`
- Final fact snapshot is saved to both PostgreSQL and CSV for Power BI consumption

### 7.2 Key Files

- `E2E_DWH_Pipeline/E2EPipelineExec.ipynb` — Notebook walkthrough of the pipeline
- `E2E_DWH_Pipeline/Pipeline_Support/ETL_MasterFunction.py` — Orchestrates extraction, transformation, and load
- `E2E_DWH_Pipeline/Pipeline_Support/ETL_SupportFunctions.py` — Transformation utilities
- `E2E_DWH_Pipeline/Pipeline_Support/FactSnapshot.py` — Fact snapshot generation

### 8.3 Example Code Snippet

```python
# ETL Pipeline Execution (from E2EPipelineExec.ipynb)
from Pipeline_Support.ETL_MasterFunction import etl_master
from Pipeline_Support.FactSnapshot import create_fact_snapshot

# PostgreSQL connection parameters
db_params = {
    'host': 'localhost',
    'database': 'Real-Estate-Management',
    'user': 'postgres',
    'password': 'asifa123'
}

# Run hybrid ETL (CSV + PostgreSQL + JSON)
Dim_Date, Dim_Location, Dim_Agent, Dim_PropertyDetails, Dim_Listing, Fact_Transaction = etl_master(
    source="hybrid", 
    db_params=db_params, 
    use_mockaroo=True
)

# Create fact snapshot
Fact_Snap = create_fact_snapshot(Dim_Date, Dim_Location, Dim_Agent, Dim_PropertyDetails, Dim_Listing, Fact_Transaction)
```

**Note:** Much of the incremental validation is performed interactively in `E2E_DWH_Pipeline/E2EPipelineExec.ipynb`. The notebook contains manual checks, ad-hoc transforms, and stepwise loads used during development.

---

## 8. Data Quality Validation

**Quality checks implemented:**
- Duplicate row detection and removal in staging
- Type and range checks (e.g., non-negative amounts)
- Referential integrity: ensuring foreign keys in fact rows map to existing dimension rows
- Cross-checks: aggregate totals compared between source CSVs and loaded fact snapshots

Logs and validation summaries are produced by the pipeline and can be found in the notebook outputs or pipeline logs.

---

## 9. Warehouse Deployment

**Deployment Process:**
- PostgreSQL database schema created using `Database/DDL Queries.sql`
- Local CSV files loaded from `Database/Datasets/` folder
- ETL pipeline (`ETL_MasterFunction.py`) combines CSV + PostgreSQL + JSON data sources
- Star schema (dimensions + facts) created and loaded into PostgreSQL
- Dimension and fact tables exported as CSV files to respective snapshot folders
- `FactSnapshot.py` generates final fact snapshot saved to both PostgreSQL and CSV
- Power BI imports `Fact_Snapshot.csv` for dashboard visualization

**Local Setup Requirements:**
- PostgreSQL server running on localhost with 'Real-Estate-Management' database
- Connection details: host=localhost, user=postgres, password=asifa123
- Optional: MOCKAROO_API_KEY environment variable for JSON data enhancement
- Power BI imports CSV from: `E2E_DWH_Pipeline/Fact Table Snapshot/Fact_Snapshot.csv`



---

## 10. Dashboard and Visualization

**Power BI Artifacts:**
- `PowerBI Desktop Template/` contains the template file for the dashboard
- `E2E_DWH_Pipeline/Dashboard Snips/` contains screenshots used in the report

**Key KPIs Displayed:**
- Total Sales (TransactionValue)
- Commission Value
- Active Agents
- Top properties by revenue

**Dashboard Features:**
The dashboard uses visuals such as bar charts, pie charts, and time-series trends. It connects directly to the fact snapshot and refreshes when new data is loaded.

**Current Implementation:**
The project uses the exported `Fact_Snapshot.csv` file for Power BI dashboard data. This CSV file is generated by the `FactSnapshot.py` script and contains the complete fact table merged with dimension attributes for optimal dashboard performance.

---

## 11. Results and Analysis

**Achievements:**
- DWH successfully integrated multiple datasets (properties, agents, clients, contracts, maintenance)
- ETL pipeline is automated and modular, facilitating reuse and extension
- Power BI dashboards provide analytical insights after ETL runs

**Sample Insights:**
- The top-performing agent generated 20% higher commission in 2024 compared to 2023 (See `Dashboard Snips`)

**Implementation Notes:**
- Power BI dashboards use the `Fact_Snapshot.csv` file as the primary data source
- The ETL pipeline generates updated snapshots which can be refreshed in Power BI
- This approach provides reliable data access and consistent performance for dashboard users

---

## 12. Challenges Faced

**Issues Encountered and Resolutions:**
- **Schema mismatch during loading** — Resolved with explicit casting and mapping functions in ETL_SupportFunctions.py
- **Null values in transaction amounts** — Handled using median imputation and business-rule defaults during data transformation
- **Data deduplication complexity** — Implemented hybrid staging approach to merge overlapping records from multiple sources

---

## 13. Future Enhancements

**Planned Improvements:**
- Automate daily ETL schedule with Airflow or OS-level cron
- Integrate live API data instead of static CSVs
- Add machine learning predictions (sales forecasting) and integrate into dashboards
- Migrate to a cloud DWH such as AWS Redshift or BigQuery for scalability

---

## 14. Conclusion

This project demonstrates the full lifecycle of a DWH implementation: data ingestion, transformation, modeling, loading, and visualization. The architecture provides a foundation for repeatable analytics and can be extended with automation and predictive capabilities. While some challenges remain (particularly around Power BI connectivity), the core system successfully delivers business insights through a well-structured data warehouse implementation.

---

## 15. References / Appendix

**Resources:**
- Mockaroo dataset generator: https://mockaroo.com/
- GitHub repository: https://github.com/AsifaSiraj/DWM-Project

**Key Project Files:**
- `Database/Datasets/` — Source data files
- `Database/DDL Queries.sql` — Database schema scripts
- `E2E_DWH_Pipeline/E2EPipelineExec.ipynb` — Main pipeline notebook
- `E2E_DWH_Pipeline/Pipeline_Support/` — ETL support modules
- `PowerBI Desktop Template/` — Dashboard templates

**Documentation:**
Add screenshots of SQL scripts, ERDs, and Power BI visuals to the `E2E_DWH_Pipeline/Dashboard Snips/` folder if available.
