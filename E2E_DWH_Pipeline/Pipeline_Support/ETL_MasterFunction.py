import os
import pandas as pd
import warnings
from sqlalchemy import create_engine
from Pipeline_Support.ETL_SupportFunctions import (
    fetch_datasets, fetch_from_postgres, correct_dtypes,
    fill_mv, create_star_schema
)

def etl_master(source="hybrid", db_params=None):
    """
    ETL Master Function
    source: "csv", "db", or "hybrid"
      - "csv" = Only CSV data
      - "db" = Only PostgreSQL data
      - "hybrid" = Merge both CSV + PostgreSQL data
    """

    # ---------- 1️⃣ Data Ingestion ----------
    tables = [
        'address', 'client', 'agent', 'owner', 'features', 'property',
        'maintenance', 'visit', 'commission', 'sale', 'contract', 'rent', 'admin'
    ]
    base_url = "https://raw.githubusercontent.com/AsifaSiraj/DWM-Project/refs/heads/main/Database/Datasets/"

    if source == "csv":
        print("📥 Fetching data from CSV files...")
        dataframes = fetch_datasets(tables, base_url)

    elif source == "db":
        print("🗄️ Fetching data from PostgreSQL...")
        if not db_params:
            raise ValueError("db_params must be provided for DB source.")
        dataframes = fetch_from_postgres(tables, **db_params)
        
    elif source == "hybrid":
        print("🔄 Fetching from both CSV + PostgreSQL...")
        if not db_params:
            raise ValueError("db_params must be provided for hybrid mode.")

        csv_data = fetch_datasets(tables, base_url)
        db_data = fetch_from_postgres(tables, **db_params)

        dataframes = {}
        for tbl in tables:
            if tbl in db_data and tbl in csv_data:
                
                merged = pd.concat([csv_data[tbl], db_data[tbl]], ignore_index=True)
                
                dataframes[tbl] = merged.reset_index(drop=True)
            elif tbl in csv_data:
                dataframes[tbl] = csv_data[tbl]
            elif tbl in db_data:
                dataframes[tbl] = db_data[tbl]

    else:
        raise ValueError("Invalid source. Use 'csv', 'db', or 'hybrid'.")
   
    print("✅ Data ingestion complete.")

    # ---------- 2️⃣ Handle Missing Values ----------
    dataframes = fill_mv(dataframes)
    print("✅ Missing values filled.")

    # ---------- 3️⃣ Data Type Corrections ----------
    dataframes = correct_dtypes(dataframes)
    print("✅ Data types corrected.")

    # ---------- 4️⃣ Star Schema Creation ----------
    Dim_Date, Dim_Location, Dim_Agent, Dim_PropertyDetails, Dim_Listing, Fact_Transaction = create_star_schema(
        sale=dataframes['sale'],
        rent=dataframes['rent'],
        maintenance=dataframes['maintenance'],
        property=dataframes['property'],
        commission=dataframes['commission'],
        visit=dataframes['visit'],
        features=dataframes['features'],
        address=dataframes['address'],
        agent=dataframes['agent'],
        start_date='2022-01-01',
        end_date='2025-12-31'
    )
    print("✅ Star schema generated successfully.")

    # 5️⃣ Create absolute directories
    dim_dir = r"D:\Github\DWM-Project\E2E_DWH_Pipeline\DimTable Snapshot"
    fact_dir = r"D:\Github\DWM-Project\E2E_DWH_Pipeline\Fact Table Snapshot"

    os.makedirs(dim_dir, exist_ok=True)
    os.makedirs(fact_dir, exist_ok=True)


    # 6️⃣ Save CSVs to your given paths
    Dim_Date.to_csv(os.path.join(dim_dir, 'Dim_Date.csv'), index=False)
    Dim_Location.to_csv(os.path.join(dim_dir, 'Dim_Location.csv'), index=False)
    Dim_Agent.to_csv(os.path.join(dim_dir, 'Dim_Agent.csv'), index=False)
    Dim_PropertyDetails.to_csv(os.path.join(dim_dir, 'Dim_PropertyDetails.csv'), index=False)
    Dim_Listing.to_csv(os.path.join(dim_dir, 'Dim_Listing.csv'), index=False)
    Fact_Transaction.to_csv(os.path.join(fact_dir, 'Fact_Transaction.csv'), index=False)

    print("✅ All CSVs saved successfully in E2E_DWH_Pipeline folders.")

    # 7️⃣ PostgreSQL Connection Setup
    engine = create_engine("postgresql+psycopg2://postgres:asifa123@localhost:5432/Real-Estate-Management")

    # 8️⃣ Load into PostgreSQL
    print("⬆️ Loading tables into PostgreSQL database...")
    Dim_Date.to_sql('dim_date', engine, if_exists='replace', index=False)
    Dim_Location.to_sql('dim_location', engine, if_exists='replace', index=False)
    Dim_Agent.to_sql('dim_agent', engine, if_exists='replace', index=False)
    Dim_PropertyDetails.to_sql('dim_propertydetails', engine, if_exists='replace', index=False)
    Dim_Listing.to_sql('dim_listing', engine, if_exists='replace', index=False)
    Fact_Transaction.to_sql('fact_transaction', engine, if_exists='replace', index=False)
    print("✅ Data successfully loaded into PostgreSQL!")

    return (
    Dim_Date,
    Dim_Location,
    Dim_Agent,
    Dim_PropertyDetails,
    Dim_Listing,
    Fact_Transaction
)

