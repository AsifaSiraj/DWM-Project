import os
import pandas as pd
import warnings
import requests
from sqlalchemy import create_engine
from .ETL_SupportFunctions import (
    fetch_datasets, fetch_from_postgres, correct_dtypes,
    fill_mv, create_star_schema
)

def etl_master(source="hybrid", db_params=None, use_mockaroo=True):
    """
    ETL Master Function
    source: "csv", "db", or "hybrid"
      - "csv" = Only CSV data
      - "db" = Only PostgreSQL data
      - "hybrid" = Merge both CSV + PostgreSQL data
    """

    def combine_parts(*dfs):
        """
        Combines multiple DataFrames (CSV, DB, Mockaroo) with union of columns,
        drops duplicate rows, and returns the merged DataFrame.
        """
        valid_dfs = [df for df in dfs if df is not None and not df.empty]
        if not valid_dfs:
            return pd.DataFrame()

        combined = pd.concat(valid_dfs, axis=0, ignore_index=True)
        combined = combined.loc[:, ~combined.columns.duplicated()]
        combined = combined.drop_duplicates()
        return combined
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
        print("🔄 Fetching from CSV + Mockaroo + PostgreSQL...")
        if not db_params:
            raise ValueError("db_params must be provided for hybrid mode.")

        csv_data = fetch_datasets(tables, base_url)
        db_data = fetch_from_postgres(tables, **db_params)

        # Attempt to fetch Mockaroo JSON for each table if API key is available
        mock_data = {}
        if use_mockaroo:
            mockaroo_key = os.getenv('MOCKAROO_API_KEY', 'e0396780')
            if mockaroo_key and mockaroo_key.strip():
                print("🌱 MOCKAROO: API key present, attempting to fetch synthetic rows...")
                for tbl in tables:
                    try:
                        url = f'https://my.api.mockaroo.com/{tbl}.json?key={mockaroo_key}'
                        resp = requests.get(url, timeout=30)
                        if resp.status_code == 200:
                            payload = resp.json()
                            # Ensure payload is a list of records
                            if isinstance(payload, dict):
                                payload = [payload]
                            mock_df = pd.DataFrame(payload)
                            if not mock_df.empty:
                                mock_data[tbl] = mock_df
                                print(f"Mockaroo: fetched {len(mock_df)} rows for '{tbl}'")
                            else:
                                print(f"Mockaroo: no rows returned for '{tbl}'")
                        else:
                            print(f"Mockaroo: skipped '{tbl}' (status {resp.status_code})")
                    except Exception as e:
                        print(f"Mockaroo: error fetching '{tbl}': {e}")
            else:
                print("🌱 MOCKAROO: no API key found in environment; skipping Mockaroo fetches")
        else:
            print("🌱 MOCKAROO: disabled by parameter; skipping Mockaroo fetches")

        dataframes = {}
        for tbl in tables:
            csv_df = csv_data.get(tbl) if isinstance(csv_data, dict) else None
            db_df = db_data.get(tbl) if isinstance(db_data, dict) else None
            mock_df = mock_data.get(tbl) if isinstance(mock_data, dict) else None

            # combine_parts performs union-of-columns concat and drops exact duplicates
            combined = combine_parts(csv_df, db_df, mock_df)
            dataframes[tbl] = combined

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

    # Cache result so repeated calls in the same kernel return identical objects
    _ETL_MASTER_RESULT = (
        Dim_Date,
        Dim_Location,
        Dim_Agent,
        Dim_PropertyDetails,
        Dim_Listing,
        Fact_Transaction,
    )
    _ETL_MASTER_RAN = True
    return _ETL_MASTER_RESULT

