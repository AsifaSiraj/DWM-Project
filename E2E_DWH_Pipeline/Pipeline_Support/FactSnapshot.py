import pandas as pd
import os

def create_fact_snapshot(Dim_Date, Dim_Location, Dim_Agent, Dim_PropertyDetails, Dim_Listing, Fact_Transaction):
    # 🧩 Merge all dimension tables into one fact snapshot
    Fact_Snap = pd.merge(Fact_Transaction, Dim_Date, on='DateID', how='left').drop(['DateID'], axis=1)
    Fact_Snap = pd.merge(Fact_Snap, Dim_Location, on='LocationID', how='left').drop(['LocationID'], axis=1)
    Fact_Snap = pd.merge(Fact_Snap, Dim_Agent, on='AgentID', how='left').drop(['AgentID'], axis=1)
    Fact_Snap = pd.merge(Fact_Snap, Dim_PropertyDetails, on='PropertyDetailsID', how='left').drop(['PropertyDetailsID'], axis=1)
    Fact_Snap = pd.merge(Fact_Snap, Dim_Listing, on='ListingID', how='left').drop(['ListingID'], axis=1)

    # 🎯 Reorder and filter columns
    Fact_Snap = Fact_Snap[
        [
            'TransactionID', 'Date', 'Year', 'Quarter', 'Month', 'Week', 'Day',
            'State', 'City', 'ZipCode', 'Gender', 'AgeCat', 'AgentSince', 'Position',
            'LotArea', 'Bedrooms', 'Bathrooms', 'Kitchens', 'Floors', 'ParkingArea',
            'BuiltSince', 'Condition', 'ListingType', 'NumVisits', 'MaintenanceExp',
            'AskedAmount', 'TransactionValue', 'CommissionRate', 'CommissionValue',
            'NegotiationDays', 'ClosingDays'
        ]
    ]

    # ✅ Define absolute paths for both folders
    fact_snapshot_dir = r"D:\Github\DWM-Project\E2E_DWH_Pipeline\Fact Table Snapshot"
    dim_snapshot_dir = r"D:\Github\DWM-Project\E2E_DWH_Pipeline\DimTable Snapshot"

    # ✅ Ensure folders exist
    os.makedirs(fact_snapshot_dir, exist_ok=True)
    os.makedirs(dim_snapshot_dir, exist_ok=True)

    # ✅ Save Fact Snapshot CSV
    fact_snapshot_path = os.path.join(fact_snapshot_dir, "Fact_Snapshot.csv")
    Fact_Snap.to_csv(fact_snapshot_path, index=False)

    # ✅ Also save the base Fact_Transaction table separately (optional best practice)
    fact_transaction_path = os.path.join(fact_snapshot_dir, "Fact_Transaction.csv")
    Fact_Transaction.to_csv(fact_transaction_path, index=False)

    print(f"✅ Fact Snapshot saved successfully at:\n{fact_snapshot_path}")
    print(f"✅ Fact_Transaction table saved at:\n{fact_transaction_path}")

    return Fact_Snap
