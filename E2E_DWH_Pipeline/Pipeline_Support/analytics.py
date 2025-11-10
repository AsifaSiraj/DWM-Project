import os
import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.cluster import KMeans
from sklearn.ensemble import RandomForestRegressor, IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
import joblib


BASE_DIR = os.path.dirname(__file__)
FACT_SNAPSHOT_PATH = os.path.join(os.path.dirname(BASE_DIR), 'Fact Table Snapshot', 'FactSnapshot.csv')
MODELS_DIR = os.path.join(BASE_DIR, 'models')
OUTPUT_DIR = os.path.join(os.path.dirname(BASE_DIR), 'Fact Table Snapshot')

os.makedirs(MODELS_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)


def load_fact_snapshot(path: str = None) -> pd.DataFrame:
    """Load the Fact snapshot CSV. If path is None will use project default.

    Returns a pandas DataFrame ready for analytics.
    """
    p = path or FACT_SNAPSHOT_PATH
    if not os.path.exists(p):
        raise FileNotFoundError(f"Fact snapshot not found at {p}")
    df = pd.read_csv(p, parse_dates=['Date'])
    return df


def compute_kpis(df: pd.DataFrame) -> pd.DataFrame:
    """Compute a small set of KPIs and return as DataFrame for export to Power BI.

    KPIs included: totals and averages, top agents and locations.
    """
    kpis = {}
    kpis['SnapshotDate'] = datetime.now().isoformat()
    kpis['TotalTransactions'] = int(len(df))
    kpis['TotalTransactionValue'] = float(df['TransactionValue'].sum())
    kpis['AvgTransactionValue'] = float(df['TransactionValue'].mean())
    kpis['MedianTransactionValue'] = float(df['TransactionValue'].median())
    kpis['TotalMaintenanceExp'] = float(df['MaintenanceExp'].sum())
    kpis['AvgNegotiationDays'] = float(df['NegotiationDays'].mean())
    kpis['AvgClosingDays'] = float(df['ClosingDays'].mean())

    # Top 5 agents by TransactionValue
    if 'Position' in df.columns:
        top_agents = (
            df.groupby('Position')['TransactionValue']
            .sum()
            .sort_values(ascending=False)
            .head(5)
            .reset_index()
        )
    else:
        top_agents = pd.DataFrame()

    # Top 5 locations by number of transactions
    top_locations = (
        df.groupby(['State', 'City'])['TransactionID']
        .count()
        .sort_values(ascending=False)
        .head(5)
        .reset_index()
        .rename(columns={'TransactionID': 'Transactions'})
    )

    kpis_df = pd.DataFrame([kpis])
    # Save KPI artifacts for Power BI
    kpis_out = os.path.join(OUTPUT_DIR, f'kpis_snapshot_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
    kpis_df.to_csv(kpis_out, index=False)
    top_agents.to_csv(os.path.join(OUTPUT_DIR, 'top_agents_by_value.csv'), index=False)
    top_locations.to_csv(os.path.join(OUTPUT_DIR, 'top_locations_by_transactions.csv'), index=False)

    return kpis_df


def generate_trend_data(df: pd.DataFrame, freq: str = 'M') -> pd.DataFrame:
    """Aggregate transaction counts and sum by time period (monthly by default)."""
    df = df.copy()
    df['Period'] = pd.to_datetime(df['Date']).dt.to_period(freq)
    trends = (
        df.groupby('Period')
        .agg(TotalTransactions=('TransactionID', 'count'), TotalValue=('TransactionValue', 'sum'))
        .reset_index()
    )
    trends['Period'] = trends['Period'].astype(str)
    out_path = os.path.join(OUTPUT_DIR, 'trends_by_period.csv')
    trends.to_csv(out_path, index=False)
    return trends


def cluster_properties(df: pd.DataFrame, n_clusters: int = 4) -> pd.DataFrame:
    """Cluster properties using numeric property features to find segments.

    Adds a PropertySegment column and saves a model artifact and CSV for Power BI.
    """
    features = ['LotArea', 'Bedrooms', 'Bathrooms', 'Kitchens', 'Floors', 'ParkingArea', 'BuiltSince', 'MaintenanceExp', 'TransactionValue']
    use_cols = [c for c in features if c in df.columns]
    prop_df = df[use_cols].dropna()
    if prop_df.empty:
        return pd.DataFrame()

    scaler = StandardScaler()
    X = scaler.fit_transform(prop_df)
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    labels = kmeans.fit_predict(X)
    prop_df = prop_df.copy()
    prop_df['PropertySegment'] = labels

    # Save model and scaler
    joblib.dump(kmeans, os.path.join(MODELS_DIR, 'kmeans_property_segments.joblib'))
    joblib.dump(scaler, os.path.join(MODELS_DIR, 'kmeans_scaler.joblib'))

    out_path = os.path.join(OUTPUT_DIR, 'property_segments.csv')
    prop_df.reset_index(drop=True).to_csv(out_path, index=False)
    return prop_df


def detect_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    """Simple anomaly detection on numeric transaction columns using IsolationForest."""
    cols = [c for c in ['TransactionValue', 'MaintenanceExp', 'CommissionValue'] if c in df.columns]
    X = df[cols].fillna(0)
    if X.shape[0] < 10:
        return pd.DataFrame()

    iso = IsolationForest(contamination=0.01, random_state=42)
    preds = iso.fit_predict(X)
    anomalies = df.copy()
    anomalies['Anomaly'] = preds
    anomalies = anomalies[anomalies['Anomaly'] == -1]
    out = os.path.join(OUTPUT_DIR, 'anomalies_transactions.csv')
    anomalies.to_csv(out, index=False)
    joblib.dump(iso, os.path.join(MODELS_DIR, 'isolation_forest_transactions.joblib'))
    return anomalies


def train_price_model(df: pd.DataFrame) -> dict:
    """Train a simple RandomForest regressor to predict TransactionValue.

    Returns a dict with model path and performance summary.
    """
    cols = [c for c in ['LotArea', 'Bedrooms', 'Bathrooms', 'Kitchens', 'Floors', 'ParkingArea', 'BuiltSince', 'MaintenanceExp'] if c in df.columns]
    if 'TransactionValue' not in df.columns or not cols:
        return {}

    modelling_df = df[cols + ['TransactionValue']].dropna()
    X = modelling_df[cols]
    y = modelling_df['TransactionValue']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    rf = RandomForestRegressor(n_estimators=100, random_state=42)
    rf.fit(X_train, y_train)
    score = rf.score(X_test, y_test)
    model_path = os.path.join(MODELS_DIR, 'rf_price_model.joblib')
    joblib.dump(rf, model_path)

    summary = {'model_path': model_path, 'r2_score': float(score)}
    return summary


def run_all(path: str = None):
    """Run the end-to-end analytics: load data, compute KPIs, trends, clusters, anomalies, train model."""
    df = load_fact_snapshot(path)
    print('Loaded fact snapshot rows:', len(df))
    kpis = compute_kpis(df)
    print('KPIs computed and exported')
    trends = generate_trend_data(df)
    print('Trends generated')
    clusters = cluster_properties(df)
    if not clusters.empty:
        print('Property clusters computed and exported')
    anomalies = detect_anomalies(df)
    print('Anomaly detection finished, anomalies found:', len(anomalies))
    model_summary = train_price_model(df)
    print('Price model training finished:', model_summary)


if __name__ == '__main__':
    try:
        run_all()
    except Exception as e:
        print('Error running analytics:', e)
