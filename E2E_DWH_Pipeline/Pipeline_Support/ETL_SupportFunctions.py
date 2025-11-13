# Pipeline_Support/ETL_SupportFunctions.py
import pandas as pd
import numpy as np
import requests
from io import StringIO
from sklearn.impute import KNNImputer
from datetime import datetime
from sqlalchemy import create_engine
import warnings



def fetch_from_postgres(table_names, db_name, user, password, host="localhost", port=5432):
    """
    Fetch data directly from PostgreSQL tables into pandas DataFrames.
    Returns dict with lowercase keys (table_name -> df).
    """
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db_name}")
    dataframes = {}
    for table in table_names:
        query = f'SELECT * FROM "{table}"'
        df = pd.read_sql(query, engine)
        dataframes[table.lower()] = df
    return dataframes

# Data Ingestion (web/csv)
def fetch_datasets(csv_files, base_url):
    """
    Fetch CSV files from base_url (raw csv urls). Returns dict with lowercase keys.
    """
    dataframes = {}
    for file in csv_files:
        csv_url = base_url.rstrip('/') + '/' + file + '.csv'
        response = requests.get(csv_url)
        if response.status_code == 200:
            df = pd.read_csv(StringIO(response.text))
            
            dataframes[file.lower()] = df
        else:
            raise Exception(f"Failed to fetch {file}.csv from {csv_url} (status {response.status_code})")
    return dataframes

# helper: safe numeric conversion
def to_numeric_safe(series, fillna=0, as_int=False):
    s = pd.to_numeric(series, errors='coerce')
    if fillna is not None:
        s = s.fillna(fillna)
    if as_int:
        # round then convert to int to handle floats like 473.0
        s = s.round().astype(int)
    return s

# DB Data Type Correction
def correct_dtypes(dataframes):
    """
    Convert commonly used columns to the correct pandas dtypes.
    Expects dataframes keys in lowercase: 'address','client','agent',...
    """
    # ---- CLEAN DUPLICATE HEADERS OR INVALID ROWS ----
    for df_name, df in list(dataframes.items()):
        # remove rows that are probably duplicated header rows (all values equal to column names)
        mask = df.apply(lambda row: all(str(v).strip().lower() in map(str.lower, df.columns) for v in row), axis=1)
        df_clean = df[~mask].reset_index(drop=True)
        
        # ---- REMOVE TRUE DUPLICATE ROWS (same values across all columns) ----
        before = len(df_clean)
        df_clean = df_clean.drop_duplicates(keep='first').reset_index(drop=True)
        after = len(df_clean)
        dataframes[df_name] = df_clean

        if before != after:
            print(f"🧹 Removed {before - after} duplicate rows from '{df_name}'")

    date_columns = ['agent_dob', 'client_dob', 'owner_dob', 'hire_date',
                    'listing_date', 'maintenance_date', 'visit_date',
                    'sale_date', 'agreement_date', 'rent_start_date',
                    'rent_end_date', 'payment_date']

    for name, df in dataframes.items():
        for col in df.columns:
            if col.lower() in [d.lower() for d in date_columns]:
                df[col] = pd.to_datetime(df[col], format='%m/%d/%Y', errors='coerce')

    # Address
    if 'address' in dataframes:
        dataframes['address']['zip_code'] = to_numeric_safe(dataframes['address'].get('zip_code', pd.Series()), fillna=0, as_int=True).astype(str)

    # Client
    if 'client' in dataframes:
        dataframes['client']['client_dob'] = pd.to_datetime(dataframes['client'].get('client_dob'), errors='coerce', dayfirst=False)

    # Agent
    if 'agent' in dataframes:
        dataframes['agent']['agent_dob'] = pd.to_datetime(dataframes['agent'].get('agent_dob'), errors='coerce', dayfirst=False)
        dataframes['agent']['hire_date'] = pd.to_datetime(dataframes['agent'].get('hire_date'), errors='coerce', dayfirst=False)

    # Owner
    if 'owner' in dataframes:
        dataframes['owner']['owner_dob'] = pd.to_datetime(dataframes['owner'].get('owner_dob'), errors='coerce', dayfirst=False)

    # Features
    if 'features' in dataframes:
        for col in ['feature_id', 'no_bedrooms', 'no_bathrooms', 'no_kitchens', 'no_floors', 'year_built',
                    'parking_area_sqft', 'lot_area_sqft', 'condition_rating']:
            if col in dataframes['features'].columns:
                dataframes['features'][col] = to_numeric_safe(dataframes['features'][col], fillna=0)

        # ensure integer columns become int when safe
        int_cols = ['feature_id', 'no_bedrooms', 'no_bathrooms', 'no_kitchens', 'no_floors', 'year_built']
        for c in int_cols:
            if c in dataframes['features'].columns:
                dataframes['features'][c] = dataframes['features'][c].round().astype(int)

    # Property
    if 'property' in dataframes:
        for col in ['property_id', 'address_id', 'owner_id', 'agent_id', 'feature_id']:
            if col in dataframes['property'].columns:
                dataframes['property'][col] = to_numeric_safe(dataframes['property'][col], fillna=0).round().astype(int)
        if 'listing_date' in dataframes['property'].columns:
            dataframes['property']['listing_date'] = pd.to_datetime(dataframes['property']['listing_date'], errors='coerce')

        if 'asking_amount' in dataframes['property'].columns:
            dataframes['property']['asking_amount'] = to_numeric_safe(dataframes['property']['asking_amount'], fillna=np.nan)

    # Maintenance
    if 'maintenance' in dataframes:
        if 'maintenance_date' in dataframes['maintenance'].columns:
            dataframes['maintenance']['maintenance_date'] = pd.to_datetime(dataframes['maintenance']['maintenance_date'], errors='coerce')
        if 'cost' in dataframes['maintenance'].columns:
            dataframes['maintenance']['cost'] = to_numeric_safe(dataframes['maintenance']['cost'], fillna=0)

    # Visit
    if 'visit' in dataframes:
        if 'visit_date' in dataframes['visit'].columns:
            dataframes['visit']['visit_date'] = pd.to_datetime(dataframes['visit']['visit_date'], errors='coerce')

    # Commission
    if 'commission' in dataframes:
        if 'commission_id' in dataframes['commission'].columns:
            dataframes['commission']['commission_id'] = to_numeric_safe(dataframes['commission']['commission_id'], fillna=0).round().astype(int)
        if 'payment_date' in dataframes['commission'].columns:
            dataframes['commission']['payment_date'] = pd.to_datetime(dataframes['commission']['payment_date'], errors='coerce')
        if 'commission_amount' in dataframes['commission'].columns:
            dataframes['commission']['commission_amount'] = to_numeric_safe(dataframes['commission']['commission_amount'], fillna=np.nan)
        if 'commission_rate' in dataframes['commission'].columns:
            dataframes['commission']['commission_rate'] = to_numeric_safe(dataframes['commission']['commission_rate'], fillna=np.nan)
  
    warnings.filterwarnings("ignore", message=" You are merging on int and float columns where the float values are not equal to their int representation.", category=UserWarning)
    # Sale
    if 'sale' in dataframes:
        if 'sale_date' in dataframes['sale'].columns:
            dataframes['sale']['sale_date'] = pd.to_datetime(dataframes['sale']['sale_date'], errors='coerce')
        if 'sale_amount' in dataframes['sale'].columns:
            dataframes['sale']['sale_amount'] = to_numeric_safe(dataframes['sale']['sale_amount'], fillna=np.nan)
        if 'commission_id' in dataframes['sale'].columns:
            dataframes['sale']['commission_id'] = to_numeric_safe(dataframes['sale']['commission_id'], fillna=0).round().astype(int)

    # Rent
    if 'rent' in dataframes:
        for col in ['rent_id', 'client_id', 'property_id', 'commission_id', 'contract_id']:
            if col in dataframes['rent'].columns:
                dataframes['rent'][col] = to_numeric_safe(dataframes['rent'][col], fillna=0).round().astype(int)
        for date_col in ['agreement_date', 'rent_start_date', 'rent_end_date']:
            if date_col in dataframes['rent'].columns:
                dataframes['rent'][date_col] = pd.to_datetime(dataframes['rent'][date_col], errors='coerce')
        if 'rent_amount' in dataframes['rent'].columns:
            dataframes['rent']['rent_amount'] = to_numeric_safe(dataframes['rent']['rent_amount'], fillna=np.nan)

    return dataframes


# Data Cleaning helpers
def knn_impute(df):
    """
    Safe KNN imputation for numeric columns only.
    Skips non-numeric or all-NaN columns to prevent shape mismatch.
    """
    import numpy as np
    import pandas as pd
    from sklearn.impute import KNNImputer

    # If no columns, return as-is
    if df.shape[1] == 0:
        return df

    # Keep only numeric columns
    numeric_df = df.select_dtypes(include=[np.number])

    # Drop columns that are completely NaN
    non_empty_cols = numeric_df.columns[numeric_df.notna().any()].tolist()
    numeric_df = numeric_df[non_empty_cols]

    if numeric_df.empty:
        # No valid numeric columns to impute
        return df

    imputer = KNNImputer()
    imputed_array = imputer.fit_transform(numeric_df)

    # Create imputed DataFrame with correct shape
    imputed_df = pd.DataFrame(imputed_array, columns=numeric_df.columns, index=df.index)

    # Replace imputed numeric columns back in the original df
    df[numeric_df.columns] = imputed_df[numeric_df.columns]

    return df


def mode_impute(df):
    for col in df.columns:
        if df[col].isna().sum() > 0:
            try:
                mode_value = df[col].mode()[0]
                df[col].fillna(mode_value, inplace=True)
            except Exception:
                df[col].fillna("", inplace=True)
    return df

# Missing value filling function (logical)
def fill_mv(dataframes):
    # Ensure keys present
    comm = dataframes.get('commission')
    sale = dataframes.get('sale')
    rent = dataframes.get('rent')

    # 1) Fill missing commission_rate when possible
    if comm is not None and 'commission_rate' in comm.columns:
        missing_rate_ids = comm[comm['commission_rate'].isna()]['commission_id'].tolist()
        for cid in missing_rate_ids:
            # try sale first
            if sale is not None and 'commission_id' in sale.columns and cid in sale['commission_id'].values:
                sale_row = sale.loc[sale['commission_id'] == cid]
                if 'sale_amount' in sale_row.columns and 'commission_amount' in comm.columns:
                    sale_amount = pd.to_numeric(sale_row['sale_amount'].values[0], errors='coerce')
                    commission_amount = pd.to_numeric(comm.loc[comm['commission_id'] == cid, 'commission_amount'].values[0], errors='coerce')
                    if pd.notna(sale_amount) and pd.notna(commission_amount) and sale_amount != 0:
                        comm.loc[comm['commission_id'] == cid, 'commission_rate'] = (commission_amount / sale_amount) * 100
            # try rent
            elif rent is not None and 'commission_id' in rent.columns and cid in rent['commission_id'].values:
                rent_row = rent.loc[rent['commission_id'] == cid]
                if 'rent_amount' in rent_row.columns and 'commission_amount' in comm.columns:
                    rent_amount = pd.to_numeric(rent_row['rent_amount'].values[0], errors='coerce')
                    commission_amount = pd.to_numeric(comm.loc[comm['commission_id'] == cid, 'commission_amount'].values[0], errors='coerce')
                    if pd.notna(rent_amount) and pd.notna(commission_amount) and rent_amount != 0:
                        comm.loc[comm['commission_id'] == cid, 'commission_rate'] = (commission_amount / rent_amount) * 100

    # 2) Fill missing commission_amount when possible (commission_amount = base_amount * rate/100)
    if comm is not None and 'commission_amount' in comm.columns:
        missing_amt_ids = comm[comm['commission_amount'].isna()]['commission_id'].tolist()
        for cid in missing_amt_ids:
            # try sale
            if sale is not None and 'commission_id' in sale.columns and cid in sale['commission_id'].values:
                sale_row = sale.loc[sale['commission_id'] == cid]
                rate = comm.loc[comm['commission_id'] == cid, 'commission_rate'].values[0]
                sale_amount = pd.to_numeric(sale_row['sale_amount'].values[0], errors='coerce') if 'sale_amount' in sale_row.columns else np.nan
                try:
                    rate = float(rate)
                except Exception:
                    rate = np.nan
                if pd.notna(sale_amount) and pd.notna(rate):
                    comm.loc[comm['commission_id'] == cid, 'commission_amount'] = (sale_amount * rate) / 100.0
            # try rent
            elif rent is not None and 'commission_id' in rent.columns and cid in rent['commission_id'].values:
                rent_row = rent.loc[rent['commission_id'] == cid]
                rate = comm.loc[comm['commission_id'] == cid, 'commission_rate'].values[0]
                rent_amount = pd.to_numeric(rent_row['rent_amount'].values[0], errors='coerce') if 'rent_amount' in rent_row.columns else np.nan
                try:
                    rate = float(rate)
                except Exception:
                    rate = np.nan
                if pd.notna(rent_amount) and pd.notna(rate):
                    comm.loc[comm['commission_id'] == cid, 'commission_amount'] = (rent_amount * rate) / 100.0

    # 3) Fill missing rent_amount using commission_amount and commission_rate if available
    if rent is not None and 'rent_amount' in rent.columns and comm is not None:
        missing_rent_ids = rent[rent['rent_amount'].isna()]['rent_id'].tolist()
        for rid in missing_rent_ids:
            comm_id_series = rent.loc[rent['rent_id'] == rid, 'commission_id']
            if comm_id_series.empty:
                continue
            comm_id = comm_id_series.values[0]
            commission_amount = comm.loc[comm['commission_id'] == comm_id, 'commission_amount']
            commission_rate = comm.loc[comm['commission_id'] == comm_id, 'commission_rate']
            if commission_amount.empty or commission_rate.empty:
                continue
            commission_amount = pd.to_numeric(commission_amount.values[0], errors='coerce')
            commission_rate = pd.to_numeric(commission_rate.values[0], errors='coerce')
            if pd.notna(commission_amount) and pd.notna(commission_rate) and commission_rate != 0:
                rent_amount = commission_amount / (commission_rate / 100.0)
                rent.loc[rent['rent_id'] == rid, 'rent_amount'] = rent_amount

    # 4) Fill missing sale_amount using commission_amount and commission_rate if available
    if sale is not None and 'sale_amount' in sale.columns and comm is not None:
        missing_sale_ids = sale[sale['sale_amount'].isna()]['sale_id'].tolist()
        for sid in missing_sale_ids:
            comm_id_series = sale.loc[sale['sale_id'] == sid, 'commission_id']
            if comm_id_series.empty:
                continue
            comm_id = comm_id_series.values[0]
            commission_amount = comm.loc[comm['commission_id'] == comm_id, 'commission_amount']
            commission_rate = comm.loc[comm['commission_id'] == comm_id, 'commission_rate']
            if commission_amount.empty or commission_rate.empty:
                continue
            commission_amount = pd.to_numeric(commission_amount.values[0], errors='coerce')
            commission_rate = pd.to_numeric(commission_rate.values[0], errors='coerce')
            if pd.notna(commission_amount) and pd.notna(commission_rate) and commission_rate != 0:
                sale_amount = commission_amount / (commission_rate / 100.0)
                sale.loc[sale['sale_id'] == sid, 'sale_amount'] = sale_amount

    # Update the dataframes back
    if comm is not None:
        dataframes['commission'] = comm
    if sale is not None:
        dataframes['sale'] = sale
    if rent is not None:
        dataframes['rent'] = rent

    # 5) KNN / mode imputation for remaining missing values
    for key in list(dataframes.keys()):
        df = dataframes[key]
        numeric_df = df.select_dtypes(include=[np.number])
        non_numeric_df = df.select_dtypes(exclude=[np.number])
        if numeric_df.isna().sum().sum() > 0:
            imputed_numeric_df = knn_impute(numeric_df)
            for col in numeric_df.columns:
                df[col] = imputed_numeric_df[col].values
        if non_numeric_df.isna().sum().sum() > 0:
            imputed_non_numeric_df = mode_impute(non_numeric_df.copy())
            for col in non_numeric_df.columns:
                df[col] = imputed_non_numeric_df[col].values
        dataframes[key] = df

    return dataframes

warnings.filterwarnings(
    "ignore",
    message="You are merging on int and float columns where the float values are not equal to their int representation.",
    category=UserWarning
)
# --- Dimensional creation functions (kept your logic, safer casts) ---
def create_date_dim(start_date, end_date):
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    date_dim = pd.DataFrame(date_range, columns=['Date'])
    date_dim['Year'] = date_dim['Date'].dt.year
    date_dim['Quarter'] = date_dim['Date'].dt.quarter
    date_dim['Month'] = date_dim['Date'].dt.month
    date_dim['Week'] = date_dim['Date'].apply(lambda x: (x - pd.Timestamp(x.year, 1, 1)).days // 7 + 1)
    date_dim['Day'] = date_dim['Date'].dt.day
    date_dim['DateID'] = range(1, len(date_dim) + 1)
    return date_dim

def create_loc_dim(address):
    location_dim = address[['address_id', 'zip_code', 'city', 'state']].drop_duplicates().reset_index(drop=True)
    location_dim['LocationID'] = location_dim['zip_code'].astype('str') + '_' + location_dim['city'].astype(str) + '_' + location_dim['state'].astype(str)
    location_dim.rename(columns={'zip_code': 'ZipCode', 'city': 'City', 'state': 'State'}, inplace=True)
    location_dim = location_dim[['address_id', 'LocationID', 'ZipCode', 'City', 'State']]
    location_dim['LocationID'] = pd.factorize(location_dim['LocationID'])[0] + 1
    location_dim['address_id'] = location_dim['address_id'].astype(int)
    location_dim['LocationID'] = location_dim['LocationID'].astype(int)
    location_dim['ZipCode'] = location_dim['ZipCode'].astype('str')
    location_dim['City'] = location_dim['City'].astype('str')
    location_dim['State'] = location_dim['State'].astype('str')
    return location_dim

def create_agent_dim(agent):
    agent_dim = agent.copy()
    agent_dim['Age'] = datetime.now().year - agent_dim['agent_dob'].dt.year
    agent_dim['AgeCat'] = pd.cut(agent_dim['Age'], bins=[0, 20, 45, 65, np.inf], labels=['Young', 'Adult', 'Middle Aged', 'Senior'], right=False)
    agent_dim['AgentSince'] = datetime.now().year - agent_dim['hire_date'].dt.year
    agent_dim.rename(columns={'agent_gender': 'Gender', 'title': 'Position'}, inplace=True)
    agent_dim = agent_dim[['agent_id', 'Gender', 'AgeCat', 'AgentSince', 'Position']].drop_duplicates().reset_index(drop=True)
    agent_dim['AgentID'] = agent_dim['Gender'].astype(str) + '_' + agent_dim['AgeCat'].astype(str) + '_' + agent_dim['AgentSince'].astype(str) + '_' + agent_dim['Position'].astype(str)
    agent_dim['AgentID'] = pd.factorize(agent_dim['AgentID'])[0] + 1
    agent_dim['agent_id'] = agent_dim['agent_id'].astype(int)
    agent_dim['AgentID'] = agent_dim['AgentID'].astype(int)
    agent_dim['Gender'] = agent_dim['Gender'].astype('str')
    agent_dim['AgeCat'] = agent_dim['AgeCat'].astype('str')
    agent_dim['AgentSince'] = agent_dim['AgentSince'].astype(int)
    agent_dim['Position'] = agent_dim['Position'].astype('str')
    return agent_dim

def create_propdet_dim(features):
    features_dim = features.copy()
    # ensure numeric
    if 'condition_rating' in features_dim.columns:
        features_dim['condition_rating'] = pd.to_numeric(features_dim['condition_rating'], errors='coerce').fillna(0)
    else:
        features_dim['condition_rating'] = 0
    features_dim['Condition'] = pd.cut(features_dim['condition_rating'], bins=[-0.1, 3.1, 7.1, 10.1], labels=['Poor', 'Average', 'Excellent'], right=True)
    # BuiltSince: handle year_built safely
    features_dim['year_built'] = pd.to_numeric(features_dim.get('year_built', pd.Series()), errors='coerce').fillna(datetime.now().year).astype(int)
    features_dim['BuiltSince'] = datetime.now().year - features_dim['year_built']
    # rename safely if columns exist
    rename_map = {}
    for k, v in [('feature_id', 'PropertyDetailsID'), ('no_bedrooms', 'Bedrooms'), ('no_bathrooms', 'Bathrooms'),
                 ('no_kitchens', 'Kitchens'), ('no_floors', 'Floors'), ('parking_area_sqft', 'ParkingArea'),
                 ('lot_area_sqft', 'LotArea')]:
        if k in features_dim.columns:
            rename_map[k] = v
    features_dim.rename(columns=rename_map, inplace=True)

    # ensure numeric columns exist and are int
    for col in ['PropertyDetailsID', 'LotArea', 'Bedrooms', 'Bathrooms', 'Kitchens', 'Floors', 'ParkingArea', 'BuiltSince']:
        if col in features_dim.columns:
            features_dim[col] = pd.to_numeric(features_dim[col], errors='coerce').fillna(0).round().astype(int)
        else:
            # create with default 0 if missing
            features_dim[col] = 0

    features_dim['Condition'] = features_dim['Condition'].astype('str')
    return features_dim

def create_listing_dim(property, visit):
    visits = visit.groupby('property_id').size().reset_index(name='NumVisits')
    listing_dim = pd.merge(property, visits, on='property_id', how='left')
    listing_dim['NumVisits'] = listing_dim['NumVisits'].fillna(0).astype(int)
    listing_dim.rename(columns={'listing_type': 'ListingType'}, inplace=True)
    listing_dim = listing_dim[['property_id', 'ListingType', 'NumVisits']].drop_duplicates().reset_index(drop=True)
    listing_dim['ListingID'] = listing_dim['ListingType'].astype(str) + '_' + listing_dim['NumVisits'].astype(str)
    listing_dim['ListingID'] = pd.factorize(listing_dim['ListingID'])[0] + 1
    listing_dim['property_id'] = listing_dim['property_id'].astype(int)
    listing_dim['ListingID'] = listing_dim['ListingID'].astype(int)
    listing_dim['ListingType'] = listing_dim['ListingType'].astype('str')
    listing_dim['NumVisits'] = listing_dim['NumVisits'].astype(int)
    return listing_dim

# Creating Fact Table
def create_fact_trans(sale, rent, maintenance, property, commission, visit, start_date, end_date, dimdate, dimloc, dimagent, dimprodet, dimlisting):
    transactions=pd.concat([sale.assign(TransactionType='Sale'),rent.assign(TransactionType='Rent')], ignore_index=True)

    transactions['TransactionDate']=transactions['sale_date']
    transactions.loc[transactions['TransactionType']=='Rent','TransactionDate']=transactions['agreement_date']
    transactions['TransactionAmount']=transactions['sale_amount']
    transactions.loc[transactions['TransactionType']=='Rent','TransactionAmount']=transactions['rent_amount']
    transactions.drop(['sale_date','agreement_date','sale_amount','rent_amount'],axis=1,inplace=True)
    
    transactions=pd.merge(transactions, property[['property_id','asking_amount']], on='property_id', how='left')

    maintenance_exp=maintenance[['property_id','cost']].groupby('property_id').sum()
    transactions=pd.merge(transactions, maintenance_exp, on='property_id', how='left')
    transactions['cost']=transactions['cost'].fillna(0).astype(int)
    
    transactions=pd.merge(transactions, commission[['commission_id', 'commission_rate', 'commission_amount']], on='commission_id', how='left')
    
    transactions=pd.merge(transactions, property[['property_id', 'listing_date']], on='property_id', how='left')
    
    last_visit=visit[['property_id','visit_date']].groupby('property_id').max()
    transactions=pd.merge(transactions, last_visit, on='property_id', how='left')
    transactions['NegotiationDays']=(transactions['TransactionDate'] - transactions['visit_date']).dt.days
    transactions['NegotiationDays']=transactions['NegotiationDays'].fillna(0).astype(int)
    transactions['ClosingDays']=(transactions['TransactionDate'] - transactions['listing_date']).dt.days
    
    transactions=transactions[(transactions['TransactionDate']>=pd.Timestamp(start_date)) & (transactions['TransactionDate']<=pd.Timestamp(end_date))]
    transactions=transactions[['property_id','TransactionDate','TransactionAmount','asking_amount','cost','commission_rate','commission_amount','NegotiationDays','ClosingDays']]
    
    transactions=pd.merge(transactions,property[['property_id','address_id','agent_id','feature_id']],on='property_id',how='left')
    transactions.rename(columns={'TransactionDate':'Date'},inplace=True)
    transactions=pd.merge(transactions,dimdate[['Date','DateID']],on='Date',how='left').drop(['Date'],axis=1)
    transactions=pd.merge(transactions,dimloc[['address_id','LocationID']],on='address_id',how='left').drop(['address_id'],axis=1)
    transactions=pd.merge(transactions,dimagent[['agent_id','AgentID']],on='agent_id',how='left').drop(['agent_id'],axis=1)
    transactions.rename(columns={'feature_id':'PropertyDetailsID'},inplace=True)
    transactions=pd.merge(transactions,dimprodet[['PropertyDetailsID']],on='PropertyDetailsID',how='left')
    transactions=pd.merge(transactions,dimlisting[['property_id','ListingID']],on='property_id',how='left').drop(['property_id'],axis=1)
    transactions.rename(columns={'TransactionAmount':'TransactionValue','asking_amount':'AskedAmount','cost':'MaintenanceExp','commission_rate':'CommissionRate',
                                 'commission_amount':'CommissionValue'},inplace=True)
    
    transactions['NegotiationDays']=transactions['NegotiationDays'].apply(lambda x: max(x, 0))
    transactions['ClosingDays']=transactions['ClosingDays'].apply(lambda x: max(x, 0))

    transactions['TransactionID']=range(1,len(transactions)+1)
    transactions=transactions[['TransactionID','DateID','LocationID','AgentID','PropertyDetailsID','ListingID','MaintenanceExp','AskedAmount','TransactionValue',
                               'CommissionRate','CommissionValue','NegotiationDays','ClosingDays']]
    
    return transactions

def create_star_schema(sale, rent, maintenance, property, commission, visit, features, address, agent, start_date, end_date):
    dimdate = create_date_dim(start_date=start_date, end_date=end_date)
    dimloc = create_loc_dim(address=address)
    dimagent = create_agent_dim(agent=agent)
    dimprodet = create_propdet_dim(features=features)
    dimlisting = create_listing_dim(property=property, visit=visit)
    transactions = create_fact_trans(sale=sale, rent=rent, maintenance=maintenance, property=property, commission=commission, visit=visit,
                                     start_date=start_date, end_date=end_date, dimdate=dimdate, dimloc=dimloc, dimagent=dimagent,
                                     dimprodet=dimprodet, dimlisting=dimlisting)

    # tidy dims
    if 'address_id' in dimloc.columns:
        dimloc = dimloc.drop(['address_id'], axis=1).drop_duplicates().reset_index(drop=True)
    if 'agent_id' in dimagent.columns:
        dimagent = dimagent.drop(['agent_id'], axis=1).drop_duplicates().reset_index(drop=True)
    if 'property_id' in dimlisting.columns:
        dimlisting = dimlisting.drop(['property_id'], axis=1).drop_duplicates().reset_index(drop=True)

    dimdate = dimdate[['DateID', 'Date', 'Year', 'Quarter', 'Month', 'Week', 'Day']]
    dimloc = dimloc[['LocationID', 'State', 'City', 'ZipCode']]
    dimagent = dimagent[['AgentID', 'Gender', 'AgeCat', 'AgentSince', 'Position']]
    dimprodet = dimprodet[['PropertyDetailsID', 'LotArea', 'Bedrooms', 'Bathrooms', 'Kitchens', 'Floors', 'ParkingArea', 'BuiltSince', 'Condition']]
    dimlisting = dimlisting[['ListingID', 'ListingType', 'NumVisits']]

    return dimdate, dimloc, dimagent, dimprodet, dimlisting, transactions
