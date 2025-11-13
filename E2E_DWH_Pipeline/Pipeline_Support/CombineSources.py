import pandas as pd


def combine_parts(csv_df: pd.DataFrame = None, db_df: pd.DataFrame = None, mock_df: pd.DataFrame = None) -> pd.DataFrame:
    """Combine up to three dataframe parts for the same logical table.

    - Accepts None for any missing part.
    - Concatenates with sort=False to preserve union of columns.
    - Drops exact duplicate rows and resets index.
    """
    parts = []
    if csv_df is not None and not csv_df.empty:
        parts.append(csv_df)
    if db_df is not None and not db_df.empty:
        parts.append(db_df)
    if mock_df is not None and not mock_df.empty:
        parts.append(mock_df)

    if not parts:
        return pd.DataFrame()

    merged = pd.concat(parts, ignore_index=True, sort=False)
    merged = merged.drop_duplicates().reset_index(drop=True)
    return merged
