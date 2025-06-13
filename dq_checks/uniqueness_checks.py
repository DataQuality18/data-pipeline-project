def check_duplicates(df):
    """
    Check for duplicate rows in the DataFrame.

    Returns a DataFrame containing only the duplicated rows.
    """
    return df[df.duplicated()]
