def check_nulls(df):
    """
    Check for null values in each column of the DataFrame.

    Returns a Series with columns that have nulls and their count.
    """
    null_counts = df.isnull().sum()
    return null_counts[null_counts > 0]
