def check_allowed_values(df, column, allowed):
    if column not in df.columns:
        return f"{column} column not found"
    
    # Return rows that do NOT contain allowed values
    return df[~df[column].isin(allowed)]
