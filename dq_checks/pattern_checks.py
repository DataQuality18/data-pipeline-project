import re
import pandas as pd

def check_pattern(df, column, pattern):
    if column not in df.columns:
        return f"'{column}' not found in DataFrame"
    
    # Return rows that do NOT match the pattern
    return df[~df[column].astype(str).str.match(pattern, na=False)]
