def check_age_range(df, min_age=18, max_age=60):
    """
    Check if 'age' values are outside the specified range.

    Returns a DataFrame with rows where 'age' is < min_age or > max_age.
    """
    if "age" not in df.columns:
        return f"'age' column not found in DataFrame."

    return df[(df["age"] < min_age) | (df["age"] > max_age)]
