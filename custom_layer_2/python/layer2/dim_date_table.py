import pandas as pd


def dim_date(start="2000-01-01", end="2025-12-31"):
    """
    function takes the start and end year time period
    Parameters: Takes the date_is as a primary key
    Returns: Returns respected query columns of the table

    """
    df = pd.DataFrame({"date_id": pd.date_range(start, end)})
    df["year"] = df.date_id.dt.year
    df["month"] = df.date_id.dt.month
    df["day"] = df.date_id.dt.day
    df["day_of_week"] = df.date_id.dt.dayofweek + 1  # range from Monday- sunday(1-7)
    df["day_name"] = df.date_id.dt.day_name()
    df["month_name"] = df.date_id.dt.month_name()
    df["quarter"] = df.date_id.dt.quarter
    return df

def check_for_dim_date(s3):
    response = s3.list_objects_v2(Bucket='totes-11-processed-data')
    list_of_files = [thing['Key'] for thing in response['Contents']]
    isinlist = False
    for filename in list_of_files:
        if 'dim_date' in filename:
            isinlist = True
            break
    return isinlist
