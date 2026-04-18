import pandas as pd


def get_num_api_calls(num_days, num_features, num_values):
    return int((num_values * (num_days / 14) * (num_features / 10)))


# Actual limit is 600/min, 5000/hour and 10000/day
def get_api_calls_per_min(
    num_days,
    num_features,
    num_values,
    max_api_calls_per_day=10000,
    max_api_calls_per_hour=5000,
    max_api_calls_per_min=600,
):
    num_api_calls_required = get_num_api_calls(num_days, num_features, num_values)
    api_calls_per_min = 0

    if num_api_calls_required >= max_api_calls_per_day:
        api_calls_per_min = max_api_calls_per_day // (24 * 60)
    elif num_api_calls_required >= max_api_calls_per_hour:
        api_calls_per_min = max_api_calls_per_hour // 60
    else:
        api_calls_per_min = max_api_calls_per_min

    print(f"API call rate: {api_calls_per_min} /min")
    print(f"Number of API calls: {num_api_calls_required:.0f}")

    return int(api_calls_per_min)


def get_date_range(df: pd.DataFrame) -> list:
    d = df[["SCH_DEP_DATE", "SCH_ARI_DATE"]].values

    d_min = d.min()
    d_max = d.max()

    return [d_min, d_max]


flight_date_range = {"start_date": None, "end_date": None}
