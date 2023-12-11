import pandas as pd


def calculate_distance_matrix(df)->pd.DataFrame():
    """
    Calculate a distance matrix based on the dataframe, df.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Distance matrix
    """
    # Write your logic here
    unique_ids = df['id_start'].append(df['id_end']).unique()

    distance_matrix = pd.DataFrame(index=unique_ids, columns=unique_ids).fillna(0)

    for _, row in df.iterrows():
        distance_matrix.at[row['id_start'], row['id_end']] += row['distance']
        distance_matrix.at[row['id_end'], row['id_start']] += row['distance']

    return distance_matrix


def unroll_distance_matrix(df)->pd.DataFrame():
    """
    Unroll a distance matrix to a DataFrame in the style of the initial dataset.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Unrolled DataFrame containing columns 'id_start', 'id_end', and 'distance'.
    """
    # Write your logic here
    unrolled_df = pd.DataFrame(columns=['id_start', 'id_end', 'distance'])

    for i in distance_matrix.index:
        for j in distance_matrix.columns:
            if i != j:
                unrolled_df = unrolled_df.append({'id_start': i, 'id_end': j, 'distance': distance_matrix.at[i, j]},
                                                 ignore_index=True)

    return unrolled_df


def find_ids_within_ten_percentage_threshold(df, reference_id)->pd.DataFrame():
    """
    Find all IDs whose average distance lies within 10% of the average distance of the reference ID.

    Args:
        df (pandas.DataFrame)
        reference_id (int)

    Returns:
        pandas.DataFrame: DataFrame with IDs whose average distance is within the specified percentage threshold
                          of the reference ID's average distance.
    """
    # Write your logic here
    reference_avg_distance = df[df['id_start'] == reference_id]['distance'].mean()
    threshold = 0.1 * reference_avg_distance

    selected_ids = df.groupby('id_start')['distance'].mean()
    selected_ids = selected_ids[(reference_avg_distance - threshold <= selected_ids) &
                                (selected_ids <= reference_avg_distance + threshold)].index

    return selected_ids


def calculate_toll_rate(df)->pd.DataFrame():
    """
    Calculate toll rates for each vehicle type based on the unrolled DataFrame.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame
    """
    # Wrie your logic here
    rate_coefficients = {'moto': 0.8, 'car': 1.2, 'rv': 1.5, 'bus': 2.2, 'truck': 3.6}

    for vehicle_type in rate_coefficients:
        df[vehicle_type] = df['distance'] * rate_coefficients[vehicle_type]

    return df


def calculate_time_based_toll_rates(df)->pd.DataFrame():
    """
    Calculate time-based toll rates for different time intervals within a day.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame
    """
    # Write your logic here
    time_ranges = [(time(0, 0, 0), time(10, 0, 0)), (time(10, 0, 0), time(18, 0, 0)), (time(18, 0, 0), time(23, 59, 59))]
    weekend_discount_factor = 0.7

    df['start_time'] = pd.to_datetime(df['startTime'])  # Update column name here
    df['end_time'] = pd.to_datetime(df['endTime'])      # Update column name here

    for _, row in df.iterrows():
        for start, end in time_ranges:
            if (start <= row['start_time'].time() <= end) and (start <= row['end_time'].time() <= end):
                discount_factor = 0.8 if row['start_time'].weekday() < 5 else weekend_discount_factor
                df.at[_, 'moto'] *= discount_factor
                df.at[_, 'car'] *= discount_factor
                df.at[_, 'rv'] *= discount_factor
                df.at[_, 'bus'] *= discount_factor
                df.at[_, 'truck'] *= discount_factor

    df['start_day'] = df['start_time'].dt.day_name()
    df['end_day'] = df['end_time'].dt.day_name()

    return df