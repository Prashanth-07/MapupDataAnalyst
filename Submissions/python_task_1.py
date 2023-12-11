import pandas as pd


def generate_car_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates a DataFrame for id combinations.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Matrix generated with 'car' values, 
                          where 'id_1' and 'id_2' are used as indices and columns respectively.
    """
    # Pivot the DataFrame to create a matrix with 'car' values
    car_matrix = df.pivot(index='id_1', columns='id_2', values='car').fillna(0)

    # Set diagonal values to 0
    for index in car_matrix.index:
        car_matrix.at[index, index] = 0

    return car_matrix


def get_type_count(df: pd.DataFrame) -> dict:
    """
    Categorizes 'car' values into types and returns a dictionary of counts.

    Args:
        df (pandas.DataFrame)

    Returns:
        dict: A dictionary with car types as keys and their counts as values.
    """
    # Create a new column 'car_type' based on specified conditions
    df['car_type'] = pd.cut(df['car'], bins=[-float('inf'), 15, 25, float('inf')],
                            labels=['low', 'medium', 'high'], right=False)

    # Count occurrences of each car_type category
    type_count = df['car_type'].value_counts().to_dict()

    # Sort the dictionary alphabetically based on keys
    type_count = dict(sorted(type_count.items()))

    return type_count


def get_bus_indexes(df: pd.DataFrame) -> list:
    """
    Returns the indexes where the 'bus' values are greater than twice the mean.

    Args:
        df (pandas.DataFrame)

    Returns:
        list: List of indexes where 'bus' values exceed twice the mean.
    """
    # Identify indices where 'bus' values are greater than twice the mean
    bus_indices = df[df['bus'] > 2 * df['bus'].mean()].index.tolist()

    # Sort the list in ascending order
    bus_indices.sort()

    return bus_indices


def filter_routes(df: pd.DataFrame) -> list:
    """
    Filters and returns routes with average 'truck' values greater than 7.

    Args:
        df (pandas.DataFrame)

    Returns:
        list: List of route names with average 'truck' values greater than 7.
    """
    # Filter routes based on the specified condition
    filtered_routes = df.groupby('route')['truck'].mean()
    filtered_routes = filtered_routes[filtered_routes > 7].index.tolist()

    # Sort the list of routes
    filtered_routes.sort()

    return filtered_routes


def multiply_matrix(matrix: pd.DataFrame) -> pd.DataFrame:
    """
    Multiplies matrix values with custom conditions.

    Args:
        matrix (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Modified matrix with values multiplied based on custom conditions.
    """
    # Apply custom conditions to modify matrix values
    modified_matrix = matrix.applymap(lambda x: x * 0.75 if x > 20 else x * 1.25)

    # Round values to 1 decimal place
    modified_matrix = modified_matrix.round(1)

    return modified_matrix


def time_check(df: pd.DataFrame) -> pd.Series:
    """
    Use shared dataset-2 to verify the completeness of the data by checking whether the timestamps for each unique (`id`, `id_2`) pair cover a full 24-hour and 7 days period.

    Args:
        df (pandas.DataFrame)

    Returns:
        pd.Series: return a boolean series
    """
    # Combine 'startDay' and 'startTime' columns to create a 'startTimestamp' column
    df['startTimestamp'] = pd.to_datetime(df['startDay'] + ' ' + df['startTime'], format='%Y-%m-%d %I:%M %p', errors='coerce')

    # Combine 'endDay' and 'endTime' columns to create an 'endTimestamp' column
    df['endTimestamp'] = pd.to_datetime(df['endDay'] + ' ' + df['endTime'], format='%Y-%m-%d %I:%M %p', errors='coerce')

    # Calculate the duration in seconds for each row
    df['duration'] = (df['endTimestamp'] - df['startTimestamp']).dt.total_seconds()

    # Aggregate information for each group
    grouped_data = df.groupby(['id', 'id_2']).agg(
        duration_check=('duration', lambda x: (x == 24 * 60 * 60).all()),
        start_day_check=('startTimestamp', lambda x: (x.dt.dayofweek == 0).all()),
        end_day_check=('endTimestamp', lambda x: (x.dt.dayofweek == 6).all())
    )

    # Check if the duration covers a full 24-hour period and spans all 7 days
    completeness_check = (
        grouped_data['duration_check'] & grouped_data['start_day_check'] & grouped_data['end_day_check']
    )

    return completeness_check
