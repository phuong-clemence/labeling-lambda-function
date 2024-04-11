import boto3
import os
import pandas as pd

rename_dict = {
    '4K1KP01DRV01_M2001_EI_AVG' : 'KilnDriAmp',
    '4K1KP01KHE01_B8701_AVG' : 'Pyrometer',
    '4G1GA01XAC01_O2_AVG' : 'GA01_Oxi',
    '4G1KJ01JST00_T8401_AVG' : 'KilnInletTemp',
    '4G1PS01GPJ02_T8201_AVG' : 'TowerOilTemp',
    '4R1GQ01JNT01_T8201_AVG' : 'RecHeadTemp',
    '41KP01DRV01_SP_AVG' : 'FurnaceSpeedSP',
    'SZ_Coal_Setpt_AVG' : 'CoalSP',
    'PC_Coal_setpt_AVG' : 'AlternativeCoalSP',
    '4G1FN01DRV01_M1001_SI_AVG' : 'FanSP',
    '4G1GA02XAC01_O2_AVG' : 'GA02_Oxi',
    '4G1GA03XAC01_O2_AVG' : 'GA03_Oxi',
    '4K1KP01DRV01_Speed_AVG' : 'FurnaceSpeed',
    'BZTL_AVG' : 'AvgBZT', 
    'Kilnfeed_SP_Total_AVG' : 'ActualFuelSP',
    'Ratio_PC_AVG' : 'HeatReplaceRatio',
    'Result_AHC_AVG' : 'TotalHeatConsumption',
}

def replace_zeros(df, column):
    count = 0
    for i in reversed(range(len(df))):
        if df[column].iloc[i] == 0:
            time_of_zero = df['DateTime'].iloc[i]
            time_threshold = time_of_zero - pd.Timedelta(minutes=10)
            window = df[(df['DateTime'] >= time_threshold) & (df['DateTime'] < time_of_zero)]

            last_non_zero = window[column].where(window[column] > 0).last_valid_index()
            if last_non_zero is not None:
                time_of_replacement = df['DateTime'].loc[last_non_zero]
                time_difference = time_of_zero - time_of_replacement

                if time_difference <= pd.Timedelta(minutes=10):
                    count += 1
                    df.at[i, column] = df.at[last_non_zero, column]
    

def lambda_handler(event: any, context: any):   
    # Prepare the DynamoDB client 
    dynamodb = boto3.resource("dynamodb")
    
    # Defining the input table and the 2 output tables
    raw_table_name = os.environ["RAW_TABLE"]
    status_table_name = os.environ["STATUS_TABLE"]
    unstable_table_name = os.environ["UNSTABLE_TABLE"]
    
    raw_table = dynamodb.Table(raw_table_name)
    status_table = dynamodb.Table(status_table_name)
    unstable_table = dynamodb.Table(unstable_table_name)
    
    # Get data from DynamoDB
    response = raw_table.scan()
    items = response['Items']
    
    # Convert items to DataFrame for preprocessing
    df = pd.DataFrame(items)
    
    # Renaming the columns according to the dictionary
    df.rename(columns=rename_dict, inplace=True)
    
    # Combining Date and Time colums to have only one primary key
    df.loc[:, 'DateTime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])

    # Now you can drop the original 'Date' and 'Time' columns if they are no longer needed
    df = df.drop(['Date', 'Time'], axis=1)
    df.dropna(subset=['DateTime'], inplace=True)
    df = df.sort_values(by='DateTime')
    df.reset_index(drop=True, inplace=True)
    
    # Filling the missing rows with the last registered data
    for column in ['CoalSP', 'FurnaceSpeedSP', 'FanSP']:
        replace_zeros(df, column)
    
    # Calculate the time difference between current row and the next one
    df['time_diff'] = df['DateTime'].diff(-1).dt.total_seconds().abs()

    # Define the time window in seconds (5 minutes)
    time_window = 5 * 60  # 5 minutes in seconds

    # Shift the columns conditionally
    df['CoalSP_next'] = df['CoalSP'].shift(-1)
    df['FurnaceSpeedSP_next'] = df['FurnaceSpeedSP'].shift(-1)
    df['FanSP_next'] = df['FanSP'].shift(-1)

    # Set the _next columns to NaN where the time difference is greater than the time window
    df.loc[df['time_diff'] > time_window, ['CoalSP_next', 'FurnaceSpeedSP_next', 'FanSP_next']] = None

    # Calculate the changes
    df['CoalSP_change'] = df['CoalSP'] - df['CoalSP_next']
    df['FurnaceSpeedSP_change'] = df['FurnaceSpeedSP'] - df['FurnaceSpeedSP_next']
    df['FanSP_change'] = df['FanSP'] - df['FanSP_next']

    # Define a threshold for change to consider a system unstable
    threshold = 0.1  # Adjust this value as needed

    # Label the rows based on the changes
    df['Label'] = 'Stable'
    df.loc[
        (df['CoalSP_change'].notna() & (df['CoalSP_change'] != 0)) |
        (df['FurnaceSpeedSP_change'].notna() & (df['FurnaceSpeedSP_change'] != 0)) |
        (df['FanSP_change'].notna() & (abs(df['FanSP_change']) > threshold)),
        'Label'
    ] = 'Unstable'
    
    dropna_df = df.dropna()
    
    # List of columns you want to drop
    columns_to_drop = ['CoalSP_next', 'FurnaceSpeedSP_next', 'FanSP_next', 'CoalSP_change', 'FurnaceSpeedSP_change', 'FanSP_change']

    # Dropping the columns
    status_df = df.drop(columns=columns_to_drop)
    
    # Convert DataFrame back to list of dictionaries
    status_items = status_df.to_dict(orient='records')
    
    # Write data to destination DynamoDB table
    with status_table.batch_writer() as batch:
        for item in status_items:
            # Convert all the columns to string
            item_to_put: dict = {} 
            for item_key in item: 
                item_to_put[item_key] = str(item[item_key])
            batch.put_item(Item=item_to_put)
            
    change_df = status_df.copy()
        
    # Initialize columns for the change in setpoints and time to return to stable
    setpoints = ['CoalSP', 'FanSP', 'FurnaceSpeedSP']

    for sp in setpoints:
        change_df[f'{sp}_Change_To_Stable'] = 0.0
    change_df['Time_To_Stable'] = 0.0

    # Iterate through the DataFrame
    for index, row in change_df.iterrows():
        if row['Label'] == 'Unstable':
            # Find the index where the system returns to stable
            stable_idx = change_df.loc[index:, 'Label'].eq('Stable').idxmax() if 'Stable' in change_df.loc[index:, 'Label'].values else None

            if stable_idx:
                # Calculate time to stable
                change_df.at[index, 'Time_To_Stable'] = stable_idx - index

                # Calculate changes in setpoints
                for sp in setpoints:
                    change_df.at[index, f'{sp}_Change_To_Stable'] = change_df.at[stable_idx, sp] - change_df.at[index, sp]
        
    unstable_df = change_df.dropna()
    # Filter the DataFrame for rows where 'label' is 'Stable', actually no need this because all of the rows are Unstable now
    unstable_df = change_df[change_df['Label'] == 'Unstable']
    
    # List of columns you want to drop
    columns_to_drop = ['time_diff', 'Label', 'Time_To_Stable']
    
    # Dropping the columns
    unstable_df = unstable_df.drop(columns=columns_to_drop)

    # Convert DataFrame back to list of dictionaries
    unstable_items = unstable_df.to_dict(orient='records')
    
    # Write data to destination DynamoDB table
    with unstable_table.batch_writer() as batch:
        for item in unstable_items:
            # Convert all the columns to string
            item_to_put: dict = {} 
            for item_key in item: 
                item_to_put[item_key] = str(item[item_key])
            batch.put_item(Item=item_to_put)
        
    return {
        'statusCode': 200,
        'body': 'Data labeling complete and stored in both DynamoDB tables'
    }

