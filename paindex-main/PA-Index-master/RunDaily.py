import pandas as pd
import paramiko
import zipfile
import os
from datetime import datetime, timedelta
import pickle
import numpy as np
import warnings

def _daily():
    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Path to the 'data' directory relative to the script's location
    data_dir = os.path.join(script_dir, '..', 'data')

    def download_most_recent_zip(hostname, port, username, password, remote_folder, local_folder):
        # Create an SSH client
        ssh = paramiko.SSHClient()

        try:
            # Automatically add the server's host key
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            # Connect to the SSH server
            ssh.connect(hostname, port, username, password)

            # Open an SFTP session
            with ssh.open_sftp() as sftp:
                # Change to the remote folder
                sftp.chdir(remote_folder)

                # List files in the remote folder
                files = sftp.listdir()

                # Find the most recent ZIP file with "UnderlyingEOD" in the name
                most_recent_zip = None
                most_recent_date = datetime(1970, 1, 1)  # Initialize with a very old date

                for file in files:
                    if file.startswith("UnderlyingEOD") and not "Summaries" in file:
                        try:
                            file_date = datetime.strptime(file.split("_")[-1].split(".")[0], "%Y-%m-%d")
                            if file_date > most_recent_date:
                                most_recent_date = file_date
                                most_recent_zip = file
                        except ValueError:
                            pass  # Ignore files with date parsing issues

                if most_recent_zip:
                    # Replace backslashes with forward slashes in file paths
                    remote_path = os.path.join(remote_folder, most_recent_zip).replace("\\", "/")
                    local_path = os.path.join(local_folder, most_recent_zip).replace("\\", "/")

                    # Download the most recent ZIP file
                    sftp.get(remote_path, local_path)

                    # Extract files from the downloaded ZIP file
                    extract_zip(local_path, local_folder)

                    # Clean up: Remove downloaded ZIP file
                    os.remove(local_path)

        except Exception as e:
            print(f"Error: {e}")

        finally:
            # Close the SSH connection
            ssh.close()

    def extract_zip(zip_filename, extraction_folder):
        with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
            # Extract all files in the ZIP archive to the extraction folder
            zip_ref.extractall(extraction_folder)

    # SFTP server details
    sftp_hostname = "sftp.datashop.livevol.com"
    sftp_port = 22  # Change the port if your SFTP server uses a different port
    sftp_username = "nan2_lehigh_edu"
    sftp_password = "PAIndex2023!"
    sftp_remote_folder = "/subscriptions/order_000046197/item_000053507"  # Change to the actual path on the SFTP server
    local_download_folder = data_dir
    # local_download_folder = "/srv/paindex-test/data/"  # Set to "input" folder inside the current directory

    # Create the local download folder if it doesn't exist
    # os.makedirs(local_download_folder, exist_ok=True)

    # Download and extract the most recent ZIP file
    download_most_recent_zip(sftp_hostname, sftp_port, sftp_username, sftp_password, sftp_remote_folder, local_download_folder)

    # Define the file path where date_dataframes.pkl is saved
    input_file_path = os.path.join(data_dir, 'date_dataframes.pkl')
    # input_file_path = "/srv/paindex-test/data/date_dataframes.pkl"

    # Load the date_dataframes dictionary from the pickled file
    with open(input_file_path, 'rb') as input_file:
        date_dataframes = pickle.load(input_file)
    local_download_folder = data_dir
    # local_download_folder = "/srv/paindex-test/data/"  # Set to "input" folder inside the current directory

    # List all CSV files in the "input" folder
    csv_files = [file for file in os.listdir(local_download_folder) if file.startswith("UnderlyingEOD_") and file.endswith(".csv")]

    # Sort the CSV files by date, assuming the date is in the format "UnderlyingEOD_YYYY-MM-DD.csv"
    sorted_csv_files = sorted(csv_files, key=lambda x: datetime.strptime(x.split("_")[-1].split(".")[0], "%Y-%m-%d"), reverse=True)

    # Use the most recent CSV file
    if sorted_csv_files:
        most_recent_csv = sorted_csv_files[0]
        csv_file_path = os.path.join(local_download_folder, most_recent_csv)

        # Extract date from the filename
        date_format = most_recent_csv.split("_")[-1].split(".")[0]

        # Read the CSV file into a DataFrame
        eod_df = pd.read_csv(csv_file_path)

        # Store the DataFrame in the dictionary with the date as the key
        date_dataframes[date_format] = eod_df

        # After using the CSV file, optionally delete it
        try:
            os.remove(csv_file_path)
            print(f"Deleted: {csv_file_path}")
        except FileNotFoundError:
            print(f"File not found: {csv_file_path}")
        except Exception as e:
            print(f"Error deleting file: {e}")
    else:
        print("No CSV files found in the 'input' folder.")

    start_2023 = '2023-11-10'
    end_2023 = '2024-07-01'
    start_2024 = '2024-07-01'
    
    excel_file_path2023 = os.path.join(data_dir, 'RAY as of Oct 23 20231_PA.xlsx')
    df2023 = pd.read_excel(excel_file_path2023)
    df2023 = df2023.sort_values('Market Cap\n',ascending=False)
    df2023.columns = df2023.columns.str.rstrip('\n')
    df2023['Ticker'] = df2023['Ticker'].str.split(' ',n=1,expand=True)[0].replace(' ','')
    float_df2023 = df2023[['Ticker','Equity Float']].head(100)
    
    eod_market_cap_pivot2023 = pd.DataFrame()

    excel_file_path2024 = os.path.join(data_dir, 'RAY as of Jul 01 20241_PA.xlsx')
    df2024 = pd.read_excel(excel_file_path2024)
    df2024 = df2024.sort_values('Market Cap\n',ascending=False)
    df2024.columns = df2024.columns.str.rstrip('\n')
    df2024['Ticker'] = df2024['Ticker'].str.split(' ',n=1,expand=True)[0].replace(' ','')
    float_df2024 = df2024[['Ticker','Equity Float', 'GICS Sector']].head(100)

    eod_market_cap_pivot2024 = pd.DataFrame()
    
    unique_dates = list(date_dataframes.keys())




    
    for date in unique_dates:

        if date >= start_2023 and date <= end_2023:
    
            eod_df2023 = date_dataframes[date]

            merged_df2023 = pd.merge(float_df2023,eod_df2023,left_on='Ticker',right_on='underlying_symbol')
    
            merged_df2023['Market Cap'] = merged_df2023['Equity Float'] * merged_df2023['close']
        
            eod_market_cap_daily2023 = merged_df2023.pivot_table(values='Market Cap',index='quote_date',columns='Ticker',aggfunc='sum')
    
            eod_market_cap_pivot2023 = pd.concat([eod_market_cap_pivot2023,eod_market_cap_daily2023])
    
    eod_market_cap_pivot2023 = eod_market_cap_pivot2023.rename_axis(index='Date')
    eod_market_cap_pivot2023.index = pd.to_datetime(eod_market_cap_pivot2023.index,errors='coerce')
    
    
    eod_market_cap_pivot2023['mkt_cap_deleted_stock'] = 0
    
    for ticker in float_df2023['Ticker']:
        last_valid_market_cap = None
        first_deletion = False
    
        for date_index, date in enumerate(eod_market_cap_pivot2023.index):
    
            if date != eod_market_cap_pivot2023.index[-1]:
            
                next_date = eod_market_cap_pivot2023.index[date_index + 1]
            
                market_cap = eod_market_cap_pivot2023.at[date,ticker]
    
                market_cap_next_date = eod_market_cap_pivot2023.at[next_date,ticker]
    
                if (pd.isna(market_cap) or market_cap == 0) and (pd.isna(market_cap_next_date) or market_cap_next_date == 0):
                    if not first_deletion and last_valid_market_cap is not None:
                        eod_market_cap_pivot2023.at[date, 'mkt_cap_deleted_stock'] = last_valid_market_cap
                        first_deletion = True
    
                if (pd.isna(market_cap) or market_cap == 0) and market_cap_next_date != 0:
                    if not first_deletion and last_valid_market_cap is not None:
                        eod_market_cap_pivot2023.at[date,ticker] = last_valid_market_cap
                        first_deletion = True
                
                else:
                    last_valid_market_cap = market_cap
                    first_deletion = False
    
            else:
                market_cap = eod_market_cap_pivot2023.at[date,ticker]
                if pd.isna(market_cap) or market_cap == 0:
                    if not first_deletion and last_valid_market_cap is not None:
                        eod_market_cap_pivot2023.at[date,ticker] = last_valid_market_cap
                        first_deletion = True
                else:
                    last_valid_market_cap = market_cap
                    first_deletion = False
    
    eod_market_cap_pivot2023["close_mkt_cap"] = eod_market_cap_pivot2023.drop(columns='mkt_cap_deleted_stock').sum(axis=1)
    
    
    eod_market_cap_pivot2023["adj_mkt_cap"] = 0
    eod_market_cap_pivot2023["divisor"] = 0
    eod_market_cap_pivot2023["gross_index_level"] = 0
    eod_market_cap_pivot2023["Index Value"] = 0
    
    
    for i in range(0, len(eod_market_cap_pivot2023)):
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=FutureWarning)
            if i == 0:
                eod_market_cap_pivot2023.at[eod_market_cap_pivot2023.index[i], "adj_mkt_cap"] = float(eod_market_cap_pivot2023.at[eod_market_cap_pivot2023.index[i], "close_mkt_cap"]) + float(eod_market_cap_pivot2023.at[eod_market_cap_pivot2023.index[i], "mkt_cap_deleted_stock"])
                eod_market_cap_pivot2023.at[eod_market_cap_pivot2023.index[i], "divisor"] = 1
                eod_market_cap_pivot2023.at[eod_market_cap_pivot2023.index[i], "gross_index_level"] = float(eod_market_cap_pivot2023.at[eod_market_cap_pivot2023.index[i], "close_mkt_cap"]) / float(eod_market_cap_pivot2023.at[eod_market_cap_pivot2023.index[i], "divisor"])
                eod_market_cap_pivot2023.at[eod_market_cap_pivot2023.index[i], "Index Value"] = float(eod_market_cap_pivot2023.at[eod_market_cap_pivot2023.index[i], "gross_index_level"]) / (float(eod_market_cap_pivot2023.at[eod_market_cap_pivot2023.index[0], "close_mkt_cap"]) / 100)
            else:
                eod_market_cap_pivot2023.at[eod_market_cap_pivot2023.index[i], "adj_mkt_cap"] = float(eod_market_cap_pivot2023.at[eod_market_cap_pivot2023.index[i-1], "close_mkt_cap"]) - float(eod_market_cap_pivot2023.at[eod_market_cap_pivot2023.index[i], "mkt_cap_deleted_stock"])
                eod_market_cap_pivot2023.at[eod_market_cap_pivot2023.index[i], "divisor"] = (float(eod_market_cap_pivot2023.at[eod_market_cap_pivot2023.index[i], "adj_mkt_cap"]) / float(eod_market_cap_pivot2023.at[eod_market_cap_pivot2023.index[i-1], "close_mkt_cap"])) * float(eod_market_cap_pivot2023.at[eod_market_cap_pivot2023.index[i-1], "divisor"])
                eod_market_cap_pivot2023.at[eod_market_cap_pivot2023.index[i], "gross_index_level"] = float(eod_market_cap_pivot2023.at[eod_market_cap_pivot2023.index[i], "close_mkt_cap"]) / float(eod_market_cap_pivot2023.at[eod_market_cap_pivot2023.index[i], "divisor"])
                eod_market_cap_pivot2023.at[eod_market_cap_pivot2023.index[i], "Index Value"] = float(eod_market_cap_pivot2023.at[eod_market_cap_pivot2023.index[i], "gross_index_level"]) / (float(eod_market_cap_pivot2023.at[eod_market_cap_pivot2023.index[0], "close_mkt_cap"]) / 100)

    last_value = eod_market_cap_pivot2023.iloc[-1]['Index Value']

    for date in unique_dates:
    
        if date >= start_2024:
        
            eod_df2024 = date_dataframes[date]
    
            merged_df2024 = pd.merge(float_df2024,eod_df2024,left_on='Ticker',right_on='underlying_symbol')
    
            merged_df2024['Market Cap'] = merged_df2024['Equity Float'] * merged_df2024['close']
        
            eod_market_cap_daily2024 = merged_df2024.pivot_table(values='Market Cap',index='quote_date',columns='Ticker',aggfunc='sum')
    
            eod_market_cap_pivot2024 = pd.concat([eod_market_cap_pivot2024,eod_market_cap_daily2024])
    
    eod_market_cap_pivot2024 = eod_market_cap_pivot2024.rename_axis(index='Date')
    eod_market_cap_pivot2024.index = pd.to_datetime(eod_market_cap_pivot2024.index,errors='coerce')
    
    
    eod_market_cap_pivot2024['mkt_cap_deleted_stock'] = 0
    
    for ticker in float_df2024['Ticker']:
        last_valid_market_cap = None
        first_deletion = False
    
        for date_index, date in enumerate(eod_market_cap_pivot2024.index):
    
            if date != eod_market_cap_pivot2024.index[-1]:
            
                next_date = eod_market_cap_pivot2024.index[date_index + 1]
            
                market_cap = eod_market_cap_pivot2024.at[date,ticker]
    
                market_cap_next_date = eod_market_cap_pivot2024.at[next_date,ticker]
    
                if (pd.isna(market_cap) or market_cap == 0) and (pd.isna(market_cap_next_date) or market_cap_next_date == 0):
                    if not first_deletion and last_valid_market_cap is not None:
                        eod_market_cap_pivot2024.at[date, 'mkt_cap_deleted_stock'] = last_valid_market_cap
                        first_deletion = True
    
                if (pd.isna(market_cap) or market_cap == 0) and market_cap_next_date != 0:
                    if not first_deletion and last_valid_market_cap is not None:
                        eod_market_cap_pivot2024.at[date,ticker] = last_valid_market_cap
                        first_deletion = True
                
                else:
                    last_valid_market_cap = market_cap
                    first_deletion = False
    
            else:
                market_cap = eod_market_cap_pivot2024.at[date,ticker]
                if pd.isna(market_cap) or market_cap == 0:
                    if not first_deletion and last_valid_market_cap is not None:
                        eod_market_cap_pivot2024.at[date,ticker] = last_valid_market_cap
                        first_deletion = True
                else:
                    last_valid_market_cap = market_cap
                    first_deletion = False
    
    eod_market_cap_pivot2024["close_mkt_cap"] = eod_market_cap_pivot2024.drop(columns='mkt_cap_deleted_stock').sum(axis=1)
    
    
    eod_market_cap_pivot2024["adj_mkt_cap"] = 0
    eod_market_cap_pivot2024["divisor"] = 0
    eod_market_cap_pivot2024["gross_index_level"] = 0
    eod_market_cap_pivot2024["Index Value"] = 0
    
    
    for i in range(0, len(eod_market_cap_pivot2024)):
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=FutureWarning)
            if i == 0:
                eod_market_cap_pivot2024.at[eod_market_cap_pivot2024.index[i], "adj_mkt_cap"] = float(eod_market_cap_pivot2024.at[eod_market_cap_pivot2024.index[i], "close_mkt_cap"]) + float(eod_market_cap_pivot2024.at[eod_market_cap_pivot2024.index[i], "mkt_cap_deleted_stock"])
                eod_market_cap_pivot2024.at[eod_market_cap_pivot2024.index[i], "divisor"] = 1
                eod_market_cap_pivot2024.at[eod_market_cap_pivot2024.index[i], "gross_index_level"] = float(eod_market_cap_pivot2024.at[eod_market_cap_pivot2024.index[i], "close_mkt_cap"]) / float(eod_market_cap_pivot2024.at[eod_market_cap_pivot2024.index[i], "divisor"])
                eod_market_cap_pivot2024.at[eod_market_cap_pivot2024.index[i], "Index Value"] = float(eod_market_cap_pivot2024.at[eod_market_cap_pivot2024.index[i], "gross_index_level"]) / (float(eod_market_cap_pivot2024.at[eod_market_cap_pivot2024.index[0], "close_mkt_cap"]) / last_value)
            else:
                eod_market_cap_pivot2024.at[eod_market_cap_pivot2024.index[i], "adj_mkt_cap"] = float(eod_market_cap_pivot2024.at[eod_market_cap_pivot2024.index[i-1], "close_mkt_cap"]) - float(eod_market_cap_pivot2024.at[eod_market_cap_pivot2024.index[i], "mkt_cap_deleted_stock"])
                eod_market_cap_pivot2024.at[eod_market_cap_pivot2024.index[i], "divisor"] = (float(eod_market_cap_pivot2024.at[eod_market_cap_pivot2024.index[i], "adj_mkt_cap"]) / float(eod_market_cap_pivot2024.at[eod_market_cap_pivot2024.index[i-1], "close_mkt_cap"])) * float(eod_market_cap_pivot2024.at[eod_market_cap_pivot2024.index[i-1], "divisor"])
                eod_market_cap_pivot2024.at[eod_market_cap_pivot2024.index[i], "gross_index_level"] = float(eod_market_cap_pivot2024.at[eod_market_cap_pivot2024.index[i], "close_mkt_cap"]) / float(eod_market_cap_pivot2024.at[eod_market_cap_pivot2024.index[i], "divisor"])
                eod_market_cap_pivot2024.at[eod_market_cap_pivot2024.index[i], "Index Value"] = float(eod_market_cap_pivot2024.at[eod_market_cap_pivot2024.index[i], "gross_index_level"]) / (float(eod_market_cap_pivot2024.at[eod_market_cap_pivot2024.index[0], "close_mkt_cap"]) / last_value)



    eod_market_cap_pivot = pd.DataFrame()
    eod_market_cap_pivot = pd.concat([eod_market_cap_pivot2023,eod_market_cap_pivot2024.iloc[1:]])


    # Extract the "Date" and "Index Value" columns
    index_df = eod_market_cap_pivot[["Index Value"]].copy()

    # Save the DataFrame to a CSV file
    index_output_path = os.path.join(data_dir, 'input.csv')
    mkt_cap_output_path = os.path.join(data_dir, 'market_cap.csv')
    # index_df.to_csv("/srv/paindex-test/data/input.csv")
    eod_market_cap_pivot.to_csv(mkt_cap_output_path)
    index_df.to_csv(index_output_path)

    # # ---Create an empty DataFrame to store market cap DataFrames for each date
    # eod_sector_weight_pivot = pd.DataFrame()
    
    # # ---Extract unique dates from date_dataframes dictionary
    # unique_dates = sorted(date_dataframes.keys())
    
    # # ---Get the latest date
    # latest_date = unique_dates[-1]
    
    # # ---Access the corresponding DataFrame from date_dataframes
    # eod_df = date_dataframes[latest_date]
    
    # # ---Merge dataframes on the common column 'Ticker' and 'underlying_symbol'
    # merged_df = pd.merge(float_df2024, eod_df, left_on='Ticker', right_on='underlying_symbol')
    
    # # ----Perform the multiplication and rename the column to 'Market Cap'
    # merged_df['Market Cap'] = merged_df['Equity Float'] * merged_df['close']
    
    # # Group by 'GICS Sector' and sum 'Equity Float'
    # merged = merged_df.groupby('GICS Sector')
    # merged_float = merged.sum('Equity Float')
    
    # # -----Use pivot_table to create a multi-level DataFrame
    # eod_market_cap_daily = merged_df.pivot_table(index='quote_date', columns='GICS Sector', values='Market Cap', aggfunc='sum')
    
    # # ------Append the daily market cap DataFrame to the main DataFrame
    # eod_sector_weight_pivot = pd.concat([eod_sector_weight_pivot, eod_market_cap_daily])
    
    # # -------Convert the 'Date' index to datetime format (if not already done)
    # eod_sector_weight_pivot.index = pd.to_datetime(eod_sector_weight_pivot.index)
    
    # # ----------Sort the DataFrame by the 'Date' index
    # eod_sector_weight_pivot.sort_index(inplace=True)
    
    # # Set the target date to the latest date
    # date_market_weight = latest_date
    
    # # Check if the date is in the index
    # if date_market_weight in eod_sector_weight_pivot.index:
    #     row_sum = eod_sector_weight_pivot.loc[date_market_weight].sum()
    
    #     market_weights = []
    #     sector_names = []
    
    #     row_values = eod_sector_weight_pivot.loc[date_market_weight]
    #     for column, value in row_values.items():
    #         weight_value = (value / row_sum) * 100
    #         market_weights.append(weight_value)
    #         sector_names.append(column)
            
    #     #-----------writing to the table.csv everytime the code runs with the new sector weight values
    #     df_mWeights = pd.DataFrame({
    #         "Sector": sector_names,
    #         "Market Weight": market_weights
    #     })
    
    #     market_weight_path = os.path.join(script_dir, 'output/market_weight.csv')
    #     df_mWeights.to_csv(market_weight_path, index=False)
    # else:
    #     print(f"Date {date_market_weight} not found in DataFrame index.")

    # output_file_path = "/srv/paindex-test/data/date_dataframes.pkl"
    output_file_path = os.path.join(data_dir, 'date_dataframes.pkl')
    with open(output_file_path, 'wb') as output_file:
        pickle.dump(date_dataframes, output_file)

    print(f"Saved date_dataframes to: {output_file_path}")
    
    return "Updated"
