# Import required libraries
import pandas as pd
from dateutil.relativedelta import relativedelta
from datetime import datetime
import pysftp
import os
import logging
import shutil
import numpy as np
from dotenv import load_dotenv
import time

# Load environment variables from .env file
load_dotenv('.env')

# Setup logging
log_filename = 'file_processing.log'
logging.basicConfig(
    filename=log_filename, 
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p'
)

# FTP server details
"""
Load FTP info from the variables file
"""

MAX_RETRIES = 3
RETRY_DELAY = 5  # delay in seconds

cnopts = pysftp.CnOpts()
cnopts.hostkeys = None 

for attempt in range(MAX_RETRIES):
    try:
        logging.info('\n\n---- Starting FTP Download ----')

        # Connect to the server
        with pysftp.Connection(SYSTEM_FTP_HOST, username=SYSTEM_FTP_USERNAME, password=SYSTEM_FTP_PASSWORD, cnopts=cnopts, port=2012) as sftp:
            logging.info('Connected to SFTP server.')  
            
            # Change to the directory where the file is located
            sftp.chdir(SYSTEM_FTP_DIRECTORY)
            
            # Define the download function
            def download_file(sftp, filename):
                sftp.get(filename)  # This will download the file with its original name

                # Remove the "-en" suffix from the downloaded file
                new_filename = filename.replace("-en", "")
                if os.path.exists(new_filename):  # Check if the file exists
                    os.remove(new_filename)  # Remove the existing file
                os.rename(filename, new_filename)  # Rename the file

           

            # Download the file
            download_file(sftp, SYSTEM_FILE_NAME)
        logging.info("File downloaded successfully.")
        
        # Print the content of the current directory for debugging
        logging.info("Files in current directory: " + str(os.listdir()))

        break  # if successful, break out of retry loop

    except Exception as e:
        logging.error("Error in FTP download: ", exc_info=True)
        if attempt < MAX_RETRIES - 1:  # don't delay on the last attempt
            time.sleep(RETRY_DELAY)  # wait before trying again

logging.info('\n\n---- Starting CSV Processing ----')

# Define the columns that need to be compared for changes
compare_cols = ['First Name', 'Last Name', 'Username', 'Email', 'Building']

# Define the date format in your CSV
date_format = "%Y-%m-%d %H:%M:%S"

# Get today's date in the format 'YYYYMMDD'
today_str = datetime.today().strftime('%Y%m%d')

# Define the start and end dates for considering hires and terminations
#start_date = datetime.strptime("2023-08-01", "%Y-%m-%d").date()  # Test start date
start_date = (datetime.now().replace(day=1) - relativedelta(months=1)).date()  # Production start date 
#end_date = datetime.strptime("2023-08-04", "%Y-%m-%d").date() # Test end date
end_date = (datetime.now().replace(day=1) - relativedelta(days=1)).date()

# Load the base CSV file
df_base = pd.read_csv('1234_CompanyName_base.csv')

# Clear the 'Action' column in the base DataFrame
df_base['Action'] = 'None'

# Backup the base file
backup_filename = f'1234_CompanyName_base_{today_str}.csv'
shutil.copy('1234_CompanyName_base.csv', backup_filename)

# Load the new CSV file
df_new = pd.read_csv('1234_CompanyName_base.csv')

# Clean up usernames and email addresses by removing periods before '@' for both dataframes
df_base['Username'] = df_base['Username'].str.replace(r'(\.)(?=[^@]*@)', '', regex=True)
df_base['Email'] = df_base['Email'].str.replace(r'(\.)(?=[^@]*@)', '', regex=True)
df_new['Username'] = df_new['Username'].str.replace(r'(\.)(?=[^@]*@)', '', regex=True)
df_new['Email'] = df_new['Email'].str.replace(r'(\.)(?=[^@]*@)', '', regex=True)

# Ensure password has 8 characters for those which are of length 7
# df_new['Password'] = df_new['Password'].apply(lambda x: '0' + str(x) if len(str(x)) == 7 else str(x))
df_new['Password'] = df_new['Password'].apply(lambda x: '0' + str(int(x)) if pd.notna(x) and len(str(int(x))) == 7 else str(int(x)) if pd.notna(x) else x)


# Check for password errors
#for idx, password in df_new['Password'].items():
 #   if len(str(password)) != 8 and str(password) != '':
  #      logging.error(f"Row {idx}: Password is not 8 characters or blank: \"{password}\"")


# Clean up usernames and email addresses by removing leading and trailing spaces
df_base['Username'] = df_base['Username'].str.strip()
df_base['Email'] = df_base['Email'].str.strip()

df_new['Username'] = df_new['Username'].str.strip()
df_new['Email'] = df_new['Email'].str.strip()

# Replace NaN values and empty strings in 'Username', 'Email', 'First Name', 'Last Name' and 'Building' columns
cols_to_process = ['Username', 'Email', 'First Name', 'Last Name', 'Building']
df_base[cols_to_process] = df_base[cols_to_process].replace([np.nan, ''], 'Not Provided')
df_new[cols_to_process] = df_new[cols_to_process].replace([np.nan, ''], 'Not Provided')

# Convert the 'Hire Date' and 'Term Date' to datetime format
df_new['Hire Date'] = pd.to_datetime(df_new['Hire Date'], format=date_format).dt.date
df_new['Term Date'] = pd.to_datetime(df_new['Term Date'], format=date_format).dt.date


# Identify new hires and terminations based on 'Hire Date' and 'Term Date'
# We'll mark these in the 'Action' column with 'A' or 'T'
df_new.loc[
    (df_new['Hire Date'] >= start_date) & (df_new['Hire Date'] <= end_date) & (df_new['Action'] != 'T'),
    'Action'
] = 'A'

df_new.loc[df_new['Term Date'].between(start_date, end_date), 'Action'] = 'T'

# Merge the new dataframe with the base one
df_merged = pd.merge(df_new, df_base, on='Employee Number', suffixes=('_new', '_base'))

# Identify the rows with changes in the specified columns
# We'll mark these in the 'Action' column with 'U'
for col in compare_cols:
    # Loop through each row for additional conditional checks
    for index, row in df_merged.iterrows():
        # Skip rows with hire dates within the range
        if (row['Hire Date'] >= start_date) and (row['Hire Date'] <= end_date):
            continue


        # Mark as 'U' if the columns don't match AND it's not a future hire
        if row[col + '_new'] != row[col + '_base']:
            df_merged.loc[index, 'Action_new'] = 'U'

# For rows marked with 'U', set the password to null
df_merged.loc[df_merged['Action_new'] == 'U', 'Password_new'] = np.nan
    
# Filter the rows to include in the output
# We're only interested in rows marked with 'A', 'T', or 'U'
df_output = df_merged.loc[df_merged['Action_new'].isin(['A', 'T', 'U'])]

# Select only the '_new' columns from the output DataFrame
#output_cols_new = [col for col in df_output.columns if col.endswith('_new')]
output_cols_new = [col for col in df_output.columns if col.endswith('_new') or col == 'Employee Number']

df_output = df_output[output_cols_new]

# Drop the _new suffix from the column names in the output
df_output.columns = df_output.columns.str.replace('_new$', '', regex=True)

# Create a DataFrame including rows marked with 'A', 'T' and 'U'
df_output_changed = df_output[df_output['Action'].isin(['A', 'T', 'U'])]

# Debugging: print the changes
for col in compare_cols:
    # Get a boolean series indicating rows with changes in this column
    change_idx = df_merged[col + '_new'] != df_merged[col + '_base']

    # If there are changes in this column, print the changes
    if change_idx.any():
        for idx in change_idx[change_idx].index:
            hire_date = df_merged.loc[idx, 'Hire Date']
            term_date = df_merged.loc[idx, 'Term Date']
            action = df_merged.loc[idx, 'Action_new']
            logging.info(f"\nRow {idx} has a change in '{col}': \"{df_merged.loc[idx, col + '_base']}\" -> \"{df_merged.loc[idx, col + '_new']}\"")
            logging.info(f"Hire Date: {hire_date}, Term Date: {term_date}, Action: {action}\n")

# Log entries that are flagged as 'U' or 'T'
a_entries = df_output[df_output['Action'] == 'A']
t_entries = df_output[df_output['Action'] == 'T']

# Logging for 'U' entries
for idx in a_entries.index:
    hire_date = df_merged.loc[idx, 'Hire Date']
    term_date = df_merged.loc[idx, 'Term Date']
    employee_number = df_merged.loc[idx, 'Employee Number']
    first_name = df_merged.loc[idx, 'First Name_new']
    last_name = df_merged.loc[idx, 'Last Name_new']
    logging.info(f"\nEmployee number {employee_number}")
    logging.info(f"{first_name}")
    logging.info(f"{last_name}")
    logging.info(f"\nRow {idx} is marked as 'A' (New Hire)")
    logging.info(f"Hire Date: {hire_date}, Term Date: {term_date}\n")

# Logging for 'T' entries
for idx in t_entries.index:
    hire_date = df_merged.loc[idx, 'Hire Date']
    term_date = df_merged.loc[idx, 'Term Date']
    employee_number = df_merged.loc[idx, 'Employee Number']
    first_name = df_merged.loc[idx, 'First Name_new']
    last_name = df_merged.loc[idx, 'Last Name_new']
    logging.info(f"\nEmployee number{employee_number}")
    logging.info(f"{first_name}")
    logging.info(f"{last_name}")
    logging.info(f"\nRow {idx} is marked as 'T' (Termination)")
    logging.info(f"Hire Date: {hire_date}, Term Date: {term_date}\n")


# Write the output dataframe to a new CSV file with today's date in the filename
df_output['Password'] = df_output['Password'].astype(str)
df_output.to_csv(f'1234_CompanyName_{today_str}.csv', index=False)

# Prepare the new base file, excluding 'Hire Date' and 'Term Date'
df_new_base = df_new.drop(columns=['Hire Date', 'Term Date'])

# Replace the base file with the new report
df_new_base.to_csv('1234_CompanyName_base.csv', index=False)

# Open the log file for review
os.startfile('file_processing.log')


