import pandas as pd
import os
import sqlite3

# Specify your folder's path
folder_path = r'/home/saturn/PycharmProjects/leadscaptain.com/backend/linkedincsv/'
all_files = os.listdir(folder_path)

# Filter out non-CSV files
csv_files = [f for f in all_files if f.endswith('.csv')]

# Create or connect to an SQLite database
database_path = os.path.join(folder_path, 'combined_data.db')
conn = sqlite3.connect(database_path)

# Table name for the SQLite database
table_name = 'combined_data'

for csv in csv_files:
    file_path = os.path.join(folder_path, csv)
    try:
        # Read in chunks and write each chunk to SQLite
        for chunk in pd.read_csv(file_path, low_memory=True, chunksize=10000):
            chunk.to_sql(table_name, conn, if_exists='append', index=False)
    except UnicodeDecodeError:
        try:
            for chunk in pd.read_csv(file_path, sep='\t', encoding='utf-16', chunksize=10000, low_memory=False):
                chunk.to_sql(table_name, conn, if_exists='append', index=False)
        except Exception as e:
            print(f"Could not read file {csv} due to encoding error: {e}")
    except Exception as e:
        print(f"Could not read file {csv} due to an unexpected error: {e}")

# Close the database connection
conn.close()
print(f"Data successfully written to SQLite database at: {database_path}")
