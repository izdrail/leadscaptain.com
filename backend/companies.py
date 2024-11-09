import sqlite3

# Path to your SQLite database
database_path = '/home/saturn/PycharmProjects/leadscaptain.com/backend/combined_data.db'


# List of continent-based tables (based on previous table structure)
continent_tables = [
    'combined_data_africa',
    'combined_data_asia',
    'combined_data_europe',
    'combined_data_north_america',
    'combined_data_south_america',
    'combined_data_australia',
    'combined_data_antartica'
]

# Connect to the SQLite database
conn = sqlite3.connect(database_path)
cursor = conn.cursor()

# Dictionary to hold companies from each continent
companies_by_continent = {}

# Loop through each continent table and extract unique company names
for table in continent_tables:
    try:
        # SQL query to select distinct company names
        cursor.execute(f"SELECT DISTINCT job_company_name FROM {table} WHERE job_company_name IS NOT NULL;")

        # Fetch all unique company names
        companies = cursor.fetchall()

        # Store the results in the dictionary under the continent's name
        companies_by_continent[table] = [company[0] for company in companies if company[0]]
        print(f"Extracted {len(companies_by_continent[table])} companies from {table}.")

    except Exception as e:
        # Catch any errors and log them
        print(f"Error processing table {table}: {e}")
        continue  # Continue to the next table

# Close the database connection
conn.close()

# Print the results or process them further as needed
for continent, companies in companies_by_continent.items():
    print(f"\nContinent: {continent}")
    print(f"Companies ({len(companies)}): {companies[:10]}")  # Print first 10 as sample
