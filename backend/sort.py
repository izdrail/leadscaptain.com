import sqlite3

# Connect to your SQLite database
database_path = '/home/saturn/PycharmProjects/leadscaptain.com/backend/combined_data.db'
conn = sqlite3.connect(database_path)

# Create a cursor object
cursor = conn.cursor()

# Step 1: Create an index on location_continent
cursor.execute("CREATE INDEX IF NOT EXISTS idx_location_continent ON combined_data(location_continent);")

# Step 2: Retrieve unique continent names
cursor.execute("SELECT DISTINCT location_continent FROM combined_data;")
continents = cursor.fetchall()

# Step 3: Create tables for each continent using lowercase names
for continent_tuple in continents:
    continent_name = continent_tuple[0]
    if continent_name:  # Check for non-null continent names
        lowercase_continent_name = continent_name.lower().replace(' ', '_')  # Convert to lowercase and replace spaces with underscores
        # Create a table for each continent
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS combined_data_{lowercase_continent_name} AS
        SELECT * FROM combined_data WHERE location_continent = ?;
        """, (continent_name,))

# Commit changes and close the connection
conn.commit()
conn.close()
print("Data partitioned by continent with lowercase table names.")
