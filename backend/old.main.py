from fastapi import FastAPI, HTTPException, Query
from typing import List, Optional
import sqlite3

# FastAPI instance
app = FastAPI()

# Path to your SQLite database

DB_PATH = 'organizations.db'

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # Allows fetching results as dictionaries
    return conn

# Endpoint for searching organizations by name, industry, location, or other fields
@app.get("/search/")
async def search_organizations(
    name: Optional[str] = Query(None, description="Organization name"),
    industry: Optional[str] = Query(None, description="Industry of the organization"),
    location: Optional[str] = Query(None, description="City or country of the organization"),
    min_employees: Optional[int] = Query(None, description="Minimum number of employees"),
    max_employees: Optional[int] = Query(None, description="Maximum number of employees")
):
    # Establish database connection
    conn = get_db_connection()
    cursor = conn.cursor()

    # Base query
    query = "SELECT * FROM Apollo_V7_V5_org_all_fields WHERE 1=1"
    params = []

    # Add conditions based on provided query parameters
    if name:
        query += " AND organization_name LIKE ?"
        params.append(f"%{name}%")
    if industry:
        query += " AND organization_industries LIKE ?"
        params.append(f"%{industry}%")
    if location:
        query += " AND (organization_hq_location_city LIKE ? OR organization_hq_location_country LIKE ?)"
        params.extend([f"%{location}%", f"%{location}%"])
    if min_employees:
        query += " AND organization_num_current_employees >= ?"
        params.append(min_employees)
    if max_employees:
        query += " AND organization_num_current_employees <= ?"
        params.append(max_employees)

    # Execute query
    cursor.execute(query, params)
    results = cursor.fetchall()

    conn.close()

    if not results:
        raise HTTPException(status_code=404, detail="No matching organizations found")

    # Return results as list of dictionaries
    return [dict(row) for row in results]

# Run the FastAPI app using uvicorn
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
