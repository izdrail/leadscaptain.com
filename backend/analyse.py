import pandas as pd
import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect('people_data.db')

# Load data from the database tables into pandas DataFrames
people_df = pd.read_sql_query("SELECT * FROM people", conn)
jobs_df = pd.read_sql_query("SELECT * FROM jobs", conn)
emails_df = pd.read_sql_query("SELECT * FROM emails", conn)
education_df = pd.read_sql_query("SELECT * FROM education", conn)

# Close the database connection
conn.close()

# Example Analysis 1: Count people by gender
gender_counts = people_df['gender'].value_counts().reset_index()
gender_counts.columns = ['gender', 'count']
gender_counts.to_csv('gender_counts.csv', index=False)

# Example Analysis 2: Average inferred years of experience
avg_experience = people_df['inferred_years_experience'].dropna().astype(int).mean()
avg_experience_df = pd.DataFrame({'average_years_experience': [avg_experience]})
avg_experience_df.to_csv('average_years_experience.csv', index=False)

# Example Analysis 3: Top 5 industries by job count
industry_counts = people_df['industry'].value_counts().head(5).reset_index()
industry_counts.columns = ['industry', 'count']
industry_counts.to_csv('top_industries.csv', index=False)

# Example Analysis 4: Most common job titles
common_job_titles = jobs_df['job_title'].value_counts().head(10).reset_index()
common_job_titles.columns = ['job_title', 'count']
common_job_titles.to_csv('common_job_titles.csv', index=False)

# Example Analysis 5: Location distribution (Countries)
country_counts = people_df['location_country'].value_counts().head(10).reset_index()
country_counts.columns = ['country', 'count']
country_counts.to_csv('top_countries.csv', index=False)

# Example Analysis 6: LinkedIn connections distribution
linkedin_connections_stats = people_df['linkedin_connections'].describe().reset_index()
linkedin_connections_stats.columns = ['statistic', 'value']
linkedin_connections_stats.to_csv('linkedin_connections_stats.csv', index=False)

# Example Analysis 7: Breakdown of degrees in education data
degree_counts = education_df['degrees'].value_counts().head(10).reset_index()
degree_counts.columns = ['degree', 'count']
degree_counts.to_csv('top_degrees.csv', index=False)

print("Analysis results saved to CSV files.")
